"""灾备演练业务验收测试。

覆盖任务要求：
* 同一数据库中“先演练、后真实事故”，演练前后生产主站/席位/容量逐表不变；
* 正常与失败 JSON fixture 均可运行且结论正确、事件可重放；
* 证据缺失、布尔成功值、代次换代、许可撤回、超时、资源清偿；
* 影子容量不足 -> 预约稍后独立影子席位 -> 强证明但错过当天期限；
* 生产验证等级由停写窗口、逐笔对账等原件决定，并记录客户损失；
* 真实 evacuate 事故不得被误标为演练；演练不得写客户真实序号。
"""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from cloudcourt import intent, seat, evac, rehearsal
from cloudcourt.store import connect, replay, one, many

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / 'fixtures'

PRODUCTION_TABLES = (
    'workload_intents', 'owner_grants', 'residency_rules',
    'cluster_inventory', 'placement_leases', 'capacity_claims',
    'incident_cases', 'move_plans', 'handoff_receipts', 'recovery_cases',
)


def load_fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding='utf-8'))


class RehearsalTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'state.sqlite')
        self.db = connect(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    # ------------------------------------------------------------- 世界搭建
    def prepare_world(self, other_standby=True):
        """intent-normal + seat-normal + assign_primary（C0=cluster-a）。"""
        for command in load_fixture('intent-normal.json'):
            intent.run(self.db, command)
        for command in load_fixture('seat-normal.json'):
            seat.run(self.db, command)
        assigned = intent.run(self.db, {
            'event_id': 'assign-primary-a', 'op': 'assign_primary',
            'intent': 'work-a', 'cluster': 'cluster-a',
            'grant': 'grant-a', 'at': 21,
        })
        self.assertEqual(assigned['decision'], 'primary_assigned')

        if other_standby:
            # C1=cluster-b 上另有一个 workload 的备席，正在为其事故待命。
            intent.run(self.db, {
                'event_id': 'other-claim', 'op': 'claim', 'id': 'work-other',
                'tenant': 'tenant-a', 'region': 'east', 'role': 'critical',
            })
            seat.run(self.db, {
                'event_id': 'obs-d', 'op': 'observe', 'id': 'cluster-d',
                'region': 'east', 'domain': 'rack-d', 'cpu': 8, 'mem': 16,
                'generation': 1, 'healthy': True,
            })
            self.assertEqual(seat.run(self.db, {
                'event_id': 'lease-other-primary', 'op': 'reserve',
                'id': 'lease-d-other', 'intent': 'work-other',
                'cluster': 'cluster-d', 'role': 'primary',
                'cpu': 2, 'mem': 2, 'expires': 90,
            })['decision'], 'reserved')
            seat.run(self.db, {'event_id': 'occupy-other-primary', 'op': 'occupy',
                               'id': 'lease-d-other', 'at': 20})
            self.assertEqual(seat.run(self.db, {
                'event_id': 'lease-other-c1', 'op': 'reserve',
                'id': 'lease-b-other', 'intent': 'work-other',
                'cluster': 'cluster-b', 'role': 'backup',
                'cpu': 3, 'mem': 4, 'expires': 90,
            })['decision'], 'reserved')
            seat.run(self.db, {'event_id': 'occupy-other-c1', 'op': 'occupy',
                               'id': 'lease-b-other', 'at': 20})
            evac.run(self.db, {
                'event_id': 'inc-other', 'op': 'incident', 'id': 'inc-other',
                'cluster': 'cluster-d', 'severity': 4, 'generation': 1,
            })

    # ------------------------------------------------------------- 快照工具
    def snapshot_production(self):
        snap = {}
        for table in PRODUCTION_TABLES:
            snap[table] = [tuple(r) for r in
                           self.db.execute(f'SELECT * FROM {table} ORDER BY rowid')]
        return snap

    def assert_production_unchanged(self, before):
        self.assertEqual(before, self.snapshot_production())

    def capacity_totals(self):
        rows = many(self.db,
                    "SELECT cluster, COALESCE(SUM(cpu),0) cpu, COALESCE(SUM(mem),0) mem "
                    "FROM capacity_claims WHERE state='active' GROUP BY cluster ORDER BY cluster")
        return {r['cluster']: (r['cpu'], r['mem']) for r in rows}

    # ------------------------------------------------- 影子演练构造原语
    def offer(self, oid='offer-s1', cluster='cluster-shadow-1', cpu=8, mem=16, available_at=0):
        return rehearsal.run(self.db, {
            'event_id': f'{oid}-cmd', 'op': 'offer_shadow', 'id': oid,
            'cluster': cluster, 'cpu': cpu, 'mem': mem, 'generation': 1,
            'available_at': available_at, 'independent': True,
        })

    def seal(self, rid='rh1', source='cluster-a', mode='shadow', at=10,
             recovery_point='seq:1000', probe_seconds=30, timeout_at=90, deadline=120,
             target=None, backup='cluster-b', **extra):
        command = {
            'event_id': f'{rid}-seal', 'op': 'seal', 'id': rid, 'intent': 'work-a',
            'source': source, 'backup': backup, 'at': at, 'mode': mode,
            'recovery_point': recovery_point, 'probe_seconds': probe_seconds,
            'timeout_at': timeout_at, 'deadline': deadline,
        }
        if target:
            command['target'] = target
        command.update(extra)
        return rehearsal.run(self.db, command)

    def consent(self, rid='rh1', at=11):
        return rehearsal.run(self.db, {'event_id': f'{rid}-consent', 'op': 'consent',
                                      'rehearsal': rid, 'at': at})

    def book(self, rid='rh1', resid='res1', offer='offer-s1', at=12, cpu=4, mem=8):
        return rehearsal.run(self.db, {
            'event_id': f'{rid}-{resid}', 'op': 'reserve_shadow', 'id': resid,
            'rehearsal': rid, 'offer': offer, 'cpu': cpu, 'mem': mem, 'at': at,
        })

    def start(self, rid='rh1', at=13, data_ref=None, event='start'):
        return rehearsal.run(self.db, {
            'event_id': f'{rid}-{event}', 'op': 'start', 'rehearsal': rid, 'at': at,
            'data_ref': data_ref or f'isolated-copy-{rid}', 'isolation': 'isolated',
        })

    def fence(self, rid='rh1', eid='ev-fence', at=14, generation=1, scope=None,
              cluster='cluster-a', cluster_generation=1):
        scope = scope or f'scope-{rid}-g{generation}'
        return rehearsal.run(self.db, {
            'event_id': f'{rid}-{eid}-cmd', 'op': 'evidence', 'id': eid,
            'rehearsal': rid, 'kind': 'source_fenced', 'at': at,
            'generation': generation, 'scope': scope, 'cluster': cluster,
            'cluster_generation': cluster_generation,
            'origin_ref': f'origin/{eid}.json', 'fence_scope': scope,
        })

    def ready(self, rid='rh1', eid='ev-ready', at=15, generation=1, scope=None,
              cluster='cluster-shadow-1', cluster_generation=1,
              recovery_point='seq:1000', recovery_seconds=24):
        scope = scope or f'scope-{rid}-g{generation}'
        return rehearsal.run(self.db, {
            'event_id': f'{rid}-{eid}-cmd', 'op': 'evidence', 'id': eid,
            'rehearsal': rid, 'kind': 'target_ready', 'at': at,
            'observed_at': at, 'generation': generation, 'scope': scope,
            'cluster': cluster, 'cluster_generation': cluster_generation,
            'origin_ref': f'origin/{eid}.json', 'recovery_point': recovery_point,
            'recovery_seconds': recovery_seconds,
        })

    def probe(self, rid='rh1', eid='ev-probe', at=46, generation=1, scope=None,
              span=(16, 46), step=5, customer=True, sustained=None):
        scope = scope or f'scope-{rid}-g{generation}'
        if sustained is not None:
            body = {'sustained_seconds': sustained}
        else:
            lo, hi = span
            times = list(range(lo, hi + 1, step))
            if times[-1] != hi:
                times.append(hi)
            body = {'samples': [{'at': t, 'ok': True} for t in times]}
        command = {
            'event_id': f'{rid}-{eid}-cmd', 'op': 'evidence', 'id': eid,
            'rehearsal': rid, 'kind': 'probe', 'at': at,
            'generation': generation, 'scope': scope,
            'origin_ref': f'origin/{eid}.json',
        }
        if customer:
            command['origin'] = 'customer_probe'
        command.update(body)
        return rehearsal.run(self.db, command)

    def certify(self, rid='rh1', cid='cert1', at=50, **extra):
        command = {'event_id': f'{rid}-{cid}-cmd', 'op': 'certify', 'id': cid,
                   'rehearsal': rid, 'at': at}
        command.update(extra)
        return rehearsal.run(self.db, command)

    def happy_shadow(self, rid='rh1', production_world=True):
        self.offer()
        self.seal(rid=rid)
        self.consent(rid)
        self.book(rid)
        self.start(rid)
        self.fence(rid)
        self.ready(rid)
        self.probe(rid)
        return self.certify(rid)


class FixtureTests(RehearsalTestBase):
    def test_normal_fixture_certifies_strong_shadow_level_and_replays(self):
        self.prepare_world(other_standby=False)
        results = [rehearsal.run(self.db, c) for c in load_fixture('rehearsal-normal.json')]
        by_event = {c['event_id']: r for c, r in
                    zip(load_fixture('rehearsal-normal.json'), results)}
        self.assertEqual(by_event['rn1-seal']['decision'], 'sealed')
        self.assertEqual(by_event['rn8-certify']['decision'], 'certified')
        self.assertEqual(by_event['rn8-certify']['level'], 'shadow_end_to_end')
        self.assertTrue(by_event['rn8-certify']['qualified'])
        self.assertEqual(by_event['rn8-certify']['conclusion'], 'filable_today')

        cert = one(self.db, 'SELECT * FROM rehearsal_certificates WHERE id=?', ('cert-normal',))
        self.assertEqual(cert['level'], 'shadow_end_to_end')
        self.assertEqual(cert['qualified'], 1)
        self.assertEqual(cert['generation'], 1)
        self.assertTrue(replay(self.path)['verified'])

    def test_failure_fixture_times_out_settles_and_never_certifies(self):
        self.prepare_world(other_standby=False)
        commands = load_fixture('rehearsal-failure.json')
        results = [rehearsal.run(self.db, c) for c in commands]
        by_event = {c['event_id']: r for c, r in zip(commands, results)}

        self.assertEqual(by_event['rf6-reserve-exhausted']['decision'],
                         'shadow_capacity_exhausted')
        self.assertEqual(by_event['rf7-certify-empty']['decision'], 'evidence_incomplete')
        self.assertEqual(by_event['rf11-probe-too-short']['decision'], 'probe_duration_unmet')
        self.assertEqual(by_event['rf12-timeout-early']['decision'], 'not_timed_out')
        timed_out = by_event['rf13-timeout']
        self.assertEqual(timed_out['decision'], 'rehearsal_timed_out')
        self.assertEqual(timed_out['released'], ['resource-fail'])
        self.assertEqual(by_event['rf14-late-probe']['decision'], 'rehearsal_closed')
        self.assertEqual(by_event['rf15-certify-closed']['decision'], 'rehearsal_closed')
        self.assertEqual(by_event['rf16-settle-again']['decision'], 'settled')
        self.assertEqual(by_event['rf16-settle-again']['count'], 0)

        resource = one(self.db, 'SELECT * FROM rehearsal_resources WHERE id=?', ('resource-fail',))
        self.assertEqual(resource['state'], 'released')
        self.assertEqual(resource['release_reason'], 'probe_sustain_timeout')
        self.assertIsNotNone(resource['released_at'])
        self.assertIsNone(one(self.db, 'SELECT 1 FROM rehearsal_certificates '
                                       'WHERE rehearsal=?', ('rehearsal-fail',)))
        case = one(self.db, 'SELECT * FROM rehearsal_cases WHERE id=?', ('rehearsal-fail',))
        self.assertEqual(case['state'], 'timed_out')
        self.assertEqual(case['fail_reason'], 'probe_sustain_timeout')
        # 失败探针原件仍可复查，但被标记为无效。
        short = one(self.db, 'SELECT * FROM rehearsal_evidence WHERE id=?',
                    ('evidence-probe-f-short',))
        self.assertEqual(short['valid'], 0)
        self.assertEqual(short['reject_reason'], 'probe_duration_unmet')
        self.assertTrue(replay(self.path)['verified'])

    def test_cli_demo_runs_with_rehearsal_fixtures(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / 'demo.sqlite')
            proc = subprocess.run(
                [sys.executable, '-m', 'cloudcourt', 'demo', '--db', db_path],
                cwd=str(ROOT), capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            report = json.loads(proc.stdout)
            self.assertIsInstance(report, list)
            self.assertTrue(Path(db_path).exists())


class IsolationBeforeRealIncidentTests(RehearsalTestBase):
    def test_rehearsal_then_real_incident_in_same_database(self):
        self.prepare_world(other_standby=True)
        before = self.snapshot_production()
        capacity_before = self.capacity_totals()

        # ---- 先做一次完整的强影子演练
        result = self.happy_shadow('rh-accept')
        self.assertEqual(result['decision'], 'certified')
        self.assertEqual(result['level'], 'shadow_end_to_end')

        # 演练前后：所有生产业务表逐行一致。
        self.assert_production_unchanged(before)
        self.assertEqual(self.capacity_totals(), capacity_before)
        primary = one(self.db, 'SELECT primary_cluster,revision FROM workload_intents WHERE id=?',
                      ('work-a',))
        self.assertEqual(primary['primary_cluster'], 'cluster-a')
        # C1 上“另一项事故待命”的备席原样保留。
        other = one(self.db, 'SELECT state FROM placement_leases WHERE id=?', ('lease-b-other',))
        self.assertEqual(other['state'], 'occupied')
        # 生产 claim 没有一条被演练释放（a、b、d-other、b-other 共 4 条 active）。
        self.assertEqual(
            one(self.db, "SELECT COUNT(*) n FROM capacity_claims WHERE state='active'")['n'], 4)

        # ---- 再做真实事故：既有 evac 链路必须照常切换
        evac.run(self.db, {'event_id': 'inc-a', 'op': 'incident', 'id': 'inc-a',
                           'cluster': 'cluster-a', 'severity': 4, 'generation': 1})
        self.assertEqual(evac.run(self.db, {
            'event_id': 'plan-a-b', 'op': 'plan', 'id': 'move-a-b', 'intent': 'work-a',
            'source': 'cluster-a', 'target': 'cluster-b', 'incident': 'inc-a',
        })['decision'], 'planned')
        self.assertEqual(evac.run(self.db, {
            'event_id': 'fence-a', 'op': 'receipt', 'id': 'rc-fence-a', 'plan': 'move-a-b',
            'kind': 'source_fenced', 'cluster': 'cluster-a', 'generation': 1,
        })['decision'], 'recorded')
        self.assertEqual(evac.run(self.db, {
            'event_id': 'ready-b', 'op': 'receipt', 'id': 'rc-ready-b', 'plan': 'move-a-b',
            'kind': 'target_ready', 'cluster': 'cluster-b', 'generation': 1,
        })['decision'], 'recorded')
        self.assertEqual(evac.run(self.db, {'event_id': 'handoff-a-b', 'op': 'handoff',
                                           'plan': 'move-a-b'})['decision'], 'transferred')
        moved = one(self.db, 'SELECT primary_cluster FROM workload_intents WHERE id=?', ('work-a',))
        self.assertEqual(moved['primary_cluster'], 'cluster-b')
        # 真实事故确实释放了源席，反衬演练阶段没有动生产资源。
        lease_a = one(self.db, "SELECT state FROM placement_leases WHERE id='lease-a'")
        self.assertEqual(lease_a['state'], 'released')
        self.assertTrue(replay(self.path)['verified'])


class EvidenceAndCertificationTests(RehearsalTestBase):
    def test_missing_evidence_and_bare_boolean_cannot_qualify(self):
        self.prepare_world(other_standby=False)
        self.offer()
        self.seal('rh-miss')
        self.consent('rh-miss')
        self.book('rh-miss', resid='res-miss')
        self.start('rh-miss')

        missing = self.certify('rh-miss', cid='cert-missing')
        self.assertEqual(missing['decision'], 'evidence_incomplete')
        self.assertEqual(set(missing['missing']),
                         {'source_fenced', 'target_ready', 'probe'})
        self.assertIsNone(one(self.db, 'SELECT 1 FROM rehearsal_certificates WHERE id=?',
                              ('cert-missing',)))

        # 直接在 certify 里塞 success=true 也不能充当原件。
        inline_boolean = self.certify('rh-miss', cid='cert-inline', success=True)
        self.assertEqual(inline_boolean['decision'], 'evidence_incomplete')

        # 只有一个布尔模拟结果时，最多得到明确“不具接管证明力”的模拟级证书。
        sim = rehearsal.run(self.db, {
            'event_id': 'miss-sim-cmd', 'op': 'evidence', 'id': 'ev-sim',
            'rehearsal': 'rh-miss', 'kind': 'simulation', 'at': 20, 'success': True,
        })
        self.assertEqual(sim['decision'], 'recorded')
        certified = self.certify('rh-miss', cid='cert-sim', at=30)
        self.assertEqual(certified['decision'], 'certified')
        self.assertEqual(certified['level'], 'simulated')
        self.assertFalse(certified['qualified'])
        self.assertEqual(certified['conclusion'], 'not_takeover_proof')
        self.assertFalse(certified['filable'])
        self.assertEqual(certified['regulatory_action'],
                         'supplement_only_not_takeover_evidence')
        self.assertTrue(replay(self.path)['verified'])

    def test_rto_limit_is_derived_from_target_original(self):
        self.prepare_world(other_standby=False)
        self.offer()
        self.seal('rh-rto', rto_limit=10)  # 目标原件报告 recovery_seconds=24
        self.consent('rh-rto')
        self.book('rh-rto', resid='res-rto')
        self.start('rh-rto')
        self.fence('rh-rto')
        self.ready('rh-rto', recovery_seconds=24)
        self.probe('rh-rto')
        cert = self.certify('rh-rto', cid='cert-rto')
        self.assertEqual(cert['decision'], 'certified')
        self.assertEqual(cert['measured_recovery_seconds'], 24)
        self.assertEqual(cert['rto_limit'], 10)
        self.assertFalse(cert['rto_met'])
        # 影子演练对客户零停写。
        self.assertEqual(cert['customer_loss']['write_stop_seconds'], 0)
        self.assertFalse(cert['customer_loss']['customer_facing_outage'])
        # RTO 未达标是事实结论，但不篡改证明等级；监管行动仍是当天可提交的强证明，
        # 是否采纳 RTO 缺口由审计员依据原件判断。
        self.assertEqual(cert['regulatory_action'], 'submit_to_regulator_today')
        self.assertTrue(replay(self.path)['verified'])

    def test_production_level_needs_window_and_entry_reconciliation(self):
        self.prepare_world(other_standby=False)
        before = self.snapshot_production()
        self.seal('rh-prod', mode='production', target='cluster-b',
                  recovery_point='seq:3000', deadline=300, timeout_at=None,
                  rto_limit=60)
        self.consent('rh-prod')

        # 没有客户确认停写窗口，不能开始生产验证。
        self.assertEqual(self.start('rh-prod', event='start-blocked')['decision'],
                         'write_window_required')
        window = rehearsal.run(self.db, {
            'event_id': 'prod-window', 'op': 'confirm_window', 'id': 'window-prod',
            'rehearsal': 'rh-prod', 'at': 12, 'confirmed': True,
            'stop_seconds': 42, 'approved_by': 'customer-dba',
        })
        self.assertEqual(window['decision'], 'window_confirmed')
        self.assertEqual(self.start('rh-prod', at=13)['decision'], 'started')
        self.fence('rh-prod', eid='p-fence')
        self.ready('rh-prod', eid='p-ready', cluster='cluster-b',
                   recovery_point='seq:3000')
        self.probe('rh-prod', eid='p-probe')

        incomplete = self.certify('rh-prod', cid='cert-prod-bad', at=60)
        self.assertEqual(incomplete['decision'], 'evidence_incomplete')
        self.assertIn('rp_reconciliation', incomplete['missing'])

        # 逐笔对账存在差异 -> 原件被拒，仍不能拿生产级证书。
        bad_recon = rehearsal.run(self.db, {
            'event_id': 'prod-recon-bad-cmd', 'op': 'evidence', 'id': 'recon-bad',
            'rehearsal': 'rh-prod', 'kind': 'reconciliation', 'at': 47,
            'generation': 1, 'scope': 'scope-rh-prod-g1',
            'origin_ref': 'origin/recon-bad.json',
            'entries': [{'seq': 1, 'matched': True}, {'seq': 2, 'matched': False}],
        })
        self.assertEqual(bad_recon['decision'], 'reconciliation_mismatch')
        still_incomplete = self.certify('rh-prod', cid='cert-prod-still', at=61)
        self.assertEqual(still_incomplete['decision'], 'evidence_incomplete')

        good_recon = rehearsal.run(self.db, {
            'event_id': 'prod-recon-good-cmd', 'op': 'evidence', 'id': 'recon-good',
            'rehearsal': 'rh-prod', 'kind': 'reconciliation', 'at': 48,
            'generation': 1, 'scope': 'scope-rh-prod-g1',
            'origin_ref': 'origin/recon-good.json',
            'entries_total': 3, 'entries_matched': 3,
        })
        self.assertEqual(good_recon['decision'], 'recorded')
        certified = self.certify('rh-prod', cid='cert-prod', at=62)
        self.assertEqual(certified['decision'], 'certified')
        self.assertEqual(certified['level'], 'production_verified')
        self.assertEqual(certified['level_rank'], 3)
        self.assertTrue(certified['qualified'])
        self.assertEqual(certified['conclusion'], 'filable_today')
        self.assertEqual(certified['regulatory_action'], 'submit_to_regulator_today')
        self.assertEqual(certified['customer_loss']['write_stop_seconds'], 42)
        self.assertTrue(certified['customer_loss']['customer_facing_outage'])
        self.assertEqual(certified['measured_recovery_seconds'], 24)
        self.assertTrue(certified['rto_met'])

        # 生产验证同样只读取/封存生产表，不改动主站、席位、容量。
        self.assert_production_unchanged(before)
        self.assertTrue(replay(self.path)['verified'])


class GenerationAndConsentTests(RehearsalTestBase):
    def test_target_rebuild_replaces_generation_and_blocks_late_signing(self):
        self.prepare_world(other_standby=False)
        self.offer()
        self.seal('rh-gen')
        self.consent('rh-gen')
        self.book('rh-gen', resid='res-gen')
        self.start('rh-gen')
        self.assertEqual(self.fence('rh-gen', eid='g-fence')['decision'], 'recorded')
        self.assertEqual(self.ready('rh-gen', eid='g-ready')['decision'], 'recorded')

        rebuilt = rehearsal.run(self.db, {
            'event_id': 'rh-gen-rebuild', 'op': 'rebuild', 'rehearsal': 'rh-gen',
            'at': 30, 'reason': 'target_node_rebuilt',
        })
        self.assertEqual(rebuilt['decision'], 'generation_replaced')
        self.assertEqual(rebuilt['generation'], 2)
        self.assertEqual(rebuilt['scope'], 'scope-rh-gen-g2')
        self.assertEqual(rebuilt['released'], ['res-gen'])
        old_resource = one(self.db, 'SELECT state FROM rehearsal_resources WHERE id=?', ('res-gen',))
        self.assertEqual(old_resource['state'], 'released')

        # 换代后迟到的旧代次探针：不能补签，只留下 valid=0 的拒收原件。
        late = self.probe('rh-gen', eid='g-late-old', at=31, generation=1,
                          scope='scope-rh-gen-g1', sustained=30)
        self.assertEqual(late['decision'], 'generation_replaced')
        self.assertEqual(late['recorded'], 'rejected')
        rejected = one(self.db, 'SELECT * FROM rehearsal_evidence WHERE id=?', ('g-late-old',))
        self.assertEqual(rejected['valid'], 0)
        self.assertEqual(rejected['reject_reason'], 'generation_replaced')

        # 新代次必须重新占用承载资源并重新提交全部原件。
        self.book('rh-gen', resid='res-gen-2', at=32)
        restarted = self.start('rh-gen', at=33, event='start-gen2')
        self.assertEqual(restarted['decision'], 'started')
        self.fence('rh-gen', eid='g-fence-2', at=34, generation=2,
                   scope='scope-rh-gen-g2')
        self.ready('rh-gen', eid='g-ready-2', at=35, generation=2,
                   scope='scope-rh-gen-g2')
        self.probe('rh-gen', eid='g-probe-2', at=66, generation=2,
                   scope='scope-rh-gen-g2', span=(36, 66))
        certified = self.certify('rh-gen', cid='cert-gen', at=70)
        self.assertEqual(certified['decision'], 'certified')
        self.assertEqual(certified['level'], 'shadow_end_to_end')
        self.assertEqual(certified['generation'], 2)
        self.assertNotIn('g-late-old', certified['basis_evidence'])
        self.assertEqual(set(certified['basis_evidence']),
                         {'g-fence-2', 'g-ready-2', 'g-probe-2'})
        self.assertTrue(replay(self.path)['verified'])

    def test_consent_withdrawal_blocks_late_results_and_settles(self):
        self.prepare_world(other_standby=False)
        before = self.snapshot_production()
        self.offer()
        self.seal('rh-wd')
        self.consent('rh-wd')
        self.book('rh-wd', resid='res-wd')
        self.start('rh-wd')
        self.fence('rh-wd', eid='w-fence')

        withdrawn = rehearsal.run(self.db, {
            'event_id': 'rh-wd-withdraw', 'op': 'withdraw', 'rehearsal': 'rh-wd',
            'at': 40, 'reason': 'customer_revoked_test_permission',
        })
        self.assertEqual(withdrawn['decision'], 'consent_withdrawn')
        self.assertEqual(withdrawn['released'], ['res-wd'])

        late_probe = self.probe('rh-wd', eid='w-late', at=41, sustained=30)
        self.assertEqual(late_probe['decision'], 'consent_withdrawn')
        self.assertEqual(late_probe['recorded'], 'rejected')
        cert = self.certify('rh-wd', cid='cert-wd', at=42)
        self.assertEqual(cert['decision'], 'consent_withdrawn')
        self.assertIsNone(one(self.db, 'SELECT 1 FROM rehearsal_certificates WHERE id=?',
                              ('cert-wd',)))

        case = one(self.db, 'SELECT state,consent FROM rehearsal_cases WHERE id=?', ('rh-wd',))
        self.assertEqual(case['state'], 'withdrawn')
        self.assertEqual(case['consent'], 'withdrawn')
        resource = one(self.db, 'SELECT state,release_reason FROM rehearsal_resources WHERE id=?',
                       ('res-wd',))
        self.assertEqual(resource['state'], 'released')
        self.assertEqual(resource['release_reason'], 'customer_revoked_test_permission')
        self.assert_production_unchanged(before)
        self.assertTrue(replay(self.path)['verified'])


class TimeoutAndSchedulingTests(RehearsalTestBase):
    def test_probe_timeout_leaves_reason_and_settlement(self):
        self.prepare_world(other_standby=False)
        self.offer()
        self.seal('rh-to', timeout_at=60)
        self.consent('rh-to')
        self.book('rh-to', resid='res-to')
        self.start('rh-to')
        self.fence('rh-to', eid='t-fence')
        self.ready('rh-to', eid='t-ready')

        short = self.probe('rh-to', eid='t-probe-short', at=33, span=(23, 28))
        self.assertEqual(short['decision'], 'probe_duration_unmet')
        self.assertEqual(short['sustained_seconds'], 5)

        early = rehearsal.run(self.db, {'event_id': 'to-early', 'op': 'timeout',
                                        'rehearsal': 'rh-to', 'at': 50})
        self.assertEqual(early['decision'], 'not_timed_out')
        timeout = rehearsal.run(self.db, {'event_id': 'to-final', 'op': 'timeout',
                                          'rehearsal': 'rh-to', 'at': 61})
        self.assertEqual(timeout['decision'], 'rehearsal_timed_out')
        self.assertEqual(timeout['released'], ['res-to'])

        self.assertEqual(self.probe('rh-to', eid='t-late', at=62, sustained=30)['decision'],
                         'rehearsal_closed')
        self.assertEqual(self.certify('rh-to', cid='cert-to', at=63)['decision'],
                         'rehearsal_closed')
        again = rehearsal.run(self.db, {'event_id': 'to-settle', 'op': 'settle',
                                        'rehearsal': 'rh-to', 'at': 64})
        self.assertEqual(again['decision'], 'settled')
        self.assertEqual(again['count'], 0)
        self.assertTrue(replay(self.path)['verified'])

    def test_insufficient_shadow_capacity_then_later_seat_certifies_late(self):
        self.prepare_world(other_standby=False)
        self.offer(oid='offer-tiny', cluster='cluster-shadow-tiny', cpu=1, mem=1)
        self.seal('rh-now', deadline=50)
        self.consent('rh-now')
        self.assertEqual(self.book('rh-now', resid='res-now', offer='offer-tiny',
                                   cpu=1, mem=1)['decision'], 'shadow_reserved')

        self.seal('rh-later', deadline=100, timeout_at=None)
        self.consent('rh-later', at=11)
        exhausted = self.book('rh-later', resid='res-later-try', offer='offer-tiny',
                              cpu=1, mem=1, at=12)
        self.assertEqual(exhausted['decision'], 'shadow_capacity_exhausted')

        # 业务负责人预约稍后才可用的独立影子席位。
        self.offer(oid='offer-future', cluster='cluster-shadow-future',
                   cpu=4, mem=8, available_at=200)
        scheduled = self.book('rh-later', resid='res-later', offer='offer-future',
                              cpu=4, mem=8, at=10)
        self.assertEqual(scheduled['decision'], 'shadow_scheduled')
        self.assertEqual(scheduled['state'], 'scheduled')

        early_start = self.start('rh-later', at=15, event='start-early')
        self.assertEqual(early_start['decision'], 'shadow_not_available')
        activated = rehearsal.run(self.db, {
            'event_id': 'later-activate', 'op': 'activate_shadow',
            'resource_id': 'res-later', 'at': 200,
        })
        self.assertEqual(activated['decision'], 'shadow_activated')
        self.assertEqual(self.start('rh-later', at=201)['decision'], 'started')

        self.fence('rh-later', eid='l-fence', at=202)
        self.ready('rh-later', eid='l-ready', at=203, cluster='cluster-shadow-future')
        self.probe('rh-later', eid='l-probe', at=233, span=(204, 234))
        certified = self.certify('rh-later', cid='cert-later', at=240)
        self.assertEqual(certified['decision'], 'certified')
        self.assertEqual(certified['level'], 'shadow_end_to_end')
        self.assertFalse(certified['meets_deadline'])
        self.assertEqual(certified['conclusion'], 'filable_late')
        # 强原件仍可出具逾期提交结论，但不能冒充“当天提交”。
        self.assertTrue(certified['filable'])
        self.assertEqual(certified['regulatory_action'],
                         'file_late_with_deadline_exception')
        self.assertTrue(replay(self.path)['verified'])


class GuardAndSealTests(RehearsalTestBase):
    def test_real_incident_cannot_be_relabeled_as_rehearsal(self):
        self.prepare_world(other_standby=True)
        evac.run(self.db, {'event_id': 'real-inc', 'op': 'incident', 'id': 'real-inc',
                           'cluster': 'cluster-a', 'severity': 4, 'generation': 1})
        result = self.seal('rh-impossible')
        self.assertEqual(result['decision'], 'real_incident_conflict')
        self.assertEqual(result['incident'], 'real-inc')
        self.assertIsNone(one(self.db, 'SELECT 1 FROM rehearsal_cases WHERE id=?', ('rh-impossible',)))

    def test_production_seat_cannot_be_registered_as_shadow(self):
        self.prepare_world(other_standby=True)
        # C0 主席与 C1 真实备席（服务另一事故待命）都必须被影子池拒绝。
        for cluster in ('cluster-a', 'cluster-b'):
            result = rehearsal.run(self.db, {
                'event_id': f'offer-{cluster}', 'op': 'offer_shadow',
                'id': f'offer-{cluster}', 'cluster': cluster,
                'cpu': 1, 'mem': 1, 'generation': 1, 'available_at': 0,
            })
            self.assertEqual(result['decision'], 'production_seat_not_shadow')
        self.assertIsNone(one(self.db, 'SELECT 1 FROM rehearsal_shadow_offers WHERE cluster=?',
                              ('cluster-b',)))

    def test_seal_preserves_generations_capacity_and_other_standby(self):
        self.prepare_world(other_standby=True)
        result = self.seal('rh-seal-check')
        self.assertEqual(result['decision'], 'sealed')
        # intent-normal 冻结到 revision 3，assign_primary 再 +1 -> 4。
        self.assertEqual(result['contract_version'], 4)
        self.assertEqual(len(result['snapshot_hash']), 64)

        case = one(self.db, 'SELECT sealed_snapshot,snapshot_hash FROM rehearsal_cases WHERE id=?',
                   ('rh-seal-check',))
        snapshot = json.loads(case['sealed_snapshot'])
        self.assertEqual(snapshot['contract_version'], 4)
        self.assertEqual(snapshot['primary_seat']['cluster_generation'], 1)
        self.assertEqual(snapshot['primary_seat']['lease_generation'], 1)
        self.assertEqual(snapshot['backup_seat']['cluster_generation'], 1)
        other_ids = [row['id'] for row in snapshot['backup_seat']['other_incident_standbys']]
        self.assertIn('lease-b-other', other_ids)
        # 封存的容量观测包含 C1 上两项占用。
        cluster_b_used = snapshot['capacity_observation']['used_by_cluster']['cluster-b']
        self.assertGreaterEqual(cluster_b_used['cpu'], 6)
        self.assertIn('lease-b', cluster_b_used['claims'])
        self.assertIn('lease-b-other', cluster_b_used['claims'])

    def test_rehearsal_only_writes_scope_bound_isolated_sequences(self):
        self.prepare_world(other_standby=False)
        before = self.snapshot_production()
        self.offer()
        self.seal('rh-seq')
        self.consent('rh-seq')
        self.book('rh-seq', resid='res-seq')
        self.start('rh-seq')

        issued = rehearsal.run(self.db, {
            'event_id': 'seq-write', 'op': 'issue_sequences', 'rehearsal': 'rh-seq',
            'at': 14, 'count': 2,
        })
        self.assertEqual(issued['decision'], 'isolated_sequences_issued')
        self.assertFalse(issued['writes_real_customer_sequence'])
        rows = many(self.db, 'SELECT * FROM rehearsal_isolated_sequences WHERE rehearsal=?',
                    ('rh-seq',))
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(r['scope'] == 'scope-rh-seq-g1' for r in rows))
        self.assertTrue(all('scope-rh-seq-g1' in r['label'] for r in rows))

        forbidden = rehearsal.run(self.db, {
            'event_id': 'real-write-attempt', 'op': 'write_customer_sequence',
            'rehearsal': 'rh-seq', 'sequence': 'CUST-REAL-1',
        })
        self.assertEqual(forbidden['decision'], 'real_sequence_write_forbidden')
        self.assert_production_unchanged(before)

    def test_event_id_replay_and_identity_conflict(self):
        self.prepare_world(other_standby=False)
        command = {'event_id': 'dedup-seal', 'op': 'seal', 'id': 'rh-dedup',
                   'intent': 'work-a', 'source': 'cluster-a', 'at': 10}
        first = rehearsal.run(self.db, command)
        second = rehearsal.run(self.db, command)
        self.assertEqual(first, second)
        conflict = rehearsal.run(self.db, {**command, 'probe_seconds': 99})
        self.assertEqual(conflict['decision'], 'event_identity_conflict')
        self.assertTrue(replay(self.path)['verified'])


if __name__ == '__main__':
    unittest.main()
