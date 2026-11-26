"""可追责密钥释放与恢复服务的验收测试（客户 W / C0 / C1）。

逐项对照链：席位（seat）→ 负载归属（intent）→ 保管人身份与资格 →
客户本次许可 → 事故状态 → 密钥代次 → 设备/测量证明 → 密文代次与客户可读
序号。同时覆盖：

* 同一人重复投票 / 改名凑数；
* 保管人资格撤销后的旧票；
* 许可撤回、撤票与释放提交的竞态顺序；
* 设备重建代次换代、旧证明与迟到/补签证明；
* 密钥轮换使旧代批准失效；
* 部分重加密；
* 事故取消；
* 明文密钥字节不得进入事件 / fixture / 审计导出；
* 审计者逐事件重放复算释放依据，以及全库 replay。
"""

import json
import tempfile
import unittest
from itertools import count
from pathlib import Path

from cloudcourt import MODULES, custody, evac, intent, seat
from cloudcourt.store import connect, one, replay

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / 'fixtures'
GOLD_MEASUREMENT = 'sha256:measurement-c1-generation-2-golden'
CANARY_DEK = 'PLAINTEXT-DEK-CANARY-w-0123456789abcdefSECRET'


# ---------------------------------------------------------------------------
# 流式场景构造器：所有命令都带唯一 event_id，逻辑时间用 at 表达。
# ---------------------------------------------------------------------------

class World:
    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'w.sqlite')
        self.db = connect(self.path)
        self._ids = count()

    def close(self):
        self.db.close()
        self.tmp.cleanup()

    def go(self, module, event_id=None, **payload):
        payload['event_id'] = event_id or f"ev-{payload['op']}-{next(self._ids)}"
        return module.run(self.db, payload)

    def it(self, **p):
        return self.go(intent, **p)

    def st(self, **p):
        return self.go(seat, **p)

    def ev(self, **p):
        return self.go(evac, **p)

    def cu(self, event_id=None, **p):
        return self.go(custody, event_id=event_id, **p)

    # -- 真实主席位：intent 合同 + C0/C1 席位 + evac 围栏交接 --------------
    def chair(self):
        self.it(op='rule', id='rule-w', tenant='w', regions=['west'], revision=1)
        self.it(op='claim', id='work-w', tenant='w', region='west', role='critical')
        self.it(op='grant', id='grant-w', intent='work-w', actor='owner-w',
                role='promote', generation=1, expires=200)
        self.st(op='observe', id='cluster-c0', region='west', domain='rack-c0',
                cpu=8, mem=16, generation=1, healthy=True)
        self.st(op='observe', id='cluster-c1', region='west', domain='rack-c1',
                cpu=8, mem=16, generation=1, healthy=True)
        self.st(op='reserve', id='lease-c0', intent='work-w', cluster='cluster-c0',
                role='primary', cpu=2, mem=4, expires=200)
        self.st(op='occupy', id='lease-c0', at=1)
        self.st(op='reserve', id='lease-c1', intent='work-w', cluster='cluster-c1',
                role='backup', cpu=2, mem=4, expires=200)
        self.st(op='occupy', id='lease-c1', at=1)
        self.it(op='assign_primary', id='assign-w', intent='work-w',
                cluster='cluster-c0', grant='grant-w', at=2)
        self.ev(op='incident', id='inc-w', cluster='cluster-c0',
                severity=4, generation=1)
        self.ev(op='plan', id='move-w', intent='work-w', source='cluster-c0',
                target='cluster-c1', incident='inc-w')
        self.ev(op='receipt', id='fence-w', plan='move-w',
                kind='source_fenced', cluster='cluster-c0', generation=1)
        self.ev(op='receipt', id='ready-w', plan='move-w',
                kind='target_ready', cluster='cluster-c1', generation=1)
        result = self.ev(op='handoff', id='handoff-w', plan='move-w')
        assert result['decision'] == 'transferred', result
        return result

    def custodians(self):
        self.cu(op='custodian', id='c-jia', tenant='w', canonical='PII-JIA-0001',
                label='custodian-jia', aliases=['jia', 'jia-old-name'], at=2)
        self.cu(op='custodian', id='c-yi', tenant='w', canonical='PII-YI-0002',
                label='custodian-yi', aliases=['yi', 'yi-former-name'], at=2)
        self.cu(op='custodian', id='c-bing', tenant='w', canonical='PII-BING-0003',
                label='custodian-bing', aliases=['bing'], at=20)

    def consent(self, cid='consent-w', **over):
        payload = dict(op='consent', id=cid, tenant='w', intent='work-w',
                       incident='inc-w', scope='incident_recovery',
                       modes=['read', 'read_write', 'rotate'],
                       terms={'package_ttl': 60, 'attestation_ttl': 30}, at=6)
        payload.update(over)
        return self.cu(**payload)

    def device(self, generation=2, measurement=GOLD_MEASUREMENT):
        self.cu(op='device', id='dev-c1', cluster='cluster-c1', generation=1,
                measurement='sha256:measurement-c1-generation-1', at=4)
        return self.cu(op='device', id='dev-c1', cluster='cluster-c1',
                       generation=generation, measurement=measurement, at=10)

    def old_world(self, prefix=8, tail_seqs=(9, 10), segments=10):
        self.cu(op='key', id='key-old', tenant='w', intent='work-w', epoch=1, at=2)
        self.cu(op='segments', id='seg-old', key='key-old',
                seqs=list(range(1, segments + 1)), cluster='cluster-c1')
        if prefix is not None:
            self.cu(op='prefix', id='pref-old', key='key-old',
                    through_seq=prefix, at=24)
        for seq in tail_seqs:
            self.cu(op='tail_write', id=f'tail-{seq}', intent='work-w',
                    seq=seq, at=24)

    def normal_quorum(self, key='key-old'):
        self.cu(op='vote', id='v-yi', key=key, actor='yi', at=21)
        self.cu(op='vote', id='v-bing', key=key, actor='bing-renamed', at=22)
        # bing 在会签前刚改名，别名必须归一到 c-bing。
        self.cu(op='custodian_alias', id='alias-bing',
                custodian='c-bing', alias='bing-renamed', at=20)

    def jia_revoked_with_old_vote(self):
        self.cu(op='vote', id='v-jia-old', key='key-old', actor='jia-old-name', at=3)
        self.cu(op='revoke_custodian', id='rev-jia', custodian='c-jia',
                at=5, reason='qualification_lapsed')

    def case_and_attest(self, mode='read', nonce='nonce-read', at_case=24,
                        at_att=25, case_id='case-read', ttl=30):
        self.cu(op='release_case', id=case_id, key='key-old',
                cluster='cluster-c1', consent='consent-w', incident='inc-w',
                device='dev-c1', mode=mode, nonce=nonce, at=at_case,
                attestation_ttl=ttl, package_ttl=60)
        self.cu(op='attest', id=f'att-{case_id}', device='dev-c1', generation=2,
                measurement=GOLD_MEASUREMENT, nonce=nonce, at=at_att)

    def ready(self, mode='read'):
        """构造一个所有裁决项都通过的 W 世界（甲已撤销，乙+丙合格会签）。"""
        self.chair()
        self.custodians()
        self.consent()
        self.device()
        self.old_world()
        self.jia_revoked_with_old_vote()
        self.normal_quorum()
        if mode == 'read':
            self.case_and_attest(mode='read', nonce='nonce-read',
                                 case_id='case-read')
        else:
            for seq in (9, 10):
                self.cu(op='reconcile_tail', id=f'rec-{seq}',
                        tail=f'tail-{seq}', at=27)
            self.case_and_attest(mode='read_write', nonce='nonce-write',
                                 case_id='case-write', at_case=28, at_att=29)

    def release(self, event_id='rel', **over):
        payload = dict(op='release', id='release-business-id', key='key-old',
                       case='case-read', cluster='cluster-c1', mode='read', at=26)
        payload.update(over)
        return self.cu(event_id=event_id, **payload)

    def assert_replay_verified(self):
        result = replay(self.path)
        if not result['verified']:
            raise AssertionError(f"replay mismatch: {result.get('mismatch')}")
        return result


def check_codes(result):
    return {c['root']: c['code'] for c in result['basis']['checks']}


# ---------------------------------------------------------------------------
# 端到端：两份 fixture（只读限时路径 / 轮换重加密路径）逐项对照
# ---------------------------------------------------------------------------

def run_envelope_fixture(db, name):
    commands = json.loads((FIXTURES / name).read_text(encoding='utf-8'))
    results = {}
    for command in commands:
        subsystem = command['subsystem']
        payload = command['payload']
        results[payload['event_id']] = MODULES[subsystem].run(db, payload)
    return results


class WFixtureAcceptance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'fixture.sqlite')
        self.db = connect(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_read_only_path_item_by_item(self):
        results = run_envelope_fixture(self.db, 'key-recovery-read.json')

        # --- 席位：C1 真实占席且围栏交接后成为主席位 ---------------------
        intent_row = one(self.db, "SELECT * FROM workload_intents WHERE id='work-w'")
        self.assertEqual(intent_row['tenant'], 'w')
        self.assertEqual(intent_row['primary_cluster'], 'cluster-c1')
        lease = one(self.db,
                    "SELECT * FROM placement_leases WHERE intent='work-w' "
                    "AND cluster='cluster-c1' AND state='occupied'")
        self.assertIsNotNone(lease)
        self.assertEqual(lease['role'], 'primary')
        inventory = one(self.db, "SELECT * FROM cluster_inventory WHERE id='cluster-c1'")
        self.assertEqual(inventory['generation'], lease['generation'])
        self.assertTrue(inventory['healthy'])

        # --- 保管人：甲撤销 + 旧票，乙、丙独立合格，改名可归一 ----------
        self.assertEqual(one(self.db, "SELECT state FROM custodians WHERE id='c-jia'")['state'],
                         'revoked')
        self.assertEqual(one(self.db, "SELECT state FROM custodians WHERE id='c-yi'")['state'],
                         'qualified')
        self.assertEqual(one(self.db, "SELECT state FROM custodians WHERE id='c-bing'")['state'],
                         'qualified')
        alias = one(self.db,
                    "SELECT custodian_id FROM custodian_aliases WHERE alias='bing-renamed'")
        self.assertEqual(alias['custodian_id'], 'c-bing')

        first = results['w-rel-read']
        self.assertEqual(first['decision'], 'key_released', first)
        basis = first['basis']
        codes = check_codes(first)
        for root in ('case', 'intent', 'seat', 'incident', 'consent',
                     'custodians', 'key_epoch', 'device', 'attestation',
                     'read_boundary'):
            self.assertTrue(any(c['root'] == root and c['ok']
                                for c in basis['checks']),
                            f'{root} not ok: {codes}')
        quorum = next(c['detail'] for c in basis['checks']
                      if c['root'] == 'custodians')
        self.assertEqual(quorum['independent_qualified'], 2)
        self.assertEqual(set(quorum['independent_ids']), {'c-yi', 'c-bing'})
        jia_vote = next(v for v in quorum['votes'] if v['actor'] == 'jia-old-name')
        self.assertFalse(jia_vote['counts'])
        self.assertEqual(jia_vote['reason'], 'qualification_revoked')

        # --- 客户许可本次用途 + 限时只读密钥包 ---------------------------
        consent_check = next(c for c in basis['checks'] if c['root'] == 'consent')
        self.assertTrue(consent_check['ok'])
        package = basis['package']
        self.assertFalse(package['plaintext_key_included'])
        self.assertTrue(package['ref'].startswith('sealed-package://'))
        self.assertEqual(package['mode'], 'read')
        self.assertEqual(package['expires_at'] - package['issued_at'], 60)

        # --- 密文代次 -> 客户可读序号：前缀 8，尾部 9/10 未对账 ---------
        prefix = one(self.db,
                     "SELECT through_seq FROM readable_prefixes WHERE id='pref-w-old'")
        self.assertEqual(prefix['through_seq'], 8)
        self.assertEqual(basis['read_boundary']['prefix_through'], 8)
        self.assertEqual(basis['read_boundary']['readable_seq'], 8)
        self.assertEqual(basis['read_boundary']['pending_tail_seqs'], [9, 10])
        self.assertFalse(basis['read_boundary']['write_enabled'])

        # 目标已占主席位，但 seat_boot_only 提醒：启动 ≠ 历史可读。
        self.assertTrue(basis['read_boundary']['seat_boot_only'])
        self.assertEqual(intent_row['primary_cluster'], 'cluster-c1')
        self.assertEqual(basis['read_boundary']['readable_seq'], 8)

        # --- 尾部逐笔对账后才开放写入 ------------------------------------
        second = results['w-rel-write']
        self.assertEqual(second['decision'], 'key_released', second)
        boundary = second['basis']['read_boundary']
        self.assertEqual(boundary['readable_seq'], 10)
        self.assertTrue(boundary['write_ready'])
        self.assertEqual(second['basis']['mode'], 'read_write')
        for seq in (9, 10):
            tail = one(self.db, 'SELECT state FROM tail_writes WHERE seq=?', (seq,))
            self.assertEqual(tail['state'], 'confirmed')

        # --- 审计逐次复算 + 全库 replay ----------------------------------
        for event_id in ('w-rel-read', 'w-rel-write'):
            audit = custody.audit_release(self.db, event_id)
            self.assertTrue(audit['matches'],
                            (event_id, audit['stored_decision'],
                             audit['recomputed_decision']))
        self.assertTrue(custody.export_audit(self.db)['all_match'])
        replay_result = replay(self.path)
        self.assertTrue(replay_result['verified'], replay_result.get('mismatch'))

    def test_rotation_path_partial_reencryption_then_full_rw(self):
        results = run_envelope_fixture(self.db, 'key-recovery-rotate.json')

        # 尾部未对账时，旧代 RW 不放行。
        self.assertEqual(results['w-rel-old-rw']['decision'], 'tail_not_reconciled')
        # 轮换后旧代批准失效。
        self.assertEqual(results['w-rel-old-after-rotate']['decision'],
                         'key_epoch_superseded')
        old = one(self.db, "SELECT state FROM key_epochs WHERE id='key-w-old'")
        new = one(self.db, "SELECT epoch,state FROM key_epochs WHERE id='key-w-e2'")
        self.assertEqual(old['state'], 'rotated')
        self.assertEqual((new['epoch'], new['state']), (2, 'sealed'))

        # 部分重加密：6/10，明确缺口序号。
        partial = results['w-rel-new-partial']
        self.assertEqual(partial['decision'], 'reencryption_incomplete', partial)
        boundary = partial['basis']['read_boundary']
        reenc = boundary['reencryption']
        self.assertEqual((reenc['done'], reenc['total']), (6, 10))
        self.assertEqual(reenc['pending_seqs'], [7, 8, 9, 10])
        self.assertFalse(reenc['complete'])
        # 部分进度作为当时的裁决依据已独立落审计快照。
        partial_audit = one(self.db,
                            'SELECT basis FROM release_audits WHERE event_id=?',
                            ('w-rel-new-partial',))
        snapshot = json.loads(partial_audit['basis'])
        self.assertEqual(
            snapshot['read_boundary']['reencryption']['pending_seqs'],
            [7, 8, 9, 10])

        # 完成 + 新代前缀 10 后一次开放读写。
        final = results['w-rel-new-rw']
        self.assertEqual(final['decision'], 'key_released', final)
        final_boundary = final['basis']['read_boundary']
        self.assertEqual(final_boundary['readable_seq'], 10)
        self.assertTrue(final_boundary['write_ready'])
        self.assertEqual(final['basis']['mode'], 'read_write')
        self.assertTrue(final['basis']['package']['ref']
                        .startswith('sealed-package://'))

        for event_id in ('w-rel-old-rw', 'w-rel-old-after-rotate',
                         'w-rel-new-partial', 'w-rel-new-rw'):
            audit = custody.audit_release(self.db, event_id)
            self.assertTrue(audit['matches'], event_id)
        replay_result = replay(self.path)
        self.assertTrue(replay_result['verified'], replay_result.get('mismatch'))

    def test_races_fixture_withdrawal_and_consent_revocation(self):
        results = run_envelope_fixture(self.db, 'key-recovery-races.json')
        self.assertEqual(results['w-handoff']['decision'], 'transferred')
        self.assertEqual(results['w-rel-withdraw']['decision'],
                         'custodian_quorum_not_met')
        self.assertEqual(results['w-rel-revoked']['decision'], 'consent_revoked')
        for event_id in ('w-rel-withdraw', 'w-rel-revoked'):
            self.assertTrue(custody.audit_release(self.db, event_id)['matches'])
        replay_result = replay(self.path)
        self.assertTrue(replay_result['verified'], replay_result.get('mismatch'))

    def test_two_paths_share_one_incident_and_consent_original(self):
        def payloads(name):
            commands = json.loads((FIXTURES / name).read_text(encoding='utf-8'))
            return {p['event_id']: p for s, p in
                    ((c['subsystem'], c['payload']) for c in commands)}

        read = payloads('key-recovery-read.json')
        rotate = payloads('key-recovery-rotate.json')
        # 同一事故原件。
        self.assertEqual(read['w-inc'], rotate['w-inc'])
        # 同一客户许可原件，且明确同时允许两条执行路径。
        self.assertEqual(read['w-consent'], rotate['w-consent'])
        self.assertEqual(read['w-consent']['incident'], 'inc-w')
        self.assertEqual(
            set(read['w-consent']['modes']), {'read', 'read_write', 'rotate'})
        self.assertTrue(read['w-consent']['terms']['dual_path_authorization'])

    def test_fixtures_include_revocation_and_rotation(self):
        read_text = (FIXTURES / 'key-recovery-read.json').read_text(encoding='utf-8')
        rotate_text = (FIXTURES / 'key-recovery-rotate.json').read_text(encoding='utf-8')
        self.assertIn('revoke_custodian', read_text)
        self.assertIn('revoke_custodian', rotate_text)
        self.assertIn('rotate', rotate_text)
        # 许可撤销也必须在 fixture 中出现（含撤销竞态反例文件）。
        race_text = (FIXTURES / 'key-recovery-races.json').read_text(encoding='utf-8')
        self.assertIn('revoke_consent', race_text)
        self.assertIn('withdraw_vote', race_text)


# ---------------------------------------------------------------------------
# 身份 / 资格 / 法定人数
# ---------------------------------------------------------------------------

class CustodianQuorumTests(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.w.chair()
        self.w.custodians()
        self.w.consent()
        self.w.device()
        self.w.old_world()

    def tearDown(self):
        self.w.assert_replay_verified()
        self.w.close()

    def test_same_actor_double_vote_counts_once(self):
        self.w.cu(op='vote', id='dup-1', key='key-old', actor='yi', at=21)
        self.w.cu(op='vote', id='dup-2', key='key-old', actor='yi', at=22)
        self.w.case_and_attest()
        result = self.w.release()
        self.assertEqual(result['decision'], 'custodian_quorum_not_met')
        detail = next(c['detail'] for c in result['basis']['checks']
                       if c['root'] == 'custodians')
        self.assertEqual(detail['independent_qualified'], 1)

    def test_renamed_same_person_does_not_make_quorum(self):
        # yi 与 yi-former-name 在名册里是同一 canonical 身份。
        self.w.cu(op='vote', id='rn-1', key='key-old', actor='yi', at=21)
        self.w.cu(op='vote', id='rn-2', key='key-old',
                  actor='yi-former-name', at=22)
        self.w.case_and_attest()
        result = self.w.release()
        self.assertEqual(result['decision'], 'custodian_quorum_not_met')
        actors = {v['actor']: v['counts']
                  for v in next(c['detail']['votes'] for c in result['basis']['checks']
                                if c['root'] == 'custodians')}
        self.assertEqual(len(actors), 2)  # 两行票都审计可见
        detail = next(c['detail'] for c in result['basis']['checks']
                      if c['root'] == 'custodians')
        self.assertEqual(detail['independent_ids'], ['c-yi'])

    def test_revoked_custodians_old_votes_are_excluded_until_new_custodian(self):
        self.w.jia_revoked_with_old_vote()
        self.w.cu(op='vote', id='only-yi', key='key-old', actor='yi', at=21)
        self.w.case_and_attest()
        denied = self.w.release(event_id='rel-jia-yi')
        self.assertEqual(denied['decision'], 'custodian_quorum_not_met')
        votes = next(c['detail']['votes'] for c in denied['basis']['checks']
                     if c['root'] == 'custodians')
        jia = next(v for v in votes if v['custodian_id'] == 'c-jia')
        self.assertEqual(jia['reason'], 'qualification_revoked')

        # 客户事故授权允许新增合格保管人丙重新会签。
        self.w.cu(op='vote', id='v-bing-new', key='key-old',
                  actor='bing-renamed', at=22)
        self.w.cu(op='custodian_alias', id='alias-bing-2',
                  custodian='c-bing', alias='bing-renamed')
        ok = self.w.release(event_id='rel-yi-bing')
        self.assertEqual(ok['decision'], 'key_released', ok)
        self.assertEqual(
            set(next(c['detail']['independent_ids'] for c in ok['basis']['checks']
                     if c['root'] == 'custodians')),
            {'c-yi', 'c-bing'})

    def test_unregistered_actor_does_not_count(self):
        self.w.cu(op='vote', id='x-1', key='key-old', actor='stranger-a', at=21)
        self.w.cu(op='vote', id='x-2', key='key-old', actor='stranger-b', at=22)
        self.w.case_and_attest()
        result = self.w.release()
        self.assertEqual(result['decision'], 'custodian_quorum_not_met')
        reasons = [v['reason'] for c in result['basis']['checks']
                   if c['root'] == 'custodians' for v in c['detail']['votes']]
        self.assertTrue(all(r == 'unknown_identity' for r in reasons))


# ---------------------------------------------------------------------------
# 撤回竞态：事件顺序决定胜负；先提交的释放不被后来的撤回改写。
# ---------------------------------------------------------------------------

class WithdrawalAndRevocationRace(unittest.TestCase):
    def test_vote_withdrawal_before_submission_blocks(self):
        w = World()
        w.ready(mode='read')
        first = w.cu(op='withdraw_vote', id='wd-bing', vote='v-bing')
        self.assertEqual(first['decision'], 'vote_withdrawn')
        result = w.release(event_id='rel-after-withdraw')
        self.assertEqual(result['decision'], 'custodian_quorum_not_met')
        w.assert_replay_verified()
        w.close()

    def test_release_first_then_withdrawal_stands_and_audits(self):
        w = World()
        w.ready(mode='read')
        released = w.release(event_id='rel-race-ok')
        self.assertEqual(released['decision'], 'key_released')
        w.cu(op='withdraw_vote', id='wd-after', vote='v-bing')
        row = one(w.db, "SELECT state,mode FROM key_releases WHERE id='release-business-id'")
        self.assertEqual(row['state'], 'released')
        audit = custody.audit_release(w.db, 'rel-race-ok')
        self.assertTrue(audit['matches'])
        w.assert_replay_verified()
        w.close()

    def test_consent_revocation_race(self):
        # 撤回先于释放提交 -> 旧许可失效。
        before = World()
        before.ready(mode='read')
        before.cu(op='revoke_consent', id='rev-consent',
                  consent='consent-w', at=25)
        denied = before.release(event_id='rel-revoked')
        self.assertEqual(denied['decision'], 'consent_revoked')
        before.assert_replay_verified()
        before.close()

        # 释放先提交 -> 既成释放记录保持可审计，后来的撤回不改写它。
        after = World()
        after.ready(mode='read')
        ok = after.release(event_id='rel-before-revoke')
        self.assertEqual(ok['decision'], 'key_released')
        after.cu(op='revoke_consent', id='rev-consent-late',
                 consent='consent-w', at=40)
        audit = custody.audit_release(after.db, 'rel-before-revoke')
        self.assertTrue(audit['matches'])
        after.assert_replay_verified()
        after.close()


# ---------------------------------------------------------------------------
# 设备换代 / 即时证明 / 迟到与补签
# ---------------------------------------------------------------------------

class DeviceEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.w.chair()
        self.w.custodians()
        self.w.consent()
        self.w.old_world()
        self.w.normal_quorum()

    def tearDown(self):
        self.w.assert_replay_verified()
        self.w.close()

    def test_device_rebuild_invalidates_pinned_case_and_old_attestation(self):
        # 先用 gen1 设备 + gen1 证明建立释放案。
        self.w.cu(op='device', id='dev-c1', cluster='cluster-c1', generation=1,
                  measurement='sha256:gen1', at=4)
        self.w.cu(op='release_case', id='case-old-dev', key='key-old',
                  cluster='cluster-c1', consent='consent-w', incident='inc-w',
                  device='dev-c1', mode='read', nonce='nonce-old-dev', at=20,
                  attestation_ttl=30)
        self.w.cu(op='attest', id='att-old-dev', device='dev-c1', generation=1,
                  measurement='sha256:gen1', nonce='nonce-old-dev', at=21)
        # 设备重建为 gen2。
        rebuilt = self.w.cu(op='rebuild_device', id='rebuild',
                            device='dev-c1', generation=2,
                            measurement=GOLD_MEASUREMENT, at=22)
        self.assertEqual(rebuilt['decision'], 'device_rebuilt')

        result = self.w.release(event_id='rel-rebuilt', case='case-old-dev',
                                nonce='nonce-old-dev', at=26)
        codes = check_codes(result)
        self.assertEqual(result['decision'], 'device_rebuilt', result)
        self.assertEqual(codes['attestation'], 'stale_attestation_generation')

    def test_late_attestation_cannot_backsign_earlier_release(self):
        self.w.device()
        self.w.cu(op='release_case', id='case-late', key='key-old',
                  cluster='cluster-c1', consent='consent-w', incident='inc-w',
                  device='dev-c1', mode='read', nonce='nonce-late', at=24,
                  attestation_ttl=30)

        # 第一次释放：没有证明，被拒绝且裁决已固化。
        early = self.w.release(event_id='rel-late', case='case-late',
                               nonce='nonce-late', at=26)
        self.assertEqual(early['decision'], 'attestation_missing')

        # 证明“迟到”，并把业务时间倒签到 challenge 之前。
        late = self.w.cu(op='attest', id='att-late', device='dev-c1', generation=2,
                         measurement=GOLD_MEASUREMENT, nonce='nonce-late', at=1)
        self.assertEqual(late['decision'], 'attestation_verified')

        # 同一 event_id 重放同一请求：act 返回已固化的拒绝，不得补签。
        retried = self.w.release(event_id='rel-late', case='case-late',
                                 nonce='nonce-late', at=26)
        self.assertEqual(retried['decision'], 'attestation_missing')

        # 新的释放提交仍被倒签时间戳识破。
        backdated = self.w.release(event_id='rel-late-2', id='release-business-id-2',
                                   case='case-late', nonce='nonce-late', at=26)
        self.assertEqual(backdated['decision'], 'attestation_backdated')

        # 合规的新即时证明可以支持一次新的提交（不是补签旧提交）。
        self.w.cu(op='attest', id='att-fresh', device='dev-c1', generation=2,
                  measurement=GOLD_MEASUREMENT, nonce='nonce-late', at=27)
        ok = self.w.release(event_id='rel-late-3', id='release-business-id-3',
                            case='case-late', nonce='nonce-late', at=28)
        self.assertEqual(ok['decision'], 'key_released', ok)

    def test_pre_challenge_event_ordering_is_rejected(self):
        self.w.device()
        # 证明事件先于 challenge 释放案（即使 nonce 预先按确定性规则构造）。
        self.w.cu(op='attest', id='att-before', device='dev-c1', generation=2,
                  measurement=GOLD_MEASUREMENT, nonce='challenge:case-order', at=25)
        self.w.cu(op='release_case', id='case-order', key='key-old',
                  cluster='cluster-c1', consent='consent-w', incident='inc-w',
                  device='dev-c1', mode='read', at=24, attestation_ttl=30)
        result = self.w.release(event_id='rel-order', case='case-order', at=26)
        self.assertEqual(result['decision'], 'attestation_before_challenge')

    def test_stale_attestation_outside_freshness_window(self):
        self.w.device()
        self.w.case_and_attest(nonce='nonce-stale', case_id='case-stale')
        result = self.w.release(event_id='rel-stale', case='case-stale',
                                nonce='nonce-stale', at=200)
        self.assertEqual(result['decision'], 'attestation_stale')

    def test_wrong_measurement_and_missing_attestation_yield_no_package(self):
        self.w.device()
        self.w.cu(op='release_case', id='case-bad-meas', key='key-old',
                  cluster='cluster-c1', consent='consent-w', incident='inc-w',
                  device='dev-c1', mode='read', nonce='n-bad', at=24)
        bad = self.w.cu(op='attest', id='att-bad', device='dev-c1', generation=2,
                        measurement='sha256:unexpected-measurement',
                        nonce='n-bad', at=25)
        self.assertEqual(bad['decision'], 'measurement_mismatch')

        # 显式引用 rejected 证明 -> 不通过；且没有任何密钥包/释放行落库。
        result = self.w.release(event_id='rel-bad-meas', id='biz-bad-meas',
                                case='case-bad-meas', attestation='att-bad',
                                nonce='n-bad', at=26)
        self.assertEqual(result['decision'], 'attestation_not_verified')
        self.assertIsNone(one(self.w.db,
                              "SELECT 1 FROM key_releases WHERE id='biz-bad-meas'"))

        # 完全没有证明的新案同样不得交付密钥包。
        self.w.cu(op='release_case', id='case-no-att', key='key-old',
                  cluster='cluster-c1', consent='consent-w', incident='inc-w',
                  device='dev-c1', mode='read', nonce='n-none', at=24)
        missing = self.w.release(event_id='rel-no-att', id='biz-no-att',
                                 case='case-no-att', nonce='n-none', at=26)
        self.assertEqual(missing['decision'], 'attestation_missing')
        self.assertIsNone(one(self.w.db,
                              "SELECT package_ref FROM key_releases "
                              "WHERE id='biz-no-att'"))


# ---------------------------------------------------------------------------
# 密钥轮换 / 部分重加密
# ---------------------------------------------------------------------------

class KeyEpochAndReencryption(unittest.TestCase):
    def test_rotation_invalidates_old_votes_and_requires_new_quorum(self):
        w = World()
        w.ready(mode='read_write')  # 尾部已对账、旧代 RW 本来可放行
        rotated = w.cu(op='rotate', id='rot', key='key-old',
                       new_key='key-new', epoch=2, at=31)
        self.assertEqual(rotated['decision'], 'key_rotated')

        old = w.release(event_id='rel-old-after-rot', case='case-write',
                        at=32)
        self.assertEqual(old['decision'], 'key_epoch_superseded')

        # 旧票投的是 key-old；新代 key-new 没有法定人数。
        w.cu(op='release_case', id='case-new', key='key-new',
             cluster='cluster-c1', consent='consent-w', incident='inc-w',
             device='dev-c1', mode='read_write', nonce='nonce-new', at=32)
        w.cu(op='attest', id='att-new', device='dev-c1', generation=2,
             measurement=GOLD_MEASUREMENT, nonce='nonce-new', at=33)
        no_quorum = w.release(event_id='rel-new-noquorum',
                              id='release-business-new', key='key-new',
                              case='case-new', cluster='cluster-c1',
                              mode='read_write', at=34)
        self.assertEqual(no_quorum['decision'], 'custodian_quorum_not_met')
        w.assert_replay_verified()
        w.close()

    def test_partial_reencryption_boundary_and_completion(self):
        w = World()
        w.chair()
        w.custodians()
        w.normal_quorum()
        w.consent()
        w.device()
        w.old_world()
        for seq in (9, 10):
            w.cu(op='reconcile_tail', id=f'rec-{seq}', tail=f'tail-{seq}')
        w.cu(op='rotate', id='rot2', key='key-old', new_key='key-e2',
             epoch=2, at=30)
        w.cu(op='reencrypt_start', id='job-1', old_key='key-old',
             new_key='key-e2', seqs=list(range(1, 11)))
        partial = w.cu(op='reencrypt_batch', id='batch-1', job='job-1',
                       seqs=list(range(1, 7)), at=31)
        self.assertEqual((partial['done'], partial['total'], partial['complete']),
                         (6, 10, False))
        boundary = custody.read_boundary(w.db, 'work-w', 'key-e2')
        self.assertEqual(boundary['reencryption']['pending_seqs'], [7, 8, 9, 10])
        self.assertFalse(boundary['write_ready'])

        # 新代会签 + 证明 + 案
        w.cu(op='vote', id='v-yi-2', key='key-e2', actor='yi', at=32)
        w.cu(op='vote', id='v-bing-2', key='key-e2', actor='bing-renamed', at=32)
        w.cu(op='release_case', id='case-e2', key='key-e2',
             cluster='cluster-c1', consent='consent-w', incident='inc-w',
             device='dev-c1', mode='read_write', nonce='n-e2', at=32)
        w.cu(op='attest', id='att-e2', device='dev-c1', generation=2,
             measurement=GOLD_MEASUREMENT, nonce='n-e2', at=33)
        denied = w.release(event_id='rel-partial', id='rel-e2-1', key='key-e2',
                           case='case-e2', mode='read_write', at=34)
        self.assertEqual(denied['decision'], 'reencryption_incomplete')

        w.cu(op='reencrypt_batch', id='batch-2', job='job-1',
             seqs=[7, 8, 9, 10], at=35)
        w.cu(op='prefix', id='pref-e2', key='key-e2', through_seq=10, at=36)
        ok = w.release(event_id='rel-full', id='rel-e2-2', key='key-e2',
                       case='case-e2', mode='read_write', at=37)
        self.assertEqual(ok['decision'], 'key_released', ok)
        self.assertEqual(ok['basis']['read_boundary']['readable_seq'], 10)
        self.assertTrue(ok['basis']['read_boundary']['write_ready'])
        audit = custody.audit_release(w.db, 'rel-full')
        self.assertTrue(audit['matches'])
        w.assert_replay_verified()
        w.close()


# ---------------------------------------------------------------------------
# 事故取消 & 席位/可读性分离
# ---------------------------------------------------------------------------

class IncidentAndBoundaryTests(unittest.TestCase):
    def test_incident_cancellation_blocks_release(self):
        w = World()
        w.ready(mode='read')
        cancelled = w.cu(op='cancel_incident', id='cancel-w',
                         incident='inc-w', at=25, reason='customer_called_off')
        self.assertEqual(cancelled['decision'], 'incident_cancelled')
        result = w.release(event_id='rel-cancelled')
        self.assertEqual(result['decision'], 'incident_cancelled')
        w.assert_replay_verified()
        w.close()

    def test_cancel_after_release_does_not_rewrite_history(self):
        w = World()
        w.ready(mode='read')
        ok = w.release(event_id='rel-ok-before-cancel')
        self.assertEqual(ok['decision'], 'key_released')
        w.cu(op='cancel_incident', id='cancel-late', incident='inc-w', at=40)
        row = one(w.db, "SELECT state FROM key_releases WHERE id='release-business-id'")
        self.assertEqual(row['state'], 'released')
        audit = custody.audit_release(w.db, 'rel-ok-before-cancel')
        self.assertTrue(audit['matches'])
        w.assert_replay_verified()
        w.close()

    def test_bare_release_auto_resolves_single_consent_and_device(self):
        # 不带 case/nonce 时，唯一有效许可与目标集群唯一设备可被自动选用。
        w = World()
        w.ready(mode='read_write')
        result = w.cu(event_id='bare-rw', op='release', id='bare-rw-biz',
                      key='key-old', cluster='cluster-c1', at=30)
        self.assertEqual(result['decision'], 'key_released', result)
        self.assertTrue(result['basis']['read_boundary']['write_ready'])
        w.assert_replay_verified()
        w.close()

    def test_readonly_package_to_real_backup_seat_but_rw_requires_chair(self):
        w = World()
        # 只有 intent 合同与 C1 真实备席，不做 assign_primary/handoff。
        w.it(op='rule', id='rule-b', tenant='w', regions=['west'], revision=1)
        w.it(op='claim', id='work-w', tenant='w', region='west', role='critical')
        w.st(op='observe', id='cluster-c1', region='west', domain='rack-c1',
             cpu=8, mem=16, generation=1, healthy=True)
        w.st(op='reserve', id='lease-c1', intent='work-w', cluster='cluster-c1',
             role='backup', cpu=2, mem=4, expires=200)
        w.st(op='occupy', id='lease-c1', at=1)
        self.assertIsNone(
            one(w.db, "SELECT primary_cluster FROM workload_intents WHERE id='work-w'")
            ['primary_cluster'])

        w.custodians()
        w.consent()
        w.device()
        w.old_world()
        w.normal_quorum()
        w.case_and_attest()
        read = w.release(event_id='rel-backup-read')
        self.assertEqual(read['decision'], 'key_released', read)
        seat_code = next(c['code'] for c in read['basis']['checks']
                         if c['root'] == 'seat')
        self.assertEqual(seat_code, 'real_seat_occupied_read_only_package')

        # 尾部对账后申请读写：备席不是主席位，仍拒绝。
        for seq in (9, 10):
            w.cu(op='reconcile_tail', id=f'rec-b-{seq}', tail=f'tail-{seq}')
        w.case_and_attest(mode='read_write', nonce='nonce-write',
                          case_id='case-write', at_case=28, at_att=29)
        rw = w.release(event_id='rel-backup-rw', id='biz-backup-rw',
                       case='case-write', mode='read_write', at=30)
        self.assertEqual(rw['decision'], 'target_not_chair')
        w.assert_replay_verified()
        w.close()

    def test_chair_does_not_imply_historical_data_readable(self):
        w = World()
        w.chair()
        # C1 已是主席位，但没有任何前缀/密文记录。
        chair_row = one(w.db, "SELECT primary_cluster FROM workload_intents WHERE id='work-w'")
        self.assertEqual(chair_row['primary_cluster'], 'cluster-c1')
        w.cu(op='key', id='key-bare', tenant='w', intent='work-w', epoch=1)
        boundary = custody.read_boundary(w.db, 'work-w', 'key-bare')
        self.assertEqual(boundary['readable_seq'], 0)
        self.assertFalse(boundary['write_ready'])
        self.assertTrue(boundary['seat_boot_only'])

        # 即使其他根都齐备，缺少可读前缀时只读释放也被拒绝。
        w.custodians()
        w.normal_quorum(key='key-bare')
        w.consent()
        w.device()
        w.cu(op='release_case', id='case-bare', key='key-bare',
             cluster='cluster-c1', consent='consent-w', incident='inc-w',
             device='dev-c1', mode='read', nonce='n-bare', at=24)
        w.cu(op='attest', id='att-bare', device='dev-c1', generation=2,
             measurement=GOLD_MEASUREMENT, nonce='n-bare', at=25)
        result = w.release(event_id='rel-bare', id='rel-bare-id', key='key-bare',
                           case='case-bare', mode='read', at=26)
        self.assertEqual(result['decision'], 'readable_prefix_missing')
        w.assert_replay_verified()
        w.close()


# ---------------------------------------------------------------------------
# 明文密钥卫生
# ---------------------------------------------------------------------------

class NoPlaintextKeyMaterial(unittest.TestCase):
    def test_raw_key_field_is_redacted_and_never_persisted(self):
        w = World()
        # 尝试把明文 DEK 直接送进命令：必须在落事件日志前被拦截/抹除。
        response = w.cu(op='key', id='evil-key', tenant='w', intent='work-w',
                        epoch=1, plaintext_key=CANARY_DEK)
        self.assertEqual(response['decision'], 'plaintext_key_forbidden')
        dumped = '\n'.join(w.db.iterdump())
        events = '\n'.join(r[0] for r in w.db.execute(
            'SELECT payload || result FROM events'))
        self.assertNotIn(CANARY_DEK, dumped)
        self.assertNotIn(CANARY_DEK, events)
        self.assertIsNone(one(w.db, "SELECT 1 FROM key_epochs WHERE id='evil-key'"))
        w.close()

    def test_full_fixtures_and_audit_export_contain_no_plaintext(self):
        for name in ('key-recovery-read.json', 'key-recovery-rotate.json',
                     'key-recovery-races.json'):
            text = (FIXTURES / name).read_text(encoding='utf-8')
            self.assertNotIn(CANARY_DEK, text)
            # fixture 只允许出现 wrapped/sealed 引用，不允许原始密钥字段。
            lowered = text.lower()
            for forbidden in ('plaintext_key', '"raw_key"', 'key_bytes'):
                self.assertNotIn(forbidden, lowered, f'{name} contains {forbidden}')

        tmp = tempfile.TemporaryDirectory()
        db = connect(str(Path(tmp.name) / 'audit.sqlite'))
        run_envelope_fixture(db, 'key-recovery-read.json')
        export = json.dumps(custody.export_audit(db), ensure_ascii=False)
        self.assertNotIn(CANARY_DEK, export)
        dump = '\n'.join(db.iterdump())
        self.assertNotIn(CANARY_DEK, dump)
        for event_id, payload, result in db.execute(
                'SELECT event_id, payload, result FROM events'):
            self.assertNotIn(CANARY_DEK, payload + result)
            if event_id in ('w-rel-read', 'w-rel-write'):
                decoded = json.loads(result)
                self.assertFalse(
                    decoded['basis']['package']['plaintext_key_included'])
        db.close()
        tmp.cleanup()


if __name__ == '__main__':
    unittest.main()
