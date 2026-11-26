import tempfile
import unittest
from pathlib import Path

from cloudcourt import residency_transition as rt
from cloudcourt.store import connect, one, many, replay

TENANT = 'tenant-T'
WORKLOAD = 'W'
C0, C1, C2 = 'C0', 'C1', 'C2'
EFFECTIVE_AT = 25
SIGNED_AT = 20


class ResidencyCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'residency.sqlite')
        self.db = connect(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def cmd(self, **x):
        return rt.run(self.db, x)

    # -- 题设场景搭建：双区规则、west 主席 C0、west 备席 C1、已确认至序号 32 ------
    def scenario(self, retention_regions=()):
        self.cmd(event_id='r1', op='sign_rule', tenant=TENANT, revision=1,
                 regions=['east', 'west'], effective_at=0, signed_at=0,
                 data_categories=['customer'], purposes=['primary_service'],
                 retention_regions=[], workload_scope='existing_and_new')
        self.cmd(event_id='admit', op='admit', id=WORKLOAD, tenant=TENANT,
                 region='west', role='critical', at=1)
        for cid, region, domain in ((C0, 'west', 'rack-west-0'),
                                    (C1, 'west', 'rack-west-1'),
                                    (C2, 'east', 'rack-east-2')):
            self.cmd(event_id=f'obs-{cid}', op='observe_cluster', id=cid,
                     region=region, domain=domain, cpu=16, mem=32,
                     generation=1, healthy=True)
        for scope, expires in (('failover', 200), ('migration', 200),
                               ('seal_retention', 200)):
            self.cmd(event_id=f'perm-{scope}', op='grant_permission',
                     id=f'perm-{scope}', workload=WORKLOAD, actor='owner-T',
                     scope=scope, revision=1, expires=expires)
        self.cmd(event_id='seat-c0', op='reserve_seat', id='seat-c0',
                 workload=WORKLOAD, cluster=C0, role='primary', cpu=2, mem=4, at=2)
        self.cmd(event_id='occ-c0', op='occupy_seat', id='seat-c0', at=3)
        self.cmd(event_id='seat-c1', op='reserve_seat', id='seat-c1',
                 workload=WORKLOAD, cluster=C1, role='backup', cpu=2, mem=4, at=4)
        self.cmd(event_id='occ-c1', op='occupy_seat', id='seat-c1', at=5)
        for seq in range(1, 33):
            r = self.cmd(event_id=f'w{seq}', op='write', workload=WORKLOAD,
                         cluster=C0, seq=seq, at=6)
            self.assertEqual(r['decision'], 'written')
        self.cmd(event_id='snap-old', op='place_artifact', id='snap-west',
                 workload=WORKLOAD, kind='snapshot', cluster=C0, seq=32, at=18)
        # 第 20 分钟签署 east-only 新规则，第 25 分钟生效。
        self.cmd(event_id='r2', op='sign_rule', tenant=TENANT, revision=2,
                 regions=['east'], effective_at=EFFECTIVE_AT, signed_at=SIGNED_AT,
                 data_categories=['customer'],
                 purposes=['primary_service', 'sealed_retention'],
                 retention_regions=list(retention_regions),
                 workload_scope='existing_and_new')

    def posture(self, at):
        return self.cmd(event_id=f'posture-{at}', op='posture',
                        workload=WORKLOAD, at=at)

    def assert_replay_ok(self):
        self.assertTrue(replay(self.path)['verified'])


class RuleVersioningAndHistory(ResidencyCase):
    def test_rules_keep_effective_time_scope_generation_and_full_history(self):
        self.scenario(retention_regions=['west'])
        history = self.cmd(event_id='hist', op='rule_history', tenant=TENANT)
        self.assertEqual([r['revision'] for r in history['rules']], [1, 2])
        new = history['rules'][1]
        self.assertEqual(new['effective_at'], 25)
        self.assertEqual(new['signed_at'], 20)
        self.assertEqual(new['regions'], ['east'])
        self.assertEqual(new['data_categories'], ['customer'])
        self.assertIn('primary_service', new['purposes'])
        self.assertEqual(new['workload_scope'], 'existing_and_new')
        # 旧规则及当时合法的处理事实原样保留、可审计。
        self.assertEqual(history['rules'][0]['regions'], ['east', 'west'])
        writes = many(self.db, 'SELECT region FROM rt_writes WHERE workload=?', (WORKLOAD,))
        self.assertEqual(len(writes), 32)
        self.assertTrue(all(w['region'] == 'west' for w in writes))

    def test_stale_revision_and_invalid_regions_rejected(self):
        self.scenario()
        stale = self.cmd(event_id='stale', op='sign_rule', tenant=TENANT,
                         revision=2, regions=['east'], effective_at=40, signed_at=30)
        self.assertEqual(stale['decision'], 'stale_rule')
        bad = self.cmd(event_id='bad', op='sign_rule', tenant=TENANT,
                       revision=3, regions=[], effective_at=40, signed_at=30)
        self.assertEqual(bad['decision'], 'invalid_regions')
        early = self.cmd(event_id='early', op='sign_rule', tenant=TENANT,
                         revision=3, regions=['east'], effective_at=10, signed_at=30)
        self.assertEqual(early['decision'], 'invalid_effective_time')


class OccupiedSeatKeepsPhysicalLocation(ResidencyCase):
    def test_occupied_non_compliant_seat_keeps_region_and_obligation(self):
        self.scenario()
        before = self.posture(22)
        self.assertTrue(all(s['compliance'] == 'compliant' for s in before['seats']))
        after = self.posture(26)
        by_id = {s['seat']: s for s in after['seats']}
        # 真实所在位置保留，绝不静默写成 compliant。
        self.assertEqual(by_id['seat-c0']['physical_region'], 'west')
        self.assertEqual(by_id['seat-c1']['physical_region'], 'west')
        self.assertEqual(by_id['seat-c0']['compliance'], 'non_compliant')
        self.assertEqual(by_id['seat-c1']['compliance'], 'non_compliant')
        self.assertEqual(after['effective_rule_revision'], 2)
        self.assertEqual(len(after['open_obligations']), 1)
        # 底层席位行仍是 west 实物，没有被改写成 east。
        row = one(self.db, 'SELECT region,state FROM rt_seats WHERE id=?', ('seat-c0',))
        self.assertEqual(row['region'], 'west')
        self.assertEqual(row['state'], 'occupied')

    def test_old_seat_cannot_be_released_while_obligation_open(self):
        self.scenario()
        rel = self.cmd(event_id='rel-c0', op='release_seat', id='seat-c0')
        self.assertEqual(rel['decision'], 'retention_required')
        self.assertEqual(one(self.db, 'SELECT state FROM rt_seats WHERE id=?',
                             ('seat-c0',))['state'], 'occupied')


class NewReservationsGatedBySignedFutureRule(ResidencyCase):
    def test_new_west_backup_reserve_and_occupy_denied_after_signing(self):
        self.scenario()
        self.cmd(event_id='obs-c3', op='observe_cluster', id='C3',
                 region='west', domain='rack-west-3', cpu=16, mem=32,
                 generation=1, healthy=True)
        # 第 23 分钟（生效前）新的 west 备席已被已签署的未来规则拦截。
        r = self.cmd(event_id='res-c3', op='reserve_seat', id='seat-c3',
                     workload=WORKLOAD, cluster='C3', role='backup',
                     cpu=1, mem=1, at=23)
        self.assertEqual(r['decision'], 'residency_denied')
        # 同一已签署规则下，east C2 的新负载席位可以预留（独立灾难域、独立容量）。
        self.cmd(event_id='admit-east', op='admit', id='W-east', tenant=TENANT,
                 region='east', role='batch', at=21)
        ok = self.cmd(event_id='res-c2', op='reserve_seat', id='seat-c2',
                      workload='W-east', cluster=C2, role='primary',
                      cpu=1, mem=1, at=23)
        self.assertEqual(ok['decision'], 'reserved')
        self.assertEqual(ok['region'], 'east')
        # W 自身在 west 的既有备席 C1 仍被占用，但姿态已标为不合规、挂迁出义务。
        posture = self.posture(26)
        c1 = {s['seat']: s for s in posture['seats']}['seat-c1']
        self.assertEqual(c1['state'], 'occupied')
        self.assertEqual(c1['compliance'], 'non_compliant')
        self.assertTrue(posture['open_obligations'])

    def test_reservation_made_before_signing_is_re_checked_at_occupy(self):
        self.scenario()
        # 先在另一 west 集群预留（与 C0/C1 不同灾难域），再签署收窄规则。
        self.cmd(event_id='obs-c4', op='observe_cluster', id='C4',
                 region='west', domain='rack-west-4', cpu=16, mem=32,
                 generation=1, healthy=True)
        # 该负载已有 primary+backup，角色已承诺；改用新负载验证占用时复审。
        self.cmd(event_id='admit2', op='admit', id='W2', tenant=TENANT,
                 region='west', role='batch', at=2)
        self.cmd(event_id='res-c4', op='reserve_seat', id='seat-c4',
                 workload='W2', cluster='C4', role='primary', cpu=1, mem=1, at=3)
        self.cmd(event_id='r2b', op='sign_rule', tenant=TENANT, revision=2,
                 regions=['east'], effective_at=25, signed_at=20)
        occ = self.cmd(event_id='occ-c4', op='occupy_seat', id='seat-c4', at=24)
        self.assertEqual(occ['decision'], 'residency_denied')
        self.assertEqual(one(self.db, "SELECT state FROM rt_seats WHERE id='seat-c4'")['state'],
                         'rejected')


class SnapshotAndWriteResidency(ResidencyCase):
    def test_snapshot_stranded_after_effective_boundary(self):
        self.scenario(retention_regions=['west'])
        # 生效前的 west 快照在当时合法，保留可审计。
        self.assertEqual(self.posture(24)['artifacts'][0]['compliance'], 'compliant')
        # 生效后新的 west 快照/缓存/恢复点一律拒绝——校验实物数据路径。
        late = self.cmd(event_id='snap-late', op='place_artifact',
                        id='snap-west-late', workload=WORKLOAD, kind='snapshot',
                        cluster=C0, seq=33, at=26)
        self.assertEqual(late['decision'], 'residency_denied')
        cache = self.cmd(event_id='cache-late', op='place_artifact',
                         id='cache-west', workload=WORKLOAD, kind='cache',
                         cluster=C1, at=26)
        self.assertEqual(cache['decision'], 'residency_denied')
        # 滞留旧快照在生效后被即时推导为 non_compliant，而不是被静默放行。
        art = {a['artifact']: a for a in self.posture(26)['artifacts']}['snap-west']
        self.assertEqual(art['physical_region'], 'west')
        self.assertEqual(art['compliance'], 'non_compliant')

    def test_writes_after_effective_time_rejected_on_west_primary(self):
        self.scenario()
        # 第 24 分钟仍可在 west 写（旧规则有效）。
        self.assertEqual(self.cmd(event_id='w33', op='write', workload=WORKLOAD,
                                  cluster=C0, seq=33, at=24)['decision'], 'written')
        # 第 25 分钟后 C1/C0 继续处理客户新写入必须被拒。
        denied = self.cmd(event_id='w34', op='write', workload=WORKLOAD,
                          cluster=C0, seq=34, at=25)
        self.assertEqual(denied['decision'], 'residency_denied')
        # 迟到写回：序号不允许被跳号或在 west 补写。
        jump = self.cmd(event_id='w35', op='write', workload=WORKLOAD,
                        cluster=C1, seq=35, at=27)
        self.assertEqual(jump['decision'], 'residency_denied')
        self.assertEqual(one(self.db, 'SELECT MAX(seq) m FROM rt_writes WHERE workload=?',
                             (WORKLOAD,))['m'], 33)

    def test_data_category_and_purpose_are_part_of_the_rule(self):
        self.scenario()
        denied = self.cmd(event_id='w-other', op='write', workload=WORKLOAD,
                          cluster=C0, seq=33, at=22,
                          data_category='telemetry', purpose='primary_service')
        self.assertEqual(denied['decision'], 'residency_denied')


class HandoffBlockedAcrossBoundary(ResidencyCase):
    def _incident_and_receipts(self, at_incident=23):
        self.cmd(event_id='inc', op='incident', id='inc-c0', cluster=C0,
                 severity=4, generation=1)

    def test_existing_backup_and_old_plan_cannot_take_over_after_boundary(self):
        self.scenario()
        self._incident_and_receipts()
        # 计划阶段即因目标实物地域不满足已签署规则而被拒。
        plan = self.cmd(event_id='plan-c1', op='plan_handoff', id='move-c1',
                        workload=WORKLOAD, source=C0, target=C1,
                        incident='inc-c0', permission='perm-failover', at=23)
        self.assertEqual(plan['decision'], 'residency_denied')

    def test_handoff_planned_before_signing_is_killed_by_late_receipts(self):
        # 新规则在接管计划之后才签署：迟到回执必须重新受审。
        self.cmd(event_id='r1b', op='sign_rule', tenant=TENANT, revision=1,
                 regions=['east', 'west'], effective_at=0, signed_at=0)
        self.cmd(event_id='admitb', op='admit', id=WORKLOAD, tenant=TENANT,
                 region='west', role='critical', at=1)
        for cid, region, domain in ((C0, 'west', 'rack-west-0'),
                                    (C1, 'west', 'rack-west-1')):
            self.cmd(event_id=f'obsb-{cid}', op='observe_cluster', id=cid,
                     region=region, domain=domain, cpu=16, mem=32,
                     generation=1, healthy=True)
        self.cmd(event_id='permb', op='grant_permission', id='perm-failover',
                 workload=WORKLOAD, actor='owner-T', scope='failover',
                 revision=1, expires=200)
        self.cmd(event_id='sb0', op='reserve_seat', id='seat-c0',
                 workload=WORKLOAD, cluster=C0, role='primary', cpu=2, mem=4, at=2)
        self.cmd(event_id='ob0', op='occupy_seat', id='seat-c0', at=3)
        self.cmd(event_id='sb1', op='reserve_seat', id='seat-c1',
                 workload=WORKLOAD, cluster=C1, role='backup', cpu=2, mem=4, at=4)
        self.cmd(event_id='ob1', op='occupy_seat', id='seat-c1', at=5)
        self.cmd(event_id='incb', op='incident', id='inc-c0', cluster=C0,
                 severity=4, generation=1)
        planned = self.cmd(event_id='planb', op='plan_handoff', id='move-old',
                           workload=WORKLOAD, source=C0, target=C1,
                           incident='inc-c0', permission='perm-failover', at=10)
        self.assertEqual(planned['decision'], 'planned')
        # 第 20 分钟收窄、第 25 分钟生效；第 26 分钟维护员才提交双端回执。
        self.cmd(event_id='r2b2', op='sign_rule', tenant=TENANT, revision=2,
                 regions=['east'], effective_at=25, signed_at=20)
        # 源端围栏只是旧主站停写的证据，可以记录。
        fence = self.cmd(event_id='fence-late', op='handoff_receipt',
                         id='rc-fence', handoff='move-old', kind='source_fenced',
                         cluster=C0, generation=1, at=26)
        self.assertEqual(fence['decision'], 'recorded')
        # 目标就绪回执建立实物数据路径：west C1 已被新规则排除，迟到回执被拒，
        # 旧接管计划随之阻断。
        ready = self.cmd(event_id='ready-late', op='handoff_receipt',
                         id='rc-ready-late', handoff='move-old', kind='target_ready',
                         cluster=C1, generation=1, at=26)
        self.assertEqual(ready['decision'], 'residency_denied')
        self.assertEqual(one(self.db, "SELECT state FROM rt_handoffs WHERE id='move-old'")['state'],
                         'blocked')
        # 即使补做 complete_handoff 也不能让越界接管生效。
        self.assertEqual(self.cmd(event_id='finish-late', op='complete_handoff',
                                  id='move-old', at=27)['decision'],
                         'plan_unavailable')
        # 越界接管未生效：主站仍是 C0，west 不接受新写入。
        self.assertEqual(one(self.db, 'SELECT primary_cluster FROM rt_workloads WHERE id=?',
                             (WORKLOAD,))['primary_cluster'], C0)

    def test_target_generation_change_invalidates_handoff(self):
        self.scenario()
        # 合规 east 备席 + east 接管路径。
        self.cmd(event_id='res-e', op='reserve_seat', id='seat-c2',
                 workload=WORKLOAD, cluster=C2, role='backup', cpu=2, mem=4, at=21)
        self.cmd(event_id='occ-e', op='occupy_seat', id='seat-c2', at=22)
        self.cmd(event_id='inc-e', op='incident', id='inc-c0', cluster=C0,
                 severity=4, generation=1)
        self.cmd(event_id='plan-e', op='plan_handoff', id='move-east',
                 workload=WORKLOAD, source=C0, target=C2, incident='inc-c0',
                 permission='perm-failover', at=23)
        self.cmd(event_id='fence-e', op='handoff_receipt', id='rc-fence',
                 handoff='move-east', kind='source_fenced',
                 cluster=C0, generation=1, at=23)
        self.cmd(event_id='ready-e', op='handoff_receipt', id='rc-ready',
                 handoff='move-east', kind='target_ready',
                 cluster=C2, generation=1, at=24)
        # 目标 C2 在接管完成前换代（重建）。
        self.cmd(event_id='rebuild-c2', op='observe_cluster', id=C2,
                 region='east', domain='rack-east-2', cpu=16, mem=32,
                 generation=2, healthy=True)
        done = self.cmd(event_id='finish-e', op='complete_handoff',
                        id='move-east', at=26)
        self.assertEqual(done['decision'], 'stale_handoff_evidence')
        self.assertEqual(one(self.db, 'SELECT primary_cluster FROM rt_workloads WHERE id=?',
                             (WORKLOAD,))['primary_cluster'], C0)

    def test_permission_withdraw_blocks_otherwise_complete_handoff(self):
        self.scenario()
        self.cmd(event_id='res-e2', op='reserve_seat', id='seat-c2',
                 workload=WORKLOAD, cluster=C2, role='backup', cpu=2, mem=4, at=21)
        self.cmd(event_id='occ-e2', op='occupy_seat', id='seat-c2', at=22)
        self.cmd(event_id='inc-e2', op='incident', id='inc-c0', cluster=C0,
                 severity=4, generation=1)
        self.cmd(event_id='plan-e2', op='plan_handoff', id='move-east2',
                 workload=WORKLOAD, source=C0, target=C2, incident='inc-c0',
                 permission='perm-failover', at=23)
        self.cmd(event_id='fence-e2', op='handoff_receipt', id='rc-fence2',
                 handoff='move-east2', kind='source_fenced',
                 cluster=C0, generation=1, at=23)
        self.cmd(event_id='ready-e2', op='handoff_receipt', id='rc-ready2',
                 handoff='move-east2', kind='target_ready',
                 cluster=C2, generation=1, at=24)
        # 客户撤回许可：即使双端回执齐备也不得接管。
        wd = self.cmd(event_id='withdraw', op='withdraw_permission',
                      id='perm-failover')
        self.assertEqual(wd['decision'], 'withdrawn')
        done = self.cmd(event_id='finish-e2', op='complete_handoff',
                        id='move-east2', at=26)
        self.assertEqual(done['decision'], 'authorization_denied')
        self.assertEqual(one(self.db, "SELECT state FROM rt_handoffs WHERE id='move-east2'")['state'],
                         'blocked')
        self.assertEqual(one(self.db, 'SELECT primary_cluster FROM rt_workloads WHERE id=?',
                             (WORKLOAD,))['primary_cluster'], C0)


class MigrationOptionA(ResidencyCase):
    def test_live_migration_to_east_before_effective_time(self):
        self.scenario()
        # 事前准许的执行方式 A：第 25 分钟前迁到有独立容量的 east C2，
        # C1 降为只保留审计证据。
        plan = self.cmd(event_id='mig', op='plan_migration', id='mig-a',
                        workload=WORKLOAD, source=C0, target=C2,
                        permission='perm-migration', mode='live_cutover', at=21)
        self.assertEqual(plan['decision'], 'migration_planned')
        self.cmd(event_id='pause', op='pause_writes', migration='mig-a', at=22)
        cp = self.cmd(event_id='copy', op='copy_prefix', migration='mig-a', at=22)
        self.assertEqual(cp['decision'], 'prefix_copied')
        self.assertEqual(cp['seq_through'], 32)
        # 目标侧 east 席位（独立容量、独立灾难域）。
        self.cmd(event_id='res-mig', op='reserve_seat', id='seat-c2',
                 workload=WORKLOAD, cluster=C2, role='backup', cpu=2, mem=4, at=22)
        self.cmd(event_id='occ-mig', op='occupy_seat', id='seat-c2', at=23)
        ver = self.cmd(event_id='verify', op='verify_target',
                       migration='mig-a', generation=1, at=23)
        self.assertEqual(ver['decision'], 'target_verified')
        # C0 与 C1 都降为证据席，不直接释放。
        self.assertEqual(self.cmd(event_id='demote-c0', op='demote_to_evidence',
                                  migration='mig-a', cluster=C0, at=23)['decision'],
                         'demoted_to_evidence')
        self.assertEqual(self.cmd(event_id='demote-c1', op='demote_to_evidence',
                                  migration='mig-a', cluster=C1, at=23)['decision'],
                         'demoted_to_evidence')
        resume = self.cmd(event_id='resume', op='resume_writes',
                          migration='mig-a', at=24)
        self.assertEqual(resume['decision'], 'writes_resumed')
        self.assertEqual(resume['cluster'], C2)
        done = self.cmd(event_id='mig-done', op='complete_migration',
                        migration='mig-a', at=24)
        self.assertEqual(done['decision'], 'migration_complete')
        # 第 25 分钟后客户写入口在 east，序号 33 连续。
        w = self.cmd(event_id='w33', op='write', workload=WORKLOAD,
                     cluster=C2, seq=33, at=26)
        self.assertEqual(w['decision'], 'written')
        self.assertEqual(w['region'], 'east')
        posture = self.posture(26)
        self.assertEqual(posture['open_obligations'], [])
        self.assertEqual(posture['primary_cluster'], C2)
        # west 旧席保留为证据，真实位置仍是 west，且不能被释放。
        seats = {s['seat']: s for s in posture['seats']}
        self.assertEqual(seats['seat-c0']['state'], 'evidence')
        self.assertEqual(seats['seat-c0']['physical_region'], 'west')
        self.assertEqual(self.cmd(event_id='rel-evidence', op='release_seat',
                                  id='seat-c1')['decision'], 'retention_required')
        self.assert_replay_ok()


class MigrationOptionB(ResidencyCase):
    def test_pause_writes_seal_in_west_then_restore_after_integrity_check(self):
        self.scenario(retention_regions=['west'])
        plan = self.cmd(event_id='mig-b', op='plan_migration', id='mig-b',
                        workload=WORKLOAD, source=C0, target=C2,
                        permission='perm-migration', mode='pause_and_seal', at=21)
        self.assertEqual(plan['decision'], 'migration_planned')
        # 客户经历有记录的中断：新写入暂停。
        self.assertEqual(self.cmd(event_id='pause-b', op='pause_writes',
                                  migration='mig-b', at=22)['decision'],
                         'writes_paused')
        blocked = self.cmd(event_id='w-paused', op='write', workload=WORKLOAD,
                           cluster=C0, seq=33, at=23)
        self.assertEqual(blocked['decision'], 'writes_paused')
        # west 旧页在规则生效后取得限定用途封存许可并封存（非普通处理）。
        seal = self.cmd(event_id='seal', op='seal_artifact', id='snap-west',
                        permission='perm-seal_retention', at=26)
        self.assertEqual(seal['decision'], 'sealed')
        posture = self.posture(26)
        art = {a['artifact']: a for a in posture['artifacts']}['snap-west']
        self.assertEqual(art['state'], 'sealed')
        self.assertEqual(art['compliance'], 'compliant')
        # 无封存许可或规则未列 retention 地域时，封存被拒（同一工件的二次封存）。
        self.assertEqual(self.cmd(event_id='seal-noperm', op='seal_artifact',
                                  id='snap-west', at=26)['decision'],
                         'authorization_denied')
        self.assertEqual(one(self.db, "SELECT state FROM rt_artifacts WHERE id='snap-west'")['state'],
                         'sealed')        # 数据完整性核验通过后才在 east 恢复。
        self.cmd(event_id='copy-b', op='copy_prefix', migration='mig-b', at=24)
        self.cmd(event_id='res-b', op='reserve_seat', id='seat-c2',
                 workload=WORKLOAD, cluster=C2, role='backup', cpu=2, mem=4, at=24)
        self.cmd(event_id='occ-b', op='occupy_seat', id='seat-c2', at=24)
        self.assertEqual(self.cmd(event_id='verify-b', op='verify_target',
                                  migration='mig-b', generation=1, at=25)['decision'],
                         'target_verified')
        self.cmd(event_id='demote-b0', op='demote_to_evidence',
                 migration='mig-b', cluster=C0, at=25)
        self.cmd(event_id='demote-b1', op='demote_to_evidence',
                 migration='mig-b', cluster=C1, at=25)
        self.assertEqual(self.cmd(event_id='resume-b', op='resume_writes',
                                  migration='mig-b', at=26)['decision'],
                         'writes_resumed')
        self.cmd(event_id='done-b', op='complete_migration',
                 migration='mig-b', at=26)
        w = self.cmd(event_id='w33b', op='write', workload=WORKLOAD,
                     cluster=C2, seq=33, at=27)
        self.assertEqual(w['decision'], 'written')
        self.assertEqual(w['region'], 'east')
        self.assert_replay_ok()

    def test_seal_denied_without_retention_region_in_rule(self):
        self.scenario(retention_regions=[])
        self.cmd(event_id='mig-c', op='plan_migration', id='mig-c',
                 workload=WORKLOAD, source=C0, target=C2,
                 permission='perm-migration', mode='pause_and_seal', at=21)
        self.cmd(event_id='pause-c', op='pause_writes', migration='mig-c', at=22)
        self.assertEqual(self.cmd(event_id='seal-c', op='seal_artifact',
                                  id='snap-west', permission='perm-seal_retention',
                                  at=26)['decision'], 'residency_denied')


class MigrationCancellationAndInterleaving(ResidencyCase):
    def test_cancel_migration_retains_obligation_and_keeps_writes_paused(self):
        self.scenario()
        self.cmd(event_id='mig-d', op='plan_migration', id='mig-d',
                 workload=WORKLOAD, source=C0, target=C2,
                 permission='perm-migration', mode='live_cutover', at=21)
        self.cmd(event_id='pause-d', op='pause_writes', migration='mig-d', at=22)
        cancel = self.cmd(event_id='cancel-d', op='cancel_migration',
                          migration='mig-d', at=24)
        self.assertEqual(cancel['decision'], 'migration_cancelled')
        self.assertTrue(cancel['obligations_retained'])
        self.assertEqual(cancel['write_state'], 'paused')
        # 取消不消除地域义务，也不会恢复越界写入。
        self.assertEqual(self.cmd(event_id='w-after-cancel', op='write',
                                  workload=WORKLOAD, cluster=C0, seq=33,
                                  at=26)['decision'], 'writes_paused')
        self.assertEqual(one(self.db, "SELECT state FROM rt_migrations WHERE id='mig-d'")['state'],
                         'cancelled')

    def test_withdrawn_migration_permission_blocks_cutover(self):
        self.scenario()
        self.cmd(event_id='mig-e', op='plan_migration', id='mig-e',
                 workload=WORKLOAD, source=C0, target=C2,
                 permission='perm-migration', mode='live_cutover', at=21)
        self.cmd(event_id='withdraw-mig', op='withdraw_permission',
                 id='perm-migration')
        self.assertEqual(self.cmd(event_id='pause-e', op='pause_writes',
                                  migration='mig-e', at=22)['decision'],
                         'authorization_denied')
        self.assertEqual(one(self.db, "SELECT state FROM rt_migrations WHERE id='mig-e'")['state'],
                         'blocked')

    def test_old_and_new_rule_interleave_by_effective_boundary(self):
        self.scenario()
        # 生效前：west 合法，写入与快照均允许。
        self.assertEqual(self.cmd(event_id='w-pre', op='write',
                                  workload=WORKLOAD, cluster=C0, seq=33,
                                  at=24)['decision'], 'written')
        snap_ok = self.cmd(event_id='snap-pre', op='place_artifact',
                           id='snap-pre-25', workload=WORKLOAD, kind='snapshot',
                           cluster=C1, seq=33, at=24)
        self.assertEqual(snap_ok['decision'], 'artifact_placed')
        # 生效后：同一 west 实物路径上的同类操作全部被拒。
        self.assertEqual(self.cmd(event_id='w-post', op='write',
                                  workload=WORKLOAD, cluster=C0, seq=34,
                                  at=25)['decision'], 'residency_denied')
        self.assertEqual(self.cmd(event_id='snap-post', op='place_artifact',
                                  id='snap-post-25', workload=WORKLOAD,
                                  kind='recovery_point', cluster=C1,
                                  at=25)['decision'], 'residency_denied')
        # east 新路径恢复点允许。
        self.assertEqual(self.cmd(event_id='snap-east', op='place_artifact',
                                  id='snap-east-25', workload=WORKLOAD,
                                  kind='recovery_point', cluster=C2,
                                  at=25)['decision'], 'artifact_placed')
        self.assert_replay_ok()


class DeterministicReplay(ResidencyCase):
    def test_full_scenario_replays_event_by_event(self):
        self.scenario(retention_regions=['west'])
        # 再跑一段接管被拒与迁移完成的混合历史，验证事件重放逐表一致。
        self.cmd(event_id='inc-r', op='incident', id='inc-c0', cluster=C0,
                 severity=4, generation=1)
        self.assertEqual(self.cmd(event_id='plan-r', op='plan_handoff',
                                  id='move-r', workload=WORKLOAD, source=C0,
                                  target=C1, incident='inc-c0',
                                  permission='perm-failover', at=23)['decision'],
                         'residency_denied')
        self.cmd(event_id='mig-r', op='plan_migration', id='mig-r',
                 workload=WORKLOAD, source=C0, target=C2,
                 permission='perm-migration', mode='live_cutover', at=21)
        self.cmd(event_id='pause-r', op='pause_writes', migration='mig-r', at=22)
        self.cmd(event_id='copy-r', op='copy_prefix', migration='mig-r', at=22)
        self.cmd(event_id='res-r', op='reserve_seat', id='seat-c2',
                 workload=WORKLOAD, cluster=C2, role='backup', cpu=2, mem=4, at=22)
        self.cmd(event_id='occ-r', op='occupy_seat', id='seat-c2', at=23)
        self.cmd(event_id='verify-r', op='verify_target',
                 migration='mig-r', generation=1, at=23)
        self.cmd(event_id='demote-r0', op='demote_to_evidence',
                 migration='mig-r', cluster=C0, at=23)
        self.cmd(event_id='demote-r1', op='demote_to_evidence',
                 migration='mig-r', cluster=C1, at=23)
        self.cmd(event_id='resume-r', op='resume_writes', migration='mig-r', at=24)
        self.cmd(event_id='done-r', op='complete_migration',
                 migration='mig-r', at=24)
        report = replay(self.path)
        self.assertTrue(report['verified'], report['mismatch'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
