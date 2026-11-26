import json
import tempfile
import unittest
from pathlib import Path

from cloudcourt import hardware, seat
from cloudcourt.store import connect, one, many, replay

FIXTURE = Path('fixtures/hardware-recall.json')


class HardwareRecall(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'recall.sqlite')
        self.db = connect(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def hw(self, payload):
        return hardware.run(self.db, payload)

    def run_fixture(self):
        commands = json.loads(FIXTURE.read_text(encoding='utf-8'))
        return {c['event_id']: self.hw(c) for c in commands}

    def stock(self, serial, lot, seal='seal', batch='batch', at=1):
        self.assertEqual(self.hw({'event_id': f'attest-{serial}', 'op': 'attest', 'serial': serial,
                                  'lot': lot, 'seal': seal, 'batch': batch, 'at': at})['decision'], 'attested')
        self.assertEqual(self.hw({'event_id': f'intake-{serial}', 'op': 'intake', 'serial': serial,
                                  'custodian': 'keeper', 'at': at})['decision'], 'stocked')

    def install(self, serial, lot, cluster, at, custodian='tech'):
        return self.hw({'event_id': f'install-{serial}-{cluster}-{at}', 'op': 'install', 'serial': serial,
                        'lot': lot, 'cluster': cluster, 'at': at, 'custodian': custodian})

    def test_recall_fixture_flow_and_replay(self):
        decisions = self.run_fixture()
        self.assertEqual(decisions['rc-recall']['decision'], 'recall_published')
        self.assertEqual(decisions['rc-recall']['affected'], 1)
        self.assertEqual(decisions['rc-remove']['decision'], 'removed_witnessed')
        for leg in ('rc-leg-out', 'rc-leg-carrier', 'rc-leg-center'):
            self.assertEqual(decisions[leg]['decision'], 'leg_recorded')
        self.assertEqual(decisions['rc-accept']['decision'], 'replacement_accepted')
        self.assertEqual(decisions['rc-restore']['decision'], 'service_restored')
        self.assertEqual(decisions['rc-restore']['restored_at'], 170)
        self.assertEqual(decisions['rc-ack']['decision'], 'vendor_acknowledged')
        settle = decisions['rc-settle']
        self.assertEqual(settle['decision'], 'settled')
        self.assertEqual(settle['warranty_waiver'], 800)
        self.assertEqual(settle['downtime_loss'], 5000)
        self.assertEqual(settle['vendor_payable'], 800)
        self.assertEqual(settle['platform_borne'], 5050)
        # 召回不改写历史:批次状态与安装事实保留
        self.assertEqual(one(self.db, 'SELECT state FROM hardware_lots WHERE id=?', ('lot-c7',))['state'], 'accepted')
        self.assertEqual(one(self.db, 'SELECT state FROM hardware_units WHERE serial=?', ('card-c7-001',))['state'],
                         'removed')
        # 供应商回执迟到(500)不倒填客户服务恢复时刻(170)
        self.assertEqual(one(self.db, 'SELECT restored_at FROM repair_orders WHERE id=?', ('ro-c7-001',))['restored_at'],
                         170)
        # 逐次保管人监管链:入库、安装、拆卸见证
        chain = many(self.db, 'SELECT kind, actor, witness, detail FROM custody_events WHERE serial=? ORDER BY at',
                     ('card-c7-001',))
        self.assertEqual([c['kind'] for c in chain], ['intake', 'install', 'remove'])
        self.assertEqual(chain[0]['actor'], 'keeper-a')
        self.assertEqual(chain[1]['actor'], 'tech-a')
        self.assertEqual(chain[2]['witness'], 'auditor-a')
        self.assertEqual(chain[2]['detail'], 'sealed')
        # 物理验收完成后集群健康观测放行
        obs = seat.run(self.db, {'event_id': 'obs-c7', 'op': 'observe', 'id': 'cluster-c7', 'region': 'east',
                                 'domain': 'rack-c7', 'cpu': 8, 'mem': 16, 'generation': 1, 'healthy': True})
        self.assertEqual(obs['decision'], 'observed')
        self.assertTrue(replay(self.path)['verified'])

    def test_duplicate_serial_cannot_occupy_two_sites(self):
        self.hw({'event_id': 'lot', 'op': 'lot', 'id': 'lot-a', 'vendor': 'v', 'model': 'm', 'revision': 1})
        self.stock('ser-1', 'lot-a')
        self.assertEqual(self.install('ser-1', 'lot-a', 'cluster-a', 5)['decision'], 'installed')
        dup = self.install('ser-1', 'lot-a', 'cluster-b', 6)
        self.assertEqual(dup['decision'], 'serial_already_installed')
        self.assertEqual(dup['cluster'], 'cluster-a')
        # 拆卸后才可在他处上线,且仍然独占
        self.hw({'event_id': 'ro', 'op': 'repair', 'id': 'ro-1', 'serial': 'ser-1', 'at': 7})
        self.assertEqual(self.hw({'event_id': 'rm', 'op': 'remove', 'serial': 'ser-1', 'custodian': 'tech',
                                  'witness': 'auditor', 'key_disposition': 'wiped', 'at': 8})['decision'],
                         'removed_witnessed')
        self.assertEqual(self.install('ser-1', 'lot-a', 'cluster-b', 9)['decision'], 'installed')
        still = self.install('ser-1', 'lot-a', 'cluster-a', 10)
        self.assertEqual(still['decision'], 'serial_already_installed')
        self.assertEqual(still['cluster'], 'cluster-b')
        self.assertTrue(replay(self.path)['verified'])

    def test_vendor_seal_and_stock_chain(self):
        self.hw({'event_id': 'lot', 'op': 'lot', 'id': 'lot-a', 'vendor': 'v', 'model': 'm', 'revision': 1})
        # 无供应商签章不得入库
        self.assertEqual(self.hw({'event_id': 'intake-x', 'op': 'intake', 'serial': 'ser-x', 'custodian': 'keeper',
                                  'at': 1})['decision'], 'unattested_serial')
        # 签章缺字段、批次缺失均被拒
        self.assertEqual(self.hw({'event_id': 'att-noseal', 'op': 'attest', 'serial': 'ser-x', 'lot': 'lot-a',
                                  'seal': '', 'batch': 'W1', 'at': 1})['decision'], 'invalid_attestation')
        self.assertEqual(self.hw({'event_id': 'att-nolot', 'op': 'attest', 'serial': 'ser-x', 'lot': 'missing',
                                  'seal': 's', 'batch': 'W1', 'at': 1})['decision'], 'lot_unavailable')
        self.assertEqual(self.hw({'event_id': 'att-ok', 'op': 'attest', 'serial': 'ser-x', 'lot': 'lot-a',
                                  'seal': 'sig-1', 'batch': 'W1', 'at': 1})['decision'], 'attested')
        # 同一序列号不得改签到其他签章
        self.assertEqual(self.hw({'event_id': 'att-conflict', 'op': 'attest', 'serial': 'ser-x', 'lot': 'lot-a',
                                  'seal': 'sig-2', 'batch': 'W1', 'at': 2})['decision'], 'attestation_conflict')
        self.assertEqual(self.hw({'event_id': 'intake-ok', 'op': 'intake', 'serial': 'ser-x', 'custodian': 'keeper',
                                  'at': 2})['decision'], 'stocked')
        self.assertEqual(self.hw({'event_id': 'intake-again', 'op': 'intake', 'serial': 'ser-x', 'custodian': 'keeper',
                                  'at': 3})['decision'], 'already_in_stock')
        att = one(self.db, 'SELECT seal, batch FROM vendor_attestations WHERE serial=?', ('ser-x',))
        self.assertEqual((att['seal'], att['batch']), ('sig-1', 'W1'))
        self.assertTrue(replay(self.path)['verified'])

    def test_removal_requires_independent_witness(self):
        self.hw({'event_id': 'lot', 'op': 'lot', 'id': 'lot-a', 'vendor': 'v', 'model': 'm', 'revision': 1})
        self.stock('ser-1', 'lot-a')
        self.install('ser-1', 'lot-a', 'cluster-a', 5)
        # 无维修单不得拆卸
        self.assertEqual(self.hw({'event_id': 'rm-nor', 'op': 'remove', 'serial': 'ser-1', 'custodian': 'tech',
                                  'witness': 'auditor', 'key_disposition': 'wiped', 'at': 6})['decision'],
                         'no_open_repair')
        self.hw({'event_id': 'ro', 'op': 'repair', 'id': 'ro-1', 'serial': 'ser-1', 'at': 6})
        # 无见证人、见证人不独立、密钥残留未处置均被拒
        self.assertEqual(self.hw({'event_id': 'rm-nowit', 'op': 'remove', 'serial': 'ser-1', 'custodian': 'tech',
                                  'key_disposition': 'wiped', 'at': 7})['decision'], 'witness_required')
        self.assertEqual(self.hw({'event_id': 'rm-self', 'op': 'remove', 'serial': 'ser-1', 'custodian': 'tech',
                                  'witness': 'tech', 'key_disposition': 'wiped', 'at': 7})['decision'],
                         'witness_not_independent')
        self.assertEqual(self.hw({'event_id': 'rm-nokey', 'op': 'remove', 'serial': 'ser-1', 'custodian': 'tech',
                                  'witness': 'auditor', 'key_disposition': 'ignored', 'at': 7})['decision'],
                         'key_residue_unhandled')
        # 拆卸时间不得早于安装时间
        self.assertEqual(self.hw({'event_id': 'rm-time', 'op': 'remove', 'serial': 'ser-1', 'custodian': 'tech',
                                  'witness': 'auditor', 'key_disposition': 'wiped', 'at': 4})['decision'],
                         'time_regression')
        self.assertEqual(self.hw({'event_id': 'rm-ok', 'op': 'remove', 'serial': 'ser-1', 'custodian': 'tech',
                                  'witness': 'auditor', 'key_disposition': 'wiped', 'at': 8})['decision'],
                         'removed_witnessed')
        self.assertEqual(self.hw({'event_id': 'rm-again', 'op': 'remove', 'serial': 'ser-1', 'custodian': 'tech',
                                  'witness': 'auditor', 'key_disposition': 'wiped', 'at': 9})['decision'],
                         'unit_not_installed')
        self.assertTrue(replay(self.path)['verified'])

    def test_transport_chain_and_vendor_receipt(self):
        self.hw({'event_id': 'lot', 'op': 'lot', 'id': 'lot-a', 'vendor': 'v', 'model': 'm', 'revision': 1})
        self.stock('ser-1', 'lot-a')
        self.install('ser-1', 'lot-a', 'cluster-a', 5)
        self.hw({'event_id': 'ro', 'op': 'repair', 'id': 'ro-1', 'serial': 'ser-1', 'at': 6})
        # 维修单已开立不等于故障件离库或被供应商收到
        gap = self.hw({'event_id': 'ack-early', 'op': 'vendor_ack', 'id': 'ack-0', 'serial': 'ser-1',
                       'repair': 'ro-1', 'at': 7})
        self.assertEqual(gap['decision'], 'handoff_gap')
        self.assertEqual(gap['missing'], 'outbound')
        self.assertEqual(self.hw({'event_id': 'leg-early', 'op': 'transport', 'id': 'leg-0', 'serial': 'ser-1',
                                  'leg': 'outbound', 'from': 'tech', 'to': 'carrier', 'integrity': 's0',
                                  'at': 7})['decision'], 'unit_not_removed')
        self.hw({'event_id': 'rm', 'op': 'remove', 'serial': 'ser-1', 'custodian': 'tech', 'witness': 'auditor',
                 'key_disposition': 'sealed', 'at': 8})
        # 跳段、缺完整性证据、时间倒退均被拒
        skip = self.hw({'event_id': 'leg-skip', 'op': 'transport', 'id': 'leg-x', 'serial': 'ser-1',
                        'leg': 'center_receipt', 'from': 'depot', 'to': 'lab', 'integrity': 's3', 'at': 9})
        self.assertEqual(skip['decision'], 'handoff_gap')
        self.assertEqual(skip['missing'], 'outbound')
        self.assertEqual(self.hw({'event_id': 'leg-noint', 'op': 'transport', 'id': 'leg-y', 'serial': 'ser-1',
                                  'leg': 'outbound', 'from': 'tech', 'to': 'carrier', 'integrity': '',
                                  'at': 9})['decision'], 'integrity_missing')
        self.assertEqual(self.hw({'event_id': 'leg-time', 'op': 'transport', 'id': 'leg-z', 'serial': 'ser-1',
                                  'leg': 'outbound', 'from': 'tech', 'to': 'carrier', 'integrity': 's1',
                                  'at': 7})['decision'], 'time_regression')
        # 逐段交接:出库、承运、维修中心收件
        self.assertEqual(self.hw({'event_id': 'leg-1', 'op': 'transport', 'id': 'leg-1', 'serial': 'ser-1',
                                  'leg': 'outbound', 'from': 'tech', 'to': 'carrier', 'integrity': 'seal-1',
                                  'at': 9})['decision'], 'leg_recorded')
        mid = self.hw({'event_id': 'leg-mid', 'op': 'transport', 'id': 'leg-m', 'serial': 'ser-1',
                       'leg': 'center_receipt', 'from': 'depot', 'to': 'lab', 'integrity': 's3', 'at': 10})
        self.assertEqual(mid['decision'], 'handoff_gap')
        self.assertEqual(mid['missing'], 'carrier')
        self.assertEqual(self.hw({'event_id': 'leg-2', 'op': 'transport', 'id': 'leg-2', 'serial': 'ser-1',
                                  'leg': 'carrier', 'from': 'carrier', 'to': 'depot', 'integrity': 'seal-2',
                                  'at': 10})['decision'], 'leg_recorded')
        self.assertEqual(self.hw({'event_id': 'leg-3', 'op': 'transport', 'id': 'leg-3', 'serial': 'ser-1',
                                  'leg': 'center_receipt', 'from': 'depot', 'to': 'lab', 'integrity': 'seal-3',
                                  'at': 11})['decision'], 'leg_recorded')
        self.assertEqual(self.hw({'event_id': 'leg-dup', 'op': 'transport', 'id': 'leg-d', 'serial': 'ser-1',
                                  'leg': 'outbound', 'from': 'a', 'to': 'b', 'integrity': 's', 'at': 12})['decision'],
                         'leg_already_recorded')
        # 交接链完整才认定供应商收到故障件
        self.assertEqual(self.hw({'event_id': 'ack', 'op': 'vendor_ack', 'id': 'ack-1', 'serial': 'ser-1',
                                  'repair': 'ro-1', 'at': 12})['decision'], 'vendor_acknowledged')
        self.assertTrue(replay(self.path)['verified'])

    def test_replacement_compatibility_and_acceptance(self):
        self.hw({'event_id': 'lot-bad', 'op': 'lot', 'id': 'lot-bad', 'vendor': 'v', 'model': 'crypto-c7',
                 'revision': 3})
        self.hw({'event_id': 'lot-good', 'op': 'lot', 'id': 'lot-good', 'vendor': 'v', 'model': 'crypto-c7',
                 'revision': 4})
        self.hw({'event_id': 'lot-other', 'op': 'lot', 'id': 'lot-other', 'vendor': 'v', 'model': 'crypto-c5',
                 'revision': 1})
        self.stock('card-1', 'lot-bad')
        self.install('card-1', 'lot-bad', 'cluster-a', 5)
        self.hw({'event_id': 'ro', 'op': 'repair', 'id': 'ro-1', 'serial': 'card-1', 'at': 6})
        self.hw({'event_id': 'rm', 'op': 'remove', 'serial': 'card-1', 'custodian': 'tech', 'witness': 'auditor',
                 'key_disposition': 'wiped', 'at': 7})
        # 未签章、未入库、故障件自我替换、型号不兼容、缺质量证明均被拒
        self.assertEqual(self.hw({'event_id': 'acc-unatt', 'op': 'accept_replacement', 'id': 'a1', 'repair': 'ro-1',
                                  'serial': 'ghost', 'cert': 'qc', 'at': 8})['decision'], 'unattested_serial')
        self.hw({'event_id': 'att-nostock', 'op': 'attest', 'serial': 'card-2', 'lot': 'lot-good', 'seal': 's',
                 'batch': 'b', 'at': 1})
        self.assertEqual(self.hw({'event_id': 'acc-nostock', 'op': 'accept_replacement', 'id': 'a2', 'repair': 'ro-1',
                                  'serial': 'card-2', 'cert': 'qc', 'at': 8})['decision'], 'not_in_stock')
        self.assertEqual(self.hw({'event_id': 'acc-self', 'op': 'accept_replacement', 'id': 'a3', 'repair': 'ro-1',
                                  'serial': 'card-1', 'cert': 'qc', 'at': 8})['decision'], 'serial_reuse')
        self.stock('card-3', 'lot-other')
        self.assertEqual(self.hw({'event_id': 'acc-incompat', 'op': 'accept_replacement', 'id': 'a4', 'repair': 'ro-1',
                                  'serial': 'card-3', 'cert': 'qc', 'at': 8})['decision'], 'model_incompatible')
        self.stock('card-4', 'lot-good')
        self.assertEqual(self.hw({'event_id': 'acc-nocert', 'op': 'accept_replacement', 'id': 'a5', 'repair': 'ro-1',
                                  'serial': 'card-4', 'cert': '', 'at': 8})['decision'], 'quality_cert_missing')
        self.assertEqual(self.hw({'event_id': 'acc-ok', 'op': 'accept_replacement', 'id': 'a6', 'repair': 'ro-1',
                                  'serial': 'card-4', 'cert': 'qc-1', 'at': 8})['decision'], 'replacement_accepted')
        self.assertEqual(self.hw({'event_id': 'acc-again', 'op': 'accept_replacement', 'id': 'a7', 'repair': 'ro-1',
                                  'serial': 'card-4', 'cert': 'qc-1', 'at': 9})['decision'], 'already_accepted')
        # 替换件未实际安装到位不得恢复服务
        self.assertEqual(self.hw({'event_id': 'rs-early', 'op': 'restore', 'repair': 'ro-1', 'at': 9})['decision'],
                         'replacement_not_installed')
        self.install('card-4', 'lot-good', 'cluster-a', 9)
        done = self.hw({'event_id': 'rs', 'op': 'restore', 'repair': 'ro-1', 'at': 10})
        self.assertEqual(done['decision'], 'service_restored')
        self.assertEqual(done['restored_at'], 10)
        self.assertEqual(self.hw({'event_id': 'rs-again', 'op': 'restore', 'repair': 'ro-1', 'at': 11})['decision'],
                         'repair_not_open')
        # 故障件未拆卸、替换件未验收同样不得恢复
        self.stock('card-9', 'lot-bad')
        self.install('card-9', 'lot-bad', 'cluster-a', 9)
        self.hw({'event_id': 'ro-9', 'op': 'repair', 'id': 'ro-9', 'serial': 'card-9', 'at': 9})
        self.assertEqual(self.hw({'event_id': 'rs-9', 'op': 'restore', 'repair': 'ro-9', 'at': 10})['decision'],
                         'faulty_part_still_installed')
        self.hw({'event_id': 'rm-9', 'op': 'remove', 'serial': 'card-9', 'custodian': 'tech', 'witness': 'auditor',
                 'key_disposition': 'wiped', 'at': 10})
        self.assertEqual(self.hw({'event_id': 'rs-9b', 'op': 'restore', 'repair': 'ro-9', 'at': 11})['decision'],
                         'replacement_not_accepted')
        self.assertTrue(replay(self.path)['verified'])

    def test_recalled_lot_blocks_install_but_preserves_history(self):
        self.hw({'event_id': 'lot', 'op': 'lot', 'id': 'lot-r', 'vendor': 'v', 'model': 'm', 'revision': 1})
        self.stock('ser-1', 'lot-r')
        self.install('ser-1', 'lot-r', 'cluster-a', 5)
        self.assertEqual(self.hw({'event_id': 'rc-nolot', 'op': 'recall', 'id': 'rc-0', 'lot': 'missing',
                                  'failure_mode': 'fm', 'announced_at': 6})['decision'], 'lot_unavailable')
        self.assertEqual(self.hw({'event_id': 'rc-nofm', 'op': 'recall', 'id': 'rc-1', 'lot': 'lot-r',
                                  'failure_mode': '', 'announced_at': 6})['decision'], 'invalid_recall')
        pub = self.hw({'event_id': 'rc', 'op': 'recall', 'id': 'rc-2', 'lot': 'lot-r',
                       'failure_mode': 'substrate_delamination', 'announced_at': 6})
        self.assertEqual(pub['decision'], 'recall_published')
        self.assertEqual(pub['affected'], 1)
        # 召回批次的新安装被阻断
        self.stock('ser-2', 'lot-r')
        self.assertEqual(self.install('ser-2', 'lot-r', 'cluster-b', 7)['decision'], 'lot_recalled')
        # 历史安装事实与批次状态不被召回抹除
        self.assertEqual(one(self.db, 'SELECT state FROM hardware_lots WHERE id=?', ('lot-r',))['state'], 'accepted')
        self.assertEqual(one(self.db, 'SELECT state FROM hardware_units WHERE serial=?', ('ser-1',))['state'],
                         'installed')
        notice = one(self.db, 'SELECT failure_mode, state FROM recall_notices WHERE id=?', ('rc-2',))
        self.assertEqual(notice['failure_mode'], 'substrate_delamination')
        self.assertEqual(notice['state'], 'active')
        self.assertTrue(replay(self.path)['verified'])

    def test_cluster_health_waits_for_physical_acceptance(self):
        self.hw({'event_id': 'lot-bad', 'op': 'lot', 'id': 'lot-bad', 'vendor': 'v', 'model': 'crypto-c7',
                 'revision': 3})
        self.hw({'event_id': 'lot-good', 'op': 'lot', 'id': 'lot-good', 'vendor': 'v', 'model': 'crypto-c7',
                 'revision': 4})
        self.stock('card-1', 'lot-bad')
        self.stock('card-2', 'lot-good')
        self.install('card-1', 'lot-bad', 'cluster-c9', 5)
        self.hw({'event_id': 'ro', 'op': 'repair', 'id': 'ro-1', 'serial': 'card-1', 'at': 6})
        observe = {'op': 'observe', 'id': 'cluster-c9', 'region': 'east', 'domain': 'rack-9', 'cpu': 8, 'mem': 16}
        # 物理验收未完成,健康观测被拒;降格(不健康)观测允许
        blocked = seat.run(self.db, dict(observe, event_id='obs-1', generation=1, healthy=True))
        self.assertEqual(blocked['decision'], 'physical_acceptance_pending')
        degraded = seat.run(self.db, dict(observe, event_id='obs-2', generation=1, healthy=False))
        self.assertEqual(degraded['decision'], 'observed')
        # 完成拆卸、替换件验收、安装与服务恢复后放行
        self.hw({'event_id': 'rm', 'op': 'remove', 'serial': 'card-1', 'custodian': 'tech', 'witness': 'auditor',
                 'key_disposition': 'sealed', 'at': 7})
        self.hw({'event_id': 'acc', 'op': 'accept_replacement', 'id': 'a1', 'repair': 'ro-1', 'serial': 'card-2',
                 'cert': 'qc', 'at': 8})
        self.install('card-2', 'lot-good', 'cluster-c9', 9)
        self.hw({'event_id': 'rs', 'op': 'restore', 'repair': 'ro-1', 'at': 10})
        healthy = seat.run(self.db, dict(observe, event_id='obs-3', generation=2, healthy=True))
        self.assertEqual(healthy['decision'], 'observed')
        self.assertTrue(replay(self.path)['verified'])

    def test_settlement_paths_and_late_vendor_ack(self):
        self.hw({'event_id': 'p-lot-bad', 'op': 'lot', 'id': 'lot-bad', 'vendor': 'vq', 'model': 'crypto-c7',
                 'revision': 3})
        self.hw({'event_id': 'p-lot-good', 'op': 'lot', 'id': 'lot-good', 'vendor': 'vq', 'model': 'crypto-c7',
                 'revision': 4})
        # 同一召回批次的两张加密卡分处两座机房
        for serial, cluster in (('card-a', 'cluster-a'), ('card-b', 'cluster-b')):
            self.stock(serial, 'lot-bad', seal=f'seal-{serial}', batch='2026-W11', at=10)
            self.install(serial, 'lot-bad', cluster, 12)
        pub = self.hw({'event_id': 'p-recall', 'op': 'recall', 'id': 'recall-1', 'lot': 'lot-bad',
                       'failure_mode': 'substrate_delamination', 'announced_at': 100})
        self.assertEqual(pub['affected'], 2)
        self.hw({'event_id': 'p-repair-a', 'op': 'repair', 'id': 'ro-a', 'serial': 'card-a', 'at': 110})
        self.hw({'event_id': 'p-repair-b', 'op': 'repair', 'id': 'ro-b', 'serial': 'card-b', 'at': 110})
        self.hw({'event_id': 'p-rm-a', 'op': 'remove', 'serial': 'card-a', 'custodian': 'tech-a',
                 'witness': 'auditor', 'key_disposition': 'wiped', 'at': 120})
        self.hw({'event_id': 'p-rm-b', 'op': 'remove', 'serial': 'card-b', 'custodian': 'tech-b',
                 'witness': 'auditor', 'key_disposition': 'sealed', 'at': 120})
        for serial in ('spare-a', 'spare-b'):
            self.stock(serial, 'lot-good', seal=f'seal-{serial}', batch='2026-W30', at=10)
        # 服务未恢复不得结算
        self.assertEqual(self.hw({'event_id': 'p-st-early', 'op': 'settle', 'id': 'st-0', 'repair': 'ro-a',
                                  'path': 'onsite_spare', 'repair_cost': 800, 'warranty': True,
                                  'downtime_rate': 10, 'at': 121})['decision'], 'service_not_restored')
        # 路径一:现场备用卡,快速恢复,旧卡密封送检
        self.hw({'event_id': 'p-acc-a', 'op': 'accept_replacement', 'id': 'acc-a', 'repair': 'ro-a',
                 'serial': 'spare-a', 'cert': 'qc-a', 'at': 130})
        self.install('spare-a', 'lot-good', 'cluster-a', 131)
        restored_a = self.hw({'event_id': 'p-restore-a', 'op': 'restore', 'repair': 'ro-a', 'at': 150})
        self.hw({'event_id': 'p-out-a', 'op': 'transport', 'id': 'leg-a1', 'serial': 'card-a', 'leg': 'outbound',
                 'from': 'tech-a', 'to': 'carrier', 'integrity': 's1', 'at': 160})
        self.hw({'event_id': 'p-car-a', 'op': 'transport', 'id': 'leg-a2', 'serial': 'card-a', 'leg': 'carrier',
                 'from': 'carrier', 'to': 'depot', 'integrity': 's2', 'at': 170})
        self.hw({'event_id': 'p-rcv-a', 'op': 'transport', 'id': 'leg-a3', 'serial': 'card-a',
                 'leg': 'center_receipt', 'from': 'depot', 'to': 'vendor-lab', 'integrity': 's3', 'at': 400})
        # 路径二:整机送供应商实验室,恢复更慢
        self.hw({'event_id': 'p-out-b', 'op': 'transport', 'id': 'leg-b1', 'serial': 'card-b', 'leg': 'outbound',
                 'from': 'tech-b', 'to': 'carrier', 'integrity': 's1', 'at': 130})
        self.hw({'event_id': 'p-car-b', 'op': 'transport', 'id': 'leg-b2', 'serial': 'card-b', 'leg': 'carrier',
                 'from': 'carrier', 'to': 'depot', 'integrity': 's2', 'at': 140})
        self.hw({'event_id': 'p-rcv-b', 'op': 'transport', 'id': 'leg-b3', 'serial': 'card-b',
                 'leg': 'center_receipt', 'from': 'depot', 'to': 'vendor-lab', 'integrity': 's3', 'at': 150})
        self.hw({'event_id': 'p-acc-b', 'op': 'accept_replacement', 'id': 'acc-b', 'repair': 'ro-b',
                 'serial': 'spare-b', 'cert': 'qc-b', 'at': 300})
        self.install('spare-b', 'lot-good', 'cluster-b', 301)
        restored_b = self.hw({'event_id': 'p-restore-b', 'op': 'restore', 'repair': 'ro-b', 'at': 320})
        # 整机送修路径在返还段缺失时不得结算
        gap = self.hw({'event_id': 'p-st-b-early', 'op': 'settle', 'id': 'st-b0', 'repair': 'ro-b',
                       'path': 'return_to_vendor', 'repair_cost': 800, 'warranty': True, 'downtime_rate': 10,
                       'at': 321})
        self.assertEqual(gap['decision'], 'handoff_gap')
        self.assertEqual(gap['missing'], 'return')
        self.hw({'event_id': 'p-ret-b', 'op': 'transport', 'id': 'leg-b4', 'serial': 'card-b', 'leg': 'return',
                 'from': 'vendor-lab', 'to': 'keeper', 'integrity': 's4', 'at': 330})
        # 非法结算路径
        self.assertEqual(self.hw({'event_id': 'p-st-bad', 'op': 'settle', 'id': 'st-x', 'repair': 'ro-a',
                                  'path': 'air_freight', 'repair_cost': 800, 'warranty': True,
                                  'downtime_rate': 10, 'at': 500})['decision'], 'invalid_settlement_path')
        # 供应商回执迟到(远晚于恢复时刻)
        self.assertEqual(self.hw({'event_id': 'p-ack-a', 'op': 'vendor_ack', 'id': 'ack-a', 'serial': 'card-a',
                                  'repair': 'ro-a', 'at': 900, 'note': 'late'})['decision'], 'vendor_acknowledged')
        self.assertEqual(self.hw({'event_id': 'p-ack-b', 'op': 'vendor_ack', 'id': 'ack-b', 'serial': 'card-b',
                                  'repair': 'ro-b', 'at': 340})['decision'], 'vendor_acknowledged')
        settle_a = self.hw({'event_id': 'p-st-a', 'op': 'settle', 'id': 'st-a', 'repair': 'ro-a',
                            'path': 'onsite_spare', 'repair_cost': 800, 'warranty': True, 'downtime_rate': 10,
                            'spare_cost': 50, 'at': 901})
        settle_b = self.hw({'event_id': 'p-st-b', 'op': 'settle', 'id': 'st-b', 'repair': 'ro-b',
                            'path': 'return_to_vendor', 'repair_cost': 800, 'warranty': True, 'downtime_rate': 10,
                            'at': 902})
        # 同一召回通知下,两条路径的客户可用时段与供应商应付金额不同
        self.assertEqual(restored_a['restored_at'], 150)
        self.assertEqual(restored_b['restored_at'], 320)
        self.assertEqual(settle_a['downtime_hours'], 30)
        self.assertEqual(settle_b['downtime_hours'], 200)
        self.assertEqual(settle_a['vendor_payable'], 800)
        self.assertEqual(settle_b['vendor_payable'], 2800)
        self.assertEqual(settle_a['platform_borne'], 350)
        self.assertEqual(settle_b['platform_borne'], 0)
        self.assertEqual(settle_a['restored_at'], 150)
        # 重复结算被拒
        self.assertEqual(self.hw({'event_id': 'p-st-a2', 'op': 'settle', 'id': 'st-a2', 'repair': 'ro-a',
                                  'path': 'onsite_spare', 'repair_cost': 800, 'warranty': True,
                                  'downtime_rate': 10, 'at': 903})['decision'], 'already_settled')
        # 迟到的供应商回执不倒填恢复时刻
        self.assertEqual(one(self.db, 'SELECT restored_at FROM repair_orders WHERE id=?', ('ro-a',))['restored_at'],
                         150)
        self.assertEqual(one(self.db, 'SELECT restored_at FROM repair_orders WHERE id=?', ('ro-b',))['restored_at'],
                         320)
        self.assertTrue(replay(self.path)['verified'])


if __name__ == '__main__':
    unittest.main()
