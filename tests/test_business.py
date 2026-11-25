import json,tempfile,unittest
from pathlib import Path
from cloudcourt import intent,seat,evac
from cloudcourt.store import connect,replay,one

def run_fixture(db,name,module):
    return [module.run(db,x) for x in json.loads((Path('fixtures')/name).read_text(encoding='utf-8'))]

class Business(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=str(Path(self.tmp.name)/'state.sqlite')
        self.db=connect(self.path)
    def tearDown(self):
        self.db.close();self.tmp.cleanup()
    def prepared(self):
        run_fixture(self.db,'intent-normal.json',intent)
        run_fixture(self.db,'seat-normal.json',seat)
        intent.run(self.db,{'event_id':'primary','op':'assign_primary','intent':'work-a','cluster':'cluster-a','grant':'grant-a','at':21})

    def test_parallel_capacity_and_disaster_domain(self):
        self.prepared()
        self.assertEqual(seat.run(self.db,{'event_id':'domain','op':'reserve','id':'l3','intent':'work-a','cluster':'cluster-c','role':'backup','cpu':2,'mem':2,'expires':80})['decision'],'same_disaster_domain')
        intent.run(self.db,{'event_id':'other','op':'claim','id':'work-other','tenant':'tenant-a','region':'east','role':'primary'})
        self.assertEqual(seat.run(self.db,{'event_id':'capacity','op':'reserve','id':'l4','intent':'work-other','cluster':'cluster-a','role':'primary','cpu':6,'mem':4,'expires':80})['decision'],'capacity_exhausted')
        self.assertTrue(replay(self.path)['verified'])

    def test_partial_handoff_keeps_old_primary(self):
        self.prepared()
        evac.run(self.db,{'event_id':'incident','op':'incident','id':'inc','cluster':'cluster-a','severity':4,'generation':1})
        self.assertEqual(evac.run(self.db,{'event_id':'plan','op':'plan','id':'move','intent':'work-a','source':'cluster-a','target':'cluster-b','incident':'inc'})['decision'],'planned')
        self.assertEqual(evac.run(self.db,{'event_id':'early','op':'handoff','plan':'move'})['decision'],'partial_handoff')
        self.assertEqual(one(self.db,'SELECT primary_cluster FROM workload_intents WHERE id=?',('work-a',))['primary_cluster'],'cluster-a')
        evac.run(self.db,{'event_id':'fence','op':'receipt','id':'fence','plan':'move','kind':'source_fenced','cluster':'cluster-a','generation':1})
        self.assertEqual(evac.run(self.db,{'event_id':'half','op':'handoff','plan':'move'})['missing'],'target_ready')
        evac.run(self.db,{'event_id':'ready','op':'receipt','id':'ready','plan':'move','kind':'target_ready','cluster':'cluster-b','generation':1})
        self.assertEqual(evac.run(self.db,{'event_id':'finish','op':'handoff','plan':'move'})['decision'],'transferred')
        self.assertEqual(one(self.db,'SELECT primary_cluster FROM workload_intents WHERE id=?',('work-a',))['primary_cluster'],'cluster-b')
        self.assertTrue(replay(self.path)['verified'])

    def test_target_rebuild_invalidates_receipts(self):
        self.prepared()
        evac.run(self.db,{'event_id':'incident','op':'incident','id':'inc','cluster':'cluster-a','severity':4,'generation':1})
        evac.run(self.db,{'event_id':'plan','op':'plan','id':'move','intent':'work-a','source':'cluster-a','target':'cluster-b','incident':'inc'})
        evac.run(self.db,{'event_id':'fence','op':'receipt','id':'fence','plan':'move','kind':'source_fenced','cluster':'cluster-a','generation':1})
        evac.run(self.db,{'event_id':'ready','op':'receipt','id':'ready','plan':'move','kind':'target_ready','cluster':'cluster-b','generation':1})
        seat.run(self.db,{'event_id':'rebuild','op':'observe','id':'cluster-b','region':'east','domain':'rack-b','cpu':8,'mem':16,'generation':2,'healthy':True})
        self.assertEqual(evac.run(self.db,{'event_id':'finish','op':'handoff','plan':'move'})['decision'],'stale_handoff_evidence')
        self.assertEqual(one(self.db,'SELECT primary_cluster FROM workload_intents WHERE id=?',('work-a',))['primary_cluster'],'cluster-a')
        self.assertEqual(evac.run(self.db,{'event_id':'abort','op':'abort','id':'recover-a','plan':'move','reason':'target_rebuilt'})['decision'],'aborted_for_replan')
        seat.run(self.db,{'event_id':'fourth','op':'observe','id':'cluster-d','region':'east','domain':'rack-d','cpu':8,'mem':16,'generation':1,'healthy':True})
        self.assertEqual(seat.run(self.db,{'event_id':'new-seat','op':'reserve','id':'lease-d','intent':'work-a','cluster':'cluster-d','role':'backup','cpu':3,'mem':4,'expires':80})['decision'],'reserved')
        seat.run(self.db,{'event_id':'occupy-new','op':'occupy','id':'lease-d','at':30})
        self.assertEqual(evac.run(self.db,{'event_id':'replan','op':'plan','id':'move-d','intent':'work-a','source':'cluster-a','target':'cluster-d','incident':'inc'})['decision'],'planned')
        evac.run(self.db,{'event_id':'fence-new','op':'receipt','id':'fence-new','plan':'move-d','kind':'source_fenced','cluster':'cluster-a','generation':1})
        evac.run(self.db,{'event_id':'ready-new','op':'receipt','id':'ready-new','plan':'move-d','kind':'target_ready','cluster':'cluster-d','generation':1})
        self.assertEqual(evac.run(self.db,{'event_id':'finish-new','op':'handoff','plan':'move-d'})['decision'],'transferred')
        self.assertEqual(one(self.db,'SELECT state FROM recovery_cases WHERE id=?',('recover-a',))['state'],'recovered')
        self.assertTrue(replay(self.path)['verified'])

    def test_contract_revoke_requires_no_live_seat(self):
        self.prepared()
        self.assertEqual(intent.run(self.db,{'event_id':'revoke','op':'revoke','intent':'work-a','revision':5})['decision'],'live_seat_blocks_revoke')
        self.assertEqual(one(self.db,'SELECT status FROM workload_intents WHERE id=?',('work-a',))['status'],'active')
        self.assertTrue(replay(self.path)['verified'])

    def test_duplicate_business_id_is_controlled_and_atomic(self):
        self.prepared()
        seat.run(self.db,{'event_id':'new-cluster','op':'observe','id':'cluster-d','region':'east','domain':'rack-d','cpu':8,'mem':8,'generation':1,'healthy':True})
        result=seat.run(self.db,{'event_id':'new-event','op':'reserve','id':'lease-a','intent':'work-a','cluster':'cluster-d','role':'other','cpu':1,'mem':1,'expires':80})
        self.assertEqual(result['decision'],'business_identity_conflict')
        self.assertEqual(one(self.db,'SELECT COUNT(*) n FROM capacity_claims WHERE id=?',('lease-a',))['n'],1)
