"""Settlement tests for shared cluster meters.

Acceptance map (see task brief):

* raw readings keep meter identity / coverage / surveyor / revision / evidence;
* duplicate uploads and out-of-order corrections are controlled and versioned;
* seat time windows separate occupied production, warm-preheat standby and
  failover replication attribution;
* the November C7 meter (certified 100) settles two pre-signed methods to
  different per-customer carbon / excess / payable results;
* shares always reconcile to the trusted total, including cross-period
  migration legs and cross-cluster workload usage;
* missing occupancy evidence stays a pending amount with a proof deadline;
* signed bills are never overwritten and a paid old bill survives correction.
"""

import json
import tempfile
import unittest
from pathlib import Path

from cloudcourt import MODULES, carbon, intent, seat
from cloudcourt.store import connect, one, many, replay

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / 'fixtures'
SHARED_FIXTURE = FIXTURES / 'carbon-shared-meter.json'


def load_envelope_fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding='utf-8'))


def run_envelopes(db, commands):
    results = []
    by_event = {}
    for command in commands:
        if 'subsystem' in command:
            module = MODULES[command['subsystem']]
            payload = command['payload']
        else:
            module = carbon
            payload = command
        result = module.run(db, payload)
        results.append(result)
        by_event[payload.get('event_id')] = result
    return results, by_event


def put(db, event_id, **payload):
    payload['event_id'] = event_id
    return carbon.run(db, payload)


def seed_c7(db, *, total=100, reading_revision=1, standby_bearer='standby_owner',
            with_windows=True, include_replication=True):
    """The shared November C7 originals: intent, seat, meter, orders, terms."""
    intent.run(db, {'event_id': 'rule-a', 'op': 'rule', 'tenant': 'tenant-a',
                    'regions': ['east'], 'revision': 1})
    intent.run(db, {'event_id': 'rule-b', 'op': 'rule', 'tenant': 'tenant-b',
                    'regions': ['east'], 'revision': 1})
    intent.run(db, {'event_id': 'intent-a', 'op': 'claim', 'id': 'work-a',
                    'tenant': 'tenant-a', 'region': 'east', 'role': 'production'})
    intent.run(db, {'event_id': 'intent-b', 'op': 'claim', 'id': 'work-b',
                    'tenant': 'tenant-b', 'region': 'east', 'role': 'hot_standby'})
    seat.run(db, {'event_id': 'obs-c7', 'op': 'observe', 'id': 'cluster-c7',
                  'region': 'east', 'domain': 'rack-c7', 'cpu': 16, 'mem': 32,
                  'generation': 1, 'healthy': True})
    seat.run(db, {'event_id': 'obs-c9', 'op': 'observe', 'id': 'cluster-c9',
                  'region': 'east', 'domain': 'rack-c9', 'cpu': 16, 'mem': 32,
                  'generation': 1, 'healthy': True})
    seat.run(db, {'event_id': 'res-a', 'op': 'reserve', 'id': 'lease-a-c7',
                  'intent': 'work-a', 'cluster': 'cluster-c7', 'role': 'primary',
                  'cpu': 4, 'mem': 8, 'expires': 200000})
    seat.run(db, {'event_id': 'occ-a', 'op': 'occupy', 'id': 'lease-a-c7', 'at': 100})
    seat.run(db, {'event_id': 'res-b', 'op': 'reserve', 'id': 'lease-b-c7',
                  'intent': 'work-b', 'cluster': 'cluster-c7', 'role': 'backup',
                  'cpu': 6, 'mem': 12, 'expires': 200000})
    seat.run(db, {'event_id': 'occ-b', 'op': 'occupy', 'id': 'lease-b-c7', 'at': 100})
    seat.run(db, {'event_id': 'res-a9', 'op': 'reserve', 'id': 'lease-a-c9',
                  'intent': 'work-a', 'cluster': 'cluster-c9', 'role': 'backup',
                  'cpu': 4, 'mem': 8, 'expires': 200000})
    seat.run(db, {'event_id': 'occ-a9', 'op': 'occupy', 'id': 'lease-a-c9', 'at': 101})

    put(db, 'meter', op='meter', id='meter-c7', cluster='cluster-c7',
        certified=True, evidence='meter-seal-77')
    put(db, 'order-a', op='order', id='order-a', tenant='tenant-a', period='2026-11',
        ceiling=45, meter='meter-c7', unit_rate=1, penalty_rate=2)
    put(db, 'order-b', op='order', id='order-b', tenant='tenant-b', period='2026-11',
        ceiling=20, meter='meter-c7', unit_rate=1, penalty_rate=2)
    put(db, 'reading', op='reading', id='reading-c7', meter='meter-c7',
        cluster='cluster-c7', period='2026-11', units=total, revision=reading_revision,
        start='2026-11-01T00:00', end='2026-12-01T00:00', surveyor='surveyor-north',
        evidence='cred://meter-c7/2026-11/certified-r%d' % reading_revision)
    put(db, 'agr-hours', op='agreement', id='agr-hours', meter='meter-c7',
        period='2026-11', method='run_hours', standby_bearer='standby_owner',
        parties=['tenant-a', 'tenant-b'], confirmed_by=['tenant-a', 'tenant-b'], signed=True)
    put(db, 'agr-capacity', op='agreement', id='agr-capacity', meter='meter-c7',
        period='2026-11', method='guaranteed_capacity', standby_bearer='standby_owner',
        parties=['tenant-a', 'tenant-b'], confirmed_by=['tenant-a', 'tenant-b'], signed=True)
    put(db, 'neg-hours', op='negotiation', id='neg-hours', agreement='agr-hours',
        confirmed_by=['tenant-a', 'tenant-b'], at='2026-11-02T10:00')
    put(db, 'neg-capacity', op='negotiation', id='neg-capacity', agreement='agr-capacity',
        confirmed_by=['tenant-a', 'tenant-b'], at='2026-11-02T10:05')
    put(db, 'neg-host', op='negotiation', id='neg-host', agreement='agr-hours',
        standby_bearer='host', confirmed_by=['tenant-a', 'tenant-b'], at='2026-11-02T10:10')

    if with_windows:
        # A production occupied 27 full days = 648h = 38880 minutes.
        put(db, 'win-a-occ', op='load_window', id='lw-a-occ', intent='work-a',
            lease='lease-a-c7', cluster='cluster-c7', kind='occupied',
            start='2026-11-01T00:00', end='2026-11-28T00:00',
            evidence='seat-export#a-occ')
        # Source replication crosses the period boundary: 72h inside November.
        if include_replication:
            put(db, 'win-a-rep', op='load_window', id='lw-a-rep-c7', intent='work-a',
                lease='lease-a-c7', cluster='cluster-c7', kind='replication',
                start='2026-11-28T00:00', end='2026-12-01T12:00',
                peer_cluster='cluster-c9', migration='move-a',
                evidence='rep-gw#source')
            # Target feed: 24h inside November on cluster-c9.
            put(db, 'win-a-rep9', op='load_window', id='lw-a-rep-c9', intent='work-a',
                lease='lease-a-c9', cluster='cluster-c9', kind='replication',
                start='2026-11-30T00:00', end='2026-12-01T12:00',
                peer_cluster='cluster-c7', migration='move-a', evidence='rep-gw#target')
        # B hot standby preheats for 10 days = 240h = 14400 minutes.
        put(db, 'win-b-pre', op='load_window', id='lw-b-pre', intent='work-b',
            lease='lease-b-c7', cluster='cluster-c7', kind='preheat',
            start='2026-11-01T00:00', end='2026-11-11T00:00',
            evidence='warm-ctrl#b')


class SharedMeterFixtureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'shared.sqlite')
        self.db = connect(self.path)
        self.commands = load_envelope_fixture('carbon-shared-meter.json')
        self.results, self.by_event = run_envelopes(self.db, self.commands)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_fixture_uses_known_operations_and_replays(self):
        bad = [r for r in self.results
               if r.get('decision') in ('unknown_operation', 'event_identity_conflict')]
        self.assertEqual(bad, [])
        conflicts = [r for r in self.results if r.get('decision') == 'business_identity_conflict']
        self.assertEqual(conflicts, [])
        self.assertTrue(replay(self.path)['verified'])

    def test_raw_reading_identity_evidence_and_versions(self):
        rows = many(self.db, 'SELECT * FROM power_readings WHERE id=? ORDER BY revision',
                    ('reading-c7',))
        self.assertEqual([r['revision'] for r in rows], [1, 2])
        first, second = rows
        self.assertEqual(first['units'], 100)       # original evidence preserved
        self.assertEqual(first['state'], 'superseded')
        self.assertEqual(second['units'], 96)
        self.assertEqual(second['state'], 'current')
        for row in rows:
            self.assertEqual(row['meter_id'], 'meter-c7')
            self.assertEqual(row['surveyor'], 'surveyor-north')
            self.assertTrue(row['evidence'].startswith('cred://meter-c7/2026-11/'))
            self.assertEqual(row['window_start'], '2026-11-01T00:00')
            self.assertEqual(row['window_end'], '2026-12-01T00:00')
        self.assertEqual(self.by_event['c7-reading-dup-upload']['decision'], 'duplicate_reading')
        self.assertEqual(len(rows), 2)  # duplicate upload added nothing

    def test_two_confirmed_methods_produce_different_bills(self):
        hours_a = one(self.db, 'SELECT * FROM carbon_bills WHERE id=?',
                      ('run-hours:tenant-a',))
        hours_b = one(self.db, 'SELECT * FROM carbon_bills WHERE id=?',
                      ('run-hours:tenant-b',))
        cap_a = one(self.db, 'SELECT * FROM carbon_bills WHERE id=?',
                    ('run-capacity:tenant-a',))
        cap_b = one(self.db, 'SELECT * FROM carbon_bills WHERE id=?',
                    ('run-capacity:tenant-b',))

        # Running-hours clause: verified weights 43200:14400 == 3:1 -> 75/25.
        self.assertEqual((hours_a['allocated_units'], hours_b['allocated_units']), (75, 25))
        self.assertEqual(hours_a['weight'], 43200)
        self.assertEqual(hours_b['weight'], 14400)
        self.assertEqual(hours_a['excess_units'], 30)   # 75 - ceiling 45
        self.assertEqual(hours_a['penalty_amount'], 60)
        self.assertEqual(hours_a['total_due'], 135)     # 75 energy + 60 penalty
        self.assertEqual(hours_b['excess_units'], 5)    # 25 - ceiling 20
        self.assertEqual(hours_b['penalty_amount'], 10)
        self.assertEqual(hours_b['total_due'], 35)

        # Guaranteed-capacity clause: reserved cpu 4 vs 6 -> 40/60.
        self.assertEqual((cap_a['allocated_units'], cap_b['allocated_units']), (40, 60))
        self.assertEqual(cap_a['excess_units'], 0)      # 40 <= 45
        self.assertEqual(cap_a['total_due'], 40)
        self.assertEqual(cap_b['excess_units'], 40)     # 60 - 20
        self.assertEqual(cap_b['penalty_amount'], 80)
        self.assertEqual(cap_b['total_due'], 140)

        # Same raw meter total, shares reconcile exactly under both clauses.
        self.assertEqual(hours_a['allocated_units'] + hours_b['allocated_units'], 100)
        self.assertEqual(cap_a['allocated_units'] + cap_b['allocated_units'], 100)

        # The two confirmed outcomes genuinely differ.
        self.assertNotEqual(hours_a['total_due'], cap_a['total_due'])
        self.assertNotEqual(hours_b['total_due'], cap_b['total_due'])
        self.assertNotEqual(hours_a['excess_units'], cap_a['excess_units'])
        self.assertNotEqual(hours_b['excess_units'], cap_b['excess_units'])

    def test_paid_old_bill_survives_correction_with_refund_position(self):
        old_a = one(self.db, 'SELECT * FROM carbon_bills WHERE id=?',
                    ('run-hours:tenant-a',))
        self.assertEqual(old_a['issue_state'], 'issued')
        self.assertEqual(old_a['pay_state'], 'paid')
        self.assertEqual(old_a['paid_amount'], 135)
        self.assertEqual(old_a['total_due'], 135)
        self.assertEqual(old_a['allocated_units'], 75)
        self.assertEqual(old_a['record_state'], 'superseded')  # marked, never rewritten

        new_a = one(self.db, 'SELECT * FROM carbon_bills WHERE id=?',
                    ('run-hours-r2:tenant-a',))
        self.assertEqual(new_a['prior_bill'], old_a['id'])
        self.assertEqual(new_a['version'], 2)
        self.assertEqual(new_a['allocated_units'], 72)       # 96 total * 3/4
        self.assertEqual(new_a['total_due'], 126)
        self.assertEqual(new_a['paid_amount'], 135)          # payment rolled forward
        self.assertEqual(new_a['pay_state'], 'paid')
        self.assertEqual(new_a['balance_due'], 0)
        self.assertEqual(new_a['refund_due'], 9)

        # Payment row still points at the immutable old bill.
        payment = one(self.db, 'SELECT * FROM bill_payments WHERE id=?', ('pay-a-hours',))
        self.assertEqual((payment['bill'], payment['amount'], payment['state']),
                         ('run-hours:tenant-a', 135, 'accepted'))

    def test_dispute_then_correction_then_payment(self):
        old_b = one(self.db, 'SELECT * FROM carbon_bills WHERE id=?',
                    ('run-hours:tenant-b',))
        self.assertEqual(old_b['dispute_state'], 'upheld')
        self.assertEqual(old_b['issue_state'], 'issued')
        self.assertEqual(old_b['allocated_units'], 25)       # old amount untouched
        self.assertEqual(old_b['total_due'], 35)

        new_b = one(self.db, 'SELECT * FROM carbon_bills WHERE id=?',
                    ('run-hours-r2:tenant-b',))
        self.assertEqual(new_b['allocated_units'], 24)
        self.assertEqual(new_b['excess_units'], 4)
        self.assertEqual(new_b['total_due'], 32)
        self.assertEqual(new_b['dispute_state'], 'upheld')
        self.assertEqual(new_b['pay_state'], 'paid')
        dispute = one(self.db, 'SELECT * FROM bill_disputes WHERE id=?', ('dsp-b',))
        self.assertEqual(dispute['state'], 'resolved')
        self.assertEqual(dispute['outcome'], 'upheld')
        self.assertEqual(dispute['successor_bill'], 'run-hours-r2:tenant-b')

    def test_month_end_migration_and_cross_cluster_reconcile(self):
        report = carbon.reconcile_workload(self.db, 'work-a', '2026-11')
        self.assertTrue(report['balanced'])
        by_cluster = {c['cluster']: c for c in report['clusters']}
        # 27 full days occupied + 72h source replication inside November.
        self.assertEqual(by_cluster['cluster-c7']['minutes'], 43200)
        # Target feed starts Nov 30: 24h inside November.
        self.assertEqual(by_cluster['cluster-c9']['minutes'], 1440)
        legs = {leg['id']: leg for leg in report['migration_legs']}
        self.assertEqual((legs['lw-a-rep-c7']['period_minutes'],
                          legs['lw-a-rep-c7']['next_period_minutes'],
                          legs['lw-a-rep-c7']['total_minutes']), (4320, 720, 5040))
        self.assertEqual((legs['lw-a-rep-c9']['period_minutes'],
                          legs['lw-a-rep-c9']['next_period_minutes']), (1440, 720))
        self.assertTrue(all(leg['split_balanced'] for leg in report['migration_legs']))
        self.assertEqual(report['overlaps'], [])
        self.assertTrue(all(tie['tied'] for tie in report['settled_bills']))


class ReadingEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.db = connect(':memory:')
        seed_c7(self.db)

    def test_duplicate_upload_is_controlled_and_idempotent(self):
        payload = {'op': 'reading', 'id': 'rd-dup', 'meter': 'm-dup',
                   'cluster': 'cluster-c7', 'period': '2026-11', 'units': 100,
                   'revision': 1, 'start': '2026-11-01T00:00',
                   'end': '2026-12-01T00:00', 'surveyor': 's', 'evidence': 'e'}
        first = put(self.db, 'up-1', **payload)
        self.assertEqual(first['decision'], 'reading_recorded')
        # Same event identity is replayed from the event log.
        again = carbon.run(self.db, {'event_id': 'up-1', **payload})
        self.assertEqual(again, first)
        # New event identity, identical business content: rejected as duplicate.
        dup = put(self.db, 'up-2-different-event', **payload)
        self.assertEqual(dup['decision'], 'duplicate_reading')
        rows = many(self.db, 'SELECT * FROM power_readings WHERE id=?', ('rd-dup',))
        self.assertEqual(len(rows), 1)

    def test_out_of_order_corrections_never_overwrite_old_revisions(self):
        # Revision 2 arrives before revision 1.
        early = put(self.db, 'rev2-first', op='reading', id='rd-x', meter='m-x',
                    cluster='cluster-c7', period='2026-11', units=98, revision=2,
                    surveyor='s', evidence='e2')
        self.assertEqual(early['decision'], 'reading_recorded')
        late = put(self.db, 'rev1-late', op='reading', id='rd-x', meter='m-x',
                   cluster='cluster-c7', period='2026-11', units=100, revision=1,
                   surveyor='s', evidence='e1')
        self.assertEqual(late['decision'], 'stale_reading_revision')
        self.assertEqual(late['current_revision'], 2)
        third = put(self.db, 'rev3', op='reading', id='rd-x', meter='m-x',
                    cluster='cluster-c7', period='2026-11', units=95, revision=3,
                    surveyor='s', evidence='e3')
        self.assertEqual(third['decision'], 'reading_correction_recorded')
        conflict = put(self.db, 'rev2-tampered', op='reading', id='rd-x', meter='m-x',
                       cluster='cluster-c7', period='2026-11', units=77, revision=2,
                       surveyor='s', evidence='different')
        self.assertEqual(conflict['decision'], 'reading_revision_conflict')
        versions = many(self.db, 'SELECT revision,units,state FROM power_readings WHERE id=? ORDER BY revision',
                        ('rd-x',))
        self.assertEqual([dict(r) for r in versions], [
            {'revision': 2, 'units': 98, 'state': 'superseded'},
            {'revision': 3, 'units': 95, 'state': 'current'}])
        self.assertEqual(carbon.current_reading(self.db, 'm-x', '2026-11')['units'], 95)

    def test_negative_reading_still_invalid(self):
        result = put(self.db, 'bad', op='reading', id='rd-bad', cluster='cluster-c7',
                     period='2026-11', units=-1, revision=1)
        self.assertEqual(result['decision'], 'invalid_reading')


class AllocationAndPreheatTests(unittest.TestCase):
    def setUp(self):
        self.db = connect(':memory:')
        seed_c7(self.db)

    def _settle(self, run_id, negotiation='neg-hours'):
        return put(self.db, 'settle-' + run_id, op='settle', id=run_id,
                   negotiation=negotiation)

    def test_identical_originals_with_one_selected_clause_each_give_two_outcomes(self):
        # Two independent chambers seeded from byte-identical November originals;
        # each negotiation selects a different pre-signed clause.
        db_hours = connect(':memory:')
        db_capacity = connect(':memory:')
        seed_c7(db_hours)
        seed_c7(db_capacity)
        hours = put(db_hours, 'settle', op='settle', id='run', negotiation='neg-hours')
        capacity = put(db_capacity, 'settle', op='settle', id='run',
                       negotiation='neg-capacity')

        h = {b['tenant']: b for b in hours['bills']}
        c = {b['tenant']: b for b in capacity['bills']}
        self.assertEqual((h['tenant-a']['units'], h['tenant-b']['units']), (75, 25))
        self.assertEqual((c['tenant-a']['units'], c['tenant-b']['units']), (40, 60))
        # Carbon balance, excess liability and payable all diverge.
        for tenant in ('tenant-a', 'tenant-b'):
            self.assertNotEqual(h[tenant]['carbon_units'], c[tenant]['carbon_units'])
            self.assertNotEqual(h[tenant]['excess_units'], c[tenant]['excess_units'])
            self.assertNotEqual(h[tenant]['total_due'], c[tenant]['total_due'])
        # Both selections explicitly fix the standby power bearer.
        self.assertEqual(hours['standby_bearer'], 'standby_owner')
        self.assertEqual(capacity['standby_bearer'], 'standby_owner')
        db_hours.close()
        db_capacity.close()

    def test_preheat_is_counted_for_standby_owner_not_double_billed(self):
        result = self._settle('run-hours')
        by_tenant = {b['tenant']: b for b in result['bills']}
        # B's warm preheat carries 14400 weight -> 25 units, definitely not zero.
        self.assertEqual(by_tenant['tenant-b']['units'], 25)
        self.assertEqual(by_tenant['tenant-a']['units'], 75)
        self.assertEqual(result['share_total'], 100)
        # Cross-tenant overlap (both seats occupied Nov 1-11) does not make the
        # meter total billable twice: shares never exceed the trusted reading.
        self.assertLessEqual(sum(b['units'] for b in result['bills']), 100)

    def test_standby_bearer_clause_can_shift_standby_load_to_host(self):
        result = self._settle('run-host', 'neg-host')
        by_tenant = {b['tenant']: b for b in result['bills']}
        # 14400 standby minutes move from B to primary owner A: 57600:0.
        self.assertEqual(by_tenant['tenant-a']['units'], 100)
        self.assertEqual(by_tenant['tenant-b']['units'], 0)
        reassignment = result['standby_reassignment']
        self.assertEqual(reassignment['to'], 'tenant-a')
        self.assertEqual(reassignment['standby_weight'], 14400)
        self.assertEqual(result['share_total'], 100)

    def test_allocation_invariant_for_several_totals(self):
        for total in (1, 3, 37, 100, 101, 255):
            db = connect(':memory:')
            seed_c7(db, total=total)
            result = put(db, 'settle', op='settle', id='run', negotiation='neg-hours')
            self.assertEqual(result['share_total'], total)
            shares = [b['units'] for b in result['bills']]
            self.assertEqual(sum(shares), total)
            db.close()

    def test_month_end_replication_is_not_omitted_from_allocation_basis(self):
        # Counterfactual database with the crossing replication leg missing.
        db_without = connect(':memory:')
        seed_c7(db_without, include_replication=False)
        without = put(db_without, 'settle-no-rep', op='settle', id='run-no-rep',
                      negotiation='neg-hours')
        a_without = next(b for b in without['bills'] if b['tenant'] == 'tenant-a')
        # 27 days occupied only: weight 38880; Hamilton gives A 73 / B 27.
        self.assertEqual(a_without['weight'], 38880)
        self.assertEqual({b['tenant']: b['units'] for b in without['bills']},
                         {'tenant-a': 73, 'tenant-b': 27})

        with_rep = self._settle('run-hours-rep')
        a_with = next(b for b in with_rep['bills'] if b['tenant'] == 'tenant-a')
        # The 72h November replication leg is included: weight 43200 -> 75/25.
        self.assertEqual(a_with['weight'], 43200)
        self.assertEqual({b['tenant']: b['units'] for b in with_rep['bills']},
                         {'tenant-a': 75, 'tenant-b': 25})
        db_without.close()

    def test_capacity_method_uses_retained_guaranteed_capacity(self):
        result = put(self.db, 'settle-cap', op='settle', id='run-cap',
                     negotiation='neg-capacity')
        by_tenant = {b['tenant']: b for b in result['bills']}
        self.assertEqual((by_tenant['tenant-a']['weight'], by_tenant['tenant-a']['units']), (4, 40))
        self.assertEqual((by_tenant['tenant-b']['weight'], by_tenant['tenant-b']['units']), (6, 60))
        # Reserved-only seat state is not silently counted as running hours.
        reserved = one(self.db, "SELECT COUNT(*) n FROM load_windows WHERE kind='reserved'")
        self.assertEqual(reserved['n'], 0)


class BillStateIndependenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'lifecycle.sqlite')
        self.db = connect(self.path)
        seed_c7(self.db)
        put(self.db, 'settle', op='settle', id='run-hours', negotiation='neg-hours')

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_payment_dispute_and_issue_states_move_independently(self):
        bill = 'run-hours:tenant-b'  # due 35
        put(self.db, 'issue', op='issue', run='run-hours')
        put(self.db, 'pay-part', op='pay', id='pay-part', bill=bill, amount=10,
            at='2026-11-20')
        row = carbon.bill_snapshot(self.db, bill)
        self.assertEqual(row['issue_state'], 'issued')
        self.assertEqual(row['pay_state'], 'partial')
        self.assertEqual(row['paid_amount'], 10)
        self.assertEqual(row['balance_due'], 25)
        self.assertEqual(row['dispute_state'], 'none')

        put(self.db, 'dispute', op='dispute', id='d1', bill=bill, reason='hours',
            at='2026-11-21')
        row = carbon.bill_snapshot(self.db, bill)
        # Dispute flips only dispute state; payment and issuance stand.
        self.assertEqual(row['dispute_state'], 'open')
        self.assertEqual(row['pay_state'], 'partial')
        self.assertEqual(row['paid_amount'], 10)
        self.assertEqual(row['issue_state'], 'issued')

        # Paying the rest while disputed keeps dispute open.
        put(self.db, 'pay-rest', op='pay', id='pay-rest', bill=bill, amount=25,
            at='2026-11-25')
        row = carbon.bill_snapshot(self.db, bill)
        self.assertEqual(row['pay_state'], 'paid')
        self.assertEqual(row['dispute_state'], 'open')
        self.assertEqual(row['balance_due'], 0)
        self.assertTrue(replay(self.path)['verified'])

    def test_duplicate_open_dispute_is_rejected(self):
        bill = 'run-hours:tenant-b'
        put(self.db, 'd1', op='dispute', id='d1', bill=bill, reason='one')
        again = put(self.db, 'd2', op='dispute', id='d2', bill=bill, reason='two')
        self.assertEqual(again['decision'], 'dispute_already_open')
        self.assertEqual(len(many(self.db, 'SELECT * FROM bill_disputes WHERE bill=?', (bill,))), 1)

    def test_partial_payment_carries_through_recalculation(self):
        bill = 'run-hours:tenant-a'  # due 135
        put(self.db, 'issue', op='issue', run='run-hours')
        put(self.db, 'pay-50', op='pay', id='pay-50', bill=bill, amount=50, at='2026-11-20')
        self.assertEqual(carbon.bill_snapshot(self.db, bill)['pay_state'], 'partial')

        put(self.db, 'r2', op='reading', id='reading-c7', meter='meter-c7',
            cluster='cluster-c7', period='2026-11', units=96, revision=2,
            surveyor='s', evidence='e2')
        put(self.db, 'recalc', op='recalculate', id='run-r2', supersedes='run-hours')
        new_bill = 'run-r2:tenant-a'  # corrected due 126
        snap = carbon.bill_snapshot(self.db, new_bill)
        self.assertEqual(snap['total_due'], 126)
        self.assertEqual(snap['paid_amount'], 50)
        self.assertEqual(snap['pay_state'], 'partial')
        self.assertEqual(snap['balance_due'], 76)

        put(self.db, 'pay-76', op='pay', id='pay-76', bill=new_bill, amount=76, at='2026-12-18')
        snap = carbon.bill_snapshot(self.db, new_bill)
        self.assertEqual(snap['paid_amount'], 126)
        self.assertEqual(snap['pay_state'], 'paid')
        self.assertEqual(snap['balance_due'], 0)
        self.assertTrue(replay(self.path)['verified'])

    def test_signed_bill_financials_are_immutable_after_correction(self):
        bill = 'run-hours:tenant-a'
        put(self.db, 'issue', op='issue', run='run-hours')
        before = dict(one(self.db, 'SELECT * FROM carbon_bills WHERE id=?', (bill,)))
        put(self.db, 'r2', op='reading', id='reading-c7', meter='meter-c7',
            cluster='cluster-c7', period='2026-11', units=90, revision=2,
            surveyor='s', evidence='e2')
        rec = put(self.db, 'recalc', op='recalculate', id='run-r2', supersedes='run-hours')
        self.assertEqual(rec['decision'], 'recalculated')
        after = dict(one(self.db, 'SELECT * FROM carbon_bills WHERE id=?', (bill,)))
        for column in ('allocated_units', 'carbon_units', 'excess_units', 'energy_amount',
                       'penalty_amount', 'total_due', 'ceiling_units', 'weight'):
            self.assertEqual(after[column], before[column], column)
        self.assertEqual(after['issue_state'], 'issued')
        self.assertEqual(after['record_state'], 'superseded')
        self.assertTrue(replay(self.path)['verified'])


class SeatIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.db = connect(':memory:')
        seed_c7(self.db)

    def test_capacity_weights_tie_directly_to_capacity_claims(self):
        claims = many(self.db, '''SELECT i.tenant tenant, SUM(c.cpu) cpu
                                  FROM capacity_claims c JOIN workload_intents i ON i.id=c.intent
                                  WHERE c.cluster='cluster-c7' AND c.state='active'
                                  GROUP BY i.tenant ORDER BY i.tenant''')
        self.assertEqual([dict(r) for r in claims],
                         [{'tenant': 'tenant-a', 'cpu': 4}, {'tenant': 'tenant-b', 'cpu': 6}])
        result = put(self.db, 'settle-cap', op='settle', id='run-cap',
                     negotiation='neg-capacity')
        weights = {b['tenant']: b['weight'] for b in result['bills']}
        self.assertEqual(weights, {'tenant-a': 4, 'tenant-b': 6})

    def test_reserved_window_is_not_counted_as_actual_running_hours(self):
        # A whole-month reservation-only window must not become running hours.
        put(self.db, 'win-b-reserved', op='load_window', id='lw-b-reserved',
            intent='work-b', lease='lease-b-c7', cluster='cluster-c7', kind='reserved',
            start='2026-11-01T00:00', end='2026-12-01T00:00', evidence='reservation-record')
        result = put(self.db, 'settle-h', op='settle', id='run-h', negotiation='neg-hours')
        self.assertEqual({b['tenant']: b['units'] for b in result['bills']},
                         {'tenant-a': 75, 'tenant-b': 25})

    def test_window_lease_cluster_mismatch_is_rejected(self):
        result = put(self.db, 'bad-window', op='load_window', id='lw-bad',
                     intent='work-a', lease='lease-a-c7', cluster='cluster-c9',
                     kind='occupied', start='2026-11-02T00:00', end='2026-11-03T00:00')
        self.assertEqual(result['decision'], 'lease_identity_conflict')
        self.assertIsNone(one(self.db, 'SELECT 1 FROM load_windows WHERE id=?', ('lw-bad',)))

    def test_window_requires_known_intent(self):
        result = put(self.db, 'bad-intent', op='load_window', id='lw-x',
                     intent='work-ghost', cluster='cluster-c7', kind='occupied',
                     start='2026-11-02T00:00', end='2026-11-03T00:00')
        self.assertEqual(result['decision'], 'unknown_intent')


class MissingEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.db = connect(':memory:')
        intent.run(self.db, {'event_id': 'rule-a', 'op': 'rule', 'tenant': 'tenant-a',
                             'regions': ['west'], 'revision': 1})
        intent.run(self.db, {'event_id': 'rule-b', 'op': 'rule', 'tenant': 'tenant-b',
                             'regions': ['west'], 'revision': 1})
        intent.run(self.db, {'event_id': 'i-a', 'op': 'claim', 'id': 'load-a',
                             'tenant': 'tenant-a', 'region': 'west', 'role': 'prod'})
        intent.run(self.db, {'event_id': 'i-b', 'op': 'claim', 'id': 'load-b',
                             'tenant': 'tenant-b', 'region': 'west', 'role': 'standby'})
        put(self.db, 'm8', op='meter', id='meter-c8', cluster='cluster-c8', certified=True)
        put(self.db, 'o8a', op='order', id='order-8a', tenant='tenant-a', period='2026-11',
            ceiling=100, unit_rate=1, penalty_rate=2)
        put(self.db, 'o8b', op='order', id='order-8b', tenant='tenant-b', period='2026-11',
            ceiling=100, unit_rate=1, penalty_rate=2)
        put(self.db, 'r8', op='reading', id='reading-c8', meter='meter-c8',
            cluster='cluster-c8', period='2026-11', units=50, revision=1,
            surveyor='surveyor-west', evidence='cred://c8/r1')
        put(self.db, 'n8', op='negotiation', id='neg-8', method='run_hours',
            meter='meter-c8', period='2026-11', standby_bearer='standby_owner',
            confirmed_by=['tenant-a', 'tenant-b'])
        # Verified windows: A 180m, B 60m -> 3:1 basis. A second B window is
        # known to be missing its occupancy evidence.
        put(self.db, 'w8a', op='load_window', id='lw-8a', intent='load-a',
            cluster='cluster-c8', kind='occupied',
            start='2026-11-02T00:00', end='2026-11-02T03:00', evidence='ok-a')
        put(self.db, 'w8b', op='load_window', id='lw-8b-known', intent='load-b',
            cluster='cluster-c8', kind='occupied',
            start='2026-11-03T00:00', end='2026-11-03T01:00', evidence='ok-b')
        put(self.db, 'evr8', op='evidence_request', id='evr-8', meter='meter-c8',
            period='2026-11', intent='load-b', tenant='tenant-b', kind='occupied',
            hold_units=10, reason='occupancy window export missing',
            deadline='2026-12-10', at='2026-12-01')

    def test_missing_window_is_pending_with_deadline_never_averaged(self):
        result = put(self.db, 'set8', op='settle', id='run-8', negotiation='neg-8')
        self.assertEqual(result['pending_units'], 10)
        self.assertEqual(result['pending_amount'], 10)  # 10 held units * unit_rate 1
        self.assertEqual(result['allocated_units'], 40)
        by_tenant = {b['tenant']: b for b in result['bills']}
        self.assertEqual((by_tenant['tenant-a']['units'], by_tenant['tenant-b']['units']), (30, 10))
        self.assertEqual(by_tenant['tenant-b']['pending_units'], 10)
        self.assertEqual(by_tenant['tenant-b']['pending_amount'], 10)
        # Confirmed shares plus suspense still reconcile to the trusted total.
        self.assertEqual(result['share_total'], 50)

        request = one(self.db, 'SELECT * FROM evidence_requests WHERE id=?', ('evr-8',))
        self.assertEqual(request['state'], 'open')
        self.assertEqual(request['deadline'], '2026-12-10')
        self.assertEqual(request['hold_amount'], 10)

        # No window was invented to fill the gap by averaging.
        self.assertEqual(one(self.db, 'SELECT COUNT(*) n FROM load_windows')['n'], 2)

        timeout = put(self.db, 'timeout', op='expire_evidence', as_of='2026-12-20')
        self.assertEqual(timeout['overdue'], ['evr-8'])
        self.assertEqual(one(self.db, 'SELECT state FROM evidence_requests WHERE id=?',
                             ('evr-8',))['state'], 'overdue')

    def test_supplementary_evidence_releases_suspense_and_reallocates(self):
        put(self.db, 'set8', op='settle', id='run-8', negotiation='neg-8')
        # Late but accepted proof: a real two-hour B window (120 minutes).
        accepted = put(self.db, 'w8b-extra', op='load_window', id='lw-8b-extra',
                       intent='load-b', cluster='cluster-c8', kind='occupied',
                       start='2026-11-05T00:00', end='2026-11-05T02:00',
                       evidence='late-seat-export', request='evr-8')
        self.assertEqual(accepted['decision'], 'load_window_recorded')
        self.assertEqual(one(self.db, 'SELECT state FROM evidence_requests WHERE id=?',
                             ('evr-8',))['state'], 'fulfilled')
        result = put(self.db, 'set8b', op='settle', id='run-8-final', negotiation='neg-8')
        self.assertEqual(result['pending_units'], 0)
        by_tenant = {b['tenant']: b for b in result['bills']}
        # B weight 60+120=180 matches A 180 -> exact 25/25 split of 50.
        self.assertEqual((by_tenant['tenant-a']['units'], by_tenant['tenant-b']['units']), (25, 25))
        self.assertEqual(result['share_total'], 50)


class FixtureReplayFromDiskTests(unittest.TestCase):
    def test_missing_evidence_fixture_pends_then_reallocates_and_replays(self):
        path = FIXTURES / 'carbon-missing-evidence.json'
        commands = load_envelope_fixture(path.name)
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / 'missing.sqlite')
            db = connect(db_path)
            results, by_event = run_envelopes(db, commands)
            self.assertTrue(results)
            self.assertFalse(any(r.get('decision') == 'unknown_operation' for r in results))

            pending = by_event['me-settle-pending']
            self.assertEqual((pending['pending_units'], pending['allocated_units']), (10, 40))
            pending_shares = {b['tenant']: b['units'] for b in pending['bills']}
            self.assertEqual(pending_shares, {'tenant-a': 30, 'tenant-b': 10})
            self.assertEqual(by_event['me-deadline']['overdue'], ['evr-8'])

            final = by_event['me-settle-final']
            self.assertEqual(final['pending_units'], 0)
            final_shares = {b['tenant']: b['units'] for b in final['bills']}
            self.assertEqual(final_shares, {'tenant-a': 25, 'tenant-b': 25})

            self.assertEqual(one(db, 'SELECT state FROM evidence_requests WHERE id=?',
                                 ('evr-8',))['state'], 'fulfilled')
            db.close()
            self.assertTrue(replay(db_path)['verified'])


if __name__ == '__main__':
    unittest.main()
