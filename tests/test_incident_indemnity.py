"""Acceptance tests for multi-tenant incident loss verification and clearing.

Every scenario is checked per claim against the real cross-system facts:

* incident row in ``incident_cases`` and the evac takeover result;
* actual occupied seats in the affected cluster reconstructed from
  ``placement_leases`` plus the replayed successful ``occupy`` events
  (proof survives the source-lease release after failover);
* customer-confirmed service interruptions;
* unique expense invoice originals;
* the shared per-incident limit, policy-year aggregate and once-per-incident
  deductible;
* third-party indemnity and service-credit offsets;
* explicit cash vs direct-to-provider settlement elections and bank receipts;
* dishonour (退票), linked late corrections / 补赔 and subrogation (追偿).

The on-disk dual-tenant fixture ``fixtures/incident-dual-tenant.json`` is an
envelope command list and must replay event-for-event and table-for-table.
"""

import json
import tempfile
import unittest
from pathlib import Path

from cloudcourt import MODULES, evac, insurance, intent, seat
from cloudcourt.store import connect, many, one, replay

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / 'fixtures'
DUAL_FIXTURE = FIXTURES / 'incident-dual-tenant.json'


# --------------------------------------------------------------------------- #
# World builders
# --------------------------------------------------------------------------- #

def put(db, event_id, op=None, **payload):
    payload['event_id'] = event_id
    if op is not None:
        payload['op'] = op
    return insurance.run(db, payload)


def seed_world(db, *, handoff=('jia', 'yi'), incident_id='inc-dual',
               cluster='cluster-a'):
    """Three tenants; jia/yi primary on cluster-a, bing healthy elsewhere."""
    for tenant in ('jia', 'yi', 'bing'):
        intent.run(db, {'event_id': f'rule-{tenant}', 'op': 'rule',
                        'tenant': tenant, 'regions': ['east'], 'revision': 1})
        intent.run(db, {'event_id': f'work-{tenant}', 'op': 'claim',
                        'id': f'work-{tenant}', 'tenant': tenant,
                        'region': 'east', 'role': 'critical-writes'})

    for cid, domain in (('cluster-a', 'rack-a'), ('cluster-b', 'rack-b'),
                        ('cluster-c', 'rack-c')):
        seat.run(db, {'event_id': f'obs-{cid}', 'op': 'observe', 'id': cid,
                      'region': 'east', 'domain': domain, 'cpu': 16, 'mem': 32,
                      'generation': 1, 'healthy': True})

    for tenant in ('jia', 'yi'):
        seat.run(db, {'event_id': f'res-{tenant}-a', 'op': 'reserve',
                      'id': f'lease-{tenant}-a', 'intent': f'work-{tenant}',
                      'cluster': 'cluster-a', 'role': 'primary',
                      'cpu': 2, 'mem': 4, 'expires': 200})
        seat.run(db, {'event_id': f'occ-{tenant}-a', 'op': 'occupy',
                      'id': f'lease-{tenant}-a', 'at': 10})
        seat.run(db, {'event_id': f'res-{tenant}-b', 'op': 'reserve',
                      'id': f'lease-{tenant}-b', 'intent': f'work-{tenant}',
                      'cluster': 'cluster-b', 'role': 'backup',
                      'cpu': 2, 'mem': 4, 'expires': 200})
        seat.run(db, {'event_id': f'occ-{tenant}-b', 'op': 'occupy',
                      'id': f'lease-{tenant}-b', 'at': 11})

    seat.run(db, {'event_id': 'res-bing-c', 'op': 'reserve',
                  'id': 'lease-bing-c', 'intent': 'work-bing',
                  'cluster': 'cluster-c', 'role': 'primary',
                  'cpu': 2, 'mem': 4, 'expires': 200})
    seat.run(db, {'event_id': 'occ-bing-c', 'op': 'occupy',
                  'id': 'lease-bing-c', 'at': 10})

    for tenant in ('jia', 'yi'):
        intent.run(db, {'event_id': f'grant-{tenant}', 'op': 'grant',
                        'id': f'grant-{tenant}', 'intent': f'work-{tenant}',
                        'actor': f'owner-{tenant}', 'role': 'promote',
                        'generation': 1, 'expires': 200})
        intent.run(db, {'event_id': f'assign-{tenant}', 'op': 'assign_primary',
                        'intent': f'work-{tenant}', 'cluster': 'cluster-a',
                        'grant': f'grant-{tenant}', 'at': 12})

    evac.run(db, {'event_id': 'incident', 'op': 'incident', 'id': incident_id,
                  'cluster': cluster, 'severity': 4, 'generation': 1})

    for tenant in handoff:
        evac.run(db, {'event_id': f'plan-{tenant}', 'op': 'plan',
                      'id': f'move-{tenant}', 'intent': f'work-{tenant}',
                      'source': 'cluster-a', 'target': 'cluster-b',
                      'incident': incident_id})
        evac.run(db, {'event_id': f'fence-{tenant}', 'op': 'receipt',
                      'id': f'fence-{tenant}', 'plan': f'move-{tenant}',
                      'kind': 'source_fenced', 'cluster': 'cluster-a',
                      'generation': 1})
        evac.run(db, {'event_id': f'ready-{tenant}', 'op': 'receipt',
                      'id': f'ready-{tenant}', 'plan': f'move-{tenant}',
                      'kind': 'target_ready', 'cluster': 'cluster-b',
                      'generation': 1})
        evac.run(db, {'event_id': f'handoff-{tenant}', 'op': 'handoff',
                      'plan': f'move-{tenant}'})


def bind_policy(db, pid='pol-1', *, tenants=('jia', 'yi'),
                incident_limit=200, annual_limit=500, deductible=20,
                year='FY2026', window_start=1, window_end=1000,
                generation=7, **extra):
    payload = dict(
        event_id=f'policy-{pid}', op='policy', id=pid,
        tenant='platform-msp', subject='platform-msp',
        subjects=list(tenants), window_start=window_start, window_end=window_end,
        policy_year=year, annual_limit=annual_limit,
        incident_limit=incident_limit, deductible=deductible,
        outage_scope=['temporary_capacity', 'write_outage'],
        recovery_scope=['data_recovery'], insurer_generation=generation)
    payload.update(extra)
    return insurance.run(db, payload)


def confirm_outage(db, oid, tenant, *, intent=None, incident='inc-dual',
                   cluster='cluster-a', event_id=None, **extra):
    payload = dict(event_id=event_id or f'out-{oid}', op='outage', id=oid,
                   incident=incident, tenant=tenant,
                   intent=intent or f'work-{tenant}', cluster=cluster,
                   confirmed_by=f'{tenant}-operations',
                   interrupted_at=20, restored_at=40, write_outage=1)
    payload.update(extra)
    return insurance.run(db, payload)


def record_invoice(db, iid, tenant, amount, *, ref=None,
                   kind='temporary_capacity', category='outage',
                   vendor='capco', incident='inc-dual', event_id=None, **extra):
    payload = dict(event_id=event_id or f'inv-{iid}', op='invoice', id=iid,
                   ref=ref or f'REF-{iid}', tenant=tenant,
                   intent=f'work-{tenant}', incident=incident, kind=kind,
                   category=category, vendor=vendor, amount=amount,
                   incurred_at=30)
    payload.update(extra)
    return insurance.run(db, payload)


def file_claim(db, cid, *, tenant='jia', policy='pol-1',
               incident='inc-dual', intent=None, outage='out-jia',
               invoices=('inv-jia',), amount=None, lease=None, at=50):
    return put(db, f'file-{cid}', op='claim', id=cid, policy=policy,
               incident=incident, tenant=tenant,
               intent=intent or f'work-{tenant}',
               lease=lease or f'lease-{tenant}-a', outage=outage,
               invoices=list(invoices),
               amount=amount if amount is not None else 100, at=at)


def prepare_jia_claim(db, *, amount=100, invoice_id='inv-jia',
                      claim_id='claim-jia', order_id='order-jia',
                      outage_id='out-jia', confirm=True, policy='pol-1'):
    confirm_outage(db, outage_id, 'jia')
    record_invoice(db, invoice_id, 'jia', amount)
    file_claim(db, claim_id, tenant='jia', policy=policy, outage=outage_id,
               invoices=[invoice_id], amount=amount)
    adjusted = put(db, f'adj-{claim_id}', op='adjust', id=order_id,
                   claim=claim_id, at=51)
    if confirm:
        put(db, f'conf-{claim_id}', op='confirm', claim=claim_id,
             confirmed_by='jia-cfo', at=53)
    return adjusted


class IncidentCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'state.sqlite')
        self.db = connect(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def assertReplays(self):
        result = replay(self.path)
        self.assertTrue(result['verified'], result['mismatch'])
        return result


# --------------------------------------------------------------------------- #
# On-disk dual-tenant fixture
# --------------------------------------------------------------------------- #

class DualTenantFixtureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'fixture.sqlite')
        self.db = connect(self.path)
        self.commands = json.loads(DUAL_FIXTURE.read_text(encoding='utf-8'))
        self.results = {}
        for command in self.commands:
            module = MODULES[command['subsystem']]
            payload = command['payload']
            result = module.run(self.db, payload)
            self.results[payload['event_id']] = result

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_fixture_is_a_nonempty_dual_tenant_envelope(self):
        self.assertGreater(len(self.commands), 40)
        subsystems = {c['subsystem'] for c in self.commands}
        self.assertEqual({'intent', 'seat', 'evac', 'insurance'}, subsystems)
        text = DUAL_FIXTURE.read_text(encoding='utf-8')
        self.assertIn('tenant-jia', text)
        self.assertIn('tenant-yi', text)

    def test_every_fixture_command_is_a_known_business_decision(self):
        for event_id, result in self.results.items():
            self.assertIn('decision', result, event_id)
            self.assertNotEqual(result['decision'], 'unknown_operation', event_id)
            self.assertNotIn(result['decision'],
                             ('business_identity_conflict', 'event_identity_conflict'),
                             event_id)

    def test_shared_limit_deductible_and_two_paid_claims(self):
        self.assertEqual(self.results['dt-adjust-jia']['approved_amount'], 80)
        self.assertEqual(self.results['dt-adjust-yi']['approved_amount'], 45)
        # 100 jia invoice - 20 once-per-incident deductible;
        # 60 yi recovery - 15 service credit, no deductible left to absorb.
        jia_adj = one(self.db, "SELECT * FROM loss_adjustments WHERE claim='claim-jia'")
        yi_adj = one(self.db, "SELECT * FROM loss_adjustments WHERE claim='claim-yi'")
        self.assertEqual(jia_adj['deductible_amount'], 20)
        self.assertEqual(yi_adj['deductible_amount'], 0)
        self.assertEqual(jia_adj['offset_amount'], 0)
        self.assertEqual(yi_adj['offset_amount'], 15)

        totals = one(self.db, """
            SELECT SUM(o.approved_amount) total
            FROM indemnity_orders o WHERE o.claim IN ('claim-jia','claim-yi')
        """)
        self.assertEqual(totals['total'], 125)
        self.assertLessEqual(totals['total'], 200)

        for claim in ('claim-jia', 'claim-yi'):
            self.assertEqual(one(
                self.db, "SELECT state FROM loss_claims WHERE id=?", (claim,))['state'],
                'paid')
            receipts = many(self.db,
                            "SELECT * FROM payment_receipts WHERE claim=?", (claim,))
            self.assertEqual(len(receipts), 1)
            self.assertEqual(receipts[0]['state'], 'paid')

    def test_cash_and_direct_pay_have_distinct_legal_attributes(self):
        jia = one(self.db, "SELECT * FROM settlements WHERE id='settle-jia-direct'")
        yi = one(self.db, "SELECT * FROM settlements WHERE id='settle-yi-cash'")
        self.assertEqual(jia['method'], 'direct_pay')
        self.assertEqual(jia['payee_kind'], 'provider')
        self.assertEqual(jia['payee'], 'capco')
        self.assertEqual(jia['invoice'], 'inv-jia-capacity')
        self.assertEqual(jia['supplier_claim'], 'extinguished_by_direct_payment')
        self.assertEqual(jia['recovery_target'], 'at-fault-storage-vendor')

        self.assertEqual(yi['method'], 'cash')
        self.assertEqual(yi['payee_kind'], 'tenant')
        self.assertEqual(yi['payee'], 'tenant-yi')
        self.assertEqual(yi['delivery_risk'], 'customer_self_procurement')

    def test_fixture_fake_tenant_is_rejected_without_order(self):
        self.assertEqual(self.results['dt-adjust-bing']['decision'],
                         'not_affected_by_incident')
        self.assertIsNone(one(
            self.db, "SELECT * FROM indemnity_orders WHERE claim='claim-bing'"))
        self.assertEqual(one(
            self.db, "SELECT state FROM loss_claims WHERE id='claim-bing'")['state'],
            'filed')

    def test_fixture_subrogation_installment(self):
        case = one(self.db, "SELECT * FROM subrogation_cases WHERE id='sub-jia'")
        self.assertEqual(case['target'], 'at-fault-storage-vendor')
        rows = many(self.db, "SELECT * FROM recovery_receipts WHERE case_id='sub-jia'")
        self.assertEqual(sum(r['amount'] for r in rows), 35)

    def test_fixture_replays(self):
        result = replay(self.path)
        self.assertTrue(result['verified'], result['mismatch'])


# --------------------------------------------------------------------------- #
# Fact verification per claim
# --------------------------------------------------------------------------- #

class IncidentFactVerificationTests(IncidentCase):
    def test_adjustment_anchors_incident_actual_seat_outage_and_invoices(self):
        seed_world(self.db)
        bind_policy(self.db)
        adjusted = prepare_jia_claim(self.db)
        self.assertEqual(adjusted['decision'], 'loss_adjusted')

        row = one(self.db, "SELECT detail FROM loss_adjustments WHERE claim='claim-jia'")
        detail = json.loads(row['detail'])

        # Real incident row.
        self.assertEqual(detail['incident']['id'], 'inc-dual')
        self.assertEqual(detail['incident']['cluster'], 'cluster-a')
        self.assertEqual(detail['incident']['state'], 'evacuate')
        self.assertEqual(detail['incident']['generation'], 1)

        # Actual occupied seat, proven through the successful occupy event even
        # though the source lease was released by the completed handoff.
        source_lease = one(self.db, "SELECT * FROM placement_leases WHERE id='lease-jia-a'")
        self.assertEqual(source_lease['state'], 'released')
        seat_ids = [s['lease'] for s in detail['seats']]
        self.assertIn('lease-jia-a', seat_ids)
        seat = next(s for s in detail['seats'] if s['lease'] == 'lease-jia-a')
        self.assertEqual(seat['role'], 'primary')
        self.assertEqual(seat['occupy_event'], 'occ-jia-a')

        # Evacuation takeover result is part of the verified record.
        self.assertTrue(detail['takeover'])
        move = detail['takeover'][0]
        self.assertEqual(move['id'], 'move-jia')
        self.assertEqual(move['state'], 'complete')
        kinds = {r['kind'] for r in move['receipts']}
        self.assertEqual(kinds, {'source_fenced', 'target_ready'})

        # Customer-confirmed interruption and invoice originals.
        self.assertEqual(detail['outage']['id'], 'out-jia')
        self.assertEqual(detail['outage']['confirmed_by'], 'jia-operations')
        self.assertEqual([i['id'] for i in detail['invoices']], ['inv-jia'])
        self.assertEqual(detail['invoices'][0]['ref'], 'REF-inv-jia')

        proof = one(self.db, "SELECT seat_proof FROM claim_evidence WHERE claim='claim-jia'")
        self.assertIn('lease-jia-a', proof['seat_proof'])
        self.assertReplays()

    def test_unaffected_tenant_cannot_recover_by_referencing_incident(self):
        seed_world(self.db)
        # bing buys its own valid policy; it simply has no seat on cluster-a.
        bind_policy(self.db, 'pol-bing', tenants=('bing',))
        confirm_outage(self.db, 'out-bing', 'bing')
        record_invoice(self.db, 'inv-bing', 'bing', 90, vendor='tempco')
        file_claim(self.db, 'claim-bing', tenant='bing', policy='pol-bing',
                   outage='out-bing', invoices=('inv-bing',), amount=90)
        result = put(self.db, 'adj-bing', op='adjust', id='order-bing',
                     claim='claim-bing')
        self.assertEqual(result['decision'], 'not_affected_by_incident')
        self.assertEqual(result['cluster'], 'cluster-a')
        self.assertIsNone(one(
            self.db, "SELECT * FROM loss_adjustments WHERE claim='claim-bing'"))
        self.assertIsNone(one(
            self.db, "SELECT * FROM indemnity_orders WHERE claim='claim-bing'"))
        self.assertReplays()

    def test_tenant_cannot_bind_another_tenants_workload(self):
        seed_world(self.db)
        bind_policy(self.db, tenants=('jia', 'bing'))
        result = put(self.db, 'claim-mismatch', op='claim', id='claim-mismatch',
                     policy='pol-1', incident='inc-dual', tenant='bing',
                     intent='work-jia', outage='out-x', invoices=[], amount=1)
        self.assertEqual(result['decision'], 'tenant_workload_mismatch')

    def test_claim_without_any_workload_is_rejected_at_adjust(self):
        seed_world(self.db)
        bind_policy(self.db, tenants=('bing',))
        put(self.db, 'claim-bare', op='claim', id='claim-bare',
            policy='pol-1', incident='inc-dual', tenant='bing', amount=10,
            at=50)
        result = put(self.db, 'adj-bare', op='adjust', id='order-bare',
                     claim='claim-bare')
        self.assertEqual(result['decision'], 'affected_workload_required')

    def test_unconfirmed_outage_blocks_adjustment(self):
        seed_world(self.db)
        bind_policy(self.db)
        # Record an unconfirmed outage row directly through the op state flag.
        put(self.db, 'out-draft', op='outage', id='out-draft',
            incident='inc-dual', tenant='jia', state='reported')
        record_invoice(self.db, 'inv-jia', 'jia', 100)
        file_claim(self.db, 'claim-jia', outage='out-draft',
                   invoices=('inv-jia',))
        result = put(self.db, 'adj-jia', op='adjust', id='order-jia',
                     claim='claim-jia')
        self.assertEqual(result['decision'], 'outage_not_confirmed')
        self.assertReplays()


# --------------------------------------------------------------------------- #
# Duplicate expense protection
# --------------------------------------------------------------------------- #

class DuplicateExpenseTests(IncidentCase):
    def test_duplicate_invoice_reference_is_rejected(self):
        seed_world(self.db)
        first = record_invoice(self.db, 'inv-a', 'jia', 100, ref='SAME-1')
        self.assertEqual(first['decision'], 'invoice_recorded')
        second = record_invoice(self.db, 'inv-b', 'jia', 120, ref='SAME-1')
        self.assertEqual(second['decision'], 'duplicate_expense')
        self.assertEqual(second['held_by'], 'inv-a')

    def test_same_invoice_cannot_fund_two_claims(self):
        seed_world(self.db)
        bind_policy(self.db)
        confirm_outage(self.db, 'out-jia', 'jia')
        record_invoice(self.db, 'inv-jia', 'jia', 100)
        file_claim(self.db, 'claim-jia')
        # A second claim tries to resubmit the identical expense original.
        duplicate = file_claim(self.db, 'claim-jia-2')
        self.assertEqual(duplicate['decision'], 'duplicate_expense')
        self.assertEqual(duplicate['held_by'], 'claim-jia')
        bindings = many(self.db,
                        "SELECT * FROM claim_invoices WHERE invoice='inv-jia'")
        self.assertEqual(len(bindings), 1)
        self.assertEqual(bindings[0]['claim'], 'claim-jia')
        self.assertReplays()

    def test_cross_tenant_invoice_tenant_mismatch(self):
        seed_world(self.db)
        bind_policy(self.db, tenants=('jia', 'yi'))
        confirm_outage(self.db, 'out-yi', 'yi')
        record_invoice(self.db, 'inv-jia', 'jia', 100)
        result = file_claim(self.db, 'claim-yi', tenant='yi',
                            outage='out-yi', invoices=('inv-jia',), amount=100)
        self.assertEqual(result['decision'], 'invoice_tenant_mismatch')


# --------------------------------------------------------------------------- #
# Shared incident limit and deductible
# --------------------------------------------------------------------------- #

class SharedLimitAndDeductibleTests(IncidentCase):
    def test_incident_purse_is_shared_and_never_exceeded(self):
        seed_world(self.db)
        bind_policy(self.db, incident_limit=100, annual_limit=1000, deductible=0)
        prepare_jia_claim(self.db, amount=60)
        yi = put(self.db, 'prep-yi-out', op='outage', id='out-yi',
                 incident='inc-dual', tenant='yi')
        record_invoice(self.db, 'inv-yi', 'yi', 60)
        file_claim(self.db, 'claim-yi', tenant='yi', outage='out-yi',
                   invoices=('inv-yi',), amount=60)
        r_yi = put(self.db, 'adj-yi', op='adjust', id='order-yi', claim='claim-yi')
        self.assertEqual(r_yi['approved_amount'], 40)
        self.assertTrue(r_yi['capped_by_incident'])

        # Third valid claim has no incident capacity left -> zero award, and
        # the persisted sum is still exactly the cap.
        record_invoice(self.db, 'inv-jia-2', 'jia', 60, ref='REF-2')
        file_claim(self.db, 'claim-jia-2', invoices=('inv-jia-2',), amount=60)
        r_three = put(self.db, 'adj-three', op='adjust', id='order-three',
                      claim='claim-jia-2')
        self.assertEqual(r_three['approved_amount'], 0)

        total = one(self.db, """
            SELECT COALESCE(SUM(approved_amount),0) total
            FROM indemnity_orders o JOIN loss_claims c ON c.id=o.claim
            WHERE c.policy='pol-1' AND c.incident='inc-dual'
              AND o.state IN ('adjusted','confirmed','paid')
        """)['total']
        self.assertEqual(total, 100)
        self.assertLessEqual(total, 100)
        self.assertReplays()

    def test_deductible_is_absorbed_once_across_all_claims(self):
        seed_world(self.db)
        bind_policy(self.db, incident_limit=500, annual_limit=1000, deductible=100)
        prepare_jia_claim(self.db, amount=30)
        jia_adj = one(self.db, "SELECT * FROM loss_adjustments WHERE claim='claim-jia'")
        self.assertEqual(jia_adj['deductible_amount'], 30)
        self.assertEqual(jia_adj['approved_amount'], 0)

        confirm_outage(self.db, 'out-yi', 'yi')
        record_invoice(self.db, 'inv-yi', 'yi', 200)
        file_claim(self.db, 'claim-yi', tenant='yi', outage='out-yi',
                   invoices=('inv-yi',), amount=200)
        r_yi = put(self.db, 'adj-yi', op='adjust', id='order-yi', claim='claim-yi')
        # 170 of deductible remained; yi absorbs 70 more, then gets 130.
        self.assertEqual(r_yi['deductible_amount'], 70)
        self.assertEqual(r_yi['approved_amount'], 130)
        total_ded = one(self.db, """
            SELECT SUM(deductible_amount) d FROM loss_adjustments a
            JOIN loss_claims c ON c.id=a.claim WHERE c.incident='inc-dual'
        """)['d']
        self.assertEqual(total_ded, 100)
        self.assertReplays()


# --------------------------------------------------------------------------- #
# Annual aggregate across policy years
# --------------------------------------------------------------------------- #

class AnnualBalanceTests(IncidentCase):
    def _second_incident(self, db, incident_id):
        evac.run(db, {'event_id': f'inc-{incident_id}', 'op': 'incident',
                      'id': incident_id, 'cluster': 'cluster-a',
                      'severity': 4, 'generation': 1})

    def test_annual_balance_accumulates_and_new_year_resets(self):
        seed_world(self.db)
        bind_policy(self.db, 'pol-2026', year='FY2026',
                    incident_limit=100, annual_limit=120, deductible=0)
        self._second_incident(self.db, 'inc-year-end')

        # First incident consumes 80 of the annual aggregate.
        prepare_jia_claim(self.db, amount=80, policy='pol-2026')
        pos = put(self.db, 'pos-26', op='coverage_position',
                  policy='pol-2026', incident='inc-dual')
        self.assertEqual(pos['annual_used'], 80)
        self.assertEqual(pos['annual_remaining'], 40)

        # Second incident: per-incident cap (100) does not bind an 80 loss,
        # but the annual aggregate only has 40 left.
        confirm_outage(self.db, 'out-jia-b', 'jia', incident='inc-year-end')
        record_invoice(self.db, 'inv-jia-b', 'jia', 80,
                       incident='inc-year-end')
        put(self.db, 'claim-jia-b', op='claim', id='claim-jia-b',
            policy='pol-2026', incident='inc-year-end', tenant='jia',
            intent='work-jia', outage='out-jia-b',
            invoices=['inv-jia-b'], amount=80, at=80)
        r_b = put(self.db, 'adj-b', op='adjust', id='order-b',
                  claim='claim-jia-b', at=81)
        self.assertEqual(r_b['approved_amount'], 40)
        self.assertTrue(r_b['capped_by_annual'])
        pos = put(self.db, 'pos-26b', op='coverage_position',
                  policy='pol-2026')
        self.assertEqual(pos['annual_used'], 120)
        self.assertEqual(pos['annual_remaining'], 0)

        # A new policy year carries a fresh aggregate; FY2026 stays exhausted.
        bind_policy(self.db, 'pol-2027', year='FY2027',
                    incident_limit=100, annual_limit=120, deductible=0)
        confirm_outage(self.db, 'out-jia-c', 'jia')
        record_invoice(self.db, 'inv-jia-c', 'jia', 80, ref='REF-C')
        put(self.db, 'claim-jia-c', op='claim', id='claim-jia-c',
            policy='pol-2027', incident='inc-dual', tenant='jia',
            intent='work-jia', outage='out-jia-c',
            invoices=['inv-jia-c'], amount=80, at=90)
        r_c = put(self.db, 'adj-c', op='adjust', id='order-c',
                  claim='claim-jia-c')
        self.assertEqual(r_c['approved_amount'], 80)
        pos27 = put(self.db, 'pos-27', op='coverage_position',
                    policy='pol-2027')
        self.assertEqual(pos27['annual_used'], 80)
        self.assertEqual(pos27['annual_remaining'], 40)
        pos26 = put(self.db, 'pos-26c', op='coverage_position',
                    policy='pol-2026')
        self.assertEqual(pos26['annual_remaining'], 0)
        self.assertReplays()


# --------------------------------------------------------------------------- #
# Offsets: third-party indemnity and service credits cannot be hidden
# --------------------------------------------------------------------------- #

class OffsetDeductionTests(IncidentCase):
    def test_unlinked_tenant_incident_offsets_are_still_deducted(self):
        seed_world(self.db)
        bind_policy(self.db, incident_limit=200, annual_limit=500, deductible=0)
        confirm_outage(self.db, 'out-jia', 'jia')
        record_invoice(self.db, 'inv-jia', 'jia', 100)
        # Both offsets are recorded only at tenant+incident level, never linked
        # to the claim: they must still surface and reduce the award.
        put(self.db, 'off-third', op='offset', id='off-third', tenant='jia',
            incident='inc-dual', kind='third_party_indemnity',
            source='storage-vendor', amount=20, at=35)
        put(self.db, 'off-credit', op='offset', id='off-credit', tenant='jia',
            incident='inc-dual', kind='service_credit',
            source='platform', amount=10, at=36)
        file_claim(self.db, 'claim-jia', amount=100)
        result = put(self.db, 'adj-jia', op='adjust', id='order-jia',
                     claim='claim-jia')
        self.assertEqual(result['offset_amount'], 30)
        self.assertEqual(result['approved_amount'], 70)
        detail = json.loads(one(
            self.db, "SELECT detail FROM loss_adjustments WHERE claim='claim-jia'"
        )['detail'])
        self.assertEqual({o['id'] for o in detail['offsets']},
                         {'off-third', 'off-credit'})
        self.assertReplays()

    def test_offset_belonging_to_another_claim_is_not_double_counted(self):
        seed_world(self.db)
        bind_policy(self.db, incident_limit=500, annual_limit=500, deductible=0)
        prepare_jia_claim(self.db, amount=100)
        put(self.db, 'off-other', op='offset', id='off-other',
            claim='claim-someone-else', tenant='jia', incident='inc-dual',
            kind='third_party', amount=999, at=35)
        # Re-adjust is blocked, but a fresh yi claim must not see that offset.
        confirm_outage(self.db, 'out-yi', 'yi')
        record_invoice(self.db, 'inv-yi', 'yi', 40)
        file_claim(self.db, 'claim-yi', tenant='yi', outage='out-yi',
                   invoices=('inv-yi',), amount=40)
        r_yi = put(self.db, 'adj-yi', op='adjust', id='order-yi', claim='claim-yi')
        self.assertEqual(r_yi['offset_amount'], 0)
        self.assertEqual(r_yi['approved_amount'], 40)
        self.assertReplays()


# --------------------------------------------------------------------------- #
# Cash vs direct provider payment: two explicit, equally legal elections
# --------------------------------------------------------------------------- #

class SettlementElectionTests(IncidentCase):
    def _ready_jia(self):
        seed_world(self.db)
        bind_policy(self.db)
        prepare_jia_claim(self.db)

    def test_method_is_required_and_is_never_inferred_from_assess(self):
        self._ready_jia()
        missing = put(self.db, 'set-missing', op='settlement', id='set-missing',
                      claim='claim-jia')
        self.assertEqual(missing['decision'], 'settlement_method_required')
        # Verification/adjustment creates no settlement instruction or receipt.
        self.assertEqual(many(self.db, "SELECT * FROM settlements"), [])
        self.assertEqual(many(self.db, "SELECT * FROM payment_receipts"), [])
        # The legacy assess path only emits a provisional order.
        legacy = insurance.run(self.db, {
            'event_id': 'legacy-pol', 'op': 'policy', 'id': 'legacy-pol',
            'tenant': 't', 'limit': 100, 'deductible': 10, 'expires': 50})
        self.assertEqual(legacy['decision'], 'coverage_recorded')
        insurance.run(self.db, {'event_id': 'legacy-claim', 'op': 'claim',
                               'id': 'legacy-claim', 'policy': 'legacy-pol',
                               'incident': 'i', 'amount': 40, 'at': 1})
        assessed = insurance.run(self.db, {'event_id': 'legacy-assess',
                                          'op': 'assess', 'id': 'legacy-order',
                                          'claim': 'legacy-claim'})
        self.assertEqual(assessed['decision'], 'provisional_assessment')
        self.assertEqual(many(self.db, "SELECT * FROM settlements"), [])

    def test_cash_election_pays_tenant_with_customer_delivery_risk(self):
        self._ready_jia()
        result = put(self.db, 'set-cash', op='settlement', id='set-cash',
                     claim='claim-jia', method='cash', at=55)
        self.assertEqual(result['decision'], 'settlement_instructed')
        self.assertEqual(result['payee_kind'], 'tenant')
        self.assertEqual(result['payee'], 'jia')
        self.assertEqual(result['delivery_risk'], 'customer_self_procurement')
        receipt = put(self.db, 'rcpt-cash', op='payment_receipt',
                      id='rcpt-cash', settlement='set-cash',
                      channel='wire', ref='BANK-1', paid_at=56)
        self.assertEqual(receipt['decision'], 'payment_receipted')
        row = one(self.db, "SELECT * FROM payment_receipts WHERE id='rcpt-cash'")
        self.assertEqual(row['amount'], 80)
        self.assertEqual(row['state'], 'paid')
        self.assertEqual(one(self.db,
                             "SELECT state FROM loss_claims WHERE id='claim-jia'")['state'],
                         'paid')
        self.assertReplays()

    def test_direct_pay_election_pays_provider_and_records_new_legal_lines(self):
        self._ready_jia()
        # Direct pay without the provider invoice is refused.
        no_invoice = put(self.db, 'set-direct-bad', op='settlement',
                         id='set-direct-bad', claim='claim-jia',
                         method='direct_pay')
        self.assertEqual(no_invoice['decision'], 'provider_invoice_required')

        result = put(self.db, 'set-direct', op='settlement', id='set-direct',
                     claim='claim-jia', method='direct_pay',
                     invoice='inv-jia', restored_at=39,
                     recovery_target='at-fault-storage-vendor', at=55)
        self.assertEqual(result['decision'], 'settlement_instructed')
        self.assertEqual(result['payee_kind'], 'provider')
        self.assertEqual(result['payee'], 'capco')
        self.assertEqual(result['supplier_claim'],
                         'extinguished_by_direct_payment')
        self.assertEqual(result['recovery_target'], 'at-fault-storage-vendor')
        self.assertEqual(result['restored_at'], 39)
        receipt = put(self.db, 'rcpt-direct', op='payment_receipt',
                      id='rcpt-direct', settlement='set-direct',
                      channel='provider-wire', ref='WIRE-9', paid_at=56)
        self.assertEqual(receipt['decision'], 'payment_receipted')
        row = one(self.db, "SELECT * FROM settlements WHERE id='set-direct'")
        self.assertEqual(row['provider'], 'capco')
        self.assertEqual(row['invoice'], 'inv-jia')
        self.assertReplays()

    def test_two_worlds_same_facts_support_either_arrangement(self):
        # Cash world.
        seed_world(self.db)
        bind_policy(self.db)
        prepare_jia_claim(self.db)
        put(self.db, 'set-cash', op='settlement', id='set-cash',
            claim='claim-jia', method='cash', restored_at=47,
            recovery_target='responsible-third-party')
        put(self.db, 'rcpt-cash', op='payment_receipt', id='rcpt-cash',
            settlement='set-cash', ref='C1')
        cash = one(self.db, "SELECT * FROM settlements WHERE id='set-cash'")
        cash_remaining = put(self.db, 'pos-cash', op='coverage_position',
                             policy='pol-1', incident='inc-dual')

        # Direct world from the identical prefix in a second database.
        other = tempfile.TemporaryDirectory()
        try:
            path2 = str(Path(other.name) / 'other.sqlite')
            db2 = connect(path2)
            seed_world(db2)
            bind_policy(db2)
            prepare_jia_claim(db2)
            put(db2, 'set-direct', op='settlement', id='set-direct',
                claim='claim-jia', method='direct_pay', invoice='inv-jia',
                restored_at=39, recovery_target='at-fault-storage-vendor')
            put(db2, 'rcpt-direct', op='payment_receipt', id='rcpt-direct',
                settlement='set-direct', ref='D1')
            direct = one(db2, "SELECT * FROM settlements WHERE id='set-direct'")
            direct_remaining = insurance.run(db2, {
                'event_id': 'pos-direct', 'op': 'coverage_position',
                'policy': 'pol-1', 'incident': 'inc-dual'})

            # Same incident, same invoice original, same approved amount...
            self.assertEqual(cash['amount'], direct['amount'])
            self.assertEqual(cash['amount'], 80)
            self.assertEqual(cash_remaining['incident_used'],
                             direct_remaining['incident_used'])
            # ...but different payee, risk/restoration and recovery target.
            self.assertNotEqual(cash['payee_kind'], direct['payee_kind'])
            self.assertNotEqual(cash['recovery_target'], direct['recovery_target'])
            self.assertIsNone(cash['supplier_claim'])
            self.assertIsNotNone(direct['supplier_claim'])
            # Direct provider funding restores write service earlier than the
            # tenant self-procuring after a cash reimbursement.
            self.assertEqual(cash['restored_at'], 47)
            self.assertEqual(direct['restored_at'], 39)
            self.assertLess(direct['restored_at'], cash['restored_at'])
            result = replay(path2)
            self.assertTrue(result['verified'], result['mismatch'])
            db2.close()
        finally:
            other.cleanup()
        self.assertReplays()


# --------------------------------------------------------------------------- #
# Dishonour and late linked correction (补赔)
# --------------------------------------------------------------------------- #

class DishonourAndCorrectionTests(IncidentCase):
    def _paid_jia(self, amount=50):
        seed_world(self.db)
        bind_policy(self.db, incident_limit=500, annual_limit=1000, deductible=0)
        prepare_jia_claim(self.db, amount=amount)
        put(self.db, 'set-1', op='settlement', id='set-1',
            claim='claim-jia', method='cash')
        put(self.db, 'rcpt-1', op='payment_receipt', id='rcpt-1',
            settlement='set-1', channel='check', ref='CHK-1', paid_at=56)

    def test_dishonour_appends_linked_record_and_allows_reissue(self):
        self._paid_jia()
        original = one(self.db, "SELECT * FROM payment_receipts WHERE id='rcpt-1'")
        self.assertEqual(original['amount'], 50)

        bounced = put(self.db, 'dis-1', op='dishonour', id='dis-1',
                      receipt='rcpt-1', reason='insufficient_funds', at=60)
        self.assertEqual(bounced['decision'], 'payment_dishonoured')
        self.assertEqual(bounced['net_settled'], 0)

        # The paid fact is untouched: same row, amount and paid state.
        after = one(self.db, "SELECT * FROM payment_receipts WHERE id='rcpt-1'")
        self.assertEqual(after['amount'], original['amount'])
        self.assertEqual(after['ref'], 'CHK-1')
        self.assertEqual(after['state'], 'paid')
        self.assertEqual(after['paid_at'], original['paid_at'])
        notice = one(self.db, "SELECT * FROM payment_dishonours WHERE id='dis-1'")
        self.assertEqual(notice['receipt'], 'rcpt-1')
        self.assertEqual(notice['amount'], 50)
        self.assertEqual(one(self.db,
                             "SELECT state FROM indemnity_orders WHERE id='order-jia'")['state'],
                         'confirmed')

        # Reissue against the same order for the reopened outstanding balance.
        put(self.db, 'set-2', op='settlement', id='set-2',
            claim='claim-jia', method='cash')
        reissue = put(self.db, 'rcpt-2', op='payment_receipt', id='rcpt-2',
                      settlement='set-2', channel='wire', ref='WIRE-2', paid_at=61)
        self.assertEqual(reissue['decision'], 'payment_receipted')
        receipts = many(self.db, "SELECT * FROM payment_receipts WHERE claim='claim-jia'")
        dishonours = many(self.db, "SELECT * FROM payment_dishonours WHERE claim='claim-jia'")
        net = sum(r['amount'] for r in receipts) - sum(d['amount'] for d in dishonours)
        self.assertEqual(net, 50)
        self.assertEqual(len(receipts), 2)
        self.assertReplays()

    def test_late_correction_creates_linked_difference_supplement(self):
        self._paid_jia()
        record_invoice(self.db, 'inv-late', 'jia', 30, ref='LATE-1')
        correction = put(self.db, 'cor-1', op='correction', id='cor-1',
                         claim='claim-jia', invoice='inv-late', delta=30,
                         reason='later_receipt_found', at=70)
        self.assertEqual(correction['decision'], 'correction_recorded')
        self.assertEqual(correction['approved_delta'], 30)
        supplement_id = correction['supplement_order']
        self.assertEqual(supplement_id, 'cor-1-order')

        # Original paid order is immutable in amount/state; the difference is a
        # separate linked order.
        original = one(self.db, "SELECT * FROM indemnity_orders WHERE id='order-jia'")
        self.assertEqual(original['approved_amount'], 50)
        self.assertEqual(original['state'], 'paid')
        supplement = one(self.db, "SELECT * FROM indemnity_orders WHERE id=?",
                         (supplement_id,))
        self.assertEqual(supplement['approved_amount'], 30)
        correction_row = one(self.db, "SELECT * FROM loss_corrections WHERE id='cor-1'")
        self.assertEqual(correction_row['prior_adjustment'],
                         one(self.db, "SELECT id FROM loss_adjustments WHERE claim='claim-jia'")['id'])

        # The supplement follows the same customer-confirm -> pay lifecycle.
        confirmed = put(self.db, 'conf-supp', op='confirm',
                        id='conf-supp', claim='claim-jia',
                        order_id=supplement_id, confirmed_by='jia-cfo')
        self.assertEqual(confirmed['decision'], 'claim_confirmed')
        put(self.db, 'set-supp', op='settlement', id='set-supp',
            claim='claim-jia', order_id=supplement_id, method='cash')
        paid = put(self.db, 'rcpt-supp', op='payment_receipt', id='rcpt-supp',
                   settlement='set-supp', ref='SUPP-1')
        self.assertEqual(paid['decision'], 'payment_receipted')

        net = insurance._settled_principal(self.db, 'claim-jia')
        self.assertEqual(net, 80)
        # Both principal orders count once toward the shared purse.
        total_awarded = one(self.db, """
            SELECT SUM(approved_amount) t FROM indemnity_orders WHERE claim='claim-jia'
        """)['t']
        self.assertEqual(total_awarded, 80)
        self.assertReplays()

    def test_correction_difference_is_still_capped_by_shared_limits(self):
        seed_world(self.db)
        bind_policy(self.db, incident_limit=60, annual_limit=1000, deductible=0)
        prepare_jia_claim(self.db, amount=60)
        put(self.db, 'conf', op='confirm', claim='claim-jia')
        put(self.db, 'set-1', op='settlement', id='set-1',
            claim='claim-jia', method='cash')
        put(self.db, 'rcpt-1', op='payment_receipt', id='rcpt-1',
            settlement='set-1', ref='C1')
        # Incident purse is full at 60; a late 40 difference cannot break cap.
        record_invoice(self.db, 'inv-late', 'jia', 40, ref='LATE-9')
        correction = put(self.db, 'cor-cap', op='correction', id='cor-cap',
                         claim='claim-jia', invoice='inv-late', delta=40)
        self.assertEqual(correction['approved_delta'], 0)
        self.assertIsNone(correction['supplement_order'])
        total = one(self.db, """
            SELECT SUM(o.approved_amount) t FROM indemnity_orders o
            JOIN loss_claims c ON c.id=o.claim WHERE c.incident='inc-dual'
        """)['t']
        self.assertEqual(total, 60)
        self.assertReplays()


# --------------------------------------------------------------------------- #
# Subrogation / third-party recovery
# --------------------------------------------------------------------------- #

class SubrogationTests(IncidentCase):
    def test_recovery_installments_close_case_and_keep_paid_fact(self):
        seed_world(self.db)
        bind_policy(self.db)
        prepare_jia_claim(self.db)
        put(self.db, 'conf2', op='confirm', claim='claim-jia')
        put(self.db, 'set-1', op='settlement', id='set-1',
            claim='claim-jia', method='cash')
        put(self.db, 'rcpt-1', op='payment_receipt', id='rcpt-1',
            settlement='set-1', ref='C1')

        opened = put(self.db, 'sub-1', op='subrogate', id='sub-1',
                     claim='claim-jia', target='faulty-storage-vendor',
                     basis='tort_subrogation', amount=80)
        self.assertEqual(opened['decision'], 'subrogation_opened')
        r1 = put(self.db, 'rec-1', op='recovery_receipt', id='rec-1',
                 case='sub-1', amount=50, ref='V-1')
        self.assertEqual(r1['state'], 'partial')
        r2 = put(self.db, 'rec-2', op='recovery_receipt', id='rec-2',
                 case='sub-1', amount=30, ref='V-2')
        self.assertEqual(r2['state'], 'recovered')
        case = one(self.db, "SELECT * FROM subrogation_cases WHERE id='sub-1'")
        self.assertEqual(case['state'], 'recovered')
        # Recovery does not rewrite the insurer's original payment receipt.
        receipt = one(self.db, "SELECT * FROM payment_receipts WHERE id='rcpt-1'")
        self.assertEqual(receipt['amount'], 80)
        self.assertReplays()

    def test_subrogation_requires_target(self):
        result = put(self.db, 'sub-x', op='subrogate', id='sub-x',
                     claim='claim-jia')
        self.assertEqual(result['decision'], 'recovery_target_required')


# --------------------------------------------------------------------------- #
# Distinct lifecycle states: provisional / adjusted / confirmed / paid
# --------------------------------------------------------------------------- #

class LifecycleStateTests(IncidentCase):
    def test_states_are_distinct_ordered_and_provisional_does_not_consume_limit(self):
        seed_world(self.db)
        bind_policy(self.db)
        confirm_outage(self.db, 'out-jia', 'jia')
        record_invoice(self.db, 'inv-jia', 'jia', 100)
        file_claim(self.db, 'claim-jia')

        # 暂估: legacy asserted-amount estimate, explicitly provisional.
        provisional = insurance.run(self.db, {
            'event_id': 'assess-jia', 'op': 'assess', 'id': 'order-provisional',
            'claim': 'claim-jia'})
        self.assertEqual(provisional['decision'], 'provisional_assessment')
        self.assertEqual(provisional['amount'], 80)
        self.assertEqual(one(self.db,
                             "SELECT state FROM indemnity_orders WHERE id='order-provisional'")['state'],
                         'provisional')

        # No adjustment/confirmation yet -> money movement is refused.
        early = put(self.db, 'pay-early', op='pay', id='pay-early',
                    claim='claim-jia', method='cash')
        self.assertEqual(early['decision'], 'adjustment_required')

        adjusted = put(self.db, 'adj-jia', op='adjust', id='order-jia',
                       claim='claim-jia')
        self.assertEqual(adjusted['decision'], 'loss_adjusted')
        self.assertEqual(one(self.db,
                             "SELECT state FROM indemnity_orders WHERE id='order-jia'")['state'],
                         'adjusted')

        before_confirm = put(self.db, 'set-early', op='settlement',
                              id='set-early', claim='claim-jia', method='cash')
        self.assertEqual(before_confirm['decision'],
                         'customer_confirmation_required')

        confirmed = put(self.db, 'conf-jia', op='confirm', claim='claim-jia',
                        confirmed_by='jia-cfo')
        self.assertEqual(confirmed['decision'], 'claim_confirmed')
        self.assertEqual(one(self.db,
                             "SELECT state FROM indemnity_orders WHERE id='order-jia'")['state'],
                         'confirmed')

        # One-step pay alias with inline cash election writes the real receipt.
        paid = put(self.db, 'pay-jia', op='pay', id='receipt-jia',
                   claim='claim-jia', method='cash', channel='wire', ref='B-1')
        self.assertEqual(paid['decision'], 'payment_receipted')
        self.assertEqual(one(self.db,
                             "SELECT state FROM indemnity_orders WHERE id='order-jia'")['state'],
                         'paid')
        self.assertIsNotNone(one(self.db,
                                 "SELECT * FROM payment_receipts WHERE id='receipt-jia'"))

        # Provisional estimate never consumed the shared purse.
        used, _ = insurance._used_totals(self.db, 'pol-1', 'inc-dual', 'FY2026')
        self.assertEqual(used, 80)
        self.assertReplays()


# --------------------------------------------------------------------------- #
# Policy terms must state every required underwriting field
# --------------------------------------------------------------------------- #

class PolicyTermsValidationTests(IncidentCase):
    def test_extended_policy_persists_all_required_terms(self):
        seed_world(self.db)
        result = bind_policy(self.db)
        self.assertEqual(result['decision'], 'coverage_bound')
        terms = one(self.db, "SELECT * FROM policy_terms WHERE policy='pol-1'")
        self.assertEqual(terms['subject'], 'platform-msp')
        self.assertEqual(json.loads(terms['subjects']), ['jia', 'yi'])
        self.assertEqual((terms['window_start'], terms['window_end']), (1, 1000))
        self.assertEqual(terms['policy_year'], 'FY2026')
        self.assertEqual(terms['annual_limit'], 500)
        self.assertEqual(terms['incident_limit'], 200)
        self.assertEqual(terms['insurer_generation'], 7)
        self.assertEqual(json.loads(terms['outage_scope']),
                         ['temporary_capacity', 'write_outage'])
        self.assertEqual(json.loads(terms['recovery_scope']), ['data_recovery'])

    def test_policy_missing_required_terms_is_rejected(self):
        result = put(self.db, 'pol-bad', op='policy', id='pol-bad',
                     tenant='jia', annual_limit=100)
        self.assertEqual(result['decision'], 'incomplete_coverage_terms')
        self.assertIn('insurer_generation', result['missing'])

    def test_annual_limit_below_incident_limit_is_invalid(self):
        result = bind_policy(self.db, 'pol-bad2', annual_limit=50,
                             incident_limit=100)
        self.assertEqual(result['decision'], 'invalid_coverage_limits')


if __name__ == '__main__':
    unittest.main()
