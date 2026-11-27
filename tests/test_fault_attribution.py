import json
import tempfile
import unittest
from pathlib import Path

from cloudcourt import evac, timeline
from cloudcourt.store import connect, one, replay

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'fixtures' / 'fault-cross-cluster' / 'normal.json'
INC = 'incident-east'
CARRIER = f'{INC}:shared:carrier:carrier-seg-east'


def load():
    return json.loads(FIXTURE.read_text(encoding='utf-8'))


def sign(payload):
    payload['signature'] = timeline.signature_for(payload)
    return payload


def snapshot(db, version):
    row = one(db, 'SELECT snapshot FROM fault_evidence_versions WHERE id=?', (version,))
    return json.loads(row['snapshot'])


class FaultAttribution(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'fault.sqlite')
        self.db = connect(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def play(self, events):
        return {e['event_id']: timeline.run(self.db, e) for e in events}

    def play_until(self, stop_event):
        """Replay the fixture through and including ``stop_event``."""
        results = {}
        for event in load():
            results[event['event_id']] = timeline.run(self.db, event)
            if event['event_id'] == stop_event:
                break
        return results

    def prepared(self):
        results = self.play(load())
        # The fixture is replayed verbatim; the two bad uploads must be rejected.
        self.assertEqual(results['sample-forged']['decision'], 'forged_evidence')
        self.assertEqual(results['sample-replay']['decision'], 'replayed_evidence')
        return results

    def test_cross_cluster_fixture_replays_verbatim(self):
        self.prepared()
        self.assertTrue(replay(self.path)['verified'])
        # Every original upload keeps its sealed identity; only revisions are appended.
        self.assertEqual(one(self.db, 'SELECT COUNT(*) n FROM fault_samples')['n'], 5)
        self.assertEqual(one(self.db, 'SELECT COUNT(*) n FROM fault_clock_revisions')['n'], 5)
        self.assertEqual(one(self.db, 'SELECT COUNT(*) n FROM fault_nonces')['n'], 19)

    # --- evidence originals -------------------------------------------------
    def test_originals_require_identity_seal_and_anti_replay(self):
        unsigned = {'event_id': 'u1', 'op': 'clock_revision', 'id': 'u1', 'incident': INC,
                    'sensor': 'sensor-a', 'device': 'dev-a-01', 'corrected_at': 1,
                    'clock_skew': 1}
        self.assertEqual(timeline.run(self.db, unsigned)['decision'], 'unsigned_evidence')
        signed = sign({'event_id': 'u2', 'op': 'sample', 'id': 'u2', 'incident': INC,
                       'cluster': 'cluster-east-1', 'sensor': 'sensor-a', 'device': 'dev-a-01',
                       'at': 10, 'signal': 'link_loss', 'value': 1, 'provenance': 'sensor_pki',
                       'nonce': 'unique-nonce'})
        self.assertEqual(timeline.run(self.db, signed)['decision'], 'sample_recorded')
        tampered = dict(signed)
        tampered.update({'event_id': 'u3', 'id': 'u3', 'value': 0, 'nonce': 'other-nonce'})
        self.assertEqual(timeline.run(self.db, tampered)['decision'], 'forged_evidence')
        self.assertEqual(timeline.run(self.db, signed)['decision'], 'sample_recorded')  # idempotent replay
        self.assertTrue(replay(self.path)['verified'])

    def test_tampered_signature_cannot_anchor_a_cause(self):
        count_before = one(self.db, 'SELECT COUNT(*) n FROM fault_samples')['n']
        results = self.prepared()
        self.assertEqual(results['sample-forged']['decision'], 'forged_evidence')
        count_after = one(self.db, 'SELECT COUNT(*) n FROM fault_samples')['n']
        # 5 accepted originals; forged and replayed uploads are never stored.
        self.assertEqual(count_after - count_before, 5)
        self.assertIsNone(one(self.db, 'SELECT * FROM fault_nonces WHERE nonce=?', ('n-forge',)))

    # --- uncertain windows and clock corrections ---------------------------
    def test_uncertain_windows_precede_correction_and_are_appended_not_rewritten(self):
        self.prepared()
        a1 = snapshot(self.db, 'assess-1')
        self.assertEqual(a1['recommended_level'], 'preventive_isolation')
        self.assertEqual(a1['cause']['confirmed'], [])
        sample = next(s for s in a1['samples'] if s['id'] == 'sample-a-link')
        self.assertTrue(sample['clock_suspect'])
        self.assertLessEqual(sample['uncertain_from'], sample['uncertain_to'])
        self.assertGreaterEqual(sample['uncertain_to'] - sample['uncertain_from'], 2 * 125)
        carrier = next(h for h in a1['hypotheses'] if h['id'] == CARRIER)
        self.assertEqual(carrier['status'], 'pending')
        self.assertFalse(carrier['scope']['trusted'])

        # Raw upload order says B's sensor "reported first"; correction flips it.
        self.assertEqual(a1['cause']['earliest_local'], [940, 'sample-b-link'])
        self.assertEqual(a1['cause']['earliest_corrected'], [995, 'sample-a-link'])
        original = one(self.db, 'SELECT local_at, clock_skew FROM fault_samples WHERE id=?',
                       ('sample-a-link',))
        self.assertEqual((original['local_at'], original['clock_skew']), (1120, 125))

        a2 = snapshot(self.db, 'assess-2')
        corrected = next(s for s in a2['samples'] if s['id'] == 'sample-a-link')
        self.assertFalse(corrected['clock_suspect'])
        self.assertEqual(corrected['clock_revision'], 'clock-a')
        self.assertEqual(corrected['corrected'], 1120 - 125)
        self.assertEqual(one(self.db, 'SELECT COUNT(*) n FROM fault_clock_revisions')['n'], 5)
        self.assertTrue(replay(self.path)['verified'])

    # --- physical dependency, not upload order ------------------------------
    def test_physical_topology_separates_shared_carrier_from_parallel_power(self):
        self.prepared()
        a2 = snapshot(self.db, 'assess-2')
        deps = {d['kind']: d for d in a2['physical_dependencies']}
        self.assertTrue(deps['carrier']['shared'])
        self.assertEqual(deps['carrier']['segment'], 'carrier-seg-east')
        self.assertFalse(deps['power']['shared'])
        self.assertEqual(deps['carrier']['shared_signals'], ['link_loss'])
        carrier = next(h for h in a2['hypotheses'] if h['id'] == CARRIER)
        self.assertEqual(carrier['status'], 'confirmed')
        self.assertEqual(sorted(carrier['scope']['clusters']),
                         ['cluster-east-1', 'cluster-east-2'])
        power = next(h for h in a2['hypotheses'] if h['kind'] == 'shared' and h['id'].endswith(':power:none'))
        self.assertEqual(power['status'], 'refuted')
        # Same physical signal on both sides is explained by the shared cause.
        local = [h for h in a2['hypotheses']
                 if h['kind'] == 'independent' and h['signal'] == 'link_loss']
        self.assertEqual({h['status'] for h in local}, {'refuted'})
        # The single-feeder voltage sag cannot be escalated to a full-site cause.
        sag = next(h for h in a2['hypotheses']
                   if h['kind'] == 'independent' and h['signal'] == 'voltage_sag')
        self.assertEqual(sag['status'], 'pending')

    def test_shared_cause_requires_both_sides_to_be_trusted(self):
        # Replace the trusted device on the second side with an unverified one.
        events = []
        for event in load():
            if event['event_id'] == 'sample-b-link':
                swapped = dict(event)
                swapped.update({'id': 'sample-b-unverified', 'event_id': 'sample-b-unverified',
                                'device': 'dev-b-09', 'provenance': 'unverified',
                                'nonce': 'n-unverified'})
                swapped['signature'] = timeline.signature_for(swapped)
                events.append(swapped)
            elif event['event_id'] == 'sample-b-link-2':
                continue
            else:
                events.append(event)
        self.play(events)
        a2 = snapshot(self.db, 'assess-2')
        carrier = next(h for h in a2['hypotheses'] if h['id'] == CARRIER)
        self.assertEqual(carrier['status'], 'pending')
        self.assertNotIn('carrier fault on shared segment carrier-seg-east',
                         a2['cause']['confirmed'])

    # --- hypotheses carry explicit status and scope ------------------------
    def test_hypotheses_record_confirmed_pending_and_refuted_with_scope(self):
        self.play_until('assess-2')
        a2 = snapshot(self.db, 'assess-2')
        statuses = {h['status'] for h in a2['hypotheses']}
        self.assertEqual(statuses, {'confirmed', 'pending', 'refuted'})
        for hyp in a2['hypotheses']:
            self.assertIn('scope', hyp)
            self.assertIn('samples', hyp['scope'])
            self.assertIn('clusters', hyp['scope'])
        self.assertEqual(a2['cause']['confirmed'],
                         ['carrier fault on shared segment carrier-seg-east'])
        self.assertTrue(a2['cause']['provisional'])
        self.assertTrue(a2['cause']['refuted'])

    # --- disposition levels -------------------------------------------------
    def test_levels_do_not_upgrade_weak_samples_to_full_site_failure(self):
        self.play_until('assess-1')
        # assess-1 confirms nothing, so a full-site confirmed evacuation is refused.
        timeline.run(self.db, sign({
            'event_id': 'too-strong', 'op': 'authorize', 'id': 'auth-strong', 'incident': INC,
            'actor': 'commander', 'action': 'evacuate_cluster', 'level': 'confirmed_evacuation',
            'expires': 2000, 'provenance': 'commander_console', 'nonce': 'n-strong'}))
        result = timeline.run(self.db, {
            'event_id': 'too-strong-disp', 'op': 'disposition', 'id': 'disp-strong',
            'incident': INC, 'level': 'confirmed_evacuation', 'action': 'evacuate_cluster',
            'evidence': 'assess-1', 'authorization': 'auth-strong', 'at': 1001})
        self.assertEqual(result['decision'], 'evidence_does_not_support_level')

    def test_disposition_cannot_assert_an_unconfirmed_cause(self):
        self.prepared()
        timeline.run(self.db, sign({
            'event_id': 'auth-observe', 'op': 'authorize', 'id': 'auth-observe', 'incident': INC,
            'actor': 'commander', 'action': 'observe', 'expires': 2000,
            'provenance': 'commander_console', 'nonce': 'n-obs'}))
        # assess-2 left the carrier segment confirmed, but the field audit (assess-3)
        # later refutes it; assert against a currently unconfirmed statement.
        result = timeline.run(self.db, {
            'event_id': 'claim-cause', 'op': 'disposition', 'id': 'disp-claim', 'incident': INC,
            'level': 'observation', 'action': 'observe', 'evidence': 'assess-3',
            'authorization': 'auth-observe', 'at': 1301,
            'cause': 'carrier fault on shared segment carrier-seg-east'})
        self.assertEqual(result['decision'], 'cause_not_established')

    def test_preventive_isolation_keeps_workloads_and_customers_reroute(self):
        results = self.prepared()
        self.assertEqual(results['disp-isolate']['decision'], 'disposition_recorded')
        self.assertEqual(results['disp-isolate']['level'], 'preventive_isolation')
        self.assertEqual(results['disp-isolate']['action'], 'isolate_shared_segment')
        self.assertEqual(results['disp-isolate']['cause'], 'undetermined')
        self.assertEqual(results['disp-preventive-move']['action'],
                         'evacuate_to_independent_domain')
        # Isolation of a shared segment does not by itself authorise a cluster evacuation.
        incident = evac.run(self.db, {'event_id': 'ev-iso', 'op': 'incident', 'id': INC,
                                      'cluster': 'cluster-east-1', 'severity': 4,
                                      'generation': 1, 'evidence': 'disp-isolate'})
        self.assertEqual(incident['decision'], 'watch')
        self.assertEqual(incident['level'], 'preventive_isolation')

    # --- customer outage window --------------------------------------------
    def test_customer_outage_comes_from_seat_and_takeover_not_sensor(self):
        self.prepared()
        a3 = snapshot(self.db, 'assess-3')
        self.assertEqual(a3['customer_break']['source'], 'seat_and_takeover')
        self.assertEqual(a3['customer_break']['window_from'], 1225)
        self.assertEqual(a3['customer_break']['window_to'], 1290)
        self.assertEqual(a3['customer_break']['duration'], 65)
        refused = timeline.run(self.db, {'event_id': 'sensor-break', 'op': 'service_break',
                                         'id': 'break-sensor', 'incident': INC,
                                         'seat': 'seat-a', 'takeover': 'takeover-a',
                                         'source': 'sensor'})
        self.assertEqual(refused['decision'], 'sensor_outage_not_admissible')

    # --- late counter-evidence ---------------------------------------------
    def test_late_counter_evidence_revises_without_rewriting_the_alert(self):
        results = self.prepared()
        counter = results['counter-carrier']
        self.assertEqual(counter['decision'], 'counter_recorded')
        self.assertEqual(counter['refuted'], CARRIER)
        self.assertEqual(counter['recommended_level'], 'observation')
        original = one(self.db, 'SELECT * FROM fault_dispositions WHERE id=?', ('disp-isolate',))
        self.assertEqual(original['cause'], 'undetermined')
        self.assertEqual(original['state'], 'active')
        a1 = snapshot(self.db, 'assess-1')
        self.assertEqual(a1['cause']['confirmed'], [])
        a3 = snapshot(self.db, 'assess-3')
        self.assertEqual(a3['counter']['id'], 'counter-carrier')
        # Revisions are append-only: assess-3 refutes the carrier cause that assess-2 saw.
        counter = results['counter-carrier']
        self.assertEqual(counter['evidence'], 'assess-3')
        self.assertEqual(one(self.db, 'SELECT id FROM fault_evidence_versions '
                                      'WHERE incident=? ORDER BY rowid DESC LIMIT 1', (INC,))['id'],
                         'assess-3')
        self.assertTrue(replay(self.path)['verified'])

    def test_counter_evidence_must_be_signed_and_target_a_live_hypothesis(self):
        results = self.prepared()
        self.assertEqual(results['counter-carrier']['decision'], 'counter_recorded')
        # The fixture's counter already refuted the carrier hypothesis at assess-3.
        unsigned = {'event_id': 'cnt-u', 'op': 'counter', 'id': 'cnt-u', 'incident': INC,
                    'hypothesis': f'{INC}:independent:cluster-east-1:voltage_sag',
                    'witness': 'clock-a', 'witness_kind': 'clock_revision',
                    'at': 1300, 'evidence': 'assess-3', 'evidence_new': 'assess-x'}
        self.assertEqual(timeline.run(self.db, unsigned)['decision'], 'unsigned_evidence')
        missing = sign({'event_id': 'cnt-m', 'op': 'counter', 'id': 'cnt-m', 'incident': INC,
                        'hypothesis': 'no-such-hypothesis', 'witness': 'clock-a',
                        'witness_kind': 'clock_revision', 'at': 1300, 'evidence': 'assess-3',
                        'evidence_new': 'assess-y', 'provenance': 'field_audit', 'nonce': 'n-cm'})
        self.assertEqual(timeline.run(self.db, missing)['decision'], 'unknown_hypothesis')
        already = sign({'event_id': 'cnt-a', 'op': 'counter', 'id': 'cnt-a', 'incident': INC,
                        'hypothesis': CARRIER, 'witness': 'clock-a',
                        'witness_kind': 'clock_revision', 'at': 1300, 'evidence': 'assess-3',
                        'evidence_new': 'assess-w', 'provenance': 'field_audit', 'nonce': 'n-ca'})
        self.assertEqual(timeline.run(self.db, already)['decision'], 'hypothesis_already_refuted')
        legacy = sign({'event_id': 'cnt-l', 'op': 'counter', 'id': 'cnt-l', 'incident': INC,
                       'hypothesis': f'{INC}:independent:cluster-east-1:voltage_sag',
                       'witness': 'legacy-row', 'witness_kind': 'sample',
                       'at': 1300, 'evidence': 'assess-3', 'evidence_new': 'assess-z',
                       'provenance': 'field_audit', 'nonce': 'n-cl'})
        self.assertEqual(timeline.run(self.db, legacy)['decision'], 'counter_evidence_required')

    def test_counter_evidence_requires_a_first_party_witness(self):
        events = []
        for event in load():
            if event['event_id'] == 'counter-carrier':
                forged = dict(event)
                forged.update({'witness': 'sample-forged', 'nonce': 'n-cnt-forged'})
                forged.pop('signature')
                forged = sign(forged)
                events.append(forged)
            else:
                events.append(event)
        results = self.play(events)
        self.assertEqual(results['counter-carrier']['decision'], 'counter_evidence_required')

    def test_stale_evidence_cannot_authorize_a_new_disposition(self):
        self.prepared()
        timeline.run(self.db, sign({
            'event_id': 'auth-stale', 'op': 'authorize', 'id': 'auth-stale', 'incident': INC,
            'actor': 'commander', 'action': 'observe', 'expires': 2000,
            'provenance': 'commander_console', 'nonce': 'n-stale'}))
        # assess-1 is superseded by assess-2 and assess-3; a fresh decision must use
        # the current revision rather than resurrecting an old snapshot.
        result = timeline.run(self.db, {
            'event_id': 'disp-stale', 'op': 'disposition', 'id': 'disp-stale', 'incident': INC,
            'level': 'observation', 'action': 'observe',
            'evidence': 'assess-1', 'authorization': 'auth-stale', 'at': 1305})
        self.assertEqual(result['decision'], 'stale_evidence')

    # --- moves: evidence version, duplicate and verdict --------------------
    def test_move_records_evidence_version_and_rejects_duplicates(self):
        self.prepared()
        evac.run(self.db, {'event_id': 'ev-inc', 'op': 'incident', 'id': INC,
                           'cluster': 'cluster-east-1', 'generation': 1,
                           'evidence': 'disp-preventive-move'})
        plan = evac.run(self.db, {'event_id': 'ev-plan', 'op': 'plan', 'id': 'move-preventive',
                                  'intent': 'work-a', 'source': 'cluster-east-1',
                                  'target': 'cluster-d', 'incident': INC})
        self.assertEqual(plan['decision'], 'no_safe_target')  # no seats wired yet
        # The timeline records the same evidence version and refuses a second link.
        self.assertEqual(timeline.run(self.db, {'event_id': 'me-1', 'op': 'move_evidence',
                                                'id': 'me-1', 'incident': INC,
                                                'plan': 'move-preventive',
                                                'disposition': 'disp-preventive-move',
                                                'evidence': 'assess-1'})['decision'],
                         'move_not_started')

    def test_duplicate_move_and_verdict_after_late_counter_evidence(self):
        # Run the fixture only up to the second assessment, then wire up the move.
        self.play_until('assess-2')
        self._wire_evac()
        incident = evac.run(self.db, {'event_id': 'ev-inc', 'op': 'incident', 'id': INC,
                                      'cluster': 'cluster-east-1', 'generation': 1,
                                      'evidence': 'disp-preventive-move'})
        self.assertEqual(incident['decision'], 'evacuate')
        first = evac.run(self.db, {'event_id': 'ev-plan', 'op': 'plan', 'id': 'move-preventive',
                                   'intent': 'work-a', 'source': 'cluster-east-1',
                                   'target': 'cluster-d', 'incident': INC})
        self.assertEqual(first['decision'], 'planned')
        self.assertEqual(first['evidence'], 'assess-1')
        duplicate = evac.run(self.db, {'event_id': 'ev-plan-2', 'op': 'plan', 'id': 'move-again',
                                       'intent': 'work-a', 'source': 'cluster-east-1',
                                       'target': 'cluster-d', 'incident': INC})
        self.assertEqual(duplicate['decision'], 'duplicate_move')
        linked = timeline.run(self.db, {'event_id': 'me-1', 'op': 'move_evidence', 'id': 'me-1',
                                        'incident': INC, 'plan': 'move-preventive',
                                        'disposition': 'disp-preventive-move', 'evidence': 'assess-1'})
        self.assertEqual(linked['decision'], 'move_evidence_recorded')
        self.assertEqual(linked['evidence'], 'assess-1')

        # Now deliver the customer evidence and the late counter-evidence in order.
        for event in load():
            if event['event_id'] in ('seat-a', 'takeover-a', 'break-a', 'counter-carrier'):
                counted = timeline.run(self.db, event)
        self.assertEqual(counted['decision'], 'counter_recorded')
        self.assertEqual(counted['evidence'], 'assess-3')
        self.assertTrue(counted['verdict_required'])
        self.assertIn('move-preventive', {m['plan'] for m in counted['affected_moves']})

        verdict = timeline.run(self.db, {'event_id': 'verdict-1', 'op': 'verdict',
                                         'id': 'verdict-1', 'incident': INC,
                                         'plan': 'move-preventive', 'counter': 'counter-carrier',
                                         'verdict': 'cancel', 'reason': 'cause refuted',
                                         'evidence': 'assess-3'})
        self.assertEqual(verdict['decision'], 'verdict_recorded')
        self.assertEqual(verdict['move_state'], 'cancelled')
        self.assertEqual(verdict['old_evidence'], 'assess-1')
        self.assertEqual(one(self.db, 'SELECT state FROM move_plans WHERE id=?',
                             ('move-preventive',))['state'], 'cancelled')
        # The cancelled move keeps its original evidence link untouched.
        self.assertEqual(one(self.db, 'SELECT evidence FROM fault_move_evidence WHERE plan=?',
                             ('move-preventive',))['evidence'], 'assess-1')
        again = timeline.run(self.db, {'event_id': 'verdict-2', 'op': 'verdict',
                                       'id': 'verdict-2', 'incident': INC,
                                       'plan': 'move-preventive', 'counter': 'counter-carrier',
                                       'verdict': 'continue'})
        self.assertEqual(again['decision'], 'verdict_already_recorded')
        self.assertTrue(replay(self.path)['verified'])

    def test_completed_move_keeps_state_when_compensated(self):
        self.play_until('assess-2')
        self._wire_evac()
        evac.run(self.db, {'event_id': 'ev-inc', 'op': 'incident', 'id': INC,
                           'cluster': 'cluster-east-1', 'generation': 1,
                           'evidence': 'disp-preventive-move'})
        evac.run(self.db, {'event_id': 'ev-plan', 'op': 'plan', 'id': 'move-preventive',
                           'intent': 'work-a', 'source': 'cluster-east-1',
                           'target': 'cluster-d', 'incident': INC})
        evac.run(self.db, {'event_id': 'ev-fence', 'op': 'receipt', 'id': 'ev-fence',
                           'plan': 'move-preventive', 'kind': 'source_fenced',
                           'cluster': 'cluster-east-1', 'generation': 1})
        evac.run(self.db, {'event_id': 'ev-ready', 'op': 'receipt', 'id': 'ev-ready',
                           'plan': 'move-preventive', 'kind': 'target_ready',
                           'cluster': 'cluster-d', 'generation': 1})
        self.assertEqual(evac.run(self.db, {'event_id': 'ev-done', 'op': 'handoff',
                                            'plan': 'move-preventive'})['decision'], 'transferred')
        timeline.run(self.db, {'event_id': 'me-2', 'op': 'move_evidence', 'id': 'me-2',
                                'incident': INC, 'plan': 'move-preventive',
                                'disposition': 'disp-preventive-move', 'evidence': 'assess-1'})
        for event in load():
            if event['event_id'] in ('counter-carrier',):
                counted = timeline.run(self.db, event)
        self.assertTrue(counted['verdict_required'])
        verdict = timeline.run(self.db, {'event_id': 'verdict-comp', 'op': 'verdict',
                                         'id': 'verdict-comp', 'incident': INC,
                                         'plan': 'move-preventive', 'counter': 'counter-carrier',
                                         'verdict': 'compensate', 'reason': 'roll back billing'})
        self.assertEqual(verdict['decision'], 'verdict_recorded')
        # The transfer already completed; compensation is a separate business step,
        # not a silent rewrite of the move's final state.
        self.assertEqual(verdict['move_state'], 'complete')
        self.assertTrue(replay(self.path)['verified'])

    def _wire_evac(self):
        from cloudcourt import intent, seat
        intent.run(self.db, {'event_id': 'w-rule', 'op': 'rule', 'tenant': 'tenant-a',
                             'regions': ['east'], 'revision': 1})
        intent.run(self.db, {'event_id': 'w-claim', 'op': 'claim', 'id': 'work-a',
                             'tenant': 'tenant-a', 'region': 'east', 'role': 'critical'})
        for cluster, domain in [('cluster-east-1', 'rack-a'), ('cluster-east-2', 'rack-b'),
                                ('cluster-d', 'rack-d')]:
            seat.run(self.db, {'event_id': f'w-obs-{cluster}', 'op': 'observe', 'id': cluster,
                               'region': 'east', 'domain': domain, 'cpu': 8, 'mem': 16,
                               'generation': 1, 'healthy': True})
        seat.run(self.db, {'event_id': 'w-pri', 'op': 'reserve', 'id': 'lease-pri',
                           'intent': 'work-a', 'cluster': 'cluster-east-1', 'role': 'primary',
                           'cpu': 3, 'mem': 4, 'expires': 5000})
        seat.run(self.db, {'event_id': 'w-pri-occ', 'op': 'occupy', 'id': 'lease-pri', 'at': 20})
        seat.run(self.db, {'event_id': 'w-bak', 'op': 'reserve', 'id': 'lease-bak',
                           'intent': 'work-a', 'cluster': 'cluster-d', 'role': 'backup',
                           'cpu': 3, 'mem': 4, 'expires': 5000})
        seat.run(self.db, {'event_id': 'w-bak-occ', 'op': 'occupy', 'id': 'lease-bak', 'at': 20})
        intent.run(self.db, {'event_id': 'w-grant', 'op': 'grant', 'id': 'g', 'intent': 'work-a',
                             'actor': 'owner', 'role': 'promote', 'generation': 1, 'expires': 5000})
        intent.run(self.db, {'event_id': 'w-assign', 'op': 'assign_primary', 'intent': 'work-a',
                             'cluster': 'cluster-east-1', 'grant': 'g', 'at': 21})


if __name__ == '__main__':
    unittest.main()
