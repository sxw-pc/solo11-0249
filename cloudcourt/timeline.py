import hashlib
import hmac
import json

from .store import act, many, one

# Evidence signatures use a fixed control-plane key.  The key is deliberately
# public in this offline simulator: uploads must still carry a provenance label,
# a one-shot nonce and a matching seal, so forged or replayed records are
# distinguishable from real ones.  Legacy rows without provenance remain
# replayable for backward compatibility but can never confirm a cause.
SECRET = 'cloudcourt-fault-evidence-v1'

LEVELS = ('observation', 'preventive_isolation', 'confirmed_evacuation')
LEVEL_RANK = {name: rank for rank, name in enumerate(LEVELS)}
EVACUATION_ACTIONS = ('evacuate_to_independent_domain', 'evacuate_cluster')

SCHEMA = '''
CREATE TABLE IF NOT EXISTS fault_samples(
    id TEXT PRIMARY KEY, incident TEXT, cluster TEXT, sensor TEXT, device TEXT,
    local_at INTEGER, signal TEXT, value INTEGER, reliability INTEGER,
    clock_skew INTEGER, suspect_from INTEGER, suspect_to INTEGER,
    window_from INTEGER, window_to INTEGER, provenance TEXT, nonce TEXT, signature TEXT);
CREATE TABLE IF NOT EXISTS fault_findings(id TEXT PRIMARY KEY, incident TEXT, cause TEXT, first_seen INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS fault_clock_revisions(
    id TEXT PRIMARY KEY, incident TEXT, sensor TEXT, device TEXT, corrected_at INTEGER,
    clock_skew INTEGER, suspect_from INTEGER, suspect_to INTEGER, reason TEXT,
    provenance TEXT, nonce TEXT, signature TEXT);
CREATE TABLE IF NOT EXISTS fault_topology(
    id TEXT PRIMARY KEY, incident TEXT, kind TEXT, member TEXT, parent TEXT,
    provenance TEXT, nonce TEXT, signature TEXT);
CREATE TABLE IF NOT EXISTS fault_evidence_versions(
    id TEXT PRIMARY KEY, incident TEXT, at INTEGER, snapshot TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS fault_authorizations(
    id TEXT PRIMARY KEY, incident TEXT, actor TEXT, action TEXT, level TEXT, expires INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS fault_dispositions(
    id TEXT PRIMARY KEY, incident TEXT, level TEXT, action TEXT, evidence TEXT,
    authorization TEXT, cause TEXT, at INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS fault_seats(
    id TEXT PRIMARY KEY, incident TEXT, cluster TEXT, generation INTEGER, state TEXT, at INTEGER,
    provenance TEXT, nonce TEXT, signature TEXT);
CREATE TABLE IF NOT EXISTS fault_takeovers(
    id TEXT PRIMARY KEY, incident TEXT, seat TEXT, at INTEGER, kind TEXT, evidence TEXT,
    provenance TEXT, nonce TEXT, signature TEXT);
CREATE TABLE IF NOT EXISTS fault_outages(
    id TEXT PRIMARY KEY, incident TEXT, seat TEXT, takeover TEXT, started_at INTEGER, ended_at INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS fault_move_evidence(
    id TEXT PRIMARY KEY, incident TEXT, plan TEXT UNIQUE, evidence TEXT, disposition TEXT);
CREATE TABLE IF NOT EXISTS fault_counters(
    id TEXT PRIMARY KEY, incident TEXT, hypothesis TEXT, kind TEXT, scope TEXT, witness TEXT,
    reason TEXT, at INTEGER, evidence_version TEXT);
CREATE TABLE IF NOT EXISTS fault_verdicts(
    id TEXT PRIMARY KEY, incident TEXT, plan TEXT UNIQUE, counter TEXT, verdict TEXT,
    reason TEXT, evidence_version TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS fault_nonces(nonce TEXT PRIMARY KEY, event TEXT);
'''


def _canonical(payload):
    body = {key: value for key, value in payload.items() if key != 'signature'}
    return json.dumps(body, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def signature_for(payload, secret=SECRET):
    """Seal a first-party evidence payload.  Exposed so operators can sign records."""
    return hmac.new(secret.encode('utf-8'), _canonical(payload).encode('utf-8'), hashlib.sha256).hexdigest()


def _seal(db, payload, strict=True):
    """Validate identity, integrity and replay state of an evidence upload."""
    provenance = payload.get('provenance')
    if not provenance:
        return 'unsigned_evidence' if strict else None
    signature = payload.get('signature')
    nonce = payload.get('nonce')
    if not signature or not nonce:
        return 'unsigned_evidence'
    if not hmac.compare_digest(str(signature), signature_for(payload)):
        return 'forged_evidence'
    if one(db, 'SELECT 1 FROM fault_nonces WHERE nonce=?', (nonce,)):
        return 'replayed_evidence'
    db.execute('INSERT INTO fault_nonces VALUES(?,?)', (nonce, payload.get('event_id')))
    return None


def _chain(parents, kind, node):
    chain = []
    seen = set()
    while (kind, node) in parents and node not in seen:
        seen.add(node)
        node = parents[(kind, node)]
        chain.append(node)
    return chain


def _common_ancestor(parents, kind, clusters):
    chains = [_chain(parents, kind, cluster) for cluster in clusters]
    if not chains or any(not chain for chain in chains):
        return None
    for candidate in chains[0]:
        if all(candidate in chain for chain in chains[1:]):
            return candidate
    return None


def _effective_time(sample, revision):
    skew = revision['clock_skew'] if revision else (sample['clock_skew'] or 0)
    suspect_from = revision['suspect_from'] if revision else sample['suspect_from']
    suspect_to = revision['suspect_to'] if revision else sample['suspect_to']
    corrected = sample['local_at'] - skew
    affected = (suspect_from is not None and suspect_to is not None
                and suspect_from <= sample['local_at'] <= suspect_to)
    # A revision reconciles the offset, so a previously suspicious clock is
    # trusted again; without one the window stays inflated by the known skew.
    suspect = affected and revision is None
    reliability = abs(int(sample['reliability'] or 0))
    uncertainty = reliability + (abs(skew) if suspect else 0)
    provenance = sample['provenance'] or 'legacy'
    trusted = provenance not in ('legacy', 'unverified') and not suspect
    return {
        'corrected': corrected,
        'uncertain_from': corrected - uncertainty,
        'uncertain_to': corrected + uncertainty,
        'clock_affected': affected,
        'clock_suspect': suspect,
        'trusted': trusted,
        'provenance': provenance,
        'device': sample['device'] or sample['sensor'],
        'uncertainty': uncertainty,
        'clock_revision': revision['id'] if revision else None,
    }


def _window(rows):
    if not rows:
        return {'window_from': None, 'window_to': None}
    return {
        'window_from': min(row['uncertain_from'] for row in rows),
        'window_to': max(row['uncertain_to'] for row in rows),
    }


def _overlap(rows):
    if not rows:
        return False
    intervals = [(row['uncertain_from'], row['uncertain_to']) for row in rows]
    return max(start for start, _ in intervals) <= min(end for _, end in intervals)


def _overlap_by_cluster(rows):
    """Cluster windows must intersect; sensor upload order carries no causal meaning."""
    clusters = {}
    for row in rows:
        low, high = clusters.get(row['cluster'], (row['uncertain_from'], row['uncertain_to']))
        clusters[row['cluster']] = (min(low, row['uncertain_from']), max(high, row['uncertain_to']))
    if len(clusters) < 2:
        return False
    intervals = list(clusters.values())
    return max(start for start, _ in intervals) <= min(end for _, end in intervals)


def _hypothesis(identifier, kind, signal, statement, status, scope, reason):
    return {
        'id': identifier,
        'kind': kind,
        'signal': signal,
        'statement': statement,
        'status': status,
        'scope': scope,
        'reason': reason,
    }


def _scope(rows, clusters, segment, signal):
    scope = {
        'clusters': clusters,
        'segment': segment,
        'signal': signal,
        'samples': [row['id'] for row in rows],
        'devices': sorted({row['device'] for row in rows}),
        'trusted': bool(rows) and all(row['trusted'] for row in rows),
        'clock_suspect': any(row['clock_suspect'] for row in rows),
        'overlap': _overlap(rows),
    }
    scope.update(_window(rows))
    return scope


def _recommend(hypotheses, anchor):
    anchor_confirmed = any(
        hyp['kind'] == 'independent' and hyp['signal'] is not None
        and hyp['status'] == 'confirmed' and hyp['scope']['clusters'] == [anchor]
        for hyp in hypotheses)
    shared_active = any(
        hyp['kind'] == 'shared' and hyp['status'] in ('confirmed', 'pending')
        for hyp in hypotheses)
    if anchor_confirmed:
        return 'confirmed_evacuation'
    if shared_active:
        return 'preventive_isolation'
    return 'observation'


def _assess(db, incident, at, anchor=None, forced=None):
    samples = many(db, 'SELECT * FROM fault_samples WHERE incident=? ORDER BY rowid', (incident,))
    revisions = many(db, 'SELECT * FROM fault_clock_revisions WHERE incident=? ORDER BY rowid', (incident,))
    latest = {}
    for revision in revisions:
        latest[(revision['sensor'], revision['device'])] = revision

    effective = []
    excluded = []
    for sample in samples:
        low, high = sample['window_from'], sample['window_to']
        if low is not None and high is not None and not low <= sample['local_at'] <= high:
            excluded.append({'sample': sample['id'], 'reason': 'outside_sampling_window'})
            continue
        revision = latest.get((sample['sensor'], sample['device']))
        row = _effective_time(sample, revision)
        row.update({
            'id': sample['id'],
            'cluster': sample['cluster'],
            'sensor': sample['sensor'],
            'local_at': sample['local_at'],
            'signal': sample['signal'],
            'value': sample['value'],
            'sampling_window_from': sample['window_from'],
            'sampling_window_to': sample['window_to'],
        })
        effective.append(row)

    if forced is None:
        forced = {row['hypothesis'] for row in many(
            db, 'SELECT hypothesis FROM fault_counters WHERE incident=?', (incident,))}
    forced = set(forced)

    clusters = sorted({row['cluster'] for row in effective})
    anchor = anchor or (clusters[0] if clusters else None)
    topology = many(db, 'SELECT * FROM fault_topology WHERE incident=? ORDER BY rowid', (incident,))
    parents = {(row['kind'], row['member']): row['parent'] for row in topology}
    kinds = sorted({row['kind'] for row in topology})
    observed = {cluster: {row['signal'] for row in effective if row['cluster'] == cluster}
                for cluster in clusters}

    dependencies = []
    shared_hypotheses = []
    for kind in kinds:
        with_path = [cluster for cluster in clusters if _chain(parents, kind, cluster)]
        if not with_path:
            continue
        segment = _common_ancestor(parents, kind, with_path) if len(with_path) >= 2 else None
        dependency = {
            'kind': kind,
            'segment': segment,
            'clusters': with_path,
            'shared': segment is not None,
            'shared_signals': [],
        }
        dependencies.append(dependency)
        if len(with_path) < 2:
            continue
        if segment is None:
            scope = _scope([row for row in effective if row['cluster'] in with_path],
                           with_path, None, None)
            scope['signals'] = []
            scope['overlap'] = False
            shared_hypotheses.append(_hypothesis(
                f'{incident}:shared:{kind}:none', 'shared', None,
                f'shared {kind} fault across {", ".join(with_path)}', 'refuted',
                scope, 'no common physical ancestor in declared topology'))
            continue
        involved = [cluster for cluster in with_path
                    if segment in _chain(parents, kind, cluster)]
        common = set.intersection(*[observed[cluster] for cluster in involved])
        dependency['shared_signals'] = sorted(common)
        rows = [row for row in effective
                if row['cluster'] in involved and row['signal'] in common]
        scope = _scope(rows, involved, segment,
                       sorted(common)[0] if len(common) == 1 else None)
        scope['signals'] = sorted(common)
        scope['overlap'] = _overlap_by_cluster(rows)
        # A shared segment is only confirmed when each side contributes its own
        # trusted device and the corrected windows intersect across the segment.
        covered = {row['cluster'] for row in rows if row['trusted']}
        scope['covered_clusters'] = sorted(covered)
        identifier = f'{incident}:shared:{kind}:{segment}'
        if identifier in forced:
            status, reason = 'refuted', 'refuted by later signed counter-evidence'
        elif not common:
            status = 'pending'
            reason = 'no signal was observed on both sides of the shared segment'
        elif all(cluster in covered for cluster in involved) and scope['overlap'] and len(scope['devices']) >= 2:
            status = 'confirmed'
            reason = 'independent devices on both sides of the shared segment overlap after clock correction'
        elif scope['clock_suspect']:
            status = 'pending'
            reason = 'clock suspicion keeps the shared segment hypothesis provisional'
        elif not scope['overlap']:
            status = 'pending'
            reason = 'corrected windows do not overlap; upload order is not causal order'
        else:
            status = 'pending'
            reason = 'trusted multi-source evidence is incomplete'
        shared_hypotheses.append(_hypothesis(
            identifier, 'shared', scope['signal'],
            f'{kind} fault on shared segment {segment}', status, scope, reason))

    def applicable(cluster):
        return [hyp for hyp in shared_hypotheses if cluster in hyp['scope']['clusters']]

    hypotheses = list(shared_hypotheses)
    for cluster in clusters:
        confirmed_shared = {signal
                            for hyp in applicable(cluster) if hyp['status'] == 'confirmed'
                            for signal in hyp['scope'].get('signals', [])}
        pending_shared = {signal
                          for hyp in applicable(cluster) if hyp['status'] == 'pending'
                          for signal in hyp['scope'].get('signals', [])}
        for signal in sorted(observed[cluster]):
            rows = [row for row in effective
                    if row['cluster'] == cluster and row['signal'] == signal]
            scope = _scope(rows, [cluster], None, signal)
            identifier = f'{incident}:independent:{cluster}:{signal}'
            if identifier in forced:
                status, reason = 'refuted', 'refuted by later signed counter-evidence'
            elif signal in confirmed_shared:
                status = 'refuted'
                reason = 'a confirmed shared-segment cause already explains this signal'
            elif signal in pending_shared:
                status = 'pending'
                reason = 'a shared-dependency hypothesis must be refuted before a local cause is confirmed'
            elif scope['trusted'] and scope['overlap'] and len(scope['devices']) >= 2:
                status = 'confirmed'
                reason = 'independent devices on this cluster overlap after clock correction'
            else:
                status = 'pending'
                reason = 'insufficient or clock-suspect local evidence'
            hypotheses.append(_hypothesis(
                identifier, 'independent', signal,
                f'independent {cluster} {signal} fault', status, scope, reason))

    clock_rows = [row for row in effective if row['clock_suspect']]
    clock_scope = _scope(clock_rows, sorted({row['cluster'] for row in clock_rows}), None, None)
    hypotheses.append(_hypothesis(
        f'{incident}:clock', 'clock', None,
        'sensor clock error can reorder the recorded first report',
        'pending' if clock_rows else 'refuted', clock_scope,
        'samples fall inside a declared suspicious clock interval' if clock_rows
        else 'no declared clock suspicion affects the samples'))

    confirmed = [hyp for hyp in hypotheses if hyp['status'] == 'confirmed']
    provisional = [hyp for hyp in hypotheses if hyp['status'] == 'pending']
    refuted = [hyp for hyp in hypotheses if hyp['status'] == 'refuted']
    earliest_local = list(min((row['local_at'], row['id']) for row in effective)) if effective else None
    earliest_corrected = None
    if effective:
        first = min(effective, key=lambda row: (row['corrected'], row['id']))
        earliest_corrected = [first['corrected'], first['id']]

    outages = many(db, 'SELECT * FROM fault_outages WHERE incident=? ORDER BY rowid', (incident,))
    customer_break = {'source': 'none', 'window_from': None, 'window_to': None, 'duration': 0}
    if outages:
        outage = outages[0]
        customer_break = {
            'source': 'seat_and_takeover',
            'seat': outage['seat'],
            'takeover': outage['takeover'],
            'window_from': outage['started_at'],
            'window_to': outage['ended_at'],
            'duration': outage['ended_at'] - outage['started_at'],
        }

    return {
        'incident': incident,
        'anchor': anchor,
        'at': at,
        'physical_dependencies': dependencies,
        'hypotheses': hypotheses,
        'cause': {
            'summary': ' & '.join(hyp['statement'] for hyp in confirmed) if confirmed else 'undetermined',
            'confirmed': [hyp['statement'] for hyp in confirmed],
            'provisional': [hyp['statement'] for hyp in provisional],
            'refuted': [hyp['statement'] for hyp in refuted],
            'anchor': anchor,
            'earliest_local': earliest_local,
            'earliest_corrected': earliest_corrected,
        },
        'recommended_level': _recommend(hypotheses, anchor),
        'samples': [{
            'id': row['id'],
            'cluster': row['cluster'],
            'sensor': row['sensor'],
            'device': row['device'],
            'signal': row['signal'],
            'value': row['value'],
            'local_at': row['local_at'],
            'corrected': row['corrected'],
            'uncertain_from': row['uncertain_from'],
            'uncertain_to': row['uncertain_to'],
            'sampling_window_from': row['sampling_window_from'],
            'sampling_window_to': row['sampling_window_to'],
            'trusted': row['trusted'],
            'clock_suspect': row['clock_suspect'],
            'clock_revision': row['clock_revision'],
        } for row in effective],
        'excluded': excluded,
        'customer_break': customer_break,
    }


def _handle_sample(db, event):
    if event.get('provenance') and not event.get('device'):
        return {'decision': 'missing_device_identity'}
    error = _seal(db, event, strict=False)
    if error:
        return {'decision': error}
    db.execute('INSERT INTO fault_samples VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
        event['id'], event.get('incident', ''), event['cluster'], event['sensor'],
        event.get('device'), event['at'], event['signal'], event['value'],
        event.get('reliability', 0), event.get('clock_skew', 0),
        event.get('suspect_from'), event.get('suspect_to'),
        event.get('window_from'), event.get('window_to'),
        event.get('provenance', 'legacy'), event.get('nonce'), event.get('signature')))
    return {'decision': 'sample_recorded'}


def _handle_clock_revision(db, event):
    if not event.get('device') or not event.get('sensor'):
        return {'decision': 'missing_device_identity'}
    error = _seal(db, event)
    if error:
        return {'decision': error}
    db.execute('INSERT INTO fault_clock_revisions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', (
        event['id'], event['incident'], event['sensor'], event['device'],
        event['corrected_at'], event['clock_skew'], event.get('suspect_from'),
        event.get('suspect_to'), event.get('reason', ''), event['provenance'],
        event['nonce'], event['signature']))
    return {'decision': 'clock_revision_appended'}


def _handle_topology(db, event):
    if event['kind'] not in ('carrier', 'power'):
        return {'decision': 'unknown_topology_kind'}
    error = _seal(db, event)
    if error:
        return {'decision': error}
    db.execute('INSERT INTO fault_topology VALUES(?,?,?,?,?,?,?,?)', (
        event['id'], event['incident'], event['kind'], event['member'],
        event['parent'], event['provenance'], event['nonce'], event['signature']))
    return {'decision': 'topology_recorded'}


def _handle_assess(db, event):
    snapshot = _assess(db, event['incident'], event['at'], event.get('cluster'))
    encoded = json.dumps(snapshot, sort_keys=True, ensure_ascii=False)
    db.execute('INSERT INTO fault_evidence_versions VALUES(?,?,?,?,?)', (
        event['id'], event['incident'], event['at'], encoded, 'open'))
    result = {'decision': 'assessed', 'evidence': event['id']}
    result.update(snapshot)
    return result


def _handle_authorize(db, event):
    if event['action'] not in ('observe', 'isolate_shared_segment', *EVACUATION_ACTIONS):
        return {'decision': 'unknown_action'}
    error = _seal(db, event)
    if error:
        return {'decision': error}
    db.execute('INSERT INTO fault_authorizations VALUES(?,?,?,?,?,?,?)', (
        event['id'], event['incident'], event['actor'], event['action'],
        event.get('level'), event['expires'], 'active'))
    return {'decision': 'authorized'}


def _handle_disposition(db, event):
    revision = one(db, 'SELECT * FROM fault_evidence_versions WHERE id=?', (event['evidence'],))
    if not revision or revision['incident'] != event['incident']:
        return {'decision': 'evidence_unavailable'}
    latest = one(db, 'SELECT id FROM fault_evidence_versions WHERE incident=? ORDER BY rowid DESC LIMIT 1',
                 (event['incident'],))
    if not latest or latest['id'] != event['evidence']:
        return {'decision': 'stale_evidence'}
    snapshot = json.loads(revision['snapshot'])
    level = event['level']
    action = event['action']
    if level not in LEVELS:
        return {'decision': 'invalid_disposition_level'}
    if LEVEL_RANK[level] > LEVEL_RANK[snapshot['recommended_level']]:
        return {'decision': 'evidence_does_not_support_level'}
    confirmed = bool(snapshot['cause']['confirmed'])
    if level == 'confirmed_evacuation':
        if action not in ('evacuate_cluster', 'evacuate_to_independent_domain'):
            return {'decision': 'invalid_action_for_level'}
        if not confirmed:
            return {'decision': 'cause_not_confirmed'}
    elif level == 'preventive_isolation':
        if action not in ('isolate_shared_segment', 'evacuate_to_independent_domain'):
            return {'decision': 'invalid_action_for_level'}
    elif action != 'observe':
        return {'decision': 'invalid_action_for_level'}

    claimed = event.get('cause')
    if claimed and claimed not in snapshot['cause']['confirmed']:
        return {'decision': 'cause_not_established'}

    authorization = one(db, 'SELECT * FROM fault_authorizations WHERE id=?', (event['authorization'],))
    if (not authorization or authorization['incident'] != event['incident']
            or authorization['state'] != 'active'
            or authorization['expires'] < event['at']
            or authorization['action'] != action):
        return {'decision': 'authorization_denied'}

    cause = claimed or snapshot['cause']['summary']
    db.execute('INSERT INTO fault_dispositions VALUES(?,?,?,?,?,?,?,?,?)', (
        event['id'], event['incident'], level, action, event['evidence'],
        event['authorization'], cause, event['at'], 'active'))
    return {
        'decision': 'disposition_recorded',
        'level': level,
        'action': action,
        'cause': cause,
        'evidence': event['evidence'],
        'recommended_level': snapshot['recommended_level'],
    }


def _handle_seat(db, event):
    if event['state'] not in ('lost', 'restored'):
        return {'decision': 'unknown_seat_state'}
    error = _seal(db, event)
    if error:
        return {'decision': error}
    db.execute('INSERT INTO fault_seats VALUES(?,?,?,?,?,?,?,?,?)', (
        event['id'], event['incident'], event['cluster'], event['generation'],
        event['state'], event['at'], event['provenance'], event['nonce'], event['signature']))
    return {'decision': 'seat_recorded'}


def _handle_takeover(db, event):
    if event['kind'] not in ('human', 'automated'):
        return {'decision': 'unknown_takeover_kind'}
    seat = one(db, 'SELECT * FROM fault_seats WHERE id=?', (event['seat'],))
    if not seat or seat['incident'] != event['incident']:
        return {'decision': 'seat_unavailable'}
    error = _seal(db, event)
    if error:
        return {'decision': error}
    db.execute('INSERT INTO fault_takeovers VALUES(?,?,?,?,?,?,?,?,?)', (
        event['id'], event['incident'], event['seat'], event['at'], event['kind'],
        event.get('evidence'), event['provenance'], event['nonce'], event['signature']))
    return {'decision': 'takeover_recorded'}


def _handle_service_break(db, event):
    if event.get('source') == 'sensor':
        return {'decision': 'sensor_outage_not_admissible'}
    seat = one(db, 'SELECT * FROM fault_seats WHERE id=?', (event['seat'],))
    takeover = one(db, 'SELECT * FROM fault_takeovers WHERE id=?', (event['takeover'],))
    if (not seat or not takeover or seat['incident'] != event['incident']
            or takeover['incident'] != event['incident'] or takeover['seat'] != seat['id']):
        return {'decision': 'not_first_party_evidence'}
    if seat['state'] != 'lost' or takeover['at'] < seat['at']:
        return {'decision': 'outage_window_unavailable'}
    db.execute('INSERT INTO fault_outages VALUES(?,?,?,?,?,?,?)', (
        event['id'], event['incident'], seat['id'], takeover['id'],
        seat['at'], takeover['at'], 'recorded'))
    return {
        'decision': 'customer_break_recorded',
        'window_from': seat['at'],
        'window_to': takeover['at'],
        'duration': takeover['at'] - seat['at'],
    }


def _handle_move_evidence(db, event):
    plan = one(db, 'SELECT * FROM move_plans WHERE id=?', (event['plan'],))
    if not plan:
        return {'decision': 'move_not_started'}
    if plan['incident'] != event['incident']:
        return {'decision': 'move_not_started'}
    disposition = one(db, 'SELECT * FROM fault_dispositions WHERE id=?', (event['disposition'],))
    if (not disposition or disposition['incident'] != event['incident']
            or plan['evidence'] != disposition['id']):
        return {'decision': 'evidence_mismatch'}
    if plan['state'] not in ('planned', 'complete'):
        return {'decision': 'move_not_active'}
    if one(db, 'SELECT 1 FROM fault_move_evidence WHERE plan=?', (event['plan'],)):
        return {'decision': 'duplicate_move_evidence'}
    db.execute('INSERT INTO fault_move_evidence VALUES(?,?,?,?,?)', (
        event['id'], event['incident'], event['plan'], disposition['evidence'], disposition['id']))
    return {'decision': 'move_evidence_recorded', 'plan': event['plan'],
            'evidence': disposition['evidence'], 'disposition': disposition['id']}


def _handle_counter(db, event):
    version = one(db, 'SELECT rowid,* FROM fault_evidence_versions WHERE id=?', (event['evidence'],))
    if not version or version['incident'] != event['incident']:
        return {'decision': 'evidence_unavailable'}
    latest = one(db, 'SELECT rowid,id,snapshot FROM fault_evidence_versions WHERE incident=? ORDER BY rowid DESC LIMIT 1',
                 (event['incident'],))
    if not latest or latest['id'] != event['evidence']:
        return {'decision': 'stale_evidence'}
    snapshot = json.loads(version['snapshot'])
    target = next((hyp for hyp in snapshot['hypotheses'] if hyp['id'] == event['hypothesis']), None)
    if not target:
        return {'decision': 'unknown_hypothesis'}
    if target['status'] == 'refuted':
        return {'decision': 'hypothesis_already_refuted'}
    witness = event.get('witness')
    witness_kind = event.get('witness_kind')
    if not witness or witness_kind not in ('sample', 'clock_revision', 'topology'):
        return {'decision': 'counter_evidence_required'}
    error = _seal(db, event)
    if error:
        return {'decision': error}
    if witness_kind == 'sample':
        row = one(db, 'SELECT provenance FROM fault_samples WHERE id=? AND incident=?', (witness, event['incident']))
    elif witness_kind == 'clock_revision':
        row = one(db, 'SELECT provenance FROM fault_clock_revisions WHERE id=? AND incident=?', (witness, event['incident']))
    else:
        row = one(db, 'SELECT provenance FROM fault_topology WHERE id=? AND incident=?', (witness, event['incident']))
    if not row or (row['provenance'] or 'legacy') == 'legacy':
        return {'decision': 'counter_evidence_required'}

    db.execute('INSERT INTO fault_counters VALUES(?,?,?,?,?,?,?,?,?)', (
        event['id'], event['incident'], target['id'], event.get('kind', 'evidence'),
        event.get('scope', ''), witness, event.get('reason', ''), event['at'], event['evidence']))

    revision = _assess(db, event['incident'], event['at'], snapshot['anchor'])
    for hyp in revision['hypotheses']:
        if hyp['id'] == target['id']:
            hyp['status'] = 'refuted'
            hyp['reason'] = event.get('reason') or 'refuted by later signed counter-evidence'
            hyp['counter'] = event['id']
    confirmed = [hyp['statement'] for hyp in revision['hypotheses'] if hyp['status'] == 'confirmed']
    provisional = [hyp['statement'] for hyp in revision['hypotheses'] if hyp['status'] == 'pending']
    refuted = [hyp['statement'] for hyp in revision['hypotheses'] if hyp['status'] == 'refuted']
    revision['cause'] = dict(revision['cause'])
    revision['cause'].update({
        'summary': ' & '.join(confirmed) if confirmed else 'undetermined',
        'confirmed': confirmed,
        'provisional': provisional,
        'refuted': refuted,
    })
    revision['recommended_level'] = _recommend(revision['hypotheses'], revision['anchor'])
    revision['counter'] = {'id': event['id'], 'hypothesis': target['id'],
                           'witness': witness, 'reason': event.get('reason', '')}
    encoded = json.dumps(revision, sort_keys=True, ensure_ascii=False)
    db.execute('INSERT INTO fault_evidence_versions VALUES(?,?,?,?,?)', (
        event['evidence_new'], event['incident'], event['at'], encoded, 'revised'))

    affected = []
    for plan in many(db, 'SELECT * FROM move_plans WHERE incident=? AND evidence IS NOT NULL ORDER BY rowid',
                     (event['incident'],)):
        disposition = one(db, 'SELECT * FROM fault_dispositions WHERE id=?', (plan['evidence'],))
        if not disposition:
            continue
        used = one(db, 'SELECT rowid FROM fault_evidence_versions WHERE id=?', (disposition['evidence'],))
        if used and used['rowid'] < version['rowid']:
            affected.append({'plan': plan['id'], 'state': plan['state'],
                             'evidence': disposition['evidence'],
                             'disposition': disposition['id']})
    return {
        'decision': 'counter_recorded',
        'evidence': event['evidence_new'],
        'refuted': target['id'],
        'cause': revision['cause']['summary'],
        'recommended_level': revision['recommended_level'],
        'affected_moves': affected,
        'verdict_required': bool(affected),
    }


def _handle_verdict(db, event):
    counter = one(db, 'SELECT * FROM fault_counters WHERE id=?', (event['counter'],))
    if not counter or counter['incident'] != event['incident']:
        return {'decision': 'counter_unavailable'}
    plan = one(db, 'SELECT * FROM move_plans WHERE id=?', (event['plan'],))
    if not plan or plan['incident'] != event['incident'] or not plan['evidence']:
        return {'decision': 'move_evidence_unavailable'}
    disposition = one(db, 'SELECT * FROM fault_dispositions WHERE id=?', (plan['evidence'],))
    if not disposition:
        return {'decision': 'move_evidence_unavailable'}
    previous = one(db, 'SELECT * FROM fault_verdicts WHERE plan=?', (event['plan'],))
    if previous:
        return {'decision': 'verdict_already_recorded', 'verdict': previous['verdict']}
    if event['verdict'] not in ('cancel', 'continue', 'compensate'):
        return {'decision': 'unknown_verdict'}
    counter_version = one(db, 'SELECT rowid,* FROM fault_evidence_versions WHERE id=?',
                          (counter['evidence_version'],))
    used = one(db, 'SELECT rowid FROM fault_evidence_versions WHERE id=?', (disposition['evidence'],))
    if not counter_version or not used or used['rowid'] >= counter_version['rowid']:
        return {'decision': 'no_later_counter_evidence'}
    db.execute('INSERT INTO fault_verdicts VALUES(?,?,?,?,?,?,?,?)', (
        event['id'], event['incident'], event['plan'], event['counter'],
        event['verdict'], event.get('reason', ''), counter_version['id'], 'recorded'))
    move_state = plan['state']
    if event['verdict'] == 'cancel' and plan['state'] == 'planned':
        db.execute("UPDATE move_plans SET state='cancelled' WHERE id=?", (plan['id'],))
        move_state = 'cancelled'
    return {
        'decision': 'verdict_recorded',
        'verdict': event['verdict'],
        'plan': event['plan'],
        'move_state': move_state,
        'old_evidence': disposition['evidence'],
        'counter': event['counter'],
    }


def handle(db, event):
    op = event['op']
    if op == 'sample':
        return _handle_sample(db, event)
    if op == 'finding':
        samples = many(db, 'SELECT local_at FROM fault_samples WHERE cluster=? AND signal=? ORDER BY local_at',
                       (event['cluster'], event['cause']))
        if not samples:
            return {'decision': 'cause_unobserved'}
        db.execute('INSERT INTO fault_findings VALUES(?,?,?,?,?)',
                   (event['id'], event['incident'], event['cause'], samples[0]['local_at'], 'provisional'))
        return {'decision': 'provisional_finding', 'first_seen': samples[0]['local_at']}
    if op == 'clock_revision':
        return _handle_clock_revision(db, event)
    if op == 'topology':
        return _handle_topology(db, event)
    if op == 'assess':
        return _handle_assess(db, event)
    if op == 'authorize':
        return _handle_authorize(db, event)
    if op == 'disposition':
        return _handle_disposition(db, event)
    if op == 'seat':
        return _handle_seat(db, event)
    if op == 'takeover':
        return _handle_takeover(db, event)
    if op == 'service_break':
        return _handle_service_break(db, event)
    if op == 'move_evidence':
        return _handle_move_evidence(db, event)
    if op == 'counter':
        return _handle_counter(db, event)
    if op == 'verdict':
        return _handle_verdict(db, event)
    return {'decision': 'unknown_operation'}


def run(db, event):
    return act(db, 'timeline', event, handle)
