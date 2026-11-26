"""Carbon settlement chamber.

The legacy ``order`` / ``reading`` / ``statement`` commands are preserved
verbatim in behaviour.  The settlement extension turns cluster-level shared
meter readings into per-customer, traceable and disputable carbon bills:

* readings carry meter identity, coverage window, surveyor, revision and
  original evidence; corrections only add evidence versions;
* allocation drivers keep reserved seats, actual occupancy/warm preheat and
  failover replication apart and join back to ``placement_leases`` /
  ``capacity_claims`` / ``workload_intents``;
* shares always sum (together with any explicit evidence suspense) to the
  trusted meter total;
* missing occupancy evidence is parked as a pending amount with a proof
  deadline instead of being filled from averages;
* ceiling, excess liability, payment and dispute are independent bill states,
  and signed/paid bills are never overwritten by corrections.
"""

import json
from datetime import datetime

from .store import act, one, many

SCHEMA = '''
CREATE TABLE IF NOT EXISTS carbon_orders(
  id TEXT PRIMARY KEY, tenant TEXT, period TEXT, ceiling INTEGER, state TEXT,
  meter TEXT, unit_rate INTEGER, penalty_rate INTEGER, carbon_factor INTEGER);
CREATE TABLE IF NOT EXISTS power_readings(
  id TEXT, revision INTEGER, cluster TEXT, period TEXT, units INTEGER, source TEXT,
  meter_id TEXT, window_start TEXT, window_end TEXT, surveyor TEXT,
  evidence TEXT, state TEXT, PRIMARY KEY(id, revision));
CREATE TABLE IF NOT EXISTS carbon_statements(id TEXT PRIMARY KEY, order_id TEXT, period TEXT, measured_units INTEGER, state TEXT);

CREATE TABLE IF NOT EXISTS carbon_meters(
  id TEXT PRIMARY KEY, cluster TEXT, certified INTEGER, evidence TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS settlement_agreements(
  id TEXT PRIMARY KEY, meter TEXT, period TEXT, method TEXT,
  standby_bearer TEXT, parties TEXT, confirmed_by TEXT, terms TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS period_negotiations(
  id TEXT PRIMARY KEY, meter TEXT, period TEXT, agreement TEXT,
  method TEXT, standby_bearer TEXT, confirmed_by TEXT, at TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS load_windows(
  id TEXT PRIMARY KEY, intent TEXT, tenant TEXT, cluster TEXT, lease TEXT, role TEXT,
  kind TEXT, start_ts TEXT, end_ts TEXT, load_factor INTEGER,
  peer_cluster TEXT, migration TEXT, evidence TEXT, revision INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS evidence_requests(
  id TEXT PRIMARY KEY, meter TEXT, period TEXT, intent TEXT, tenant TEXT,
  lease TEXT, kind TEXT, hold_units INTEGER, hold_amount INTEGER, reason TEXT,
  deadline TEXT, state TEXT, window_id TEXT, requested_at TEXT);
CREATE TABLE IF NOT EXISTS settlement_runs(
  id TEXT PRIMARY KEY, meter TEXT, period TEXT, negotiation TEXT,
  method TEXT, standby_bearer TEXT, reading_id TEXT, reading_revision INTEGER,
  total_units INTEGER, allocated_units INTEGER, pending_units INTEGER, pending_amount INTEGER,
  supersedes TEXT, version INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS carbon_bills(
  id TEXT PRIMARY KEY, run_id TEXT, tenant TEXT, order_id TEXT, period TEXT,
  version INTEGER, prior_bill TEXT, record_state TEXT,
  weight INTEGER, allocated_units INTEGER, pending_units INTEGER, pending_amount INTEGER,
  ceiling_units INTEGER, carbon_units INTEGER, excess_units INTEGER,
  unit_rate INTEGER, energy_amount INTEGER, penalty_rate INTEGER, penalty_amount INTEGER,
  total_due INTEGER, paid_amount INTEGER, pay_state TEXT,
  dispute_state TEXT, issue_state TEXT, issued_reading_revision INTEGER,
  balance_due INTEGER, refund_due INTEGER);
CREATE TABLE IF NOT EXISTS bill_payments(
  id TEXT PRIMARY KEY, bill TEXT, amount INTEGER, at TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS bill_disputes(
  id TEXT PRIMARY KEY, bill TEXT, tenant TEXT, reason TEXT, at TEXT,
  deadline TEXT, state TEXT, outcome TEXT, successor_bill TEXT);
'''

HOURS_METHODS = {'run_hours', 'hours', 'actual_hours', 'running_hours', 'verified_hours', 'running_time'}
CAPACITY_METHODS = {'guaranteed_capacity', 'capacity', 'reserved_capacity', 'assured_capacity', 'guaranteed'}
ACTUAL_KINDS = {'occupied', 'preheat', 'standby', 'warm_standby', 'running', 'replication'}
STANDBY_KINDS = {'preheat', 'standby', 'warm_standby'}
WINDOW_KINDS = ACTUAL_KINDS | {'reserved', 'reservation'}

OP_ALIASES = {
    'register_meter': 'meter',
    'upload_reading': 'reading', 'meter_reading': 'reading', 'record_reading': 'reading',
    'settlement_agreement': 'agreement', 'terms': 'agreement', 'contract_terms': 'agreement',
    'settlement_terms': 'agreement',
    'select_terms': 'negotiation', 'negotiate': 'negotiation', 'award': 'negotiation',
    'select_clause': 'negotiation', 'terms_selection': 'negotiation',
    'window': 'load_window', 'occupancy_window': 'load_window',
    'usage_window': 'load_window', 'seat_window': 'load_window',
    'occupancy': 'load_window', 'usage': 'load_window', 'load': 'load_window',
    'missing_window': 'evidence_request', 'request_evidence': 'evidence_request',
    'evidence_gap': 'evidence_request', 'missing_occupancy': 'evidence_request',
    'allocate': 'settle', 'apportion': 'settle', 'split': 'settle',
    'settlement': 'settle', 'run_settlement': 'settle',
    'settle_statement': 'settle', 'create_statements': 'settle',
    'allocate_reading': 'settle', 'customer_statements': 'settle',
    'sign': 'issue', 'issue_bills': 'issue', 'issue_statement': 'issue',
    'invoice': 'issue', 'issue_bill': 'issue', 'sign_bill': 'issue',
    'payment': 'pay', 'register_payment': 'pay',
    'object': 'dispute', 'objection': 'dispute', 'raise_dispute': 'dispute',
    'raise_objection': 'dispute',
    'resolve_objection': 'resolve_dispute',
    'recompute': 'recalculate', 'recalc': 'recalculate',
    'correct_bill': 'recalculate', 'reissue': 'recalculate',
    'provide_evidence': 'fulfill_evidence', 'attach_evidence': 'fulfill_evidence',
    'evidence_timeout': 'expire_evidence',
    'get_bill': 'bill', 'show_bill': 'bill', 'bill_snapshot': 'bill',
    'reconcile': 'reconcile_workload', 'workload_reconciliation': 'reconcile_workload',
}


def pick(x, *names, default=None):
    for name in names:
        if name in x and x[name] is not None:
            return x[name]
    return default


def parse_ts(value):
    """Parse an endpoint as a datetime, or pass through a numeric logical clock."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    try:
        dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except ValueError:
        pass
    try:
        number = float(text)
        return int(number) if number.is_integer() else number
    except ValueError:
        return None


def store_ts(value):
    if value is None:
        return None
    return value.isoformat() if isinstance(value, datetime) else str(value)


def period_bounds(period):
    year, month = (int(part) for part in str(period).split('-')[:2])
    start = datetime(year, month, 1)
    end_year = year + (1 if month == 12 else 0)
    end_month = 1 if month == 12 else month + 1
    return start, datetime(end_year, end_month, 1)


def duration_units(start, end):
    if isinstance(start, datetime) and isinstance(end, datetime):
        return int((end - start).total_seconds() // 60)
    return end - start


def overlap_span(start, end, low, high):
    """Overlap in minutes (datetime basis) or raw units (numeric clock basis)."""
    if start is None or end is None:
        return 0
    datetime_basis = isinstance(start, datetime) and isinstance(low, datetime)
    numeric_basis = isinstance(start, (int, float)) and isinstance(low, (int, float))
    if datetime_basis or numeric_basis:
        seg_start = max(start, low)
        seg_end = min(end, high)
        if seg_end <= seg_start:
            return 0
        if datetime_basis:
            return int((seg_end - seg_start).total_seconds() // 60)
        return seg_end - seg_start
    # Mixed bases cannot be clipped; use the window's own full span.
    return max(0, duration_units(start, end))


def allocation_bounds(reading, period):
    if reading:
        start = parse_ts(reading['window_start'])
        end = parse_ts(reading['window_end'])
        if start is not None and end is not None and type(start) is type(end):
            return start, end
    return period_bounds(period)


def normalize_method(value):
    if value in HOURS_METHODS:
        return 'run_hours'
    if value in CAPACITY_METHODS:
        return 'guaranteed_capacity'
    return None


def largest_remainder(total, weighted):
    """Allocate integer ``total`` units by integer weights; shares sum exactly."""
    weighted = [(key, int(weight)) for key, weight in weighted]
    weight_sum = sum(weight for _, weight in weighted)
    shares = {key: 0 for key, _ in weighted}
    if total <= 0 or weight_sum <= 0:
        return shares
    allocated = 0
    remainders = []
    for key, weight in weighted:
        quotient, remainder = divmod(total * weight, weight_sum)
        shares[key] = quotient
        allocated += quotient
        remainders.append((remainder, key))
    leftover = total - allocated
    # Deterministic tie break: larger remainder first, then stable key.
    for _, key in sorted(remainders, key=lambda item: (-item[0], str(item[1]))):
        if leftover <= 0:
            break
        shares[key] += 1
        leftover -= 1
    return shares


def current_reading(db, meter, period):
    return one(db, '''SELECT * FROM power_readings
                      WHERE meter_id=? AND period=? AND state='current'
                      ORDER BY revision DESC LIMIT 1''', (meter, period))


def bill_id(run_id, tenant):
    return f'{run_id}:{tenant}'


def bill_snapshot(db, bill):
    row = one(db, 'SELECT * FROM carbon_bills WHERE id=?', (bill,)) if isinstance(bill, str) \
        else one(db, 'SELECT * FROM carbon_bills WHERE id=?', (bill['id'],))
    if not row:
        return None
    payments = many(db, 'SELECT * FROM bill_payments WHERE bill=? ORDER BY id', (row['id'],))
    disputes = many(db, 'SELECT * FROM bill_disputes WHERE bill=? ORDER BY id', (row['id'],))
    return {**row, 'payments': payments, 'disputes': disputes}


def reconcile_workload(db, intent_id, period):
    """Tie a workload's verified load windows across clusters and the boundary."""
    intent = one(db, 'SELECT * FROM workload_intents WHERE id=?', (intent_id,))
    low, high = period_bounds(period)
    windows = many(db, "SELECT * FROM load_windows WHERE intent=? AND state='current' ORDER BY start_ts,id",
                   (intent_id,))
    clusters = {}
    leg_rows = []
    parsed = []
    for w in windows:
        start, end = parse_ts(w['start_ts']), parse_ts(w['end_ts'])
        in_period = overlap_span(start, end, low, high)
        factor = w['load_factor'] or 1
        weight = in_period * factor
        parsed.append((w, start, end))
        bucket = clusters.setdefault(w['cluster'], {'cluster': w['cluster'], 'minutes': 0,
                                                    'weight': 0, 'windows': 0})
        bucket['minutes'] += in_period
        bucket['weight'] += weight
        bucket['windows'] += 1
        if w['kind'] == 'replication':
            total_minutes = duration_units(start, end)
            before_low = overlap_span(start, end, start, low) if start < low else 0
            after_high = overlap_span(start, end, high, end) if end > high else 0
            leg_rows.append({'id': w['id'], 'cluster': w['cluster'], 'peer_cluster': w['peer_cluster'],
                             'total_minutes': total_minutes, 'period_minutes': in_period,
                             'before_period_minutes': before_low,
                             'next_period_minutes': after_high,
                             'split_balanced': before_low + in_period + after_high == total_minutes})
    # Same-cluster overlapping actual windows would double count one seat.
    overlaps = []
    actual = [(w, s, e) for w, s, e in parsed if w['kind'] in ACTUAL_KINDS]
    for i in range(len(actual)):
        wi, si, ei = actual[i]
        for j in range(i + 1, len(actual)):
            wj, sj, ej = actual[j]
            if (wi['cluster'] == wj['cluster'] and type(si) is type(ej)
                    and si < ej and sj < ei):
                overlaps.append({'windows': [wi['id'], wj['id']], 'cluster': wi['cluster']})
    run_weights = many(db, '''SELECT b.run_id,b.id bill,b.tenant,b.weight,b.allocated_units,
                                     r.meter,r.method,r.state run_state
                              FROM carbon_bills b JOIN settlement_runs r ON r.id=b.run_id
                              WHERE b.period=? AND b.tenant=? AND r.method='run_hours'
                              ORDER BY b.id''', (period, intent['tenant'])) if intent else []
    settled = [dict(row) for row in run_weights]
    cluster_weight = {c['cluster']: c['weight'] for c in clusters.values()}
    ties = []
    for row in settled:
        meter = one(db, 'SELECT * FROM carbon_meters WHERE id=?', (row['meter'],))
        cluster = meter['cluster'] if meter else None
        ties.append({'bill': row['bill'], 'run': row['run_id'], 'cluster': cluster,
                     'bill_weight': row['weight'],
                     'window_weight': cluster_weight.get(cluster, 0),
                     'tied': row['weight'] == cluster_weight.get(cluster, 0)})
    balanced = (not overlaps and all(leg['split_balanced'] for leg in leg_rows)
                and all(tie['tied'] for tie in ties))
    return {'intent': intent_id, 'tenant': intent['tenant'] if intent else None,
            'period': period, 'clusters': sorted(clusters.values(), key=lambda c: c['cluster']),
            'migration_legs': leg_rows, 'overlaps': overlaps,
            'settled_bills': ties, 'balanced': balanced}


def _record_reading(db, x):
    if x['units'] < 0:
        return {'decision': 'invalid_reading'}
    series = x['id']
    revision = int(x.get('revision', 1))
    meter = pick(x, 'meter', 'meter_id', default=series)
    window = pick(x, 'window', 'coverage', default=None)
    window_start = pick(x, 'start', 'window_start', 'covered_from', 'from',
                        default=window[0] if isinstance(window, (list, tuple)) else None)
    window_end = pick(x, 'end', 'window_end', 'covered_to', 'to',
                      default=window[1] if isinstance(window, (list, tuple)) else None)
    if window_start is None or window_end is None:
        low, high = period_bounds(x['period'])
        window_start = window_start if window_start is not None else low.isoformat()
        window_end = window_end if window_end is not None else high.isoformat()
    surveyor = pick(x, 'surveyor', 'measured_by', 'reader', 'recorded_by')
    evidence = pick(x, 'evidence', 'credential', 'source_ref', 'artifact', 'receipt',
                    default=x.get('source'))
    fingerprint = (
        x.get('cluster'), x['period'], x['units'], x.get('source'), meter,
        str(window_start), str(window_end), surveyor, evidence)
    existing = one(db, 'SELECT * FROM power_readings WHERE id=? AND revision=?',
                   (series, revision))
    if existing:
        same = (existing['cluster'], existing['period'], existing['units'], existing['source'],
                existing['meter_id'], existing['window_start'], existing['window_end'],
                existing['surveyor'], existing['evidence']) == fingerprint
        if same:
            return {'decision': 'duplicate_reading', 'id': series, 'revision': revision}
        return {'decision': 'reading_revision_conflict', 'id': series, 'revision': revision}
    max_rev = one(db, 'SELECT COALESCE(MAX(revision),0) rev FROM power_readings WHERE id=?',
                  (series,))['rev']
    if revision < max_rev:
        return {'decision': 'stale_reading_revision', 'id': series,
                'revision': revision, 'current_revision': max_rev}
    if revision > max_rev and max_rev > 0:
        db.execute("UPDATE power_readings SET state='superseded' WHERE id=? AND state='current'",
                   (series,))
        decision = 'reading_correction_recorded'
    else:
        decision = 'reading_recorded'
    db.execute('''INSERT INTO power_readings
                  (id,revision,cluster,period,units,source,meter_id,window_start,window_end,
                   surveyor,evidence,state)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
               (series, revision, x['cluster'], x['period'], x['units'], x.get('source'),
                meter, str(window_start), str(window_end), surveyor, evidence, 'current'))
    return {'decision': decision, 'id': series, 'revision': revision, 'meter': meter}


def _handle(db, x):
    op = OP_ALIASES.get(x.get('op'), x.get('op'))

    # ---------------------------------------------------------------- legacy
    if op == 'order':
        if x['ceiling'] < 0:
            return {'decision': 'invalid_ceiling'}
        meter = pick(x, 'meter', 'meter_id', 'cluster')
        unit_rate = pick(x, 'unit_rate', 'rate', 'price', default=1)
        penalty_rate = pick(x, 'penalty_rate', 'excess_rate', default=0)
        carbon_factor = pick(x, 'carbon_factor', 'factor', default=1)
        db.execute('''INSERT INTO carbon_orders
                      (id,tenant,period,ceiling,state,meter,unit_rate,penalty_rate,carbon_factor)
                      VALUES(?,?,?,?,?,?,?,?,?)''',
                   (x['id'], x['tenant'], x['period'], x['ceiling'], 'open', meter,
                    unit_rate, penalty_rate, carbon_factor))
        return {'decision': 'ordered'}

    if op == 'reading':
        return _record_reading(db, x)

    if op in ('correct', 'correction'):
        references_settlement = pick(x, 'supersedes', 'prior', 'replaces', 'settlement_run') \
            or x.get('run')
        looks_like_reading = 'units' in x and (
            x.get('meter') or x.get('meter_id') or x.get('reading')
            or x.get('evidence') or x.get('surveyor') or 'cluster' in x)
        if looks_like_reading and not references_settlement:
            x.setdefault('id', pick(x, 'reading', 'reading_id', 'series'))
            return _record_reading(db, x)
        return _settle(db, x, 'recalculate')

    if op == 'statement':
        order = one(db, 'SELECT * FROM carbon_orders WHERE id=?', (x['order'],))
        if not order or order['state'] != 'open':
            return {'decision': 'order_unavailable'}
        measured = one(db, 'SELECT COALESCE(SUM(units),0) units FROM power_readings WHERE period=?',
                       (order['period'],))
        db.execute('INSERT INTO carbon_statements VALUES(?,?,?,?,?)',
                   (x['id'], order['id'], order['period'], measured['units'], 'draft'))
        return {'decision': 'drafted', 'units': measured['units']}

    # ---------------------------------------------------------------- meters
    if op == 'meter':
        old = one(db, 'SELECT * FROM carbon_meters WHERE id=?', (x['id'],))
        if old:
            return {'decision': 'meter_exists'}
        db.execute('INSERT INTO carbon_meters VALUES(?,?,?,?,?)',
                   (x['id'], x['cluster'], int(bool(x.get('certified', False))),
                    pick(x, 'evidence', 'seal', 'credential'), 'active'))
        return {'decision': 'meter_registered'}

    # ------------------------------------------------------------- agreements
    if op == 'agreement':
        method = normalize_method(pick(x, 'method', 'allocation', 'basis'))
        if not method:
            return {'decision': 'invalid_allocation_method'}
        parties = pick(x, 'parties', 'tenants', default=[])
        confirmed_by = pick(x, 'confirmed_by', 'signed_by', 'signatures', default=[])
        state = x.get('state') or ('signed' if pick(x, 'signed', 'confirmed', default=False) else 'draft')
        db.execute('INSERT INTO settlement_agreements VALUES(?,?,?,?,?,?,?,?,?)',
                   (x['id'], pick(x, 'meter', 'meter_id'), x['period'], method,
                    pick(x, 'standby_bearer', 'standby', 'idle_bearer'),
                    json.dumps(list(parties), ensure_ascii=False),
                    json.dumps(list(confirmed_by), ensure_ascii=False),
                    json.dumps(x.get('terms', {}), sort_keys=True, ensure_ascii=False), state))
        return {'decision': 'agreement_recorded', 'state': state, 'method': method}

    if op == 'negotiation':
        agreement_id = pick(x, 'agreement', 'agreement_id')
        agreement = one(db, 'SELECT * FROM settlement_agreements WHERE id=?', (agreement_id,)) \
            if agreement_id else None
        if agreement_id and not agreement:
            return {'decision': 'unknown_agreement'}
        method = normalize_method(pick(x, 'method', 'allocation',
                                       default=agreement['method'] if agreement else None))
        if not method:
            return {'decision': 'invalid_allocation_method'}
        standby_bearer = pick(x, 'standby_bearer', 'standby',
                              default=agreement['standby_bearer'] if agreement else None)
        confirmed_by = pick(x, 'confirmed_by', 'by', 'parties', default=[])
        if isinstance(confirmed_by, str):
            confirmed_by = [confirmed_by]
        parties = json.loads(agreement['parties']) if agreement and agreement['parties'] else []
        if agreement and agreement['state'] != 'signed':
            return {'decision': 'agreement_not_signed'}
        if parties and not set(parties).issubset(set(confirmed_by)):
            return {'decision': 'agreement_not_confirmed', 'missing':
                    sorted(set(parties) - set(confirmed_by))}
        if not confirmed_by:
            return {'decision': 'negotiation_unconfirmed'}
        meter = pick(x, 'meter', 'meter_id', default=agreement['meter'] if agreement else None)
        period = pick(x, 'period', default=agreement['period'] if agreement else None)
        db.execute('INSERT INTO period_negotiations VALUES(?,?,?,?,?,?,?,?,?)',
                   (x['id'], meter, period, agreement_id, method, standby_bearer,
                    json.dumps(list(confirmed_by), ensure_ascii=False),
                    pick(x, 'at', 'recorded_at'), 'agreed'))
        return {'decision': 'terms_selected', 'method': method, 'standby_bearer': standby_bearer}

    # ---------------------------------------------------------- load windows
    if op == 'load_window':
        kind = x.get('kind')
        if kind not in WINDOW_KINDS:
            return {'decision': 'invalid_window_kind'}
        intent = one(db, 'SELECT * FROM workload_intents WHERE id=?', (x['intent'],))
        if not intent:
            return {'decision': 'unknown_intent'}
        tenant = pick(x, 'tenant', 'tenant_id', default=intent['tenant'])
        if x.get('tenant') and x['tenant'] != intent['tenant']:
            return {'decision': 'tenant_identity_conflict'}
        lease_id = pick(x, 'lease', 'lease_id')
        role = pick(x, 'role')
        if lease_id:
            lease = one(db, 'SELECT * FROM placement_leases WHERE id=?', (lease_id,))
            if not lease:
                return {'decision': 'unknown_lease'}
            if lease['intent'] != x['intent'] or lease['cluster'] != x['cluster']:
                return {'decision': 'lease_identity_conflict'}
            role = role or lease['role']
        start_ts, end_ts = parse_ts(x['start']), parse_ts(x['end'])
        if start_ts is None or end_ts is None or end_ts <= start_ts:
            return {'decision': 'invalid_window_span'}
        db.execute('INSERT INTO load_windows VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['intent'], tenant, x['cluster'], lease_id, role, kind,
                    store_ts(start_ts), store_ts(end_ts), int(x.get('load_factor', 1)),
                    pick(x, 'peer_cluster', 'target', 'target_cluster'),
                    pick(x, 'migration', 'plan', 'move_plan'),
                    pick(x, 'evidence', 'source', 'credential'),
                    int(x.get('revision', 1)), pick(x, 'state', default='current')))
        linked_request = pick(x, 'request', 'evidence_request')
        if linked_request:
            req = one(db, 'SELECT * FROM evidence_requests WHERE id=?', (linked_request,))
            if req and req['state'] in ('open', 'overdue'):
                db.execute("UPDATE evidence_requests SET state='fulfilled',window_id=? WHERE id=?",
                           (x['id'], linked_request))
        return {'decision': 'load_window_recorded', 'tenant': tenant, 'kind': kind}

    # ----------------------------------------------------- evidence suspense
    if op == 'evidence_request':
        hold_units = int(x.get('hold_units', x.get('pending_units', 0)))
        if hold_units < 0:
            return {'decision': 'invalid_hold_amount'}
        hold_amount = pick(x, 'hold_amount', 'pending_amount', 'amount_held')
        if hold_amount is None:
            # Default money suspense to energy charge at the tenant order rate.
            order = None
            tenant = pick(x, 'tenant', 'tenant_id')
            if tenant:
                order = one(db, "SELECT unit_rate FROM carbon_orders WHERE tenant=? AND period=?",
                            (tenant, x['period']))
            rate = order['unit_rate'] if order and order['unit_rate'] is not None else 1
            hold_amount = hold_units * rate
        db.execute('INSERT INTO evidence_requests VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], pick(x, 'meter', 'meter_id'), x['period'],
                    pick(x, 'intent'), pick(x, 'tenant', 'tenant_id'),
                    pick(x, 'lease', 'lease_id'), pick(x, 'kind', default='occupied'),
                    hold_units, int(hold_amount), x.get('reason'),
                    pick(x, 'deadline', 'due'), 'open', None, pick(x, 'at', 'requested_at')))
        return {'decision': 'evidence_requested', 'hold_units': hold_units,
                'hold_amount': int(hold_amount), 'deadline': pick(x, 'deadline', 'due')}

    if op == 'fulfill_evidence':
        req = one(db, 'SELECT * FROM evidence_requests WHERE id=?',
                  (pick(x, 'request', 'evidence_request', 'id'),))
        if not req:
            return {'decision': 'unknown_evidence_request'}
        window_id = pick(x, 'window', 'window_id')
        if window_id and not one(db, 'SELECT 1 FROM load_windows WHERE id=?', (window_id,)):
            return {'decision': 'unknown_window'}
        db.execute("UPDATE evidence_requests SET state='fulfilled',window_id=COALESCE(?,window_id) WHERE id=?",
                   (window_id, req['id']))
        return {'decision': 'evidence_accepted'}

    if op == 'expire_evidence':
        as_of = x['as_of']
        rows = many(db, "SELECT id,deadline FROM evidence_requests WHERE state='open' AND deadline IS NOT NULL ORDER BY id")
        overdue = [r['id'] for r in rows if str(r['deadline']) <= str(as_of)]
        for request_id in overdue:
            db.execute("UPDATE evidence_requests SET state='overdue' WHERE id=?", (request_id,))
        return {'decision': 'evidence_deadline_checked', 'as_of': as_of, 'overdue': overdue}

    # ------------------------------------------------------------ settlement
    if op in ('settle', 'recalculate'):
        return _settle(db, x, op)

    if op == 'issue':
        return _issue(db, x)

    if op == 'pay':
        return _pay(db, x)

    if op == 'dispute':
        return _dispute(db, x)

    if op == 'resolve_dispute':
        return _resolve_dispute(db, x)

    if op == 'bill':
        snapshot = bill_snapshot(db, pick(x, 'bill', 'id'))
        if not snapshot:
            return {'decision': 'unknown_bill'}
        return {'decision': 'bill_snapshot', **snapshot}

    if op == 'reconcile_workload':
        intent_id = pick(x, 'intent', 'workload', 'intent_id', 'id')
        report = reconcile_workload(db, intent_id, x['period'])
        report['decision'] = 'workload_reconciled'
        return report

    return {'decision': 'unknown_operation'}


def _standby_target(db, negotiation, cluster, tenants):
    bearer = negotiation['standby_bearer']
    if not bearer or bearer in ('standby_owner', 'owner', 'standby', 'self', 'tenant'):
        return 'standby_owner'
    if bearer in ('host', 'primary', 'operator', 'meter_host', 'primary_owner'):
        # The primary role holder on this meter's cluster receives standby weight.
        row = one(db, '''SELECT i.tenant FROM placement_leases l
                         JOIN workload_intents i ON i.id=l.intent
                         WHERE l.cluster=? AND l.role='primary'
                           AND l.state IN ('occupied','reserved')
                         ORDER BY CASE l.state WHEN 'occupied' THEN 0 ELSE 1 END, l.id LIMIT 1''',
                  (cluster,))
        return row['tenant'] if row else 'host'
    # Explicit tenant id.
    return bearer if bearer in tenants else bearer


def _driver_weights(db, negotiation, period, reading=None):
    method = negotiation['method']
    meter_id = negotiation['meter']
    meter = one(db, 'SELECT * FROM carbon_meters WHERE id=?', (meter_id,))
    cluster = meter['cluster'] if meter else negotiation['meter']
    low, high = allocation_bounds(reading, period)
    weights = {}
    standby_weights = {}
    if method == 'run_hours':
        rows = many(db, "SELECT * FROM load_windows WHERE cluster=? AND state='current'", (cluster,))
        for w in rows:
            if w['kind'] not in ACTUAL_KINDS:
                continue
            start, end = parse_ts(w['start_ts']), parse_ts(w['end_ts'])
            minutes = overlap_span(start, end, low, high)
            weight = minutes * (w['load_factor'] or 1)
            if weight <= 0:
                continue
            weights[w['tenant']] = weights.get(w['tenant'], 0) + weight
            is_standby = w['kind'] in STANDBY_KINDS or w['role'] == 'backup'
            if is_standby:
                standby_weights[w['tenant']] = standby_weights.get(w['tenant'], 0) + weight
    else:
        rows = many(db, '''SELECT cc.intent intent, cc.cpu cpu, cc.mem mem, i.tenant tenant
                           FROM capacity_claims cc JOIN workload_intents i ON i.id=cc.intent
                           WHERE cc.cluster=? AND cc.state='active' ''', (cluster,))
        metric = 'cpu'
        for c in rows:
            # Claims are one per reservation id; aggregate defensively.
            weights[c['tenant']] = weights.get(c['tenant'], 0) + int(c[metric] or 0)
    # Standby bearer clause reassigns standby running weight (hours method only;
    # capacity allocation follows retained guaranteed capacity holdings).
    target = _standby_target(db, negotiation, cluster, set(weights))
    reassigned = None
    if method == 'run_hours' and target != 'standby_owner':
        standby_total = sum(standby_weights.values())
        if standby_total:
            for tenant, amount in standby_weights.items():
                weights[tenant] -= amount
                weights[target] = weights.get(target, 0) + amount
            reassigned = {'standby_weight': standby_total, 'from_tenants': sorted(standby_weights),
                          'to': target}
    return cluster, weights, reassigned


def _settle(db, x, op):
    recalc = op == 'recalculate'
    run_id = x['id']
    if one(db, 'SELECT 1 FROM settlement_runs WHERE id=?', (run_id,)):
        return {'decision': 'settlement_exists'}

    prior_run = None
    if recalc:
        prior_id = pick(x, 'supersedes', 'prior', 'replaces')
        prior_run = one(db, 'SELECT * FROM settlement_runs WHERE id=?', (prior_id,))
        if not prior_run:
            return {'decision': 'unknown_prior_settlement'}

    negotiation_id = pick(x, 'negotiation', 'terms', 'selection',
                          default=prior_run['negotiation'] if prior_run else None)
    negotiation = one(db, 'SELECT * FROM period_negotiations WHERE id=?', (negotiation_id,))
    if not negotiation:
        return {'decision': 'unknown_negotiation'}
    meter_id = pick(x, 'meter', 'meter_id',
                    default=prior_run['meter'] if prior_run else negotiation['meter'])
    period = pick(x, 'period',
                  default=prior_run['period'] if prior_run else negotiation['period'])
    reading = current_reading(db, meter_id, period)
    if not reading:
        return {'decision': 'no_trusted_reading'}
    total_units = int(reading['units'])

    pending_rows = many(db, '''SELECT * FROM evidence_requests
                               WHERE meter=? AND period=? AND state='open' ''', (meter_id, period))
    pending_units = sum(int(r['hold_units']) for r in pending_rows)
    pending_amount = sum(int(r['hold_amount'] or 0) for r in pending_rows)
    pending_by_tenant = {}
    for r in pending_rows:
        bucket = pending_by_tenant.setdefault(r['tenant'], {'units': 0, 'amount': 0})
        bucket['units'] += int(r['hold_units'])
        bucket['amount'] += int(r['hold_amount'] or 0)
    if pending_units > total_units:
        return {'decision': 'pending_exceeds_reading', 'pending_units': pending_units,
                'total_units': total_units}
    distributable = total_units - pending_units

    cluster, weights, reassigned = _driver_weights(db, negotiation, period, reading)
    weight_total = sum(weights.values())
    if distributable > 0 and weight_total <= 0:
        return {'decision': 'no_allocation_basis', 'meter': meter_id, 'period': period}
    shares = largest_remainder(distributable, list(weights.items()))

    # Every weight-bearing tenant needs a carbon order carrying ceiling/rates.
    orders = {}
    for tenant in shares:
        order = one(db, 'SELECT * FROM carbon_orders WHERE tenant=? AND period=? AND state=\'open\'',
                    (tenant, period))
        if not order:
            return {'decision': 'missing_carbon_order', 'tenant': tenant, 'period': period}
        orders[tenant] = order

    version = (prior_run['version'] + 1) if prior_run else 1
    supersedes = prior_run['id'] if prior_run else None
    db.execute('''INSERT INTO settlement_runs
                  (id,meter,period,negotiation,method,standby_bearer,reading_id,reading_revision,
                   total_units,allocated_units,pending_units,pending_amount,supersedes,version,state)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
               (run_id, meter_id, period, negotiation['id'], negotiation['method'],
                negotiation['standby_bearer'], reading['id'], reading['revision'],
                total_units, distributable, pending_units, pending_amount,
                supersedes, version, 'prepared'))

    bills = []
    for tenant in sorted(shares):
        order = orders[tenant]
        units = int(shares[tenant])
        pending_share = pending_by_tenant.get(tenant, {'units': 0, 'amount': 0})
        factor = order['carbon_factor'] if order['carbon_factor'] is not None else 1
        carbon_units = units * factor
        ceiling = int(order['ceiling'])
        excess = max(0, carbon_units - ceiling)
        unit_rate = order['unit_rate'] if order['unit_rate'] is not None else 1
        penalty_rate = order['penalty_rate'] if order['penalty_rate'] is not None else 0
        energy_amount = units * unit_rate
        penalty_amount = excess * penalty_rate
        total_due = energy_amount + penalty_amount
        prior_bill = None
        paid_forward = 0
        inherited_dispute = 'none'
        if prior_run:
            old = one(db, 'SELECT * FROM carbon_bills WHERE run_id=? AND tenant=?',
                      (prior_run['id'], tenant))
            if old:
                prior_bill = old['id']
                chain = []
                cursor = old
                seen = set()
                while cursor and cursor['id'] not in seen:
                    seen.add(cursor['id'])
                    chain.append(cursor['id'])
                    cursor = one(db, 'SELECT * FROM carbon_bills WHERE id=?',
                                 (cursor['prior_bill'],)) if cursor['prior_bill'] else None
                if chain:
                    placeholders = ','.join('?' for _ in chain)
                    row = one(db, f'''SELECT COALESCE(SUM(amount),0) paid FROM bill_payments
                                     WHERE bill IN ({placeholders}) AND state='accepted' ''',
                              tuple(chain))
                    paid_forward = int(row['paid'])
                if old['dispute_state'] == 'open':
                    inherited_dispute = 'open'
        balance_due = max(0, total_due - paid_forward)
        refund_due = max(0, paid_forward - total_due)
        if paid_forward <= 0:
            pay_state = 'unpaid'
        elif paid_forward < total_due:
            pay_state = 'partial'
        else:
            pay_state = 'paid'
        this_bill_id = bill_id(run_id, tenant)
        db.execute('''INSERT INTO carbon_bills
                      (id,run_id,tenant,order_id,period,version,prior_bill,record_state,
                       weight,allocated_units,pending_units,pending_amount,
                       ceiling_units,carbon_units,excess_units,
                       unit_rate,energy_amount,penalty_rate,penalty_amount,total_due,
                       paid_amount,pay_state,dispute_state,issue_state,issued_reading_revision,
                       balance_due,refund_due)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                   (this_bill_id, run_id, tenant, order['id'], period, version, prior_bill, 'current',
                    int(weights.get(tenant, 0)), units, pending_share['units'], pending_share['amount'],
                    ceiling, carbon_units, excess,
                    unit_rate, energy_amount, penalty_rate, penalty_amount, total_due,
                    paid_forward, pay_state, inherited_dispute, 'draft', None,
                    balance_due, refund_due))
        if prior_run:
            db.execute("UPDATE carbon_bills SET record_state='superseded' WHERE run_id=?",
                       (prior_run['id'],))
        bills.append({'id': this_bill_id, 'tenant': tenant, 'weight': int(weights.get(tenant, 0)),
                      'units': units, 'carbon_units': carbon_units, 'ceiling': ceiling,
                      'excess_units': excess, 'energy_amount': energy_amount,
                      'penalty_amount': penalty_amount, 'total_due': total_due,
                      'pending_units': pending_share['units'],
                      'pending_amount': pending_share['amount'],
                      'paid_forward': paid_forward, 'balance_due': balance_due,
                      'refund_due': refund_due, 'pay_state': pay_state,
                      'dispute_state': inherited_dispute})

    return {'decision': 'recalculated' if recalc else 'settled', 'run': run_id,
            'version': version, 'method': negotiation['method'],
            'standby_bearer': negotiation['standby_bearer'],
            'reading_revision': reading['revision'], 'total_units': total_units,
            'allocated_units': distributable, 'pending_units': pending_units,
            'pending_amount': pending_amount,
            'share_total': sum(b['units'] for b in bills) + pending_units,
            'standby_reassignment': reassigned, 'bills': bills}


def _target_bills(db, x):
    if x.get('bill'):
        row = one(db, 'SELECT * FROM carbon_bills WHERE id=?', (x['bill'],))
        return [row] if row else []
    if x.get('bills'):
        return [one(db, 'SELECT * FROM carbon_bills WHERE id=?', (b,)) for b in x['bills']]
    run = pick(x, 'run', 'settlement', 'settlement_id')
    if run:
        return many(db, 'SELECT * FROM carbon_bills WHERE run_id=? ORDER BY tenant', (run,))
    return []


def _issue(db, x):
    bills = [b for b in _target_bills(db, x) if b]
    if not bills:
        return {'decision': 'unknown_bill'}
    issued = []
    for b in bills:
        if b['issue_state'] == 'issued':
            continue
        reading_rev = b['issued_reading_revision']
        if reading_rev is None:
            run = one(db, 'SELECT * FROM settlement_runs WHERE id=?', (b['run_id'],))
            reading_rev = run['reading_revision'] if run else None
        db.execute("UPDATE carbon_bills SET issue_state='issued',issued_reading_revision=? WHERE id=?",
                   (reading_rev, b['id']))
        issued.append(b['id'])
    return {'decision': 'issued', 'bills': issued}


def _carried_paid(db, bill_row):
    """Accepted payments on the prior-version chain rolled into this bill."""
    carried = 0
    cursor_id = bill_row['prior_bill']
    seen = set()
    while cursor_id and cursor_id not in seen:
        seen.add(cursor_id)
        row = one(db, 'SELECT paid_amount,prior_bill FROM carbon_bills WHERE id=?', (cursor_id,))
        if not row:
            break
        # paid_amount on an ancestor already includes its own carry, so taking
        # the immediate prior bill's paid_amount counts the whole chain once.
        return int(row['paid_amount'] or 0)
    return carried


def _pay(db, x):
    bill_row = one(db, 'SELECT * FROM carbon_bills WHERE id=?', (x['bill'],))
    if not bill_row:
        return {'decision': 'unknown_bill'}
    amount = int(pick(x, 'amount', 'units', default=0))
    if amount <= 0:
        return {'decision': 'invalid_payment'}
    payment_id = x['id']
    db.execute('INSERT INTO bill_payments VALUES(?,?,?,?,?)',
               (payment_id, x['bill'], amount, pick(x, 'at', 'paid_at'), 'accepted'))
    local = one(db, "SELECT COALESCE(SUM(amount),0) paid FROM bill_payments WHERE bill=? AND state='accepted'",
                (x['bill'],))['paid']
    paid = int(local) + _carried_paid(db, bill_row)
    if paid >= bill_row['total_due']:
        pay_state = 'paid'
    elif paid > 0:
        pay_state = 'partial'
    else:
        pay_state = 'unpaid'
    balance_due = max(0, bill_row['total_due'] - paid)
    refund_due = max(0, paid - bill_row['total_due'])
    db.execute('UPDATE carbon_bills SET paid_amount=?,pay_state=?,balance_due=?,refund_due=? WHERE id=?',
               (paid, pay_state, balance_due, refund_due, x['bill']))
    return {'decision': 'payment_recorded', 'bill': x['bill'], 'paid_amount': paid,
            'pay_state': pay_state, 'balance_due': balance_due, 'refund_due': refund_due}


def _dispute(db, x):
    bill_row = one(db, 'SELECT * FROM carbon_bills WHERE id=?', (x['bill'],))
    if not bill_row:
        return {'decision': 'unknown_bill'}
    if one(db, "SELECT 1 FROM bill_disputes WHERE bill=? AND state='open' LIMIT 1", (x['bill'],)):
        return {'decision': 'dispute_already_open'}
    db.execute('INSERT INTO bill_disputes VALUES(?,?,?,?,?,?,?,?,?)',
               (x['id'], x['bill'], pick(x, 'tenant', default=bill_row['tenant']),
                x.get('reason'), pick(x, 'at', 'raised_at'),
                pick(x, 'deadline', 'due'), 'open', None, None))
    db.execute("UPDATE carbon_bills SET dispute_state='open' WHERE id=?", (x['bill'],))
    return {'decision': 'dispute_opened', 'bill': x['bill'], 'dispute': x['id']}


def _resolve_dispute(db, x):
    dispute_id = pick(x, 'dispute', 'dispute_id', 'id')
    d = one(db, 'SELECT * FROM bill_disputes WHERE id=?', (dispute_id,))
    if not d:
        return {'decision': 'unknown_dispute'}
    outcome = x.get('outcome', 'resolved')
    if outcome not in ('upheld', 'rejected', 'resolved', 'withdrawn'):
        return {'decision': 'invalid_dispute_outcome'}
    successor = pick(x, 'successor', 'successor_bill', 'new_bill')
    db.execute("UPDATE bill_disputes SET state='resolved',outcome=?,successor_bill=? WHERE id=?",
               (outcome, successor, dispute_id))
    db.execute("UPDATE carbon_bills SET dispute_state=? WHERE id=?", (outcome, d['bill']))
    if successor:
        db.execute("UPDATE carbon_bills SET dispute_state=? WHERE id=?", (outcome, successor))
    return {'decision': 'dispute_resolved', 'outcome': outcome, 'successor': successor}


def handle(db, x):
    return _handle(db, x)


def run(db, x):
    return act(db, 'carbon', x, handle)
