"""Multi-tenant incident loss verification and insurance clearing.

The legacy ``policy`` / ``claim`` / ``assess`` operations are preserved
verbatim for replay compatibility.  On top of them sits an evidence-bound
incident workflow:

    policy_terms -> invoice / outage_confirmation / cost offset
        -> verified_claim -> adjust (核损) -> confirm (客户确认)
        -> settlement (cash | direct provider pay) -> payment_receipt (实付回执)
        -> dishonour (退票) / correction (迟到修正, linked 补赔)
        -> subrogation (追偿) -> recovery_receipt

Every balance (per-incident limit, annual aggregate, deductible, offsets) is
derived from persisted rows, so several claims under one policy share the same
incident and yearly purse and can never sum past it.  Paid facts are append
only: dishonours and late corrections insert linked records instead of
rewriting history.
"""

import json

from .store import act, one, many

SCHEMA = '''
CREATE TABLE IF NOT EXISTS coverage_policies(id TEXT PRIMARY KEY, tenant TEXT, limit_amount INTEGER, deductible INTEGER, expires INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS loss_claims(id TEXT PRIMARY KEY, policy TEXT, incident TEXT, asserted_amount INTEGER, state TEXT, tenant TEXT);
CREATE TABLE IF NOT EXISTS indemnity_orders(id TEXT PRIMARY KEY, claim TEXT, approved_amount INTEGER, state TEXT);

CREATE TABLE IF NOT EXISTS policy_terms(
  policy TEXT PRIMARY KEY,
  subject TEXT,
  subjects TEXT,
  window_start INTEGER,
  window_end INTEGER,
  policy_year TEXT,
  annual_limit INTEGER,
  incident_limit INTEGER,
  outage_scope TEXT,
  recovery_scope TEXT,
  insurer_generation INTEGER,
  signed_at INTEGER,
  cash_settlement INTEGER DEFAULT 1,
  direct_pay INTEGER DEFAULT 1,
  FOREIGN KEY(policy) REFERENCES coverage_policies(id)
);
CREATE TABLE IF NOT EXISTS outage_confirmations(
  id TEXT PRIMARY KEY,
  incident TEXT,
  tenant TEXT,
  intent TEXT,
  cluster TEXT,
  confirmed_by TEXT,
  interrupted_at INTEGER,
  restored_at INTEGER,
  write_outage INTEGER DEFAULT 1,
  state TEXT DEFAULT 'confirmed'
);
CREATE TABLE IF NOT EXISTS expense_invoices(
  id TEXT PRIMARY KEY,
  ref TEXT UNIQUE,
  tenant TEXT,
  intent TEXT,
  incident TEXT,
  kind TEXT,
  category TEXT,
  vendor TEXT,
  amount INTEGER,
  incurred_at INTEGER,
  state TEXT DEFAULT 'verified'
);
CREATE TABLE IF NOT EXISTS claim_invoices(
  claim TEXT,
  invoice TEXT UNIQUE,
  PRIMARY KEY(claim, invoice)
);
CREATE TABLE IF NOT EXISTS cost_offsets(
  id TEXT PRIMARY KEY,
  claim TEXT,
  tenant TEXT,
  incident TEXT,
  kind TEXT,
  source TEXT,
  amount INTEGER,
  contract_clause TEXT,
  at INTEGER
);
CREATE TABLE IF NOT EXISTS claim_evidence(
  claim TEXT PRIMARY KEY,
  intent TEXT,
  lease TEXT,
  outage TEXT,
  verified_cluster TEXT,
  seat_proof TEXT,
  payload TEXT
);
CREATE TABLE IF NOT EXISTS loss_adjustments(
  id TEXT PRIMARY KEY,
  claim TEXT UNIQUE,
  gross_amount INTEGER,
  eligible_amount INTEGER,
  offset_amount INTEGER,
  deductible_amount INTEGER,
  incident_award INTEGER,
  annual_award INTEGER,
  approved_amount INTEGER,
  incident_remaining INTEGER,
  annual_remaining INTEGER,
  detail TEXT,
  at INTEGER
);
CREATE TABLE IF NOT EXISTS claim_confirmations(
  id TEXT PRIMARY KEY,
  claim TEXT,
  order_id TEXT,
  confirmed_by TEXT,
  at INTEGER
);
CREATE TABLE IF NOT EXISTS settlements(
  id TEXT PRIMARY KEY,
  claim TEXT,
  order_id TEXT,
  method TEXT,
  payee_kind TEXT,
  payee TEXT,
  provider TEXT,
  invoice TEXT,
  amount INTEGER,
  delivery_risk TEXT,
  restored_at INTEGER,
  recovery_target TEXT,
  supplier_claim TEXT,
  at INTEGER,
  state TEXT DEFAULT 'instructed'
);
CREATE TABLE IF NOT EXISTS payment_receipts(
  id TEXT PRIMARY KEY,
  settlement TEXT,
  claim TEXT,
  order_id TEXT,
  amount INTEGER,
  channel TEXT,
  ref TEXT,
  paid_at INTEGER,
  state TEXT DEFAULT 'paid'
);
CREATE TABLE IF NOT EXISTS payment_dishonours(
  id TEXT PRIMARY KEY,
  receipt TEXT,
  settlement TEXT,
  claim TEXT,
  amount INTEGER,
  reason TEXT,
  at INTEGER
);
CREATE TABLE IF NOT EXISTS loss_corrections(
  id TEXT PRIMARY KEY,
  claim TEXT,
  prior_adjustment TEXT,
  delta INTEGER,
  reason TEXT,
  invoice TEXT,
  order_id TEXT,
  at INTEGER
);
CREATE TABLE IF NOT EXISTS subrogation_cases(
  id TEXT PRIMARY KEY,
  claim TEXT,
  order_id TEXT,
  target TEXT,
  basis TEXT,
  amount INTEGER,
  at INTEGER,
  state TEXT DEFAULT 'open'
);
CREATE TABLE IF NOT EXISTS recovery_receipts(
  id TEXT PRIMARY KEY,
  case_id TEXT,
  claim TEXT,
  target TEXT,
  amount INTEGER,
  ref TEXT,
  at INTEGER
);
'''

COMMITTED_ORDER_STATES = ('adjusted', 'confirmed', 'paid')


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def _j(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _loads(text, default):
    if not text:
        return default
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return default


def _subjects(terms, policy):
    """Covered tenants for a policy term row."""
    if not terms:
        return [policy['tenant']]
    explicit = _loads(terms['subjects'], None)
    if explicit:
        return list(explicit)
    subject = terms['subject'] or policy['tenant']
    return [subject]


def _policy_terms(db, policy_id):
    return one(db, 'SELECT * FROM policy_terms WHERE policy=?', (policy_id,))


def _year_of(x):
    return str(x.get('policy_year') or x.get('year') or 'default')


def _scope_allows(terms, category, kind):
    """Whether an invoice category/kind is within the policy indemnity scope."""
    if not terms:
        return True
    column = 'outage_scope' if category == 'outage' else 'recovery_scope'
    raw = terms[column] if column in terms.keys() else None
    scope = _loads(raw, None)
    if scope is None:
        # No explicit list: the cover flag itself is the gate, default covered.
        return True
    if isinstance(scope, str):
        scope = [scope]
    return kind in scope or category in scope or '*' in scope


def _seat_at_incident(db, intent_id, cluster):
    """Reconstruct actual occupancy of ``cluster`` by ``intent_id``.

    A seat counts when a placement lease existed on the cluster and actually
    reached ``occupied``.  Occupancy is proven by the live lease state or by
    the replayed successful ``occupy`` event (the lease row survives later
    release/failover, so post-handoff verification still sees who held the
    seat during the incident).
    """
    leases = many(
        db,
        "SELECT * FROM placement_leases WHERE intent=? AND cluster=? ORDER BY id",
        (intent_id, cluster),
    )
    proof = []
    for lease in leases:
        occupied = lease['state'] == 'occupied'
        event = one(
            db,
            """
            SELECT e.event_id, e.payload, e.result
            FROM events e
            WHERE e.subsystem='seat' AND e.command='occupy'
              AND json_extract(e.payload, '$.id')=?
              AND json_extract(e.result, '$.decision')='occupied'
            ORDER BY e.seq LIMIT 1
            """,
            (lease['id'],),
        )
        if event:
            occupied = True
        if occupied:
            proof.append({
                'lease': lease['id'],
                'role': lease['role'],
                'generation': lease['generation'],
                'state': lease['state'],
                'occupy_event': event['event_id'] if event else None,
            })
    return proof


def _takeover(db, intent_id, incident_id):
    """Evacuation plan/handoff result corroborating the affected workload."""
    plans = many(
        db,
        "SELECT id, source, target, state FROM move_plans "
        "WHERE intent=? AND incident=? ORDER BY id",
        (intent_id, incident_id),
    )
    for plan in plans:
        plan['receipts'] = many(
            db,
            "SELECT kind, cluster, generation, state FROM handoff_receipts "
            "WHERE plan=? ORDER BY kind",
            (plan['id'],),
        )
    return plans


def _incident(db, incident_id):
    return one(db, 'SELECT * FROM incident_cases WHERE id=?', (incident_id,))


def _used_totals(db, policy_id, incident_id, year):
    """Committed indemnity principal already consumed.

    Sums approved principal on adjusted/confirmed/paid orders joined to their
    claims so both the shared incident purse and the policy-year aggregate can
    be checked.  Linked supplement orders are ordinary rows and therefore
    count exactly once.
    """
    row = one(
        db,
        f"""
        SELECT
          COALESCE(SUM(CASE WHEN c.incident=? THEN o.approved_amount ELSE 0 END), 0) incident_used,
          COALESCE(SUM(o.approved_amount), 0) annual_used
        FROM indemnity_orders o
        JOIN loss_claims c ON c.id=o.claim
        LEFT JOIN policy_terms t ON t.policy=c.policy
        WHERE c.policy=? AND o.state IN ({','.join('?' for _ in COMMITTED_ORDER_STATES)})
          AND COALESCE(t.policy_year, 'default')=?
        """,
        (incident_id, policy_id, *COMMITTED_ORDER_STATES, year),
    )
    return row['incident_used'], row['annual_used']


def _deductible_used_by_others(db, policy_id, incident_id, claim_id):
    row = one(
        db,
        f"""
        SELECT COALESCE(SUM(a.deductible_amount), 0) d
        FROM loss_adjustments a
        JOIN loss_claims c ON c.id=a.claim
        WHERE c.policy=? AND c.incident=? AND a.claim<>?
        """,
        (policy_id, incident_id, claim_id),
    )
    return row['d']


def _claim_invoice_rows(db, claim_id):
    return many(
        db,
        """
        SELECT i.* FROM expense_invoices i
        JOIN claim_invoices ci ON ci.invoice=i.id
        WHERE ci.claim=? ORDER BY i.id
        """,
        (claim_id,),
    )


def _offset_rows(db, claim):
    """All offsets that must reduce this claim.

    Offsets recorded directly against the claim, plus tenant/incident-level
    offsets that were never hidden by being left unlinked.
    """
    return many(
        db,
        """
        SELECT * FROM cost_offsets
        WHERE claim=?
           OR (claim IS NULL AND tenant=? AND incident=?)
        ORDER BY id
        """,
        (claim['id'], claim['tenant'], claim['incident']),
    )


def _settled_principal(db, claim_id, order_id=None):
    """Net cash actually out the door: paid receipts minus dishonours."""
    if order_id:
        paid = one(
            db,
            "SELECT COALESCE(SUM(amount),0) s FROM payment_receipts "
            "WHERE claim=? AND order_id=?",
            (claim_id, order_id),
        )['s']
        bounced = one(
            db,
            """
            SELECT COALESCE(SUM(d.amount),0) s FROM payment_dishonours d
            WHERE d.claim=? AND EXISTS (
                SELECT 1 FROM payment_receipts r
                WHERE r.id=d.receipt AND r.order_id=?)
            """,
            (claim_id, order_id),
        )['s']
    else:
        paid = one(
            db,
            "SELECT COALESCE(SUM(amount),0) s FROM payment_receipts WHERE claim=?",
            (claim_id,),
        )['s']
        bounced = one(
            db,
            "SELECT COALESCE(SUM(amount),0) s FROM payment_dishonours WHERE claim=?",
            (claim_id,),
        )['s']
    return paid - bounced


# --------------------------------------------------------------------------- #
# Operation handlers
# --------------------------------------------------------------------------- #

EXTENDED_POLICY_KEYS = (
    'subject', 'subjects', 'window_start', 'window_end', 'annual_limit',
    'incident_limit', 'insurer_generation', 'policy_year', 'year',
    'outage_scope', 'recovery_scope',
)


def _is_extended_policy(x):
    # The seed policy command only carries limit/deductible/expires/tenant.
    return any(k in x for k in EXTENDED_POLICY_KEYS)


def _op_policy(db, x):
    # Legacy operation, behaviour and response preserved exactly.
    if not _is_extended_policy(x):
        if x['limit'] <= x['deductible'] or x['deductible'] < 0:
            return {'decision': 'invalid_coverage'}
        db.execute('INSERT INTO coverage_policies VALUES(?,?,?,?,?,?)',
                   (x['id'], x['tenant'], x['limit'], x['deductible'], x['expires'], 'active'))
        return {'decision': 'coverage_recorded'}
    return _op_policy_terms(db, x)


def _op_policy_terms(db, x):
    subject = x.get('subject') or x.get('tenant') or x.get('insured')
    window = x.get('window') or x.get('incident_window')
    window_start = x.get('window_start', x.get('window_from'))
    window_end = x.get('window_end', x.get('window_to'))
    if window and isinstance(window, (list, tuple)) and len(window) == 2:
        window_start = window_start if window_start is not None else window[0]
        window_end = window_end if window_end is not None else window[1]
    incident_limit = x.get('incident_limit', x.get('per_incident_limit', x.get('limit')))
    annual_limit = x.get('annual_limit',
                         x.get('aggregate_limit', x.get('annual', x.get('annual_aggregate'))))
    signer_generation = x.get('insurer_generation',
                              x.get('signer_generation', x.get('signing_generation')))
    deductible = x.get('deductible')
    policy_year = x.get('policy_year', x.get('year'))

    missing = []
    if window_start is None:
        missing.append('window_start')
    if window_end is None:
        missing.append('window_end')
    if annual_limit is None:
        missing.append('annual_limit')
    if incident_limit is None:
        missing.append('incident_limit')
    if deductible is None:
        missing.append('deductible')
    if signer_generation is None:
        missing.append('insurer_generation')
    if not subject:
        missing.append('subject')
    if missing:
        return {'decision': 'incomplete_coverage_terms', 'missing': missing}
    if incident_limit <= 0 or annual_limit < incident_limit:
        return {'decision': 'invalid_coverage_limits'}
    if deductible < 0 or incident_limit <= deductible:
        return {'decision': 'invalid_coverage'}
    if window_end < window_start:
        return {'decision': 'invalid_incident_window'}
    if int(signer_generation) < 1:
        return {'decision': 'invalid_signer_generation'}

    old = one(db, 'SELECT id FROM coverage_policies WHERE id=?', (x['id'],))
    if old:
        return {'decision': 'policy_exists'}

    subjects = x.get('subjects', x.get('covered_tenants', [subject]))
    db.execute('INSERT INTO coverage_policies VALUES(?,?,?,?,?,?)',
               (x['id'], x.get('tenant', subject), incident_limit,
                deductible, window_end, 'active'))
    db.execute(
        '''INSERT INTO policy_terms(policy, subject, subjects, window_start, window_end,
             policy_year, annual_limit, incident_limit, outage_scope, recovery_scope,
             insurer_generation, signed_at, cash_settlement, direct_pay)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (x['id'], subject, _j(subjects),
         window_start, window_end, str(policy_year) if policy_year is not None else 'default',
         annual_limit, incident_limit,
         _j(x.get('outage_scope')) if x.get('outage_scope') is not None else None,
         _j(x.get('recovery_scope')) if x.get('recovery_scope') is not None else None,
         signer_generation, x.get('signed_at', window_start),
         int(x.get('cash_allowed', x.get('cash_settlement', 1))),
         int(x.get('direct_pay_allowed', x.get('direct_pay', 1)))),
    )
    return {'decision': 'coverage_bound', 'policy': x['id'],
            'insurer_generation': signer_generation}


def _op_outage(db, x):
    db.execute(
        '''INSERT INTO outage_confirmations(id, incident, tenant, intent, cluster,
             confirmed_by, interrupted_at, restored_at, write_outage, state)
           VALUES(?,?,?,?,?,?,?,?,?,?)''',
        (x['id'], x['incident'], x['tenant'], x.get('intent'), x.get('cluster'),
         x.get('confirmed_by', 'customer'), x.get('interrupted_at', x.get('at')),
         x.get('restored_at'), int(x.get('write_outage', 1)),
         x.get('state', 'confirmed')),
    )
    return {'decision': 'outage_confirmed', 'id': x['id']}


def _op_invoice(db, x):
    ref = x.get('ref') or x['id']
    duplicate = one(db, 'SELECT id FROM expense_invoices WHERE ref=? AND id<>?',
                    (ref, x['id']))
    if duplicate:
        return {'decision': 'duplicate_expense', 'ref': ref,
                'held_by': duplicate['id']}
    category = x.get('category')
    if not category:
        category = 'recovery' if x.get('kind') == 'data_recovery' else 'outage'
    if x['amount'] <= 0:
        return {'decision': 'invalid_invoice_amount'}
    db.execute(
        '''INSERT INTO expense_invoices(id, ref, tenant, intent, incident, kind,
             category, vendor, amount, incurred_at, state)
           VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
        (x['id'], ref, x.get('tenant'), x.get('intent'), x.get('incident'),
         x.get('kind', 'remediation'), category, x.get('vendor'), x['amount'],
         x.get('incurred_at', x.get('at')), x.get('state', 'verified')),
    )
    return {'decision': 'invoice_recorded', 'id': x['id'], 'ref': ref}


def _op_offset(db, x):
    if x['amount'] <= 0:
        return {'decision': 'invalid_offset_amount'}
    db.execute(
        '''INSERT INTO cost_offsets(id, claim, tenant, incident, kind, source,
             amount, contract_clause, at)
           VALUES(?,?,?,?,?,?,?,?,?)''',
        (x['id'], x.get('claim'), x.get('tenant'), x.get('incident'),
         x.get('kind', 'third_party'), x.get('source'), x['amount'],
         x.get('contract_clause'), x.get('at')),
    )
    return {'decision': 'offset_recorded', 'id': x['id'], 'amount': x['amount']}


def _op_claim(db, x):
    # Legacy minimal claim: exactly the seed behaviour.
    extended = any(k in x for k in ('intent', 'lease', 'outage', 'invoices',
                                    'tenant', 'evidence'))
    policy = one(db, 'SELECT * FROM coverage_policies WHERE id=?', (x['policy'],))
    at = x.get('at', 0)
    if not policy or policy['state'] != 'active' or policy['expires'] < at:
        return {'decision': 'policy_unavailable'}

    if not extended:
        db.execute(
            'INSERT INTO loss_claims(id, policy, incident, asserted_amount, state) '
            'VALUES(?,?,?,?,?)',
            (x['id'], x['policy'], x['incident'], x['amount'], 'filed'))
        return {'decision': 'claim_filed'}

    terms = _policy_terms(db, x['policy'])
    tenant = x.get('tenant') or policy['tenant']
    subjects = _subjects(terms, policy)
    if terms and tenant not in subjects:
        return {'decision': 'tenant_not_covered', 'tenant': tenant,
                'subjects': subjects}

    incident = _incident(db, x['incident'])
    if not incident:
        return {'decision': 'incident_not_found'}

    intent_id = x.get('intent')
    workload = one(db, 'SELECT * FROM workload_intents WHERE id=?', (intent_id,)) \
        if intent_id else None
    if intent_id and not workload:
        return {'decision': 'workload_not_found'}
    if workload and workload['tenant'] != tenant:
        return {'decision': 'tenant_workload_mismatch',
                'workload_tenant': workload['tenant'], 'tenant': tenant}

    outage_id = x.get('outage')
    outage = one(db, 'SELECT * FROM outage_confirmations WHERE id=?', (outage_id,)) \
        if outage_id else None
    if outage_id and not outage:
        return {'decision': 'outage_not_found'}
    if outage and (outage['tenant'] != tenant or outage['incident'] != x['incident']):
        return {'decision': 'outage_binding_mismatch'}

    # Every invoice must exist and must not already fund another claim.
    invoice_ids = list(x.get('invoices', []))
    for invoice_id in invoice_ids:
        invoice = one(db, 'SELECT * FROM expense_invoices WHERE id=?', (invoice_id,))
        if not invoice:
            return {'decision': 'invoice_not_found', 'invoice': invoice_id}
        bound = one(db, 'SELECT claim FROM claim_invoices WHERE invoice=?', (invoice_id,))
        if bound and bound['claim'] != x['id']:
            return {'decision': 'duplicate_expense', 'invoice': invoice_id,
                    'held_by': bound['claim']}
        if invoice['tenant'] and invoice['tenant'] != tenant:
            return {'decision': 'invoice_tenant_mismatch', 'invoice': invoice_id}

    db.execute(
        'INSERT INTO loss_claims(id, policy, incident, asserted_amount, state, tenant) '
        'VALUES(?,?,?,?,?,?)',
        (x['id'], x['policy'], x['incident'],
         x.get('amount', x.get('asserted_amount', 0)), 'filed', tenant))
    for invoice_id in invoice_ids:
        db.execute('INSERT OR IGNORE INTO claim_invoices(claim, invoice) VALUES(?,?)',
                   (x['id'], invoice_id))

    db.execute(
        '''INSERT INTO claim_evidence(claim, intent, lease, outage,
             verified_cluster, seat_proof, payload)
           VALUES(?,?,?,?,?,?,?)''',
        (x['id'], intent_id, x.get('lease'), outage_id,
         incident['cluster'] if incident else None, None,
         _j(x.get('evidence', {}))),
    )
    return {'decision': 'claim_filed', 'claim': x['id'],
            'invoices': invoice_ids}


def _op_assess_legacy(db, x):
    # Seed operation preserved exactly.
    claim = one(db, 'SELECT * FROM loss_claims WHERE id=?', (x['claim'],))
    if not claim or claim['state'] != 'filed':
        return {'decision': 'claim_unavailable'}
    policy = one(db, 'SELECT * FROM coverage_policies WHERE id=?', (claim['policy'],))
    amount = min(policy['limit_amount'],
                 max(0, claim['asserted_amount'] - policy['deductible']))
    db.execute('INSERT INTO indemnity_orders VALUES(?,?,?,?)',
               (x['id'], x['claim'], amount, 'provisional'))
    return {'decision': 'provisional_assessment', 'amount': amount}


def _op_adjust(db, x):
    """核损: verify facts and settle the claim against shared purses."""
    claim = one(db, 'SELECT * FROM loss_claims WHERE id=?', (x['claim'],))
    if not claim:
        return {'decision': 'claim_unavailable'}
    prior = one(db, 'SELECT id FROM loss_adjustments WHERE claim=?', (claim['id'],))
    if prior:
        return {'decision': 'claim_already_adjusted', 'adjustment': prior['id']}

    policy = one(db, 'SELECT * FROM coverage_policies WHERE id=?', (claim['policy'],))
    terms = _policy_terms(db, claim['policy'])
    if not policy or policy['state'] != 'active':
        return {'decision': 'policy_unavailable'}

    evidence = one(db, 'SELECT * FROM claim_evidence WHERE claim=?', (claim['id'],))
    intent_id = x.get('intent') or (evidence['intent'] if evidence else None)
    outage_id = x.get('outage') or (evidence['outage'] if evidence else None)

    incident = _incident(db, claim['incident'])
    if not incident:
        return {'decision': 'incident_not_found'}

    # 1. Real affected seat in the incident cluster.
    if not intent_id:
        return {'decision': 'affected_workload_required'}
    seats = _seat_at_incident(db, intent_id, incident['cluster'])
    if not seats:
        return {'decision': 'not_affected_by_incident',
                'incident': claim['incident'], 'cluster': incident['cluster'],
                'intent': intent_id}
    lease_hint = x.get('lease') or (evidence['lease'] if evidence else None)
    if lease_hint and not any(s['lease'] == lease_hint for s in seats):
        return {'decision': 'seat_not_in_incident_cluster', 'lease': lease_hint}

    # 2. Customer-confirmed interruption bound to the same facts.
    if not outage_id:
        return {'decision': 'confirmed_outage_required'}
    outage = one(db, 'SELECT * FROM outage_confirmations WHERE id=?', (outage_id,))
    if not outage or outage['state'] != 'confirmed':
        return {'decision': 'outage_not_confirmed'}
    if outage['incident'] != claim['incident']:
        return {'decision': 'outage_incident_mismatch'}

    # 3. Policy incident window (against interruption + invoice dates).
    if terms and terms['window_start'] is not None:
        if outage['interrupted_at'] is not None and not (
                terms['window_start'] <= outage['interrupted_at'] <= terms['window_end']):
            return {'decision': 'outside_incident_window', 'when': outage['interrupted_at']}

    # 4. Expense originals, scope and duplicate protection.
    invoices = _claim_invoice_rows(db, claim['id'])
    if not invoices:
        return {'decision': 'expense_invoice_required'}
    if terms and terms['window_start'] is not None:
        for inv in invoices:
            if inv['incurred_at'] is not None and not (
                    terms['window_start'] <= inv['incurred_at'] <= terms['window_end']):
                return {'decision': 'expense_outside_window', 'invoice': inv['id']}

    gross = sum(i['amount'] for i in invoices)
    excluded = [i['id'] for i in invoices
                if not _scope_allows(terms, i['category'], i['kind'])]
    eligible = sum(i['amount'] for i in invoices if i['id'] not in excluded)

    # 5. Contract offsets that cannot be hidden.
    offsets = _offset_rows(db, claim)
    offset_amount = sum(o['amount'] for o in offsets)
    after_offsets = max(0, eligible - offset_amount)

    # 6. Per-incident deductible shared across valid claims.
    deductible = policy['deductible']
    ded_used_others = _deductible_used_by_others(
        db, claim['policy'], claim['incident'], claim['id'])
    deductible_remaining = max(0, deductible - ded_used_others)
    deductible_charge = min(after_offsets, deductible_remaining)
    after_deductible = after_offsets - deductible_charge

    # 7. Shared incident limit and annual aggregate.
    year = terms['policy_year'] if terms else 'default'
    incident_limit = terms['incident_limit'] if terms and terms['incident_limit'] is not None \
        else policy['limit_amount']
    annual_limit = terms['annual_limit'] if terms and terms['annual_limit'] is not None \
        else policy['limit_amount']
    incident_used, annual_used = _used_totals(
        db, claim['policy'], claim['incident'], year)
    incident_remaining = max(0, incident_limit - incident_used)
    annual_remaining = max(0, annual_limit - annual_used)

    incident_award = min(after_deductible, incident_remaining)
    annual_award = min(incident_award, annual_remaining)
    approved = annual_award

    at = x.get('at')
    detail = {
        'incident': dict(incident),
        'seats': seats,
        'takeover': _takeover(db, intent_id, claim['incident']),
        'outage': dict(outage),
        'invoices': [{'id': i['id'], 'ref': i['ref'], 'kind': i['kind'],
                      'category': i['category'], 'amount': i['amount']}
                     for i in invoices],
        'excluded': excluded,
        'offsets': [{'id': o['id'], 'kind': o['kind'], 'amount': o['amount']}
                    for o in offsets],
        'deductible_total': deductible,
        'deductible_used_by_other_claims': ded_used_others,
        'deductible_remaining_before': deductible_remaining,
        'incident_limit': incident_limit,
        'annual_limit': annual_limit,
        'incident_used_before': incident_used,
        'annual_used_before': annual_used,
    }

    adjustment_id = x.get('adjustment', x['id'] + '-adj')
    db.execute(
        '''INSERT INTO loss_adjustments(id, claim, gross_amount, eligible_amount,
             offset_amount, deductible_amount, incident_award, annual_award,
             approved_amount, incident_remaining, annual_remaining, detail, at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (adjustment_id, claim['id'], gross, eligible, offset_amount,
         deductible_charge, incident_award, annual_award, approved,
         incident_remaining - incident_award,
         annual_remaining - annual_award, _j(detail), at),
    )
    order_id = x['id']
    db.execute('INSERT INTO indemnity_orders VALUES(?,?,?,?)',
               (order_id, claim['id'], approved, 'adjusted'))
    db.execute("UPDATE loss_claims SET state='adjusted' WHERE id=?", (claim['id'],))
    if evidence:
        db.execute('UPDATE claim_evidence SET seat_proof=? WHERE claim=?',
                   (_j(seats), claim['id']))

    return {
        'decision': 'loss_adjusted',
        'adjustment': adjustment_id,
        'order': order_id,
        'claim': claim['id'],
        'gross_amount': gross,
        'eligible_amount': eligible,
        'excluded_invoices': excluded,
        'offset_amount': offset_amount,
        'deductible_amount': deductible_charge,
        'deductible_remaining': max(0, deductible_remaining - deductible_charge),
        'approved_amount': approved,
        'amount': approved,
        'incident_remaining': incident_remaining - incident_award,
        'annual_remaining': annual_remaining - annual_award,
        'capped_by_incident': after_deductible > incident_award,
        'capped_by_annual': incident_award > annual_award,
    }


def _op_confirm(db, x):
    claim_id = x['claim']
    claim = one(db, 'SELECT * FROM loss_claims WHERE id=?', (claim_id,))
    adjustment = one(db, 'SELECT * FROM loss_adjustments WHERE claim=?', (claim_id,))
    if not claim or not adjustment:
        return {'decision': 'adjustment_required'}

    order_ref = x.get('order_id') or x.get('order')
    if order_ref:
        order = one(db, 'SELECT * FROM indemnity_orders WHERE id=? AND claim=?',
                    (order_ref, claim_id))
    else:
        order = one(
            db,
            "SELECT * FROM indemnity_orders WHERE claim=? AND state='adjusted' "
            "ORDER BY rowid DESC LIMIT 1",
            (claim_id,),
        )
    if not order:
        if one(db, 'SELECT 1 FROM claim_confirmations WHERE claim=?', (claim_id,)):
            return {'decision': 'claim_already_confirmed'}
        return {'decision': 'adjusted_order_required'}

    prior = one(db, 'SELECT id FROM claim_confirmations WHERE order_id=?', (order['id'],))
    if prior:
        return {'decision': 'order_already_confirmed', 'confirmation': prior['id']}

    confirmation_id = x.get('id') or ('confirm-' + order['id'])
    db.execute(
        'INSERT INTO claim_confirmations(id, claim, order_id, confirmed_by, at) '
        'VALUES(?,?,?,?,?)',
        (confirmation_id, claim_id, order['id'],
         x.get('confirmed_by', 'customer'), x.get('at')),
    )
    db.execute("UPDATE indemnity_orders SET state='confirmed' WHERE id=?", (order['id'],))
    db.execute("UPDATE loss_claims SET state='confirmed' WHERE id=?", (claim_id,))
    return {'decision': 'claim_confirmed', 'claim': claim_id,
            'order': order['id'], 'confirmation': confirmation_id,
            'approved_amount': order['approved_amount']}


def _settlement_instruction(db, x):
    claim_id = x['claim']
    claim = one(db, 'SELECT * FROM loss_claims WHERE id=?', (claim_id,))
    if not claim:
        return {'decision': 'claim_unavailable'}
    adjustment = one(db, 'SELECT * FROM loss_adjustments WHERE claim=?', (claim_id,))
    if not adjustment:
        return {'decision': 'adjustment_required'}
    if not one(db, 'SELECT 1 FROM claim_confirmations WHERE claim=?', (claim_id,)):
        return {'decision': 'customer_confirmation_required'}

    method = x.get('method')
    if method not in ('cash', 'direct_pay'):
        # The arrangement is an explicit election; asserted_amount can never
        # choose it implicitly.
        return {'decision': 'settlement_method_required',
                'allowed': ['cash', 'direct_pay']}

    terms = _policy_terms(db, claim['policy'])
    if terms:
        if method == 'cash' and not terms['cash_settlement']:
            return {'decision': 'cash_settlement_not_permitted'}
        if method == 'direct_pay' and not terms['direct_pay']:
            return {'decision': 'direct_pay_not_permitted'}

    order = None
    order_ref = x.get('order_id') or x.get('order')
    if order_ref:
        order = one(db, 'SELECT * FROM indemnity_orders WHERE id=? AND claim=?',
                    (order_ref, claim_id))
        if not order or order['state'] not in ('confirmed', 'paid'):
            return {'decision': 'confirmed_order_required'}
    else:
        order = one(db,
                    "SELECT * FROM indemnity_orders WHERE claim=? "
                    "AND state IN ('confirmed','paid') "
                    "ORDER BY rowid DESC LIMIT 1",
                    (claim_id,))
    if not order:
        return {'decision': 'confirmed_order_required'}

    already = _settled_principal(db, claim_id, order['id'])
    outstanding = order['approved_amount'] - already
    amount = x.get('amount', outstanding)
    if amount <= 0 or amount > outstanding:
        return {'decision': 'settlement_amount_exceeds_award',
                'order': order['id'],
                'approved_amount': order['approved_amount'],
                'outstanding': outstanding,
                'settled_net': already}

    tenant = claim['tenant'] or policy_tenant(db, claim['policy'])
    if method == 'cash':
        payee_kind = 'tenant'
        payee = x.get('payee', tenant)
        provider = None
        invoice = None
        delivery_risk = x.get('delivery_risk', 'customer_self_procurement')
        recovery_target = x.get('recovery_target', 'responsible_third_party')
        supplier_claim = None
    else:
        payee_kind = 'provider'
        invoice_id = x.get('invoice')
        invoice = one(db, 'SELECT * FROM expense_invoices WHERE id=?', (invoice_id,)) \
            if invoice_id else None
        if not invoice:
            return {'decision': 'provider_invoice_required'}
        provider = x.get('provider') or invoice['vendor']
        payee = x.get('payee', provider)
        delivery_risk = x.get('delivery_risk', 'insurer_directed_provider')
        # Direct payment extinguishes the supplier claim against the tenant and
        # moves the insurer's recovery line to the elected target.
        recovery_target = x.get('recovery_target', 'at_fault_vendor')
        supplier_claim = x.get('supplier_claim', 'extinguished_by_direct_payment')

    settlement_id = x['id']
    db.execute(
        '''INSERT INTO settlements(id, claim, order_id, method, payee_kind, payee,
             provider, invoice, amount, delivery_risk, restored_at, recovery_target,
             supplier_claim, at, state)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (settlement_id, claim_id, order['id'], method, payee_kind, payee,
         provider, x.get('invoice'), amount, delivery_risk, x.get('restored_at'),
         recovery_target, supplier_claim, x.get('at'), 'instructed'),
    )
    return {
        'decision': 'settlement_instructed',
        'settlement': settlement_id,
        'method': method,
        'payee_kind': payee_kind,
        'payee': payee,
        'amount': amount,
        'delivery_risk': delivery_risk,
        'recovery_target': recovery_target,
        'supplier_claim': supplier_claim,
        'restored_at': x.get('restored_at'),
    }


def policy_tenant(db, policy_id):
    p = one(db, 'SELECT tenant FROM coverage_policies WHERE id=?', (policy_id,))
    return p['tenant'] if p else None


def _op_payment_receipt(db, x):
    settlement_id = x.get('settlement')
    settlement = one(db, 'SELECT * FROM settlements WHERE id=?', (settlement_id,)) \
        if settlement_id else None

    # One-step ``pay``: accept the settlement election inline and record it
    # before the actual-money receipt.  The two-step instruct/receipt flow is
    # unchanged when a settlement id is supplied.
    if not settlement and x.get('claim') and x.get('method'):
        instruction = dict(x)
        instruction['id'] = x.get('instruction_id') or (x['id'] + '-instruction')
        instructed = _settlement_instruction(db, instruction)
        if instructed.get('decision') != 'settlement_instructed':
            return instructed
        settlement_id = instructed['settlement']
        settlement = one(db, 'SELECT * FROM settlements WHERE id=?', (settlement_id,))

    if not settlement:
        return {'decision': 'settlement_required'}
    if settlement['state'] == 'paid':
        return {'decision': 'settlement_already_paid'}

    receipt_id = x['id']
    db.execute(
        '''INSERT INTO payment_receipts(id, settlement, claim, order_id, amount,
             channel, ref, paid_at, state)
           VALUES(?,?,?,?,?,?,?,?,?)''',
        (receipt_id, settlement_id, settlement['claim'], settlement['order_id'],
         x.get('amount', settlement['amount']), x.get('channel'),
         x.get('ref', receipt_id), x.get('paid_at', x.get('at')), 'paid'),
    )
    db.execute("UPDATE settlements SET state='paid' WHERE id=?", (settlement_id,))
    db.execute("UPDATE indemnity_orders SET state='paid' WHERE id=?",
               (settlement['order_id'],))
    db.execute("UPDATE loss_claims SET state='paid' WHERE id=?", (settlement['claim'],))
    return {'decision': 'payment_receipted', 'receipt': receipt_id,
            'settlement': settlement_id,
            'amount': x.get('amount', settlement['amount'])}


def _op_dishonour(db, x):
    """退票: append a linked reversal; the original paid fact is untouched."""
    receipt = one(db, 'SELECT * FROM payment_receipts WHERE id=?', (x['receipt'],))
    if not receipt:
        return {'decision': 'receipt_not_found'}
    existing = one(db, 'SELECT id FROM payment_dishonours WHERE id=?', (x['id'],))
    if existing:
        return {'decision': 'dishonour_exists'}
    db.execute(
        '''INSERT INTO payment_dishonours(id, receipt, settlement, claim, amount,
             reason, at)
           VALUES(?,?,?,?,?,?,?)''',
        (x['id'], receipt['id'], receipt['settlement'], receipt['claim'],
         x.get('amount', receipt['amount']), x.get('reason', 'dishonoured'),
         x.get('at')),
    )
    # The order becomes payable again only to the extent net settled dropped.
    net = _settled_principal(db, receipt['claim'], receipt['order_id'])
    order = one(db, 'SELECT approved_amount FROM indemnity_orders WHERE id=?',
                (receipt['order_id'],))
    if order and net < order['approved_amount']:
        db.execute("UPDATE indemnity_orders SET state='confirmed' WHERE id=?",
                   (receipt['order_id'],))
        db.execute("UPDATE loss_claims SET state='confirmed' WHERE id=?",
                   (receipt['claim'],))
    return {'decision': 'payment_dishonoured', 'dishonour': x['id'],
            'receipt': receipt['id'], 'net_settled': net}


def _op_correction(db, x):
    """Late correction: append only the linked difference (补赔)."""
    claim_id = x['claim']
    claim = one(db, 'SELECT * FROM loss_claims WHERE id=?', (claim_id,))
    adjustment = one(db, 'SELECT * FROM loss_adjustments WHERE claim=?', (claim_id,))
    if not claim or not adjustment:
        return {'decision': 'adjustment_required'}

    delta = x.get('delta')
    invoice_id = x.get('invoice')
    # A late invoice may be supplied and bound in the same command.
    if invoice_id and not one(db, 'SELECT 1 FROM claim_invoices WHERE invoice=?',
                              (invoice_id,)):
        inv = one(db, 'SELECT * FROM expense_invoices WHERE id=?', (invoice_id,))
        if not inv:
            return {'decision': 'invoice_not_found'}
        held = one(db, 'SELECT claim FROM claim_invoices WHERE invoice=?', (invoice_id,))
        if held and held['claim'] != claim_id:
            return {'decision': 'duplicate_expense', 'invoice': invoice_id,
                    'held_by': held['claim']}
        db.execute('INSERT OR IGNORE INTO claim_invoices(claim, invoice) VALUES(?,?)',
                   (claim_id, invoice_id))
        if delta is None:
            delta = inv['amount']

    if delta is None:
        return {'decision': 'correction_delta_required'}

    # The extra difference still has to fit the shared incident/annual purses.
    terms = _policy_terms(db, claim['policy'])
    year = terms['policy_year'] if terms else 'default'
    incident_used, annual_used = _used_totals(
        db, claim['policy'], claim['incident'], year)
    incident_limit = terms['incident_limit'] if terms and terms['incident_limit'] is not None \
        else one(db, 'SELECT limit_amount FROM coverage_policies WHERE id=?',
                 (claim['policy'],))['limit_amount']
    annual_limit = terms['annual_limit'] if terms and terms['annual_limit'] is not None \
        else incident_limit
    incident_room = max(0, incident_limit - incident_used)
    annual_room = max(0, annual_limit - annual_used)
    awarded_delta = max(0, min(delta, incident_room, annual_room))

    correction_id = x['id']
    order_id = x.get('order_id', correction_id + '-order')
    db.execute(
        '''INSERT INTO loss_corrections(id, claim, prior_adjustment, delta, reason,
             invoice, order_id, at)
           VALUES(?,?,?,?,?,?,?,?)''',
        (correction_id, claim_id, adjustment['id'], awarded_delta,
         x.get('reason', 'late_correction'), invoice_id, order_id, x.get('at')),
    )
    if awarded_delta > 0:
        # Linked supplement order; original paid order is never modified.
        db.execute('INSERT INTO indemnity_orders VALUES(?,?,?,?)',
                   (order_id, claim_id, awarded_delta, 'adjusted'))
    return {'decision': 'correction_recorded', 'correction': correction_id,
            'supplement_order': order_id if awarded_delta > 0 else None,
            'requested_delta': delta, 'approved_delta': awarded_delta,
            'incident_remaining': incident_room - awarded_delta,
            'annual_remaining': annual_room - awarded_delta}


def _op_subrogate(db, x):
    claim_id = x['claim']
    target = x.get('target')
    if not target:
        return {'decision': 'recovery_target_required'}
    case_id = x['id']
    db.execute(
        '''INSERT INTO subrogation_cases(id, claim, order_id, target, basis,
             amount, at, state)
           VALUES(?,?,?,?,?,?,?,?)''',
        (case_id, claim_id, x.get('order_id'), target,
         x.get('basis', 'indemnity_payment'), x.get('amount', 0),
         x.get('at'), 'open'),
    )
    return {'decision': 'subrogation_opened', 'case': case_id, 'target': target}


def _op_recovery(db, x):
    case_row = one(db, 'SELECT * FROM subrogation_cases WHERE id=?', (x['case'],))
    if not case_row:
        return {'decision': 'subrogation_case_not_found'}
    receipt_id = x['id']
    db.execute(
        '''INSERT INTO recovery_receipts(id, case_id, claim, target, amount, ref, at)
           VALUES(?,?,?,?,?,?,?)''',
        (receipt_id, case_row['id'], case_row['claim'], case_row['target'],
         x['amount'], x.get('ref', receipt_id), x.get('at')),
    )
    total = one(db, 'SELECT COALESCE(SUM(amount),0) s FROM recovery_receipts WHERE case_id=?',
                (case_row['id'],))['s']
    state = 'recovered' if total >= case_row['amount'] and case_row['amount'] else 'partial'
    db.execute('UPDATE subrogation_cases SET state=? WHERE id=?', (state, case_row['id']))
    return {'decision': 'recovery_recorded', 'receipt': receipt_id,
            'case': case_row['id'], 'total_recovered': total, 'state': state}


def _op_position(db, x):
    policy_id = x['policy']
    incident_id = x.get('incident')
    policy = one(db, 'SELECT * FROM coverage_policies WHERE id=?', (policy_id,))
    if not policy:
        return {'decision': 'policy_unavailable', 'policy': policy_id}
    terms = _policy_terms(db, policy_id)
    year = terms['policy_year'] if terms else _year_of(x)
    incident_used, annual_used = _used_totals(
        db, policy_id, incident_id or '', year)
    incident_limit = terms['incident_limit'] if terms and terms['incident_limit'] is not None \
        else policy['limit_amount']
    annual_limit = terms['annual_limit'] if terms and terms['annual_limit'] is not None \
        else policy['limit_amount']
    return {
        'decision': 'coverage_position',
        'policy': policy_id,
        'year': year,
        'incident_limit': incident_limit,
        'incident_used': incident_used if incident_id else None,
        'incident_remaining': max(0, incident_limit - incident_used) if incident_id else None,
        'annual_limit': annual_limit,
        'annual_used': annual_used,
        'annual_remaining': max(0, annual_limit - annual_used),
    }


ALIASES = {
    'file_claim': 'claim',
    'submit_claim': 'claim',
    'register_claim': 'claim',
    'bind_policy': 'policy_terms',
    'issue_policy': 'policy_terms',
    'verify': 'adjust',
    'verify_claim': 'adjust',
    'verify_loss': 'adjust',
    'adjust_loss': 'adjust',
    'loss_adjustment': 'adjust',
    'loss_adjust': 'adjust',
    'customer_confirm': 'confirm',
    'customer_accept': 'confirm',
    'accept_offer': 'confirm',
    'settle': 'settlement',
    'instruct_settlement': 'settlement',
    'pay_indemnity': 'payment_receipt',
    'execute_payment': 'payment_receipt',
    'issue_payment': 'payment_receipt',
    'pay': 'payment_receipt',
    'payment': 'payment_receipt',
    'payment_receipt': 'payment_receipt',
    'bounce': 'dishonour',
    'dishonor': 'dishonour',
    'return_payment': 'dishonour',
    'supplement': 'correction',
    'supplementary_assessment': 'correction',
    'late_correction': 'correction',
    'subrogation': 'subrogate',
    'open_subrogation': 'subrogate',
    'recovery': 'recovery_receipt',
    'record_recovery': 'recovery_receipt',
    'recovery_received': 'recovery_receipt',
    'position': 'coverage_position',
    'coverage_position': 'coverage_position',
}

# Tolerate the common ``*_id`` spelling of cross-resource references.  Only
# fills a canonical key when the canonical spelling is absent.
KEY_ALIASES = {
    'claim_id': 'claim',
    'policy_id': 'policy',
    'case_id': 'case',
    'receipt_id': 'receipt',
    'settlement_id': 'settlement',
    'invoice_id': 'invoice',
    'intent_id': 'intent',
    'workload': 'intent',
    'outage_ref': 'outage',
    'outage_id': 'outage',
    'tenant_id': 'tenant',
}


def _normalize_payload(x):
    x = dict(x)
    for alias, canonical in KEY_ALIASES.items():
        if canonical not in x and alias in x and x[alias] is not None:
            x[canonical] = x[alias]
    if 'amount' not in x and 'amount_cents' in x:
        x['amount'] = x['amount_cents']
    return x


def handle(db, raw):
    x = _normalize_payload(raw)
    op = ALIASES.get(x['op'], x['op'])

    if op == 'policy':
        return _op_policy(db, x)
    if op == 'policy_terms':
        return _op_policy_terms(db, x)
    if op == 'outage':
        return _op_outage(db, x)
    if op == 'invoice':
        return _op_invoice(db, x)
    if op == 'offset':
        return _op_offset(db, x)
    if op == 'claim':
        return _op_claim(db, x)
    if op == 'assess':
        return _op_assess_legacy(db, x)
    if op == 'adjust':
        return _op_adjust(db, x)
    if op == 'confirm':
        return _op_confirm(db, x)
    if op == 'settlement':
        return _settlement_instruction(db, x)
    if op == 'payment_receipt':
        return _op_payment_receipt(db, x)
    if op == 'dishonour':
        return _op_dishonour(db, x)
    if op == 'correction':
        return _op_correction(db, x)
    if op == 'subrogate':
        return _op_subrogate(db, x)
    if op == 'recovery_receipt':
        return _op_recovery(db, x)
    if op == 'coverage_position':
        return _op_position(db, x)
    return {'decision': 'unknown_operation'}


def run(db, x):
    return act(db, 'insurance', x, handle)
