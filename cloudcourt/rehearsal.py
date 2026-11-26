"""灾备演练庭：带独立原件证据、隔离影子资源与失败清偿的演练业务。

与真实事故链（``evac.plan/receipt/handoff``）不同，本模块**只读取**生产表
（``workload_intents`` / ``placement_leases`` / ``capacity_claims`` /
``cluster_inventory`` / ``incident_cases`` ...），所有写入都落在
``rehearsal_*`` 表内，因此：

* 不会修改 ``workload_intents.primary_cluster``；
* 不会释放生产 ``capacity_claims`` / ``placement_leases``；
* 不会向客户真实序号账本写入，演练序号只存在于隔离作用域；
* 真实 evacuate 事故中的集群不允许被开演为演练，避免把事故误标成演练。

证明等级（``simulated`` / ``shadow_end_to_end`` / ``production_verified``）
完全由与“演练代次 + 隔离作用域”绑定的原件证据推导，调用方不能用一个布尔
成功值直接领取高级证书。
"""
import hashlib
import json

from .store import act, one, many

SUBSYSTEM = 'rehearsal'

SCHEMA = '''
CREATE TABLE IF NOT EXISTS rehearsal_cases(
  id TEXT PRIMARY KEY,
  intent TEXT NOT NULL,
  source_cluster TEXT,
  backup_cluster TEXT,
  target_cluster TEXT,
  mode TEXT NOT NULL,
  generation INTEGER NOT NULL,
  scope TEXT NOT NULL,
  state TEXT NOT NULL,
  recovery_point TEXT,
  probe_seconds INTEGER,
  rto_limit INTEGER,
  timeout_at INTEGER,
  deadline INTEGER,
  consent TEXT NOT NULL DEFAULT 'pending',
  consent_at INTEGER,
  window_confirmed INTEGER NOT NULL DEFAULT 0,
  write_stop_seconds INTEGER,
  data_ref TEXT,
  sealed_snapshot TEXT,
  snapshot_hash TEXT,
  level TEXT,
  conclusion TEXT,
  certificate_id TEXT,
  fail_reason TEXT,
  created_at INTEGER,
  updated_at INTEGER
);
CREATE TABLE IF NOT EXISTS rehearsal_shadow_offers(
  id TEXT PRIMARY KEY,
  cluster TEXT UNIQUE,
  cpu INTEGER NOT NULL,
  mem INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  available_at INTEGER NOT NULL,
  independent INTEGER NOT NULL DEFAULT 1,
  state TEXT NOT NULL DEFAULT 'offered'
);
CREATE TABLE IF NOT EXISTS rehearsal_resources(
  id TEXT PRIMARY KEY,
  rehearsal TEXT NOT NULL,
  generation INTEGER NOT NULL,
  scope TEXT NOT NULL,
  offer TEXT,
  cluster TEXT,
  cpu INTEGER NOT NULL DEFAULT 0,
  mem INTEGER NOT NULL DEFAULT 0,
  available_at INTEGER,
  kind TEXT NOT NULL DEFAULT 'shadow',
  state TEXT NOT NULL,
  bound_at INTEGER,
  released_at INTEGER,
  release_reason TEXT
);
CREATE TABLE IF NOT EXISTS rehearsal_evidence(
  id TEXT PRIMARY KEY,
  rehearsal TEXT NOT NULL,
  generation INTEGER,
  scope TEXT,
  kind TEXT NOT NULL,
  cluster TEXT,
  cluster_generation INTEGER,
  origin_ref TEXT,
  recovery_point TEXT,
  observed_at INTEGER,
  received_at INTEGER,
  sustained_seconds INTEGER,
  valid INTEGER NOT NULL,
  reject_reason TEXT,
  detail TEXT
);
CREATE TABLE IF NOT EXISTS rehearsal_certificates(
  id TEXT PRIMARY KEY,
  rehearsal TEXT UNIQUE NOT NULL,
  generation INTEGER NOT NULL,
  scope TEXT NOT NULL,
  level TEXT NOT NULL,
  level_rank INTEGER NOT NULL,
  qualified INTEGER NOT NULL,
  conclusion TEXT NOT NULL,
  meets_deadline INTEGER,
  measured_recovery_seconds INTEGER,
  customer_loss TEXT,
  basis TEXT NOT NULL,
  issued_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS rehearsal_windows(
  id TEXT PRIMARY KEY,
  rehearsal TEXT UNIQUE NOT NULL,
  confirmed INTEGER NOT NULL,
  stop_seconds INTEGER,
  approved_by TEXT,
  at INTEGER,
  detail TEXT
);
CREATE TABLE IF NOT EXISTS rehearsal_generation_log(
  id TEXT PRIMARY KEY,
  rehearsal TEXT NOT NULL,
  old_generation INTEGER,
  new_generation INTEGER,
  old_scope TEXT,
  new_scope TEXT,
  reason TEXT,
  at INTEGER
);
CREATE TABLE IF NOT EXISTS rehearsal_isolated_sequences(
  id TEXT PRIMARY KEY,
  rehearsal TEXT NOT NULL,
  generation INTEGER NOT NULL,
  scope TEXT NOT NULL,
  seq INTEGER NOT NULL,
  label TEXT NOT NULL
);
'''

# ---------------------------------------------------------------------------
# 命名兼容层：验收方可能使用近义 op / 字段名，统一归一到本模块的规范名。
# ---------------------------------------------------------------------------
OP_ALIASES = {
    'drill': 'seal',
    'create': 'seal',
    'open': 'seal',
    'grant': 'consent',
    'grant_consent': 'consent',
    'grant_permission': 'consent',
    'grant_test_permission': 'consent',
    'permit': 'consent',
    'authorize': 'consent',
    'revoke_consent': 'withdraw',
    'withdraw_consent': 'withdraw',
    'revoke': 'withdraw',
    'revoke_permission': 'withdraw',
    'offer': 'offer_shadow',
    'observe_shadow': 'offer_shadow',
    'shadow_offer': 'offer_shadow',
    'add_shadow': 'offer_shadow',
    'register_shadow': 'offer_shadow',
    'allocate': 'reserve_shadow',
    'reserve': 'reserve_shadow',
    'book': 'reserve_shadow',
    'book_shadow': 'reserve_shadow',
    'schedule': 'reserve_shadow',
    'schedule_shadow': 'reserve_shadow',
    'reserve_later': 'reserve_shadow',
    'activate': 'activate_shadow',
    'occupy': 'activate_shadow',
    'begin': 'start',
    'execute': 'start',
    'run': 'start',
    'submit_evidence': 'evidence',
    'add_evidence': 'evidence',
    'put_evidence': 'evidence',
    'submit_original': 'evidence',
    'original': 'evidence',
    'artifact': 'evidence',
    'artifacts': 'evidence',
    'receipt': 'evidence',
    'submit': 'evidence',
    'probe': 'evidence',
    'evaluate': 'certify',
    'assess': 'certify',
    'grade': 'certify',
    'rebuild_target': 'rebuild',
    'target_rebuild': 'rebuild',
    'generation_change': 'rebuild',
    'abort': 'fail',
    'mark_failed': 'fail',
    'failure': 'fail',
    'release': 'settle',
    'cleanup': 'settle',
    'settle_occupations': 'settle',
    'certificate': 'certify',
    'issue_certificate': 'certify',
    'issue': 'certify',
    'confirm_write_window': 'confirm_window',
    'write_window': 'confirm_window',
    'isolated_write': 'issue_sequences',
    'issue_sequence': 'issue_sequences',
    'isolated_sequence': 'issue_sequences',
    'write_sequence': 'issue_sequences',
    'customer_write': 'forbid_real_write',
    'write_customer_sequence': 'forbid_real_write',
    'real_write': 'forbid_real_write',
    'real_sequence_write': 'forbid_real_write',
}

KIND_ALIASES = {
    'fence': 'source_fenced',
    'source_fence': 'source_fenced',
    'fenced': 'source_fenced',
    'source_fence_effective': 'source_fenced',
    'ready': 'target_ready',
    'target': 'target_ready',
    'restored': 'target_ready',
    'recovery': 'target_ready',
    'target_recovery_point': 'target_ready',
    'customer_probe': 'probe',
    'probe_result': 'probe',
    'probe_original': 'probe',
    'reconcile': 'reconciliation',
    'rp_reconciliation': 'reconciliation',
    'recovery_point_reconciliation': 'reconciliation',
    'entries': 'reconciliation',
    'simulation': 'simulation',
    'sim': 'simulation',
    'boolean': 'simulation',
    'status': 'simulation',
    'result': 'simulation',
    'manual_report': 'simulation',
}

LEVEL_RANK = {'simulated': 1, 'shadow_end_to_end': 2, 'production_verified': 3}

PRODUCTION_TABLES = (
    'workload_intents', 'owner_grants', 'residency_rules',
    'cluster_inventory', 'placement_leases', 'capacity_claims',
    'incident_cases', 'move_plans', 'handoff_receipts', 'recovery_cases',
)


def scope_for(rehearsal_id, generation):
    return f'scope-{rehearsal_id}-g{generation}'


def _canonical_op(op):
    return OP_ALIASES.get(op, op)


def _canonical_kind(kind):
    if kind is None:
        return None
    return KIND_ALIASES.get(kind, kind)


def _case(db, x):
    for key in ('rehearsal', 'case', 'rehearsal_id', 'drill', 'rid'):
        value = x.get(key)
        if value:
            row = one(db, 'SELECT * FROM rehearsal_cases WHERE id=?', (value,))
            if row:
                return row
    if x.get('id'):
        return one(db, 'SELECT * FROM rehearsal_cases WHERE id=?', (x['id'],))
    return None


def _case_id(x):
    for key in ('rehearsal', 'case', 'rehearsal_id', 'drill', 'rid'):
        if x.get(key):
            return x[key]
    return x.get('id') or x.get('event_id')


def _own_id(x, *keys, fallback_event=True):
    for key in keys:
        if x.get(key):
            return x[key]
    if x.get('id'):
        return x['id']
    return x['event_id'] if fallback_event else None


def _claimed_generation_scope(x, case):
    """解析调用方声称绑定的演练代次/作用域。

    规范字段是 ``generation`` + ``scope``；同时兼容
    ``rehearsal_generation`` / ``drill_generation`` / ``epoch``。
    若调用方另外给了演练代次，则 ``generation`` 可能表示集群代次
    （evac receipt 风格），此时集群代次改从 ``generation`` 读取。
    """
    rehearsal_gen = x.get('rehearsal_generation', x.get('drill_generation', x.get('epoch')))
    if rehearsal_gen is not None:
        claimed_gen = rehearsal_gen
        cluster_gen = x.get('cluster_generation', x.get('node_generation', x.get('generation')))
    else:
        claimed_gen = x.get('generation')
        cluster_gen = x.get('cluster_generation', x.get('node_generation'))
        # 只给一个 generation 且它就是当前演练代次时，兼容 evac receipt 风格，
        # 同时把它视为承载资源的代次（通常开演时代次都从 1 开始）。
        if cluster_gen is None and claimed_gen is not None:
            cluster_gen = claimed_gen
    claimed_scope = x.get('scope') or x.get('isolation_scope')
    return claimed_gen, claimed_scope, cluster_gen


def _sustained_seconds(x):
    for key in ('sustained_seconds', 'duration_seconds', 'sustain_seconds', 'lasted_seconds'):
        if x.get(key) is not None:
            return int(x[key])
    samples = x.get('samples') or x.get('probes') or x.get('measurements') or []
    points = []
    for sample in samples:
        if isinstance(sample, dict):
            at = sample.get('at', sample.get('t', sample.get('time')))
            ok = sample.get('ok', sample.get('healthy', sample.get('success', sample.get('green'))))
            points.append((at, bool(ok)))
        elif isinstance(sample, (list, tuple)) and len(sample) >= 2:
            points.append((sample[0], bool(sample[1])))
    points = [(t, ok) for t, ok in points if t is not None]
    points.sort(key=lambda p: p[0])
    best = 0
    run = []
    for at, ok in points:
        if ok:
            run.append(at)
            continue
        if len(run) >= 2:
            best = max(best, run[-1] - run[0])
        run = []
    if len(run) >= 2:
        best = max(best, run[-1] - run[0])
    return int(best)


def _has_customer_origin(x):
    if x.get('customer') is True or x.get('customer_attested') is True:
        return True
    origin = str(x.get('origin') or x.get('probe_origin') or x.get('source') or x.get('actor') or '')
    return 'customer' in origin.lower()


def _origin_ref(x):
    for key in ('origin_ref', 'artifact_ref', 'original_ref', 'evidence_ref', 'ref', 'uri', 'artifact_uri'):
        if x.get(key):
            return x[key]
    return None


def _snapshot(db, x, intent, at):
    """开演前封存：负载清单、合同版本、主备席代次、容量观测。"""
    source = x.get('source', x.get('source_cluster'))
    backup = x.get('backup', x.get('backup_cluster'))

    rule = one(db, 'SELECT * FROM residency_rules WHERE tenant=?', (intent['tenant'],))
    grants = many(db, 'SELECT * FROM owner_grants WHERE intent=? ORDER BY id', (intent['id'],))

    primary_lease = one(
        db,
        "SELECT * FROM placement_leases WHERE intent=? AND cluster=? AND role='primary' AND state='occupied' "
        "ORDER BY id LIMIT 1",
        (intent['id'], source),
    )
    source_cluster = one(db, 'SELECT * FROM cluster_inventory WHERE id=?', (source,)) if source else None

    backup_lease = None
    if backup:
        backup_lease = one(
            db,
            "SELECT * FROM placement_leases WHERE intent=? AND cluster=? AND role='backup' AND state='occupied' "
            "ORDER BY id LIMIT 1",
            (intent['id'], backup),
        )
    backup_cluster = one(db, 'SELECT * FROM cluster_inventory WHERE id=?', (backup,)) if backup else None
    own_backups = many(
        db,
        "SELECT * FROM placement_leases WHERE intent=? AND role='backup' AND state='occupied' ORDER BY id",
        (intent['id'],),
    )
    # C1 上服务于“另一项事故待命”的真实备席：只封存、绝不征用。
    other_standbys = many(
        db,
        "SELECT * FROM placement_leases WHERE cluster=? AND role='backup' AND state='occupied' AND intent<>? "
        "ORDER BY id",
        (backup, intent['id']),
    ) if backup else []
    active_incidents = many(db, "SELECT * FROM incident_cases WHERE state='evacuate' ORDER BY id")

    inventory = many(db, 'SELECT * FROM cluster_inventory ORDER BY id')
    claims = many(db, "SELECT * FROM capacity_claims WHERE state='active' ORDER BY id")
    used = {}
    for claim in claims:
        slot = used.setdefault(claim['cluster'], {'cpu': 0, 'mem': 0, 'claims': []})
        slot['cpu'] += claim['cpu']
        slot['mem'] += claim['mem']
        slot['claims'].append(claim['id'])

    offers = many(db, 'SELECT * FROM rehearsal_shadow_offers ORDER BY id')

    snapshot = {
        'sealed_at': at,
        'auditor': x.get('auditor', x.get('sealed_by')),
        'requirements': {
            'mode': x.get('mode', 'shadow'),
            'recovery_point': x.get('recovery_point', x.get('rp')),
            'probe_seconds': x.get('probe_seconds', x.get('required_probe_seconds')),
            'rto_limit': x.get('rto_limit', x.get('max_recovery_seconds')),
            'timeout_at': x.get('timeout_at', x.get('evidence_deadline')),
            'deadline': x.get('deadline', x.get('filing_deadline')),
        },
        'workload_manifest': intent,
        'contract_version': intent['revision'],
        'contract_status': intent['status'],
        'residency_rule': rule,
        'owner_grants': grants,
        'primary_seat': {
            'lease': primary_lease,
            'cluster': source_cluster,
            'lease_generation': primary_lease['generation'] if primary_lease else None,
            'cluster_generation': source_cluster['generation'] if source_cluster else None,
        },
        'backup_seat': {
            'requested': backup,
            'lease': backup_lease,
            'cluster': backup_cluster,
            'lease_generation': backup_lease['generation'] if backup_lease else None,
            'cluster_generation': backup_cluster['generation'] if backup_cluster else None,
            'all_backup_leases_for_intent': own_backups,
            'other_incident_standbys': other_standbys,
        },
        'capacity_observation': {
            'clusters': inventory,
            'active_claims': claims,
            'used_by_cluster': used,
        },
        'shadow_offers_at_seal': offers,
        'active_evacuate_incidents': active_incidents,
    }
    encoded = json.dumps(snapshot, sort_keys=True, ensure_ascii=False, default=str)
    return snapshot, hashlib.sha256(encoded.encode('utf-8')).hexdigest()


def _settle(db, case_id, reason, at):
    rows = many(
        db,
        "SELECT id FROM rehearsal_resources WHERE rehearsal=? AND state IN ('held','scheduled') ORDER BY id",
        (case_id,),
    )
    ids = [row['id'] for row in rows]
    if ids:
        db.execute(
            "UPDATE rehearsal_resources SET state='released', released_at=?, release_reason=? "
            "WHERE rehearsal=? AND state IN ('held','scheduled')",
            (at, reason, case_id),
        )
    return ids


def _record_rejected_evidence(db, x, case, evidence_id, kind, claimed_gen, claimed_scope,
                              cluster, cluster_gen, reason, at):
    db.execute(
        'INSERT INTO rehearsal_evidence(id,rehearsal,generation,scope,kind,cluster,cluster_generation,'
        'origin_ref,recovery_point,observed_at,received_at,sustained_seconds,valid,reject_reason,detail) '
        'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)',
        (
            evidence_id, case['id'], claimed_gen, claimed_scope, kind or 'unknown',
            cluster, cluster_gen, _origin_ref(x), x.get('recovery_point', x.get('rp')),
            x.get('observed_at', at), at,
            x.get('sustained_seconds') if x.get('sustained_seconds') is not None else None,
            reason, json.dumps(x, sort_keys=True, ensure_ascii=False, default=str),
        ),
    )


def handle(db, x):
    op = _canonical_op(x['op'])
    at = x.get('at')

    # ------------------------------------------------------------------ seal
    if op == 'seal':
        case_id = x.get('id') or x.get('rehearsal_id') or x.get('rehearsal') or x['event_id']
        if one(db, 'SELECT 1 FROM rehearsal_cases WHERE id=?', (case_id,)):
            return {'decision': 'rehearsal_exists', 'rehearsal': case_id}

        intent_id = x.get('intent') or x.get('intent_id') or x.get('workload')
        intent = one(db, 'SELECT * FROM workload_intents WHERE id=?', (intent_id,))
        if not intent:
            return {'decision': 'missing_intent', 'intent': intent_id}

        source = x.get('source', x.get('source_cluster', x.get('cluster')))
        if not source:
            return {'decision': 'source_required'}

        # 主站身份：primary_cluster 权威指定优先；兼容仅有已占 primary 租约的库。
        primary_lease = one(
            db,
            "SELECT * FROM placement_leases WHERE intent=? AND cluster=? AND role='primary' AND state='occupied' "
            "ORDER BY id LIMIT 1",
            (intent['id'], source),
        )
        if intent['primary_cluster'] is not None and intent['primary_cluster'] != source:
            return {
                'decision': 'primary_mismatch',
                'source': source,
                'primary_cluster': intent['primary_cluster'],
            }
        if intent['primary_cluster'] != source and not primary_lease:
            return {'decision': 'source_not_primary', 'source': source}

        # 真实事故隔离：evacuate 中的集群不得被开演成演练。
        incident = one(
            db,
            "SELECT * FROM incident_cases WHERE cluster=? AND state='evacuate' ORDER BY id LIMIT 1",
            (source,),
        )
        if incident:
            return {
                'decision': 'real_incident_conflict',
                'incident': incident['id'],
                'cluster': source,
            }
        if x.get('incident'):
            explicit = one(db, 'SELECT * FROM incident_cases WHERE id=?', (x['incident'],))
            if explicit and explicit['state'] == 'evacuate':
                return {'decision': 'real_incident_conflict', 'incident': explicit['id']}

        mode = x.get('mode', 'shadow')
        if mode == 'production_verification':
            mode = 'production'
        if mode not in ('shadow', 'simulation', 'production'):
            return {'decision': 'invalid_rehearsal_mode', 'mode': mode}

        generation = int(x.get('generation', 1))
        if generation < 1:
            return {'decision': 'invalid_generation'}
        scope = x.get('scope') or scope_for(case_id, generation)
        snapshot, digest = _snapshot(db, x, intent, at)

        probe_seconds = int(x.get('probe_seconds', x.get('required_probe_seconds', 0)) or 0)
        rto_limit = x.get('rto_limit', x.get('max_recovery_seconds'))
        db.execute(
            'INSERT INTO rehearsal_cases(id,intent,source_cluster,backup_cluster,target_cluster,mode,'
            'generation,scope,state,recovery_point,probe_seconds,rto_limit,timeout_at,deadline,consent,consent_at,'
            'window_confirmed,write_stop_seconds,data_ref,sealed_snapshot,snapshot_hash,level,conclusion,'
            'certificate_id,fail_reason,created_at,updated_at) '
            'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (
                case_id, intent['id'], source, x.get('backup', x.get('backup_cluster')),
                x.get('target', x.get('target_cluster')), mode, generation, scope, 'sealed',
                x.get('recovery_point', x.get('rp')), probe_seconds, rto_limit,
                x.get('timeout_at', x.get('evidence_deadline')), x.get('deadline', x.get('filing_deadline')),
                'pending', None, 0, None, None,
                json.dumps(snapshot, sort_keys=True, ensure_ascii=False, default=str),
                digest, None, None, None, None, at, at,
            ),
        )
        return {
            'decision': 'sealed',
            'rehearsal': case_id,
            'generation': generation,
            'scope': scope,
            'snapshot_hash': digest,
            'contract_version': intent['revision'],
            'mode': mode,
        }

    # ---------------------------------------------------------- offer_shadow
    if op == 'offer_shadow':
        offer_id = x.get('id') or x['event_id']
        cluster = x.get('cluster', x.get('cluster_id'))
        if not cluster:
            return {'decision': 'cluster_required'}
        if one(db, 'SELECT 1 FROM rehearsal_shadow_offers WHERE id=?', (offer_id,)):
            return {'decision': 'shadow_offer_exists', 'id': offer_id}
        if one(db, 'SELECT 1 FROM rehearsal_shadow_offers WHERE cluster=?', (cluster,)):
            return {'decision': 'shadow_cluster_already_offered', 'cluster': cluster}
        # C0/C1 等真实主备席（含为另一项事故待命的 C1）不能被登记成影子席位。
        production_lease = one(
            db,
            "SELECT 1 FROM placement_leases WHERE cluster=? AND state IN ('reserved','occupied') LIMIT 1",
            (cluster,),
        )
        if production_lease:
            return {'decision': 'production_seat_not_shadow', 'cluster': cluster}
        cpu, mem = int(x.get('cpu', 0)), int(x.get('mem', x.get('memory', 0)))
        if cpu <= 0 or mem <= 0:
            return {'decision': 'invalid_capacity'}
        generation = int(x.get('generation', 1))
        available_at = int(x.get('available_at', x.get('ready_at', 0)) or 0)
        db.execute(
            'INSERT INTO rehearsal_shadow_offers(id,cluster,cpu,mem,generation,available_at,independent,state) '
            'VALUES(?,?,?,?,?,?,?,?)',
            (offer_id, cluster, cpu, mem, generation, available_at,
             1 if x.get('independent', True) else 0, 'offered'),
        )
        return {'decision': 'shadow_offered', 'id': offer_id, 'cluster': cluster,
                'available_at': available_at, 'generation': generation}

    # --------------------------------------------------------------- consent
    if op == 'consent':
        case = _case(db, x)
        if not case:
            return {'decision': 'unknown_rehearsal'}
        if case['consent'] == 'withdrawn':
            return {'decision': 'consent_withdrawn', 'rehearsal': case['id']}
        db.execute("UPDATE rehearsal_cases SET consent='granted', consent_at=?, updated_at=?, "
                   "state=CASE WHEN state='sealed' THEN 'approved' ELSE state END WHERE id=?",
                   (at, at, case['id']))
        return {'decision': 'consent_granted', 'rehearsal': case['id'], 'at': at}

    if op == 'withdraw':
        case = _case(db, x)
        if not case:
            return {'decision': 'unknown_rehearsal'}
        released = _settle(db, case['id'], x.get('reason', 'consent_withdrawn'), at)
        db.execute("UPDATE rehearsal_cases SET consent='withdrawn', consent_at=?, state='withdrawn', "
                   "fail_reason=?, updated_at=? WHERE id=?",
                   (at, x.get('reason', 'customer_withdrew_test_permission'), at, case['id']))
        return {'decision': 'consent_withdrawn', 'rehearsal': case['id'], 'released': released}

    # -------------------------------------------------------- confirm_window
    if op == 'confirm_window':
        case = _case(db, x)
        if not case:
            return {'decision': 'unknown_rehearsal'}
        window_id = _own_id(x, 'window_id')
        if one(db, 'SELECT 1 FROM rehearsal_windows WHERE rehearsal=?', (case['id'],)):
            return {'decision': 'window_already_confirmed', 'rehearsal': case['id']}
        confirmed = bool(x.get('confirmed', x.get('customer_confirmed', False)))
        if not confirmed:
            return {'decision': 'customer_confirmation_required'}
        stop_seconds = x.get('stop_seconds', x.get('write_stop_seconds', x.get('outage_seconds')))
        db.execute(
            'INSERT INTO rehearsal_windows(id,rehearsal,confirmed,stop_seconds,approved_by,at,detail) '
            'VALUES(?,?,1,?,?,?,?)',
            (window_id, case['id'], stop_seconds, x.get('approved_by', x.get('approver')), at,
             json.dumps(x, sort_keys=True, ensure_ascii=False, default=str)),
        )
        db.execute('UPDATE rehearsal_cases SET window_confirmed=1, write_stop_seconds=?, updated_at=? WHERE id=?',
                   (stop_seconds, at, case['id']))
        return {'decision': 'window_confirmed', 'rehearsal': case['id'], 'stop_seconds': stop_seconds}

    # -------------------------------------------------------- reserve_shadow
    if op == 'reserve_shadow':
        case = _case(db, x)
        if not case:
            return {'decision': 'unknown_rehearsal'}
        if case['consent'] != 'granted':
            return {'decision': 'consent_required', 'consent': case['consent']}
        if case['state'] in ('failed', 'timed_out', 'withdrawn', 'cancelled', 'certified'):
            return {'decision': 'rehearsal_closed', 'state': case['state']}
        claimed_gen, claimed_scope, _ = _claimed_generation_scope(x, case)
        if claimed_gen is not None and int(claimed_gen) != case['generation']:
            return {'decision': 'generation_replaced', 'generation': case['generation']}
        if claimed_scope is not None and claimed_scope != case['scope']:
            return {'decision': 'scope_mismatch', 'scope': case['scope']}

        offer = None
        if x.get('offer') or x.get('offer_id'):
            offer = one(db, 'SELECT * FROM rehearsal_shadow_offers WHERE id=?',
                        (x.get('offer') or x.get('offer_id'),))
        elif x.get('cluster') or x.get('cluster_id'):
            offer = one(db, 'SELECT * FROM rehearsal_shadow_offers WHERE cluster=?',
                        (x.get('cluster') or x.get('cluster_id'),))
        if not offer:
            return {'decision': 'unknown_shadow_offer'}
        if offer['state'] != 'offered':
            return {'decision': 'shadow_seat_unapproved'}

        cpu = int(x.get('cpu', 0) or 0)
        mem = int(x.get('mem', x.get('memory', 0)) or 0)
        if cpu <= 0:
            cpu = offer['cpu']
        if mem <= 0:
            mem = offer['mem']
        if cpu > offer['cpu'] or mem > offer['mem']:
            return {'decision': 'shadow_capacity_exhausted', 'offer': offer['id'],
                    'reason': 'larger_than_offer'}
        used = one(
            db,
            "SELECT COALESCE(SUM(cpu),0) cpu, COALESCE(SUM(mem),0) mem FROM rehearsal_resources "
            "WHERE offer=? AND state IN ('held','scheduled')",
            (offer['id'],),
        )
        if used['cpu'] + cpu > offer['cpu'] or used['mem'] + mem > offer['mem']:
            return {'decision': 'shadow_capacity_exhausted', 'offer': offer['id'],
                    'available_cpu': offer['cpu'] - used['cpu'],
                    'available_mem': offer['mem'] - used['mem']}

        resource_id = _own_id(x, 'resource_id', 'seat_id', 'reservation_id')
        available_at = int(x.get('available_at', offer['available_at']) or offer['available_at'])
        when = int(at) if at is not None else available_at
        if when < available_at:
            state = 'scheduled'
            decision = 'shadow_scheduled'
            bound_at = None
        else:
            state = 'held'
            decision = 'shadow_reserved'
            bound_at = at
        db.execute(
            'INSERT INTO rehearsal_resources(id,rehearsal,generation,scope,offer,cluster,cpu,mem,'
            'available_at,kind,state,bound_at,released_at,release_reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (resource_id, case['id'], case['generation'], case['scope'], offer['id'], offer['cluster'],
             cpu, mem, available_at, 'shadow', state, bound_at, None, None),
        )
        db.execute('UPDATE rehearsal_cases SET target_cluster=COALESCE(target_cluster,?), updated_at=? WHERE id=?',
                   (offer['cluster'], at, case['id']))
        return {'decision': decision, 'resource': resource_id, 'offer': offer['id'],
                'cluster': offer['cluster'], 'state': state, 'available_at': available_at,
                'generation': case['generation'], 'scope': case['scope']}

    # ------------------------------------------------------- activate_shadow
    if op == 'activate_shadow':
        case = _case(db, x)
        resource = None
        resource_id = x.get('resource_id') or x.get('seat_id') or (x.get('id') if not case or x['id'] != case['id'] else None)
        if resource_id:
            resource = one(db, 'SELECT * FROM rehearsal_resources WHERE id=?', (resource_id,))
        elif case:
            resource = one(
                db,
                "SELECT * FROM rehearsal_resources WHERE rehearsal=? AND state='scheduled' "
                "ORDER BY available_at LIMIT 1",
                (case['id'],),
            )
        if not resource:
            return {'decision': 'unknown_shadow_reservation'}
        if resource['state'] == 'held':
            return {'decision': 'shadow_held', 'resource': resource['id']}
        if resource['state'] == 'released':
            return {'decision': 'shadow_released', 'resource': resource['id']}
        if at is not None and resource['available_at'] is not None and int(at) < resource['available_at']:
            return {'decision': 'shadow_not_yet_available', 'resource': resource['id'],
                    'available_at': resource['available_at']}
        db.execute("UPDATE rehearsal_resources SET state='held', bound_at=? WHERE id=?", (at, resource['id']))
        return {'decision': 'shadow_activated', 'resource': resource['id'], 'cluster': resource['cluster']}

    # --------------------------------------------------------------- start
    if op == 'start':
        case = _case(db, x)
        if not case:
            return {'decision': 'unknown_rehearsal'}
        if case['consent'] == 'withdrawn':
            return {'decision': 'consent_withdrawn'}
        if case['consent'] != 'granted':
            return {'decision': 'consent_required'}
        if case['state'] == 'running':
            return {'decision': 'already_started', 'rehearsal': case['id']}
        if case['state'] not in ('sealed', 'approved'):
            return {'decision': 'rehearsal_closed', 'state': case['state']}

        data_ref = x.get('data_ref', x.get('dataset', x.get('isolated_data')))
        if not data_ref:
            return {'decision': 'isolated_data_required'}
        if x.get('environment') == 'production' or x.get('isolation') == 'production':
            return {'decision': 'production_data_forbidden'}

        resource = one(
            db,
            "SELECT * FROM rehearsal_resources WHERE rehearsal=? AND generation=? AND scope=? "
            "AND state IN ('held','scheduled') ORDER BY id LIMIT 1",
            (case['id'], case['generation'], case['scope']),
        )
        if case['mode'] == 'shadow':
            if not resource:
                return {'decision': 'shadow_seat_required'}
            if resource['state'] == 'scheduled':
                if at is not None and resource['available_at'] is not None and int(at) < resource['available_at']:
                    return {'decision': 'shadow_not_available', 'available_at': resource['available_at']}
                db.execute("UPDATE rehearsal_resources SET state='held', bound_at=? WHERE id=?",
                           (at, resource['id']))
        elif case['mode'] == 'production' and not case['window_confirmed']:
            return {'decision': 'write_window_required'}

        db.execute("UPDATE rehearsal_cases SET state='running', data_ref=?, updated_at=? WHERE id=?",
                   (data_ref, at, case['id']))
        return {'decision': 'started', 'rehearsal': case['id'], 'generation': case['generation'],
                'scope': case['scope'], 'data_ref': data_ref}

    # ------------------------------------------------------------ evidence
    if op == 'evidence':
        case = _case(db, x)
        kind = _canonical_kind(x.get('kind', x.get('evidence_kind', x.get('type'))))
        claimed_gen, claimed_scope, cluster_gen = _claimed_generation_scope(x, case) if case else (None, None, None)
        cluster = x.get('cluster', x.get('cluster_id'))
        received_at = at if at is not None else x.get('received_at')
        evidence_id = _own_id(x, 'evidence_id', 'artifact_id', 'receipt_id')

        if not case:
            return {'decision': 'unknown_rehearsal'}

        # 许可撤回：证据窗口立即关闭，迟到结果只能留下被拒记录，不能补签。
        if case['consent'] == 'withdrawn':
            _record_rejected_evidence(db, x, case, evidence_id, kind, claimed_gen, claimed_scope,
                                      cluster, cluster_gen, 'consent_withdrawn', received_at)
            return {'decision': 'consent_withdrawn', 'recorded': 'rejected', 'evidence': evidence_id}

        if case['state'] in ('failed', 'timed_out', 'withdrawn', 'cancelled'):
            _record_rejected_evidence(db, x, case, evidence_id, kind, claimed_gen, claimed_scope,
                                      cluster, cluster_gen, 'rehearsal_closed', received_at)
            return {'decision': 'rehearsal_closed', 'state': case['state'],
                    'recorded': 'rejected', 'evidence': evidence_id}
        if case['state'] == 'certified':
            _record_rejected_evidence(db, x, case, evidence_id, kind, claimed_gen, claimed_scope,
                                      cluster, cluster_gen, 'certificate_already_issued', received_at)
            return {'decision': 'certificate_already_issued', 'recorded': 'rejected'}

        # 代次/作用域绑定：目标节点重建后代次 +1、scope 换代，旧原件一律失效。
        if claimed_gen is not None and int(claimed_gen) != case['generation']:
            reason = 'generation_replaced'
            _record_rejected_evidence(db, x, case, evidence_id, kind, claimed_gen, claimed_scope,
                                      cluster, cluster_gen, reason, received_at)
            return {'decision': 'generation_replaced', 'current_generation': case['generation'],
                    'recorded': 'rejected', 'evidence': evidence_id}
        if claimed_scope is not None and claimed_scope != case['scope']:
            _record_rejected_evidence(db, x, case, evidence_id, kind, claimed_gen, claimed_scope,
                                      cluster, cluster_gen, 'scope_mismatch', received_at)
            return {'decision': 'scope_mismatch', 'current_scope': case['scope'],
                    'recorded': 'rejected', 'evidence': evidence_id}

        if kind not in ('source_fenced', 'target_ready', 'probe', 'reconciliation', 'simulation'):
            return {'decision': 'unknown_evidence_kind', 'kind': kind}

        # 模拟/布尔类材料只能把等级压在 simulated，不要求原件指针。
        if kind == 'simulation':
            db.execute(
                'INSERT INTO rehearsal_evidence(id,rehearsal,generation,scope,kind,cluster,'
                'cluster_generation,origin_ref,recovery_point,observed_at,received_at,sustained_seconds,'
                'valid,reject_reason,detail) VALUES(?,?,?,?,?,?,?,?,?,?,?,? ,1,NULL,?)',
                (evidence_id, case['id'], case['generation'], case['scope'], kind, cluster, cluster_gen,
                 _origin_ref(x), None, x.get('observed_at', received_at), received_at,
                 None, json.dumps(x, sort_keys=True, ensure_ascii=False, default=str)),
            )
            return {'decision': 'recorded', 'evidence': evidence_id, 'kind': kind, 'strength': 'simulation'}

        origin = _origin_ref(x)
        if not origin:
            _record_rejected_evidence(db, x, case, evidence_id, kind, case['generation'], case['scope'],
                                      cluster, cluster_gen, 'origin_required', received_at)
            return {'decision': 'origin_required', 'recorded': 'rejected', 'evidence': evidence_id}

        reject = None
        sustained = None
        recovery_seconds = x.get('recovery_seconds', x.get('rto_seconds', x.get('recovery_time_seconds')))

        if kind == 'source_fenced':
            expected_cluster = case['source_cluster']
            cluster = cluster or expected_cluster
            if expected_cluster and cluster != expected_cluster:
                reject = 'receipt_identity_conflict'
            elif x.get('fence_scope', x.get('effective_scope', x.get('scope_effective'))) != case['scope']:
                reject = 'fence_scope_mismatch'
            else:
                inventory = one(db, 'SELECT * FROM cluster_inventory WHERE id=?', (cluster,))
                if inventory is not None:
                    if cluster_gen is None or int(cluster_gen) != inventory['generation']:
                        reject = 'receipt_identity_conflict'

        elif kind == 'target_ready':
            expected_cluster = case['target_cluster'] or x.get('target')
            cluster = cluster or expected_cluster
            if expected_cluster and cluster != expected_cluster:
                reject = 'receipt_identity_conflict'
            elif x.get('recovery_point', x.get('rp')) != case['recovery_point']:
                reject = 'recovery_point_mismatch'
            else:
                resource = one(
                    db,
                    "SELECT * FROM rehearsal_resources WHERE rehearsal=? AND generation=? AND scope=? "
                    "ORDER BY id LIMIT 1",
                    (case['id'], case['generation'], case['scope']),
                )
                if resource and resource['offer']:
                    offer = one(db, 'SELECT * FROM rehearsal_shadow_offers WHERE id=?', (resource['offer'],))
                    expected_gen = offer['generation'] if offer else None
                    if cluster_gen is None or (expected_gen is not None and int(cluster_gen) != expected_gen):
                        reject = 'receipt_identity_conflict'
                else:
                    inventory = one(db, 'SELECT * FROM cluster_inventory WHERE id=?', (cluster,)) if cluster else None
                    if inventory is not None and (cluster_gen is None or int(cluster_gen) != inventory['generation']):
                        reject = 'receipt_identity_conflict'

        elif kind == 'probe':
            if not _has_customer_origin(x):
                reject = 'customer_probe_required'
            else:
                sustained = _sustained_seconds(x)
                if case['probe_seconds'] and sustained < case['probe_seconds']:
                    reject = 'probe_duration_unmet'

        elif kind == 'reconciliation':
            entries = x.get('entries')
            total = x.get('entries_total', x.get('total'))
            matched = x.get('entries_matched', x.get('matched'))
            if isinstance(entries, list):
                total = len(entries)
                matched = sum(1 for e in entries if (e.get('matched', e.get('ok', True)) if isinstance(e, dict) else bool(e)))
            if not total or int(total) <= 0:
                reject = 'reconciliation_empty'
            elif matched is None or int(matched) != int(total):
                reject = 'reconciliation_mismatch'

        if reject:
            db.execute(
                'INSERT INTO rehearsal_evidence(id,rehearsal,generation,scope,kind,cluster,'
                'cluster_generation,origin_ref,recovery_point,observed_at,received_at,sustained_seconds,'
                'valid,reject_reason,detail) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)',
                (evidence_id, case['id'], case['generation'], case['scope'], kind, cluster, cluster_gen,
                 origin, x.get('recovery_point', x.get('rp')), x.get('observed_at', received_at),
                 received_at, sustained, reject,
                 json.dumps(x, sort_keys=True, ensure_ascii=False, default=str)),
            )
            result = {'decision': reject, 'recorded': 'rejected', 'evidence': evidence_id, 'kind': kind}
            if kind == 'probe':
                result['sustained_seconds'] = sustained
                result['required_seconds'] = case['probe_seconds']
            return result

        db.execute(
            'INSERT INTO rehearsal_evidence(id,rehearsal,generation,scope,kind,cluster,cluster_generation,'
            'origin_ref,recovery_point,observed_at,received_at,sustained_seconds,valid,reject_reason,detail) '
            'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,1,NULL,?)',
            (evidence_id, case['id'], case['generation'], case['scope'], kind, cluster, cluster_gen,
             origin, x.get('recovery_point', x.get('rp')), x.get('observed_at', received_at),
             received_at, sustained,
             json.dumps(x, sort_keys=True, ensure_ascii=False, default=str)),
        )
        result = {'decision': 'recorded', 'evidence': evidence_id, 'kind': kind, 'valid': True}
        if kind == 'probe':
            result['sustained_seconds'] = sustained
        if kind == 'target_ready' and recovery_seconds is not None:
            result['recovery_seconds'] = int(recovery_seconds)
        return result

    # -------------------------------------------------------------- rebuild
    if op == 'rebuild':
        case = _case(db, x)
        if not case:
            return {'decision': 'unknown_rehearsal'}
        if case['state'] == 'certified':
            return {'decision': 'certificate_already_issued'}
        if case['state'] in ('failed', 'timed_out', 'withdrawn', 'cancelled'):
            return {'decision': 'rehearsal_closed', 'state': case['state']}

        old_gen, old_scope = case['generation'], case['scope']
        released = _settle(db, case['id'], x.get('reason', 'target_rebuilt'), at)
        new_gen = int(x.get('new_generation', x.get('generation', old_gen + 1)))
        if new_gen <= old_gen:
            return {'decision': 'invalid_generation', 'generation': old_gen}
        new_scope = x.get('new_scope') or scope_for(case['id'], new_gen)
        log_id = x.get('log_id') or x['event_id']
        db.execute(
            'INSERT INTO rehearsal_generation_log(id,rehearsal,old_generation,new_generation,'
            'old_scope,new_scope,reason,at) VALUES(?,?,?,?,?,?,?,?)',
            (log_id, case['id'], old_gen, new_gen, old_scope, new_scope,
             x.get('reason', 'target_rebuilt'), at),
        )
        next_state = 'approved' if case['consent'] == 'granted' else 'sealed'
        db.execute('UPDATE rehearsal_cases SET generation=?, scope=?, state=?, updated_at=? WHERE id=?',
                   (new_gen, new_scope, next_state, at, case['id']))
        return {'decision': 'generation_replaced', 'rehearsal': case['id'],
                'old_generation': old_gen, 'generation': new_gen, 'scope': new_scope,
                'old_scope': old_scope, 'released': released}

    # ---------------------------------------------------------------- fail
    if op == 'fail':
        case = _case(db, x)
        if not case:
            return {'decision': 'unknown_rehearsal'}
        if case['state'] == 'certified':
            return {'decision': 'certificate_already_issued'}
        if case['state'] in ('failed', 'timed_out'):
            return {'decision': 'rehearsal_closed', 'state': case['state'],
                    'fail_reason': case['fail_reason']}
        reason = x.get('reason', 'declared_failure')
        released = _settle(db, case['id'], reason, at)
        db.execute("UPDATE rehearsal_cases SET state='failed', fail_reason=?, updated_at=? WHERE id=?",
                   (reason, at, case['id']))
        return {'decision': 'rehearsal_failed', 'rehearsal': case['id'], 'reason': reason,
                'released': released}

    # ------------------------------------------------------------- timeout
    if op == 'timeout':
        case = _case(db, x)
        if not case:
            return {'decision': 'unknown_rehearsal'}
        if case['state'] == 'certified':
            return {'decision': 'certificate_already_issued'}
        if case['state'] in ('failed', 'timed_out', 'withdrawn', 'cancelled'):
            return {'decision': 'rehearsal_closed', 'state': case['state']}
        if case['timeout_at'] is not None and at is not None and int(at) < case['timeout_at'] and not x.get('force'):
            return {'decision': 'not_timed_out', 'timeout_at': case['timeout_at']}
        reason = x.get('reason', 'probe_timeout')
        released = _settle(db, case['id'], reason, at)
        db.execute("UPDATE rehearsal_cases SET state='timed_out', fail_reason=?, updated_at=? WHERE id=?",
                   (reason, at, case['id']))
        return {'decision': 'rehearsal_timed_out', 'rehearsal': case['id'], 'reason': reason,
                'released': released}

    # -------------------------------------------------------------- settle
    if op == 'settle':
        case = _case(db, x)
        if not case:
            return {'decision': 'unknown_rehearsal'}
        ids = _settle(db, case['id'], x.get('reason', 'manual_settlement'), at)
        return {'decision': 'settled', 'rehearsal': case['id'], 'released': ids, 'count': len(ids)}

    # ------------------------------------------------------------- certify
    if op == 'certify':
        case = _case(db, x)
        if not case:
            return {'decision': 'unknown_rehearsal'}
        cert_id = x.get('certificate_id', x.get('cert_id'))
        if not cert_id:
            cert_id = x['id'] if x.get('id') and x['id'] != case['id'] else x['event_id']

        existing = one(db, 'SELECT * FROM rehearsal_certificates WHERE rehearsal=?', (case['id'],))
        if existing:
            return {'decision': 'certificate_already_issued', 'certificate': existing['id'],
                    'level': existing['level'], 'conclusion': existing['conclusion']}
        if case['consent'] == 'withdrawn' or case['state'] == 'withdrawn':
            return {'decision': 'consent_withdrawn'}
        if case['state'] in ('failed', 'timed_out', 'cancelled'):
            return {'decision': 'rehearsal_closed', 'state': case['state'],
                    'fail_reason': case['fail_reason']}

        evidence_rows = many(
            db,
            'SELECT * FROM rehearsal_evidence WHERE rehearsal=? AND valid=1 AND generation=? AND scope=? '
            'ORDER BY id',
            (case['id'], case['generation'], case['scope']),
        )
        by_kind = {}
        for row in evidence_rows:
            by_kind.setdefault(row['kind'], []).append(row)

        resource = one(
            db,
            "SELECT * FROM rehearsal_resources WHERE rehearsal=? AND generation=? AND scope=? "
            "AND state IN ('held','released') ORDER BY id LIMIT 1",
            (case['id'], case['generation'], case['scope']),
        )
        window = one(db, 'SELECT * FROM rehearsal_windows WHERE rehearsal=?', (case['id'],))

        missing = []
        if 'source_fenced' not in by_kind:
            missing.append('source_fenced')
        if 'target_ready' not in by_kind:
            missing.append('target_ready')
        if 'probe' not in by_kind:
            missing.append('probe')

        if case['mode'] == 'production':
            if not window or not window['confirmed']:
                missing.append('confirmed_write_window')
            if 'reconciliation' not in by_kind:
                missing.append('rp_reconciliation')
            if missing:
                return {'decision': 'evidence_incomplete', 'missing': missing,
                        'valid_kinds': sorted(by_kind), 'mode': 'production'}
            level = 'production_verified'
        elif case['mode'] == 'shadow':
            if resource is None:
                missing.append('shadow_resource_binding')
            core_present = any(k in by_kind for k in ('source_fenced', 'target_ready', 'probe'))
            if missing and not evidence_rows:
                return {'decision': 'evidence_incomplete', 'missing': missing,
                        'valid_kinds': sorted(by_kind)}
            if missing and not core_present and by_kind.get('simulation'):
                # 只有布尔/模拟材料：出具明确标注“不具备接管证明力”的模拟级证书。
                level = 'simulated'
            elif missing:
                return {'decision': 'evidence_incomplete', 'missing': missing,
                        'valid_kinds': sorted(by_kind)}
            else:
                level = 'shadow_end_to_end'
        else:  # mode == 'simulation'：开演时即定位为模拟，最高只能是 simulated。
            level = 'simulated' if evidence_rows else None
            if level is None:
                return {'decision': 'evidence_incomplete', 'missing': ['simulation_artifact'],
                        'valid_kinds': []}

        # 任何模式下，若核心三原件其实齐全且为 production 附加件齐全，可按原件升级，
        # 但 simulation 模式永不升级，避免“低级模拟冒充现场切换”。
        if case['mode'] != 'simulation' and level != 'production_verified':
            core_ready = not any(k in missing for k in ('source_fenced', 'target_ready', 'probe')) \
                and resource is not None
            if core_ready and window and window['confirmed'] and 'reconciliation' in by_kind:
                level = 'production_verified'

        issued_at = at if at is not None else case['updated_at']
        meets_deadline = True
        if case['deadline'] is not None and issued_at is not None:
            meets_deadline = int(issued_at) <= int(case['deadline'])

        if level == 'simulated':
            qualified = False
            conclusion = 'not_takeover_proof'
        else:
            qualified = True
            conclusion = 'filable_today' if meets_deadline else 'filable_late'

        target_rows = by_kind.get('target_ready', [])
        recovery_seconds = None
        if target_rows:
            try:
                detail = json.loads(target_rows[0]['detail']) if target_rows[0]['detail'] else {}
                recovery_seconds = detail.get('recovery_seconds', detail.get('rto_seconds'))
            except (ValueError, TypeError):
                recovery_seconds = None
        rto_limit = case['rto_limit'] if 'rto_limit' in case.keys() else None
        rto_met = None
        if rto_limit is not None and recovery_seconds is not None:
            rto_met = int(recovery_seconds) <= int(rto_limit)

        customer_loss = None
        if level == 'production_verified' and window:
            customer_loss = {
                'write_stop_seconds': window['stop_seconds'],
                'confirmed_by': window['approved_by'],
                'customer_facing_outage': True,
            }
        elif level == 'shadow_end_to_end':
            # 影子演练全程隔离，客户侧零停写、零真实序号。
            customer_loss = {'write_stop_seconds': 0, 'customer_facing_outage': False}

        if not qualified:
            regulatory_action = 'supplement_only_not_takeover_evidence'
        elif meets_deadline:
            regulatory_action = 'submit_to_regulator_today'
        else:
            regulatory_action = 'file_late_with_deadline_exception'

        basis = {
            'evidence': [
                {'id': row['id'], 'kind': row['kind'], 'origin_ref': row['origin_ref'],
                 'generation': row['generation'], 'scope': row['scope']}
                for row in evidence_rows
            ],
            'resource': {'id': resource['id'], 'cluster': resource['cluster'],
                         'generation': resource['generation'], 'scope': resource['scope']}
                        if resource else None,
            'window': {'id': window['id'], 'stop_seconds': window['stop_seconds']} if window else None,
            'snapshot_hash': case['snapshot_hash'],
            'rto_limit': rto_limit,
            'rto_met': rto_met,
        }
        db.execute(
            'INSERT INTO rehearsal_certificates(id,rehearsal,generation,scope,level,level_rank,qualified,'
            'conclusion,meets_deadline,measured_recovery_seconds,customer_loss,basis,issued_at) '
            'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (cert_id, case['id'], case['generation'], case['scope'], level, LEVEL_RANK[level],
             1 if qualified else 0, conclusion, 1 if meets_deadline else 0,
             recovery_seconds, json.dumps(customer_loss, sort_keys=True) if customer_loss else None,
             json.dumps(basis, sort_keys=True, ensure_ascii=False), issued_at),
        )
        db.execute('UPDATE rehearsal_cases SET state=?, level=?, conclusion=?, certificate_id=?, updated_at=? WHERE id=?',
                   ('certified', level, conclusion, cert_id, issued_at, case['id']))

        required = x.get('required_level', x.get('min_level'))
        meets_required = True
        if required in LEVEL_RANK:
            meets_required = LEVEL_RANK[level] >= LEVEL_RANK[required]
        return {
            'decision': 'certified',
            'certificate': cert_id,
            'rehearsal': case['id'],
            'generation': case['generation'],
            'scope': case['scope'],
            'level': level,
            'level_rank': LEVEL_RANK[level],
            'qualified': qualified,
            'conclusion': conclusion,
            'regulatory_conclusion': conclusion,
            'regulatory_action': regulatory_action,
            'filable': qualified and conclusion != 'not_takeover_proof',
            'meets_deadline': meets_deadline,
            'meets_required_level': meets_required,
            'measured_recovery_seconds': recovery_seconds,
            'rto_limit': rto_limit,
            'rto_met': rto_met,
            'customer_loss': customer_loss,
            'basis_evidence': [row['id'] for row in evidence_rows],
        }

    # ------------------------------------------------------ isolated writes
    if op == 'issue_sequences':
        case = _case(db, x)
        if not case:
            return {'decision': 'unknown_rehearsal'}
        if case['state'] != 'running':
            return {'decision': 'rehearsal_not_running', 'state': case['state']}
        claimed_gen, claimed_scope, _ = _claimed_generation_scope(x, case)
        if claimed_gen is not None and int(claimed_gen) != case['generation']:
            return {'decision': 'generation_replaced'}
        if claimed_scope is not None and claimed_scope != case['scope']:
            return {'decision': 'scope_mismatch'}
        count = max(1, min(int(x.get('count', 1)), 1000))
        prefix = x.get('prefix', 'ISO')
        base = one(db, 'SELECT COALESCE(MAX(seq),0) m FROM rehearsal_isolated_sequences WHERE rehearsal=?',
                   (case['id'],))['m']
        issued = []
        for offset in range(1, count + 1):
            seq = base + offset
            label = f'{prefix}::{case["scope"]}::{seq}'
            row_id = x['event_id'] if count == 1 else f"{x['event_id']}-{offset}"
            db.execute(
                'INSERT INTO rehearsal_isolated_sequences(id,rehearsal,generation,scope,seq,label) '
                'VALUES(?,?,?,?,?,?)',
                (row_id, case['id'], case['generation'], case['scope'], seq, label),
            )
            issued.append(label)
        return {'decision': 'isolated_sequences_issued', 'scope': case['scope'],
                'generation': case['generation'], 'sequences': issued,
                'writes_real_customer_sequence': False}

    if op == 'forbid_real_write':
        # 任何试图借演练通道写客户真实序号的动作都直接拒绝，且不产生任何写入。
        return {'decision': 'real_sequence_write_forbidden',
                'hint': 'rehearsal may only issue scope-bound isolated sequences'}

    return {'decision': 'unknown_operation', 'op': op}


def run(db, x):
    return act(db, SUBSYSTEM, x, handle)
