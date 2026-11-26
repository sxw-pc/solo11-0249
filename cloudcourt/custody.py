"""可追责的密钥封存、保管人法定人数与释放/恢复裁决。

本模块在最初的 ``key / vote / release`` 事件模型上扩展。为了不破坏既有
事件与测试，**未被治理的密钥**（没有保管人名册、客户许可、释放案，且
release 载荷不带任何严格字段）仍按原来的“两票”语义裁决，只是把计数改为
``COUNT(DISTINCT actor)``，使同一 actor 的重复行不再凑数。

一旦某个租户建立了保管人名册/客户许可/释放案，或 release 显式携带
``case/consent/device/attestation/mode/nonce`` 等字段，释放就必须逐项通过：

1. 负载合同与租户归属（``workload_intents``，来自 intent）；
2. 真实且当前的目标席位与主席位（``placement_leases`` /
   ``cluster_inventory`` / ``workload_intents.primary_cluster``，来自 seat）；
3. 事故未取消、客户本次用途许可有效且覆盖所请模式；
4. 至少两名**独立、合格**的保管人：别名/改名归一到同一 canonical 身份，
   已撤销资格者的旧票一律剔除；
5. 密钥代次当前仍为 sealed：轮换后旧代即 ``rotated``，旧批准失效；
6. 目标设备当前代次、黄金测量值与即时（绑定 challenge nonce、新鲜度窗口、
   且事件顺序在 challenge 之后）证明；
7. 客户可读边界：旧代可读前缀、逐笔尾部对账、重加密进度分别建表保存，
   读/写释放分别裁决，目标能启动/占席不等于历史数据可读。

明文密钥字节不得进入事件日志：:func:`run` 在进入 ``store.act`` 之前递归
抹除常见的原始密钥字段。成功释放只给出 ``sealed-package://`` 引用。

每次严格裁决（包括被拒绝的尝试）都把依据快照写入 ``release_audits``；
:func:`audit_release` 通过把该事件之前的全部事件重放到内存库来逐次复算。
"""

import json

from .store import act, one, many

SCHEMA = '''
CREATE TABLE IF NOT EXISTS key_epochs(
  id TEXT PRIMARY KEY, tenant TEXT, intent TEXT, epoch INTEGER, state TEXT,
  family TEXT, created_at INTEGER, rotated_at INTEGER, sealed_ref TEXT
);
CREATE TABLE IF NOT EXISTS custodian_votes(
  id TEXT PRIMARY KEY, key_id TEXT, actor TEXT, at INTEGER, state TEXT,
  case_id TEXT, mode TEXT
);
CREATE TABLE IF NOT EXISTS key_releases(
  id TEXT PRIMARY KEY, key_id TEXT, cluster TEXT, at INTEGER, state TEXT,
  mode TEXT, package_ref TEXT
);

-- 保管人：canonical 是自然人/独立身份的归一键，alias 覆盖改名与旧名。
CREATE TABLE IF NOT EXISTS custodians(
  id TEXT PRIMARY KEY, tenant TEXT, canonical TEXT UNIQUE, label TEXT,
  state TEXT, qualified_at INTEGER, revoked_at INTEGER, revision INTEGER
);
CREATE TABLE IF NOT EXISTS custodian_aliases(
  alias TEXT PRIMARY KEY, custodian_id TEXT NOT NULL
);

-- 客户针对一次事故/用途的许可原件；modes/terms 为 JSON。
CREATE TABLE IF NOT EXISTS customer_consents(
  id TEXT PRIMARY KEY, tenant TEXT, intent TEXT, incident TEXT,
  scope TEXT, modes TEXT, terms TEXT, state TEXT, revision INTEGER,
  issued_at INTEGER, expires_at INTEGER, revoked_at INTEGER
);

-- 目标设备当前代次（重建以更大 generation 覆盖）与测量值。
CREATE TABLE IF NOT EXISTS target_devices(
  id TEXT PRIMARY KEY, cluster TEXT, generation INTEGER,
  measurement TEXT, state TEXT, registered_at INTEGER
);
CREATE TABLE IF NOT EXISTS device_attestations(
  id TEXT PRIMARY KEY, device TEXT, generation INTEGER, measurement TEXT,
  nonce TEXT, at INTEGER, state TEXT, event_id TEXT
);

-- 一次释放申请/challenge，钉住提交时的全部证据代次。
CREATE TABLE IF NOT EXISTS release_cases(
  id TEXT PRIMARY KEY, tenant TEXT, intent TEXT, incident TEXT,
  consent TEXT, key_id TEXT, key_epoch INTEGER,
  cluster TEXT, device TEXT, device_generation INTEGER, measurement TEXT,
  nonce TEXT, modes TEXT, at INTEGER,
  attestation_ttl INTEGER, package_ttl INTEGER,
  state TEXT, event_id TEXT
);

-- 事故取消记录（custody 侧的取消原件，释放裁决必查）。
CREATE TABLE IF NOT EXISTS recovery_cancellations(
  incident TEXT PRIMARY KEY, at INTEGER, reason TEXT, event_id TEXT
);

-- 旧代/各代密文段清单（只保存引用，不保存密钥字节）。
CREATE TABLE IF NOT EXISTS cipher_segments(
  id TEXT PRIMARY KEY, intent TEXT, key_id TEXT, epoch INTEGER,
  seq INTEGER, cluster TEXT, wrapped_ref TEXT,
  UNIQUE(key_id, seq)
);
-- 客户已确认可读前缀，与段清单分开保存。
CREATE TABLE IF NOT EXISTS readable_prefixes(
  id TEXT PRIMARY KEY, intent TEXT, key_id TEXT, epoch INTEGER,
  through_seq INTEGER, at INTEGER, source TEXT
);
-- 未确认尾部写入及其逐笔对账状态，单独保存。
CREATE TABLE IF NOT EXISTS tail_writes(
  id TEXT PRIMARY KEY, intent TEXT, seq INTEGER, digest_ref TEXT,
  state TEXT, at INTEGER, reconciled_at INTEGER,
  UNIQUE(intent, seq)
);
-- 重加密作业与逐序号进度，单独保存。
CREATE TABLE IF NOT EXISTS reencryption_jobs(
  id TEXT PRIMARY KEY, intent TEXT, old_key TEXT, new_key TEXT,
  at INTEGER, state TEXT
);
CREATE TABLE IF NOT EXISTS reencryption_items(
  id TEXT PRIMARY KEY, job TEXT, old_key TEXT, new_key TEXT,
  seq INTEGER, state TEXT, at INTEGER,
  UNIQUE(new_key, seq)
);

-- 每次严格 release 裁决的可复算依据快照（按事件 id 保存，允许同一业务
-- release id 在被拒绝后用新事件重试）。
CREATE TABLE IF NOT EXISTS release_audits(
  event_id TEXT PRIMARY KEY, release_id TEXT, key_id TEXT,
  cluster TEXT, at INTEGER, verdict TEXT, basis TEXT
);
'''

REQUIRED_CUSTODIANS = 2
DEFAULT_ATTESTATION_TTL = 30

_FORBIDDEN_KEY_FIELDS = frozenset({
    'plaintext_key', 'plaintext', 'raw_key', 'key_bytes', 'raw_key_bytes',
    'plaintext_key_bytes', 'dek_bytes', 'raw_dek', 'secret_material',
    'key_material_plaintext',
})

_MODE_ALIASES = {
    'read': 'read', 'readonly': 'read', 'read_only': 'read', 'ro': 'read',
    'read-old': 'read', 'old_data_read_only': 'read',
    'read_write': 'read_write', 'readwrite': 'read_write', 'rw': 'read_write',
    'write': 'read_write', 'read-write': 'read_write',
    'read_write_after_reconcile': 'read_write', 'all': 'read_write',
}


def norm_mode(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [norm_mode(v) for v in value]
    text = str(value).strip().lower().replace('-', '_')
    text = {'read_only': 'read', 'readwrite': 'read_write', 'rw': 'read_write',
            'write': 'read_write'}.get(text, text)
    return _MODE_ALIASES.get(text, text)


def _redact(value):
    """递归抹除原始密钥字段；返回 (净化后的值, 是否发现敏感字段)。"""
    found = False
    if isinstance(value, dict):
        clean = {}
        for key, item in value.items():
            if isinstance(key, str) and key.lower() in _FORBIDDEN_KEY_FIELDS and item:
                clean[key] = '[redacted]'
                found = True
            else:
                cleaned, hit = _redact(item)
                clean[key] = cleaned
                found = found or hit
        return clean, found
    if isinstance(value, list):
        items = []
        for item in value:
            cleaned, hit = _redact(item)
            items.append(cleaned)
            found = found or hit
        return items, found
    return value, False


def run(db, x):
    safe, attempted = _redact(x)
    if attempted:
        safe['_plaintext_forbidden'] = True
    return act(db, 'custody', safe, handle)


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------

def _json_loads(value, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _dumps(value):
    # 排序键，保证在线写入与事件重放得到逐字节一致的状态行。
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _resolve_custodian_id(db, ref):
    if ref is None:
        return None
    row = one(db, 'SELECT id FROM custodians WHERE id=?', (ref,))
    if row:
        return row['id']
    row = one(db, 'SELECT custodian_id FROM custodian_aliases WHERE alias=?', (ref,))
    return row['custodian_id'] if row else None


def _event_seq(db, event_id):
    if not event_id:
        return None
    row = one(db, 'SELECT seq FROM events WHERE event_id=?', (event_id,))
    return row['seq'] if row else None


# ---------------------------------------------------------------------------
# 客户可读边界：前缀 / 尾部 / 重加密三套状态分别保存，这里只做归并复算。
# ---------------------------------------------------------------------------

def read_boundary(db, intent, key_id):
    key = one(db, 'SELECT * FROM key_epochs WHERE id=?', (key_id,))
    epoch = key['epoch'] if key else None

    prefix_row = one(
        db,
        'SELECT COALESCE(MAX(through_seq),0) AS p FROM readable_prefixes WHERE key_id=?',
        (key_id,),
    )
    prefix_through = int(prefix_row['p'] or 0)

    tails = many(
        db,
        'SELECT id, seq, state, reconciled_at FROM tail_writes '
        'WHERE intent=? AND seq>? ORDER BY seq',
        (intent, prefix_through),
    )
    readable_seq = prefix_through
    confirmed_tail, pending_tail, rejected_tail = [], [], []
    for tail in tails:
        seq = int(tail['seq'])
        if tail['state'] == 'confirmed':
            confirmed_tail.append(seq)
            if seq == readable_seq + 1:
                readable_seq = seq
        elif tail['state'] == 'rejected':
            rejected_tail.append(seq)
        else:
            pending_tail.append(seq)

    unresolved = [t['seq'] for t in tails if t['state'] == 'unconfirmed']

    max_sequence = prefix_through
    seg = one(db, 'SELECT COALESCE(MAX(seq),0) AS m FROM cipher_segments WHERE key_id=?',
              (key_id,))
    max_sequence = max(max_sequence, int(seg['m'] or 0))
    tail_max = one(db, 'SELECT COALESCE(MAX(seq),0) AS m FROM tail_writes WHERE intent=?',
                   (intent,))
    max_sequence = max(max_sequence, int(tail_max['m'] or 0))

    # 以本代为 new_key 的重加密作业决定新代密文实际可读的连续边界。
    reencryption = None
    jobs = many(db, 'SELECT * FROM reencryption_jobs WHERE new_key=? ORDER BY rowid',
                (key_id,))
    reenc_complete = True
    for job in jobs:
        items = many(db,
                     'SELECT seq, state FROM reencryption_items WHERE job=? ORDER BY seq',
                     (job['id'],))
        done = sorted(int(i['seq']) for i in items if i['state'] == 'reencrypted')
        pending = sorted(int(i['seq']) for i in items if i['state'] != 'reencrypted')
        frontier = 0
        for seq in sorted(int(i['seq']) for i in items):
            state = next(i['state'] for i in items if int(i['seq']) == seq)
            if state == 'reencrypted' and seq == frontier + 1:
                frontier = seq
            else:
                break
        complete = len(pending) == 0 and len(items) > 0
        reenc_complete = reenc_complete and complete
        readable_seq = min(readable_seq, frontier) if items else readable_seq
        reencryption = {
            'job': job['id'],
            'old_key': job['old_key'],
            'new_key': job['new_key'],
            'total': len(items),
            'done': len(done),
            'done_seqs': done,
            'pending_seqs': pending,
            'contiguous_through': frontier,
            'complete': complete,
        }

    write_ready = (
        len(unresolved) == 0
        and reenc_complete
        and readable_seq >= max_sequence
        and max_sequence > 0
    )
    return {
        'intent': intent,
        'key_id': key_id,
        'epoch': epoch,
        'prefix_through': prefix_through,
        'readable_seq': readable_seq,
        'confirmed_tail_seqs': sorted(confirmed_tail),
        'pending_tail_seqs': sorted(pending_tail),
        'rejected_tail_seqs': sorted(rejected_tail),
        'unresolved_tail_count': len(unresolved),
        'max_sequence': max_sequence,
        'reencryption': reencryption,
        'write_ready': bool(write_ready),
        'write_enabled': bool(write_ready),
        'seat_boot_only': True,
    }


# ---------------------------------------------------------------------------
# 严格释放裁决：纯读，返回 (verdict, basis)，可被审计重放反复复算。
# ---------------------------------------------------------------------------

def _add(checks, root, ok, code, detail=None):
    checks.append({'root': root, 'ok': bool(ok), 'code': code, 'detail': detail or {}})
    return ok


def _resolve_consent(db, x, key, case):
    explicit = x.get('consent') or x.get('permit') or x.get('permission') \
        or x.get('authorization') or (case['consent'] if case else None)
    if explicit:
        return one(db, 'SELECT * FROM customer_consents WHERE id=?', (explicit,))
    candidates = many(
        db,
        'SELECT * FROM customer_consents WHERE tenant=? ORDER BY rowid',
        (key['tenant'],),
    )
    incident = x.get('incident') or (case['incident'] if case else None)
    mode = norm_mode(x.get('mode')) or (
        _json_loads(case['modes'], [None])[0] if case else None)
    active = [c for c in candidates
              if c['state'] == 'active'
              and (not c['intent'] or c['intent'] == key['intent'])
              and (not incident or not c['incident'] or c['incident'] == incident)]
    if mode:
        allowed = []
        for c in active:
            modes = norm_mode(_json_loads(c['modes'], []))
            if not modes or '*' in modes or mode in modes:
                allowed.append(c)
        active = allowed
    if len(active) == 1:
        return active[0]
    return None


def evaluate_release(db, x, key):
    at = x.get('at')
    at = at if isinstance(at, int) else 0
    cluster = x.get('cluster') or x.get('target') or x.get('cluster_id')
    release_id = x.get('id') or x.get('event_id')
    case_id = x.get('case') or x.get('request')
    case = one(db, 'SELECT * FROM release_cases WHERE id=?', (case_id,)) if case_id else None
    mode = norm_mode(x.get('mode'))
    if mode is None and case:
        modes = norm_mode(_json_loads(case['modes'], []))
        mode = modes[0] if modes else 'read_write'
    if mode is None:
        mode = 'read_write'

    checks = []
    evidence = {'case_id': case['id'] if case else None}

    # 0. 释放案与载荷不能张冠李戴。
    if case_id and not case:
        _add(checks, 'case', False, 'release_case_missing', {'case_id': case_id})
    elif case and case['key_id'] != key['id']:
        _add(checks, 'case', False, 'release_case_key_mismatch',
             {'case_key': case['key_id'], 'release_key': key['id']})
    else:
        _add(checks, 'case', True, 'release_case_bound',
             {'case_id': case['id'] if case else None, 'mode': mode})

    # 1. 负载合同与租户归属（intent 端）。
    intent = one(db, 'SELECT * FROM workload_intents WHERE id=?', (key['intent'],))
    if not intent:
        _add(checks, 'intent', False, 'intent_missing', {'intent': key['intent']})
    elif intent['tenant'] != key['tenant']:
        _add(checks, 'intent', False, 'intent_tenant_mismatch',
             {'intent_tenant': intent['tenant'], 'key_tenant': key['tenant']})
    elif intent['status'] == 'revoked':
        _add(checks, 'intent', False, 'intent_revoked', {'status': intent['status']})
    else:
        _add(checks, 'intent', True, 'intent_ownership_matches',
             {'intent': intent['id'], 'tenant': intent['tenant'],
              'status': intent['status'], 'primary_cluster': intent['primary_cluster']})

    # 2. 真实目标席位 + 当前集群代次。只读包可交真实占席的备席；
    #    读写释放还必须持有主席位（primary_cluster 指向目标）。
    lease = one(
        db,
        "SELECT * FROM placement_leases WHERE intent=? AND cluster=? "
        "AND state='occupied' ORDER BY rowid LIMIT 1",
        (key['intent'], cluster),
    )
    inventory = one(db, 'SELECT * FROM cluster_inventory WHERE id=?', (cluster,))
    is_chair = bool(intent and intent['primary_cluster'] == cluster)
    seat_detail = {'cluster': cluster, 'lease_id': lease['id'] if lease else None,
                   'role': lease['role'] if lease else None,
                   'lease_generation': lease['generation'] if lease else None,
                   'inventory_generation': inventory['generation'] if inventory else None,
                   'primary_cluster': intent['primary_cluster'] if intent else None,
                   'is_chair': is_chair, 'mode': mode}
    if not lease:
        _add(checks, 'seat', False, 'target_seat_not_occupied', seat_detail)
    elif lease['expires'] is not None and lease['expires'] < at:
        _add(checks, 'seat', False, 'seat_lease_expired',
             {**seat_detail, 'expires': lease['expires'], 'at': at})
    elif not inventory or not inventory['healthy']:
        _add(checks, 'seat', False, 'seat_cluster_unhealthy', seat_detail)
    elif inventory['generation'] != lease['generation']:
        _add(checks, 'seat', False, 'stale_seat_generation', seat_detail)
    elif mode == 'read_write' and not is_chair:
        _add(checks, 'seat', False, 'target_not_chair', seat_detail)
    elif is_chair:
        _add(checks, 'seat', True, 'real_chair_occupied', seat_detail)
    else:
        _add(checks, 'seat', True, 'real_seat_occupied_read_only_package', seat_detail)

    # 3. 客户本次用途许可 + 事故取消。
    consent = _resolve_consent(db, x, key, case)
    incident = x.get('incident') or (case['incident'] if case else None) \
        or (consent['incident'] if consent else None)
    cancellation = one(db, 'SELECT * FROM recovery_cancellations WHERE incident=?',
                       (incident,)) if incident else None
    evidence['incident'] = incident
    evidence['consent'] = consent['id'] if consent else None

    if cancellation:
        _add(checks, 'incident', False, 'incident_cancelled',
             {'incident': incident, 'at': cancellation['at'],
              'reason': cancellation['reason']})
    else:
        _add(checks, 'incident', True, 'incident_active', {'incident': incident})

    if not consent:
        _add(checks, 'consent', False, 'customer_authorization_required',
             {'tenant': key['tenant'], 'mode': mode})
    elif consent['state'] == 'revoked' or consent['revoked_at'] is not None:
        _add(checks, 'consent', False, 'consent_revoked',
             {'consent': consent['id'], 'revoked_at': consent['revoked_at']})
    elif consent['state'] == 'cancelled':
        _add(checks, 'consent', False, 'consent_cancelled', {'consent': consent['id']})
    elif consent['expires_at'] is not None and consent['expires_at'] < at:
        _add(checks, 'consent', False, 'consent_expired',
             {'consent': consent['id'], 'expires_at': consent['expires_at'], 'at': at})
    else:
        allowed_modes = norm_mode(_json_loads(consent['modes'], []))
        if allowed_modes and '*' not in allowed_modes and mode not in allowed_modes:
            _add(checks, 'consent', False, 'consent_scope_denied',
                 {'consent': consent['id'], 'mode': mode, 'allowed_modes': allowed_modes})
        else:
            _add(checks, 'consent', True, 'consent_covers_this_use',
                 {'consent': consent['id'], 'mode': mode,
                  'revision': consent['revision'], 'issued_at': consent['issued_at']})

    # 4. 两名独立合格保管人；改名/重复投票归一，撤销后的旧票剔除。
    votes = many(
        db,
        "SELECT * FROM custodian_votes WHERE key_id=? AND state='accepted' ORDER BY rowid",
        (key['id'],),
    )
    vote_details = []
    independent = set()
    for vote in votes:
        custodian_id = _resolve_custodian_id(db, vote['actor'])
        custodian = one(db, 'SELECT * FROM custodians WHERE id=?',
                        (custodian_id,)) if custodian_id else None
        if case and vote['case_id'] and vote['case_id'] != case['id']:
            counts, reason = False, 'bound_to_other_case'
        elif not custodian:
            counts, reason = False, 'unknown_identity'
        elif custodian['tenant'] not in (key['tenant'], '*'):
            counts, reason = False, 'tenant_mismatch'
        elif custodian['state'] != 'qualified' or custodian['revoked_at'] is not None:
            counts, reason = False, 'qualification_revoked'
        else:
            counts, reason = True, 'qualified'
        if counts:
            independent.add(custodian['id'])
        vote_details.append({
            'vote_id': vote['id'], 'actor': vote['actor'], 'at': vote['at'],
            'custodian_id': custodian['id'] if custodian else None,
            'canonical': custodian['canonical'] if custodian else None,
            'counts': counts, 'reason': reason,
        })
    quorum_ok = len(independent) >= REQUIRED_CUSTODIANS
    _add(checks, 'custodians', quorum_ok,
         'custodian_quorum_met' if quorum_ok else 'custodian_quorum_not_met',
         {'required': REQUIRED_CUSTODIANS,
          'independent_qualified': len(independent),
          'independent_ids': sorted(independent),
          'votes': vote_details})

    # 5. 密钥代次当前状态。
    epoch_ok = key['state'] == 'sealed'
    if key['state'] == 'rotated':
        epoch_code = 'key_epoch_superseded'
    elif not epoch_ok:
        epoch_code = 'key_unavailable'
    else:
        epoch_code = 'key_epoch_current'
    pin_epoch = x.get('epoch')
    if pin_epoch is None and case:
        pin_epoch = case['key_epoch']
    epoch_match = pin_epoch is None or pin_epoch == key['epoch']
    if not epoch_match:
        _add(checks, 'key_epoch', False, 'key_epoch_mismatch',
             {'key_id': key['id'], 'epoch': key['epoch'], 'pinned_epoch': pin_epoch,
              'state': key['state']})
    else:
        _add(checks, 'key_epoch', epoch_ok, epoch_code,
             {'key_id': key['id'], 'epoch': key['epoch'], 'state': key['state'],
              'family': key['family']})

    # 6. 目标设备与即时测量证明。
    attestation_id = x.get('attestation') or x.get('attestation_id') \
        or x.get('receipt')
    attestation = one(db, 'SELECT * FROM device_attestations WHERE id=?',
                      (attestation_id,)) if attestation_id else None

    device_id = x.get('device') or x.get('device_id') \
        or (case['device'] if case else None)
    if not device_id and attestation:
        device_id = attestation['device']
    device = one(db, 'SELECT * FROM target_devices WHERE id=?', (device_id,)) \
        if device_id else None
    if not device and cluster:
        # 未显式指定时，若目标集群恰好只有一台登记设备，则自动选用。
        candidates = many(db, 'SELECT * FROM target_devices WHERE cluster=?',
                          (cluster,))
        if len(candidates) == 1:
            device = candidates[0]
            device_id = device['id']
    if not device:
        _add(checks, 'device', False, 'target_device_missing', {'device': device_id})
    elif device['cluster'] != cluster:
        _add(checks, 'device', False, 'device_not_on_target',
             {'device': device_id, 'device_cluster': device['cluster'],
              'release_cluster': cluster})
    elif case and case['device_generation'] != device['generation']:
        _add(checks, 'device', False, 'device_rebuilt',
             {'device': device_id, 'pinned_generation': case['device_generation'],
              'current_generation': device['generation']})
    else:
        _add(checks, 'device', True, 'device_generation_current',
             {'device': device_id, 'generation': device['generation'],
              'measurement': device['measurement']})

    expected_nonce = x.get('nonce') or (case['nonce'] if case else None)
    if not attestation and device:
        query = ("SELECT * FROM device_attestations WHERE device=? AND state='verified'")
        args = [device['id']]
        if expected_nonce is not None:
            query += ' AND nonce=?'
            args.append(expected_nonce)
        query += ' ORDER BY at DESC, rowid DESC LIMIT 1'
        attestation = one(db, query, tuple(args))

    ttl = x.get('attestation_ttl')
    if ttl is None and case:
        ttl = case['attestation_ttl']
    terms = _json_loads(consent['terms'], {}) if consent else {}
    if ttl is None:
        ttl = terms.get('attestation_ttl', DEFAULT_ATTESTATION_TTL)

    if not attestation:
        _add(checks, 'attestation', False, 'attestation_missing',
             {'device': device_id, 'nonce': expected_nonce})
    elif attestation['state'] != 'verified':
        _add(checks, 'attestation', False, 'attestation_not_verified',
             {'attestation': attestation['id'], 'state': attestation['state']})
    elif device and attestation['device'] != device['id']:
        _add(checks, 'attestation', False, 'attestation_device_mismatch',
             {'attestation_device': attestation['device'], 'device': device['id']})
    elif device and attestation['generation'] != device['generation']:
        _add(checks, 'attestation', False, 'stale_attestation_generation',
             {'attestation_generation': attestation['generation'],
              'device_generation': device['generation']})
    elif device and attestation['measurement'] != device['measurement']:
        _add(checks, 'attestation', False, 'measurement_mismatch',
             {'attestation_measurement': attestation['measurement'],
              'expected_measurement': device['measurement']})
    elif expected_nonce is not None and attestation['nonce'] != expected_nonce:
        _add(checks, 'attestation', False, 'attestation_nonce_mismatch',
             {'expected_nonce': expected_nonce, 'nonce': attestation['nonce']})
    else:
        att_seq = _event_seq(db, attestation['event_id'])
        case_seq = _event_seq(db, case['event_id']) if case else None
        age = at - attestation['at']
        if case and case_seq is not None and att_seq is not None and att_seq <= case_seq:
            _add(checks, 'attestation', False, 'attestation_before_challenge',
                 {'attestation_event_recorded_after_challenge': False})
        elif case and attestation['at'] < case['at']:
            _add(checks, 'attestation', False, 'attestation_backdated',
                 {'attested_at': attestation['at'], 'challenge_at': case['at']})
        elif age < 0:
            _add(checks, 'attestation', False, 'attestation_from_future',
                 {'attested_at': attestation['at'], 'release_at': at})
        elif age > ttl:
            _add(checks, 'attestation', False, 'attestation_stale',
                 {'attested_at': attestation['at'], 'release_at': at,
                  'age': age, 'ttl': ttl})
        else:
            _add(checks, 'attestation', True, 'fresh_measured_attestation',
                 {'attestation': attestation['id'],
                  'generation': attestation['generation'],
                  'measurement': attestation['measurement'],
                  'nonce': attestation['nonce'], 'attested_at': attestation['at'],
                  'age': age, 'ttl': ttl,
                  'recorded_after_challenge': True})

    # 7. 客户可读边界（前缀 / 尾部 / 重加密分别建表）。
    boundary = read_boundary(db, key['intent'], key['id'])
    if mode == 'read':
        boundary_ok = boundary['readable_seq'] >= 1
        boundary_code = 'readable_boundary_ready' if boundary_ok \
            else 'readable_prefix_missing'
    else:
        if boundary['unresolved_tail_count']:
            boundary_code = 'tail_not_reconciled'
            boundary_ok = False
        elif boundary['reencryption'] and not boundary['reencryption']['complete']:
            boundary_code = 'reencryption_incomplete'
            boundary_ok = False
        elif not boundary['write_ready']:
            boundary_code = 'write_boundary_not_ready'
            boundary_ok = False
        else:
            boundary_code = 'read_write_boundary_ready'
            boundary_ok = True
    _add(checks, 'read_boundary', boundary_ok, boundary_code, boundary)

    failed = next((c['code'] for c in checks if not c['ok']), None)
    verdict = failed or 'key_released'

    package_ttl = x.get('package_ttl')
    if package_ttl is None and case:
        package_ttl = case['package_ttl']
    if package_ttl is None and consent:
        package_ttl = terms.get('package_ttl') or terms.get('max_read_ttl')
    package = {
        'ref': f'sealed-package://{release_id}',
        'format': 'wrapped_data_key_reference',
        'plaintext_key_included': False,
        'key_id': key['id'],
        'epoch': key['epoch'],
        'mode': mode,
        'issued_at': at,
        'expires_at': at + package_ttl if isinstance(package_ttl, int) else None,
        'readable_seq': boundary['readable_seq'],
        'write_enabled': bool(mode == 'read_write' and boundary['write_ready']
                              and verdict == 'key_released'),
    }

    basis = {
        'release_event_id': x.get('event_id'),
        'release_id': release_id,
        'at': at,
        'tenant': key['tenant'],
        'intent': key['intent'],
        'cluster': cluster,
        'mode': mode,
        'incident': incident,
        'evidence': evidence,
        'checks': checks,
        'read_boundary': boundary,
        'package': package,
        'verdict': verdict,
    }
    return verdict, basis


def _governed(db, x, key):
    if any(k in x for k in ('case', 'request', 'consent', 'permit', 'permission',
                            'authorization', 'device', 'attestation', 'nonce',
                            'mode', 'modes')):
        return True
    if one(db, "SELECT 1 FROM custodians WHERE tenant=? OR tenant='*' LIMIT 1",
           (key['tenant'],)):
        return True
    if one(db, 'SELECT 1 FROM customer_consents WHERE tenant=? LIMIT 1',
           (key['tenant'],)):
        return True
    if one(db, 'SELECT 1 FROM release_cases WHERE key_id=? LIMIT 1', (key['id'],)):
        return True
    return False


# ---------------------------------------------------------------------------
# 事件处理器
# ---------------------------------------------------------------------------

def handle(db, x):
    if x.get('_plaintext_forbidden'):
        return {'decision': 'plaintext_key_forbidden'}

    op = x['op']

    # 裸 revoke 按载荷/名册消歧为撤销保管人资格或撤销客户许可。
    if op == 'revoke':
        ref = x.get('id')
        if x.get('consent') or x.get('permission') or x.get('authorization') \
                or (ref and one(db, 'SELECT 1 FROM customer_consents WHERE id=?', (ref,))):
            op = 'revoke_consent'
        elif x.get('custodian') or x.get('actor') or x.get('identity') \
                or (ref and _resolve_custodian_id(db, ref)):
            op = 'revoke_custodian'
        else:
            return {'decision': 'revoke_target_missing'}

    # ---- 密钥封存 / 轮换 -------------------------------------------------
    if op == 'key':
        db.execute(
            'INSERT INTO key_epochs(id,tenant,intent,epoch,state,family,'
            'created_at,rotated_at,sealed_ref) VALUES(?,?,?,?,?,?,?,?,?)',
            (x['id'], x['tenant'], x['intent'], x['epoch'], 'sealed',
             x.get('family') or x['id'], x.get('at'), None, x.get('sealed_ref')),
        )
        return {'decision': 'key_sealed'}

    if op in ('rotate', 'rotate_key', 'key_rotate', 'rotate_epoch'):
        old = one(db, 'SELECT * FROM key_epochs WHERE id=?', (x.get('key'),))
        if not old:
            return {'decision': 'key_unavailable'}
        if old['state'] != 'sealed':
            return {'decision': 'key_unavailable'}
        new_id = (x.get('new_key') or x.get('new_key_id')
                  or x.get('target_key') or x.get('id'))
        if not new_id:
            return {'decision': 'rotation_target_required'}
        new_epoch = x.get('epoch') or x.get('new_epoch') or old['epoch'] + 1
        at = x.get('at')
        db.execute(
            'INSERT INTO key_epochs(id,tenant,intent,epoch,state,family,'
            'created_at,rotated_at,sealed_ref) VALUES(?,?,?,?,?,?,?,?,?)',
            (new_id, old['tenant'], old['intent'], new_epoch, 'sealed',
             old['family'], at, None, x.get('sealed_ref')),
        )
        db.execute("UPDATE key_epochs SET state='rotated', rotated_at=? WHERE id=?",
                   (at, old['id']))
        return {'decision': 'key_rotated', 'old_key': old['id'],
                'new_key': new_id, 'epoch': new_epoch}

    # ---- 保管人资格 / 别名 / 撤销 ----------------------------------------
    if op in ('custodian', 'register_custodian', 'qualify_custodian',
              'add_custodian', 'add_qualified_custodian'):
        cid = x.get('id') or x.get('custodian') or x.get('actor')
        if not cid:
            return {'decision': 'custodian_id_required'}
        if one(db, 'SELECT 1 FROM custodians WHERE id=?', (cid,)):
            return {'decision': 'custodian_already_registered', 'custodian': cid}
        canonical = x.get('canonical') or x.get('identity') or f'canonical:{cid}'
        if one(db, 'SELECT 1 FROM custodians WHERE canonical=? AND id<>?',
               (canonical, cid)):
            return {'decision': 'custodian_identity_conflict', 'canonical': canonical}
        at = x.get('at')
        db.execute('INSERT INTO custodians VALUES(?,?,?,?,?,?,?,?)',
                   (cid, x.get('tenant', '*'), canonical, x.get('label') or x.get('name'),
                    'qualified', x.get('qualified_at', at), None,
                    x.get('revision', 1)))
        aliases = list(x.get('aliases') or [])
        aliases.append(cid)
        if x.get('actor'):
            aliases.append(x['actor'])
        for alias in set(aliases):
            owner = one(db, 'SELECT custodian_id FROM custodian_aliases WHERE alias=?',
                        (alias,))
            if owner and owner['custodian_id'] != cid:
                return {'decision': 'alias_conflict', 'alias': alias}
            db.execute('INSERT OR IGNORE INTO custodian_aliases(alias,custodian_id) '
                       'VALUES(?,?)', (alias, cid))
        return {'decision': 'custodian_qualified', 'custodian': cid,
                'canonical': canonical}

    if op in ('custodian_alias', 'alias_custodian', 'rename_custodian', 'add_alias'):
        cid = _resolve_custodian_id(db, x.get('custodian') or x.get('id'))
        if not cid:
            return {'decision': 'custodian_missing'}
        alias = x.get('alias') or x.get('new_actor')
        owner = one(db, 'SELECT custodian_id FROM custodian_aliases WHERE alias=?',
                    (alias,))
        if owner and owner['custodian_id'] != cid:
            return {'decision': 'alias_conflict', 'alias': alias}
        db.execute('INSERT OR IGNORE INTO custodian_aliases(alias,custodian_id) '
                   'VALUES(?,?)', (alias, cid))
        return {'decision': 'custodian_alias_recorded', 'custodian': cid, 'alias': alias}

    if op in ('revoke_custodian', 'custodian_revoke', 'custodian_revocation'):
        cid = _resolve_custodian_id(db, x.get('custodian') or x.get('id')
                                    or x.get('actor'))
        if not cid:
            return {'decision': 'custodian_missing'}
        db.execute("UPDATE custodians SET state='revoked', revoked_at=? WHERE id=?",
                   (x.get('at'), cid))
        return {'decision': 'custodian_revoked', 'custodian': cid, 'at': x.get('at')}

    # ---- 客户许可与撤销 ---------------------------------------------------
    if op in ('consent', 'customer_consent', 'permit', 'permission',
              'authorize', 'grant_permission', 'authorize_release',
              'grant_consent', 'customer_authorization'):
        cid = x['id']
        modes = norm_mode(x.get('modes') or x.get('allowed_modes')
                          or ([x['mode']] if x.get('mode') else ['*']))
        db.execute('INSERT INTO customer_consents VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                   (cid, x['tenant'], x.get('intent'), x.get('incident'),
                    x.get('scope', 'incident_recovery'),
                    _dumps(modes),
                    _dumps(x.get('terms') or {}),
                    x.get('state', 'active'), x.get('revision', 1),
                    x.get('issued_at', x.get('at')), x.get('expires_at'), None))
        return {'decision': 'consent_recorded', 'consent': cid, 'modes': modes}

    if op in ('revoke_consent', 'consent_revoke', 'revoke_permission',
              'revoke_authorization', 'permission_revoke'):
        cid = x.get('consent') or x.get('permission') or x.get('id')
        row = one(db, 'SELECT * FROM customer_consents WHERE id=?', (cid,))
        if not row:
            return {'decision': 'consent_missing'}
        db.execute("UPDATE customer_consents SET state='revoked', revoked_at=? WHERE id=?",
                   (x.get('at'), cid))
        return {'decision': 'consent_revoked', 'consent': cid, 'at': x.get('at')}

    # ---- 事故取消 ---------------------------------------------------------
    if op in ('cancel_incident', 'incident_cancel', 'cancel_recovery', 'cancel'):
        incident = x.get('incident') or x.get('id')
        if not incident:
            return {'decision': 'incident_required'}
        if one(db, 'SELECT 1 FROM recovery_cancellations WHERE incident=?', (incident,)):
            return {'decision': 'incident_already_cancelled', 'incident': incident}
        db.execute('INSERT INTO recovery_cancellations VALUES(?,?,?,?)',
                   (incident, x.get('at'), x.get('reason'), x['event_id']))
        db.execute("UPDATE customer_consents SET state='cancelled' "
                   "WHERE incident=? AND state='active'", (incident,))
        db.execute("UPDATE release_cases SET state='cancelled' "
                   "WHERE incident=? AND state='open'", (incident,))
        return {'decision': 'incident_cancelled', 'incident': incident}

    # ---- 设备登记/重建与测量证明 ------------------------------------------
    if op in ('device', 'register_device', 'observe_device', 'upsert_device',
              'register_target_device'):
        did = x['id']
        old = one(db, 'SELECT * FROM target_devices WHERE id=?', (did,))
        generation = int(x['generation'])
        if old and generation <= old['generation']:
            return {'decision': 'stale_device_registration',
                    'current_generation': old['generation']}
        db.execute(
            'INSERT INTO target_devices(id,cluster,generation,measurement,state,'
            'registered_at) VALUES(?,?,?,?,?,?) '
            'ON CONFLICT(id) DO UPDATE SET cluster=excluded.cluster,'
            'generation=excluded.generation,measurement=excluded.measurement,'
            'state=excluded.state,registered_at=excluded.registered_at',
            (did, x['cluster'], generation, x['measurement'], 'active', x.get('at')))
        code = 'device_rebuilt' if old else 'device_registered'
        return {'decision': code, 'device': did, 'generation': generation}

    if op in ('rebuild_device', 'device_rebuild'):
        did = x.get('device') or x.get('id')
        old = one(db, 'SELECT * FROM target_devices WHERE id=?', (did,))
        if not old:
            return {'decision': 'target_device_missing'}
        generation = x.get('generation') or old['generation'] + 1
        measurement = x.get('measurement') or old['measurement']
        db.execute(
            'UPDATE target_devices SET generation=?, measurement=?, state=?, '
            'registered_at=?, cluster=? WHERE id=?',
            (generation, measurement, 'active', x.get('at'),
             x.get('cluster', old['cluster']), did))
        return {'decision': 'device_rebuilt', 'device': did, 'generation': generation}

    if op in ('attest', 'device_attest', 'attestation', 'submit_attestation',
              'verify_attestation', 'device_proof', 'prove_device',
              'submit_measurement'):
        did = x.get('device')
        device = one(db, 'SELECT * FROM target_devices WHERE id=?', (did,))
        if not device:
            return {'decision': 'target_device_missing'}
        if int(x['generation']) != device['generation']:
            state, decision = 'rejected', 'stale_device_generation'
        elif x['measurement'] != device['measurement']:
            state, decision = 'rejected', 'measurement_mismatch'
        else:
            state, decision = 'verified', 'attestation_verified'
        db.execute('INSERT INTO device_attestations VALUES(?,?,?,?,?,?,?,?)',
                   (x['id'], did, x['generation'], x['measurement'], x.get('nonce'),
                    x.get('at'), state, x['event_id']))
        return {'decision': decision, 'attestation': x['id'], 'state': state}

    # ---- 释放案 / challenge ----------------------------------------------
    if op in ('release_case', 'request_release', 'open_release', 'release_request',
              'case', 'approval_packet', 'open_case', 'mandate'):
        kid = x.get('key') or x.get('key_id')
        key = one(db, 'SELECT * FROM key_epochs WHERE id=?', (kid,))
        if not key:
            return {'decision': 'key_unavailable'}
        device = one(db, 'SELECT * FROM target_devices WHERE id=?', (x.get('device'),))
        if not device:
            return {'decision': 'target_device_missing'}
        modes = norm_mode(x.get('modes') or ([x['mode']] if x.get('mode') else ['read']))
        nonce = x.get('nonce') or f'challenge:{x["id"]}'
        db.execute('INSERT INTO release_cases VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], key['tenant'], key['intent'], x.get('incident'),
                    x.get('consent'), kid, key['epoch'], x.get('cluster'),
                    device['id'], device['generation'], device['measurement'],
                    nonce, _dumps(modes), x.get('at'),
                    x.get('attestation_ttl'), x.get('package_ttl'),
                    x.get('state', 'open'), x['event_id']))
        return {'decision': 'release_case_opened', 'case': x['id'], 'nonce': nonce,
                'modes': modes}

    # ---- 投票 / 撤票（保留原 vote 行为） ----------------------------------
    if op == 'vote':
        key = one(db, 'SELECT * FROM key_epochs WHERE id=?', (x['key'],))
        if not key or key['state'] != 'sealed':
            return {'decision': 'key_unavailable'}
        db.execute(
            'INSERT INTO custodian_votes(id,key_id,actor,at,state,case_id,mode) '
            "VALUES(?,?,?,?,'accepted',?,?)",
            (x['id'], x['key'], x['actor'], x['at'], x.get('case'), x.get('mode')))
        return {'decision': 'vote_recorded'}

    if op in ('withdraw_vote', 'vote_withdraw', 'withdraw'):
        vote_id = x.get('vote') or x.get('vote_id') or x.get('id')
        vote = one(db, 'SELECT * FROM custodian_votes WHERE id=?', (vote_id,))
        if not vote:
            return {'decision': 'vote_missing'}
        if vote['state'] != 'withdrawn':
            db.execute("UPDATE custodian_votes SET state='withdrawn' WHERE id=?",
                       (vote_id,))
        return {'decision': 'vote_withdrawn', 'vote': vote_id}

    # ---- 密文段 / 可读前缀 / 尾部写入与对账 -------------------------------
    if op in ('segment', 'cipher_segment', 'register_segment', 'segments'):
        kid = x.get('key') or x.get('key_id')
        key = one(db, 'SELECT * FROM key_epochs WHERE id=?', (kid,))
        if not key:
            return {'decision': 'key_unavailable'}
        items = x.get('items')
        if items is None:
            seqs = x.get('seqs')
            if seqs is not None:
                items = [{'seq': seq} for seq in seqs]
            else:
                items = [x]
        count = 0
        for index, item in enumerate(items):
            seq = int(item['seq'])
            sid = item.get('id') or f'seg:{kid}:{seq}'
            db.execute(
                'INSERT OR IGNORE INTO cipher_segments VALUES(?,?,?,?,?,?,?)',
                (sid, key['intent'], kid, key['epoch'], seq,
                 item.get('cluster', x.get('cluster')),
                 item.get('wrapped_ref', f'wrapped://{kid}/{seq}')))
            count += 1
        return {'decision': 'segments_recorded', 'key': kid, 'count': count}

    if op in ('prefix', 'readable_prefix', 'confirm_prefix', 'record_prefix',
              'read_prefix', 'readable_frontier'):
        kid = x.get('key') or x.get('key_id')
        key = one(db, 'SELECT * FROM key_epochs WHERE id=?', (kid,))
        if not key:
            return {'decision': 'key_unavailable'}
        through = int(x['through_seq'])
        covered = one(
            db,
            'SELECT COUNT(DISTINCT seq) AS n FROM cipher_segments '
            'WHERE key_id=? AND seq BETWEEN 1 AND ?',
            (kid, through))
        if through > 0 and covered['n'] < through:
            return {'decision': 'prefix_beyond_copy',
                    'through_seq': through, 'covered_seq_count': covered['n']}
        db.execute('INSERT INTO readable_prefixes VALUES(?,?,?,?,?,?,?)',
                   (x['id'], key['intent'], kid, key['epoch'], through,
                    x.get('at'), x.get('source', 'customer_committed')))
        return {'decision': 'prefix_recorded', 'key': kid, 'through_seq': through}

    if op in ('tail_write', 'record_tail', 'tail', 'tail_append', 'append_tail'):
        intent = x.get('intent')
        if not intent:
            key = one(db, 'SELECT * FROM key_epochs WHERE id=?', (x.get('key'),))
            intent = key['intent'] if key else None
        if not intent:
            return {'decision': 'intent_missing'}
        db.execute('INSERT INTO tail_writes VALUES(?,?,?,?,?,?,?)',
                   (x['id'], intent, int(x['seq']), x.get('digest_ref'),
                    'unconfirmed', x.get('at'), None))
        return {'decision': 'tail_recorded', 'tail': x['id'], 'seq': x['seq']}

    if op in ('reconcile_tail', 'reconcile', 'tail_reconcile', 'confirm_tail'):
        ref = x.get('tail') or x.get('tail_id') or x.get('id')
        tail = one(db, 'SELECT * FROM tail_writes WHERE id=?', (ref,))
        if not tail and x.get('seq') is not None:
            tail = one(db, 'SELECT * FROM tail_writes WHERE intent=? AND seq=?',
                       (x.get('intent'), x['seq']))
        if not tail:
            return {'decision': 'tail_missing'}
        if 'confirm' in x:
            state = 'confirmed' if x['confirm'] else 'rejected'
        else:
            state = x.get('outcome') or x.get('state') or 'confirmed'
        if state not in ('confirmed', 'rejected'):
            return {'decision': 'invalid_tail_state'}
        db.execute('UPDATE tail_writes SET state=?, reconciled_at=? WHERE id=?',
                   (state, x.get('at'), tail['id']))
        return {'decision': 'tail_reconciled', 'tail': tail['id'], 'state': state}

    # ---- 重加密进度 -------------------------------------------------------
    if op in ('reencrypt_start', 'start_reencryption'):
        old_key = x.get('old_key') or x.get('from_key')
        new_key = x.get('new_key') or x.get('to_key')
        old = one(db, 'SELECT * FROM key_epochs WHERE id=?', (old_key,))
        new = one(db, 'SELECT * FROM key_epochs WHERE id=?', (new_key,))
        if not old or not new:
            return {'decision': 'key_unavailable'}
        seqs = x.get('seqs')
        if seqs is None:
            seqs = [r['seq'] for r in many(
                db, 'SELECT seq FROM cipher_segments WHERE key_id=? ORDER BY seq',
                (old_key,))]
        db.execute('INSERT INTO reencryption_jobs VALUES(?,?,?,?,?,?)',
                   (x['id'], old['intent'], old_key, new_key, x.get('at'), 'running'))
        for seq in seqs:
            db.execute('INSERT OR IGNORE INTO reencryption_items VALUES(?,?,?,?,?,?,?)',
                       (f'item:{x["id"]}:{seq}', x['id'], old_key, new_key,
                        int(seq), 'pending', x.get('at')))
        return {'decision': 'reencryption_started', 'job': x['id'], 'total': len(seqs)}

    if op in ('reencrypt_batch', 'reencrypt_progress', 'reencrypt'):
        job_id = x.get('job')
        job = one(db, 'SELECT * FROM reencryption_jobs WHERE id=?', (job_id,))
        if not job:
            return {'decision': 'reencryption_job_missing'}
        seqs = x.get('seqs') or []
        at = x.get('at')
        for seq in seqs:
            db.execute("UPDATE reencryption_items SET state='reencrypted', at=? "
                       'WHERE job=? AND seq=?', (at, job_id, int(seq)))
            seq = int(seq)
            # 重加密完成的序号同时形成新代密文段（只写 wrapped 引用）。
            new_key = one(db, 'SELECT * FROM key_epochs WHERE id=?', (job['new_key'],))
            db.execute('INSERT OR IGNORE INTO cipher_segments VALUES(?,?,?,?,?,?,?)',
                       (f'seg:{job["new_key"]}:{seq}', job['intent'], job['new_key'],
                        new_key['epoch'] if new_key else None, seq,
                        x.get('cluster'), f'reencrypted://{job_id}/{seq}'))
        progress = one(
            db,
            "SELECT SUM(state='reencrypted') AS done, COUNT(*) AS total "
            'FROM reencryption_items WHERE job=?', (job_id,))
        done, total = int(progress['done'] or 0), int(progress['total'] or 0)
        complete = done == total and total > 0
        if complete:
            db.execute("UPDATE reencryption_jobs SET state='complete' WHERE id=?",
                       (job_id,))
        return {'decision': 'reencryption_recorded', 'job': job_id,
                'done': done, 'total': total, 'complete': complete}

    # ---- 释放（兼容旧语义 + 严格治理） ------------------------------------
    if op == 'release':
        key_ref = x.get('key') or x.get('key_id')
        release_business_id = x.get('id') or x['event_id']
        release_at = x.get('at') if isinstance(x.get('at'), int) else None
        key = one(db, 'SELECT * FROM key_epochs WHERE id=?', (key_ref,))
        if not key:
            return {'decision': 'approval_incomplete'}

        if not _governed(db, x, key):
            votes = one(
                db,
                "SELECT COUNT(DISTINCT actor) AS n FROM custodian_votes "
                "WHERE key_id=? AND state='accepted'",
                (key_ref,))
            if votes['n'] < 2:
                return {'decision': 'approval_incomplete'}
            db.execute('INSERT INTO key_releases(id,key_id,cluster,at,state,'
                       "mode,package_ref) VALUES(?,?,?,?, 'released', ?, ?)",
                       (release_business_id, key_ref,
                        x.get('cluster') or x.get('target'), release_at, None, None))
            return {'decision': 'key_released'}

        verdict, basis = evaluate_release(db, x, key)
        db.execute('INSERT INTO release_audits(event_id,release_id,key_id,cluster,'
                   'at,verdict,basis) VALUES(?,?,?,?,?,?,?)',
                   (x['event_id'], release_business_id, key_ref,
                    x.get('cluster') or x.get('target'), release_at,
                    verdict, _dumps(basis)))
        if verdict != 'key_released':
            return {'decision': verdict, 'basis': basis}

        package = basis['package']
        db.execute('INSERT INTO key_releases(id,key_id,cluster,at,state,mode,'
                   "package_ref) VALUES(?,?,?,?, 'released', ?, ?)",
                   (release_business_id, key_ref,
                    x.get('cluster') or x.get('target'), release_at,
                    basis['mode'], package['ref']))
        return {'decision': 'key_released', 'basis': basis}

    return {'decision': 'unknown_operation'}


# ---------------------------------------------------------------------------
# 审计：把某释放事件之前的全部事件重放到内存库，重新裁决并比对依据快照。
# ---------------------------------------------------------------------------

def audit_release(db, event_id):
    event = one(db, 'SELECT * FROM events WHERE event_id=?', (event_id,))
    if not event:
        return {'event_id': event_id, 'found': False, 'matches': False}

    payload = json.loads(event['payload'])
    stored_result = json.loads(event['result'])
    seq = event['seq']

    sim = connect_memory()
    try:
        from . import MODULES
        history = many(db,
                       'SELECT subsystem,payload FROM events WHERE seq<? ORDER BY seq',
                       (seq,))
        for row in history:
            MODULES[row['subsystem']].run(sim, json.loads(row['payload']))

        key_ref = payload.get('key') or payload.get('key_id')
        key = one(sim, 'SELECT * FROM key_epochs WHERE id=?', (key_ref,))
        recomputed_basis = None
        governed = False
        if key is None:
            recomputed_decision = 'approval_incomplete'
        else:
            governed = _governed(sim, payload, key)
            if governed:
                recomputed_decision, recomputed_basis = evaluate_release(sim, payload, key)
            else:
                count = one(
                    sim,
                    "SELECT COUNT(DISTINCT actor) AS n FROM custodian_votes "
                    "WHERE key_id=? AND state='accepted'", (key_ref,))
                recomputed_decision = 'key_released' if count['n'] >= 2 \
                    else 'approval_incomplete'

        audit_row = one(db, 'SELECT * FROM release_audits WHERE event_id=?', (event_id,))
        stored_basis = _json_loads(audit_row['basis'], None) if audit_row else None

        matches = recomputed_decision == stored_result.get('decision')
        if governed:
            matches = matches and recomputed_basis == stored_basis
        else:
            matches = matches and audit_row is None
        return {
            'event_id': event_id,
            'seq': seq,
            'found': True,
            'governed': governed,
            'matches': bool(matches),
            'stored_decision': stored_result.get('decision'),
            'recomputed_decision': recomputed_decision,
            'stored_basis': stored_basis,
            'recomputed_basis': recomputed_basis,
        }
    finally:
        sim.close()


def connect_memory():
    from .store import connect
    return connect(':memory:')


def export_audit(db):
    rows = many(db, 'SELECT event_id FROM release_audits ORDER BY rowid')
    releases = [audit_release(db, row['event_id']) for row in rows]
    return {
        'release_audits': releases,
        'all_match': all(item['matches'] for item in releases),
        'key_releases': many(db, 'SELECT * FROM key_releases ORDER BY rowid'),
    }
