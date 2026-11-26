"""地域收窄过渡期的合同、席位与接管一致性。

既有 intent/seat/evac 三个入口的断点是：新规则只覆盖 ``residency_rules`` 一行，
已存在的 ``workload_intents.region`` 与备席不会重新受审；规则没有生效时刻、
数据类别、处理目的和代次；接管链从不回看法域。本模块用追加式（不可改写）的
规则代次和义务账本修复这些问题：

* 规则注明生效时刻、数据类别、获准处理目的、签署代次与适用的既有负载范围，
  旧规则及在其有效期内合法的处理事实原样保留、始终可审计。
* 生效后新写入、缓存、快照、事故恢复点必须落在有效地域，校验的是实物数据路径
  （集群/工件的实际 region），而不是任何主站标签。
* 已占席但不合规的负载保留真实所在位置与迁出义务，合规状态由当前规则即时推导，
  绝不静默写成 compliant。
* 备席的预留、占用、接管分别核对「已签署且与承诺时间窗相交的全部规则」与实物
  数据路径；目标换代、许可撤回、迟到回执都不能让越界接管生效。
* 迁出未完成时旧席只能封存或保留，不得直接释放。
"""
import json
from .store import act, one, many

SCHEMA = '''
CREATE TABLE IF NOT EXISTS rt_rule_versions(
  tenant TEXT, revision INTEGER, regions TEXT, effective_at INTEGER,
  data_categories TEXT, purposes TEXT, retention_regions TEXT,
  workload_scope TEXT, signed_at INTEGER,
  PRIMARY KEY(tenant,revision));
CREATE TABLE IF NOT EXISTS rt_workloads(
  id TEXT PRIMARY KEY, tenant TEXT, contract_region TEXT, role TEXT,
  status TEXT, revision INTEGER, primary_cluster TEXT, write_state TEXT,
  created_at INTEGER);
CREATE TABLE IF NOT EXISTS rt_clusters(
  id TEXT PRIMARY KEY, region TEXT, domain TEXT, cpu INTEGER, mem INTEGER,
  generation INTEGER, healthy INTEGER);
CREATE TABLE IF NOT EXISTS rt_seats(
  id TEXT PRIMARY KEY, workload TEXT, cluster TEXT, region TEXT, domain TEXT,
  role TEXT, cpu INTEGER, mem INTEGER, generation INTEGER, state TEXT,
  created_at INTEGER);
CREATE TABLE IF NOT EXISTS rt_artifacts(
  id TEXT PRIMARY KEY, workload TEXT, kind TEXT, region TEXT, cluster TEXT,
  data_category TEXT, purpose TEXT, seq INTEGER, state TEXT, created_at INTEGER);
CREATE TABLE IF NOT EXISTS rt_permissions(
  id TEXT PRIMARY KEY, workload TEXT, actor TEXT, scope TEXT,
  revision INTEGER, expires INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS rt_obligations(
  id TEXT PRIMARY KEY, workload TEXT, rule_revision INTEGER, region TEXT,
  state TEXT, created_at INTEGER, closed_at INTEGER);
CREATE TABLE IF NOT EXISTS rt_migrations(
  id TEXT PRIMARY KEY, workload TEXT, source TEXT, target TEXT,
  permission TEXT, state TEXT, mode TEXT, created_at INTEGER, closed_at INTEGER);
CREATE TABLE IF NOT EXISTS rt_incidents(
  id TEXT PRIMARY KEY, cluster TEXT, severity INTEGER, generation INTEGER,
  state TEXT);
CREATE TABLE IF NOT EXISTS rt_handoffs(
  id TEXT PRIMARY KEY, workload TEXT, source TEXT, target TEXT,
  incident TEXT, permission TEXT, state TEXT, created_at INTEGER);
CREATE TABLE IF NOT EXISTS rt_receipts(
  id TEXT PRIMARY KEY, handoff TEXT, kind TEXT, cluster TEXT,
  generation INTEGER, region TEXT, at INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS rt_writes(
  workload TEXT, seq INTEGER, region TEXT, cluster TEXT,
  data_category TEXT, purpose TEXT, at INTEGER,
  PRIMARY KEY(workload,seq,cluster));
'''

DEFAULT_CATEGORY = 'customer'
DEFAULT_PURPOSE = 'primary_service'


# --------------------------------------------------------------------------- #
# 规则查询：规则只追加，旧代次永久可查
# --------------------------------------------------------------------------- #
def rules_for(db, tenant):
    return many(db, '''SELECT * FROM rt_rule_versions WHERE tenant=?
                       ORDER BY revision ASC''', (tenant,))


def effective_rule(db, tenant, at):
    """at 时刻有效的最新已生效规则；尚未生效时回退到上一代次（可能为 None）。"""
    rows = many(db, '''SELECT * FROM rt_rule_versions WHERE tenant=?
                       AND effective_at<=? ORDER BY revision DESC''', (tenant, at))
    return rows[0] if rows else None


def signed_rules_in_window(db, tenant, at):
    """截至 at 已签署、且生效时间窗与「at 之后的承诺」相交的全部规则。

    预留/占用/接管建立的承诺会跨过规则生效边界，因此必须同时满足当时有效规则
    与所有已签署的未来规则——只看当前规则会让第 23 分钟的 west 备席绕过
    第 25 分钟生效的 east-only 规则。
    """
    return many(db, '''SELECT * FROM rt_rule_versions WHERE tenant=?
                       AND signed_at<=? ORDER BY revision ASC''', (tenant, at))


def region_allowed(rule, region):
    return bool(rule) and region in json.loads(rule['regions'])


def gate_region(rules, region):
    """一组规则对 region 的交集裁决：全部允许才放行。"""
    return bool(rules) and all(region_allowed(r, region) for r in rules)


def rule_allows_usage(rule, region, category, purpose):
    if not region_allowed(rule, region):
        return False
    cats = json.loads(rule['data_categories'])
    purs = json.loads(rule['purposes'])
    return category in cats and purpose in purs


def retention_allowed(rule, region, category):
    """限定用途封存：旧地域已退出处理地域（不在 regions 内），但仍可凭封存许可
    在 retention_regions 中保留既有数据；普通处理目的在旧地域依旧被拒。"""
    if not rule:
        return False
    kept = json.loads(rule['retention_regions'])
    cats = json.loads(rule['data_categories'])
    return region in kept and category in cats \
        and 'sealed_retention' in json.loads(rule['purposes'])


def latest_revision(db, tenant):
    row = one(db, '''SELECT COALESCE(MAX(revision),0) r FROM rt_rule_versions
                     WHERE tenant=?''', (tenant,))
    return row['r']


# --------------------------------------------------------------------------- #
# 合规姿态：从不可变事实即时推导，永不落库为 compliant
# --------------------------------------------------------------------------- #
def seat_posture(db, seat, at):
    """席位真实位置 + 当前规则 -> 合规姿态，附带迁出义务。"""
    rule = effective_rule(db, _seat_workload_tenant(db, seat['workload']), at)
    allowed = region_allowed(rule, seat['region']) if rule else False
    obl = one(db, '''SELECT * FROM rt_obligations WHERE workload=?
                     AND state='open' ORDER BY created_at DESC LIMIT 1''',
              (seat['workload'],))
    return {
        'seat': seat['id'],
        'workload': seat['workload'],
        'physical_region': seat['region'],
        'cluster': seat['cluster'],
        'role': seat['role'],
        'state': seat['state'],
        'compliance': 'compliant' if allowed else 'non_compliant',
        'effective_rule_revision': rule['revision'] if rule else None,
        'open_obligation': obl['id'] if obl else None,
    }


def _seat_workload_tenant(db, workload):
    w = one(db, 'SELECT tenant FROM rt_workloads WHERE id=?', (workload,))
    return w['tenant'] if w else None


def _open_obligations(db, workload):
    return many(db, '''SELECT * FROM rt_obligations WHERE workload=?
                       AND state='open' ORDER BY created_at''', (workload,))


def _permission_valid(db, perm_id, workload, scope, at):
    if not perm_id:
        return None
    p = one(db, 'SELECT * FROM rt_permissions WHERE id=?', (perm_id,))
    if not p or p['workload'] != workload or p['scope'] != scope:
        return None
    if p['state'] != 'active' or p['expires'] < at:
        return None
    return p


# --------------------------------------------------------------------------- #
# 命令处理
# --------------------------------------------------------------------------- #
def handle(db, x):
    op = x['op']

    if op == 'sign_rule':
        tenant = x['tenant']
        if x['revision'] <= latest_revision(db, tenant):
            return {'decision': 'stale_rule'}
        regions = x['regions']
        if not regions or len(set(regions)) != len(regions):
            return {'decision': 'invalid_regions'}
        if x['effective_at'] < x['signed_at']:
            return {'decision': 'invalid_effective_time'}
        cats = x.get('data_categories') or [DEFAULT_CATEGORY]
        purs = x.get('purposes') or [DEFAULT_PURPOSE]
        if not cats or not purs:
            return {'decision': 'invalid_rule_scope'}
        scope = x.get('workload_scope', 'existing_and_new')
        retention = x.get('retention_regions', [])
        db.execute('''INSERT INTO rt_rule_versions VALUES(?,?,?,?,?,?,?,?,?)''',
                   (tenant, x['revision'], json.dumps(regions), x['effective_at'],
                    json.dumps(cats), json.dumps(purs), json.dumps(retention),
                    scope, x['signed_at']))
        # 对签署时已存在、其合同地域被新规则排除的负载建立迁出/封存义务。
        obligations = []
        if scope in ('existing_and_new', 'existing_only'):
            for w in many(db, 'SELECT * FROM rt_workloads WHERE tenant=?', (tenant,)):
                if w['contract_region'] not in regions:
                    oid = f"obl-{w['id']}-r{x['revision']}"
                    if not one(db, 'SELECT 1 FROM rt_obligations WHERE id=?', (oid,)):
                        db.execute('''INSERT INTO rt_obligations VALUES(?,?,?,?,?,?,?)''',
                                   (oid, w['id'], x['revision'], w['contract_region'],
                                    'open', x['signed_at'], None))
                        obligations.append(oid)
        return {'decision': 'rule_recorded',
                'revision': x['revision'],
                'effective_at': x['effective_at'],
                'obligations': obligations}

    if op == 'admit':
        if one(db, 'SELECT 1 FROM rt_workloads WHERE id=?', (x['id'],)):
            return {'decision': 'intent_exists'}
        rules = signed_rules_in_window(db, x['tenant'], x['at'])
        if not rules or not gate_region(rules, x['region']):
            return {'decision': 'residency_denied'}
        db.execute('''INSERT INTO rt_workloads VALUES(?,?,?,?,?,?,?,?,?)''',
                   (x['id'], x['tenant'], x['region'], x['role'], 'active', 1,
                    None, 'writable', x['at']))
        return {'decision': 'contract_accepted', 'workload': x['id']}

    if op == 'observe_cluster':
        old = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (x['id'],))
        if old and x['generation'] <= old['generation']:
            return {'decision': 'stale_inventory'}
        if min(x['cpu'], x['mem']) <= 0:
            return {'decision': 'invalid_capacity'}
        db.execute('''INSERT INTO rt_clusters VALUES(?,?,?,?,?,?,?)
                      ON CONFLICT(id) DO UPDATE SET
                      region=excluded.region,domain=excluded.domain,
                      cpu=excluded.cpu,mem=excluded.mem,
                      generation=excluded.generation,healthy=excluded.healthy''',
                   (x['id'], x['region'], x['domain'], x['cpu'], x['mem'],
                    x['generation'], int(x['healthy'])))
        return {'decision': 'observed'}

    if op == 'grant_permission':
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (x['workload'],))
        if not w:
            return {'decision': 'missing_workload'}
        db.execute('''INSERT INTO rt_permissions VALUES(?,?,?,?,?,?,?)''',
                   (x['id'], x['workload'], x['actor'], x['scope'],
                    x['revision'], x['expires'], 'active'))
        return {'decision': 'granted'}

    if op == 'withdraw_permission':
        p = one(db, 'SELECT * FROM rt_permissions WHERE id=?', (x['id'],))
        if not p or p['state'] != 'active':
            return {'decision': 'permission_unavailable'}
        db.execute("UPDATE rt_permissions SET state='withdrawn' WHERE id=?", (x['id'],))
        # 依赖该许可、尚未完成的接管与迁出立即失去授权。
        db.execute("UPDATE rt_handoffs SET state='blocked' WHERE permission=? AND state='planned'",
                   (x['id'],))
        db.execute("UPDATE rt_migrations SET state='blocked' WHERE permission=? AND state IN ('planned','in_flight')",
                   (x['id'],))
        return {'decision': 'withdrawn'}

    if op == 'reserve_seat':
        if x['role'] not in ('primary', 'backup'):
            return {'decision': 'invalid_placement_role'}
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (x['workload'],))
        c = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (x['cluster'],))
        if not w or w['status'] != 'active':
            return {'decision': 'missing_workload'}
        if not c or not c['healthy']:
            return {'decision': 'ineligible_cluster'}
        # 承诺跨越生效边界：已签署的全部规则（含未来规则）都必须允许实物地域。
        rules = signed_rules_in_window(db, w['tenant'], x['at'])
        if not gate_region(rules, c['region']):
            return {'decision': 'residency_denied'}
        siblings = many(db, '''SELECT * FROM rt_seats WHERE workload=?
                               AND state IN ('reserved','occupied')''',
                        (x['workload'],))
        if any(s['domain'] == c['domain'] for s in siblings):
            return {'decision': 'same_disaster_domain'}
        # 角色承诺只由「在所有已签署规则下仍合规」的席位持有；被新规则排除的
        # 旧席虽仍占用实物资源并挂着迁出义务，但不再阻止合规地域的同角色新席。
        compliant = [s for s in siblings if gate_region(rules, s['region'])]
        if any(s['role'] == x['role'] for s in compliant):
            return {'decision': 'role_already_committed'}
        used = one(db, '''SELECT COALESCE(SUM(cpu),0) cpu,COALESCE(SUM(mem),0) mem
                          FROM rt_seats WHERE cluster=? AND state IN ('reserved','occupied')''',
                   (x['cluster'],))
        if min(x['cpu'], x['mem']) <= 0 or used['cpu'] + x['cpu'] > c['cpu'] \
                or used['mem'] + x['mem'] > c['mem']:
            return {'decision': 'capacity_exhausted'}
        db.execute('''INSERT INTO rt_seats VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
                   (x['id'], x['workload'], c['id'], c['region'], c['domain'],
                    x['role'], x['cpu'], x['mem'], c['generation'], 'reserved', x['at']))
        return {'decision': 'reserved', 'seat': x['id'], 'region': c['region']}

    if op == 'occupy_seat':
        s = one(db, 'SELECT * FROM rt_seats WHERE id=?', (x['id'],))
        if not s or s['state'] != 'reserved':
            return {'decision': 'stale_lease'}
        c = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (s['cluster'],))
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (s['workload'],))
        if not c or c['generation'] != s['generation'] or not c['healthy']:
            return {'decision': 'stale_lease'}
        # 占用是新的实物承诺，重新核对签署时已存在的全部规则代次。
        rules = signed_rules_in_window(db, w['tenant'], x['at'])
        if not gate_region(rules, c['region']):
            db.execute("UPDATE rt_seats SET state='rejected' WHERE id=?", (x['id'],))
            return {'decision': 'residency_denied'}
        db.execute("UPDATE rt_seats SET state='occupied' WHERE id=?", (x['id'],))
        if s['role'] == 'primary' and w['primary_cluster'] is None:
            db.execute('UPDATE rt_workloads SET primary_cluster=? WHERE id=?',
                       (c['id'], w['id']))
        return {'decision': 'occupied', 'seat': x['id'], 'region': c['region']}

    if op == 'write':
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (x['workload'],))
        if not w or w['status'] != 'active':
            return {'decision': 'workload_unavailable'}
        if w['write_state'] != 'writable':
            return {'decision': 'writes_paused'}
        # 先按实物数据路径裁决法域：任何集群（含备席 C1）尝试承接客户新写入，
        # 都必须落在有效地域，避免「找不到主席」掩盖越界写入这一事实。
        target = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (x['cluster'],))
        rule = effective_rule(db, w['tenant'], x['at'])
        category = x.get('data_category', DEFAULT_CATEGORY)
        purpose = x.get('purpose', DEFAULT_PURPOSE)
        if not target or not rule_allows_usage(rule, target['region'], category, purpose):
            return {'decision': 'residency_denied'}
        seat = one(db, '''SELECT * FROM rt_seats WHERE workload=? AND cluster=?
                          AND role='primary' AND state='occupied' ''',
                   (x['workload'], x['cluster']))
        if not seat:
            return {'decision': 'no_writable_seat'}
        prev = one(db, 'SELECT COALESCE(MAX(seq),0) m FROM rt_writes WHERE workload=?',
                   (x['workload'],))
        if x['seq'] != prev['m'] + 1:
            return {'decision': 'sequence_conflict'}
        db.execute('''INSERT INTO rt_writes VALUES(?,?,?,?,?,?,?)''',
                   (x['workload'], x['seq'], seat['region'], x['cluster'],
                    category, purpose, x['at']))
        return {'decision': 'written', 'seq': x['seq'], 'region': seat['region']}

    if op == 'place_artifact':
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (x['workload'],))
        if not w:
            return {'decision': 'missing_workload'}
        c = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (x['cluster'],))
        region = c['region'] if c else x['region']
        rule = effective_rule(db, w['tenant'], x['at'])
        category = x.get('data_category', DEFAULT_CATEGORY)
        purpose = x.get('purpose', DEFAULT_PURPOSE)
        if not rule_allows_usage(rule, region, category, purpose):
            return {'decision': 'residency_denied'}
        db.execute('''INSERT INTO rt_artifacts VALUES(?,?,?,?,?,?,?,?,?,?)''',
                   (x['id'], x['workload'], x['kind'], region,
                    c['id'] if c else None, category, purpose,
                    x.get('seq'), 'live', x['at']))
        return {'decision': 'artifact_placed', 'artifact': x['id'], 'region': region}

    if op == 'seal_artifact':
        a = one(db, 'SELECT * FROM rt_artifacts WHERE id=?', (x['id'],))
        if not a:
            return {'decision': 'missing_artifact'}
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (a['workload'],))
        rule = effective_rule(db, w['tenant'], x['at'])
        # 旧地域的限定用途封存：当前规则必须仍把该地域列为封存保留地，
        # 且出示了对应的封存许可；普通处理目的在旧地域仍然被拒。
        perm = _permission_valid(db, x.get('permission'), a['workload'], 'seal_retention', x['at'])
        if not perm:
            return {'decision': 'authorization_denied'}
        if not retention_allowed(rule, a['region'], a['data_category']):
            return {'decision': 'residency_denied'}
        db.execute("UPDATE rt_artifacts SET state='sealed' WHERE id=?", (x['id'],))
        return {'decision': 'sealed', 'artifact': x['id'], 'region': a['region']}

    if op == 'copy_artifact':
        a = one(db, 'SELECT * FROM rt_artifacts WHERE id=?', (x['id'],))
        if not a or a['state'] not in ('live', 'sealed'):
            return {'decision': 'missing_artifact'}
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (a['workload'],))
        c = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (x['target_cluster'],))
        if not c or not c['healthy']:
            return {'decision': 'ineligible_cluster'}
        # 迁出拷贝同样受规则交集约束，不能把滞留快照搬进另一个越界地域。
        rules = signed_rules_in_window(db, w['tenant'], x['at'])
        if not gate_region(rules, c['region']):
            return {'decision': 'residency_denied'}
        if one(db, 'SELECT 1 FROM rt_artifacts WHERE id=?', (x['new_id'],)):
            return {'decision': 'artifact_exists'}
        db.execute('''INSERT INTO rt_artifacts VALUES(?,?,?,?,?,?,?,?,?,?)''',
                   (x['new_id'], a['workload'], a['kind'], c['region'], c['id'],
                    a['data_category'], a['purpose'], a['seq'], 'live', x['at']))
        return {'decision': 'artifact_copied', 'artifact': x['new_id'],
                'region': c['region']}

    if op == 'incident':
        old = one(db, 'SELECT * FROM rt_incidents WHERE id=?', (x['id'],))
        if old and x['generation'] <= old['generation']:
            return {'decision': 'stale_incident'}
        state = 'evacuate' if x['severity'] >= 3 else 'watch'
        db.execute('''INSERT INTO rt_incidents VALUES(?,?,?,?,?)
                      ON CONFLICT(id) DO UPDATE SET severity=excluded.severity,
                      generation=excluded.generation,state=excluded.state''',
                   (x['id'], x['cluster'], x['severity'], x['generation'], state))
        return {'decision': state}

    if op == 'plan_handoff':
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (x['workload'],))
        inc = one(db, 'SELECT * FROM rt_incidents WHERE id=?', (x['incident'],))
        source = one(db, '''SELECT * FROM rt_seats WHERE workload=? AND cluster=?
                            AND role='primary' AND state='occupied' ''',
                     (x['workload'], x['source']))
        target = one(db, '''SELECT * FROM rt_seats WHERE workload=? AND cluster=?
                            AND role='backup' AND state='occupied' ''',
                     (x['workload'], x['target']))
        if not w or w['primary_cluster'] != x['source']:
            return {'decision': 'no_safe_target'}
        if not inc or inc['state'] != 'evacuate' or inc['cluster'] != x['source']:
            return {'decision': 'no_safe_target'}
        if not source or not target:
            return {'decision': 'no_safe_target'}
        if not _permission_valid(db, x.get('permission'), x['workload'], 'failover', x['at']):
            return {'decision': 'authorization_denied'}
        tc = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (x['target'],))
        if not tc or not tc['healthy'] or tc['generation'] != target['generation']:
            return {'decision': 'target_changed'}
        # 接管链必须回看法域：目标实物地域要满足所有已签署规则。
        rules = signed_rules_in_window(db, w['tenant'], x['at'])
        if not gate_region(rules, tc['region']):
            return {'decision': 'residency_denied'}
        db.execute('''INSERT INTO rt_handoffs VALUES(?,?,?,?,?,?,?,?)''',
                   (x['id'], x['workload'], x['source'], x['target'], x['incident'],
                    x.get('permission'), 'planned', x['at']))
        return {'decision': 'planned', 'handoff': x['id']}

    if op == 'handoff_receipt':
        h = one(db, 'SELECT * FROM rt_handoffs WHERE id=?', (x['handoff'],))
        if not h or h['state'] != 'planned':
            return {'decision': 'plan_unavailable'}
        if x['kind'] not in ('source_fenced', 'target_ready'):
            return {'decision': 'invalid_receipt_kind'}
        expected = h['source'] if x['kind'] == 'source_fenced' else h['target']
        c = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (expected,))
        if not c or c['id'] != x['cluster'] or c['generation'] != x['generation']:
            return {'decision': 'receipt_identity_conflict'}
        # 源端围栏只证明旧主站停止写入，不是新的数据处理承诺，无需法域裁决；
        # 目标就绪回执才建立实物数据路径，迟到时必须按提交时刻的法域重新受审，
        # 旧接管计划不能凭旧 intent.region 继续生效。
        if x['kind'] == 'target_ready':
            w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (h['workload'],))
            rules = signed_rules_in_window(db, w['tenant'], x['at'])
            if not gate_region(rules, c['region']):
                db.execute("UPDATE rt_handoffs SET state='blocked' WHERE id=?", (h['id'],))
                return {'decision': 'residency_denied'}
        db.execute('''INSERT INTO rt_receipts VALUES(?,?,?,?,?,?,?,?)''',
                   (x['id'], h['id'], x['kind'], x['cluster'], x['generation'],
                    c['region'], x['at'], 'accepted'))
        return {'decision': 'recorded'}

    if op == 'complete_handoff':
        h = one(db, 'SELECT * FROM rt_handoffs WHERE id=?', (x['id'],))
        if not h:
            return {'decision': 'plan_unavailable'}
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (h['workload'],))
        # 许可撤回即时阻断，即使双端回执齐备；优先于状态/回执检查给出真实原因。
        if not _permission_valid(db, h['permission'], h['workload'], 'failover', x['at']):
            db.execute("UPDATE rt_handoffs SET state='blocked' WHERE id=?", (h['id'],))
            return {'decision': 'authorization_denied'}
        if h['state'] != 'planned':
            return {'decision': 'plan_unavailable'}
        for kind, cluster in (('source_fenced', h['source']), ('target_ready', h['target'])):
            r = one(db, '''SELECT * FROM rt_receipts WHERE handoff=? AND kind=?
                           AND cluster=? AND state='accepted' ''',
                    (h['id'], kind, cluster))
            if not r:
                return {'decision': 'partial_handoff', 'missing': kind}
            cur = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (cluster,))
            if not cur or cur['generation'] != r['generation']:
                return {'decision': 'stale_handoff_evidence', 'cluster': cluster}
            if kind == 'target_ready' and not cur['healthy']:
                return {'decision': 'stale_handoff_evidence', 'cluster': cluster}
        target_cluster = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (h['target'],))
        # 完成时刻再次核对法域交集与实物数据路径。
        rules = signed_rules_in_window(db, w['tenant'], x['at'])
        if not gate_region(rules, target_cluster['region']):
            db.execute("UPDATE rt_handoffs SET state='blocked' WHERE id=?", (h['id'],))
            return {'decision': 'residency_denied'}
        db.execute("UPDATE rt_handoffs SET state='complete' WHERE id=?", (h['id'],))
        db.execute('UPDATE rt_workloads SET primary_cluster=?,revision=revision+1 WHERE id=?',
                   (h['target'], h['workload']))
        # 原主席不释放：降为只保留审计证据，迁出义务仍挂在负载上。
        db.execute('''UPDATE rt_seats SET state='evidence',role='evidence'
                      WHERE workload=? AND cluster=? AND role='primary' ''',
                   (h['workload'], h['source']))
        db.execute('''UPDATE rt_seats SET role='primary' WHERE workload=?
                      AND cluster=? AND role='backup' AND state='occupied' ''',
                   (h['workload'], h['target']))
        return {'decision': 'transferred', 'primary': h['target'],
                'source_seat': 'retained_as_evidence'}

    if op == 'plan_migration':
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (x['workload'],))
        if not w:
            return {'decision': 'missing_workload'}
        if not _permission_valid(db, x.get('permission'), x['workload'], 'migration', x['at']):
            return {'decision': 'authorization_denied'}
        tc = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (x['target'],))
        if not tc or not tc['healthy']:
            return {'decision': 'ineligible_cluster'}
        rules = signed_rules_in_window(db, w['tenant'], x['at'])
        if not gate_region(rules, tc['region']):
            return {'decision': 'residency_denied'}
        # 目标须有独立容量与独立灾难域（证据席已退出灾难域承诺，不参与比较）。
        existing = many(db, '''SELECT * FROM rt_seats WHERE workload=?
                               AND state IN ('reserved','occupied')''',
                        (x['workload'],))
        if any(s['domain'] == tc['domain'] for s in existing):
            return {'decision': 'same_disaster_domain'}
        used = one(db, '''SELECT COALESCE(SUM(cpu),0) cpu,COALESCE(SUM(mem),0) mem
                          FROM rt_seats WHERE cluster=? AND state IN ('reserved','occupied')''',
                   (tc['id'],))
        need_cpu = x.get('cpu', 1)
        need_mem = x.get('mem', 1)
        if used['cpu'] + need_cpu > tc['cpu'] or used['mem'] + need_mem > tc['mem']:
            return {'decision': 'capacity_exhausted'}
        db.execute('''INSERT INTO rt_migrations VALUES(?,?,?,?,?,?,?,?,?)''',
                   (x['id'], x['workload'], x['source'], x['target'],
                    x.get('permission'), 'planned', x['mode'], x['at'], None))
        return {'decision': 'migration_planned', 'migration': x['id'], 'mode': x['mode']}

    if op == 'pause_writes':
        m = one(db, 'SELECT * FROM rt_migrations WHERE id=?', (x['migration'],))
        if not m:
            return {'decision': 'migration_unavailable'}
        if not _permission_valid(db, m['permission'], m['workload'], 'migration', x['at']):
            db.execute("UPDATE rt_migrations SET state='blocked' WHERE id=?", (m['id'],))
            return {'decision': 'authorization_denied'}
        if m['state'] not in ('planned', 'in_flight'):
            return {'decision': 'migration_unavailable'}
        db.execute("UPDATE rt_workloads SET write_state='paused' WHERE id=?", (m['workload'],))
        db.execute("UPDATE rt_migrations SET state='in_flight' WHERE id=?", (m['id'],))
        return {'decision': 'writes_paused', 'workload': m['workload']}

    if op == 'copy_prefix':
        m = one(db, 'SELECT * FROM rt_migrations WHERE id=?', (x['migration'],))
        if not m or m['state'] != 'in_flight':
            return {'decision': 'migration_unavailable'}
        if not _permission_valid(db, m['permission'], m['workload'], 'migration', x['at']):
            return {'decision': 'authorization_denied'}
        tc = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (m['target'],))
        rules = signed_rules_in_window(db,
                   one(db, 'SELECT tenant FROM rt_workloads WHERE id=?', (m['workload'],))['tenant'],
                   x['at'])
        if not gate_region(rules, tc['region']):
            return {'decision': 'residency_denied'}
        # 逐页复制已确认前缀与全部滞留工件，目标必须是合规实物地域。
        # 源端 west 事实行原样保留（审计），目标 east 另立同序号副本。
        copied = []
        for row in many(db, 'SELECT * FROM rt_writes WHERE workload=? ORDER BY seq',
                        (m['workload'],)):
            db.execute('''INSERT INTO rt_writes VALUES(?,?,?,?,?,?,?)''',
                       (row['workload'], row['seq'], tc['region'], tc['id'],
                        row['data_category'], row['purpose'], x['at']))
            copied.append(row['seq'])
        for a in many(db, "SELECT * FROM rt_artifacts WHERE workload=? AND state IN ('live','sealed')",
                      (m['workload'],)):
            new_id = f"{a['id']}->copy"
            if not one(db, 'SELECT 1 FROM rt_artifacts WHERE id=?', (new_id,)):
                db.execute('''INSERT INTO rt_artifacts VALUES(?,?,?,?,?,?,?,?,?,?)''',
                           (new_id, a['workload'], a['kind'], tc['region'], tc['id'],
                            a['data_category'], a['purpose'], a['seq'], 'live', x['at']))
        db.execute("UPDATE rt_migrations SET state='prefix_copied' WHERE id=?", (m['id'],))
        return {'decision': 'prefix_copied', 'seq_through': max(copied) if copied else 0,
                'region': tc['region']}

    if op == 'verify_target':
        m = one(db, 'SELECT * FROM rt_migrations WHERE id=?', (x['migration'],))
        if not m or m['state'] != 'prefix_copied':
            return {'decision': 'migration_unavailable'}
        tc = one(db, 'SELECT * FROM rt_clusters WHERE id=?', (m['target'],))
        if not tc or not tc['healthy'] or tc['generation'] != x['generation']:
            return {'decision': 'target_changed'}
        # 数据完整性核验：目标侧已复制序号必须与已确认前缀连续一致。
        src_max = one(db, 'SELECT COALESCE(MAX(seq),0) m FROM rt_writes WHERE workload=?',
                      (m['workload'],))['m']
        tgt = many(db, 'SELECT seq FROM rt_writes WHERE workload=? AND cluster=? ORDER BY seq',
                   (m['workload'], m['target']))
        seqs = [r['seq'] for r in tgt]
        if seqs != list(range(1, src_max + 1)):
            return {'decision': 'integrity_check_failed'}
        db.execute("UPDATE rt_migrations SET state='verified' WHERE id=?", (m['id'],))
        return {'decision': 'target_verified', 'seq_through': src_max}

    if op == 'demote_to_evidence':
        m = one(db, 'SELECT * FROM rt_migrations WHERE id=?', (x['migration'],))
        if not m or m['state'] not in ('in_flight', 'prefix_copied', 'verified'):
            return {'decision': 'migration_unavailable'}
        cluster = x.get('cluster', m['source'])
        changed = db.execute('''UPDATE rt_seats SET state='evidence',role='evidence'
                                WHERE workload=? AND cluster=?
                                AND state IN ('reserved','occupied') ''',
                             (m['workload'], cluster)).rowcount
        if not changed:
            return {'decision': 'no_source_seat'}
        return {'decision': 'demoted_to_evidence', 'cluster': cluster}

    if op == 'resume_writes':
        m = one(db, 'SELECT * FROM rt_migrations WHERE id=?', (x['migration'],))
        if not m or m['state'] != 'verified':
            return {'decision': 'migration_unavailable'}
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (m['workload'],))
        rule = effective_rule(db, w['tenant'], x['at'])
        primary = one(db, '''SELECT * FROM rt_seats WHERE workload=? AND role='primary'
                             AND state='occupied' ''', (m['workload'],))
        if not primary or not region_allowed(rule, primary['region']):
            # 切换点：核验通过的 east 备席升为主席，旧越界主席已是证据席。
            target = one(db, '''SELECT * FROM rt_seats WHERE workload=? AND cluster=?
                                AND role='backup' AND state='occupied' ''',
                         (m['workload'], m['target']))
            if not target or not region_allowed(rule, target['region']):
                return {'decision': 'no_writable_seat'}
            db.execute('''UPDATE rt_seats SET role='primary' WHERE id=?''', (target['id'],))
            primary = target
        db.execute("UPDATE rt_workloads SET write_state='writable',primary_cluster=? WHERE id=?",
                   (primary['cluster'], m['workload']))
        return {'decision': 'writes_resumed', 'cluster': primary['cluster']}

    if op == 'complete_migration':
        m = one(db, 'SELECT * FROM rt_migrations WHERE id=?', (x['migration'],))
        if not m or m['state'] != 'verified':
            return {'decision': 'migration_unavailable'}
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (m['workload'],))
        # 完成条件：不存在仍可写的越界席位；旧席只能是 evidence/sealed 保留态。
        live = many(db, '''SELECT * FROM rt_seats WHERE workload=?
                           AND state IN ('reserved','occupied')''', (m['workload'],))
        rule = effective_rule(db, w['tenant'], x['at'])
        for s in live:
            if not region_allowed(rule, s['region']):
                return {'decision': 'residency_denied'}
        db.execute("UPDATE rt_migrations SET state='complete',closed_at=? WHERE id=?",
                   (x['at'], m['id']))
        # 核验通过的 east 备席升为主席，客户写入口随之落在有效地域。
        db.execute('''UPDATE rt_seats SET role='primary' WHERE workload=?
                      AND cluster=? AND role='backup' AND state='occupied' ''',
                   (m['workload'], m['target']))
        db.execute('''UPDATE rt_obligations SET state='closed',closed_at=?
                      WHERE workload=? AND state='open' ''', (x['at'], m['workload']))
        db.execute('UPDATE rt_workloads SET revision=revision+1 WHERE id=?', (m['workload'],))
        return {'decision': 'migration_complete', 'migration': m['id']}

    if op == 'cancel_migration':
        m = one(db, 'SELECT * FROM rt_migrations WHERE id=?', (x['migration'],))
        if not m or m['state'] not in ('planned', 'in_flight', 'prefix_copied',
                                       'verified', 'blocked'):
            return {'decision': 'migration_unavailable'}
        db.execute("UPDATE rt_migrations SET state='cancelled',closed_at=? WHERE id=?",
                   (x['at'], m['id']))
        # 取消迁出不消除义务：真实位置与 open 义务原样保留，新写入仍被暂停。
        open_obl = _open_obligations(db, m['workload'])
        return {'decision': 'migration_cancelled',
                'obligations_retained': [o['id'] for o in open_obl],
                'write_state': one(db, 'SELECT write_state FROM rt_workloads WHERE id=?',
                                   (m['workload'],))['write_state']}

    if op == 'release_seat':
        s = one(db, 'SELECT * FROM rt_seats WHERE id=?', (x['id'],))
        if not s:
            return {'decision': 'missing_seat'}
        # 证据席持有审计证据，任何时候都不能直接释放。
        if s['state'] == 'evidence':
            return {'decision': 'retention_required'}
        if s['state'] not in ('reserved', 'rejected'):
            # 迁出未完成的已占席不得直接释放，必须先有保留/封存/暂停安排。
            if _open_obligations(db, s['workload']):
                return {'decision': 'retention_required'}
        db.execute("UPDATE rt_seats SET state='released' WHERE id=?", (x['id'],))
        return {'decision': 'released', 'seat': x['id']}

    if op == 'posture':
        w = one(db, 'SELECT * FROM rt_workloads WHERE id=?', (x['workload'],))
        if not w:
            return {'decision': 'missing_workload'}
        rule = effective_rule(db, w['tenant'], x['at'])
        seats = [seat_posture(db, s, x['at'])
                 for s in many(db, 'SELECT * FROM rt_seats WHERE workload=? ORDER BY created_at',
                               (x['workload'],))]
        artifacts = []
        for a in many(db, 'SELECT * FROM rt_artifacts WHERE workload=? ORDER BY created_at',
                      (x['workload'],)):
            if a['state'] == 'sealed':
                allowed = retention_allowed(rule, a['region'], a['data_category'])
            else:
                allowed = rule_allows_usage(rule, a['region'], a['data_category'], a['purpose']) \
                    if rule else False
            artifacts.append({'artifact': a['id'], 'kind': a['kind'],
                              'physical_region': a['region'], 'state': a['state'],
                              'compliance': 'compliant' if allowed else 'non_compliant'})
        return {'decision': 'posture',
                'workload': x['workload'],
                'contract_region': w['contract_region'],
                'primary_cluster': w['primary_cluster'],
                'write_state': w['write_state'],
                'effective_rule_revision': rule['revision'] if rule else None,
                'open_obligations': [o['id'] for o in _open_obligations(db, x['workload'])],
                'seats': seats,
                'artifacts': artifacts}

    if op == 'rule_history':
        rows = rules_for(db, x['tenant'])
        return {'decision': 'rule_history',
                'rules': [{'tenant': r['tenant'], 'revision': r['revision'],
                           'regions': json.loads(r['regions']),
                           'effective_at': r['effective_at'],
                           'signed_at': r['signed_at'],
                           'data_categories': json.loads(r['data_categories']),
                           'purposes': json.loads(r['purposes']),
                           'retention_regions': json.loads(r['retention_regions']),
                           'workload_scope': r['workload_scope']} for r in rows]}

    return {'decision': 'unknown_operation'}


def run(db, x):
    return act(db, 'residency', x, handle)
