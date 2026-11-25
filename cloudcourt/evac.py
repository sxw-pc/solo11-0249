from .store import act,one
SCHEMA='''
CREATE TABLE IF NOT EXISTS incident_cases(id TEXT PRIMARY KEY,cluster TEXT,severity INTEGER,generation INTEGER,state TEXT);
CREATE TABLE IF NOT EXISTS move_plans(id TEXT PRIMARY KEY,intent TEXT,source TEXT,target TEXT,incident TEXT,state TEXT);
CREATE TABLE IF NOT EXISTS handoff_receipts(id TEXT PRIMARY KEY,plan TEXT,kind TEXT,cluster TEXT,generation INTEGER,state TEXT);
CREATE TABLE IF NOT EXISTS recovery_cases(id TEXT PRIMARY KEY,plan TEXT,reason TEXT,source TEXT,target TEXT,state TEXT);
'''

def handle(db,x):
    op=x['op']
    if op=='incident':
        old=one(db,'SELECT * FROM incident_cases WHERE id=?',(x['id'],))
        if old and x['generation']<=old['generation']:return {'decision':'stale_incident'}
        state='evacuate' if x['severity']>=3 else 'watch'
        db.execute('INSERT INTO incident_cases VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET severity=excluded.severity,generation=excluded.generation,state=excluded.state',(x['id'],x['cluster'],x['severity'],x['generation'],state))
        return {'decision':state}
    if op=='plan':
        incident=one(db,'SELECT * FROM incident_cases WHERE id=?',(x['incident'],))
        intent=one(db,'SELECT * FROM workload_intents WHERE id=?',(x['intent'],))
        lease=one(db,"SELECT * FROM placement_leases WHERE intent=? AND cluster=? AND role='backup' AND state='occupied'",(x['intent'],x['target']))
        source=one(db,"SELECT * FROM placement_leases WHERE intent=? AND cluster=? AND role='primary' AND state='occupied'",(x['intent'],x['source']))
        if not incident or incident['state']!='evacuate' or not intent or intent['primary_cluster']!=x['source'] or incident['cluster']!=x['source'] or not lease or not source:
            return {'decision':'no_safe_target'}
        target=one(db,'SELECT * FROM cluster_inventory WHERE id=?',(x['target'],))
        if not target or not target['healthy'] or target['generation']!=lease['generation']:
            return {'decision':'target_changed'}
        db.execute('INSERT INTO move_plans VALUES(?,?,?,?,?,?)',(x['id'],x['intent'],x['source'],x['target'],x['incident'],'planned'))
        return {'decision':'planned'}
    if op=='abort':
        p=one(db,'SELECT * FROM move_plans WHERE id=?',(x['plan'],))
        if not p or p['state']!='planned':return {'decision':'plan_unavailable'}
        target=one(db,'SELECT * FROM cluster_inventory WHERE id=?',(p['target'],))
        lease=one(db,"SELECT * FROM placement_leases WHERE intent=? AND cluster=? AND role='backup' AND state='occupied'",(p['intent'],p['target']))
        if target and target['healthy'] and lease and target['generation']==lease['generation']:
            return {'decision':'target_still_eligible'}
        db.execute("UPDATE move_plans SET state='aborted' WHERE id=?",(p['id'],))
        db.execute("UPDATE placement_leases SET state='released' WHERE intent=? AND cluster=? AND role='backup' AND state='occupied'",(p['intent'],p['target']))
        db.execute("UPDATE capacity_claims SET state='released' WHERE intent=? AND cluster=? AND state='active'",(p['intent'],p['target']))
        db.execute('INSERT INTO recovery_cases VALUES(?,?,?,?,?,?)',(x['id'],p['id'],x['reason'],p['source'],p['target'],'awaiting_replan'))
        return {'decision':'aborted_for_replan'}
    if op=='receipt':
        p=one(db,'SELECT * FROM move_plans WHERE id=?',(x['plan'],))
        if not p or p['state']!='planned':return {'decision':'plan_unavailable'}
        expected=p['source'] if x['kind']=='source_fenced' else p['target'] if x['kind']=='target_ready' else None
        c=one(db,'SELECT * FROM cluster_inventory WHERE id=?',(expected,)) if expected else None
        if not c or c['id']!=x['cluster'] or c['generation']!=x['generation']:
            return {'decision':'receipt_identity_conflict'}
        db.execute('INSERT INTO handoff_receipts VALUES(?,?,?,?,?,?)',(x['id'],x['plan'],x['kind'],x['cluster'],x['generation'],'accepted'))
        return {'decision':'recorded'}
    if op=='handoff':
        p=one(db,'SELECT * FROM move_plans WHERE id=?',(x['plan'],))
        if not p or p['state']!='planned':return {'decision':'plan_unavailable'}
        for kind,cluster in [('source_fenced',p['source']),('target_ready',p['target'])]:
            receipt=one(db,'SELECT * FROM handoff_receipts WHERE plan=? AND kind=? AND cluster=?',(x['plan'],kind,cluster))
            current=one(db,'SELECT * FROM cluster_inventory WHERE id=?',(cluster,))
            if not receipt:
                return {'decision':'partial_handoff','missing':kind}
            if not current or current['generation']!=receipt['generation'] or not current['healthy'] and kind=='target_ready':
                return {'decision':'stale_handoff_evidence','cluster':cluster}
        db.execute("UPDATE move_plans SET state='complete' WHERE id=?",(x['plan'],))
        db.execute('UPDATE workload_intents SET primary_cluster=?,revision=revision+1 WHERE id=?',(p['target'],p['intent']))
        db.execute("UPDATE placement_leases SET state='released' WHERE intent=? AND cluster=? AND state='occupied'",(p['intent'],p['source']))
        db.execute("UPDATE placement_leases SET role='primary' WHERE intent=? AND cluster=? AND role='backup' AND state='occupied'",(p['intent'],p['target']))
        db.execute("UPDATE capacity_claims SET state='released' WHERE intent=? AND cluster=? AND state='active'",(p['intent'],p['source']))
        db.execute("UPDATE recovery_cases SET state='recovered' WHERE source=? AND state='awaiting_replan' AND plan IN (SELECT id FROM move_plans WHERE intent=?)",(p['source'],p['intent']))
        return {'decision':'transferred','primary':p['target']}
    return {'decision':'unknown_operation'}

def run(db,x):return act(db,'evac',x,handle)
