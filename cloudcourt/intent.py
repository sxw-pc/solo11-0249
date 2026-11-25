from .store import act,one,many
SCHEMA='''
CREATE TABLE IF NOT EXISTS residency_rules(tenant TEXT PRIMARY KEY,regions TEXT,revision INTEGER);
CREATE TABLE IF NOT EXISTS workload_intents(id TEXT PRIMARY KEY,tenant TEXT,region TEXT,role TEXT,status TEXT,revision INTEGER,primary_cluster TEXT);
CREATE TABLE IF NOT EXISTS owner_grants(id TEXT PRIMARY KEY,intent TEXT,actor TEXT,role TEXT,generation INTEGER,expires INTEGER,status TEXT);
'''

def handle(db,x):
    op=x['op']
    if op=='rule':
        old=one(db,'SELECT * FROM residency_rules WHERE tenant=?',(x['tenant'],))
        if old and x['revision']<=old['revision']:return {'decision':'stale_rule'}
        if not x['regions'] or len(set(x['regions']))!=len(x['regions']):return {'decision':'invalid_regions'}
        import json
        db.execute('INSERT INTO residency_rules VALUES(?,?,?) ON CONFLICT(tenant) DO UPDATE SET regions=excluded.regions,revision=excluded.revision',(x['tenant'],json.dumps(x['regions']),x['revision']))
        return {'decision':'rule_recorded'}
    if op=='grant':
        i=one(db,'SELECT * FROM workload_intents WHERE id=?',(x['intent'],))
        if not i:return {'decision':'missing_intent'}
        db.execute('INSERT INTO owner_grants VALUES(?,?,?,?,?,?,?)',(x['id'],x['intent'],x['actor'],x['role'],x['generation'],x['expires'],'active'))
        return {'decision':'granted'}
    if op=='claim':
        import json
        rule=one(db,'SELECT * FROM residency_rules WHERE tenant=?',(x['tenant'],))
        if not rule or x['region'] not in json.loads(rule['regions']):return {'decision':'residency_denied'}
        old=one(db,'SELECT * FROM workload_intents WHERE id=?',(x['id'],))
        if old:return {'decision':'intent_exists'}
        db.execute('INSERT INTO workload_intents VALUES(?,?,?,?,?,?,?)',(x['id'],x['tenant'],x['region'],x['role'],'active',1,None))
        return {'decision':'contract_accepted'}
    if op=='freeze':
        i=one(db,'SELECT * FROM workload_intents WHERE id=?',(x['intent'],))
        if not i or x['revision']<=i['revision']:return {'decision':'stale_contract'}
        db.execute('UPDATE workload_intents SET status=?,revision=? WHERE id=?',('frozen' if x['frozen'] else 'active',x['revision'],x['intent']))
        return {'decision':'frozen' if x['frozen'] else 'active'}
    if op=='revoke':
        i=one(db,'SELECT * FROM workload_intents WHERE id=?',(x['intent'],))
        if not i or x['revision']<=i['revision']:return {'decision':'stale_contract'}
        occupied=one(db,"SELECT 1 FROM placement_leases WHERE intent=? AND state='occupied' LIMIT 1",(x['intent'],))
        if occupied:return {'decision':'live_seat_blocks_revoke'}
        db.execute("UPDATE workload_intents SET status='revoked',revision=? WHERE id=?",(x['revision'],x['intent']))
        db.execute("UPDATE placement_leases SET state='released' WHERE intent=? AND state='reserved'",(x['intent'],))
        db.execute("UPDATE capacity_claims SET state='released' WHERE intent=? AND state='active'",(x['intent'],))
        return {'decision':'revoked'}
    if op=='assign_primary':
        i=one(db,'SELECT * FROM workload_intents WHERE id=?',(x['intent'],))
        g=one(db,'SELECT * FROM owner_grants WHERE id=?',(x['grant'],))
        if not i or i['status']!='active' or not g or g['intent']!=x['intent'] or g['role']!='promote' or g['expires']<x['at']:
            return {'decision':'authorization_denied'}
        if i['primary_cluster'] is not None:
            return {'decision':'handoff_required'}
        lease=one(db,"SELECT * FROM placement_leases WHERE intent=? AND cluster=? AND state='occupied'",(x['intent'],x['cluster']))
        if not lease:return {'decision':'no_occupied_seat'}
        db.execute('UPDATE workload_intents SET primary_cluster=?,revision=revision+1 WHERE id=?',(x['cluster'],x['intent']))
        return {'decision':'primary_assigned','cluster':x['cluster']}
    return {'decision':'unknown_operation'}

def run(db,x):return act(db,'intent',x,handle)
