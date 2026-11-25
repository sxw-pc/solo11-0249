from .store import act,one,many
SCHEMA='''
CREATE TABLE IF NOT EXISTS cluster_inventory(id TEXT PRIMARY KEY,region TEXT,domain TEXT,cpu INTEGER,mem INTEGER,generation INTEGER,healthy INTEGER);
CREATE TABLE IF NOT EXISTS capacity_claims(id TEXT PRIMARY KEY,cluster TEXT,intent TEXT,cpu INTEGER,mem INTEGER,state TEXT);
CREATE TABLE IF NOT EXISTS placement_leases(id TEXT PRIMARY KEY,intent TEXT,cluster TEXT,role TEXT,expires INTEGER,generation INTEGER,state TEXT);
'''

def handle(db,x):
    op=x['op']
    if op=='observe':
        old=one(db,'SELECT * FROM cluster_inventory WHERE id=?',(x['id'],))
        if old and x['generation']<=old['generation']:return {'decision':'stale_inventory'}
        if min(x['cpu'],x['mem'])<=0:return {'decision':'invalid_capacity'}
        db.execute('INSERT INTO cluster_inventory VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET region=excluded.region,domain=excluded.domain,cpu=excluded.cpu,mem=excluded.mem,generation=excluded.generation,healthy=excluded.healthy',(x['id'],x['region'],x['domain'],x['cpu'],x['mem'],x['generation'],int(x['healthy'])))
        return {'decision':'observed'}
    if op=='reserve':
        if x['role'] not in ('primary','backup'):return {'decision':'invalid_placement_role'}
        if x['expires']<=x.get('at',0):return {'decision':'invalid_lease_window'}
        i=one(db,'SELECT * FROM workload_intents WHERE id=?',(x['intent'],))
        c=one(db,'SELECT * FROM cluster_inventory WHERE id=?',(x['cluster'],))
        if not i or i['status']!='active' or not c or not c['healthy'] or c['region']!=i['region']:
            return {'decision':'ineligible_cluster'}
        siblings=many(db,"SELECT c.domain FROM placement_leases l JOIN cluster_inventory c ON c.id=l.cluster WHERE l.intent=? AND l.state IN ('reserved','occupied')",(x['intent'],))
        if any(s['domain']==c['domain'] for s in siblings):return {'decision':'same_disaster_domain'}
        if one(db,"SELECT 1 FROM placement_leases WHERE intent=? AND role=? AND state IN ('reserved','occupied')",(x['intent'],x['role'])):
            return {'decision':'role_already_committed'}
        used=one(db,"SELECT COALESCE(SUM(cpu),0) cpu,COALESCE(SUM(mem),0) mem FROM capacity_claims WHERE cluster=? AND state='active'",(x['cluster'],))
        if min(x['cpu'],x['mem'])<=0 or used['cpu']+x['cpu']>c['cpu'] or used['mem']+x['mem']>c['mem']:
            return {'decision':'capacity_exhausted'}
        db.execute('INSERT INTO capacity_claims VALUES(?,?,?,?,?,?)',(x['id'],x['cluster'],x['intent'],x['cpu'],x['mem'],'active'))
        db.execute('INSERT INTO placement_leases VALUES(?,?,?,?,?,?,?)',(x['id'],x['intent'],x['cluster'],x['role'],x['expires'],c['generation'],'reserved'))
        return {'decision':'reserved'}
    if op=='occupy':
        l=one(db,'SELECT * FROM placement_leases WHERE id=?',(x['id'],))
        c=one(db,'SELECT * FROM cluster_inventory WHERE id=?',(l['cluster'],)) if l else None
        if not l or l['state']!='reserved' or l['expires']<x['at'] or not c or c['generation']!=l['generation'] or not c['healthy']:
            return {'decision':'stale_lease'}
        db.execute("UPDATE placement_leases SET state='occupied' WHERE id=?",(x['id'],))
        return {'decision':'occupied'}
    if op=='expire':
        l=one(db,'SELECT * FROM placement_leases WHERE id=?',(x['id'],))
        if not l or l['state']!='reserved' or l['expires']>=x['at']:return {'decision':'not_expirable'}
        db.execute("UPDATE placement_leases SET state='expired' WHERE id=?",(x['id'],))
        db.execute("UPDATE capacity_claims SET state='released' WHERE id=?",(x['id'],))
        return {'decision':'expired'}
    return {'decision':'unknown_operation'}

def run(db,x):return act(db,'seat',x,handle)
