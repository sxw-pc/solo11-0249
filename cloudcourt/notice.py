from .store import act, one

SCHEMA = '''
CREATE TABLE IF NOT EXISTS contact_routes(id TEXT PRIMARY KEY, tenant TEXT, channel TEXT, address TEXT, revision INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS incident_notices(id TEXT PRIMARY KEY, incident TEXT, tenant TEXT, route TEXT, summary TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS delivery_receipts(id TEXT PRIMARY KEY, notice TEXT, delivered INTEGER, at INTEGER);
'''

def handle(db, x):
    if x['op'] == 'route':
        db.execute('INSERT INTO contact_routes VALUES(?,?,?,?,?,?)',
                   (x['id'], x['tenant'], x['channel'], x['address'], x['revision'], 'active'))
        return {'decision': 'route_recorded'}
    if x['op'] == 'notice':
        route = one(db, 'SELECT * FROM contact_routes WHERE id=?', (x['route'],))
        if not route or route['state'] != 'active' or route['tenant'] != x['tenant']:
            return {'decision': 'route_unavailable'}
        db.execute('INSERT INTO incident_notices VALUES(?,?,?,?,?,?)',
                   (x['id'], x['incident'], x['tenant'], x['route'], x['summary'], 'queued'))
        return {'decision': 'notice_queued'}
    if x['op'] == 'receipt':
        notice = one(db, 'SELECT * FROM incident_notices WHERE id=?', (x['notice'],))
        if not notice:
            return {'decision': 'notice_missing'}
        db.execute('INSERT INTO delivery_receipts VALUES(?,?,?,?)',
                   (x['id'], x['notice'], int(x['delivered']), x['at']))
        db.execute('UPDATE incident_notices SET state=? WHERE id=?',
                   ('delivered' if x['delivered'] else 'failed', x['notice']))
        return {'decision': 'receipt_recorded'}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'notice', x, handle)
