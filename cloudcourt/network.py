from .store import act, one

SCHEMA = '''
CREATE TABLE IF NOT EXISTS network_paths(id TEXT PRIMARY KEY, source TEXT, target TEXT, segment TEXT, bandwidth INTEGER, revision INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS path_bookings(id TEXT PRIMARY KEY, path_id TEXT, intent TEXT, bandwidth INTEGER, state TEXT);
'''

def handle(db, x):
    if x['op'] == 'path':
        if x['bandwidth'] < 1:
            return {'decision': 'invalid_bandwidth'}
        db.execute('INSERT INTO network_paths VALUES(?,?,?,?,?,?,?)',
                   (x['id'], x['source'], x['target'], x['segment'], x['bandwidth'], x['revision'], 'active'))
        return {'decision': 'path_recorded'}
    if x['op'] == 'book':
        path = one(db, 'SELECT * FROM network_paths WHERE id=?', (x['path'],))
        if not path or path['state'] != 'active' or x['bandwidth'] < 1:
            return {'decision': 'path_unavailable'}
        used = one(db, "SELECT COALESCE(SUM(bandwidth),0) n FROM path_bookings WHERE path_id=? AND state='active'", (x['path'],))['n']
        if used + x['bandwidth'] > path['bandwidth']:
            return {'decision': 'bandwidth_exhausted'}
        db.execute('INSERT INTO path_bookings VALUES(?,?,?,?,?)',
                   (x['id'], x['path'], x['intent'], x['bandwidth'], 'active'))
        return {'decision': 'path_booked'}
    if x['op'] == 'release':
        booking = one(db, 'SELECT * FROM path_bookings WHERE id=?', (x['id'],))
        if not booking or booking['state'] != 'active':
            return {'decision': 'not_booked'}
        db.execute("UPDATE path_bookings SET state='released' WHERE id=?", (x['id'],))
        return {'decision': 'released'}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'network', x, handle)
