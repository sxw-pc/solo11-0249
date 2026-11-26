from .store import act, one

SCHEMA = '''
CREATE TABLE IF NOT EXISTS hardware_lots(id TEXT PRIMARY KEY, vendor TEXT, model TEXT, revision INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS hardware_units(serial TEXT PRIMARY KEY, lot TEXT, cluster TEXT, installed_at INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS repair_orders(id TEXT PRIMARY KEY, serial TEXT, cluster TEXT, opened_at INTEGER, state TEXT);
'''

def handle(db, x):
    if x['op'] == 'lot':
        db.execute('INSERT INTO hardware_lots VALUES(?,?,?,?,?)',
                   (x['id'], x['vendor'], x['model'], x['revision'], 'accepted'))
        return {'decision': 'lot_recorded'}
    if x['op'] == 'install':
        lot = one(db, 'SELECT * FROM hardware_lots WHERE id=?', (x['lot'],))
        if not lot or lot['state'] != 'accepted':
            return {'decision': 'lot_unavailable'}
        db.execute('INSERT INTO hardware_units VALUES(?,?,?,?,?)',
                   (x['serial'], x['lot'], x['cluster'], x['at'], 'installed'))
        return {'decision': 'installed'}
    if x['op'] == 'repair':
        unit = one(db, 'SELECT * FROM hardware_units WHERE serial=?', (x['serial'],))
        if not unit or unit['state'] != 'installed':
            return {'decision': 'unit_unavailable'}
        db.execute('INSERT INTO repair_orders VALUES(?,?,?,?,?)',
                   (x['id'], x['serial'], unit['cluster'], x['at'], 'open'))
        return {'decision': 'repair_opened'}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'hardware', x, handle)
