from .store import act, one

SCHEMA = '''
CREATE TABLE IF NOT EXISTS carbon_orders(id TEXT PRIMARY KEY, tenant TEXT, period TEXT, ceiling INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS power_readings(id TEXT PRIMARY KEY, cluster TEXT, period TEXT, units INTEGER, source TEXT, revision INTEGER);
CREATE TABLE IF NOT EXISTS carbon_statements(id TEXT PRIMARY KEY, order_id TEXT, period TEXT, measured_units INTEGER, state TEXT);
'''

def handle(db, x):
    if x['op'] == 'order':
        if x['ceiling'] < 0:
            return {'decision': 'invalid_ceiling'}
        db.execute('INSERT INTO carbon_orders VALUES(?,?,?,?,?)',
                   (x['id'], x['tenant'], x['period'], x['ceiling'], 'open'))
        return {'decision': 'ordered'}
    if x['op'] == 'reading':
        if x['units'] < 0:
            return {'decision': 'invalid_reading'}
        db.execute('INSERT INTO power_readings VALUES(?,?,?,?,?,?)',
                   (x['id'], x['cluster'], x['period'], x['units'], x['source'], x['revision']))
        return {'decision': 'reading_recorded'}
    if x['op'] == 'statement':
        order = one(db, 'SELECT * FROM carbon_orders WHERE id=?', (x['order'],))
        if not order or order['state'] != 'open':
            return {'decision': 'order_unavailable'}
        measured = one(db, 'SELECT COALESCE(SUM(units),0) units FROM power_readings WHERE period=?', (order['period'],))
        db.execute('INSERT INTO carbon_statements VALUES(?,?,?,?,?)',
                   (x['id'], order['id'], order['period'], measured['units'], 'draft'))
        return {'decision': 'drafted', 'units': measured['units']}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'carbon', x, handle)
