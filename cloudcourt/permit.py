from .store import act, one

SCHEMA = '''
CREATE TABLE IF NOT EXISTS maintenance_windows(id TEXT PRIMARY KEY, cluster TEXT, starts INTEGER, ends INTEGER, supervisor TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS work_permits(id TEXT PRIMARY KEY, window_id TEXT, worker TEXT, equipment TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS work_checkins(id TEXT PRIMARY KEY, permit TEXT, at INTEGER, kind TEXT);
'''

def handle(db, x):
    if x['op'] == 'window':
        if x['ends'] <= x['starts']:
            return {'decision': 'invalid_window'}
        db.execute('INSERT INTO maintenance_windows VALUES(?,?,?,?,?,?)',
                   (x['id'], x['cluster'], x['starts'], x['ends'], x['supervisor'], 'open'))
        return {'decision': 'window_recorded'}
    if x['op'] == 'permit':
        window = one(db, 'SELECT * FROM maintenance_windows WHERE id=?', (x['window'],))
        if not window or window['state'] != 'open':
            return {'decision': 'window_unavailable'}
        db.execute('INSERT INTO work_permits VALUES(?,?,?,?,?)',
                   (x['id'], x['window'], x['worker'], x['equipment'], 'issued'))
        return {'decision': 'permit_issued'}
    if x['op'] == 'checkin':
        permit = one(db, 'SELECT * FROM work_permits WHERE id=?', (x['permit'],))
        if not permit or permit['state'] != 'issued':
            return {'decision': 'permit_unavailable'}
        db.execute('INSERT INTO work_checkins VALUES(?,?,?,?)',
                   (x['id'], x['permit'], x['at'], x['kind']))
        return {'decision': 'checkin_recorded'}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'permit', x, handle)
