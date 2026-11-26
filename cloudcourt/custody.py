from .store import act, one

SCHEMA = '''
CREATE TABLE IF NOT EXISTS key_epochs(id TEXT PRIMARY KEY, tenant TEXT, intent TEXT, epoch INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS custodian_votes(id TEXT PRIMARY KEY, key_id TEXT, actor TEXT, at INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS key_releases(id TEXT PRIMARY KEY, key_id TEXT, cluster TEXT, at INTEGER, state TEXT);
'''

def handle(db, x):
    if x['op'] == 'key':
        db.execute('INSERT INTO key_epochs VALUES(?,?,?,?,?)',
                   (x['id'], x['tenant'], x['intent'], x['epoch'], 'sealed'))
        return {'decision': 'key_sealed'}
    if x['op'] == 'vote':
        key = one(db, 'SELECT * FROM key_epochs WHERE id=?', (x['key'],))
        if not key or key['state'] != 'sealed':
            return {'decision': 'key_unavailable'}
        db.execute('INSERT INTO custodian_votes VALUES(?,?,?,?,?)',
                   (x['id'], x['key'], x['actor'], x['at'], 'accepted'))
        return {'decision': 'vote_recorded'}
    if x['op'] == 'release':
        key = one(db, 'SELECT * FROM key_epochs WHERE id=?', (x['key'],))
        votes = one(db, "SELECT COUNT(*) n FROM custodian_votes WHERE key_id=? AND state='accepted'", (x['key'],))['n']
        if not key or votes < 2:
            return {'decision': 'approval_incomplete'}
        db.execute('INSERT INTO key_releases VALUES(?,?,?,?,?)',
                   (x['id'], x['key'], x['cluster'], x['at'], 'released'))
        return {'decision': 'key_released'}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'custody', x, handle)
