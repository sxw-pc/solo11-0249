from .store import act, one, many

SCHEMA = '''
CREATE TABLE IF NOT EXISTS fault_samples(id TEXT PRIMARY KEY, cluster TEXT, sensor TEXT, local_at INTEGER, signal TEXT, value INTEGER);
CREATE TABLE IF NOT EXISTS fault_findings(id TEXT PRIMARY KEY, incident TEXT, cause TEXT, first_seen INTEGER, state TEXT);
'''

def handle(db, x):
    if x['op'] == 'sample':
        db.execute('INSERT INTO fault_samples VALUES(?,?,?,?,?,?)',
                   (x['id'], x['cluster'], x['sensor'], x['at'], x['signal'], x['value']))
        return {'decision': 'sample_recorded'}
    if x['op'] == 'finding':
        samples = many(db, 'SELECT local_at FROM fault_samples WHERE cluster=? AND signal=? ORDER BY local_at',
                       (x['cluster'], x['cause']))
        if not samples:
            return {'decision': 'cause_unobserved'}
        db.execute('INSERT INTO fault_findings VALUES(?,?,?,?,?)',
                   (x['id'], x['incident'], x['cause'], samples[0]['local_at'], 'provisional'))
        return {'decision': 'provisional_finding', 'first_seen': samples[0]['local_at']}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'timeline', x, handle)
