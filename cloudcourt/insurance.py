from .store import act, one

SCHEMA = '''
CREATE TABLE IF NOT EXISTS coverage_policies(id TEXT PRIMARY KEY, tenant TEXT, limit_amount INTEGER, deductible INTEGER, expires INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS loss_claims(id TEXT PRIMARY KEY, policy TEXT, incident TEXT, asserted_amount INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS indemnity_orders(id TEXT PRIMARY KEY, claim TEXT, approved_amount INTEGER, state TEXT);
'''

def handle(db, x):
    if x['op'] == 'policy':
        if x['limit'] <= x['deductible'] or x['deductible'] < 0:
            return {'decision': 'invalid_coverage'}
        db.execute('INSERT INTO coverage_policies VALUES(?,?,?,?,?,?)',
                   (x['id'], x['tenant'], x['limit'], x['deductible'], x['expires'], 'active'))
        return {'decision': 'coverage_recorded'}
    if x['op'] == 'claim':
        policy = one(db, 'SELECT * FROM coverage_policies WHERE id=?', (x['policy'],))
        if not policy or policy['state'] != 'active' or policy['expires'] < x['at']:
            return {'decision': 'policy_unavailable'}
        db.execute('INSERT INTO loss_claims VALUES(?,?,?,?,?)',
                   (x['id'], x['policy'], x['incident'], x['amount'], 'filed'))
        return {'decision': 'claim_filed'}
    if x['op'] == 'assess':
        claim = one(db, 'SELECT * FROM loss_claims WHERE id=?', (x['claim'],))
        if not claim or claim['state'] != 'filed':
            return {'decision': 'claim_unavailable'}
        policy = one(db, 'SELECT * FROM coverage_policies WHERE id=?', (claim['policy'],))
        amount = min(policy['limit_amount'], max(0, claim['asserted_amount'] - policy['deductible']))
        db.execute('INSERT INTO indemnity_orders VALUES(?,?,?,?)',
                   (x['id'], x['claim'], amount, 'provisional'))
        return {'decision': 'provisional_assessment', 'amount': amount}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'insurance', x, handle)
