from .store import act, one, many

SCHEMA = '''
CREATE TABLE IF NOT EXISTS hardware_lots(id TEXT PRIMARY KEY, vendor TEXT, model TEXT, revision INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS hardware_units(serial TEXT PRIMARY KEY, lot TEXT, cluster TEXT, installed_at INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS repair_orders(id TEXT PRIMARY KEY, serial TEXT, cluster TEXT, opened_at INTEGER, state TEXT, restored_at INTEGER);
CREATE TABLE IF NOT EXISTS recall_notices(id TEXT PRIMARY KEY, lot TEXT, failure_mode TEXT, announced_at INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS vendor_attestations(serial TEXT PRIMARY KEY, lot TEXT, seal TEXT, batch TEXT, at INTEGER);
CREATE TABLE IF NOT EXISTS custody_events(id TEXT PRIMARY KEY, serial TEXT, kind TEXT, actor TEXT, witness TEXT, at INTEGER, detail TEXT);
CREATE TABLE IF NOT EXISTS transport_legs(id TEXT PRIMARY KEY, serial TEXT, leg TEXT, from_party TEXT, to_party TEXT, at INTEGER, integrity TEXT);
CREATE TABLE IF NOT EXISTS replacement_acceptances(id TEXT PRIMARY KEY, repair TEXT, serial TEXT, cluster TEXT, cert TEXT, at INTEGER);
CREATE TABLE IF NOT EXISTS vendor_acks(id TEXT PRIMARY KEY, serial TEXT, repair TEXT, at INTEGER, note TEXT);
CREATE TABLE IF NOT EXISTS settlements(id TEXT PRIMARY KEY, repair TEXT, path TEXT, repair_cost INTEGER, warranty_waiver INTEGER, downtime_loss INTEGER, spare_cost INTEGER, vendor_payable INTEGER, at INTEGER);
'''

LEG_ORDER = ['outbound', 'carrier', 'center_receipt', 'return']
SETTLEMENT_PATHS = ('onsite_spare', 'return_to_vendor')
KEY_DISPOSITIONS = ('wiped', 'sealed')


def custody(db, x, serial, kind, actor=None, witness=None, detail=None):
    db.execute('INSERT INTO custody_events VALUES(?,?,?,?,?,?,?)',
               (x['event_id'], serial, kind, actor, witness, x.get('at'), detail))


def legs(db, serial):
    return {row['leg']: row for row in many(db, 'SELECT * FROM transport_legs WHERE serial=?', (serial,))}


def chain_gap(db, serial, through='center_receipt'):
    have = legs(db, serial)
    for leg in LEG_ORDER[:LEG_ORDER.index(through) + 1]:
        if leg not in have:
            return leg
    return None


def removed_at(db, serial):
    row = one(db, "SELECT at FROM custody_events WHERE serial=? AND kind='remove'", (serial,))
    return row['at'] if row else None


def handle(db, x):
    op = x['op']
    if op == 'lot':
        db.execute('INSERT INTO hardware_lots VALUES(?,?,?,?,?)',
                   (x['id'], x['vendor'], x['model'], x['revision'], 'accepted'))
        return {'decision': 'lot_recorded'}
    if op == 'attest':
        if not one(db, 'SELECT 1 FROM hardware_lots WHERE id=?', (x['lot'],)):
            return {'decision': 'lot_unavailable'}
        if not x.get('seal') or not x.get('batch'):
            return {'decision': 'invalid_attestation'}
        old = one(db, 'SELECT * FROM vendor_attestations WHERE serial=?', (x['serial'],))
        if old:
            if (old['lot'], old['seal'], old['batch']) != (x['lot'], x['seal'], x['batch']):
                return {'decision': 'attestation_conflict'}
            return {'decision': 'attested'}
        db.execute('INSERT INTO vendor_attestations VALUES(?,?,?,?,?)',
                   (x['serial'], x['lot'], x['seal'], x['batch'], x['at']))
        return {'decision': 'attested'}
    if op == 'intake':
        if not one(db, 'SELECT 1 FROM vendor_attestations WHERE serial=?', (x['serial'],)):
            return {'decision': 'unattested_serial'}
        if one(db, "SELECT 1 FROM custody_events WHERE serial=? AND kind='intake'", (x['serial'],)):
            return {'decision': 'already_in_stock'}
        custody(db, x, x['serial'], 'intake', actor=x.get('custodian'))
        return {'decision': 'stocked'}
    if op == 'install':
        lot = one(db, 'SELECT * FROM hardware_lots WHERE id=?', (x['lot'],))
        if not lot or lot['state'] != 'accepted':
            return {'decision': 'lot_unavailable'}
        if one(db, "SELECT 1 FROM recall_notices WHERE lot=? AND state='active'", (x['lot'],)):
            return {'decision': 'lot_recalled'}
        unit = one(db, 'SELECT * FROM hardware_units WHERE serial=?', (x['serial'],))
        if unit and unit['state'] == 'installed':
            return {'decision': 'serial_already_installed', 'cluster': unit['cluster']}
        if unit:
            if unit['lot'] != x['lot']:
                return {'decision': 'lot_mismatch'}
            have = legs(db, x['serial'])
            if 'outbound' in have and 'return' not in have:
                return {'decision': 'part_in_transit'}
            db.execute("UPDATE hardware_units SET cluster=?, installed_at=?, state='installed' WHERE serial=?",
                       (x['cluster'], x['at'], x['serial']))
        else:
            db.execute('INSERT INTO hardware_units VALUES(?,?,?,?,?)',
                       (x['serial'], x['lot'], x['cluster'], x['at'], 'installed'))
        custody(db, x, x['serial'], 'install', actor=x.get('custodian'))
        return {'decision': 'installed'}
    if op == 'repair':
        unit = one(db, 'SELECT * FROM hardware_units WHERE serial=?', (x['serial'],))
        if not unit or unit['state'] != 'installed':
            return {'decision': 'unit_unavailable'}
        db.execute('INSERT INTO repair_orders(id,serial,cluster,opened_at,state) VALUES(?,?,?,?,?)',
                   (x['id'], x['serial'], unit['cluster'], x['at'], 'open'))
        return {'decision': 'repair_opened'}
    if op == 'recall':
        if not one(db, 'SELECT 1 FROM hardware_lots WHERE id=?', (x['lot'],)):
            return {'decision': 'lot_unavailable'}
        if not x.get('failure_mode'):
            return {'decision': 'invalid_recall'}
        db.execute('INSERT INTO recall_notices VALUES(?,?,?,?,?)',
                   (x['id'], x['lot'], x['failure_mode'], x['announced_at'], 'active'))
        affected = one(db, "SELECT COUNT(*) n FROM hardware_units WHERE lot=? AND state='installed'",
                       (x['lot'],))['n']
        return {'decision': 'recall_published', 'affected': affected}
    if op == 'remove':
        unit = one(db, 'SELECT * FROM hardware_units WHERE serial=?', (x['serial'],))
        if not unit or unit['state'] != 'installed':
            return {'decision': 'unit_not_installed'}
        repair = one(db, "SELECT * FROM repair_orders WHERE serial=? AND state='open'", (x['serial'],))
        if not repair:
            return {'decision': 'no_open_repair'}
        if not x.get('witness'):
            return {'decision': 'witness_required'}
        if x['witness'] == x.get('custodian'):
            return {'decision': 'witness_not_independent'}
        if x.get('key_disposition') not in KEY_DISPOSITIONS:
            return {'decision': 'key_residue_unhandled'}
        if x['at'] < unit['installed_at']:
            return {'decision': 'time_regression'}
        db.execute("UPDATE hardware_units SET state='removed', cluster=NULL WHERE serial=?", (x['serial'],))
        custody(db, x, x['serial'], 'remove', actor=x.get('custodian'), witness=x['witness'],
                detail=x['key_disposition'])
        return {'decision': 'removed_witnessed', 'repair': repair['id']}
    if op == 'transport':
        leg = x.get('leg')
        if leg not in LEG_ORDER:
            return {'decision': 'invalid_leg'}
        unit = one(db, 'SELECT * FROM hardware_units WHERE serial=?', (x['serial'],))
        if not unit:
            return {'decision': 'unit_unavailable'}
        have = legs(db, x['serial'])
        if leg in have:
            return {'decision': 'leg_already_recorded'}
        if leg == 'outbound':
            if unit['state'] != 'removed':
                return {'decision': 'unit_not_removed'}
            gone = removed_at(db, x['serial'])
            if gone is not None and x['at'] < gone:
                return {'decision': 'time_regression'}
        else:
            missing = next((step for step in LEG_ORDER[:LEG_ORDER.index(leg)] if step not in have), None)
            if missing:
                return {'decision': 'handoff_gap', 'missing': missing}
            prev = LEG_ORDER[LEG_ORDER.index(leg) - 1]
            if x['at'] < have[prev]['at']:
                return {'decision': 'time_regression'}
        if not x.get('integrity'):
            return {'decision': 'integrity_missing'}
        db.execute('INSERT INTO transport_legs VALUES(?,?,?,?,?,?,?)',
                   (x['id'], x['serial'], leg, x.get('from'), x.get('to'), x['at'], x['integrity']))
        return {'decision': 'leg_recorded', 'leg': leg}
    if op == 'vendor_ack':
        repair = one(db, 'SELECT * FROM repair_orders WHERE id=?', (x['repair'],))
        if not repair or repair['serial'] != x['serial']:
            return {'decision': 'repair_unavailable'}
        gap = chain_gap(db, x['serial'])
        if gap:
            return {'decision': 'handoff_gap', 'missing': gap}
        db.execute('INSERT INTO vendor_acks VALUES(?,?,?,?,?)',
                   (x['id'], x['serial'], x['repair'], x['at'], x.get('note')))
        return {'decision': 'vendor_acknowledged', 'at': x['at']}
    if op == 'accept_replacement':
        repair = one(db, 'SELECT * FROM repair_orders WHERE id=?', (x['repair'],))
        if not repair or repair['state'] != 'open':
            return {'decision': 'repair_unavailable'}
        if one(db, 'SELECT 1 FROM replacement_acceptances WHERE repair=?', (x['repair'],)):
            return {'decision': 'already_accepted'}
        if x['serial'] == repair['serial']:
            return {'decision': 'serial_reuse'}
        att = one(db, 'SELECT * FROM vendor_attestations WHERE serial=?', (x['serial'],))
        if not att:
            return {'decision': 'unattested_serial'}
        if not one(db, "SELECT 1 FROM custody_events WHERE serial=? AND kind='intake'", (x['serial'],)):
            return {'decision': 'not_in_stock'}
        faulty = one(db, 'SELECT l.model FROM hardware_units u JOIN hardware_lots l ON l.id=u.lot WHERE u.serial=?',
                     (repair['serial'],))
        source = one(db, 'SELECT * FROM hardware_lots WHERE id=?', (att['lot'],))
        if not faulty or not source or source['model'] != faulty['model']:
            return {'decision': 'model_incompatible'}
        if one(db, "SELECT 1 FROM recall_notices WHERE lot=? AND state='active'", (att['lot'],)):
            return {'decision': 'lot_recalled'}
        if not x.get('cert'):
            return {'decision': 'quality_cert_missing'}
        clash = one(db, "SELECT cluster FROM hardware_units WHERE serial=? AND state='installed'", (x['serial'],))
        if clash:
            return {'decision': 'serial_already_installed', 'cluster': clash['cluster']}
        db.execute('INSERT INTO replacement_acceptances VALUES(?,?,?,?,?,?)',
                   (x['id'], x['repair'], x['serial'], repair['cluster'], x['cert'], x['at']))
        return {'decision': 'replacement_accepted'}
    if op == 'restore':
        repair = one(db, 'SELECT * FROM repair_orders WHERE id=?', (x['repair'],))
        if not repair:
            return {'decision': 'repair_unavailable'}
        if repair['state'] != 'open':
            return {'decision': 'repair_not_open'}
        unit = one(db, 'SELECT * FROM hardware_units WHERE serial=?', (repair['serial'],))
        if not unit or unit['state'] != 'removed':
            return {'decision': 'faulty_part_still_installed'}
        acc = one(db, 'SELECT * FROM replacement_acceptances WHERE repair=?', (repair['id'],))
        if not acc:
            return {'decision': 'replacement_not_accepted'}
        repl = one(db, 'SELECT * FROM hardware_units WHERE serial=?', (acc['serial'],))
        if not repl or repl['state'] != 'installed' or repl['cluster'] != repair['cluster']:
            return {'decision': 'replacement_not_installed'}
        gone = removed_at(db, repair['serial'])
        if gone is not None and x['at'] < gone:
            return {'decision': 'time_regression'}
        db.execute("UPDATE repair_orders SET state='restored', restored_at=? WHERE id=?", (x['at'], repair['id']))
        return {'decision': 'service_restored', 'cluster': repair['cluster'], 'restored_at': x['at']}
    if op == 'settle':
        repair = one(db, 'SELECT * FROM repair_orders WHERE id=?', (x['repair'],))
        if not repair:
            return {'decision': 'repair_unavailable'}
        if repair['state'] != 'restored':
            return {'decision': 'service_not_restored'}
        if x.get('path') not in SETTLEMENT_PATHS:
            return {'decision': 'invalid_settlement_path'}
        if one(db, 'SELECT 1 FROM settlements WHERE repair=?', (x['repair'],)):
            return {'decision': 'already_settled'}
        through = 'return' if x['path'] == 'return_to_vendor' else 'center_receipt'
        gap = chain_gap(db, repair['serial'], through=through)
        if gap:
            return {'decision': 'handoff_gap', 'missing': gap}
        downtime = repair['restored_at'] - removed_at(db, repair['serial'])
        downtime_loss = downtime * x['downtime_rate']
        warranty_waiver = x['repair_cost'] if x.get('warranty') else 0
        spare_cost = x.get('spare_cost', 0) if x['path'] == 'onsite_spare' else 0
        vendor_downtime = downtime_loss if x['path'] == 'return_to_vendor' else 0
        vendor_payable = warranty_waiver + vendor_downtime
        platform_borne = x['repair_cost'] - warranty_waiver + downtime_loss - vendor_downtime + spare_cost
        db.execute('INSERT INTO settlements VALUES(?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['repair'], x['path'], x['repair_cost'], warranty_waiver, downtime_loss,
                    spare_cost, vendor_payable, x['at']))
        return {'decision': 'settled', 'vendor_payable': vendor_payable, 'warranty_waiver': warranty_waiver,
                'downtime_loss': downtime_loss, 'downtime_hours': downtime,
                'platform_borne': platform_borne, 'restored_at': repair['restored_at']}
    return {'decision': 'unknown_operation'}


def run(db, x):
    return act(db, 'hardware', x, handle)
