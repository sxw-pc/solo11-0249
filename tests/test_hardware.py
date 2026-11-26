import tempfile
import unittest
from pathlib import Path

from cloudcourt import hardware
from cloudcourt.store import connect, one, replay


class HardwareBaseline(unittest.TestCase):
    def test_lot_install_and_repair_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'hardware.sqlite')
            db = connect(path)
            self.assertEqual(hardware.run(db, {'event_id': 'lot', 'op': 'lot', 'id': 'l',
                                               'vendor': 'v', 'model': 'm', 'revision': 1})['decision'], 'lot_recorded')
            self.assertEqual(hardware.run(db, {'event_id': 'install', 'op': 'install', 'serial': 's',
                                               'lot': 'l', 'cluster': 'c', 'at': 1})['decision'], 'installed')
            self.assertEqual(hardware.run(db, {'event_id': 'repair', 'op': 'repair', 'id': 'r',
                                               'serial': 's', 'at': 2})['decision'], 'repair_opened')
            self.assertEqual(one(db, 'SELECT cluster FROM repair_orders WHERE id=?', ('r',))['cluster'], 'c')
            self.assertTrue(replay(path)['verified'])
            db.close()
