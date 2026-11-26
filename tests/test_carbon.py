import tempfile
import unittest
from pathlib import Path

from cloudcourt import carbon
from cloudcourt.store import connect, one, replay


class CarbonBaseline(unittest.TestCase):
    def test_order_reading_and_draft_statement_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'carbon.sqlite')
            db = connect(path)
            self.assertEqual(carbon.run(db, {'event_id': 'o', 'op': 'order', 'id': 'order-a',
                                             'tenant': 'tenant-a', 'period': '2026-11',
                                             'ceiling': 20})['decision'], 'ordered')
            self.assertEqual(carbon.run(db, {'event_id': 'r', 'op': 'reading', 'id': 'meter-a',
                                             'cluster': 'cluster-a', 'period': '2026-11',
                                             'units': 7, 'source': 'grid-a',
                                             'revision': 1})['decision'], 'reading_recorded')
            self.assertEqual(carbon.run(db, {'event_id': 's', 'op': 'statement', 'id': 'stmt-a',
                                             'order': 'order-a'})['units'], 7)
            self.assertEqual(one(db, 'SELECT state FROM carbon_statements WHERE id=?',
                                 ('stmt-a',))['state'], 'draft')
            self.assertTrue(replay(path)['verified'])
            db.close()
