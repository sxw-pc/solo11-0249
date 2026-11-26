import tempfile
import unittest
from pathlib import Path

from cloudcourt import network
from cloudcourt.store import connect, replay


class NetworkBaseline(unittest.TestCase):
    def test_path_booking_and_capacity_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'network.sqlite')
            db = connect(path)
            self.assertEqual(network.run(db, {'event_id': 'p', 'op': 'path', 'id': 'p',
                                              'source': 'c0', 'target': 'c1', 'segment': 's',
                                              'bandwidth': 5, 'revision': 1})['decision'], 'path_recorded')
            self.assertEqual(network.run(db, {'event_id': 'b1', 'op': 'book', 'id': 'b1',
                                              'path': 'p', 'intent': 'w1', 'bandwidth': 4})['decision'], 'path_booked')
            self.assertEqual(network.run(db, {'event_id': 'b2', 'op': 'book', 'id': 'b2',
                                              'path': 'p', 'intent': 'w2', 'bandwidth': 2})['decision'], 'bandwidth_exhausted')
            self.assertEqual(network.run(db, {'event_id': 'r', 'op': 'release',
                                              'id': 'b1'})['decision'], 'released')
            self.assertTrue(replay(path)['verified'])
            db.close()
