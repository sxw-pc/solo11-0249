import tempfile
import unittest
from pathlib import Path

from cloudcourt import permit
from cloudcourt.store import connect, replay


class PermitBaseline(unittest.TestCase):
    def test_window_permit_and_checkin_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'permit.sqlite')
            db = connect(path)
            self.assertEqual(permit.run(db, {'event_id': 'w', 'op': 'window', 'id': 'w',
                                             'cluster': 'c', 'starts': 1, 'ends': 10,
                                             'supervisor': 's'})['decision'], 'window_recorded')
            self.assertEqual(permit.run(db, {'event_id': 'p', 'op': 'permit', 'id': 'p',
                                             'window': 'w', 'worker': 'a',
                                             'equipment': 'e'})['decision'], 'permit_issued')
            self.assertEqual(permit.run(db, {'event_id': 'c', 'op': 'checkin', 'id': 'c',
                                             'permit': 'p', 'at': 2,
                                             'kind': 'entered'})['decision'], 'checkin_recorded')
            self.assertTrue(replay(path)['verified'])
            db.close()
