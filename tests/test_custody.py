import tempfile
import unittest
from pathlib import Path

from cloudcourt import custody
from cloudcourt.store import connect, replay


class CustodyBaseline(unittest.TestCase):
    def test_votes_and_release_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'keys.sqlite')
            db = connect(path)
            self.assertEqual(custody.run(db, {'event_id': 'k', 'op': 'key', 'id': 'k',
                                              'tenant': 't', 'intent': 'w', 'epoch': 1})['decision'], 'key_sealed')
            self.assertEqual(custody.run(db, {'event_id': 'v1', 'op': 'vote', 'id': 'v1',
                                              'key': 'k', 'actor': 'alice', 'at': 1})['decision'], 'vote_recorded')
            self.assertEqual(custody.run(db, {'event_id': 'early', 'op': 'release', 'id': 'r1',
                                              'key': 'k', 'cluster': 'c', 'at': 2})['decision'], 'approval_incomplete')
            custody.run(db, {'event_id': 'v2', 'op': 'vote', 'id': 'v2',
                             'key': 'k', 'actor': 'bob', 'at': 3})
            self.assertEqual(custody.run(db, {'event_id': 'release', 'op': 'release', 'id': 'r2',
                                              'key': 'k', 'cluster': 'c', 'at': 4})['decision'], 'key_released')
            self.assertTrue(replay(path)['verified'])
            db.close()
