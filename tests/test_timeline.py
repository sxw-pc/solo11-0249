import tempfile
import unittest
from pathlib import Path

from cloudcourt import timeline
from cloudcourt.store import connect, replay


class TimelineBaseline(unittest.TestCase):
    def test_sample_and_provisional_finding_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'timeline.sqlite')
            db = connect(path)
            self.assertEqual(timeline.run(db, {'event_id': 's', 'op': 'sample', 'id': 's',
                                               'cluster': 'c', 'sensor': 'n', 'at': 5,
                                               'signal': 'link_down', 'value': 1})['decision'], 'sample_recorded')
            result = timeline.run(db, {'event_id': 'f', 'op': 'finding', 'id': 'f',
                                       'incident': 'i', 'cluster': 'c', 'cause': 'link_down'})
            self.assertEqual(result['first_seen'], 5)
            self.assertTrue(replay(path)['verified'])
            db.close()
