import tempfile
import unittest
from pathlib import Path

from cloudcourt import notice
from cloudcourt.store import connect, one, replay


class NoticeBaseline(unittest.TestCase):
    def test_route_notice_and_delivery_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'notice.sqlite')
            db = connect(path)
            self.assertEqual(notice.run(db, {'event_id': 'route', 'op': 'route', 'id': 'r',
                                             'tenant': 't', 'channel': 'email', 'address': 'ops@example.test',
                                             'revision': 1})['decision'], 'route_recorded')
            self.assertEqual(notice.run(db, {'event_id': 'notice', 'op': 'notice', 'id': 'n',
                                             'incident': 'i', 'tenant': 't', 'route': 'r',
                                             'summary': 'service interruption'})['decision'], 'notice_queued')
            self.assertEqual(notice.run(db, {'event_id': 'receipt', 'op': 'receipt', 'id': 'd',
                                             'notice': 'n', 'delivered': True,
                                             'at': 5})['decision'], 'receipt_recorded')
            self.assertEqual(one(db, 'SELECT state FROM incident_notices WHERE id=?', ('n',))['state'], 'delivered')
            self.assertTrue(replay(path)['verified'])
            db.close()
