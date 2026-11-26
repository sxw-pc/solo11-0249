import tempfile
import unittest
from pathlib import Path

from cloudcourt import insurance
from cloudcourt.store import connect, replay


class InsuranceBaseline(unittest.TestCase):
    def test_claim_assessment_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'insurance.sqlite')
            db = connect(path)
            self.assertEqual(insurance.run(db, {'event_id': 'p', 'op': 'policy', 'id': 'p',
                                                'tenant': 't', 'limit': 100, 'deductible': 10,
                                                'expires': 50})['decision'], 'coverage_recorded')
            self.assertEqual(insurance.run(db, {'event_id': 'c', 'op': 'claim', 'id': 'c',
                                                'policy': 'p', 'incident': 'i', 'amount': 40,
                                                'at': 1})['decision'], 'claim_filed')
            self.assertEqual(insurance.run(db, {'event_id': 'a', 'op': 'assess', 'id': 'a',
                                                'claim': 'c'})['amount'], 30)
            self.assertTrue(replay(path)['verified'])
            db.close()
