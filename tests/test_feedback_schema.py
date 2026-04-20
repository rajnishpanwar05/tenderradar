import unittest
import importlib.util
import os

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SCHEMAS_PATH = os.path.join(_ROOT, "api", "schemas.py")
_spec = importlib.util.spec_from_file_location("api_schemas_direct", _SCHEMAS_PATH)
_mod = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_mod)
OutcomeRequest = _mod.OutcomeRequest


class FeedbackSchemaTest(unittest.TestCase):
    def test_accepts_controlled_values(self):
        req = OutcomeRequest(tender_id="T1", outcome="won", bid_decision="bid")
        self.assertEqual(req.outcome, "won")
        self.assertEqual(req.bid_decision, "bid")

    def test_aliases_normalize(self):
        req = OutcomeRequest(tender_id="T2", outcome="No Submission", bid_decision="Review Later")
        self.assertEqual(req.outcome, "no_submission")
        self.assertEqual(req.bid_decision, "review_later")

    def test_rejects_invalid_combo(self):
        with self.assertRaises(Exception):
            OutcomeRequest(tender_id="T3", outcome="won", bid_decision="no_bid")


if __name__ == "__main__":
    unittest.main()
