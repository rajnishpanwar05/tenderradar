import unittest

from intelligence.rag_validation import validate_chat_payload


class RagGroundingTest(unittest.TestCase):
    def test_invalid_payload_falls_back(self):
        out = validate_chat_payload("not-json", max_source_idx=5)
        self.assertEqual(out["citations"], [])
        self.assertIn("Insufficient grounded evidence", out["answer"])

    def test_citation_bounds_enforced(self):
        raw = '{"answer":"ok","citations":[1,2,9,-1,2]}'
        out = validate_chat_payload(raw, max_source_idx=3)
        self.assertEqual(out["citations"], [1, 2])


if __name__ == "__main__":
    unittest.main()
