import unittest

from pipeline.learning_pipeline import evaluate_ranking


class LearningMetricsTest(unittest.TestCase):
    def test_ranking_metrics_shape(self):
        rows = [
            {"outcome": "won"},
            {"outcome": "lost"},
            {"outcome": "won"},
            {"outcome": "pending"},
        ]
        scores = [0.9, 0.8, 0.2, 0.1]
        m = evaluate_ranking(rows, scores, k=3)
        self.assertTrue(0.0 <= m["precision_at_k"] <= 1.0)
        self.assertTrue(0.0 <= m["recall_at_k"] <= 1.0)
        self.assertTrue(0.0 <= m["ndcg_at_k"] <= 1.0)


if __name__ == "__main__":
    unittest.main()
