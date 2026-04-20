#!/usr/bin/env python3
"""
test_ml_pipeline.py — Complete ML Pipeline Testing Guide

Tests all 3 ML components:
1. Feedback ingestion (Excel → bid_pipeline table)
2. Feature engineering (raw tender → 14-dim vector)
3. LogisticRegression training & prediction
4. Decision calibration (threshold tuning)

Usage:
  # 1. Generate test feedback (INSERT sample bid decisions)
  python3 scripts/test_ml_pipeline.py --generate-test-data

  # 2. View what was created
  python3 scripts/test_ml_pipeline.py --show-test-data

  # 3. Train ML model on test data
  python3 scripts/test_ml_pipeline.py --train

  # 4. Make predictions on new tenders
  python3 scripts/test_ml_pipeline.py --predict

  # 5. Calibrate decision thresholds
  python3 scripts/test_ml_pipeline.py --calibrate

  # 6. Full test (all steps)
  python3 scripts/test_ml_pipeline.py --full
"""

import sys
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import random

sys.path.insert(0, os.path.expanduser("~/tender_system"))

from database.db import get_connection
from pipeline.learning_pipeline import (
    maybe_run_weekly_learning,
    _featurize,
    _fit_logreg,
    _load_model,
)
from pipeline.decision_calibrator import (
    compute_decision_accuracy,
    suggest_threshold_adjustment,
    compute_winning_signals,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("ml_test")


def generate_test_feedback(num_samples: int = 50) -> int:
    """
    Insert synthetic test feedback into bid_pipeline table.

    Creates realistic scenarios:
    - 60% "bid" decisions (is_bid=True)
    - 40% "no bid" decisions (is_bid=False)
    - Outcomes: 70% won, 20% lost, 10% pending
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Get random tenders from database
        cursor.execute(
            f"SELECT id, relevance_score, priority_score FROM tenders LIMIT {num_samples}"
        )
        tenders = cursor.fetchall()

        if not tenders:
            logger.error("No tenders in database. Run scrapers first.")
            return 0

        logger.info(f"Creating feedback for {len(tenders)} random tenders...")

        inserted = 0
        for tender in tenders:
            tender_id = tender["id"]

            # Simulate user decision: 60% bid, 40% no bid
            is_bid = random.random() < 0.6

            # Simulate outcome: 70% won, 20% lost, 10% pending
            outcome_rand = random.random()
            if outcome_rand < 0.70:
                outcome = "won"
            elif outcome_rand < 0.90:
                outcome = "lost"
            else:
                outcome = "pending"

            # Only insert if outcome is not pending (pending = still in progress)
            if outcome != "pending":
                try:
                    cursor.execute(
                        """
                        INSERT INTO bid_pipeline (tender_id, is_bid, outcome, feedback_date, notes)
                        VALUES (%s, %s, %s, NOW(), %s)
                        ON DUPLICATE KEY UPDATE
                            is_bid = VALUES(is_bid),
                            outcome = VALUES(outcome),
                            feedback_date = NOW()
                        """,
                        (tender_id, is_bid, outcome, "TEST DATA")
                    )
                    inserted += 1
                except Exception as e:
                    logger.warning(f"Failed to insert feedback for tender {tender_id}: {e}")

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"✓ Inserted {inserted} feedback records")
        return inserted

    except Exception as e:
        logger.error(f"Error generating test data: {e}")
        return 0


def show_test_data() -> None:
    """Display current feedback data in bid_pipeline."""
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Summary stats
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN is_bid=1 THEN 1 ELSE 0 END) as bid_count,
                SUM(CASE WHEN is_bid=0 THEN 1 ELSE 0 END) as no_bid_count,
                SUM(CASE WHEN outcome='won' THEN 1 ELSE 0 END) as won_count,
                SUM(CASE WHEN outcome='lost' THEN 1 ELSE 0 END) as lost_count
            FROM bid_pipeline
        """)
        stats = cursor.fetchone()

        logger.info("=" * 80)
        logger.info("FEEDBACK PIPELINE STATISTICS")
        logger.info("=" * 80)
        logger.info(f"Total feedback records:  {stats['total']}")
        logger.info(f"  → Bid decisions:       {stats['bid_count']}")
        logger.info(f"  → No-bid decisions:    {stats['no_bid_count']}")
        logger.info(f"  → Won outcomes:        {stats['won_count']}")
        logger.info(f"  → Lost outcomes:       {stats['lost_count']}")
        logger.info(f"  → Bid success rate:    {stats['won_count']}/{stats['bid_count']} = {100*stats['won_count']/max(stats['bid_count'],1):.1f}%")
        logger.info("")

        # Sample recent records
        cursor.execute("""
            SELECT
                bp.tender_id,
                bp.is_bid,
                bp.outcome,
                bp.feedback_date,
                t.title,
                t.relevance_score,
                t.priority_score
            FROM bid_pipeline bp
            JOIN tenders t ON bp.tender_id = t.id
            ORDER BY bp.feedback_date DESC
            LIMIT 10
        """)

        logger.info("Latest 10 feedback records:")
        logger.info("-" * 80)
        for row in cursor.fetchall():
            logger.info(
                f"  {row['tender_id']:8} | Bid: {str(row['is_bid']):5} | "
                f"Outcome: {row['outcome']:7} | Score: {row['relevance_score']:.0f}/{row['priority_score']:.0f}"
            )

        cursor.close()
        conn.close()
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Error displaying test data: {e}")


def train_model() -> bool:
    """
    Train LogisticRegression model on bid_pipeline feedback.

    Returns True if training succeeded, False if not enough data.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Check if we have enough feedback
        cursor.execute("SELECT COUNT(*) as count FROM bid_pipeline WHERE outcome IN ('won', 'lost')")
        count = cursor.fetchone()["count"]

        logger.info("=" * 80)
        logger.info("ML TRAINING")
        logger.info("=" * 80)
        logger.info(f"Feedback samples with outcomes: {count}")

        if count < 25:
            logger.warning(f"⚠ Need 25+ feedback samples to train ML. You have {count}.")
            logger.warning("  → Add more test data: python3 scripts/test_ml_pipeline.py --generate-test-data 100")
            logger.info("  → For now, using handcrafted weights (no personalization)")
            return False

        logger.info(f"✓ Sufficient data ({count} samples). Training model...")

        # Force training
        maybe_run_weekly_learning(force=True)

        logger.info("✓ Model training complete!")
        logger.info("=" * 80)
        return True

    except Exception as e:
        logger.error(f"Training failed: {e}")
        return False


def predict_on_tenders() -> None:
    """
    Load trained model and make predictions on tenders.

    Shows predicted bid probability for random tenders.
    """
    try:
        logger.info("=" * 80)
        logger.info("ML PREDICTIONS")
        logger.info("=" * 80)

        # Load model
        model = _load_model()
        if not model:
            logger.warning("⚠ No trained model found. Run --train first.")
            logger.info("=" * 80)
            return

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Get random tenders
        cursor.execute("""
            SELECT * FROM tenders
            WHERE id NOT IN (SELECT DISTINCT tender_id FROM bid_pipeline)
            ORDER BY RAND()
            LIMIT 10
        """)
        tenders = cursor.fetchall()

        if not tenders:
            logger.warning("No unseen tenders available for prediction.")
            cursor.close()
            conn.close()
            logger.info("=" * 80)
            return

        logger.info(f"Making predictions on {len(tenders)} unseen tenders...\n")

        for tender in tenders:
            # Feature engineering
            features = _featurize(tender)

            # Predict
            bid_probability = model.predict_proba([features])[0][1]
            prediction = "BID" if bid_probability > 0.5 else "NO BID"

            logger.info(
                f"  {tender['id']:8} | Prob={bid_probability:.2%} | "
                f"Decision: {prediction:7} | Score: {tender.get('relevance_score', 0):.0f} | "
                f"{tender['title'][:50]}"
            )

        cursor.close()
        conn.close()
        logger.info("\n" + "=" * 80)
        logger.info("Model learned from feedback! It now predicts bid probability.")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Prediction failed: {e}")


def calibrate_thresholds() -> None:
    """
    Analyze winning signals and suggest threshold adjustments.

    Identifies:
    - Which sectors/clients have high win rates
    - Optimal decision thresholds (BID_NOW vs STRONG_CONSIDER)
    """
    try:
        logger.info("=" * 80)
        logger.info("DECISION CALIBRATION")
        logger.info("=" * 80)

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Check for sufficient outcome data
        cursor.execute("""
            SELECT COUNT(*) as count FROM bid_pipeline
            WHERE outcome IN ('won', 'lost')
        """)
        count = cursor.fetchone()["count"]

        if count < 10:
            logger.warning(f"⚠ Need 10+ outcomes for calibration. You have {count}.")
            logger.info("=" * 80)
            cursor.close()
            conn.close()
            return

        # Compute accuracy metrics
        accuracy = compute_decision_accuracy()
        logger.info("Accuracy Metrics:")
        logger.info(f"  Bid Success Rate:  {accuracy.get('bid_success_rate', 0):.1%}")
        logger.info(f"  Win Rate (if bid):  {accuracy.get('win_rate', 0):.1%}")
        logger.info(f"  Avg Bid Score:     {accuracy.get('avg_bid_score', 0):.0f}")
        logger.info("")

        # Get winning signals
        signals = compute_winning_signals()
        logger.info("Winning Signals (sectors/clients with high win rates):")
        for signal in signals[:5]:
            logger.info(f"  {signal.get('type')} = {signal.get('value')} "
                       f"(win rate: {signal.get('win_rate', 0):.1%})")
        logger.info("")

        # Suggest adjustments
        suggestion = suggest_threshold_adjustment()
        logger.info("Recommended Threshold Adjustment:")
        logger.info(f"  Action: {suggestion.get('action')}")
        logger.info(f"  Reason: {suggestion.get('reason')}")
        logger.info(f"  New BID_NOW threshold: {suggestion.get('bid_now_threshold', 'N/A')}")
        logger.info(f"  New STRONG_CONSIDER threshold: {suggestion.get('strong_consider_threshold', 'N/A')}")

        cursor.close()
        conn.close()
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Calibration failed: {e}")


def full_test():
    """Run complete ML pipeline test."""
    logger.info("\n" * 2)
    logger.info("╔" + "=" * 78 + "╗")
    logger.info("║" + " " * 20 + "TENDERRADAR ML PIPELINE TEST" + " " * 32 + "║")
    logger.info("╚" + "=" * 78 + "╝")

    steps = [
        ("Generating test feedback (50 samples)", lambda: generate_test_feedback(50) > 0),
        ("Showing feedback statistics", lambda: (show_test_data(), True)[1]),
        ("Training ML model", train_model),
        ("Making predictions", lambda: (predict_on_tenders(), True)[1]),
        ("Calibrating decision thresholds", lambda: (calibrate_thresholds(), True)[1]),
    ]

    for step_name, step_func in steps:
        logger.info(f"\n▶ {step_name}...")
        try:
            result = step_func()
            if result:
                logger.info(f"✓ {step_name} complete\n")
        except Exception as e:
            logger.error(f"✗ {step_name} failed: {e}\n")

    logger.info("\n" * 2)
    logger.info("╔" + "=" * 78 + "╗")
    logger.info("║" + " " * 15 + "ML PIPELINE TEST COMPLETE!" + " " * 36 + "║")
    logger.info("╚" + "=" * 78 + "╝")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Test ML Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 50 test feedback records
  python3 scripts/test_ml_pipeline.py --generate-test-data

  # Generate 100 test records
  python3 scripts/test_ml_pipeline.py --generate-test-data 100

  # Show current feedback statistics
  python3 scripts/test_ml_pipeline.py --show-test-data

  # Train model on feedback
  python3 scripts/test_ml_pipeline.py --train

  # Make predictions on unseen tenders
  python3 scripts/test_ml_pipeline.py --predict

  # Analyze winning patterns
  python3 scripts/test_ml_pipeline.py --calibrate

  # Run complete pipeline test
  python3 scripts/test_ml_pipeline.py --full
        """
    )

    parser.add_argument("--generate-test-data", nargs="?", const=50, type=int,
                       help="Generate N test feedback records (default: 50)")
    parser.add_argument("--show-test-data", action="store_true",
                       help="Display feedback statistics")
    parser.add_argument("--train", action="store_true",
                       help="Train ML model on feedback")
    parser.add_argument("--predict", action="store_true",
                       help="Make predictions on unseen tenders")
    parser.add_argument("--calibrate", action="store_true",
                       help="Analyze winning signals & suggest thresholds")
    parser.add_argument("--full", action="store_true",
                       help="Run complete test (all steps)")

    args = parser.parse_args()

    if args.generate_test_data is not None:
        generate_test_feedback(args.generate_test_data)
    elif args.show_test_data:
        show_test_data()
    elif args.train:
        train_model()
    elif args.predict:
        predict_on_tenders()
    elif args.calibrate:
        calibrate_thresholds()
    elif args.full:
        full_test()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
