#!/usr/bin/env python3
"""
reindex_tenders_with_keywords.py — Re-embed all tenders with CAP STAT keywords

This script:
1. Loads all tenders from the database
2. Re-scores using the new IDCG keyword taxonomy
3. Updates tender_structured_intel with new keyword-based scores
4. Re-embeds into ChromaDB with enriched context

Usage:
  python3 scripts/reindex_tenders_with_keywords.py [--dry-run] [--limit 100]

Run this once after deploying idcg_keywords.json to apply new scoring.
"""

import sys
import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

sys.path.insert(0, os.path.expanduser("~/tender_system"))

from database.db import get_connection
from intelligence.tender_intelligence import enrich_one, store_batch
from vector_store import index_tenders_batch
from config.config import UNIFIED_EXCEL_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("reindex")


def load_idcg_keywords() -> Dict:
    """Load CAP STAT keyword taxonomy."""
    keyword_file = os.path.expanduser("~/tender_system/config/idcg_keywords.json")
    try:
        with open(keyword_file) as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Keywords file not found: {keyword_file}")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in keywords file: {e}")
        return {}


def get_all_tenders(limit: Optional[int] = None) -> List[Dict]:
    """Load all tenders from database."""
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = "SELECT * FROM seen_tenders ORDER BY created_at DESC"
        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query)
        tenders = cursor.fetchall()
        cursor.close()
        conn.close()

        logger.info(f"Loaded {len(tenders)} tenders from database")
        return tenders

    except Exception as e:
        logger.error(f"Error loading tenders: {e}")
        return []


def reindex_batch(tenders: List[Dict], batch_size: int = 100) -> int:
    """
    Re-enrich and re-index tenders in batches.

    Returns:
        Number of tenders successfully processed
    """
    total = len(tenders)
    processed = 0
    failed = 0

    for i in range(0, total, batch_size):
        batch = tenders[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1} ({i+1}-{min(i+batch_size, total)} of {total})")

        # Enrich batch with keywords
        try:
            enriched = []
            for tender in batch:
                try:
                    enriched_tender = enrich_one(tender)
                    if enriched_tender:
                        enriched.append(enriched_tender)
                        processed += 1
                except Exception as e:
                    logger.warning(f"Failed to enrich tender {tender.get('id')}: {e}")
                    failed += 1

            # Store enriched records
            if enriched:
                try:
                    stored = store_batch(enriched)
                    logger.info(f"  ✓ Stored {stored} enriched records")
                except Exception as e:
                    logger.error(f"  ✗ Failed to store batch: {e}")

            # Re-index in vector store with enriched text
            if enriched:
                try:
                    index_tenders_batch(enriched)
                    logger.info(f"  ✓ Re-indexed {len(enriched)} vectors")
                except Exception as e:
                    logger.warning(f"  ⚠ Vector indexing failed (non-critical): {e}")

        except Exception as e:
            logger.error(f"Batch processing error: {e}")
            failed += len(batch)

    logger.info(f"Reindexing complete: {processed} processed, {failed} failed")
    return processed


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Re-index tenders with CAP STAT keywords")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without changes")
    parser.add_argument("--limit", type=int, help="Limit to N tenders (for testing)")
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("TenderRadar Keyword Re-Indexing")
    logger.info("=" * 80)

    # Load keywords
    logger.info("Loading IDCG keyword taxonomy...")
    keywords = load_idcg_keywords()
    if not keywords:
        logger.error("No keywords loaded. Aborting.")
        return 1

    logger.info(f"✓ Loaded keywords with {len(keywords)} top-level sections")

    if args.dry_run:
        logger.info("DRY RUN MODE — no changes will be made")

    # Load all tenders
    logger.info("Loading tenders from database...")
    tenders = get_all_tenders(limit=args.limit)
    if not tenders:
        logger.error("No tenders found. Aborting.")
        return 1

    if args.limit:
        logger.info(f"Limited to {args.limit} tenders for testing")

    # Re-index
    if not args.dry_run:
        logger.info("Starting re-indexing...")
        processed = reindex_batch(tenders)
        logger.info(f"Re-indexing complete: {processed} tenders processed")
    else:
        logger.info(f"Would re-index {len(tenders)} tenders with new keywords")

    logger.info("=" * 80)
    logger.info("✓ Done!")
    logger.info("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
