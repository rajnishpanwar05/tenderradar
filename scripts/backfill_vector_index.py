#!/usr/bin/env python3
"""
backfill_vector_index.py — Backfill ALL historical tenders into tenders_v2

The pipeline only indexes tenders scraped in EACH RUN. This script backfills
ALL 20,650+ enriched tenders from tender_structured_intel into tenders_v2.

BEFORE: tenders_v2 = 850 vectors  (only recent runs)
AFTER:  tenders_v2 = 20,000+ vectors  (full history)

Run:
  # Dry run — show what would happen
  python3 scripts/backfill_vector_index.py --dry-run

  # Test with 500 records first
  python3 scripts/backfill_vector_index.py --limit 500

  # Full backfill (takes ~15-30 min)
  python3 scripts/backfill_vector_index.py
"""
import sys, os, time, logging, argparse
sys.path.insert(0, os.path.expanduser("~/tender_system"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("backfill")


def load_enriched_tenders(limit=None, offset=0):
    """
    Load tenders joined with structured intel — the richest dataset.
    This gives us title, sector, region, scores, insights for best embeddings.
    """
    from database.db import get_connection
    conn = get_connection()
    cur  = conn.cursor(dictionary=True)

    query = """
        SELECT
            st.tender_id,
            st.title,
            st.source_site          AS source_portal,
            st.url,
            st.date_first_seen,
            tsi.sector,
            tsi.consulting_type,
            tsi.region,
            tsi.organization,
            tsi.deadline_category,
            tsi.relevance_score     AS bid_fit_score,
            tsi.priority_score,
            tsi.competition_level,
            tsi.opportunity_size,
            tsi.opportunity_insight
        FROM seen_tenders st
        INNER JOIN tender_structured_intel tsi ON tsi.tender_id = st.tender_id
        ORDER BY tsi.priority_score DESC, st.id ASC
        LIMIT %s OFFSET %s
    """
    count_query = """
        SELECT COUNT(*)
        FROM seen_tenders st
        INNER JOIN tender_structured_intel tsi ON tsi.tender_id = st.tender_id
    """
    cur.execute(count_query)
    total = cur.fetchone()["COUNT(*)"]

    actual_limit = limit if limit else total
    cur.execute(query, (actual_limit, offset))
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows, total


def rebuild_chroma(tenders, batch_size=100):
    """Index tenders into tenders_v2 in batches."""
    from intelligence.vector_store import index_tenders_batch
    total   = len(tenders)
    indexed = 0
    failed  = 0
    start   = time.time()

    for i in range(0, total, batch_size):
        batch     = tenders[i : i + batch_size]
        batch_num = i // batch_size + 1
        n_batches = (total + batch_size - 1) // batch_size

        try:
            stored = index_tenders_batch(batch)
            indexed += len(batch)

            elapsed = time.time() - start
            rate    = indexed / elapsed if elapsed > 0 else 0
            eta     = (total - indexed) / rate if rate > 0 else 0
            log.info(
                f"  Batch {batch_num}/{n_batches} ✓  "
                f"{indexed:,}/{total:,}  |  {rate:.1f}/s  |  ETA {eta:.0f}s"
            )
        except Exception as e:
            log.error(f"  Batch {batch_num}/{n_batches} ✗  {e}")
            failed += len(batch)

    return indexed, failed


def main():
    p = argparse.ArgumentParser(description="Backfill vector index from tender_structured_intel")
    p.add_argument("--limit",      type=int, default=None, help="Max rows (default: all)")
    p.add_argument("--batch-size", type=int, default=100,  help="ChromaDB batch size (default: 100)")
    p.add_argument("--dry-run",    action="store_true",    help="Show stats, don't write")
    args = p.parse_args()

    log.info("=" * 70)
    log.info("TenderRadar — Vector Index Backfill")
    log.info("=" * 70)

    # Current state
    try:
        import chromadb
        path   = os.path.expanduser("~/tender_system/chroma_db")
        client = chromadb.PersistentClient(path=path)
        col    = client.get_collection("tenders_v2")
        log.info(f"tenders_v2 BEFORE: {col.count():,} vectors")
    except Exception as e:
        log.warning(f"Could not read ChromaDB count: {e}")

    log.info("Loading enriched tenders from tender_structured_intel...")
    tenders, total_available = load_enriched_tenders(limit=args.limit)
    log.info(f"Available in tender_structured_intel: {total_available:,}")
    log.info(f"Loading: {len(tenders):,} tenders (limit={args.limit or 'none'})")

    if args.dry_run:
        log.info("DRY RUN — showing sample data, no writes")
        for t in tenders[:3]:
            log.info(f"  Sample: {t['tender_id']} | {t['title'][:50]} | {t['sector']} | {t['source_portal']}")
        return

    log.info(f"Starting backfill with batch_size={args.batch_size}...")
    indexed, failed = rebuild_chroma(tenders, batch_size=args.batch_size)

    log.info("=" * 70)
    log.info(f"Backfill complete!")
    log.info(f"  Indexed: {indexed:,}")
    log.info(f"  Failed:  {failed:,}")
    log.info(f"  Total:   {len(tenders):,}")

    # Final state
    try:
        col = client.get_collection("tenders_v2")
        log.info(f"tenders_v2 AFTER: {col.count():,} vectors")
    except Exception as e:
        log.warning(f"Could not verify final count: {e}")

    log.info("=" * 70)
    log.info("Done! Restart the FastAPI backend to pick up the new index.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
