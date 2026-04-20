#!/usr/bin/env python3
"""
rebuild_vector_index.py — Re-index ALL tenders into tenders_v2 collection

This fixes the gap where only 850 of 2,499 tenders are in ChromaDB tenders_v2.

Run:
  python3 scripts/rebuild_vector_index.py
  python3 scripts/rebuild_vector_index.py --batch-size 50 --limit 500  # test first
"""
import sys, os, logging, argparse, time
sys.path.insert(0, os.path.expanduser("~/tender_system"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rebuild_vectors")


def get_all_tenders(limit=None):
    from database.db import get_connection
    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    q = "SELECT * FROM tenders ORDER BY id ASC"
    if limit: q += f" LIMIT {limit}"
    cur.execute(q)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def index_batch(tenders):
    from intelligence.vector_store import index_tenders_batch
    try:
        index_tenders_batch(tenders)
        return True
    except Exception as e:
        log.error(f"  index_tenders_batch failed: {e}")
        return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--limit",      type=int, default=None, help="Limit rows (for testing)")
    p.add_argument("--dry-run",    action="store_true")
    args = p.parse_args()

    log.info("=" * 70)
    log.info("TenderRadar — Rebuild tenders_v2 Vector Index")
    log.info("=" * 70)

    log.info("Loading tenders from DB...")
    tenders = get_all_tenders(limit=args.limit)
    log.info(f"  Loaded {len(tenders):,} tenders")

    if args.dry_run:
        log.info("DRY RUN — no changes made")
        return

    total      = len(tenders)
    indexed    = 0
    failed     = 0
    start_time = time.time()

    for i in range(0, total, args.batch_size):
        batch = tenders[i : i + args.batch_size]
        batch_num = i // args.batch_size + 1
        total_batches = (total + args.batch_size - 1) // args.batch_size

        log.info(f"Batch {batch_num}/{total_batches}  ({i+1}–{min(i+len(batch), total)} of {total})")

        if index_batch(batch):
            indexed += len(batch)
            elapsed = time.time() - start_time
            rate    = indexed / elapsed
            eta     = (total - indexed) / rate if rate > 0 else 0
            log.info(f"  ✓ {indexed:,}/{total:,} indexed  |  {rate:.1f}/s  |  ETA {eta:.0f}s")
        else:
            failed += len(batch)
            log.warning(f"  ✗ Batch {batch_num} failed")

    log.info("=" * 70)
    log.info(f"Done!  Indexed: {indexed:,}  Failed: {failed:,}  Time: {time.time()-start_time:.0f}s")
    log.info("=" * 70)

    # Verify final count
    try:
        import chromadb
        path   = os.path.expanduser("~/tender_system/chroma_db")
        client = chromadb.PersistentClient(path=path)
        col    = client.get_collection("tenders_v2")
        log.info(f"tenders_v2 now has: {col.count():,} vectors")
    except Exception as e:
        log.warning(f"Could not verify ChromaDB count: {e}")


if __name__ == "__main__":
    main()
