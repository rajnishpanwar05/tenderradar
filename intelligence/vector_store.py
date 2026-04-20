# =============================================================================
# vector_store.py — Semantic Memory for TenderRadar (ChromaDB)
#
# Persistent local vector store powering:
#   - Semantic duplicate detection before alerting
#   - Similarity search for the future chatbot interface
#
# Storage: ~/tender_system/chroma_db/  (auto-created on first run)
# Collection: "tenders" with cosine distance space
#
# Design: fail-open — if ChromaDB is unavailable, all functions return
# safe defaults so the pipeline never crashes.
# =============================================================================

import hashlib
import logging
import os
import platform
import sys
from datetime import datetime, timedelta
from typing import List, Optional

logger = logging.getLogger(__name__)

_collection      = None   # module-level cache — initialised once per process
_chroma_missing  = False  # flag so we only log the "not installed" error ONCE
_model_cache     = None   # sentence-transformers model — loaded once
_runtime_block_logged = False


def _vector_runtime_enabled() -> bool:
    """
    Safety gate for native vector stack on environments known to segfault.
    Default behavior:
      - Respect explicit DISABLE_VECTOR_SEARCH=1
      - On macOS + Python 3.9, disable by default unless FORCE_VECTOR_SEARCH=1
    """
    if os.getenv("DISABLE_VECTOR_SEARCH", "0") == "1":
        return False
    if os.getenv("FORCE_VECTOR_SEARCH", "0") == "1":
        return True
    if platform.system() == "Darwin" and sys.version_info[:2] <= (3, 9):
        return False
    return True


def _log_runtime_block_once() -> None:
    global _runtime_block_logged
    if _runtime_block_logged:
        return
    _runtime_block_logged = True
    logger.warning(
        "[vector_store] Native vector search disabled for runtime safety "
        "(macOS + Python 3.9 can crash in C extensions). "
        "Set FORCE_VECTOR_SEARCH=1 to override after upgrading/stabilizing stack."
    )


# =============================================================================
# INTERNAL — Collection initialisation
# =============================================================================

def _get_collection():
    """
    Lazy-init ChromaDB persistent collection.
    Returns None (silently) if ChromaDB is not installed or path is bad.
    Logs a clear single-line error ONCE if chromadb package is missing.
    """
    global _collection, _chroma_missing
    if not _vector_runtime_enabled():
        _log_runtime_block_once()
        return None
    if _collection is not None:
        return _collection
    if _chroma_missing:
        return None

    try:
        import chromadb
    except ImportError:
        _chroma_missing = True
        logger.error(
            "[vector_store] chromadb not installed — run: pip install chromadb>=0.4.0 "
            "  Semantic search and vector indexing are disabled until then."
        )
        return None

    try:
        _BASE = os.path.expanduser("~/tender_system")
        if _BASE not in sys.path:
            sys.path.insert(0, _BASE)
        from config import CHROMA_DB_PATH, CHROMA_HOST, CHROMA_PORT

        if CHROMA_HOST:
            client = chromadb.HttpClient(host=str(CHROMA_HOST), port=int(CHROMA_PORT))
            logger.info(
                "[vector_store] Using remote ChromaDB at %s:%s",
                CHROMA_HOST,
                CHROMA_PORT,
            )
        else:
            client = chromadb.PersistentClient(path=str(CHROMA_DB_PATH))

        # tenders_v3: 384-dim — matches multi-qa-MiniLM-L6-cos-v1 used in vector_store
        _COLLECTION_NAME = "tenders_v3"

        _collection = client.get_or_create_collection(
            name=_COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )
        if CHROMA_HOST:
            logger.info(
                "[vector_store] ChromaDB ready at %s:%s (collection=%s, %s docs stored)",
                CHROMA_HOST,
                CHROMA_PORT,
                _COLLECTION_NAME,
                _collection.count(),
            )
        else:
            logger.info(
                f"[vector_store] ChromaDB ready at {CHROMA_DB_PATH} "
                f"(collection={_COLLECTION_NAME}, {_collection.count()} docs stored)"
            )
        return _collection

    except Exception as e:
        logger.error(f"[vector_store] ChromaDB init failed: {e}")
        return None


def _get_model():
    """Load sentence-transformers model once and cache it."""
    global _model_cache
    if not _vector_runtime_enabled():
        _log_runtime_block_once()
        return None
    if _model_cache is not None:
        return _model_cache
    try:
        from sentence_transformers import SentenceTransformer
        # multi-qa-MiniLM-L6-cos-v1: 384-dim, trained for asymmetric query→document retrieval.
        # Significantly better than all-MiniLM-L6-v2 for "find tenders matching query" tasks.
        # Same dimension so ChromaDB collection is compatible; rebuild index after model change.
        _model_cache = SentenceTransformer("multi-qa-MiniLM-L6-cos-v1")
        logger.info("[vector_store] Sentence-transformers model loaded (multi-qa-MiniLM-L6-cos-v1)")
        return _model_cache
    except ImportError:
        logger.error(
            "[vector_store] sentence_transformers not installed — "
            "run: pip install sentence-transformers>=2.2.2"
        )
        return None
    except Exception as e:
        logger.error(f"[vector_store] Model load failed: {e}")
        return None


# =============================================================================
# PUBLIC API
# =============================================================================

def is_duplicate(embedding: "np.ndarray", threshold: float = 0.95) -> bool:
    """
    Check whether a semantically near-identical tender was seen in the last 30 days.

    Uses ChromaDB cosine distance: distance = 1 - cosine_similarity.
    A distance ≤ (1 - threshold) means similarity ≥ threshold.

    Args:
        embedding:  numpy array from sentence-transformers.
        threshold:  cosine similarity threshold (default 0.95 = very close match).

    Returns:
        True  if a duplicate exists (safe to suppress alert).
        False if no duplicate, or if ChromaDB is unavailable (fail-open).
    """
    coll = _get_collection()
    if coll is None or coll.count() == 0:
        return False

    try:
        results = coll.query(
            query_embeddings=[embedding.tolist()],
            n_results=min(5, coll.count()),
            include=["distances", "metadatas"],
        )

        # ChromaDB cosine distance: 0 = identical, 1 = orthogonal
        cutoff_ts = int((datetime.now() - timedelta(days=30)).timestamp())
        distances = results.get("distances", [[]])[0]
        metadatas = results.get("metadatas", [[]])[0]

        for dist, meta in zip(distances, metadatas):
            added_at = meta.get("added_at", 0)
            if added_at >= cutoff_ts and dist <= (1.0 - threshold):
                logger.info(
                    f"[vector_store] Duplicate detected "
                    f"(similarity={1 - dist:.3f}, title='{meta.get('title', '')[:50]}')"
                )
                return True

        return False

    except Exception as e:
        logger.warning(f"[vector_store] is_duplicate query failed: {e}")
        return False   # fail-open


def store_tender(
    tender_id: str,
    embedding: list,
    metadata:  dict,
) -> Optional[str]:
    """
    Upsert a tender embedding into ChromaDB.

    Args:
        tender_id: Stable unique identifier (URL hash or pipeline ID).
        embedding: Float list from sentence-transformers encode().
        metadata:  Dict of scalar values (str/int/float/bool) to store alongside.

    Returns:
        embedding_id string (MD5 of tender_id) on success, None on failure.
    """
    coll = _get_collection()
    if coll is None:
        return None

    try:
        emb_id = hashlib.md5(str(tender_id).encode()).hexdigest()

        # ChromaDB metadata must be all-scalar; filter out non-scalars
        safe_meta = {
            k: v for k, v in metadata.items()
            if isinstance(v, (str, int, float, bool))
        }
        safe_meta["added_at"] = int(datetime.now().timestamp())

        coll.upsert(
            ids=[emb_id],
            embeddings=[embedding],
            metadatas=[safe_meta],
        )
        return emb_id

    except Exception as e:
        logger.warning(f"[vector_store] store_tender failed for '{tender_id[:40]}': {e}")
        return None


def index_tenders_batch(tenders: List[dict]) -> int:
    """
    Encode and index a batch of tender dicts into ChromaDB.

    Called automatically after enrichment in main.py.
    Each tender must have at minimum: tender_id (or id), title.
    Uses title + description as the embedding input.

    Returns:
        Number of tenders successfully indexed (0 if ChromaDB/model unavailable).
    """
    coll  = _get_collection()
    model = _get_model()
    if coll is None or model is None:
        return 0

    ids        = []
    embeddings = []
    metadatas  = []
    texts      = []

    for t in tenders:
        tender_id = str(
            t.get("tender_id") or t.get("id") or t.get("sol_num") or ""
        ).strip()
        if not tender_id:
            continue

        title  = str(t.get("title") or t.get("Title") or "").strip()
        desc   = str(t.get("description") or t.get("Description") or "").strip()
        # Enrich embedding text with structured metadata for better semantic matching.
        # Industry best practice: embed title + description + key metadata fields
        # so "education evaluation India" matches tenders tagged with those attributes.
        org    = str(t.get("organization") or t.get("Organisation Name") or "").strip()
        sector = str(t.get("sector") or t.get("Sector") or "").strip()
        region = str(t.get("region") or t.get("country") or "").strip()
        source = str(t.get("source_site") or t.get("source") or "").strip()
        insight = str(t.get("opportunity_insight") or "").strip()

        # Build rich text: title is most important, then description, then metadata
        parts = [title]
        if desc:
            parts.append(desc[:500])  # Cap description to avoid noise
        if org and org.lower() not in ("unknown", "not specified", ""):
            parts.append(f"Organization: {org}")
        if sector and sector.lower() not in ("unknown", "general", ""):
            parts.append(f"Sector: {sector}")
        if region and region.lower() not in ("global", "unknown", ""):
            parts.append(f"Region: {region}")
        if insight:
            parts.append(insight[:200])

        text = " | ".join(parts)
        if not text or len(text) < 10:
            continue

        emb_id = hashlib.md5(tender_id.encode()).hexdigest()
        ids.append(emb_id)
        texts.append(text)
        metadatas.append({
            "tender_id":    tender_id,
            "title":        title[:200],
            "source":       str(t.get("source_site") or t.get("source") or ""),
            "url":          str(t.get("url") or ""),
            "fit_score":    float(t.get("relevance_score") or t.get("bid_fit_score") or 0),
            "added_at":     int(datetime.now().timestamp()),
        })

    if not ids:
        return 0

    try:
        # Batch encode — much faster than one-by-one
        vecs = model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
        embeddings = [v.tolist() for v in vecs]

        # Upsert in chunks of 500 to avoid memory spikes
        chunk_size = 500
        stored = 0
        for i in range(0, len(ids), chunk_size):
            coll.upsert(
                ids        = ids[i:i + chunk_size],
                embeddings = embeddings[i:i + chunk_size],
                metadatas  = metadatas[i:i + chunk_size],
            )
            stored += len(ids[i:i + chunk_size])

        logger.info(
            f"[vector_store] Indexed {stored} tender(s) — "
            f"store now has {coll.count()} total vectors"
        )
        return stored

    except Exception as e:
        logger.warning(f"[vector_store] index_tenders_batch failed: {e}")
        return 0


def find_similar_tenders(query: str, top_k: int = 10) -> list[dict]:
    """
    Semantic search: find the top_k most similar stored tenders for a query string.

    Powers the future TenderRadar chatbot interface.
    e.g.: find_similar_tenders("M&E evaluation Bihar education", top_k=5)

    Args:
        query:  Natural language query string.
        top_k:  Number of results to return (default 10).

    Returns:
        List of dicts: {title, source, fit_score, url, added_at, similarity}
        Empty list if ChromaDB unavailable or no results.
    """
    coll = _get_collection()
    if coll is None or coll.count() == 0:
        return []

    try:
        model     = _get_model()
        if model is None:
            return []
        query_emb = model.encode(query, convert_to_numpy=True)

        n = min(top_k, coll.count())
        results = coll.query(
            query_embeddings=[query_emb.tolist()],
            n_results=n,
            include=["metadatas", "distances"],
        )

        output = []
        for meta, dist in zip(results["metadatas"][0], results["distances"][0]):
            output.append({
                **meta,
                "similarity": round(1.0 - float(dist), 3),
            })

        output.sort(key=lambda x: x["similarity"], reverse=True)
        return output

    except Exception as e:
        logger.error(f"[vector_store] find_similar_tenders failed: {e}")
        return []


def get_store_stats() -> dict:
    """
    Return basic stats about the vector store.
    Useful for health checks and dashboard display.
    """
    coll = _get_collection()
    if coll is None:
        return {"status": "unavailable", "total_docs": 0}

    try:
        return {
            "status":     "ok",
            "total_docs": coll.count(),
            "collection": coll.name,
        }
    except Exception as e:
        return {"status": f"error: {e}", "total_docs": 0}


def check_vector_db_sync(warn_threshold: float = 0.5) -> dict:
    """
    Compare ChromaDB vector count against MySQL seen_tenders count.

    Returns a dict with sync status. Logs a prominent WARNING if the
    vector store has fewer than (1 - warn_threshold) of DB rows, which
    typically means ChromaDB was wiped/reset without re-indexing.

    Args:
        warn_threshold: If chroma_count / db_count < warn_threshold, warn.
                        Default 0.5 = warn if ChromaDB has < 50% of DB rows.

    Returns:
        {
            "status":       "ok" | "diverged" | "empty" | "unavailable",
            "chroma_docs":  int,
            "db_tenders":   int,
            "sync_ratio":   float,   # chroma / db  (1.0 = perfectly in sync)
            "message":      str,
        }
    """
    coll = _get_collection()
    if coll is None:
        return {
            "status": "unavailable", "chroma_docs": 0,
            "db_tenders": 0, "sync_ratio": 0.0,
            "message": "ChromaDB not available",
        }

    chroma_count = 0
    db_count     = 0

    try:
        chroma_count = coll.count()
    except Exception as e:
        logger.warning("[vector_store] check_sync: could not read ChromaDB count: %s", e)

    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM seen_tenders;")
        row = cur.fetchone()
        db_count = int(row[0]) if row else 0
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("[vector_store] check_sync: could not read DB count: %s", e)

    if db_count == 0:
        return {
            "status": "ok", "chroma_docs": chroma_count,
            "db_tenders": 0, "sync_ratio": 1.0,
            "message": "DB is empty — nothing to sync yet",
        }

    if chroma_count == 0:
        logger.warning(
            "[vector_store] DIVERGENCE: ChromaDB is EMPTY but DB has %d tenders. "
            "Semantic search is disabled. Run: "
            "python3 intelligence/vector_store.py --rebuild",
            db_count,
        )
        return {
            "status": "empty", "chroma_docs": 0,
            "db_tenders": db_count, "sync_ratio": 0.0,
            "message": (
                f"ChromaDB empty but DB has {db_count} tenders. "
                "Run: python3 intelligence/vector_store.py --rebuild"
            ),
        }

    ratio = chroma_count / db_count
    if ratio < warn_threshold:
        logger.warning(
            "[vector_store] DIVERGENCE: ChromaDB has %d docs but DB has %d tenders "
            "(%.0f%% synced). Semantic search quality degraded. "
            "Run: python3 intelligence/vector_store.py --rebuild",
            chroma_count, db_count, ratio * 100,
        )
        return {
            "status": "diverged", "chroma_docs": chroma_count,
            "db_tenders": db_count, "sync_ratio": round(ratio, 3),
            "message": (
                f"ChromaDB has {chroma_count} docs ({ratio*100:.0f}% of {db_count} DB tenders). "
                "Run: python3 intelligence/vector_store.py --rebuild"
            ),
        }

    logger.info(
        "[vector_store] Sync OK: ChromaDB=%d, DB=%d (%.0f%% indexed)",
        chroma_count, db_count, ratio * 100,
    )
    return {
        "status": "ok", "chroma_docs": chroma_count,
        "db_tenders": db_count, "sync_ratio": round(ratio, 3),
        "message": f"Sync OK — {chroma_count} vectors for {db_count} tenders",
    }


# =============================================================================
# CLI entry point
#
# Usage:
#   python3 intelligence/vector_store.py              # self-test
#   python3 intelligence/vector_store.py --rebuild    # full re-index from seen_tenders
#   python3 intelligence/vector_store.py --rebuild --limit 5000
# =============================================================================

if __name__ == "__main__":
    import argparse
    import numpy as np

    _BASE = os.path.expanduser("~/tender_system")
    if _BASE not in sys.path:
        sys.path.insert(0, _BASE)

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        level=logging.INFO,
    )

    ap = argparse.ArgumentParser(description="TenderRadar Vector Store CLI")
    ap.add_argument("--rebuild", action="store_true",
                    help="Re-index all tenders from seen_tenders into ChromaDB")
    ap.add_argument("--limit", type=int, default=20_000,
                    help="Max rows to index during --rebuild (default: 20000)")
    args = ap.parse_args()

    if args.rebuild:
        print(f"\n{'='*60}")
        print("  TenderRadar — Vector Store Rebuild")
        print(f"{'='*60}\n")
        print(f"Stats before: {get_store_stats()}")

        try:
            from database.db import get_connection
            conn = get_connection()
            cur  = conn.cursor(dictionary=True)
            # Only index tenders with relevance_score > 25 (real consulting tenders).
            # This excludes supply/construction/IT tenders from the vector index entirely,
            # which dramatically improves search precision.
            # Tenders with no intel row yet (relevance_score IS NULL) are also included
            # so newly scraped tenders are searchable immediately.
            cur.execute(
                "SELECT t.tender_id, t.title, t.source_site, t.url, "
                "       COALESCE(t.description, '') AS description, "
                "       COALESCE(i.relevance_score, 0) AS relevance_score, "
                "       COALESCE(i.organization, '') AS organization, "
                "       COALESCE(i.sector, '') AS sector, "
                "       COALESCE(i.region, '') AS region, "
                "       COALESCE(i.opportunity_insight, '') AS opportunity_insight "
                "FROM seen_tenders t "
                "LEFT JOIN tender_structured_intel i USING (tender_id) "
                "WHERE i.relevance_score IS NULL OR i.relevance_score > 25 "
                "ORDER BY COALESCE(i.relevance_score, 50) DESC, t.date_first_seen DESC "
                "LIMIT %s",
                (args.limit,),
            )
            rows = cur.fetchall() or []
            cur.close(); conn.close()
            print(f"Fetched {len(rows)} tenders from DB — encoding and indexing…")
            n = index_tenders_batch(rows)
            print(f"\n✅  Rebuild complete — {n} vectors stored")
            print(f"Stats after:  {get_store_stats()}")
        except Exception as e:
            print(f"❌  Rebuild failed: {e}")
            sys.exit(1)

        sys.exit(0)

    # ── Default: self-test ─────────────────────────────────────────────────────
    print("Testing vector_store...\n")
    print(f"Stats: {get_store_stats()}")

    fake_emb = np.random.rand(384).astype("float32")
    fake_emb /= np.linalg.norm(fake_emb)

    eid = store_tender("TEST_001", fake_emb.tolist(), {
        "title": "Test M&E tender India", "source": "test", "fit_score": 75.0, "url": "http://test.com",
    })
    print(f"Stored test tender, embedding_id={eid}")
    print(f"Stats after store: {get_store_stats()}")

    is_dup = is_duplicate(fake_emb, threshold=0.95)
    print(f"is_duplicate (same vector, should be True): {is_dup}")

    noise_emb = np.random.rand(384).astype("float32")
    noise_emb /= np.linalg.norm(noise_emb)
    is_dup2 = is_duplicate(noise_emb, threshold=0.95)
    print(f"is_duplicate (random vector, should be False): {is_dup2}")

    results = find_similar_tenders("education evaluation Bihar", top_k=3)
    print(f"find_similar_tenders: {results}")
    print("\nvector_store test complete.")
