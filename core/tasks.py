import logging
from typing import List, Dict, Any
from core.celery_app import celery_app

logger = logging.getLogger("tenderradar.tasks")


def process_intelligence_batch_sync(raw_tenders: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Run Phase 1/2 intelligence synchronously (smart-batching version).

    Strategy:
      1. Pre-score ALL tenders with the fast local keyword scorer (0 API cost).
      2. Deep-enrich only the TOP N most relevant tenders (default: top 150).
         — Deep-enriching all 11k+ in a live run would take hours; the rest
           are picked up by scripts/backend_maintenance.py backfill-deep.
      3. Run the intelligence layer (LLM extraction + semantic scoring) on
         the same top-N subset only — LLM quotas are limited.
      4. Return both the enriched subset and amendment list.

    The Celery task below reuses this helper so both modes stay aligned.
    """
    if not raw_tenders:
        return {
            "processed": 0,
            "deep_results": [],
            "deep_saved": 0,
            "deep_amended": [],
            "enriched": [],
        }

    from intelligence.keywords import score_relevance
    from scrapers.deep_scraper import enrich_batch_deep, save_deep_enrichment
    from intelligence.intelligence_layer import process_batch

    # ── Step 1: fast local pre-score to pick the best candidates ────────────
    _LIVE_DEEP_LIMIT  = 150   # max tenders to deep-enrich in a live run
    _LIVE_INTEL_LIMIT = 300   # max tenders to send through LLM intelligence

    scored = []
    for t in raw_tenders:
        title = t.get("title") or t.get("Title") or ""
        desc  = t.get("description") or t.get("Description") or ""
        sc    = score_relevance(title, desc)   # returns comma-joined category string or ""
        # Use string length as a proxy for relevance strength (more categories = higher score)
        scored.append((len(sc), sc, t))

    # Sort descending by relevance strength
    scored.sort(key=lambda x: x[0], reverse=True)

    # Only take tenders that matched at least one category (sc != "")
    deep_candidates  = [t for weight, sc, t in scored[:_LIVE_DEEP_LIMIT]  if weight > 0]
    intel_candidates = [t for weight, sc, t in scored[:_LIVE_INTEL_LIMIT] if weight > 0]

    logger.info(
        "[sync] %d new tenders → deep-enriching top %d, intelligence on top %d",
        len(raw_tenders), len(deep_candidates), len(intel_candidates),
    )

    # ── Step 2: deep-enrich top candidates (threaded) ─────────────────────────
    deep_results  = []
    deep_saved    = 0
    deep_amended  = []

    if deep_candidates:
        deep_results = enrich_batch_deep(
            deep_candidates, max_workers=6, delay=1.0, timeout=30
        )
        for tender in deep_results:
            tender_id = str(tender.get("tender_id") or tender.get("id") or "").strip()
            if tender_id and not tender.get("error"):
                if save_deep_enrichment(tender_id, tender):
                    deep_saved += 1
                if tender.get("amendment_detected"):
                    deep_amended.append(tender)

    # Merge deep results back into intel_candidates (replace matched entries)
    deep_by_id = {
        str(t.get("tender_id") or t.get("id") or ""): t
        for t in deep_results
    }
    merged_intel = [
        deep_by_id.get(str(t.get("tender_id") or t.get("id") or ""), t)
        for t in intel_candidates
    ]

    # ── Step 3: run intelligence layer on enriched top candidates ─────────────
    enriched = []
    if merged_intel:
        logger.info("[sync] Running intelligence layer on %d tenders…", len(merged_intel))
        enriched = process_batch(merged_intel)

    logger.info(
        "[sync] Complete — deep_saved=%d, amended=%d, enriched=%d "
        "(remaining %d queued for nightly backfill-deep)",
        deep_saved, len(deep_amended), len(enriched),
        len(raw_tenders) - len(deep_candidates),
    )

    return {
        "processed":    len(enriched),
        "deep_results": deep_results,
        "deep_saved":   deep_saved,
        "deep_amended": deep_amended,
        "enriched":     enriched,
    }


def run_phase3_intelligence_sync(tender_batch: List[Dict[str, Any]]) -> int:
    """
    Run Phase 3 intelligence synchronously.

    Keeps structured enrichment, vector indexing, scoring, and insights alive
    even when Celery workers are not running.
    """
    if not tender_batch:
        return 0

    from intelligence.tender_intelligence import enrich_and_store_batch
    from intelligence.vector_store import index_tenders_batch
    from intelligence.opportunity_engine import score_and_store_batch
    from intelligence.opportunity_insights import generate_and_store_batch

    logger.info("[sync] Phase 3 PIPELINE: Starting for %d tenders...", len(tender_batch))
    enrich_and_store_batch(tender_batch)
    index_tenders_batch(tender_batch)
    score_and_store_batch(tender_batch)
    generate_and_store_batch(tender_batch)
    logger.info(
        "[sync] Phase 3 PIPELINE: Successfully processed batch of %d",
        len(tender_batch),
    )
    return len(tender_batch)

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def task_process_intelligence_batch(self, raw_tenders: List[Dict[str, Any]]):
    """
    Background worker task to execute the massive intelligence pipeline
    without blocking the main scraping cron job.
    """
    if not raw_tenders:
        return 0

    try:
        result = process_intelligence_batch_sync(raw_tenders)
        return int(result.get("processed", 0))
    except Exception as exc:
        logger.error(f"[Celery] Batch processing failed: {exc}")
        # Automatically retry the task if we hit network/queue errors
        raise self.retry(exc=exc)

@celery_app.task(bind=True, max_retries=3)
def task_run_phase3_intelligence(self, tender_batch: List[Dict[str, Any]]):
    """
    Background worker task for structured scoring, insights, and vector indexing.
    """
    if not tender_batch:
        return 0

    try:
        return run_phase3_intelligence_sync(tender_batch)
    except Exception as exc:
        logger.error(f"[Celery] Phase 3 pipeline failed: {exc}")
        raise self.retry(exc=exc, countdown=60)


@celery_app.task(bind=True, name="core.tasks.run_full_pipeline", max_retries=1)
def run_full_pipeline(self):
    """
    Scheduled daily task: run all enabled scrapers then process intelligence.
    Triggered automatically by Celery Beat at 02:00 UTC every day.
    """
    logger.info("[Pipeline] Starting scheduled full scrape run")
    try:
        from core.registry import auto_jobs
        from core.runner import JobRunner

        jobs = auto_jobs()
        logger.info("[Pipeline] %d portal jobs queued", len(jobs))

        runner = JobRunner()
        results = runner.run(jobs)

        all_tenders: List[Dict[str, Any]] = []
        for r in results:
            if r.tenders:
                all_tenders.extend(r.tenders)

        logger.info("[Pipeline] Scraped %d raw tenders across %d portals", len(all_tenders), len(results))

        if all_tenders:
            process_intelligence_batch_sync(all_tenders)

        logger.info("[Pipeline] Daily run complete")
        return {"scraped": len(all_tenders), "portals": len(results)}
    except Exception as exc:
        logger.error("[Pipeline] Daily run failed: %s", exc)
        raise self.retry(exc=exc, countdown=300)
