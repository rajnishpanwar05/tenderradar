# =============================================================================
# main.py — TenderRadar Unified Runner  (Phase 3 — pipeline orchestration)
#
# Usage:
#   python3 main.py                    # run all auto pipelines (no CAPTCHA)
#   python3 main.py --wb               # World Bank only
#   python3 main.py --gem              # GeM only
#   python3 main.py --devnet           # DevNet only
#   python3 main.py --cg               # CG eProcurement only
#   python3 main.py --giz              # GIZ India only
#   python3 main.py --undp             # UNDP Procurement Notices only
#   python3 main.py --meghalaya        # Meghalaya Basin Dev Authority only
#   python3 main.py --ngobox           # NGO Box RFP/EOI only
#   python3 main.py --iucn             # IUCN Procurement only
#   python3 main.py --whh              # Deutsche Welthungerhilfe only
#   python3 main.py --ungm             # UNGM (UN Global Marketplace) only
#   python3 main.py --sidbi            # SIDBI Tenders only
#   python3 main.py --afdb             # AfDB Consultants only
#   python3 main.py --afd              # AFD France only
#   python3 main.py --icfre            # ICFRE Tenders only
#   python3 main.py --phfi             # PHFI Tenders only
#   python3 main.py --jtds             # Jharkhand Tribal Dev Society only
#   python3 main.py --ted              # TED EU (Tenders Electronic Daily) only
#   python3 main.py --sam              # SAM.gov US federal opportunities only
#   python3 main.py --karnataka        # Karnataka eProcurement only
#   python3 main.py --usaid            # USAID sub-opportunities only
#   python3 main.py --dtvp             # DTVP German Vergabeportal only
#   python3 main.py --taneps           # TANEPS Tanzania only
#   python3 main.py --sikkim           # Sikkim (manual CAPTCHA required)
#   python3 main.py --nic              # NIC State Portals (manual CAPTCHA)
#   python3 main.py --portal wb        # single portal by flag
#   python3 main.py --portal wb --debug # single portal with verbose debug logging
#   python3 main.py --dry-run          # skip notification
#   python3 main.py --no-parallel      # sequential mode (debug / low-memory)
#   python3 main.py --debug            # enable verbose debug for selected portal(s)
#
# Cron (every 6 hours):
#   0 */6 * * * /usr/bin/python3 ~/tender_system/main.py >> ~/tender_system/run.log 2>&1
#
# What changed in Phase 3 vs Phase 1/2:
#   • Scrapers now run in parallel (ThreadPoolExecutor, max 7 workers)
#   • Selenium scrapers capped at 2 concurrent instances
#   • Each scraper retries automatically on crash or zero-row result
#   • Each scraper has a per-job timeout (no more infinite hangs)
#   • Structured JSON log: monitoring/tenderradar.log (rotating 10MB × 5)
#   • Post-run diagnostics table printed to run.log
#   • Optional Telegram health report on WARN/FAIL/TIMEOUT
#   • Adding a new portal: 1 line in pipeline/registry.py + 1 argparse line here
# =============================================================================

import argparse
import os
import sys
import time
from datetime import datetime

# ── Ensure we run from the tender_system directory ────────────────────────────
BASE_DIR = os.path.expanduser("~/tender_system")
sys.path.insert(0, BASE_DIR)
os.chdir(BASE_DIR)

# ── Structured logging — must be set up before any other tenderradar imports ─
from monitoring.logs   import setup_logging
_log = setup_logging()

import logging
logger = logging.getLogger("tenderradar.main")
from monitoring.sentry import init_sentry
init_sentry(service_name="tenderradar-pipeline")

# ── Pipeline infrastructure ───────────────────────────────────────────────────
from core.registry import resolve_run_list, all_jobs
from core.runner   import JobRunner
from core.reporter import RunReporter

# ── Core system modules ───────────────────────────────────────────────────────
from config.config   import (
    LOG_FILE,
    NOTIFICATIONS_ENABLED,
    email_configured,
    log_optional_service_status,
    validate as _validate_config,
)
from database.db     import (
    DatabasePreflightError,
    get_stats,
    init_db,
    preflight_db_connection,
)
from notifier        import notify_all, send_rich_alert


# =============================================================================
# LEGACY LOG HELPER — kept for run.log compat (cron reads this file)
# =============================================================================

def log(msg: str) -> None:
    """Print + append to run.log with timestamp. Used for top-level milestones."""
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass


def _build_canonical_new_tenders(results: list) -> list[dict]:
    """
    Normalize new-tender payloads at the orchestration boundary so downstream
    pipeline stages always receive a stable tender_id plus any richer fields
    present in the scraper's all_rows output.
    """
    try:
        from intelligence.normalizer import normalize_tender
    except Exception:
        normalize_tender = None

    canonical: list[dict] = []

    for result in results:
        rows_by_id = {}
        rows_by_url = {}
        rows_by_title = {}

        for row in getattr(result, "all_rows", []) or []:
            row_tid = str(row.get("tender_id") or row.get("Tender ID") or row.get("id") or "").strip()
            row_url = str(row.get("url") or row.get("URL") or "").strip()
            row_title = str(row.get("title") or row.get("Title") or "").strip().lower()
            if row_tid:
                rows_by_id[row_tid] = row
            if row_url:
                rows_by_url[row_url] = row
            if row_title:
                rows_by_title[row_title] = row

        for raw in getattr(result, "new_tenders", []) or []:
            payload = dict(raw or {})

            raw_tid = str(payload.get("tender_id") or payload.get("Tender ID") or payload.get("id") or "").strip()
            raw_url = str(payload.get("url") or payload.get("URL") or "").strip()
            raw_title = str(payload.get("title") or payload.get("Title") or "").strip().lower()

            match = None
            if raw_tid and raw_tid in rows_by_id:
                match = rows_by_id[raw_tid]
            elif raw_url and raw_url in rows_by_url:
                match = rows_by_url[raw_url]
            elif raw_title and raw_title in rows_by_title:
                match = rows_by_title[raw_title]

            if match:
                merged = dict(match)
                merged.update(payload)
                payload = merged

            source_hint = str(
                payload.get("source_portal")
                or payload.get("source")
                or result.flag
            ).strip()
            if source_hint:
                payload["source_portal"] = source_hint
                payload["source"] = source_hint

            tid = str(payload.get("tender_id") or payload.get("Tender ID") or payload.get("id") or "").strip()
            if normalize_tender:
                try:
                    normalized = normalize_tender(payload, tender_id=tid)
                    payload.setdefault("tender_id", normalized.tender_id)
                    payload.setdefault("title", normalized.title)
                    payload.setdefault("description", normalized.description)
                    payload.setdefault("organization", normalized.organization)
                    payload.setdefault("deadline", normalized.deadline_raw)
                    payload.setdefault("value", normalized.value_raw)
                    payload.setdefault("url", normalized.url)
                    payload.setdefault("source_portal", normalized.source_portal)
                    payload.setdefault("source", payload.get("source_portal") or normalized.source_portal)
                except Exception:
                    pass

            final_tid = str(payload.get("tender_id") or payload.get("id") or "").strip()
            if final_tid:
                payload["tender_id"] = final_tid
                canonical.append(payload)

    return canonical


# =============================================================================
# ARGUMENT PARSING
# =============================================================================

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TenderRadar — Tender Monitoring System")

    # ── One flag per portal (in registry order) ───────────────────────────────
    p.add_argument("--wb",        action="store_true", help="World Bank")
    p.add_argument("--gem",       action="store_true", help="GeM BidPlus")
    p.add_argument("--devnet",    action="store_true", help="DevNet India")
    p.add_argument("--cg",        action="store_true", help="CG eProcurement")
    p.add_argument("--giz",       action="store_true", help="GIZ India")
    p.add_argument("--undp",      action="store_true", help="UNDP Procurement")
    p.add_argument("--meghalaya", action="store_true", help="Meghalaya MBDA")
    p.add_argument("--ngobox",    action="store_true", help="NGO Box")
    p.add_argument("--iucn",      action="store_true", help="IUCN Procurement")
    p.add_argument("--whh",       action="store_true", help="Deutsche Welthungerhilfe")
    p.add_argument("--ungm",      action="store_true", help="UNGM")
    p.add_argument("--sidbi",     action="store_true", help="SIDBI Tenders")
    p.add_argument("--afdb",      action="store_true", help="AfDB Consultants")
    p.add_argument("--afd",       action="store_true", help="AFD France")
    p.add_argument("--icfre",     action="store_true", help="ICFRE Tenders")
    p.add_argument("--phfi",      action="store_true", help="PHFI Tenders")
    p.add_argument("--jtds",      action="store_true", help="JTDS Jharkhand")
    p.add_argument("--ted",       action="store_true", help="TED EU")
    p.add_argument("--sam",       action="store_true", help="SAM.gov")
    p.add_argument("--karnataka",   action="store_true", help="Karnataka eProcure")
    p.add_argument("--usaid",       action="store_true", help="USAID")
    p.add_argument("--maharashtra", action="store_true", help="Maharashtra Tenders")
    p.add_argument("--up",          action="store_true", help="UP eTenders")
    p.add_argument("--dtvp",      action="store_true", help="DTVP Germany")
    p.add_argument("--taneps",    action="store_true", help="TANEPS Tanzania")
    p.add_argument("--adb",         action="store_true", help="ADB (Asian Development Bank)")
    p.add_argument("--ec",          action="store_true", help="EC (European Commission)")
    p.add_argument("--devbusiness", action="store_true", help="Dev Business UN (devbusiness.un.org)")
    # Early-stage intelligence
    p.add_argument("--wb-early",  action="store_true",
                   help="World Bank Early Pipeline (pre-RFP project signals)")
    # Manual CAPTCHA — always explicit
    p.add_argument("--sikkim",    action="store_true", help="Sikkim (CAPTCHA)")
    p.add_argument("--nic",       action="store_true", help="NIC State Portals (CAPTCHA)")
    # ── Single-portal debug mode (Phase 4 plugin system) ─────────────────────
    p.add_argument(
        "--portal",
        metavar="FLAG",
        help=(
            "Run a single portal by flag, e.g. --portal wb  "
            "(see: python3 scripts/list_portals.py)"
        ),
    )
    # Runner options
    p.add_argument("--dry-run",     action="store_true", help="Skip notifications")
    p.add_argument("--no-parallel", action="store_true",
                   help="Run scrapers sequentially (debug / low-memory mode)")
    p.add_argument("--debug",       action="store_true",
                   help="Enable verbose debug logging for the selected portal(s). "
                        "Best used with --portal, e.g.: python3 main.py --portal wb --debug")
    return p


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    args = _build_parser().parse_args()

    # ── Validate config — fail fast before doing any work ────────────────────
    try:
        _validate_config(raise_on_error=True)
    except RuntimeError as exc:
        log(f"FATAL: {exc}")
        sys.exit(1)
    log_optional_service_status(logger)

    # ── EARLY-EXIT: --wb-early runs in complete isolation ─────────────────────
    # When this flag is present we skip resolve_run_list(), runner.run(),
    # the intelligence pipeline, and the unified Excel merge entirely.
    # Nothing below this block is executed.
    if getattr(args, "wb_early", False):
        log("=" * 65)
        log("TenderRadar — World Bank Early Pipeline (isolated run)")
        log("=" * 65)

        # Respect --dry-run
        if args.dry_run:
            import database.db as _db_module
            _db_module.DRY_RUN = True
            log("DRY-RUN mode active — DB writes disabled")

        # DB must be ready before the scraper touches it
        try:
            db_status = preflight_db_connection(debug=getattr(args, "debug", False))
            if db_status["database_exists"]:
                log(
                    f"DB preflight OK — MySQL reachable at {db_status['host']}:{db_status['port']} "
                    f"(database '{db_status['database']}' ready)"
                )
            else:
                log(
                    f"DB preflight OK — MySQL reachable at {db_status['host']}:{db_status['port']} "
                    f"(database '{db_status['database']}' will be created if needed)"
                )
            init_db()
        except DatabasePreflightError as exc:
            log(f"FATAL: {exc}")
            if getattr(args, "debug", False) and exc.debug_detail:
                log(f"DEBUG: {exc.debug_detail}")
            sys.exit(1)
        except Exception as exc:
            log(f"FATAL: Database init failed: {exc}")
            sys.exit(1)

        try:
            from scrapers.portals.wb_early_pipeline import (
                run      as _wbe_run,
                set_debug as _wbe_set_debug,
            )
            if getattr(args, "debug", False):
                _wbe_set_debug(True)
                log("[debug] World Bank Early Pipeline: debug mode ACTIVE")

            _wbe_new, _wbe_all = _wbe_run(
                debug=getattr(args, "debug", False)
            )
            log(f"World Bank Early Pipeline complete — "
                f"{len(_wbe_new)} new, {len(_wbe_all)} total")
        except Exception as exc:
            log(f"ERROR: World Bank Early Pipeline failed: {exc}")
            sys.exit(1)

        log("=" * 65)
        sys.exit(0)
    # ── END early-exit block ──────────────────────────────────────────────────

    jobs = resolve_run_list(args)

    run_start = time.time()
    log("=" * 65)
    log("TenderRadar — run started")
    log(f"  Portals  : {len(jobs)} ({', '.join(j.label for j in jobs[:6])}"
        f"{'…' if len(jobs) > 6 else ''})")
    from core.runner import JobRunner as _JR
    _mw = 1 if args.no_parallel else _JR.MAX_WORKERS
    log(f"  Parallel : {'NO (--no-parallel)' if args.no_parallel else f'YES (max {_mw} workers)'}")
    log(f"  Dry-run  : {args.dry_run}")
    log(f"  Debug    : {getattr(args, 'debug', False)}")
    log("=" * 65)

    # ── Debug mode — set portal-level debug flags before runner starts ────────
    if getattr(args, "debug", False):
        try:
            from scrapers.portals.worldbank_scraper import set_debug as _wb_set_debug
            _wb_set_debug(True)
            log("[debug] World Bank scraper: debug mode ACTIVE")
        except Exception:
            pass  # WB not in this run — ignore silently
        try:
            from scrapers.portals.ungm_scraper import set_debug as _ungm_set_debug
            _ungm_set_debug(True)
            log("[debug] UNGM scraper: debug mode ACTIVE")
        except Exception:
            pass  # UNGM not in this run — ignore silently
        try:
            from scrapers.portals.wb_early_pipeline import set_debug as _wbe_set_debug
            _wbe_set_debug(True)
            log("[debug] World Bank Early Pipeline: debug mode ACTIVE")
        except Exception:
            pass  # wb_early not in this run — ignore silently

    # ── Dry-run guard — set BEFORE init_db so mark_as_seen becomes a no-op ──
    if args.dry_run:
        import database.db as _db_module
        _db_module.DRY_RUN = True
        log("DRY-RUN mode: DB writes disabled, Excel exports active, alerts skipped")

    # ── DB init ───────────────────────────────────────────────────────────────
    try:
        db_status = preflight_db_connection(debug=getattr(args, "debug", False))
        if db_status["database_exists"]:
            log(
                f"DB preflight OK — MySQL reachable at {db_status['host']}:{db_status['port']} "
                f"(database '{db_status['database']}' ready)"
            )
        else:
            log(
                f"DB preflight OK — MySQL reachable at {db_status['host']}:{db_status['port']} "
                f"(database '{db_status['database']}' will be created if needed)"
            )
        init_db()
        logger.info("[main] Database initialised OK")
    except DatabasePreflightError as exc:
        log(f"FATAL: {exc}")
        if getattr(args, "debug", False) and exc.debug_detail:
            log(f"DEBUG: {exc.debug_detail}")
        sys.exit(1)
    except Exception as exc:
        log(f"FATAL: Database init failed: {exc}")
        sys.exit(1)

    # ── CAPTCHA reminder ──────────────────────────────────────────────────────
    if any(j.needs_captcha for j in jobs):
        log("  >>> CAPTCHA jobs detected — Chrome window will open.")
        log("  >>> Solve the CAPTCHA when prompted.")

    # ── Run all scrapers ──────────────────────────────────────────────────────
    runner = JobRunner()
    if args.no_parallel:
        runner.MAX_WORKERS = 1   # force sequential (one thread = sequential)

    results = runner.run(jobs)
    logger.info(f"[main] All {len(results)} jobs complete")
    canonical_new_tenders = _build_canonical_new_tenders(results)

    # ── Scraper health tracking ────────────────────────────────────────────────
    # Records run results into health.db and auto-flags unstable scrapers.
    # Non-fatal: any failure logs a warning and run continues.
    if not args.dry_run:
        try:
            from monitoring.scraper_health_manager import (
                record_run_results as _health_record,
                update_confidence_scores as _confidence_update,
            )
            _health_record(results)
            log(f"Scraper health: recorded {len(results)} run result(s)")
        except Exception as _she:
            log(f"WARNING: Scraper health tracking failed (non-fatal): {_she}")

    # ── Bid Pipeline — register new tenders as 'discovered' ───────────────────
    # Uses INSERT IGNORE so existing pipeline entries are never overwritten.
    # Non-fatal: failure logs a warning and the run continues.
    if not args.dry_run:
        try:
            from pipeline.opportunity_pipeline import ensure_pipeline_entry_batch as _bp_batch
            _new_rows = canonical_new_tenders
            _new_tids = [
                str(t.get("tender_id") or t.get("id") or "").strip()
                for t in _new_rows
            ]
            _model_tags = {}
            for t in _new_rows:
                _tid = str(t.get("tender_id") or t.get("id") or "").strip()
                if not _tid:
                    continue
                _tag = (
                    t.get("model_decision_tag")
                    or t.get("decision_tag")
                    or t.get("bid_tag")
                )
                if _tag:
                    _model_tags[_tid] = str(_tag).strip()
            if _new_tids:
                log(f"Bid pipeline: registering {len(_new_tids)} new tender(s)…")
                _bp_added = _bp_batch(_new_tids, model_tags=_model_tags or None)
                if _bp_added >= 0:
                    log(f"Bid pipeline: {_bp_added} entry(ies) added as 'discovered'")
        except Exception as _bp_exc:
            log(f"WARNING: Bid pipeline tracking failed (non-fatal): {_bp_exc}")

    # ── Aggregate new tenders once for downstream enrichment / notifications ──
    all_new_tenders = canonical_new_tenders

    # ── Cross-portal fuzzy deduplication ─────────────────────────────────────
    # Compares new tenders against last 60 days in DB using fuzzy title+org+deadline
    # matching. Marks duplicates, merges unique fields into the canonical record,
    # and populates tender_cross_sources so the detail page can show all portal links.
    # Must run BEFORE intelligence pipeline so dedup flags are set before scoring.
    # Non-fatal: failure logs a warning only.
    if not args.dry_run:
        if all_new_tenders:
            try:
                from intelligence.fuzzy_dedup import (
                    deduplicate_against_db as _fuzz_dedup,
                    apply_db_merges        as _fuzz_merge,
                )
                log(f"Fuzzy dedup: checking {len(all_new_tenders)} new tender(s) "
                    f"against DB (60-day window)…")
                _unique_new, _updates = _fuzz_dedup(all_new_tenders, lookback_days=60)
                _merged = _fuzz_merge(_updates)
                all_new_tenders = _unique_new
                if _merged:
                    log(f"Fuzzy dedup: {_merged} duplicate record(s) merged; {len(all_new_tenders)} unique new tender(s) remain")
                else:
                    log("Fuzzy dedup: no cross-portal duplicates found")
            except Exception as _fd_exc:
                log(f"WARNING: Fuzzy dedup failed (non-fatal): {_fd_exc}")

    # ── Early normalization/materialization into `tenders` ───────────────────
    # Persist raw scraper output immediately so descriptions/URLs are retained
    # even if later AI stages fail. This closes the raw → normalized gap.
    if not args.dry_run:
        try:
            from database.db import materialize_normalized_batch, backfill_normalized_from_seen_tenders
            _all_scraped_rows = [row for r in results for row in r.all_rows]
            if _all_scraped_rows:
                _mat = materialize_normalized_batch(_all_scraped_rows)
                log(
                    f"Normalized materialization: {_mat['saved']}/{_mat['total']} rows saved "
                    f"({ _mat['failed'] } failed)"
                )
            _norm_backfill = backfill_normalized_from_seen_tenders(limit=5000)
            if _norm_backfill:
                log(f"Normalized materialization: {_norm_backfill} historical row(s) backfilled from seen_tenders")
        except Exception as _nm_exc:
            log(f"WARNING: normalized materialization failed (non-fatal): {_nm_exc}")

    # ── Intelligence pipeline — track failures for cumulative health check ───────
    _intel_failures: list = []  # collect layer names that fail

    # ── Phase 3 intelligence — run synchronously when Celery is not active ─────
    if not args.dry_run:
        try:
            from core.tasks import run_phase3_intelligence_sync
            from intelligence.tender_intelligence import backfill_from_seen_tenders
            if all_new_tenders:
                log(f"Phase 3 intelligence: processing {len(all_new_tenders)} new tender(s) synchronously…")
                _phase3_done = run_phase3_intelligence_sync(all_new_tenders)
                log(f"Phase 3 intelligence: {_phase3_done} tender(s) processed")

            log("Structured intelligence: backfilling un-enriched historical tenders (max 1000)…")
            _backfill_written = backfill_from_seen_tenders(limit=1000)
            if _backfill_written:
                log(f"Structured intelligence: {_backfill_written} historical record(s) backfilled")
        except Exception as queue_exc:
            log(f"WARNING: Phase 3 intelligence failed: {queue_exc}")
            _intel_failures.append("phase3")

    # ── Opportunity insights backfill ─────────────────────────────────────────
    # Generates rule-based strategic insights for any tender_structured_intel
    # rows that are missing opportunity_insight text.  Runs after phase 3 so
    # scoring is already present.  Cap at 500 per run to keep it fast.
    if not args.dry_run:
        try:
            from intelligence.opportunity_insights import backfill as _oi_backfill
            _oi_written = _oi_backfill(limit=500)
            if _oi_written:
                log(f"Opportunity insights: {_oi_written} insight(s) generated")
        except Exception as _oi_exc:
            log(f"WARNING: opportunity insights backfill failed (non-fatal): {_oi_exc}")

    # ── Data confidence scores ─────────────────────────────────────────────────
    # Writes data_confidence_score per tender to tender_structured_intel.
    # Based on: source success_rate + completeness + deadline + org + sector.
    # Non-fatal: any failure logs a warning only.
    if not args.dry_run:
        try:
            from monitoring.scraper_health_manager import update_confidence_scores as _cs_update
            _cs_rows = [row for r in results for row in r.all_rows]
            if _cs_rows:
                _cs_written = _cs_update(_cs_rows)
                log(f"Data confidence scores: updated {_cs_written} tender(s)")
        except Exception as _cs_exc:
            log(f"WARNING: Data confidence scoring failed (non-fatal): {_cs_exc}")

    # ── Intelligence pipeline health check ────────────────────────────────────
    # If 2+ layers failed in the same run, something systemic is wrong.
    # Log a clear ALERT so it's visible in run.log / Telegram.
    if len(_intel_failures) >= 2:
        log(
            f"ALERT: {len(_intel_failures)}/4 intelligence layers FAILED this run: "
            f"{', '.join(_intel_failures)}. "
            f"Tenders may be un-scored and un-indexed. "
            f"Check run.log and restart after fixing."
        )
    elif _intel_failures:
        log(f"NOTE: Intelligence layer '{_intel_failures[0]}' had a non-fatal error this run.")

    # ── Unified Excel export (all portals, sector/service columns) ────────────
    excel_path = ""
    _digest_packaging_nonfatal_error = False

    try:
        from exporters.excel_exporter import write_unified_excel
        excel_path = write_unified_excel(results, dry_run=args.dry_run)
        if excel_path:
            log(f"Unified Excel written: {excel_path}")
        else:
            log("WARNING: unified Excel export returned empty path (no rows?)")
    except Exception as exc:
        log(f"WARNING: unified Excel export failed (non-fatal): {exc}")

    # ── Evidence packaging (new shortlisted tenders only) ─────────────────────
    # Reads master Excel, classifies evidence state, writes per-tender packages.
    # Non-fatal: any failure logs a warning and the pipeline continues.
    # Does NOT modify any Excel files or labeling columns.
    try:
        from exporters.evidence_packager import package_new_shortlisted
        _pkg_result = package_new_shortlisted(dry_run=args.dry_run)
        _pkg_n = _pkg_result.get("packaged_count", 0)
        _pkg_ev = _pkg_result.get("evidence_summary", {})
        if _pkg_n > 0:
            log(f"Evidence packaging: {_pkg_n} package(s) created → "
                f"{_pkg_result.get('run_dir', '')} | {_pkg_ev}")
        else:
            log("Evidence packaging: no new shortlisted tenders to package")
    except Exception as exc:
        _digest_packaging_nonfatal_error = True
        log(f"WARNING: evidence packaging failed (non-fatal): {exc}")

    # ── World Bank Early Pipeline (pre-RFP project intelligence) ─────────────
    # Runs independently of the main scraper loop.
    # Triggered by --wb-early flag OR included in default all-portal run.
    # Non-fatal: any failure logs a warning and the main run continues.
    _run_wb_early = getattr(args, "wb_early", False) or (
        not any([
            args.wb, args.gem, args.devnet, args.cg, args.giz, args.undp,
            args.meghalaya, args.ngobox, args.iucn, args.whh, args.ungm,
            args.sidbi, args.afdb, args.afd, args.icfre, args.phfi,
            args.jtds, args.ted, args.sam, args.karnataka, args.usaid,
            getattr(args, "maharashtra", False),
            getattr(args, "up", False),
            args.dtvp, args.taneps,
            getattr(args, "adb", False),
            getattr(args, "ec",  False),
            args.sikkim, args.nic,
            getattr(args, "portal", None),
        ])
    )
    if _run_wb_early and not args.dry_run:
        try:
            from scrapers.portals.wb_early_pipeline import run as _wbe_run
            log("World Bank Early Pipeline: scanning for pre-RFP signals…")
            _wbe_new, _wbe_all = _wbe_run(debug=getattr(args, "debug", False))
            log(f"World Bank Early Pipeline: {len(_wbe_new)} new project(s), "
                f"{len(_wbe_all)} total with consulting signals")
        except Exception as _wbe_exc:
            log(f"WARNING: World Bank Early Pipeline failed (non-fatal): {_wbe_exc}")
    elif getattr(args, "wb_early", False) and args.dry_run:
        log("DRY-RUN: World Bank Early Pipeline skipped (dry-run mode)")

    # ── Excel feedback sync (read My Decision / Outcome → DB) ─────────────────
    # Reads any BID / NO / LATER decisions and WON / LOST outcomes the user
    # has filled in the master Excel and persists them to bid_pipeline so the
    # calibrator can track accuracy over time.
    if not args.dry_run:
        try:
            from exporters.excel_feedback_sync import (
                sync_excel_feedback,
                compute_feedback_metrics,
                print_feedback_summary,
            )
            _sync_result = sync_excel_feedback()
            if _sync_result.get("synced", 0) > 0 or _sync_result.get("total", 0) > 0:
                log(f"Excel feedback sync: {_sync_result['note']}")
                _fb_metrics = compute_feedback_metrics()
                print_feedback_summary(_fb_metrics)
            else:
                log("Excel feedback sync: no decisions found in master Excel "
                    "(fill 'My Decision' / 'Outcome' columns to enable learning)")
        except Exception as exc:
            log(f"WARNING: Excel feedback sync failed (non-fatal): {exc}")

    # ── Weekly feedback learning + evaluation ─────────────────────────────────
    # Trains lightweight bid/no-bid model from historical feedback and computes
    # ranking quality metrics (precision@k / recall@k / ndcg / decision accuracy).
    # Non-fatal: failures are logged only.
    if not args.dry_run:
        try:
            from pipeline.learning_pipeline import maybe_run_weekly_learning
            _lp = maybe_run_weekly_learning(force=False)
            if _lp.get("skipped"):
                log(f"Learning pipeline: {_lp.get('note', 'skipped')}")
            elif _lp.get("ok"):
                log(
                    "Learning pipeline: "
                    f"decision_acc={_lp.get('decision_accuracy', 0):.3f}, "
                    f"p@k={_lp.get('precision_at_k', 0):.3f}, "
                    f"r@k={_lp.get('recall_at_k', 0):.3f}, "
                    f"ndcg@k={_lp.get('ndcg_at_k', 0):.3f}"
                )
            else:
                log(f"Learning pipeline: {_lp.get('note', 'no data')}")
        except Exception as _lp_exc:
            log(f"WARNING: Learning pipeline failed (non-fatal): {_lp_exc}")

    # ── Phase 1/2 intelligence — synchronous deep enrichment + AI enrichment ──
    enriched = []
    if all_new_tenders and not args.dry_run:
        log(f"Phase 1/2 intelligence: processing {len(all_new_tenders)} new tender(s) synchronously…")
        try:
            from core.tasks import process_intelligence_batch_sync
            _sync_result = process_intelligence_batch_sync(all_new_tenders)
            enriched = _sync_result.get("enriched", [])
            _deep_saved = int(_sync_result.get("deep_saved", 0))
            _deep_amended = _sync_result.get("deep_amended", [])
            log(f"Phase 1/2 intelligence: {len(enriched)} tender(s) enriched; deep fields saved for {_deep_saved}")

            if _deep_amended and NOTIFICATIONS_ENABLED:
                log(f"Amendment detection: {len(_deep_amended)} tender(s) changed content since last scrape — sending alert…")
                try:
                    from notifier import send_amendment_alert as _amend_alert
                    _amend_ok = _amend_alert(_deep_amended)
                    log(f"Amendment alert {'sent OK' if _amend_ok else 'FAILED'}")
                except Exception as _amend_exc:
                    log(f"WARNING: Amendment alert failed (non-fatal): {_amend_exc}")
            elif _deep_amended:
                log(f"Amendment detection: {len(_deep_amended)} tender(s) changed content — alerts disabled (NOTIFICATIONS_ENABLED=false)")
        except Exception as exc:
            log(f"WARNING: Phase 1/2 intelligence failed: {exc}")

    tender_notification_status = "NONE"

    # ── Notifications (unchanged from Phase 1/2) ──────────────────────────────
    # Group new tenders by scraper flag for notifications
    from collections import defaultdict
    by_flag = defaultdict(list)
    for r in results:
        try:
            flag = getattr(r, "flag", None)
            if not flag:
                continue
            for t in (getattr(r, "new_rows", []) or []):
                by_flag[flag].append(t)
        except Exception:
            continue

    total_new = len(all_new_tenders)

    if not NOTIFICATIONS_ENABLED:
        log("Notifications DISABLED — skipping.")
        tender_notification_status = "DISABLED"
    elif total_new == 0:
        log("No new tenders — skipping tender notification.")
        tender_notification_status = "NO-NEW"
    elif args.dry_run:
        log(f"DRY-RUN: Would send notification for {total_new} new tender(s).")
        tender_notification_status = "DRY-RUN"
    elif not email_configured():
        log("Notifications skipped — email delivery is disabled by SMTP config.")
        tender_notification_status = "SKIPPED-EMAIL-CONFIG"
    else:
        log(f"Sending tender notification for {total_new} new tender(s)…")
        try:
            if enriched:
                ok = send_rich_alert(enriched)
            else:
                # Pass each source bucket via **kwargs — notify_all defaults unknown to []
                ok = notify_all(**{f"new_{flag}": tenders
                                   for flag, tenders in by_flag.items()})
            log(f"Tender notification {'sent OK' if ok else 'FAILED'}")
            tender_notification_status = "SENT" if ok else "FAILED"
        except Exception as exc:
            tender_notification_status = "CRASHED"
            log(f"ERROR: notification crashed: {exc}")

    # ── DB stats ───────────────────────────────────────────────────────────────
    _db_total = 0
    try:
        stats = get_stats()
        _db_total = sum(stats.values())
        log("Database stats (seen_tenders):")
        for site, count in sorted(stats.items(), key=lambda x: -x[1])[:10]:
            log(f"  {site:<22} : {count:>5}")
        if len(stats) > 10:
            log(f"  … and {len(stats) - 10} more sources")
    except Exception as exc:
        log(f"ERROR: get_stats failed: {exc}")

    # ── End-of-run summary ────────────────────────────────────────────────────
    try:
        _total_new    = len(all_new_tenders)
        _total_scraped = sum(len(r.all_rows) for r in results)

        # Enrichment coverage from DB
        try:
            from database.db import get_connection as _gc
            _ec = _gc()
            _cur = _ec.cursor()
            _cur.execute(
                "SELECT COUNT(*) FROM tender_structured_intel "
                "WHERE sector != 'unknown' AND sector != ''"
            )
            _enriched_count = (_cur.fetchone() or (0,))[0]
            _cur.execute("SELECT COUNT(*) FROM seen_tenders")
            _total_seen = (_cur.fetchone() or (0,))[0]
            _cur.close(); _ec.close()
            _coverage_pct = round(_enriched_count / _total_seen * 100, 1) if _total_seen else 0
        except Exception:
            _enriched_count = 0; _total_seen = _db_total; _coverage_pct = 0.0

        # Top priority score from enriched tenders this run
        _top_priority = 0
        try:
            from database.db import get_connection as _gc2
            _pc = _gc2()
            _pcur = _pc.cursor()
            _pcur.execute(
                "SELECT COALESCE(MAX(priority_score), 0) FROM tender_structured_intel"
            )
            _top_priority = (_pcur.fetchone() or (0,))[0]
            _pcur.close(); _pc.close()
        except Exception:
            pass

        _notif_sent = tender_notification_status

        log("")
        log("╔" + "═" * 55 + "╗")
        log("║  RUN SUMMARY" + " " * 42 + "║")
        log("╠" + "═" * 55 + "╣")
        log(f"║  Total in DB        : {_total_seen:<32}║")
        log(f"║  Scraped this run   : {_total_scraped:<32}║")
        log(f"║  New this run       : {_total_new:<32}║")
        log(f"║  Enrichment cover   : {_enriched_count}/{_total_seen} ({_coverage_pct}%){' ' * max(0, 20 - len(str(_enriched_count)) - len(str(_total_seen)) - len(str(_coverage_pct)))}║")
        log(f"║  Top priority score : {_top_priority:<32}║")
        log(f"║  Notifications      : {_notif_sent:<32}║")
        log("╠" + "═" * 55 + "╣")

        # ── Quality section ───────────────────────────────────────────────
        try:
            from database.db import get_connection as _gc3
            _qc = _gc3(); _qcur = _qc.cursor()
            # Average priority (exclude zeros — they're un-scored, not low quality)
            _qcur.execute(
                "SELECT AVG(priority_score), COUNT(*) "
                "FROM tender_structured_intel WHERE priority_score > 0"
            )
            _qrow = _qcur.fetchone()
            _avg_p    = round(float(_qrow[0] or 0), 1)
            _scored_n = int(_qrow[1] or 0)
            # High priority count (>75)
            _qcur.execute(
                "SELECT COUNT(*) FROM tender_structured_intel WHERE priority_score > 75"
            )
            _high_n = (_qcur.fetchone() or (0,))[0]
            # Top 5 scores
            _qcur.execute(
                "SELECT si.priority_score, st.title "
                "FROM tender_structured_intel si "
                "JOIN seen_tenders st USING (tender_id) "
                "WHERE si.priority_score > 0 "
                "ORDER BY si.priority_score DESC LIMIT 5"
            )
            _top5 = _qcur.fetchall() or []
            _qcur.close(); _qc.close()

            log(f"║  QUALITY METRICS ({_scored_n} scored tenders){' ' * max(0, 26 - len(str(_scored_n)))}║")
            log(f"║  Avg priority score : {_avg_p:<32}║")
            log(f"║  High priority (>75): {_high_n:<32}║")
            if _top5:
                log(f"║  Top 5 scores       :{' ' * 33}║")
                for _sc, _ti in _top5:
                    _ti_s = (_ti or "")[:38]
                    log(f"║    {_sc:>3}  {_ti_s:<47}║")
        except Exception as _qe:
            log(f"║  Quality metrics    : unavailable ({_qe}){'':>5}║")

        log("╚" + "═" * 55 + "╝")
        log("")
    except Exception as _sum_exc:
        log(f"WARNING: summary block failed (non-fatal): {_sum_exc}")

    # ── Diagnostics report ─────────────────────────────────────────────────────
    reporter = RunReporter(results, run_start)
    log("")
    reporter.log_summary()       # ASCII table → run.log + tenderradar.log

    if not args.dry_run:
        reporter.send_health_telegram(total_new=total_new)

    if not args.dry_run:
        try:
            from config.config import (
                AUTO_DAILY_DIGEST,
                DAILY_DIGEST_ATTACH_PACKAGES,
                DAILY_DIGEST_DRY_RUN,
                DAILY_DIGEST_MAX_PACKAGES,
            )
            if AUTO_DAILY_DIGEST:
                log("[digest] auto-email enabled")
                if DAILY_DIGEST_DRY_RUN:
                    log("[digest] running in dry-run mode")

                _excel_ready = bool(excel_path and os.path.exists(excel_path))
                if not _excel_ready:
                    log("[digest] skipped — master Excel not available")
                else:
                    from notifier.daily_digest import send_daily_digest

                    _digest_result = send_daily_digest(
                        workbook_path=excel_path,
                        include_packages=DAILY_DIGEST_ATTACH_PACKAGES,
                        max_package_count=DAILY_DIGEST_MAX_PACKAGES,
                        dry_run=DAILY_DIGEST_DRY_RUN,
                    )

                    if _digest_result.get("skipped_no_new"):
                        log("[digest] skipped — no new tenders")
                    elif _digest_result.get("duplicate_blocked"):
                        log("[digest] skipped — duplicate already sent")
                    elif _digest_result.get("ok"):
                        log("[digest] sent successfully")
                    else:
                        log("[digest] failed: digest returned unsuccessful status")

                    if _digest_packaging_nonfatal_error and DAILY_DIGEST_ATTACH_PACKAGES:
                        log("[digest] note: packaging had a non-fatal error earlier; package attachment may be partial or absent")
            else:
                log("[digest] auto-email disabled")
        except Exception as exc:
            log(f"[digest] failed: {exc}")

    elapsed = time.time() - run_start
    log(f"Run complete in {elapsed / 60:.1f} min")
    log("=" * 65)


if __name__ == "__main__":
    main()
