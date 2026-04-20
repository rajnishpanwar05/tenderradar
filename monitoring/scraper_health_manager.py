# =============================================================================
# monitoring/scraper_health_manager.py — Scraper Reliability & Stability Layer
#
# Builds on top of monitoring/health_report.py (existing SQLite run history).
#
# WHAT IT ADDS vs. the existing health_report:
#   Part 1 — Rich per-scraper metrics (success_rate, avg_rows, consecutive_failures)
#   Part 2 — Auto-disable unstable scrapers (3 zero-row runs OR success_rate < 30%)
#   Part 3 — Smart retry advice (don't retry if portal historically returns 0)
#   Part 4 — data_confidence_score computation for tender records
#
# NO modifications to: scrapers, core/runner.py, or any intelligence module.
# Integrates ONLY through:
#   • main.py  — record_run_results(results) called post-run (one block, non-fatal)
#   • GET /api/v1/health — returns this module's output
#
# Integration point for Part 3 (optional future runner hook):
#   from monitoring.scraper_health_manager import should_smart_retry
#   if not should_smart_retry(job.label, rows=0):
#       return current_result   # skip retry
#
# =============================================================================

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger("tenderradar.health_manager")

# Reuse the same SQLite DB as health_report.py
_HEALTH_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "monitoring", "health.db",
)

# =============================================================================
# THRESHOLDS — tune here without touching any other file
# =============================================================================

#: Mark scraper unstable after this many consecutive zero-row runs
UNSTABLE_CONSECUTIVE_ZEROS: int = 5

#: Auto-expire unstable flags after this many hours so sources get another
#: chance without requiring manual intervention forever.
UNSTABLE_TTL_HOURS: int = 24

#: Mark scraper unstable if success_rate (last 10 runs) drops below this
UNSTABLE_MIN_SUCCESS_RATE: float = 30.0   # percent

#: Window for success_rate calculation
SUCCESS_RATE_WINDOW: int = 10

#: A scraper that historically returns 0 rows ≥ this % of the time is a
#: "structurally zero" source — don't retry on zero rows (Part 3).
STRUCTURAL_ZERO_THRESHOLD: float = 60.0  # percent

# Portals that are officially closed or expected to return zero rows.
# Zero-row runs from these sources should not trigger instability.
STRUCTURAL_ZERO_SOURCES = {
    "USAID",
    # PHFI posts tenders intermittently — weeks of zero rows is normal.
    "PHFI Tenders",
    "PHFI",
    # ADB is Cloudflare-blocked — disabled in enabled_portals.json but kept
    # here in case it gets re-enabled temporarily during testing.
    "ADB (Asian Dev Bank)",
    "ADB",
}

_SOURCE_ALIASES = {
    "world bank": ["World Bank", "worldbank", "wb"],
    "ted eu": ["TED EU", "TED-EU", "ted", "tedeu"],
    "afdb consultants": ["AfDB Consultants", "AfDB", "afdb"],
    "afd france": ["AFD France", "AFD", "afd"],
    "ungm": ["UNGM", "ungm"],
    "usaid": ["USAID", "usaid"],
    "gem bidplus": ["GeM BidPlus", "GeM", "gem"],
    "devnet india": ["DevNet India", "DevNet", "devnet"],
    "cg eprocurement": ["CG eProcurement", "CG", "cg"],
    "undp procurement": ["UNDP Procurement", "UNDP", "undp"],
    "meghalaya mbda": ["Meghalaya MBDA", "MBDA", "meghalaya"],
    "iucn procurement": ["IUCN Procurement", "IUCN", "iucn"],
    "sidbi tenders": ["SIDBI Tenders", "SIDBI", "sidbi"],
    "icfre tenders": ["ICFRE Tenders", "ICFRE", "icfre"],
    "phfi tenders": ["PHFI Tenders", "PHFI", "phfi"],
    "jtds jharkhand": ["JTDS Jharkhand", "JTDS", "jtds"],
    "maharashtra tenders": ["Maharashtra Tenders", "Maharashtra", "maharashtra"],
    "up etenders": ["UP eTenders", "UP eTender", "UP", "up", "upetender"],
    "taneps tanzania": ["TANEPS Tanzania", "TANEPS", "taneps"],
    "giz india": ["GIZ India", "GIZ", "giz"],
    "ngo box": ["NGO Box", "NGOBox", "ngobox"],
    "welthungerhilfe": ["Welthungerhilfe", "whh"],
    "karnataka eprocure": ["Karnataka eProcure", "Karnataka", "karnataka"],
    "dtvp germany": ["DTVP Germany", "DTVP", "dtvp"],
    "ilo procurement": ["ILO Procurement", "ILO", "ilo"],
    "sam.gov": ["SAM.gov", "SAM", "sam"],
    "european commission (ec)": ["European Commission (EC)", "European Commission", "EC", "ec"],
    "adb (asian dev bank)": ["ADB (Asian Dev Bank)", "ADB", "adb"],
}


def _is_noise_source(source: str) -> bool:
    key = str(source or "").strip().lower()
    if not key:
        return True
    return key.startswith("test_") or key in {"test warn", "test source", "dummy", "sample"}


# =============================================================================
# INTERNAL SQLITE HELPERS
# =============================================================================

def _get_conn() -> sqlite3.Connection:
    """Open the shared health.db, creating the unstable_scrapers table if needed."""
    os.makedirs(os.path.dirname(_HEALTH_DB), exist_ok=True)
    conn = sqlite3.connect(_HEALTH_DB, timeout=10)
    conn.row_factory = sqlite3.Row

    # Extend the existing schema — safe to call every time
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS unstable_scrapers (
            source    TEXT PRIMARY KEY,
            marked_at TEXT NOT NULL,
            reason    TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS scraper_runs_v2 (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source      TEXT    NOT NULL,
            run_at      TEXT    NOT NULL,
            row_count   INTEGER NOT NULL DEFAULT 0,
            new_count   INTEGER NOT NULL DEFAULT 0,
            status      TEXT    NOT NULL DEFAULT 'ok',
            error_msg   TEXT    DEFAULT '',
            duration_s  REAL    NOT NULL DEFAULT 0.0,
            attempts    INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_v2_source ON scraper_runs_v2(source);
        CREATE INDEX IF NOT EXISTS idx_v2_run_at ON scraper_runs_v2(run_at);
    """)
    conn.commit()
    return conn


# =============================================================================
# PART 1 — Record a run + compute rich per-scraper metrics
# =============================================================================

def record_run_results(results: list) -> int:
    """
    Persist JobResult objects produced by core/runner.JobRunner into
    scraper_runs_v2 and immediately evaluate stability.

    This is the ONLY call that main.py needs to make (one non-fatal block).

    Args:
        results: List[core.runner.JobResult]

    Returns:
        Number of rows recorded.
    """
    if not results:
        return 0

    now = datetime.utcnow().isoformat()
    rows_to_insert = []

    for r in results:
        # JobResult fields: flag, label, new_tenders, all_rows,
        #                   elapsed, status, error, attempts
        source    = getattr(r, "label", getattr(r, "flag", "unknown"))
        row_count = len(getattr(r, "all_rows", []))
        new_count = len(getattr(r, "new_tenders", []))
        status    = getattr(r, "status", "unknown")   # ok|warn|fail|timeout|skip
        error_msg = getattr(r, "error",  "")
        duration  = float(getattr(r, "elapsed", 0.0))
        attempts  = int(getattr(r, "attempts",  1))

        if source in STRUCTURAL_ZERO_SOURCES and row_count == 0:
            # Treat expected zero-row runs as neutral so health doesn't degrade.
            status = "skip"
            if not error_msg:
                error_msg = "structural zero (expected)"

        rows_to_insert.append((
            source, now, row_count, new_count,
            status, error_msg, duration, attempts,
        ))

    try:
        conn = _get_conn()
        conn.executemany(
            """INSERT INTO scraper_runs_v2
               (source, run_at, row_count, new_count, status, error_msg, duration_s, attempts)
               VALUES (?,?,?,?,?,?,?,?)""",
            rows_to_insert,
        )
        conn.commit()

        # Evaluate stability for each scraper after recording
        sources = {r[0] for r in rows_to_insert}
        for src in sources:
            _evaluate_stability(conn, src)

        conn.close()
        logger.info(
            "[health_manager] Recorded %d run result(s) and evaluated stability.",
            len(rows_to_insert),
        )
        return len(rows_to_insert)

    except Exception as exc:
        logger.warning("[health_manager] record_run_results failed (non-fatal): %s", exc)
        return 0


# =============================================================================
# PART 2 — Auto-disable unstable scrapers
# =============================================================================

def _evaluate_stability(conn: sqlite3.Connection, source: str) -> None:
    """
    Check whether a scraper should be flagged as unstable and persist the decision.
    Called automatically after every run recording.

    Unstable if EITHER:
      A) consecutive_failures >= UNSTABLE_CONSECUTIVE_ZEROS
      B) success_rate (last N runs) < UNSTABLE_MIN_SUCCESS_RATE
    """
    metrics = _compute_metrics(conn, source, window=SUCCESS_RATE_WINDOW)
    consec  = metrics["consecutive_failures"]
    rate    = metrics["success_rate"]

    reason = None
    if consec >= UNSTABLE_CONSECUTIVE_ZEROS:
        reason = (
            f"{consec} consecutive zero-row runs "
            f"(threshold: {UNSTABLE_CONSECUTIVE_ZEROS})"
        )
    elif rate < UNSTABLE_MIN_SUCCESS_RATE and metrics["total_runs"] >= 3:
        reason = (
            f"success_rate={rate:.0f}% < {UNSTABLE_MIN_SUCCESS_RATE:.0f}% "
            f"(last {SUCCESS_RATE_WINDOW} runs)"
        )

    # If the most recent run succeeded with rows, treat as recovered.
    try:
        latest = conn.execute(
            """SELECT row_count, status FROM scraper_runs_v2
               WHERE source = ?
               ORDER BY run_at DESC
               LIMIT 1""",
            (source,),
        ).fetchone()
        if latest and latest["status"] == "ok" and int(latest["row_count"] or 0) > 0:
            reason = None
    except Exception:
        pass

    if reason:
        # Mark as unstable (INSERT OR REPLACE so we always update timestamp + reason)
        conn.execute(
            "INSERT OR REPLACE INTO unstable_scrapers (source, marked_at, reason) "
            "VALUES (?, ?, ?)",
            (source, datetime.utcnow().isoformat(), reason),
        )
        conn.commit()
        logger.warning(
            "[health_manager] UNSTABLE: %s — %s — will be skipped next run.",
            source, reason,
        )
    else:
        # Clear unstable flag if scraper has recovered
        conn.execute(
            "DELETE FROM unstable_scrapers WHERE source = ?", (source,)
        )
        conn.commit()


def _compute_metrics(
    conn: sqlite3.Connection,
    source: str,
    window: int = 10,
) -> Dict[str, Any]:
    """
    Compute health metrics for one scraper using the last `window` runs.
    """
    rows = conn.execute(
        """SELECT row_count, status FROM scraper_runs_v2
           WHERE source = ?
           ORDER BY run_at DESC
           LIMIT ?""",
        (source, window),
    ).fetchall()

    effective_rows = [r for r in rows if r["status"] != "skip"]

    if not effective_rows:
        return {
            "total_runs":           0,
            "success_rate":         100.0,
            "average_rows":         0.0,
            "consecutive_failures": 0,
        }

    total   = len(effective_rows)
    ok_runs = sum(1 for r in effective_rows if r["status"] == "ok")
    rate    = round((ok_runs / total) * 100, 1) if total else 100.0
    avg_rows = round(sum(r["row_count"] for r in effective_rows) / total, 1) if total else 0.0

    consec = 0
    for r in effective_rows:   # rows are newest-first
        if r["row_count"] == 0:
            consec += 1
        else:
            break

    return {
        "total_runs":           total,
        "success_rate":         rate,
        "average_rows":         avg_rows,
        "consecutive_failures": consec,
    }


# =============================================================================
# PART 3 — Smart retry advice (for optional runner.py integration)
# =============================================================================

def should_smart_retry(source: str, current_rows: int = 0) -> bool:
    """
    Return True if the runner SHOULD retry a zero-row result.
    Return False if this scraper is known to be "structurally zero" (e.g.
    it simply didn't find new tenders this run — retrying wastes time).

    Integration point — add to core/runner.py _execute_with_retry():
        from monitoring.scraper_health_manager import should_smart_retry
        if self.ZERO_RETRY and not should_smart_retry(job.label, current_rows):
            # Structurally zero source — skip retry to save time
            return current_zero_result

    Only applies when current_rows == 0.  If an exception was raised, always retry.
    """
    if current_rows != 0:
        return True   # rows found — no retry needed

    try:
        conn = _get_conn()
        runs = conn.execute(
            """SELECT row_count, status FROM scraper_runs_v2
               WHERE source = ?
               ORDER BY run_at DESC
               LIMIT ?""",
            (source, SUCCESS_RATE_WINDOW),
        ).fetchall()
        conn.close()

        effective = [r for r in runs if r["status"] != "skip"]
        if len(effective) < 3:
            return True   # not enough history — default to retry

        zero_pct = (sum(1 for r in effective if r["row_count"] == 0) / len(effective)) * 100
        if zero_pct >= STRUCTURAL_ZERO_THRESHOLD:
            logger.debug(
                "[health_manager] %s: %.0f%% historical zero-row rate — "
                "skipping retry (structurally zero source).",
                source, zero_pct,
            )
            return False  # structurally zero — don't retry

        return True

    except Exception:
        return True  # safe default: always retry on error


# =============================================================================
# PART 2 continued — should_skip / query unstable registry
# =============================================================================

def should_skip(source: str) -> bool:
    """
    Return True if this scraper has been marked unstable and should be skipped.

    Integration point — add to core/runner.py before _run_single():
        from monitoring.scraper_health_manager import should_skip
        if should_skip(job.label):
            return JobResult(flag=job.flag, label=job.label, status='skip',
                             error='marked unstable by health manager')
    """
    try:
        conn = _get_conn()
        stale_before = (datetime.utcnow() - timedelta(hours=UNSTABLE_TTL_HOURS)).isoformat()
        conn.execute(
            "DELETE FROM unstable_scrapers WHERE marked_at < ?",
            (stale_before,),
        )
        conn.commit()
        row  = conn.execute(
            "SELECT 1 FROM unstable_scrapers WHERE source = ? LIMIT 1",
            (source,),
        ).fetchone()
        conn.close()
        return row is not None
    except Exception:
        return False  # safe default: never block a scraper due to our own failure


def clear_unstable(source: str) -> bool:
    """
    Manually re-enable a scraper that was auto-disabled.
    """
    try:
        conn = _get_conn()
        conn.execute(
            "DELETE FROM unstable_scrapers WHERE source = ?", (source,)
        )
        conn.commit()
        conn.close()
        logger.info("[health_manager] Cleared unstable flag for: %s", source)
        return True
    except Exception as exc:
        logger.warning("[health_manager] clear_unstable failed: %s", exc)
        return False


# =============================================================================
# PART 4 — data_confidence_score computation
# =============================================================================

def compute_data_confidence(tender: dict, source: Optional[str] = None) -> int:
    """
    Compute a 0-100 data confidence score for a single tender record.

    Scoring:
      Source reliability  (0-40):  portal success_rate × 0.4
      Has description     (0-20):  word_count>100→20 | word_count>20→10
      Has deadline        (0-20):  non-null deadline present
      Has organization    (0-10):  org != 'unknown' and not empty
      Has sector          (0-10):  sector != 'unknown' and not empty

    Args:
        tender: dict with tender fields from seen_tenders / tenders table
        source: portal/source name override (falls back to tender['source_site'])

    Returns:
        int 0-100
    """
    score = 0

    # ── Source reliability (0-40) ─────────────────────────────────────────────
    src = source or str(
        tender.get("source_site") or tender.get("source_portal") or ""
    )
    if src:
        rate = _get_portal_success_rate(src)
        score += round(rate * 0.40)   # 100% → 40 pts, 50% → 20 pts, 0% → 0 pts

    # ── Extraction completeness (0-20) ────────────────────────────────────────
    word_count = int(tender.get("word_count") or 0)
    if word_count >= 100:
        score += 20
    elif word_count >= 20:
        score += 10

    # ── Presence of deadline (0-20) ───────────────────────────────────────────
    deadline = tender.get("deadline") or tender.get("deadline_raw") or \
               tender.get("deadline_category")
    if deadline and str(deadline).strip() not in ("", "None", "null", "unknown"):
        score += 20

    # ── Presence of organization (0-10) ──────────────────────────────────────
    org = str(tender.get("organization") or "").strip()
    if org and org.lower() not in ("", "unknown", "none", "null"):
        score += 10

    # ── Presence of sector (0-10) ─────────────────────────────────────────────
    sector = str(tender.get("sector") or tender.get("primary_sector") or "").strip()
    if sector and sector.lower() not in ("", "unknown", "none", "null"):
        score += 10

    return min(100, max(0, score))


def _get_portal_success_rate(source: str) -> float:
    """
    Return the success rate (0-100.0) for a portal using recent history.
    Returns 70.0 as default when no history exists (gives benefit of the doubt).
    """
    try:
        conn = _get_conn()
        rows = conn.execute(
            """SELECT status FROM scraper_runs_v2
               WHERE source = ?
               ORDER BY run_at DESC
               LIMIT ?""",
            (source, SUCCESS_RATE_WINDOW),
        ).fetchall()
        conn.close()

        if not rows:
            # Try the existing health_report table as fallback
            return _get_legacy_success_rate(source)

        ok = sum(1 for r in rows if r["status"] == "ok")
        return round((ok / len(rows)) * 100, 1)

    except Exception:
        return 70.0   # default: moderately reliable


def _get_legacy_success_rate(source: str) -> float:
    """
    Fallback: compute success_rate from the legacy scraper_health table
    (written by BaseScraper via monitoring/health_report.py).
    """
    try:
        conn = _get_conn()
        rows = conn.execute(
            """SELECT status FROM scraper_health
               WHERE source = ?
               ORDER BY run_at DESC
               LIMIT ?""",
            (source, SUCCESS_RATE_WINDOW),
        ).fetchall()
        conn.close()

        if not rows:
            return 70.0

        ok = sum(1 for r in rows if r["status"] == "OK")
        return round((ok / len(rows)) * 100, 1)

    except Exception:
        return 70.0


# =============================================================================
# PART 4 continued — update data_confidence_score in tender_structured_intel
# =============================================================================

def update_confidence_scores(tenders: List[dict]) -> int:
    """
    Compute and persist data_confidence_score for a batch of tenders.

    Writes to the `data_confidence_score` column in tender_structured_intel.
    The column is added by add_confidence_column_if_missing() which is called
    from database/db.py → init_db() on every startup.

    Args:
        tenders: raw tender dicts from scraper results (need tender_id + source_site)

    Returns:
        Number of rows updated.
    """
    if not tenders:
        return 0

    pairs = []
    for t in tenders:
        tid = str(t.get("tender_id") or "").strip()
        if not tid:
            continue
        score = compute_data_confidence(t)
        pairs.append((score, tid))

    if not pairs:
        return 0

    try:
        from database.db import get_connection, DRY_RUN
        if DRY_RUN:
            return 0

        conn = get_connection()
        cur  = conn.cursor()
        cur.executemany(
            """UPDATE tender_structured_intel
               SET data_confidence_score = %s
               WHERE tender_id = %s""",
            pairs,
        )
        conn.commit()
        written = cur.rowcount
        cur.close()
        conn.close()
        logger.info(
            "[health_manager] Updated data_confidence_score for %d tender(s).",
            len(pairs),
        )
        return written

    except Exception as exc:
        logger.warning(
            "[health_manager] update_confidence_scores failed (non-fatal): %s", exc
        )
        return 0


# =============================================================================
# PART 5 support — get_all_health() for the API endpoint
# =============================================================================

def get_all_health(window: int = 10) -> Dict[str, Any]:
    """
    Return a comprehensive health snapshot for all known scrapers.

    Used by GET /api/v1/health.

    Returns:
        {
          "portals": [PortalHealth, ...],
          "stable_count":   int,
          "unstable_count": int,
          "partial_count":  int,
          "generated_at":   str,
        }
    """
    try:
        conn = _get_conn()
        stale_before = (datetime.utcnow() - timedelta(hours=UNSTABLE_TTL_HOURS)).isoformat()
        conn.execute(
            "DELETE FROM unstable_scrapers WHERE marked_at < ?",
            (stale_before,),
        )
        conn.commit()

        # All scrapers that have ever been recorded
        sources = [
            r[0] for r in conn.execute(
                "SELECT DISTINCT source FROM scraper_runs_v2"
            ).fetchall()
        ]

        # Also pull from legacy table in case health_manager hasn't run yet
        legacy_sources = [
            r[0] for r in conn.execute(
                "SELECT DISTINCT source FROM scraper_health"
            ).fetchall()
        ]
        all_sources = sorted(
            src for src in (set(sources) | set(legacy_sources))
            if not _is_noise_source(src)
        )

        # Unstable set
        unstable_rows = conn.execute(
            "SELECT source, marked_at, reason FROM unstable_scrapers"
        ).fetchall()
        unstable_map = {
            r["source"]: dict(r)
            for r in unstable_rows
            if not _is_noise_source(r["source"])
        }
        portal_stats = _load_portal_db_stats(conn)

        portals = []
        for src in all_sources:
            metrics = _compute_metrics_with_legacy(conn, src, window)
            is_unstable = src in unstable_map
            db_stats = portal_stats.get(_normalize_source_key(src), portal_stats.get(src, {}))

            # Stability classification
            if is_unstable:
                stability = "unstable"
            elif metrics["success_rate"] >= 70.0 and metrics["consecutive_failures"] < 2:
                stability = "stable"
            else:
                stability = "partial"

            portals.append({
                "source":               src,
                "stability":            stability,
                "success_rate":         metrics["success_rate"],
                "average_rows":         metrics["average_rows"],
                "consecutive_failures": metrics["consecutive_failures"],
                "total_runs":           metrics["total_runs"],
                "last_success_time":    metrics.get("last_success_time"),
                "disabled_reason":      unstable_map.get(src, {}).get("reason"),
                "seen_tenders":         db_stats.get("seen_tenders", 0),
                "normalized_tenders":   db_stats.get("normalized_tenders", 0),
                "descriptions":         db_stats.get("descriptions", 0),
                "deep_enriched":        db_stats.get("deep_enriched", 0),
                "coverage_pct":         db_stats.get("coverage_pct", 0.0),
            })

        conn.close()

        stable_count   = sum(1 for p in portals if p["stability"] == "stable")
        partial_count  = sum(1 for p in portals if p["stability"] == "partial")
        unstable_count = sum(1 for p in portals if p["stability"] == "unstable")

        return {
            "portals":        portals,
            "stable_count":   stable_count,
            "partial_count":  partial_count,
            "unstable_count": unstable_count,
            "generated_at":   datetime.utcnow().isoformat() + "Z",
        }

    except Exception as exc:
        logger.error("[health_manager] get_all_health error: %s", exc)
        return {
            "portals":        [],
            "stable_count":   0,
            "partial_count":  0,
            "unstable_count": 0,
            "generated_at":   datetime.utcnow().isoformat() + "Z",
        }


def _load_portal_db_stats(_conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    """
    Pull backend coverage stats per portal so health output reflects not just
    run success but also how much usable data each source contributes.
    """
    try:
        from database.db import get_connection

        db_conn = get_connection()
        cur = db_conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
                st.source_site AS source,
                COUNT(*) AS seen_tenders,
                COUNT(t.tender_id) AS normalized_tenders,
                SUM(
                    CASE
                        WHEN COALESCE(NULLIF(TRIM(t.description), ''), '') <> '' THEN 1
                        ELSE 0
                    END
                ) AS descriptions,
                SUM(
                    CASE
                        WHEN COALESCE(NULLIF(TRIM(t.deep_scope), ''), '') <> ''
                          OR COALESCE(NULLIF(TRIM(t.deep_description), ''), '') <> ''
                          OR COALESCE(NULLIF(TRIM(t.deep_eval_criteria), ''), '') <> ''
                        THEN 1
                        ELSE 0
                    END
                ) AS deep_enriched
            FROM seen_tenders st
            LEFT JOIN tenders t ON st.tender_id = t.tender_id
            GROUP BY st.source_site
            """
        )
        rows = cur.fetchall() or []
        cur.close()
        db_conn.close()
    except Exception:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        source = str(row["source"] or "").strip()
        if not source:
            continue
        seen = int(row["seen_tenders"] or 0)
        normalized = int(row["normalized_tenders"] or 0)
        stats = {
            "seen_tenders": seen,
            "normalized_tenders": normalized,
            "descriptions": int(row["descriptions"] or 0),
            "deep_enriched": int(row["deep_enriched"] or 0),
            "coverage_pct": round((normalized / seen) * 100, 2) if seen else 0.0,
        }
        out[source] = stats
        out[_normalize_source_key(source)] = stats
    return out


def _normalize_source_key(source: str) -> str:
    """
    Collapse labels and source slugs into one lookup key so runner health and
    DB coverage can be joined even when they use different names.
    """
    key = str(source or "").strip().lower()
    if not key:
        return ""
    for canonical, aliases in _SOURCE_ALIASES.items():
        alias_keys = {str(a).strip().lower() for a in aliases}
        if key == canonical or key in alias_keys:
            return canonical
    return key


def _compute_metrics_with_legacy(
    conn: sqlite3.Connection,
    source: str,
    window: int,
) -> Dict[str, Any]:
    """
    Compute metrics preferring scraper_runs_v2; falls back to scraper_health.
    """
    rows = conn.execute(
        """SELECT row_count, status, run_at FROM scraper_runs_v2
           WHERE source = ?
           ORDER BY run_at DESC
           LIMIT ?""",
        (source, window),
    ).fetchall()

    if not rows:
        # Fall back to legacy scraper_health table
        rows_legacy = conn.execute(
            """SELECT rows_found AS row_count, status, run_at FROM scraper_health
               WHERE source = ?
               ORDER BY run_at DESC
               LIMIT ?""",
            (source, window),
        ).fetchall()
        if not rows_legacy:
            return {
                "total_runs":           0,
                "success_rate":         100.0,
                "average_rows":         0.0,
                "consecutive_failures": 0,
                "last_success_time":    None,
            }
        # Normalise: legacy "OK" → "ok"
        rows = [
            {"row_count": r["row_count"],
             "status":    "ok" if r["status"] == "OK" else "warn",
             "run_at":    r["run_at"]}
            for r in rows_legacy
        ]

    effective_rows = [r for r in rows if r["status"] != "skip"]
    if not effective_rows:
        return {
            "total_runs":           0,
            "success_rate":         100.0,
            "average_rows":         0.0,
            "consecutive_failures": 0,
            "last_success_time":    None,
        }

    total    = len(effective_rows)
    ok_runs  = sum(1 for r in effective_rows if r["status"] in ("ok", "OK"))
    rate     = round((ok_runs / total) * 100, 1) if total else 100.0
    avg_rows = round(sum(r["row_count"] for r in effective_rows) / total, 1) if total else 0.0

    consec = 0
    for r in effective_rows:
        if r["row_count"] == 0:
            consec += 1
        else:
            break

    # Last time rows > 0
    last_success = next(
        (r["run_at"] for r in effective_rows if r["row_count"] > 0), None
    )

    return {
        "total_runs":           total,
        "success_rate":         rate,
        "average_rows":         avg_rows,
        "consecutive_failures": consec,
        "last_success_time":    last_success,
    }
