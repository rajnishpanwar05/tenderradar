# =============================================================================
# pipeline/health.py — Scraper health monitoring
#
# Called by BaseScraper._check_health() after every pipeline run.
# Writes per-run metrics to a local SQLite file so no MySQL config is needed.
#
# Key function: log_scraper_health()
# Key query:    get_last_run_rows()  — used by BaseScraper to detect zero-row drift
#
# Dashboard can query health.db directly or call get_health_summary().
# =============================================================================

import os
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger("tenderradar.health")

# SQLite file lives in monitoring/ alongside run.log
_HEALTH_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "monitoring", "health.db",
)


def _get_conn() -> sqlite3.Connection:
    """Open (and auto-create) the health database."""
    os.makedirs(os.path.dirname(_HEALTH_DB), exist_ok=True)
    conn = sqlite3.connect(_HEALTH_DB, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scraper_health (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source      TEXT    NOT NULL,
            rows_found  INTEGER NOT NULL DEFAULT 0,
            errors      INTEGER NOT NULL DEFAULT 0,
            elapsed_ms  INTEGER NOT NULL DEFAULT 0,
            status      TEXT    NOT NULL DEFAULT 'OK',
            run_at      TEXT    NOT NULL
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_health_source ON scraper_health(source)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_health_run_at ON scraper_health(run_at)"
    )
    conn.commit()
    return conn


# ── Public API ────────────────────────────────────────────────────────────────

def _consecutive_zeros(source: str, n: int = 2) -> bool:
    """
    Return True if the last `n` completed runs for `source` all returned 0 rows.
    Used to escalate WARN → POSSIBLE SCRAPER BREAK after repeated failures.
    (The current run is not yet written, so we check the previous n runs.)
    """
    try:
        conn = _get_conn()
        runs = conn.execute(
            "SELECT rows_found FROM scraper_health "
            "WHERE source = ? ORDER BY run_at DESC LIMIT ?",
            (source, n),
        ).fetchall()
        conn.close()
        return len(runs) == n and all(r["rows_found"] == 0 for r in runs)
    except Exception:
        return False


def log_scraper_health(
    source: str,
    rows_found: int,
    errors: int,
    elapsed_ms: int,
) -> None:
    """
    Write one health record for a completed scraper run.
    Called automatically by BaseScraper._check_health() — do not call manually.

    Status logic:
      OK                    — at least one row found
      FAIL                  — zero rows AND errors present (fetch/parse broke)
      POSSIBLE SCRAPER BREAK — zero rows, no errors, AND last 2 runs also zero
      WARN                  — zero rows with NO errors (first or second time)
    """
    if rows_found > 0:
        status = "OK"
    elif errors > 0:
        status = "FAIL"
    elif _consecutive_zeros(source, n=2):
        # This will be the 3rd consecutive zero-row run
        status = "POSSIBLE SCRAPER BREAK"
    else:
        status = "WARN"

    try:
        conn = _get_conn()
        conn.execute(
            "INSERT INTO scraper_health (source, rows_found, errors, elapsed_ms, status, run_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (source, rows_found, errors, elapsed_ms, status,
             datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()

        # Alert threshold: if WARN/FAIL, surface to root logger so it reaches run.log
        if status in ("WARN", "FAIL"):
            logger.warning(
                f"[health] {source} [{status}] rows={rows_found} "
                f"errors={errors} elapsed={elapsed_ms}ms"
            )
    except Exception as exc:
        # Health recording must never crash the pipeline
        logger.debug(f"[health] DB write error (non-fatal): {exc}")


def get_consecutive_zero_runs(source: str) -> int:
    """
    Return the count of consecutive most-recent zero-row runs for `source`.
    Returns 0 if the last run had rows (or no history exists).
    Used by the reporter to flag portals as POSSIBLE SCRAPER BREAK.
    """
    try:
        conn = _get_conn()
        runs = conn.execute(
            "SELECT rows_found FROM scraper_health "
            "WHERE source = ? ORDER BY run_at DESC LIMIT 10",
            (source,),
        ).fetchall()
        conn.close()
        count = 0
        for r in runs:
            if r["rows_found"] == 0:
                count += 1
            else:
                break
        return count
    except Exception:
        return 0


def get_last_run_rows(source: str) -> Optional[int]:
    """
    Return rows_found from the most recent run of `source`.
    Returns None if there are no previous runs.
    Used by BaseScraper._check_health() to detect sudden zero-row regression.
    """
    try:
        conn = _get_conn()
        row  = conn.execute(
            "SELECT rows_found FROM scraper_health "
            "WHERE source = ? ORDER BY run_at DESC LIMIT 1",
            (source,),
        ).fetchone()
        conn.close()
        return row["rows_found"] if row else None
    except Exception:
        return None


def get_health_summary(days: int = 7) -> List[Dict]:
    """
    Return per-source health statistics for the last N days.
    Useful for a monitoring dashboard or Telegram status report.

    Returns list of dicts:
      source, total_runs, ok_runs, warn_runs, fail_runs,
      avg_rows, avg_elapsed_ms, last_status, last_run_at
    """
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    try:
        conn = _get_conn()
        rows = conn.execute("""
            SELECT
                source,
                COUNT(*)                                    AS total_runs,
                SUM(CASE WHEN status='OK'   THEN 1 ELSE 0 END) AS ok_runs,
                SUM(CASE WHEN status='WARN' THEN 1 ELSE 0 END) AS warn_runs,
                SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) AS fail_runs,
                ROUND(AVG(rows_found), 1)                   AS avg_rows,
                ROUND(AVG(elapsed_ms), 0)                   AS avg_elapsed_ms,
                MAX(run_at)                                 AS last_run_at
            FROM scraper_health
            WHERE run_at >= ?
            GROUP BY source
            ORDER BY fail_runs DESC, warn_runs DESC, source ASC
        """, (since,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.debug(f"[health] get_health_summary error: {exc}")
        return []


def get_broken_scrapers(min_prev_rows: int = 5) -> List[Dict]:
    """
    Return scrapers whose last run returned 0 rows but whose previous runs
    averaged > min_prev_rows — these are likely broken (selector drift, etc.).

    Used by monitoring alerts.
    """
    broken = []
    try:
        conn = _get_conn()
        sources = [
            r["source"]
            for r in conn.execute(
                "SELECT DISTINCT source FROM scraper_health"
            ).fetchall()
        ]
        for src in sources:
            runs = conn.execute(
                "SELECT rows_found, status FROM scraper_health "
                "WHERE source = ? ORDER BY run_at DESC LIMIT 10",
                (src,),
            ).fetchall()
            if not runs:
                continue
            last      = runs[0]
            prev_avg  = (
                sum(r["rows_found"] for r in runs[1:]) / len(runs[1:])
                if len(runs) > 1 else 0
            )
            if last["rows_found"] == 0 and prev_avg >= min_prev_rows:
                broken.append({
                    "source":       src,
                    "last_status":  last["status"],
                    "prev_avg_rows": round(prev_avg, 1),
                })
        conn.close()
    except Exception as exc:
        logger.debug(f"[health] get_broken_scrapers error: {exc}")
    return broken
