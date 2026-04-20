# =============================================================================
# pipeline/opportunity_pipeline.py — Bid Lifecycle Tracking Layer
#
# Tracks every discovered tender through the firm's bid lifecycle:
#
#   discovered → shortlisted → proposal_in_progress → submitted → won | lost
#
# Design constraints:
#   • NO scrapers, runner, intelligence layers, or Excel exporter modified
#   • Entirely independent — any failure degrades to a non-fatal warning
#   • Bulk INSERT IGNORE in ensure_pipeline_entry_batch() keeps overhead
#     well under 0.1 s for ~800 tenders per run (single executemany call)
#   • Idempotent: calling ensure_pipeline_entry on an existing tender_id
#     is a no-op — existing status/owner/notes are never overwritten
#
# Database table: bid_pipeline
#   tender_id          VARCHAR(255) PK — matches seen_tenders.tender_id
#   status             VARCHAR(50)  — lifecycle stage (see VALID_STATUSES)
#   owner              VARCHAR(255) — team member responsible
#   notes              TEXT         — partner strategy, prep notes
#   proposal_deadline  DATE         — internal deadline (may differ from official)
#   created_at         TIMESTAMP    — when tender was first registered
#   updated_at         TIMESTAMP    — auto-updated on every change
#
# Feedback / learning columns (Task 1+2 — learning loop):
#   model_decision_tag VARCHAR(20)  — system recommendation at discovery time
#                                     (BID_NOW | STRONG_CONSIDER | WEAK_CONSIDER | IGNORE)
#                                     Stored once at INSERT; never overwritten.
#   bid_decision       VARCHAR(20)  — what the firm actually did
#                                     (bid | no_bid | pending — default pending)
#   outcome            VARCHAR(20)  — result when known
#                                     (won | lost | no_bid)
#   evaluated_at       TIMESTAMP    — when outcome was recorded
#
# Public API:
#   initialize_pipeline_table()                  → None
#   add_feedback_columns_if_missing()            → None  (safe migration)
#   ensure_pipeline_entry(tender_id, tag)        → bool
#   ensure_pipeline_entry_batch(ids, tags)       → int
#   record_outcome(tid, outcome, bid_decision)   → bool  (NEW — feedback loop)
#   update_pipeline_status(tid, status)          → bool
#   assign_owner(tid, owner)                     → bool
#   add_notes(tid, notes)                        → bool
#   set_proposal_deadline(tid, date_str)         → bool
#   list_pipeline(stage, limit)                  → list[dict]
#   get_pipeline_summary()                       → dict
#
# CLI:
#   python3 pipeline/opportunity_pipeline.py --list discovered
#   python3 pipeline/opportunity_pipeline.py --update <tid> shortlisted
#   python3 pipeline/opportunity_pipeline.py --assign <tid> "John Smith"
#   python3 pipeline/opportunity_pipeline.py --notes  <tid> "Partner: ABC"
#   python3 pipeline/opportunity_pipeline.py --deadline <tid> 2025-09-30
#   python3 pipeline/opportunity_pipeline.py --outcome <tid> won bid
#   python3 pipeline/opportunity_pipeline.py --accuracy
#   python3 pipeline/opportunity_pipeline.py --summary
# =============================================================================

import logging
import os
import sys
from datetime import date, datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("tenderradar.opportunity_pipeline")

# ── Ensure package root on sys.path when run directly ─────────────────────────
_BASE = os.path.expanduser("~/tender_system")
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)

# ── Valid lifecycle stages ─────────────────────────────────────────────────────
VALID_STATUSES = (
    "discovered",
    "shortlisted",
    "proposal_in_progress",
    "submitted",
    "won",
    "lost",
)

# ── Valid feedback values ──────────────────────────────────────────────────────
# Canonical production schema (strict, structured):
#   bid_decision: bid | no_bid | review_later
#   outcome:      won | lost | no_submission | pending
VALID_OUTCOMES      = ("won", "lost", "no_submission", "pending")
VALID_BID_DECISIONS = ("bid", "no_bid", "review_later")

# ── Valid model decision tags (mirrors quality_engine constants) ───────────────
VALID_MODEL_TAGS = ("BID_NOW", "STRONG_CONSIDER", "WEAK_CONSIDER", "IGNORE")

_TABLE = "bid_pipeline"


# =============================================================================
# SECTION 1 — Schema
# =============================================================================

def initialize_pipeline_table() -> None:
    """
    Create the bid_pipeline table if it does not exist.
    Safe to call on every run.  Non-fatal on any DB error.

    Called automatically from database.db.init_db() so no manual
    invocation is needed in normal operation.

    The table includes feedback / learning columns:
      model_decision_tag — system recommendation at discovery (stored once)
      bid_decision       — what the firm actually decided (bid | no_bid | pending)
      outcome            — final result (won | lost | no_bid)
      evaluated_at       — timestamp when outcome was recorded
    """
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{_TABLE}` (
                tender_id          VARCHAR(255)  NOT NULL,
                status             VARCHAR(50)   NOT NULL DEFAULT 'discovered',
                owner              VARCHAR(255)  DEFAULT NULL,
                notes              TEXT          DEFAULT NULL,
                proposal_deadline  DATE          DEFAULT NULL,
                model_decision_tag VARCHAR(20)   DEFAULT NULL,
                bid_decision       VARCHAR(20)   NOT NULL DEFAULT 'review_later',
                outcome            VARCHAR(20)   DEFAULT NULL,
                evaluated_at       TIMESTAMP     DEFAULT NULL,
                created_at         TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at         TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                   ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (tender_id),
                INDEX idx_bp_status            (status),
                INDEX idx_bp_owner             (owner),
                INDEX idx_bp_proposal_deadline (proposal_deadline),
                INDEX idx_bp_created_at        (created_at),
                INDEX idx_bp_model_tag         (model_decision_tag),
                INDEX idx_bp_outcome           (outcome)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("[opportunity_pipeline] Table '%s' ready.", _TABLE)
        # Run migration for existing installs that already have the table
        add_feedback_columns_if_missing()
    except Exception as exc:
        logger.warning(
            "[opportunity_pipeline] initialize_pipeline_table failed (non-fatal): %s",
            exc,
        )


def add_feedback_columns_if_missing() -> None:
    """
    Safe migration: add feedback/learning columns to bid_pipeline if absent.

    Handles existing installs that had the table before the learning loop
    was introduced.  Uses INFORMATION_SCHEMA so it works on MySQL 5.7 / 8.0
    and MariaDB.  Non-fatal — any failure is logged and swallowed.

    Columns added (only if not present):
      model_decision_tag  VARCHAR(20)
      bid_decision        VARCHAR(20) DEFAULT 'review_later'
      outcome             VARCHAR(20)
      evaluated_at        TIMESTAMP   NULL
    """
    _FEEDBACK_COLUMNS = [
        ("model_decision_tag", "ADD COLUMN model_decision_tag VARCHAR(20) DEFAULT NULL"),
        ("bid_decision",       "ADD COLUMN bid_decision       VARCHAR(20) NOT NULL DEFAULT 'review_later'"),
        ("outcome",            "ADD COLUMN outcome            VARCHAR(20) DEFAULT NULL"),
        ("evaluated_at",       "ADD COLUMN evaluated_at       TIMESTAMP   DEFAULT NULL"),
    ]
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor()
        for col_name, alter_clause in _FEEDBACK_COLUMNS:
            cur.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME   = %s
                  AND COLUMN_NAME  = %s
            """, (_TABLE, col_name))
            exists = (cur.fetchone() or (0,))[0] > 0
            if not exists:
                cur.execute(f"ALTER TABLE `{_TABLE}` {alter_clause};")
                conn.commit()
                logger.info(
                    "[opportunity_pipeline] Column '%s' added to %s.", col_name, _TABLE
                )
        # Normalize legacy values to canonical schema.
        cur.execute(
            f"UPDATE `{_TABLE}` SET bid_decision='review_later' "
            f"WHERE bid_decision='pending'"
        )
        cur.execute(
            f"UPDATE `{_TABLE}` SET outcome='no_submission' "
            f"WHERE outcome='no_bid'"
        )
        cur.close()
        conn.close()
    except Exception as exc:
        logger.warning(
            "[opportunity_pipeline] add_feedback_columns_if_missing failed (non-fatal): %s",
            exc,
        )


# =============================================================================
# SECTION 2 — Entry creation (called by main pipeline)
# =============================================================================

def ensure_pipeline_entry(
    tender_id:         str,
    model_decision_tag: Optional[str] = None,
) -> bool:
    """
    Register a single tender as 'discovered' if not already tracked.
    Uses INSERT IGNORE — existing rows (any status) are untouched.

    model_decision_tag : system recommendation at discovery time
        (BID_NOW | STRONG_CONSIDER | WEAK_CONSIDER | IGNORE).
        Stored once at INSERT; never overwritten by subsequent calls.

    Returns True on success, False on DB error.
    """
    tags = {tender_id: model_decision_tag} if model_decision_tag else None
    return ensure_pipeline_entry_batch([tender_id], model_tags=tags) >= 0


def ensure_pipeline_entry_batch(
    tender_ids: List[str],
    model_tags: Optional[Dict[str, Optional[str]]] = None,
) -> int:
    """
    Register many tenders as 'discovered' in a single database round-trip.
    Also stores the model_decision_tag at insert time so the feedback loop
    can later measure how accurate the system's recommendations were.

    Performance
    -----------
    Uses INSERT IGNORE with executemany — one network call regardless of
    batch size.  Typical overhead for 800 rows: < 30 ms.

    Parameters
    ----------
    tender_ids : list of tender_id strings (duplicates and empty strings skipped)
    model_tags : optional dict {tender_id: decision_tag} — stored at INSERT only;
                 never overwrites an existing row's model_decision_tag.

    Returns
    -------
    Number of NEW rows inserted (0 if all already existed, -1 on DB error).
    """
    # Deduplicate and sanitise
    clean_ids: List[str] = []
    seen_in_batch: set   = set()
    for tid in tender_ids:
        tid = str(tid).strip()[:255]
        if tid and tid not in seen_in_batch:
            clean_ids.append(tid)
            seen_in_batch.add(tid)

    if not clean_ids:
        return 0

    try:
        from database.db import get_connection, DRY_RUN
        if DRY_RUN:
            logger.info(
                "[opportunity_pipeline] DRY-RUN: skipping %d pipeline INSERT(s)",
                len(clean_ids),
            )
            return 0

        conn = get_connection()
        cur  = conn.cursor()

        # INSERT IGNORE with model_decision_tag captured at discovery
        sql = (
            f"INSERT IGNORE INTO `{_TABLE}` "
            f"(tender_id, status, model_decision_tag) "
            f"VALUES (%s, 'discovered', %s)"
        )
        rows = [
            (tid, (model_tags or {}).get(tid))
            for tid in clean_ids
        ]
        cur.executemany(sql, rows)
        conn.commit()
        inserted = cur.rowcount
        cur.close()
        conn.close()

        logger.info(
            "[opportunity_pipeline] ensure_pipeline_entry_batch: "
            "%d/%d inserted as 'discovered' (tags attached: %d)",
            inserted, len(clean_ids),
            sum(1 for _, tag in rows if tag),
        )
        return inserted

    except Exception as exc:
        logger.warning(
            "[opportunity_pipeline] ensure_pipeline_entry_batch failed: %s", exc
        )
        return -1


# =============================================================================
# SECTION 2b — Outcome recording (feedback loop)
# =============================================================================

def record_outcome(
    tender_id:    str,
    outcome:      str,
    bid_decision: str = "bid",
) -> bool:
    """
    Record the real-world result for a tender to close the feedback loop.

    This is the single write operation that feeds the learning system.
    Call it when:
      - A bid was submitted and result is known (won / lost)
      - The firm decided not to bid after review (no_bid)

    Parameters
    ----------
    tender_id    : must already exist in bid_pipeline
    outcome      : 'won' | 'lost' | 'no_bid'
    bid_decision : 'bid' | 'no_bid' | 'pending'  (default 'bid')
                   Set to 'no_bid' when firm chose not to bid at all.

    Side-effects
    ------------
    - Writes outcome, bid_decision, evaluated_at to bid_pipeline
    - Also advances status to 'won' or 'lost' if outcome is one of those
      (keeps lifecycle and feedback columns consistent)

    Returns True on success, False if tender not found or DB error.

    Raises ValueError for invalid outcome / bid_decision values.
    """
    outcome      = outcome.strip().lower()
    bid_decision = bid_decision.strip().lower()

    if outcome not in VALID_OUTCOMES:
        raise ValueError(
            f"Invalid outcome '{outcome}'. Valid: {', '.join(VALID_OUTCOMES)}"
        )
    if bid_decision not in VALID_BID_DECISIONS:
        raise ValueError(
            f"Invalid bid_decision '{bid_decision}'. "
            f"Valid: {', '.join(VALID_BID_DECISIONS)}"
        )
    if outcome in ("won", "lost") and bid_decision != "bid":
        raise ValueError("For outcome 'won'/'lost', bid_decision must be 'bid'.")
    if outcome == "no_submission" and bid_decision not in ("no_bid", "review_later"):
        raise ValueError("For outcome 'no_submission', bid_decision must be 'no_bid' or 'review_later'.")

    try:
        from database.db import get_connection, DRY_RUN
        if DRY_RUN:
            logger.info(
                "[opportunity_pipeline] DRY-RUN: skipping record_outcome for %s",
                tender_id,
            )
            return True

        conn = get_connection()
        cur  = conn.cursor()

        # Determine whether to also advance the lifecycle status
        new_status = None
        if outcome in ("won", "lost"):
            new_status = outcome   # advance lifecycle stage to match

        if new_status:
            cur.execute(
                f"""
                UPDATE `{_TABLE}`
                SET outcome       = %s,
                    bid_decision  = %s,
                    evaluated_at  = NOW(),
                    status        = %s
                WHERE tender_id = %s
                """,
                (outcome, bid_decision, new_status, str(tender_id)[:255]),
            )
        else:
            cur.execute(
                f"""
                UPDATE `{_TABLE}`
                SET outcome      = %s,
                    bid_decision = %s,
                    evaluated_at = NOW()
                WHERE tender_id = %s
                """,
                (outcome, bid_decision, str(tender_id)[:255]),
            )

        conn.commit()
        affected = cur.rowcount
        cur.close()
        conn.close()

        if affected == 0:
            logger.warning(
                "[opportunity_pipeline] record_outcome: tender_id '%s' "
                "not found in pipeline", tender_id,
            )
        else:
            logger.info(
                "[opportunity_pipeline] record_outcome: %s → outcome=%s bid=%s",
                tender_id, outcome, bid_decision,
            )
        return affected > 0

    except Exception as exc:
        logger.warning(
            "[opportunity_pipeline] record_outcome failed: %s", exc
        )
        return False


# =============================================================================
# SECTION 3 — Lifecycle updates
# =============================================================================

def _update_field(tender_id: str, field: str, value: Any) -> bool:
    """
    Generic single-field UPDATE helper used by the public update functions.
    Returns True on success, False on any error.
    """
    try:
        from database.db import get_connection, DRY_RUN
        if DRY_RUN:
            logger.info(
                "[opportunity_pipeline] DRY-RUN: skipping UPDATE %s=%s for %s",
                field, value, tender_id,
            )
            return True
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute(
            f"UPDATE `{_TABLE}` SET `{field}` = %s WHERE tender_id = %s",
            (value, str(tender_id)[:255]),
        )
        conn.commit()
        affected = cur.rowcount
        cur.close()
        conn.close()
        if affected == 0:
            logger.warning(
                "[opportunity_pipeline] UPDATE %s: tender_id '%s' not found in pipeline",
                field, tender_id,
            )
        return affected > 0
    except Exception as exc:
        logger.warning(
            "[opportunity_pipeline] _update_field(%s) failed: %s", field, exc
        )
        return False


def update_pipeline_status(tender_id: str, status: str) -> bool:
    """
    Advance a tender to the given lifecycle stage.

    Valid values: discovered | shortlisted | proposal_in_progress |
                  submitted | won | lost

    Returns True on success, False if tender_id not found or DB error.
    Raises ValueError for invalid status strings.
    """
    status = status.lower().strip()
    if status not in VALID_STATUSES:
        raise ValueError(
            f"Invalid status '{status}'. "
            f"Valid values: {', '.join(VALID_STATUSES)}"
        )
    return _update_field(tender_id, "status", status)


def assign_owner(tender_id: str, owner: str) -> bool:
    """
    Assign a team member as the lead for this opportunity.
    Pass owner=None to unassign.
    """
    val = str(owner).strip()[:255] if owner else None
    return _update_field(tender_id, "owner", val)


def add_notes(tender_id: str, notes: str) -> bool:
    """
    Set (replace) the free-text notes for a pipeline entry.
    Used for partner strategy, consortium notes, proposal prep details.
    """
    return _update_field(tender_id, "notes", str(notes) if notes else None)


def set_proposal_deadline(tender_id: str, date_str: str) -> bool:
    """
    Set an internal proposal deadline (may differ from the official deadline).

    Accepts ISO date strings (YYYY-MM-DD) or Python date objects.
    Pass None / empty string to clear the deadline.

    Raises ValueError for unparseable date strings.
    """
    if not date_str:
        return _update_field(tender_id, "proposal_deadline", None)

    if isinstance(date_str, date):
        dl = date_str
    else:
        try:
            dl = date.fromisoformat(str(date_str).strip()[:10])
        except ValueError:
            raise ValueError(
                f"Invalid date '{date_str}'. Expected format: YYYY-MM-DD"
            )
    return _update_field(tender_id, "proposal_deadline", dl.isoformat())


# =============================================================================
# SECTION 4 — Query functions
# =============================================================================

def list_pipeline(
    stage:  Optional[str] = None,
    limit:  int           = 100,
    owner:  Optional[str] = None,
) -> List[Dict]:
    """
    Return pipeline entries, optionally filtered by lifecycle stage and/or owner.
    Enriches each row with title and scores from related tables (LEFT JOIN).

    Parameters
    ----------
    stage : lifecycle stage filter (None → all stages)
    limit : max rows returned (default 100)
    owner : filter by assigned owner

    Returns
    -------
    List of dicts with keys: tender_id, status, owner, notes,
    proposal_deadline, created_at, updated_at, title,
    relevance_score, priority_score, source_site
    """
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        where_clauses: List[str] = []
        params: List[Any] = []

        if stage:
            stage = stage.lower().strip()
            where_clauses.append("bp.status = %s")
            params.append(stage)

        if owner:
            where_clauses.append("bp.owner = %s")
            params.append(owner)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        cur.execute(
            f"""
            SELECT
                bp.tender_id,
                bp.status,
                bp.owner,
                bp.notes,
                bp.proposal_deadline,
                bp.created_at,
                bp.updated_at,
                st.title,
                st.source_site,
                tsi.relevance_score,
                tsi.priority_score,
                tsi.sector,
                tsi.deadline_category
            FROM `{_TABLE}` bp
            LEFT JOIN seen_tenders        st  ON bp.tender_id = st.tender_id
            LEFT JOIN tender_structured_intel tsi ON bp.tender_id = tsi.tender_id
            {where_sql}
            ORDER BY bp.updated_at DESC
            LIMIT %s
            """,
            params + [limit],
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        return rows

    except Exception as exc:
        logger.warning("[opportunity_pipeline] list_pipeline failed: %s", exc)
        return []


def get_pipeline_summary() -> Dict[str, Any]:
    """
    Return aggregate counts per lifecycle stage plus total portfolio size.

    Returns dict with keys:
        total, by_status (dict stage→count), owners (list of owner summary dicts)
    Returns empty dict on DB error.
    """
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        # ── Per-stage counts ──────────────────────────────────────────────────
        cur.execute(
            f"SELECT status, COUNT(*) AS cnt FROM `{_TABLE}` GROUP BY status"
        )
        stage_rows = cur.fetchall() or []
        by_status  = {r["status"]: int(r["cnt"]) for r in stage_rows}
        total      = sum(by_status.values())

        # Ensure all stages present (even if 0)
        for s in VALID_STATUSES:
            by_status.setdefault(s, 0)

        # ── Per-owner workload ────────────────────────────────────────────────
        cur.execute(
            f"""
            SELECT
                COALESCE(owner, 'Unassigned') AS owner,
                COUNT(*) AS total,
                SUM(status = 'shortlisted')           AS shortlisted,
                SUM(status = 'proposal_in_progress')  AS in_progress,
                SUM(status = 'submitted')             AS submitted
            FROM `{_TABLE}`
            GROUP BY owner
            ORDER BY total DESC
            LIMIT 20
            """
        )
        owner_rows = cur.fetchall() or []

        cur.close()
        conn.close()

        return {
            "total":     total,
            "by_status": by_status,
            "owners":    [dict(r) for r in owner_rows],
        }

    except Exception as exc:
        logger.warning("[opportunity_pipeline] get_pipeline_summary failed: %s", exc)
        return {}


# =============================================================================
# SECTION 5 — CLI entry point
# =============================================================================

_STATUS_COLOUR = {
    "discovered":           "\033[2m",    # dim
    "shortlisted":          "\033[36m",   # cyan
    "proposal_in_progress": "\033[33m",   # yellow
    "submitted":            "\033[34m",   # blue
    "won":                  "\033[32m",   # green
    "lost":                 "\033[31m",   # red
}
_RESET = "\033[0m"


def _coloured_status(status: str) -> str:
    if not sys.stdout.isatty():
        return status
    code = _STATUS_COLOUR.get(status, "")
    return f"{code}{status}{_RESET}" if code else status


def _print_pipeline_table(rows: List[Dict]) -> None:
    if not rows:
        print("  (no entries match the filter)")
        return

    header = (
        f"{'STATUS':<22}  {'OWNER':<18}  {'DEADLINE':<12}  "
        f"{'SCORE':>5}  {'TITLE'}"
    )
    sep = "─" * min(120, max(len(header), 80))
    print()
    print(f"  {header}")
    print(f"  {sep}")

    for r in rows:
        status  = _coloured_status(str(r.get("status", "")).ljust(22))
        owner   = str(r.get("owner") or "—").ljust(18)[:18]
        dl      = str(r.get("proposal_deadline") or "—").ljust(12)[:12]
        score   = str(r.get("priority_score") or r.get("relevance_score") or "—").rjust(5)
        title   = str(r.get("title") or r.get("tender_id") or "")[:55]
        print(f"  {status}  {owner}  {dl}  {score}  {title}")

    print(f"  {sep}")
    print(f"  {len(rows)} entry(ies) shown\n")


def _print_summary(summary: Dict) -> None:
    if not summary:
        print("  Could not load summary (DB not configured?)")
        return

    print(f"\n  Bid Pipeline Summary  —  {summary.get('total', 0)} total entries\n")
    print(f"  {'STAGE':<24} {'COUNT':>7}")
    print("  " + "─" * 34)
    for stage in VALID_STATUSES:
        count = summary.get("by_status", {}).get(stage, 0)
        bar   = "█" * min(count, 30)
        print(f"  {stage:<24} {count:>7}  {bar}")

    owners = summary.get("owners", [])
    if owners:
        print(f"\n  {'OWNER':<22} {'TOTAL':>6}  {'SHORTLISTED':>11}  {'IN PROGRESS':>11}  {'SUBMITTED':>9}")
        print("  " + "─" * 70)
        for o in owners[:10]:
            print(
                f"  {str(o.get('owner','')):<22} "
                f"{int(o.get('total',0)):>6}  "
                f"{int(o.get('shortlisted',0) or 0):>11}  "
                f"{int(o.get('in_progress',0) or 0):>11}  "
                f"{int(o.get('submitted',0) or 0):>9}"
            )
    print()


def main() -> None:
    import argparse

    logging.basicConfig(
        format="%(levelname)s  %(name)s — %(message)s",
        level=logging.WARNING,   # suppress info noise in CLI
    )

    ap = argparse.ArgumentParser(
        description="TenderRadar — Bid Pipeline CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python3 pipeline/opportunity_pipeline.py --summary
  python3 pipeline/opportunity_pipeline.py --list discovered
  python3 pipeline/opportunity_pipeline.py --list shortlisted
  python3 pipeline/opportunity_pipeline.py --update WB-2025-001 shortlisted
  python3 pipeline/opportunity_pipeline.py --assign WB-2025-001 "Priya Sharma"
  python3 pipeline/opportunity_pipeline.py --notes  WB-2025-001 "Partner: ABC Consulting. Lead: Priya."
  python3 pipeline/opportunity_pipeline.py --deadline WB-2025-001 2025-09-15
        """,
    )

    ap.add_argument("--list",     metavar="STAGE",    help=f"List pipeline entries for STAGE (or 'all'). Valid: {', '.join(VALID_STATUSES)}")
    ap.add_argument("--update",   metavar=("TID","STATUS"), nargs=2, help="Move tender_id to a new status")
    ap.add_argument("--assign",   metavar=("TID","OWNER"),  nargs=2, help="Assign an owner to a tender")
    ap.add_argument("--notes",    metavar=("TID","NOTES"),  nargs=2, help="Set notes for a tender")
    ap.add_argument("--deadline", metavar=("TID","DATE"),   nargs=2, help="Set internal proposal deadline (YYYY-MM-DD)")
    ap.add_argument("--outcome",  metavar=("TID","OUTCOME","DECISION"), nargs=3,
                    help=f"Record result. OUTCOME: {', '.join(VALID_OUTCOMES)}. "
                         f"DECISION: {', '.join(VALID_BID_DECISIONS)}")
    ap.add_argument("--accuracy", action="store_true",               help="Print model decision accuracy report")
    ap.add_argument("--apply-tuning", action="store_true",           help="Apply suggested threshold adjustments from calibrator")
    ap.add_argument("--summary",  action="store_true",               help="Print aggregate pipeline summary")
    ap.add_argument("--owner",    metavar="OWNER",                   help="Filter --list by owner name")
    ap.add_argument("--limit",    type=int, default=50,              help="Max rows for --list (default: 50)")
    ap.add_argument("--init",     action="store_true",               help="Create bid_pipeline table if absent")

    args = ap.parse_args()

    if not any([args.list, args.update, args.assign, args.notes,
                args.deadline, args.outcome, args.accuracy,
                args.apply_tuning, args.summary, args.init]):
        ap.print_help()
        sys.exit(0)

    # ── --init ────────────────────────────────────────────────────────────
    if args.init:
        initialize_pipeline_table()
        print("  ✅  bid_pipeline table ready.")
        sys.exit(0)

    # ── --summary ─────────────────────────────────────────────────────────
    if args.summary:
        _print_summary(get_pipeline_summary())
        sys.exit(0)

    # ── --list <stage|all> ────────────────────────────────────────────────
    if args.list:
        stage = None if args.list.lower() == "all" else args.list.lower()
        if stage and stage not in VALID_STATUSES:
            print(f"  ✗  Invalid stage '{stage}'. Valid: {', '.join(VALID_STATUSES)}")
            sys.exit(1)
        rows = list_pipeline(stage=stage, limit=args.limit, owner=args.owner)
        label = f"stage={stage}" if stage else "all stages"
        if args.owner:
            label += f", owner={args.owner}"
        print(f"\n  Bid Pipeline — {label}")
        _print_pipeline_table(rows)
        sys.exit(0)

    # ── --update <tid> <status> ───────────────────────────────────────────
    if args.update:
        tid, status = args.update
        try:
            ok = update_pipeline_status(tid, status)
        except ValueError as e:
            print(f"  ✗  {e}")
            sys.exit(1)
        if ok:
            print(f"  ✅  [{tid}] → status: {status}")
        else:
            print(f"  ✗  tender_id '{tid}' not found in pipeline")
            print(f"     Run: python3 pipeline/opportunity_pipeline.py --list all")
        sys.exit(0 if ok else 1)

    # ── --assign <tid> <owner> ────────────────────────────────────────────
    if args.assign:
        tid, owner = args.assign
        ok = assign_owner(tid, owner)
        if ok:
            print(f"  ✅  [{tid}] → owner: {owner}")
        else:
            print(f"  ✗  tender_id '{tid}' not found in pipeline")
        sys.exit(0 if ok else 1)

    # ── --notes <tid> <notes> ─────────────────────────────────────────────
    if args.notes:
        tid, notes = args.notes
        ok = add_notes(tid, notes)
        if ok:
            print(f"  ✅  [{tid}] → notes updated")
        else:
            print(f"  ✗  tender_id '{tid}' not found in pipeline")
        sys.exit(0 if ok else 1)

    # ── --deadline <tid> <date> ───────────────────────────────────────────
    if args.deadline:
        tid, dl = args.deadline
        try:
            ok = set_proposal_deadline(tid, dl)
        except ValueError as e:
            print(f"  ✗  {e}")
            sys.exit(1)
        if ok:
            print(f"  ✅  [{tid}] → proposal_deadline: {dl}")
        else:
            print(f"  ✗  tender_id '{tid}' not found in pipeline")
        sys.exit(0 if ok else 1)

    # ── --outcome <tid> <outcome> <bid_decision> ──────────────────────────
    if args.outcome:
        tid, outcome, bid_dec = args.outcome
        try:
            ok = record_outcome(tid, outcome, bid_dec)
        except ValueError as e:
            print(f"  ✗  {e}")
            sys.exit(1)
        if ok:
            print(f"  ✅  [{tid}] → outcome: {outcome}  bid_decision: {bid_dec}")
            print(f"      Feedback recorded — system will learn from this result.")
        else:
            print(f"  ✗  tender_id '{tid}' not found in pipeline")
        sys.exit(0 if ok else 1)

    # ── --accuracy ────────────────────────────────────────────────────────
    if args.accuracy:
        try:
            from pipeline.decision_calibrator import print_accuracy_report
            print_accuracy_report()
        except ImportError as e:
            print(f"  ✗  decision_calibrator not available: {e}")
            sys.exit(1)
        sys.exit(0)

    # ── --apply-tuning ────────────────────────────────────────────────────
    if args.apply_tuning:
        try:
            from pipeline.decision_calibrator import (
                compute_decision_accuracy, suggest_threshold_adjustment,
                apply_threshold_adjustment,
            )
            metrics  = compute_decision_accuracy()
            suggest  = suggest_threshold_adjustment(metrics)
            if suggest.get("no_change"):
                print(f"  ✔  No threshold adjustment needed: {suggest.get('reason')}")
            else:
                print(f"  Suggested adjustment: {suggest}")
                confirm = input("  Apply these thresholds? [y/N] ").strip().lower()
                if confirm == "y":
                    apply_threshold_adjustment(suggest)
                    print("  ✅  Thresholds written to calibration_config.json")
                else:
                    print("  Aborted — no changes made.")
        except ImportError as e:
            print(f"  ✗  decision_calibrator not available: {e}")
            sys.exit(1)
        sys.exit(0)


if __name__ == "__main__":
    main()
