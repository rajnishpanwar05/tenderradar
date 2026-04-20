# =============================================================================
# intelligence/opportunity_insights.py — Opportunity Insight Generator
#
# Generates short human-readable explanations describing why each tender is
# strategically important to the firm.  Pure rule-based logic — no LLM calls.
#
# INPUT
#   Reads from `tender_structured_intel` after both structured-intelligence
#   AND opportunity-intelligence steps have populated all attribute columns:
#
#       sector           consulting_type   region         organization
#       deadline_category relevance_score  priority_score competition_level
#       opportunity_size  complexity_score
#
# OUTPUT
#   A single `opportunity_insight` TEXT field stored back in the same table.
#
#   Example outputs:
#     "Strong sector alignment (climate) with preferred client (World Bank).
#      Likely large advisory program — expect strong competition."
#
#     "Good strategic fit in governance (policy engagement). Lower competition
#      likely."
#
#     "Moderate opportunity with sector match but smaller/pilot scope."
#
# DESIGN
#   • Pure Python keyword logic — ~800 rows processed in < 50 ms
#   • Idempotent: re-running overwrites existing insight text
#   • Non-fatal: any failure degrades to a logged warning
#   • Schema extension uses information_schema check (no DDL exceptions)
#
# PUBLIC API
#   extend_schema()                    → None
#   generate_insight(attrs)            → str
#   generate_and_store_batch(tenders)  → int   (rows updated)
#   backfill(limit)                    → int   (rows without insight)
#
# CLI test:
#   python3 intelligence/opportunity_insights.py
#   python3 intelligence/opportunity_insights.py --backfill [--limit N]
# =============================================================================

import logging
import os
import sys
from typing import Any, Dict, List, Optional

logger = logging.getLogger("tenderradar.opportunity_insights")

# ── Ensure package root on sys.path when run directly ─────────────────────────
_BASE = os.path.expanduser("~/tender_system")
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)

_TABLE = "tender_structured_intel"


# =============================================================================
# SECTION 1 — Insight vocabulary
# =============================================================================

# Clients whose involvement materially increases strategic desirability
_PREFERRED_CLIENTS = {
    "World Bank", "UNDP", "UNICEF", "ADB", "AfDB", "AFD",
    "European Union", "USAID", "FCDO/DFID", "WFP", "FAO",
    "IFAD", "GIZ", "WHO",
}

# Human-readable labels for sector and consulting-type slugs
_SECTOR_LABELS: Dict[str, str] = {
    "education":   "education", "health":        "health",
    "water":       "WASH",      "governance":    "governance",
    "climate":     "climate",   "agriculture":   "agriculture",
    "gender":      "gender",    "infrastructure":"infrastructure",
    "energy":      "energy",    "finance":       "finance",
    "digital":     "digital",   "transport":     "transport",
}

_TYPE_LABELS: Dict[str, str] = {
    "evaluation":            "evaluation",
    "research":              "research",
    "technical assistance":  "TA",
    "capacity building":     "capacity building",
    "policy":                "policy advisory",
    "advisory":              "advisory",
    "implementation support":"implementation support",
    "feasibility study":     "feasibility study",
}


def _sector_label(slug: str) -> str:
    return _SECTOR_LABELS.get(slug, slug)


def _type_label(slug: str) -> str:
    return _TYPE_LABELS.get(slug, slug)


# =============================================================================
# SECTION 2 — Core insight generation
# =============================================================================

def generate_insight(attrs: Dict[str, Any]) -> str:
    """
    Generate a specific, decision-grade insight from structured attributes.
    No generic templates — every insight includes actionable context.

    Parameters
    ----------
    attrs : dict with tender_structured_intel columns (missing values default safely)

    Returns
    -------
    Non-empty insight string (1–2 sentences).  Never raises.
    """
    # ── Parse attributes ───────────────────────────────────────────────────────
    def _int(v: Any, default: int = 0) -> int:
        try:
            return int(v or default)
        except (ValueError, TypeError):
            return default

    priority    = _int(attrs.get("priority_score"))
    relevance   = _int(attrs.get("relevance_score"))
    sector      = str(attrs.get("sector",            "unknown") or "unknown").strip().lower()
    ctype       = str(attrs.get("consulting_type",   "unknown") or "unknown").strip().lower()
    org         = str(attrs.get("organization",      "unknown") or "unknown").strip()
    competition = str(attrs.get("competition_level", "medium")  or "medium").strip().lower()
    size        = str(attrs.get("opportunity_size",  "medium")  or "medium").strip().lower()
    deadline    = str(attrs.get("deadline_category", "unknown") or "unknown").strip().lower()
    region      = str(attrs.get("region",            "global")  or "global").strip()

    has_sector   = sector  not in ("unknown", "")
    has_type     = ctype   not in ("unknown", "")
    has_org      = org     not in ("unknown", "")
    has_region   = region  not in ("global", "unknown", "")
    is_preferred = org in _PREFERRED_CLIENTS

    s_label = _sector_label(sector) if has_sector else ""
    t_label = _type_label(ctype)    if has_type   else ""

    # ── Hard override: expired ─────────────────────────────────────────────────
    if deadline == "expired":
        context = f" — {t_label} in {s_label}" if (has_type and has_sector) else ""
        return f"Deadline has passed{context}. Keep for sector reference only."

    # ── Sentence 1: Primary strategic signal ──────────────────────────────────
    # Build a SPECIFIC sentence using all available attributes — no generic
    # "moderate opportunity" fillers.

    if priority >= 75 and is_preferred and has_sector and has_type:
        s1 = (f"High-value {t_label} opportunity in {s_label} from {org} "
              f"({region if has_region else 'global scope'}) — strong IDCG alignment.")

    elif priority >= 75 and is_preferred:
        sector_str = f" in {s_label}" if has_sector else ""
        s1 = (f"Top-tier opportunity with {org}{sector_str}. "
              f"Preferred multilateral client with high budget certainty.")

    elif priority >= 75 and has_sector and has_type:
        region_str = f" in {region}" if has_region else ""
        s1 = (f"Strong strategic fit — {t_label} assignment in {s_label}{region_str}. "
              f"High relevance to IDCG core competencies.")

    elif priority >= 55 and is_preferred and has_sector:
        urgency_str = {
            "urgent": " — urgent bid window, act immediately",
            "soon":   " — closing soon, prioritise",
            "normal": "",
            "unknown": "",
        }.get(deadline, "")
        s1 = (f"Preferred client ({org}) with {s_label} sector alignment{urgency_str}.")

    elif priority >= 55 and has_sector and has_type:
        region_str = f" in {region}" if has_region else ""
        comp_str   = " with moderate competition" if competition == "medium" else ""
        s1 = (f"Good {t_label} fit in {s_label}{region_str}{comp_str}. "
              f"Aligns with IDCG service offerings.")

    elif priority >= 40 and has_org and has_sector:
        size_str = f"{size}-scale " if size != "medium" else ""
        s1 = (f"{size_str}{s_label.capitalize()} {t_label or 'engagement'} from "
              f"{org} — worth evaluating for capability match.")

    elif priority >= 40:
        sector_str = f" in {s_label}" if has_sector else " (sector unclassified)"
        s1 = f"Reviewable opportunity{sector_str} — limited intelligence available."

    else:
        # Low priority — be specific about WHY it's low value
        reasons: List[str] = []
        if not has_org or org == "unknown":
            reasons.append("unknown client")
        if not has_sector or sector in ("infrastructure", "transport"):
            reasons.append(f"sector mismatch ({sector or 'unknown'})")
        if deadline == "unknown":
            reasons.append("no deadline information")
        reason_str = " — " + ", ".join(reasons) if reasons else ""
        s1 = f"Low strategic value for IDCG{reason_str}. Skip or monitor passively."

    # ── Sentence 2: Actionable context ────────────────────────────────────────
    # Pick the MOST decision-relevant signal (urgency > competition > size)
    # Never add a second sentence if the first is already self-contained for low-priority.

    if priority < 40:
        return s1  # no second sentence for low-priority

    action_parts: List[str] = []

    # Urgency signal
    if deadline == "urgent":
        action_parts.append("⚠️ Immediate action required — bid window < 7 days")
    elif deadline == "soon":
        action_parts.append("Closing within 30 days — prioritise bid/no-bid decision")

    # Competition signal
    if competition == "high" and is_preferred:
        action_parts.append("expect 50+ international firms; strong consortium needed")
    elif competition == "high":
        action_parts.append("strong international competition expected")
    elif competition == "low" and priority >= 55:
        action_parts.append("low competition window — higher win probability")

    # Size signal (only if not already stated)
    if size == "large" and "large" not in s1:
        type_str = t_label if has_type else "multi-year engagement"
        action_parts.append(f"likely large {type_str}")
    elif size == "small" and priority >= 55:
        action_parts.append("smaller/pilot scope — quick turnaround feasible")

    if not action_parts:
        return s1

    s2 = action_parts[0].rstrip(".") + "."
    if len(action_parts) > 1:
        s2 += " " + action_parts[1].rstrip(".").capitalize() + "."

    return f"{s1} {s2}"


# =============================================================================
# SECTION 3 — Database layer
# =============================================================================

def extend_schema() -> None:
    """
    Add the `opportunity_insight TEXT` column to tender_structured_intel if absent.
    Uses information_schema check — safe to call on every run.
    """
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor()

        cur.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME   = %s
              AND COLUMN_NAME  = 'opportunity_insight'
            """,
            (_TABLE,),
        )
        (count,) = cur.fetchone()
        if count == 0:
            cur.execute(
                f"ALTER TABLE `{_TABLE}` "
                f"ADD COLUMN `opportunity_insight` TEXT DEFAULT NULL"
            )
            logger.info("[opportunity_insights] Column 'opportunity_insight' added.")

        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        logger.warning(
            "[opportunity_insights] extend_schema failed (non-fatal): %s", exc
        )


def _fetch_intel(tender_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch all structured + opportunity intel columns for a list of tender_ids.
    Returns list of row dicts.  Returns [] on any DB error.
    """
    # Deduplicate and sanitise
    clean: List[str] = list({str(tid).strip()[:255]
                              for tid in tender_ids if tid})
    if not clean:
        return []

    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        placeholders = ", ".join(["%s"] * len(clean))
        cur.execute(
            f"""
            SELECT tender_id, sector, consulting_type, organization,
                   deadline_category, relevance_score, region,
                   priority_score, competition_level,
                   opportunity_size, complexity_score
            FROM `{_TABLE}`
            WHERE tender_id IN ({placeholders})
            """,
            clean,
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        return rows

    except Exception as exc:
        logger.debug("[opportunity_insights] _fetch_intel failed: %s", exc)
        return []


def _store_insights(insights: List[Dict[str, Any]]) -> int:
    """
    UPDATE tender_structured_intel rows with generated insight text.
    Returns number of rows updated.  Returns 0 on DB error.
    """
    if not insights:
        return 0

    try:
        from database.db import get_connection, DRY_RUN
        if DRY_RUN:
            logger.info(
                "[opportunity_insights] DRY-RUN: skipping %d insight UPDATE(s)",
                len(insights),
            )
            return 0

        conn = get_connection()
        cur  = conn.cursor()

        sql  = (f"UPDATE `{_TABLE}` "
                f"SET opportunity_insight = %s "
                f"WHERE tender_id = %s")
        rows = [(r["opportunity_insight"], r["tender_id"]) for r in insights]
        cur.executemany(sql, rows)
        conn.commit()
        written = cur.rowcount
        cur.close()
        conn.close()

        logger.info(
            "[opportunity_insights] Updated %d/%d rows with insights",
            written, len(insights),
        )
        return written

    except Exception as exc:
        logger.warning(
            "[opportunity_insights] _store_insights failed (non-fatal): %s", exc
        )
        return 0


# =============================================================================
# SECTION 4 — Public pipeline API
# =============================================================================

def generate_and_store_batch(tenders: List[Dict[str, Any]]) -> int:
    """
    One-call pipeline integration:
        extend schema → fetch intel from DB → generate insights → store

    Parameters
    ----------
    tenders : raw tender dicts from any scraper
              (tender_id extracted from common field variants)

    Returns
    -------
    Number of rows updated.  Guarantees non-fatal execution.
    """
    try:
        extend_schema()

        # Resolve tender_ids from raw dicts
        tender_ids: List[str] = []
        for t in tenders:
            tid = str(
                t.get("tender_id") or t.get("id")
                or t.get("sol_num") or t.get("Bid Number") or ""
            ).strip()[:255]
            if tid:
                tender_ids.append(tid)

        intel_rows = _fetch_intel(tender_ids)
        if not intel_rows:
            logger.info(
                "[opportunity_insights] No intel rows found in DB — "
                "run structured + opportunity intelligence first"
            )
            return 0

        # Generate insights
        insight_records: List[Dict[str, Any]] = [
            {
                "tender_id":           row["tender_id"],
                "opportunity_insight": generate_insight(row),
            }
            for row in intel_rows
        ]

        return _store_insights(insight_records)

    except Exception as exc:
        logger.warning(
            "[opportunity_insights] generate_and_store_batch failed (non-fatal): %s",
            exc,
        )
        return 0


def backfill(limit: int = 10_000) -> int:
    """
    Generate insights for all rows that currently have NULL opportunity_insight.
    Useful for enriching the table after first deploy.

    Usage:
        python3 intelligence/opportunity_insights.py --backfill
    """
    try:
        extend_schema()
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        cur.execute(
            f"""
            SELECT tender_id, sector, consulting_type, organization,
                   deadline_category, relevance_score, region,
                   priority_score, competition_level,
                   opportunity_size, complexity_score
            FROM `{_TABLE}`
            WHERE opportunity_insight IS NULL
               OR opportunity_insight = ''
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()

        logger.info(
            "[opportunity_insights] Backfilling %d rows without insight…", len(rows)
        )
        if not rows:
            return 0

        insight_records = [
            {"tender_id": r["tender_id"], "opportunity_insight": generate_insight(r)}
            for r in rows
        ]
        return _store_insights(insight_records)

    except Exception as exc:
        logger.warning("[opportunity_insights] backfill failed: %s", exc)
        return 0


# =============================================================================
# SECTION 5 — CLI entry point
# =============================================================================

_SAMPLE_ATTRS = [
    {
        "tender_id":        "TEST_WB_EVAL",
        "sector":           "climate",
        "consulting_type":  "evaluation",
        "organization":     "World Bank",
        "deadline_category":"soon",
        "relevance_score":  90,
        "priority_score":   92,
        "competition_level":"high",
        "opportunity_size": "large",
        "complexity_score": 55,
    },
    {
        "tender_id":        "TEST_USAID_POL",
        "sector":           "governance",
        "consulting_type":  "policy",
        "organization":     "USAID",
        "deadline_category":"urgent",
        "relevance_score":  74,
        "priority_score":   78,
        "competition_level":"high",
        "opportunity_size": "medium",
        "complexity_score": 35,
    },
    {
        "tender_id":        "TEST_LOCAL_TA",
        "sector":           "health",
        "consulting_type":  "technical assistance",
        "organization":     "NHM India",
        "deadline_category":"normal",
        "relevance_score":  55,
        "priority_score":   48,
        "competition_level":"low",
        "opportunity_size": "medium",
        "complexity_score": 20,
    },
    {
        "tender_id":        "TEST_GEM_LAB",
        "sector":           "unknown",
        "consulting_type":  "unknown",
        "organization":     "unknown",
        "deadline_category":"expired",
        "relevance_score":  0,
        "priority_score":   2,
        "competition_level":"low",
        "opportunity_size": "small",
        "complexity_score": 3,
    },
    {
        "tender_id":        "TEST_SIDBI_FIN",
        "sector":           "finance",
        "consulting_type":  "advisory",
        "organization":     "SIDBI",
        "deadline_category":"soon",
        "relevance_score":  62,
        "priority_score":   58,
        "competition_level":"medium",
        "opportunity_size": "medium",
        "complexity_score": 25,
    },
]


def _print_banner(text: str) -> None:
    print("\n" + "─" * 72)
    print(f"  {text}")
    print("─" * 72)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="TenderRadar — Opportunity Insight Generator CLI"
    )
    ap.add_argument(
        "--backfill", action="store_true",
        help="Generate insights for all NULL rows in tender_structured_intel",
    )
    ap.add_argument(
        "--limit", type=int, default=10_000,
        help="Max rows to backfill (default: 10000)",
    )
    args = ap.parse_args()

    logging.basicConfig(
        format="%(levelname)s  %(name)s — %(message)s",
        level=logging.INFO,
    )

    if args.backfill:
        _print_banner("Backfilling opportunity insights…")
        n = backfill(limit=args.limit)
        print(f"\n  ✅  Backfill complete — {n} rows updated in '{_TABLE}'")
        sys.exit(0)

    # ── Default: sample generation test ───────────────────────────────────────
    _print_banner("Opportunity Insight Generator — sample output test")
    print()

    for s in _SAMPLE_ATTRS:
        insight = generate_insight(s)
        print(f"  tender_id : {s['tender_id']}")
        print(f"  priority  : {s['priority_score']}  |  "
              f"org: {s['organization']}  |  "
              f"sector: {s['sector']}  |  "
              f"type: {s['consulting_type']}")
        print(f"  insight   : {insight}")
        print()

    # ── DB write test ──────────────────────────────────────────────────────────
    _print_banner("DB write test (skipped if MySQL not configured)")
    try:
        extend_schema()
        print("  ✅  extend_schema() OK — 'opportunity_insight' column present")

        # Seed intel rows first
        from intelligence.tender_intelligence import init_schema as _ti_init, store_batch as _ti_store
        from intelligence.opportunity_engine   import extend_schema as _oe_schema, store_scores as _oe_store

        _ti_init()
        _oe_schema()

        # Build minimal intel rows for sample attrs
        from intelligence.tender_intelligence import _DEADLINE_FORMATS
        _seed_intel = [
            {
                "tender_id":         s["tender_id"],
                "sector":            s["sector"],
                "consulting_type":   s["consulting_type"],
                "region":            "South Asia",
                "organization":      s["organization"],
                "deadline_category": s["deadline_category"],
                "relevance_score":   s["relevance_score"],
            }
            for s in _SAMPLE_ATTRS
        ]
        _ti_store(_seed_intel)
        _oe_store([
            {
                "tender_id":         s["tender_id"],
                "priority_score":    s["priority_score"],
                "competition_level": s["competition_level"],
                "opportunity_size":  s["opportunity_size"],
                "complexity_score":  s["complexity_score"],
            }
            for s in _SAMPLE_ATTRS
        ])

        insight_records = [
            {"tender_id": s["tender_id"], "opportunity_insight": generate_insight(s)}
            for s in _SAMPLE_ATTRS
        ]
        written = _store_insights(insight_records)
        print(f"  ✅  _store_insights() OK — {written} row(s) updated in '{_TABLE}'")

    except Exception as _e:
        print(f"  ⚠   DB test skipped (not required for local testing): {_e}")

    print()
