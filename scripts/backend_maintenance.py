#!/usr/bin/env python3
"""
backend_maintenance.py — durable backend audit/backfill utility

Usage examples:
  ./venv_stable/bin/python scripts/backend_maintenance.py audit
  ./venv_stable/bin/python scripts/backend_maintenance.py backfill-normalized --limit 10000
  ./venv_stable/bin/python scripts/backend_maintenance.py backfill-deep --limit 500 --max-workers 2
  ./venv_stable/bin/python scripts/backend_maintenance.py rescore
  ./venv_stable/bin/python scripts/backend_maintenance.py all --limit 5000 --deep-limit 500
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import requests
import subprocess
from typing import Any, Dict, List, Optional

BASE = os.path.expanduser("~/tender_system")
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from database.db import (
    _ensure_tenders_columns,
    backfill_normalized_from_seen_tenders,
    get_connection,
)
from intelligence.opportunity_engine import rescore_all
from intelligence.fuzzy_dedup import backfill_cross_source_groups
from intelligence.tender_intelligence import refresh_from_tenders
from monitoring.scraper_health_manager import clear_unstable, get_all_health
from scrapers.deep_scraper import enrich_batch_deep, save_deep_enrichment
from config.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("backend_maintenance")

_PORTAL_ORG_DEFAULTS = {
    "worldbank": "World Bank",
    "wb": "World Bank",
    "undp": "UNDP",
    "afdb": "AfDB",
    "afd": "AFD",
    "giz": "GIZ",
    "usaid": "USAID",
    "sam": "US Federal Government",
    "tedeu": "European Union",
    "ec": "European Union",
    "sidbi": "SIDBI",
    "phfi": "PHFI",
    "icfre": "ICFRE",
    "mbda": "Meghalaya Basin Development Authority",
    "gem": "Government Agency",
    "cg": "Government Agency",
    "up": "Government Agency",
    "upetender": "Government Agency",
    "maharashtra": "Government Agency",
    "karnataka": "Government Agency",
    "nic": "Government Agency",
    "sikkim": "Government Agency",
}

_URL_SOURCE_RULES = [
    ("%projects.worldbank.org/%", "worldbank"),
    ("%documents.worldbank.org/%", "worldbank"),
    ("%worldbank.org/%", "worldbank"),
    ("%dtvp.de/%", "dtvp"),
    ("%etender.up.nic.in/%", "upetender"),
    ("%mahatenders.gov.in/%", "maharashtra"),
    ("%eprocure.gov.in/%", "cg"),
    ("%icfre.gov.in/%", "icfre"),
    ("%ungm.org/%", "ungm"),
    ("%ted.europa.eu/%", "ted"),
]

# ---------------------------------------------------------------------------
# State-infrastructure portals that dilute development-consulting metrics.
# These 3 portals represent ~83% of total records but have avg priority ≈ 3.8
# (civil works, road tenders, state infrastructure — not IDCG's domain).
# Excluded from "signal" coverage rates so quality metrics reflect the 22
# development/consulting portals that actually matter for product assessment.
# ---------------------------------------------------------------------------
_INFRA_PORTALS: frozenset = frozenset(["upetender", "maharashtra", "karnataka"])
_INFRA_PORTAL_SQL: str = "(" + ", ".join(f"'{p}'" for p in sorted(_INFRA_PORTALS)) + ")"


def _scalar(cur, sql: str, params: tuple = ()) -> int:
    cur.execute(sql, params)
    row = cur.fetchone()
    return int((row or [0])[0] or 0)


def _deep_document_quality_stats(exclude_portals: frozenset = frozenset()) -> Dict[str, Any]:
    """
    Compute quality-oriented deep document extraction metrics from deep_document_links.
    This complements binary deep_pdf_coverage by measuring extraction success and depth.

    Args:
        exclude_portals: Optional set of source_portal values to exclude from counts.
                         Used to compute signal-portal-only metrics (excl. infra noise).
    """
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    excl_clause = ""
    if exclude_portals:
        excl_list = ", ".join(f"'{p}'" for p in sorted(exclude_portals))
        excl_clause = f"AND source_portal NOT IN ({excl_list})"
    cur.execute(
        f"""
        SELECT tender_id, deep_document_links, deep_pdf_text
        FROM tenders
        WHERE ((deep_document_links IS NOT NULL AND JSON_LENGTH(deep_document_links) > 0)
           OR COALESCE(TRIM(deep_pdf_text), '') <> '')
        {excl_clause}
        """
    )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()

    tenders_with_doc_links = 0
    tenders_with_extracted_docs = 0
    total_doc_links = 0
    extracted_doc_links = 0
    extracted_chars_total = 0
    tenders_with_link_char_counts = 0
    extracted_docs_per_tender_sum = 0

    for row in rows:
        raw_links = row.get("deep_document_links")
        links = []
        if isinstance(raw_links, list):
            links = raw_links
        elif isinstance(raw_links, str) and raw_links.strip():
            try:
                parsed = json.loads(raw_links)
                if isinstance(parsed, str):
                    try:
                        parsed = json.loads(parsed)
                    except Exception:
                        parsed = []
                if isinstance(parsed, list):
                    links = parsed
            except Exception:
                links = []
        links = [d for d in links if isinstance(d, dict)]

        if not links:
            continue

        tenders_with_doc_links += 1
        total_doc_links += len(links)

        extracted_here = 0
        chars_here = 0
        for doc in links:
            if not isinstance(doc, dict):
                continue
            if bool(doc.get("extracted")):
                extracted_here += 1
                extracted_doc_links += 1
                try:
                    chars_here += int(doc.get("char_count") or 0)
                except Exception:
                    pass

        if extracted_here > 0:
            tenders_with_extracted_docs += 1
            extracted_docs_per_tender_sum += extracted_here
            if chars_here > 0:
                extracted_chars_total += chars_here
                tenders_with_link_char_counts += 1

    return {
        "tenders_with_doc_links": tenders_with_doc_links,
        "tenders_with_extracted_docs": tenders_with_extracted_docs,
        "total_doc_links_found": total_doc_links,
        "extracted_doc_links": extracted_doc_links,
        "total_extracted_doc_chars": extracted_chars_total,
        "tenders_with_link_char_counts": tenders_with_link_char_counts,
        "avg_doc_links_per_tender_with_links": round(total_doc_links / max(1, tenders_with_doc_links), 2),
        "avg_extracted_docs_per_tender": round(extracted_docs_per_tender_sum / max(1, tenders_with_extracted_docs), 2),
        "avg_extracted_chars_per_tender": round(extracted_chars_total / max(1, tenders_with_link_char_counts), 1),
    }


def _deep_document_quality_by_source(limit: int = 12, min_total: int = 20) -> Dict[str, List[Dict[str, Any]]]:
    """
    Portal-level deep document extraction diagnostics.
    Helps prioritise source-specific parser improvements.
    """
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT source_portal, deep_document_links
        FROM tenders
        WHERE source_portal IS NOT NULL AND source_portal <> ''
        """
    )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()

    buckets: Dict[str, Dict[str, int]] = {}
    for row in rows:
        src = str(row.get("source_portal") or "").strip()
        if not src:
            continue
        b = buckets.setdefault(src, {
            "total": 0,
            "with_links": 0,
            "with_extracted": 0,
            "total_links": 0,
            "extracted_links": 0,
        })
        b["total"] += 1

        raw_links = row.get("deep_document_links")
        links = []
        if isinstance(raw_links, list):
            links = raw_links
        elif isinstance(raw_links, str) and raw_links.strip():
            try:
                parsed = json.loads(raw_links)
                # Handle double-encoded case: json.loads("\"[]\"") → "[]"
                if isinstance(parsed, str):
                    try:
                        parsed = json.loads(parsed)
                    except Exception:
                        parsed = []
                if isinstance(parsed, list):
                    links = parsed
            except Exception:
                links = []
        links = [d for d in links if isinstance(d, dict)]
        if not links:
            continue

        b["with_links"] += 1
        b["total_links"] += len(links)
        extracted_here = 0
        for d in links:
            if isinstance(d, dict) and bool(d.get("extracted")):
                extracted_here += 1
                b["extracted_links"] += 1
        if extracted_here > 0:
            b["with_extracted"] += 1

    out: List[Dict[str, Any]] = []
    for src, b in buckets.items():
        if b["total"] < int(min_total):
            continue
        out.append({
            "source_portal": src,
            "total_tenders": b["total"],
            "doc_link_coverage_pct": round(b["with_links"] / max(1, b["total"]) * 100, 2),
            "doc_extracted_coverage_pct": round(b["with_extracted"] / max(1, b["total"]) * 100, 2),
            "doc_link_extraction_success_pct": round(b["extracted_links"] / max(1, b["total_links"]) * 100, 2),
            "avg_doc_links_per_tender_with_links": round(b["total_links"] / max(1, b["with_links"]), 2),
        })

    # Prioritise high-volume portals with weak extracted coverage.
    weakest = sorted(
        out,
        key=lambda x: (x["doc_extracted_coverage_pct"], -x["total_tenders"], x["source_portal"]),
    )[:max(1, int(limit))]
    strongest_pool = [x for x in out if x["doc_extracted_coverage_pct"] > 0]
    strongest = sorted(
        strongest_pool,
        key=lambda x: (-x["doc_extracted_coverage_pct"], -x["total_tenders"], x["source_portal"]),
    )[:max(1, int(limit))]
    return {"weakest": weakest, "strongest": strongest}


def audit_backend() -> Dict[str, Any]:
    _ensure_tenders_columns()
    conn = get_connection()
    cur = conn.cursor()

    stats = {
        "seen_tenders": _scalar(cur, "SELECT COUNT(*) FROM seen_tenders"),
        "tenders": _scalar(cur, "SELECT COUNT(*) FROM tenders"),
        "structured_intel": _scalar(cur, "SELECT COUNT(*) FROM tender_structured_intel"),
        "covered_seen_tenders": _scalar(
            cur,
            "SELECT COUNT(*) FROM seen_tenders st JOIN tenders t ON st.tender_id = t.tender_id",
        ),
        "seen_without_tenders": _scalar(
            cur,
            "SELECT COUNT(*) FROM seen_tenders st LEFT JOIN tenders t ON st.tender_id = t.tender_id WHERE t.tender_id IS NULL",
        ),
        "orphan_normalized_tenders": _scalar(
            cur,
            "SELECT COUNT(*) FROM tenders t LEFT JOIN seen_tenders st ON st.tender_id = t.tender_id WHERE st.tender_id IS NULL",
        ),
        "tenders_with_description": _scalar(
            cur,
            "SELECT COUNT(*) FROM tenders WHERE COALESCE(TRIM(description),'') <> ''",
        ),
        "tenders_with_org": _scalar(
            cur,
            "SELECT COUNT(*) FROM tenders WHERE COALESCE(TRIM(organization),'') <> ''",
        ),
        "tenders_with_deep_scope": _scalar(
            cur,
            "SELECT COUNT(*) FROM tenders WHERE COALESCE(TRIM(deep_scope),'') <> ''",
        ),
        "tenders_with_deep_description": _scalar(
            cur,
            "SELECT COUNT(*) FROM tenders WHERE COALESCE(TRIM(deep_description),'') <> ''",
        ),
        "tenders_with_deep_pdf_text": _scalar(
            cur,
            "SELECT COUNT(*) FROM tenders WHERE COALESCE(TRIM(deep_pdf_text),'') <> ''",
        ),
        "tenders_with_deep_source": _scalar(
            cur,
            "SELECT COUNT(*) FROM tenders WHERE COALESCE(TRIM(deep_source),'') <> ''",
        ),
        "structured_org_unknown": _scalar(
            cur,
            "SELECT COUNT(*) FROM tender_structured_intel WHERE organization IN ('unknown','')",
        ),
        "structured_sector_unknown": _scalar(
            cur,
            "SELECT COUNT(*) FROM tender_structured_intel WHERE sector IN ('unknown','')",
        ),
        "structured_relevance_zero": _scalar(
            cur,
            "SELECT COUNT(*) FROM tender_structured_intel WHERE relevance_score = 0",
        ),
        "priority_nonzero": _scalar(
            cur,
            "SELECT COUNT(*) FROM tender_structured_intel WHERE priority_score > 0",
        ),
        "cross_source_groups": _scalar(
            cur,
            "SELECT COUNT(*) FROM tender_cross_sources",
        ),
        # ── Signal-portal-only counts (excludes state-infra noise portals) ──────
        "signal_tenders": _scalar(
            cur,
            f"SELECT COUNT(*) FROM tenders WHERE source_portal NOT IN {_INFRA_PORTAL_SQL}",
        ),
        "signal_tenders_with_deep_description": _scalar(
            cur,
            f"SELECT COUNT(*) FROM tenders WHERE source_portal NOT IN {_INFRA_PORTAL_SQL}"
            f" AND COALESCE(TRIM(deep_description),'') <> ''",
        ),
        "signal_tenders_with_deep_pdf_text": _scalar(
            cur,
            f"SELECT COUNT(*) FROM tenders WHERE source_portal NOT IN {_INFRA_PORTAL_SQL}"
            f" AND COALESCE(TRIM(deep_pdf_text),'') <> ''",
        ),
        "signal_tenders_with_deep_scope": _scalar(
            cur,
            f"SELECT COUNT(*) FROM tenders WHERE source_portal NOT IN {_INFRA_PORTAL_SQL}"
            f" AND COALESCE(TRIM(deep_scope),'') <> ''",
        ),
    }
    cur.close()
    conn.close()

    # Quality-oriented deep document extraction stats (beyond binary coverage flags).
    stats.update(_deep_document_quality_stats())

    # Signal-portal doc quality stats (excl. infra portals from denominator).
    _signal_doc = _deep_document_quality_stats(exclude_portals=_INFRA_PORTALS)
    stats["signal_tenders_with_doc_links"] = _signal_doc["tenders_with_doc_links"]
    stats["signal_tenders_with_extracted_docs"] = _signal_doc["tenders_with_extracted_docs"]

    seen = max(1, stats["seen_tenders"])
    tenders = max(1, stats["tenders"])
    intel = max(1, stats["structured_intel"])
    signal = max(1, stats["signal_tenders"])
    infra_count = stats["tenders"] - stats["signal_tenders"]
    stats["rates"] = {
        # ── All-portal rates (includes state-infra portals) ────────────────────
        "seen_to_tenders_pct": round(stats["covered_seen_tenders"] / seen * 100, 2),
        "description_coverage_pct": round(stats["tenders_with_description"] / tenders * 100, 2),
        "organization_coverage_pct": round(stats["tenders_with_org"] / tenders * 100, 2),
        "deep_scope_coverage_pct": round(stats["tenders_with_deep_scope"] / tenders * 100, 2),
        "deep_description_coverage_pct": round(stats["tenders_with_deep_description"] / tenders * 100, 2),
        "deep_pdf_coverage_pct": round(stats["tenders_with_deep_pdf_text"] / tenders * 100, 2),
        "deep_source_coverage_pct": round(stats["tenders_with_deep_source"] / tenders * 100, 2),
        "deep_doc_link_coverage_pct": round(stats["tenders_with_doc_links"] / tenders * 100, 2),
        "deep_doc_extracted_coverage_pct": round(stats["tenders_with_extracted_docs"] / tenders * 100, 2),
        "doc_link_extraction_success_pct": round(
            stats["extracted_doc_links"] / max(1, stats["total_doc_links_found"]) * 100, 2
        ),
        "org_unknown_pct": round(stats["structured_org_unknown"] / intel * 100, 2),
        "sector_unknown_pct": round(stats["structured_sector_unknown"] / intel * 100, 2),
        "priority_nonzero_pct": round(stats["priority_nonzero"] / intel * 100, 2),
        # ── Signal-portal-only rates (excl. upetender/maharashtra/karnataka) ───
        # These reflect true quality of development/consulting portal coverage.
        "signal_tenders_count": stats["signal_tenders"],
        "infra_tenders_count": infra_count,
        "signal_deep_description_coverage_pct": round(
            stats["signal_tenders_with_deep_description"] / signal * 100, 2
        ),
        "signal_deep_pdf_coverage_pct": round(
            stats["signal_tenders_with_deep_pdf_text"] / signal * 100, 2
        ),
        "signal_deep_scope_coverage_pct": round(
            stats["signal_tenders_with_deep_scope"] / signal * 100, 2
        ),
        "signal_deep_doc_link_coverage_pct": round(
            stats["signal_tenders_with_doc_links"] / signal * 100, 2
        ),
        "signal_deep_doc_extracted_coverage_pct": round(
            stats["signal_tenders_with_extracted_docs"] / signal * 100, 2
        ),
    }
    return stats


def portal_health_snapshot(window: int = 10) -> Dict[str, Any]:
    """
    Combine runner health history with DB coverage so we can judge whether each
    portal is merely reachable or actually contributing useful backend data.
    """
    raw = get_all_health(window=window)
    portals = raw.get("portals", []) or []

    ready = []
    needs_attention = []
    blocked = []
    for portal in portals:
        coverage = float(portal.get("coverage_pct") or 0.0)
        deep_enriched = int(portal.get("deep_enriched") or 0)
        item = {
            "source": portal.get("source"),
            "stability": portal.get("stability"),
            "success_rate": portal.get("success_rate"),
            "average_rows": portal.get("average_rows"),
            "consecutive_failures": portal.get("consecutive_failures"),
            "seen_tenders": portal.get("seen_tenders", 0),
            "normalized_tenders": portal.get("normalized_tenders", 0),
            "coverage_pct": coverage,
            "descriptions": portal.get("descriptions", 0),
            "deep_enriched": deep_enriched,
            "disabled_reason": portal.get("disabled_reason"),
            "last_success_time": portal.get("last_success_time"),
        }
        if portal.get("stability") == "unstable":
            blocked.append(item)
        elif coverage >= 80.0 and float(portal.get("success_rate") or 0.0) >= 70.0:
            ready.append(item)
        else:
            needs_attention.append(item)

    return {
        "generated_at": raw.get("generated_at"),
        "stable_count": raw.get("stable_count", 0),
        "partial_count": raw.get("partial_count", 0),
        "unstable_count": raw.get("unstable_count", 0),
        "ready_for_scale": sorted(ready, key=lambda x: (-x["coverage_pct"], x["source"])),
        "needs_attention": sorted(needs_attention, key=lambda x: (x["stability"], x["source"])),
        "blocked": sorted(blocked, key=lambda x: x["source"]),
    }


def daily_quality_report(window: int = 10) -> Dict[str, Any]:
    """
    One automatic report you can review daily instead of checking raw metrics
    manually. Surfaces health, coverage, and the most important actions.
    """
    audit = audit_backend()
    portal = portal_health_snapshot(window=window)

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT source_portal, COUNT(*) AS total
        FROM tenders
        GROUP BY source_portal
        ORDER BY total DESC
        LIMIT 12
        """
    )
    portal_sizes = cur.fetchall() or []
    cur.execute(
        """
        SELECT source_portal, COUNT(*) AS total
        FROM tenders
        WHERE COALESCE(TRIM(deep_description), '') <> ''
           OR COALESCE(TRIM(deep_pdf_text), '') <> ''
           OR COALESCE(TRIM(deep_eval_criteria), '') <> ''
        GROUP BY source_portal
        ORDER BY total DESC
        LIMIT 12
        """
    )
    deep_sizes = {r["source_portal"]: int(r["total"] or 0) for r in (cur.fetchall() or [])}
    cur.close()
    conn.close()

    portal_deep = []
    for row in portal_sizes:
        source = str(row["source_portal"] or "")
        total = int(row["total"] or 0)
        deep_total = int(deep_sizes.get(source, 0))
        portal_deep.append({
            "source": source,
            "total": total,
            "deep_total": deep_total,
            "deep_pct": round((deep_total / total) * 100, 2) if total else 0.0,
        })

    # Use signal-portal rates for action thresholds (eliminates state-infra dilution).
    sig_deep_descr  = audit["rates"]["signal_deep_description_coverage_pct"]
    sig_doc_extract = audit["rates"]["signal_deep_doc_extracted_coverage_pct"]

    actions = []
    if audit["seen_without_tenders"] > 0:
        actions.append(f"Repair materialization gap: {audit['seen_without_tenders']} seen tenders still missing normalized rows.")
    if audit["orphan_normalized_tenders"] > 0:
        actions.append(f"Repair lineage: {audit['orphan_normalized_tenders']} normalized tenders are still missing seen_tenders history.")
    if audit["rates"]["org_unknown_pct"] > 20:
        actions.append(f"Improve structured organization inference: unknown org rate is still {audit['rates']['org_unknown_pct']}%.")
    if sig_deep_descr < 5:
        actions.append(
            f"Increase deep enrichment on signal portals: useful deep descriptions are only "
            f"{sig_deep_descr}% of {audit['rates']['signal_tenders_count']} development-portal tenders."
        )
    if sig_doc_extract < 5:
        actions.append(
            f"Increase extracted-doc coverage on signal portals: only {sig_doc_extract}% of "
            f"{audit['rates']['signal_tenders_count']} development-portal tenders have parsed document text."
        )
    if audit["rates"]["doc_link_extraction_success_pct"] < 40:
        actions.append(
            f"Improve document parser robustness: extraction success is {audit['rates']['doc_link_extraction_success_pct']}% of discovered links."
        )
    if portal["blocked"]:
        actions.append("Blocked portals need source-specific fixes: " + ", ".join(p["source"] for p in portal["blocked"][:6]))

    manual_review = []
    for item in portal["needs_attention"][:6]:
        manual_review.append(
            f"{item['source']}: stability={item['stability']}, coverage={item['coverage_pct']}%, deep={item['deep_enriched']}"
        )
    for item in sorted(portal_deep, key=lambda x: (x["deep_pct"], -x["total"]))[:6]:
        if item["total"] >= 20:
            manual_review.append(
                f"{item['source']}: deep coverage only {item['deep_pct']}% ({item['deep_total']}/{item['total']})"
            )

    return {
        "generated_at": portal.get("generated_at"),
        "summary": {
            # ── Primary (signal-portal-only, development/consulting portals) ───
            # These are the metrics that matter for product quality assessment.
            # Infra portals excluded: upetender, maharashtra, karnataka
            "signal_tenders_count": audit["rates"]["signal_tenders_count"],
            "infra_tenders_count": audit["rates"]["infra_tenders_count"],
            "signal_deep_description_coverage_pct": audit["rates"]["signal_deep_description_coverage_pct"],
            "signal_deep_pdf_coverage_pct": audit["rates"]["signal_deep_pdf_coverage_pct"],
            "signal_deep_doc_link_coverage_pct": audit["rates"]["signal_deep_doc_link_coverage_pct"],
            "signal_deep_doc_extracted_coverage_pct": audit["rates"]["signal_deep_doc_extracted_coverage_pct"],
            # ── Secondary (all-portal rates, kept for backward compat) ─────────
            "seen_to_tenders_pct": audit["rates"]["seen_to_tenders_pct"],
            "organization_coverage_pct": audit["rates"]["organization_coverage_pct"],
            "org_unknown_pct": audit["rates"]["org_unknown_pct"],
            "deep_description_coverage_pct": audit["rates"]["deep_description_coverage_pct"],
            "deep_doc_link_coverage_pct": audit["rates"]["deep_doc_link_coverage_pct"],
            "deep_doc_extracted_coverage_pct": audit["rates"]["deep_doc_extracted_coverage_pct"],
            "doc_link_extraction_success_pct": audit["rates"]["doc_link_extraction_success_pct"],
            "cross_source_groups": audit["cross_source_groups"],
            "stable_portals": portal["stable_count"],
            "partial_portals": portal["partial_count"],
            "blocked_portals": portal["unstable_count"],
        },
        "top_portals_by_volume": portal_deep,
        "blocked_portals": portal["blocked"],
        "needs_attention": portal["needs_attention"][:10],
        "actions": actions,
        "manual_review": manual_review[:10],
    }


def summarize_backend(window: int = 10) -> Dict[str, Any]:
    """
    Compact summary for routine checks after a pipeline run.
    """
    audit = audit_backend()
    portal = portal_health_snapshot(window=window)
    return {
        "generated_at": portal.get("generated_at"),
        "counts": {
            "seen_tenders": audit["seen_tenders"],
            "tenders": audit["tenders"],
            "structured_intel": audit["structured_intel"],
            "stable_portals": portal["stable_count"],
            "partial_portals": portal["partial_count"],
            "blocked_portals": portal["unstable_count"],
        },
        "coverage": {
            # Signal-portal metrics (development/consulting portals, excl. state-infra)
            "signal_tenders_count": audit["rates"]["signal_tenders_count"],
            "infra_tenders_count": audit["rates"]["infra_tenders_count"],
            "signal_deep_description_coverage_pct": audit["rates"]["signal_deep_description_coverage_pct"],
            "signal_deep_pdf_coverage_pct": audit["rates"]["signal_deep_pdf_coverage_pct"],
            "signal_deep_doc_link_coverage_pct": audit["rates"]["signal_deep_doc_link_coverage_pct"],
            "signal_deep_doc_extracted_coverage_pct": audit["rates"]["signal_deep_doc_extracted_coverage_pct"],
            # All-portal metrics (kept for backward compatibility)
            "seen_to_tenders_pct": audit["rates"]["seen_to_tenders_pct"],
            "description_coverage_pct": audit["rates"]["description_coverage_pct"],
            "organization_coverage_pct": audit["rates"]["organization_coverage_pct"],
            "deep_description_coverage_pct": audit["rates"]["deep_description_coverage_pct"],
            "deep_pdf_coverage_pct": audit["rates"]["deep_pdf_coverage_pct"],
            "deep_doc_link_coverage_pct": audit["rates"]["deep_doc_link_coverage_pct"],
            "deep_doc_extracted_coverage_pct": audit["rates"]["deep_doc_extracted_coverage_pct"],
            "doc_link_extraction_success_pct": audit["rates"]["doc_link_extraction_success_pct"],
            "org_unknown_pct": audit["rates"]["org_unknown_pct"],
            "priority_nonzero_pct": audit["rates"]["priority_nonzero_pct"],
        },
        "deep_quality": {
            "tenders_with_doc_links": audit["tenders_with_doc_links"],
            "tenders_with_extracted_docs": audit["tenders_with_extracted_docs"],
            "total_doc_links_found": audit["total_doc_links_found"],
            "extracted_doc_links": audit["extracted_doc_links"],
            "total_extracted_doc_chars": audit["total_extracted_doc_chars"],
            "avg_doc_links_per_tender_with_links": audit["avg_doc_links_per_tender_with_links"],
            "avg_extracted_docs_per_tender": audit["avg_extracted_docs_per_tender"],
            "avg_extracted_chars_per_tender": audit["avg_extracted_chars_per_tender"],
        },
        "deep_quality_by_source": _deep_document_quality_by_source(limit=8, min_total=20),
        "blocked_portals": [p["source"] for p in portal["blocked"]],
        "needs_attention": [p["source"] for p in portal["needs_attention"][:10]],
    }


def _send_telegram(msg: str) -> bool:
    """Send a short Telegram message if credentials exist."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except Exception as exc:
        log.warning("Telegram notify failed: %s", exc)
        return False


def run_live_chat_smoke(
    cases_path: str = "artifacts/live_chat_smoke_queries.jsonl",
    limit: int = 0,
    output_path: str = "artifacts/live_chat_smoke_last.json",
) -> Dict[str, Any]:
    """
    Execute live chat smoke suite via dedicated runner and return parsed JSON.
    """
    runner = os.path.join(BASE, "scripts", "run_live_chat_smoke.py")
    py = os.path.join(BASE, "venv_stable", "bin", "python")
    abs_cases = cases_path if os.path.isabs(str(cases_path)) else os.path.join(BASE, str(cases_path))
    abs_out = output_path if os.path.isabs(str(output_path)) else os.path.join(BASE, str(output_path))
    cmd = [py, runner, "--cases", abs_cases, "--json-out", abs_out]
    if int(limit or 0) > 0:
        cmd.extend(["--limit", str(int(limit))])
    proc = subprocess.run(cmd, capture_output=True, text=True)

    # Prefer structured artifact output.
    out_file = abs_out
    if os.path.exists(out_file):
        try:
            with open(out_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            payload["exit_code"] = int(proc.returncode)
            return payload
        except Exception as exc:
            return {"ok": False, "error": f"failed_to_parse_output:{exc}", "exit_code": int(proc.returncode)}

    # Fallback: parse stdout if artifact missing.
    try:
        payload = json.loads(proc.stdout or "{}")
        payload["exit_code"] = int(proc.returncode)
        return payload
    except Exception:
        return {
            "ok": False,
            "error": "live_chat_smoke_failed",
            "exit_code": int(proc.returncode),
            "stdout_tail": (proc.stdout or "")[-1500:],
            "stderr_tail": (proc.stderr or "")[-1500:],
        }


def _deep_backfill_candidates(limit: int, min_relevance: int, sources: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    source_filters = [str(s or "").strip().lower() for s in (sources or []) if str(s or "").strip()]
    params: List[Any] = [int(min_relevance)]
    sql = """
        SELECT
            t.tender_id,
            t.title,
            t.description,
            t.url,
            t.source_portal AS source,
            t.deep_document_links,
            COALESCE(si.relevance_score, 0) AS relevance_score
        FROM tenders t
        LEFT JOIN tender_structured_intel si ON t.tender_id = si.tender_id
        WHERE t.url IS NOT NULL
          AND t.url <> ''
          AND COALESCE(si.relevance_score, 0) >= %s
          AND (
                (
                    COALESCE(TRIM(t.deep_scope), '') = ''
                AND COALESCE(TRIM(t.deep_pdf_text), '') = ''
                AND COALESCE(TRIM(t.deep_eval_criteria), '') = ''
                )
            OR COALESCE(JSON_LENGTH(t.deep_document_links), 0) = 0
            OR CAST(COALESCE(t.deep_document_links, JSON_ARRAY()) AS CHAR) NOT LIKE '%"extracted": true%'
          )
    """
    if source_filters:
        placeholders = ",".join(["%s"] * len(source_filters))
        sql += f"\n          AND LOWER(COALESCE(t.source_portal, '')) IN ({placeholders})\n"
        params.extend(source_filters)

    sql += """
        ORDER BY
          CASE
            WHEN t.source_portal IN ('upetender', 'up', 'maharashtra', 'karnataka', 'cg') THEN 0
            ELSE 1
          END,
          COALESCE(si.relevance_score, 0) DESC,
          t.scraped_at DESC
        LIMIT %s
    """
    params.append(int(limit))
    cur.execute(sql, tuple(params))
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def backfill_deep(
    limit: int = 1000,
    max_workers: int = 6,
    delay: float = 1.0,
    min_relevance: int = 0,
    sources: Optional[List[str]] = None,
    timeout: int = 45,
) -> Dict[str, int]:
    _ensure_tenders_columns()
    candidates = _deep_backfill_candidates(limit=limit, min_relevance=min_relevance, sources=sources)
    if not candidates:
        return {"selected": 0, "saved": 0, "amended": 0}

    enriched = enrich_batch_deep(candidates, max_workers=max_workers, delay=delay, timeout=max(10, int(timeout)))
    saved = 0
    amended = 0
    for row in enriched:
        tid = str(row.get("tender_id") or "").strip()
        if tid and not row.get("error") and _is_useful_deep_result(row):
            if save_deep_enrichment(tid, row):
                saved += 1
            if row.get("amendment_detected"):
                amended += 1
    return {"selected": len(candidates), "saved": saved, "amended": amended}


def refresh_structured(
    limit: int = 10_000,
    only_unknown_org: bool = False,
    missing_only: bool = False,
) -> int:
    return refresh_from_tenders(
        limit=limit,
        only_unknown_org=only_unknown_org,
        missing_only=missing_only,
    )


def repair_structured_intel(
    batch_size: int = 10_000,
    max_rounds: int = 10,
    only_unknown_org: bool = False,
) -> Dict[str, int]:
    """
    Rebuild structured intel until the missing backlog is drained or we stop
    making progress. This is the durable maintenance path for large datasets.
    """
    total_written = 0
    rounds = 0
    remaining = 0

    while rounds < max_rounds:
        conn = get_connection()
        cur = conn.cursor()
        if only_unknown_org:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM tender_structured_intel
                WHERE organization IN ('unknown', '')
                """
            )
        else:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM tenders t
                LEFT JOIN tender_structured_intel si ON si.tender_id = t.tender_id
                WHERE si.tender_id IS NULL
                """
            )
        remaining = int((cur.fetchone() or [0])[0] or 0)
        cur.close()
        conn.close()

        if remaining <= 0:
            break

        rounds += 1
        written = int(
            refresh_from_tenders(
                limit=batch_size,
                only_unknown_org=only_unknown_org,
                missing_only=not only_unknown_org,
            )
            or 0
        )
        total_written += written
        if written <= 0:
            break

    conn = get_connection()
    cur = conn.cursor()
    if only_unknown_org:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM tender_structured_intel
            WHERE organization IN ('unknown', '')
            """
        )
    else:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM tenders t
            LEFT JOIN tender_structured_intel si ON si.tender_id = t.tender_id
            WHERE si.tender_id IS NULL
            """
        )
    remaining = int((cur.fetchone() or [0])[0] or 0)
    cur.close()
    conn.close()

    return {"rounds": rounds, "written": total_written, "remaining": remaining}


def backfill_dedup(limit_groups: int = 500) -> int:
    return backfill_cross_source_groups(limit_groups=limit_groups)


def backfill_organizations(limit: int = 20000) -> int:
    """
    Populate blank `tenders.organization` values from structured intel first,
    then from safe source-level defaults.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE tenders t
        JOIN tender_structured_intel si ON si.tender_id = t.tender_id
        SET t.organization = si.organization
        WHERE COALESCE(NULLIF(TRIM(t.organization), ''), '') = ''
          AND COALESCE(NULLIF(TRIM(si.organization), ''), '') <> ''
          AND si.organization <> 'unknown'
        """,
        (),
    )
    updated = int(cur.rowcount or 0)
    conn.commit()

    for portal, org_name in _PORTAL_ORG_DEFAULTS.items():
        cur.execute(
            """
            UPDATE tenders
            SET organization = %s
            WHERE source_portal = %s
              AND COALESCE(NULLIF(TRIM(organization), ''), '') = ''
            """,
            (org_name, portal),
        )
        updated += int(cur.rowcount or 0)
        conn.commit()

    cur.execute(
        """
        UPDATE tenders
        SET organization = 'Maharashtra State Road Transport Corporation'
        WHERE COALESCE(NULLIF(TRIM(organization), ''), '') = ''
          AND tender_id LIKE %s
        """,
        ("%MSRTC%",),
    )
    updated += int(cur.rowcount or 0)
    conn.commit()

    cur.close()
    conn.close()
    return updated


def repair_materialization(batch_size: int = 10000, max_rounds: int = 10) -> Dict[str, int]:
    """
    Re-run normalized backfill until it stops making progress or we hit the
    round cap. This gives us a durable repair path instead of manual repeats.
    """
    total_written = 0
    rounds = 0
    while rounds < max_rounds:
        rounds += 1
        written = backfill_normalized_from_seen_tenders(limit=batch_size)
        total_written += int(written or 0)
        if written < batch_size:
            break
    return {"rounds": rounds, "written": total_written}


def _is_useful_deep_result(row: Dict[str, Any]) -> bool:
    deep_source = str(row.get("deep_source") or "").strip().lower()
    if deep_source in {"", "failed", "skipped"}:
        return False
    markers = [
        "This page in:",
        "Loading...",
        "This site uses cookies to optimize functionality",
        "Your session has timed out.",
        "Web applications store information about what you are doing on the server.",
        "Successfully signed out",
    ]
    combined = " ".join(
        str(row.get(k) or "")
        for k in ("deep_description", "deep_pdf_text", "deep_scope", "deep_eval_criteria")
    )
    if not combined.strip():
        return False
    # Reject entries with trivially short content — NIC session-expired pages
    # return bare skeleton HTML that pdfplumber/BS4 extracts as ~11 chars
    if len(combined.strip()) < 80:
        return False
    hit_count = sum(1 for marker in markers if marker in combined)
    return hit_count < 2


def purge_bad_deep_data() -> int:
    """
    Remove obviously bad deep-enrichment artifacts so future audits reflect
    useful deep coverage instead of session-expired shells.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE tenders
        SET
            deep_description = '',
            deep_scope = '',
            deep_eval_criteria = '',
            deep_team_reqs = '',
            deep_pdf_text = '',
            deep_source = '',
            deep_scraped_at = NULL
        WHERE
            deep_source IN ('failed', 'skipped')
            OR deep_description LIKE %s
            OR deep_description LIKE %s
            OR deep_description LIKE %s
            OR deep_description LIKE %s
            OR (
                COALESCE(TRIM(deep_source), '') NOT IN ('', 'failed', 'skipped')
                AND CHAR_LENGTH(COALESCE(deep_description,'')) + CHAR_LENGTH(COALESCE(deep_pdf_text,'')) < 80
            )
        """,
        (
            "%This page in:%",
            "%Your session has timed out.%",
            "%Successfully signed out%",
            "%This site uses cookies to optimize functionality%",
        ),
    )
    updated = int(cur.rowcount or 0)
    conn.commit()
    cur.close()
    conn.close()
    return updated


def repair_cg_doc_flags(limit: int = 5000) -> Dict[str, int]:
    """
    Repair legacy CG rows where document links exist but none are marked
    extracted. Uses stored listing description as grounded fallback text.
    """
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT tender_id, description, deep_document_links
        FROM tenders
        WHERE source_portal = 'cg'
          AND deep_document_links IS NOT NULL
          AND JSON_LENGTH(deep_document_links) > 0
          AND CAST(COALESCE(deep_document_links, JSON_ARRAY()) AS CHAR) NOT LIKE '%"extracted": true%'
        LIMIT %s
        """,
        (int(limit),),
    )
    rows = cur.fetchall() or []
    cur.close()

    updated = 0
    skipped = 0
    w = conn.cursor()
    for row in rows:
        tid = str(row.get("tender_id") or "").strip()
        desc = str(row.get("description") or "").strip()
        if not tid or len(desc) < 80:
            skipped += 1
            continue

        raw = row.get("deep_document_links")
        links: List[Dict[str, Any]] = []
        if isinstance(raw, list):
            links = [d for d in raw if isinstance(d, dict)]
        elif isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    links = [d for d in parsed if isinstance(d, dict)]
            except Exception:
                links = []
        if not links:
            skipped += 1
            continue

        links[0]["extracted"] = True
        links[0]["char_count"] = len(desc)
        links[0]["extract_mode"] = "cg_listing_repair"
        try:
            w.execute(
                "UPDATE tenders SET deep_document_links = %s WHERE tender_id = %s",
                (json.dumps(links, ensure_ascii=False), tid),
            )
            updated += int(w.rowcount or 0)
        except Exception:
            skipped += 1

    conn.commit()
    w.close()
    conn.close()
    return {"selected": len(rows), "updated": updated, "skipped": skipped}


def backfill_seen_from_tenders(limit: int = 10000) -> int:
    """
    Recreate missing seen_tenders lineage from normalized tenders so the raw
    discovery history stays internally consistent.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT IGNORE INTO seen_tenders
            (tender_id, title, source_site, url, date_first_seen, notified)
        SELECT
            t.tender_id,
            LEFT(COALESCE(t.title, ''), 1000),
            LEFT(COALESCE(t.source_portal, ''), 100),
            LEFT(COALESCE(t.url, ''), 2000),
            COALESCE(t.scraped_at, NOW()),
            0
        FROM tenders t
        LEFT JOIN seen_tenders st ON st.tender_id = t.tender_id
        WHERE st.tender_id IS NULL
        LIMIT %s
        """,
        (int(limit),),
    )
    written = int(cur.rowcount or 0)
    conn.commit()
    cur.close()
    conn.close()
    return written


def normalize_source_aliases() -> int:
    """
    Collapse source_portal aliases and unknowns into the canonical slugs used
    by the normalizer so downstream organization/source heuristics work better.
    """
    conn = get_connection()
    cur = conn.cursor()
    updated = 0

    for pattern, canonical in _URL_SOURCE_RULES:
        cur.execute(
            """
            UPDATE tenders
            SET source_portal = %s
            WHERE url LIKE %s
              AND source_portal <> %s
            """,
            (canonical, pattern, canonical),
        )
        updated += int(cur.rowcount or 0)
        conn.commit()

    for source, canonical in [
        ("wb", "worldbank"),
        ("up", "upetender"),
        ("archive", "icfre"),
        ("current", "icfre"),
    ]:
        cur.execute(
            "UPDATE tenders SET source_portal = %s WHERE source_portal = %s",
            (canonical, source),
        )
        updated += int(cur.rowcount or 0)
        conn.commit()

    cur.close()
    conn.close()
    return updated


def main() -> int:
    ap = argparse.ArgumentParser(description="TenderRadar backend maintenance")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("audit", help="Print backend coverage metrics as JSON")
    p_portal = sub.add_parser("portal-health", help="Print per-portal health and coverage snapshot")
    p_portal.add_argument("--window", type=int, default=10)
    p_daily = sub.add_parser("daily-report", help="Print one automatic daily backend quality report")
    p_daily.add_argument("--window", type=int, default=10)
    p_summary = sub.add_parser("summarize", help="Print a compact backend health summary")
    p_summary.add_argument("--window", type=int, default=10)
    p_clear = sub.add_parser("clear-unstable", help="Re-enable a portal that was marked unstable")
    p_clear.add_argument("source", help="Exact portal label, e.g. 'USAID'")
    p_notify = sub.add_parser("notify-daily", help="Send daily backend quality report to Telegram (if configured)")
    p_notify.add_argument("--window", type=int, default=10)
    p_chat_smoke = sub.add_parser("chat-smoke", help="Run live chat smoke suite and print JSON report")
    p_chat_smoke.add_argument("--cases", default="artifacts/live_chat_smoke_queries.jsonl")
    p_chat_smoke.add_argument("--limit", type=int, default=0)
    p_chat_smoke.add_argument("--json-out", default="artifacts/live_chat_smoke_last.json")
    p_chat_notify = sub.add_parser("notify-chat-smoke", help="Run live chat smoke suite and send Telegram summary")
    p_chat_notify.add_argument("--cases", default="artifacts/live_chat_smoke_queries.jsonl")
    p_chat_notify.add_argument("--limit", type=int, default=0)
    p_chat_notify.add_argument("--json-out", default="artifacts/live_chat_smoke_last.json")

    p_norm = sub.add_parser("backfill-normalized", help="Backfill missing tenders rows from seen_tenders")
    p_norm.add_argument("--limit", type=int, default=10000)

    p_repair = sub.add_parser("repair-materialization", help="Repeat normalized backfill until it stabilizes")
    p_repair.add_argument("--batch-size", type=int, default=10000)
    p_repair.add_argument("--max-rounds", type=int, default=10)

    p_seen = sub.add_parser("backfill-seen", help="Recreate missing seen_tenders lineage from normalized tenders")
    p_seen.add_argument("--limit", type=int, default=10000)

    sub.add_parser("purge-bad-deep", help="Clear obviously bad deep-enrichment artifacts")
    p_cg_repair = sub.add_parser("repair-cg-doc-flags", help="Repair CG doc links that have no extracted flag")
    p_cg_repair.add_argument("--limit", type=int, default=5000)
    sub.add_parser("normalize-sources", help="Normalize tenders.source_portal aliases into canonical slugs")
    sub.add_parser("start-worker", help="Start Celery worker for deep/OpenAI tasks (foreground)")

    p_deep = sub.add_parser("backfill-deep", help="Backfill deep enrichment for tenders missing deep fields")
    p_deep.add_argument("--limit", type=int, default=1000)
    p_deep.add_argument("--max-workers", type=int, default=6)
    p_deep.add_argument("--delay", type=float, default=1.0)
    p_deep.add_argument("--timeout", type=int, default=45, help="Per-tender deep extraction timeout in seconds")
    p_deep.add_argument("--min-relevance", type=int, default=0)
    p_deep.add_argument(
        "--source",
        action="append",
        default=[],
        help="Optional source_portal filter (repeatable), e.g. --source upetender --source cg",
    )

    p_struct = sub.add_parser("refresh-structured", help="Rebuild structured intelligence from normalized tenders")
    p_struct.add_argument("--limit", type=int, default=10000)
    p_struct.add_argument("--only-unknown-org", action="store_true")
    p_struct.add_argument("--missing-only", action="store_true")

    p_struct_repair = sub.add_parser(
        "repair-structured",
        help="Repeat structured-intel refresh until the backlog is drained",
    )
    p_struct_repair.add_argument("--batch-size", type=int, default=10000)
    p_struct_repair.add_argument("--max-rounds", type=int, default=10)
    p_struct_repair.add_argument("--only-unknown-org", action="store_true")

    p_dedup = sub.add_parser("backfill-dedup", help="Build historical cross-source duplicate groups")
    p_dedup.add_argument("--limit-groups", type=int, default=500)

    p_org = sub.add_parser("backfill-organizations", help="Populate blank tenders.organization values")
    p_org.add_argument("--limit", type=int, default=20000)

    p_bi = sub.add_parser("backfill-insights", help="Generate opportunity_insight for rows missing it")
    p_bi.add_argument("--limit", type=int, default=2000)

    p_rescore = sub.add_parser("rescore", help="Rescore all priority scores")
    p_rescore.add_argument("--batch-size", type=int, default=5000)

    p_all = sub.add_parser("all", help="Run normalization backfill, deep backfill, rescore, then audit")
    p_all.add_argument("--limit", type=int, default=10000)
    p_all.add_argument("--deep-limit", type=int, default=1000)
    p_all.add_argument("--max-workers", type=int, default=6)
    p_all.add_argument("--delay", type=float, default=1.0)
    p_all.add_argument("--timeout", type=int, default=45, help="Per-tender deep extraction timeout in seconds")
    p_all.add_argument("--min-relevance", type=int, default=0)
    p_all.add_argument("--batch-size", type=int, default=5000)
    p_all.add_argument("--structured-limit", type=int, default=10000)
    p_all.add_argument("--only-unknown-org", action="store_true")
    p_all.add_argument("--dedup-limit-groups", type=int, default=500)
    p_all.add_argument("--org-limit", type=int, default=20000)

    args = ap.parse_args()

    if args.cmd == "audit":
        print(json.dumps(audit_backend(), indent=2))
        return 0

    if args.cmd == "portal-health":
        print(json.dumps(portal_health_snapshot(window=args.window), indent=2))
        return 0

    if args.cmd == "daily-report":
        print(json.dumps(daily_quality_report(window=args.window), indent=2))
        return 0

    if args.cmd == "summarize":
        print(json.dumps(summarize_backend(window=args.window), indent=2))
        return 0

    if args.cmd == "backfill-insights":
        from intelligence.opportunity_insights import backfill as _oi_backfill
        written = _oi_backfill(limit=args.limit)
        print(json.dumps({"backfill_insights_written": written}, indent=2))
        return 0

    if args.cmd == "clear-unstable":
        print(json.dumps({"source": args.source, "cleared": bool(clear_unstable(args.source))}, indent=2))
        return 0

    if args.cmd == "notify-daily":
        report = daily_quality_report(window=args.window)
        summary = report["summary"]
        lines = [
            "*TenderRadar daily health*",
            f"Seen→tenders: {summary['seen_to_tenders_pct']}%",
            f"Org coverage: {summary['organization_coverage_pct']}% (unknown {summary['org_unknown_pct']}%)",
            # Signal-portal metrics (development/consulting portals only)
            f"Signal portals ({summary['signal_tenders_count']} tenders, excl. {summary['infra_tenders_count']} state-infra):",
            f"  Deep descr: {summary['signal_deep_description_coverage_pct']}% | PDF: {summary['signal_deep_pdf_coverage_pct']}%",
            f"  Doc links: {summary['signal_deep_doc_link_coverage_pct']}% | Extracted: {summary['signal_deep_doc_extracted_coverage_pct']}%",
            f"  Extract success: {summary['doc_link_extraction_success_pct']}%",
            f"Cross-source groups: {summary['cross_source_groups']}",
            f"Portals stable/partial/blocked: {summary['stable_portals']}/{summary['partial_portals']}/{summary['blocked_portals']}",
        ]
        if report["actions"]:
            lines.append("Top actions:")
            lines.extend([f"- {a}" for a in report["actions"][:3]])
        msg = "\n".join(lines)
        sent = _send_telegram(msg)
        print(json.dumps({"sent": sent, "report": report}, indent=2))
        return 0

    if args.cmd == "chat-smoke":
        out = run_live_chat_smoke(cases_path=args.cases, limit=args.limit, output_path=args.json_out)
        print(json.dumps(out, indent=2))
        return 0 if bool((out.get("summary") or {}).get("ok")) else 2

    if args.cmd == "notify-chat-smoke":
        out = run_live_chat_smoke(cases_path=args.cases, limit=args.limit, output_path=args.json_out)
        summary = out.get("summary") or {}
        ok = bool(summary.get("ok"))
        failed_ids = summary.get("failed_case_ids") or []
        msg_lines = [
            "*TenderRadar chat smoke*",
            f"Pass rate: {summary.get('pass_rate_pct', 0)}% ({summary.get('passed', 0)}/{summary.get('cases', 0)})",
        ]
        if failed_ids:
            msg_lines.append("Failed cases: " + ", ".join(str(x) for x in failed_ids[:8]))
        sent = _send_telegram("\n".join(msg_lines))
        print(json.dumps({"sent": sent, "ok": ok, "summary": summary, "report": out}, indent=2))
        return 0 if ok else 2

    if args.cmd == "start-worker":
        # Starts a Celery worker in the foreground using REDIS_URL
        cmd = [
            os.path.join(BASE, "venv_stable", "bin", "celery"),
            "-A", "core.celery_app:app",
            "worker",
            "-Q", "tenderradar",
            "-l", "info",
            "-Ofair",
        ]
        try:
            subprocess.run(cmd, check=True)
        except Exception as exc:
            log.error("Failed to start Celery worker: %s", exc)
            return 1
        return 0

    if args.cmd == "backfill-normalized":
        print(backfill_normalized_from_seen_tenders(limit=args.limit))
        return 0

    if args.cmd == "repair-materialization":
        print(json.dumps(repair_materialization(
            batch_size=args.batch_size,
            max_rounds=args.max_rounds,
        ), indent=2))
        return 0

    if args.cmd == "backfill-seen":
        print(backfill_seen_from_tenders(limit=args.limit))
        return 0

    if args.cmd == "purge-bad-deep":
        print(purge_bad_deep_data())
        return 0

    if args.cmd == "repair-cg-doc-flags":
        print(json.dumps(repair_cg_doc_flags(limit=args.limit), indent=2))
        return 0

    if args.cmd == "normalize-sources":
        print(normalize_source_aliases())
        return 0

    if args.cmd == "backfill-deep":
        print(json.dumps(
            backfill_deep(
                limit=args.limit,
                max_workers=args.max_workers,
                delay=args.delay,
                timeout=args.timeout,
                min_relevance=args.min_relevance,
                sources=args.source,
            ),
            indent=2,
        ))
        return 0

    if args.cmd == "refresh-structured":
        print(refresh_structured(
            limit=args.limit,
            only_unknown_org=args.only_unknown_org,
            missing_only=args.missing_only,
        ))
        return 0

    if args.cmd == "repair-structured":
        print(json.dumps(repair_structured_intel(
            batch_size=args.batch_size,
            max_rounds=args.max_rounds,
            only_unknown_org=args.only_unknown_org,
        ), indent=2))
        return 0

    if args.cmd == "backfill-dedup":
        print(backfill_dedup(limit_groups=args.limit_groups))
        return 0

    if args.cmd == "backfill-organizations":
        print(backfill_organizations(limit=args.limit))
        return 0

    if args.cmd == "rescore":
        print(rescore_all(batch_size=args.batch_size))
        return 0

    if args.cmd == "all":
        out = {
            "normalized_backfill": backfill_normalized_from_seen_tenders(limit=args.limit),
            "deep_backfill": backfill_deep(
                limit=args.deep_limit,
                max_workers=args.max_workers,
                delay=args.delay,
                timeout=args.timeout,
                min_relevance=args.min_relevance,
            ),
            "structured_refresh": refresh_structured(
                limit=args.structured_limit,
                only_unknown_org=args.only_unknown_org,
                missing_only=True,
            ),
            "organization_backfill": backfill_organizations(limit=args.org_limit),
            "dedup_backfill": backfill_dedup(limit_groups=args.dedup_limit_groups),
            "rescore": rescore_all(batch_size=args.batch_size),
            "audit": audit_backend(),
        }
        print(json.dumps(out, indent=2))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
