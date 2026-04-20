"""
intelligence/maturity.py — Opportunity Maturity Layer
======================================================

Classifies each tender into a business-facing maturity tier based on the
evidence available in its scraped/enriched fields.

Internal evidence states (deterministic, from deep_scope / ai_summary / doc URLs):
    SIGNAL_ONLY      — URL only; no page content, no docs
    PAGE_ONLY        — page content present, no doc attachments
    PARTIAL_PACKAGE  — some doc URLs found but package incomplete
    FULL_PACKAGE     — rich content + doc attachments present

Business-facing maturity (for analysts):
    Signal First     — SIGNAL_ONLY
    Partial Package  — PAGE_ONLY or PARTIAL_PACKAGE
    Full Package     — FULL_PACKAGE

This module is the single source of truth for:
  - evidence state classification
  - business maturity label
  - maturity summary (short sentence)
  - recommended action

Imported by:
  - exporters/excel_exporter.py      (all rows at export time)
  - exporters/evidence_packager.py   (new shortlisted tenders)
  - notifier/daily_digest.py         (digest row enrichment)

No DB writes here. Pure classification, no side effects.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Tuple

# ── Document URL pattern ───────────────────────────────────────────────────────
_DOC_URL_PAT = re.compile(
    r"https?://\S+\.(?:pdf|docx?|xlsx?|zip|rar)\b",
    re.IGNORECASE,
)
_DOC_HINT_PAT = re.compile(
    r"\b(tor|terms of reference|rfp|rfq|eoi|bid document|tender document|annex|attachment)\b",
    re.IGNORECASE,
)

# ── Thresholds ─────────────────────────────────────────────────────────────────
_MIN_DEEP_SCOPE_CHARS = 100   # minimum chars to count deep_scope as "present"
_MIN_SUMMARY_CHARS    = 50    # minimum chars to count ai_summary as "present"
_MIN_PAGE_RICH_CHARS  = 180   # substantial listing/deep page text
_MIN_DOC_TEXT_CHARS   = 200   # extracted document text strong enough for package maturity


# =============================================================================
# EVIDENCE STATE CLASSIFICATION
# =============================================================================

def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _parse_document_links(raw: Any) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, str):
                parsed = json.loads(parsed)
            if isinstance(parsed, list):
                return [item for item in parsed if isinstance(item, dict)]
        except Exception:
            return []
    return []


def _collect_evidence(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Collect and normalize evidence inputs for maturity classification.

    Authoritative source order:
      - Current-run rows: DB-enriched tender fields (`deep_scope`, `deep_description`,
        `deep_pdf_text`, `deep_document_links`, `deep_ai_summary`) when present.
      - Carry-over rows: persisted workbook columns (`Deep Scope`, `AI Summary`,
        `Evaluation Criteria`, `Cross Sources`) plus any DB-enriched fields we can
        rehydrate by tender_id / URL before export or packaging.
    """
    deep_scope = _clean_text(row.get("deep_scope") or row.get("Deep Scope"))
    deep_description = _clean_text(row.get("deep_description") or row.get("Deep Description"))
    deep_pdf_text = _clean_text(row.get("deep_pdf_text") or row.get("Deep PDF Text"))
    summary = _clean_text(
        row.get("deep_ai_summary")
        or row.get("ai_summary")
        or row.get("AI Summary")
    )
    eval_criteria = _clean_text(row.get("deep_eval_criteria") or row.get("Evaluation Criteria"))
    cross_sources = _clean_text(row.get("cross_sources") or row.get("Cross Sources"))
    description = _clean_text(row.get("description") or row.get("Description"))
    raw_doc_links = (
        row.get("deep_document_links")
        or row.get("_deep_document_links")
        or row.get("Deep Document Links")
    )
    doc_links = _parse_document_links(raw_doc_links)

    doc_urls: List[str] = []
    extracted_doc_count = 0
    extracted_chars = 0
    doc_keyword_hits = 0
    for link in doc_links:
        url = _clean_text(link.get("url"))
        label = _clean_text(link.get("label"))
        file_type = _clean_text(link.get("file_type"))
        if url:
            doc_urls.append(url)
        if bool(link.get("extracted")):
            extracted_doc_count += 1
        try:
            extracted_chars += int(link.get("char_count") or 0)
        except Exception:
            pass
        doc_keyword_hits += len(_DOC_HINT_PAT.findall(" ".join([url, label, file_type])))

    text_blob = " ".join([
        deep_scope,
        deep_description,
        deep_pdf_text,
        summary,
        eval_criteria,
        cross_sources,
        description,
    ])
    regex_doc_urls = _DOC_URL_PAT.findall(text_blob)
    doc_urls.extend(regex_doc_urls)
    deduped_doc_urls = list(dict.fromkeys(doc_urls))

    return {
        "deep_scope": deep_scope,
        "deep_description": deep_description,
        "deep_pdf_text": deep_pdf_text,
        "summary": summary,
        "eval_criteria": eval_criteria,
        "cross_sources": cross_sources,
        "description": description,
        "doc_urls": deduped_doc_urls,
        "doc_url_count": len(deduped_doc_urls),
        "extracted_doc_count": extracted_doc_count,
        "extracted_chars": extracted_chars,
        "doc_keyword_hits": doc_keyword_hits + len(_DOC_HINT_PAT.findall(text_blob)),
    }


def classify_evidence(row: Dict[str, Any]) -> Tuple[str, int]:
    """
    Classify a tender row into an evidence state.

    Args:
        row: dict with keys: deep_scope, ai_summary, cross_sources,
             evaluation_criteria, tender_url (any subset is fine)

    Returns:
        (evidence_state, doc_url_count)
        evidence_state: one of SIGNAL_ONLY / PAGE_ONLY / PARTIAL_PACKAGE / FULL_PACKAGE
        doc_url_count:  number of document URLs found in the row fields
    """
    evidence = _collect_evidence(row)
    has_deep = len(evidence["deep_scope"]) >= _MIN_DEEP_SCOPE_CHARS
    has_summary = len(evidence["summary"]) >= _MIN_SUMMARY_CHARS
    has_page_text = any(
        len(evidence[key]) >= _MIN_PAGE_RICH_CHARS
        for key in ("deep_description", "description")
    ) or len(evidence["eval_criteria"]) >= 80
    has_doc_text = (
        len(evidence["deep_pdf_text"]) >= _MIN_DOC_TEXT_CHARS
        or evidence["extracted_chars"] >= _MIN_DOC_TEXT_CHARS
        or evidence["extracted_doc_count"] > 0
    )
    doc_url_count = int(evidence["doc_url_count"])
    has_doc_signals = doc_url_count > 0 or evidence["doc_keyword_hits"] > 0
    has_rich_page = has_deep or has_summary or has_page_text or has_doc_text

    # Deterministic state machine
    if not has_rich_page and not has_doc_signals:
        return "SIGNAL_ONLY", 0

    if has_rich_page and not has_doc_signals:
        return "PAGE_ONLY", 0

    if has_doc_signals and (has_doc_text or (has_deep and (has_summary or has_page_text))):
        return "FULL_PACKAGE", doc_url_count

    if has_doc_signals:
        return "PARTIAL_PACKAGE", doc_url_count

    # Fallback (unreachable but safe)
    return "PAGE_ONLY", 0


def extract_doc_urls(row: Dict[str, Any]) -> list:
    """Return deduplicated list of document URLs found in row content fields."""
    return _collect_evidence(row)["doc_urls"]


# =============================================================================
# BUSINESS-FACING MATURITY MAPPING
# =============================================================================

# Internal state → human-readable label (simple, analyst-friendly)
_MATURITY_LABEL: Dict[str, str] = {
    "SIGNAL_ONLY":      "Signal First",
    "PAGE_ONLY":        "Partial Package",
    "PARTIAL_PACKAGE":  "Partial Package",
    "FULL_PACKAGE":     "Full Package",
}

# Short summary sentence shown in Excel and digest
_MATURITY_SUMMARY: Dict[str, str] = {
    "SIGNAL_ONLY":      "Listing only — no TOR or page content yet",
    "PAGE_ONLY":        "Rich page content available, no document attachments",
    "PARTIAL_PACKAGE":  "Some documents present, package incomplete",
    "FULL_PACKAGE":     "Full content and documents — bid-ready package",
}

# Recommended action for each state
_RECOMMENDED_ACTION: Dict[str, str] = {
    "SIGNAL_ONLY":      "Monitor",
    "PAGE_ONLY":        "Review",
    "PARTIAL_PACKAGE":  "Review",
    "FULL_PACKAGE":     "Prepare Bid",
}

# Digest-friendly note (used in email body per opportunity)
_MATURITY_DIGEST_NOTE: Dict[str, str] = {
    "SIGNAL_ONLY":      "Monitor for TOR publication",
    "PAGE_ONLY":        "Partial content — TOR/docs not yet attached",
    "PARTIAL_PACKAGE":  "Partial documents available — review soon",
    "FULL_PACKAGE":     "Bid-ready package — begin preparation",
}


def maturity_label(evidence_state: str) -> str:
    """Return business-facing maturity label for an evidence state."""
    return _MATURITY_LABEL.get(evidence_state, "Unknown")


def maturity_summary(evidence_state: str) -> str:
    """Return short maturity summary sentence."""
    return _MATURITY_SUMMARY.get(evidence_state, "Evidence state unknown")


def recommended_action(evidence_state: str) -> str:
    """Return recommended action string."""
    return _RECOMMENDED_ACTION.get(evidence_state, "Review")


def maturity_digest_note(evidence_state: str) -> str:
    """Return short digest-appropriate maturity note."""
    return _MATURITY_DIGEST_NOTE.get(evidence_state, "Review")


# =============================================================================
# CONVENIENCE: classify + return all business fields in one call
# =============================================================================

def classify_row(row: Dict[str, Any]) -> Dict[str, str]:
    """
    Classify a row and return all business-facing maturity fields.

    Returns dict with:
        evidence_state      — SIGNAL_ONLY / PAGE_ONLY / PARTIAL_PACKAGE / FULL_PACKAGE
        opportunity_maturity — Signal First / Partial Package / Full Package
        maturity_summary    — one-line description
        recommended_action  — Monitor / Review / Prepare Bid
        doc_url_count       — str (number of doc URLs found)
    """
    evidence_state, doc_url_count = classify_evidence(row)
    return {
        "evidence_state":       evidence_state,
        "opportunity_maturity": maturity_label(evidence_state),
        "maturity_summary":     maturity_summary(evidence_state),
        "recommended_action":   recommended_action(evidence_state),
        "doc_url_count":        str(doc_url_count),
    }
