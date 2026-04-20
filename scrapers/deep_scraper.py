# =============================================================================
# scrapers/deep_scraper.py — Tier 1 Deep Page Enrichment
#
# Purpose:
#   After a scraper captures basic tender info (title, URL, deadline, org),
#   this module follows each tender URL to extract EVERYTHING available:
#     - Full description / scope of work
#     - Evaluation criteria and team requirements
#     - Budget / contract value
#     - Submission instructions and contact info
#     - Attached PDFs (ToR, RFP, EOI documents) — full text extraction
#
# Design:
#   - Async-friendly: runs in a ThreadPoolExecutor so it never blocks scrapers
#   - Fail-open: if a page fetch fails, original data is unchanged
#   - Dedup-aware: skips re-enrichment if content_hash unchanged since last run
#   - Respects rate limits: configurable delay and timeout per portal
#   - PDF extraction via pdfplumber (falls back gracefully if not installed)
#
# Usage:
#   from scrapers.deep_scraper import enrich_tender_deep, enrich_batch_deep
#
#   # Single tender (blocking)
#   enriched = enrich_tender_deep(tender)
#
#   # Batch (threaded, up to max_workers parallel)
#   enriched_list = enrich_batch_deep(tenders, max_workers=5)
# =============================================================================

from __future__ import annotations

import hashlib
import io
import logging
import re
import time
import zipfile
from html import unescape as html_unescape
from concurrent.futures import ThreadPoolExecutor, as_completed
from xml.etree import ElementTree as ET
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("tenderradar.deep_scraper")

# ── Per-request configuration ────────────────────────────────────────────────
_DEFAULT_TIMEOUT    = 20          # seconds per HTTP request
_DEFAULT_DELAY      = 1.5         # seconds between requests to same domain
_MAX_PDF_BYTES      = 10_000_000  # 10MB PDF cap — larger files skipped
_MAX_DESCRIPTION    = 15_000      # max chars to store from deep scrape
_MAX_DOC_LINKS      = 15          # max document links stored per tender
_MAX_DOCS_TO_EXTRACT = 8          # max documents to fetch/read per tender
_MAX_DOC_TEXT_PER_FILE = 6_000    # cap per document contribution
_USER_AGENT         = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Domains known to block bots — skip deep scrape silently
_BLOCKED_DOMAINS = {
    "sam.gov",
    "workwithusaid.org",
    # eu-supply.com: login wall, returns 202 + empty body without session cookie
    "eu.eu-supply.com",
}

_WB_PROC_API = (
    "https://search.worldbank.org/api/procnotices"
    "?format=xml&apilang=en"
    "&fl=project_name,id,notice_type,submission_deadline_date,bid_description,"
    "bid_reference_no,project_ctry_name,contact_organization,procurement_method_name,"
    "borrower_name,notice_text&id={notice_id}"
)

# =============================================================================
# Extraction pattern library — upgraded Tier 1 field extraction
#
# Philosophy:
#   Every pattern uses a NAMED group so callers always know which field
#   matched.  Patterns are listed most-specific → least-specific so the
#   first match wins the best text.  All patterns are compiled once at
#   module load (fast).
# =============================================================================

# ── Scope / description ───────────────────────────────────────────────────────
_SCOPE_PATTERNS = [
    re.compile(r"(?:scope\s+of\s+(?:work|services|assignment))\s*[:\-]\s*(.{200,4000}?)(?=\n\s*\n|\Z)", re.I | re.S),
    re.compile(r"(?:terms\s+of\s+reference|tor)\s*[:\-]\s*(.{200,4000}?)(?=\n\s*\n|\Z)",               re.I | re.S),
    re.compile(r"(?:objective[s]?\s+of\s+(?:the\s+)?(?:assignment|consultancy))\s*[:\-]\s*(.{100,3000}?)(?=\n\s*\n|\Z)", re.I | re.S),
    re.compile(r"(?:background)\s*[:\-]\s*(.{100,2000}?)(?=\n\s*\n|\Z)",                               re.I | re.S),
]

# ── Budget / contract value ────────────────────────────────────────────────────
_BUDGET_PATTERNS = [
    # "Estimated contract value: USD 2,500,000" / "INR 45 lakh"
    re.compile(
        r"(?:estimated\s+(?:contract\s+)?value|contract\s+(?:amount|value)|total\s+(?:budget|value)|budget\s+(?:ceiling|estimate))"
        r"\s*[:\-]?\s*"
        r"(?P<currency>USD|INR|EUR|GBP|EUR|CHF|JPY|CAD|AUD)?\s*"
        r"(?P<amount>[\d,\.]+(?:\s*(?:million|billion|lakh|crore|thousand))?)",
        re.I
    ),
    # Standalone "USD 2.5 million" / "€ 500,000"
    re.compile(
        r"(?P<currency>USD|INR|EUR|GBP|CHF|JPY|CAD|AUD|€|\$|£|¥)\s*"
        r"(?P<amount>[\d,\.]+(?:\s*(?:million|billion|lakh|crore|thousand))?)",
        re.I
    ),
]

# ── Dates (submission deadline + other key dates) ─────────────────────────────
_DATE_STR = (
    r"(?:"
    r"\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}"     # 12 March 2026
    r"|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}"                              # Mar 12, 2026
    r"|\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}"                                                                            # 12/03/2026
    r"|\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}"                                                                              # 2026-03-12
    r")"
)
_DEADLINE_PATTERNS = [
    re.compile(r"(?:submission\s+deadline|closing\s+date|last\s+date\s+(?:of\s+)?submission|proposal\s+due\s+date|bid\s+closing)\s*[:\-]\s*(" + _DATE_STR + r")", re.I),
    re.compile(r"(?:due\s+date|deadline)\s*[:\-]\s*(" + _DATE_STR + r")", re.I),
    re.compile(r"(?:submit(?:ted)?\s+by|receipt\s+of\s+(?:proposals|bids|applications))\s*[:\-]?\s*(?:no\s+later\s+than\s+)?\s*(" + _DATE_STR + r")", re.I),
]
# Other key dates beyond the submission deadline
_KEY_DATE_PATTERNS = {
    "date_pre_bid":       re.compile(r"(?:pre[-\s]?bid\s+(?:conference|meeting)|mandatory\s+site\s+visit)\s*[:\-]\s*(" + _DATE_STR + r")", re.I),
    "date_qa_deadline":   re.compile(r"(?:last\s+date\s+(?:for\s+)?(?:queries|questions|clarifications)|q\s*[&/]\s*a\s+deadline)\s*[:\-]\s*(" + _DATE_STR + r")", re.I),
    "date_contract_start":re.compile(r"(?:contract\s+(?:commencement|start\s+date)|expected\s+start)\s*[:\-]\s*(" + _DATE_STR + r")", re.I),
}

# ── Contract duration ─────────────────────────────────────────────────────────
_DURATION_PATTERNS = [
    re.compile(r"(?:duration\s+of\s+(?:the\s+)?(?:contract|assignment|consultancy|project)|contract\s+period|assignment\s+duration)\s*[:\-]\s*(.{5,80}?)(?=\n|\.|\Z)", re.I),
    re.compile(r"(\d+)\s*(?:calendar\s+)?(?:months?|years?|weeks?)\s*(?:from|starting|commencing)", re.I),
    re.compile(r"(?:over\s+(?:a\s+period\s+of\s+)?|for\s+(?:a\s+period\s+of\s+)?)\s*(\d+\s*(?:months?|years?|weeks?))", re.I),
]

# ── Evaluation weights ────────────────────────────────────────────────────────
_EVAL_WEIGHT_PATTERNS = [
    # "Technical: 70%, Financial: 30%"  OR  "70/30" OR "80:20"
    re.compile(
        r"(?:technical\s+(?:score|proposal|evaluation|criteria)[:\s/–\-]*(?P<tech>\d+)\s*%?"
        r"\s*[,;/\s]*financial\s*[:\s/–\-]*(?P<fin>\d+)\s*%?)",
        re.I
    ),
    re.compile(r"(?P<tech>\d+)\s*[:/]\s*(?P<fin>\d+)\s*(?:technical[:\s/–]*financial|t[:\s/–]*f)", re.I),
    re.compile(
        r"(?:evaluation\s+(?:criteria|method|methodology|scheme))[:\-\s]*(.{50,600}?)(?=\n\s*\n|\Z)",
        re.I | re.S
    ),
]

# ── Eligibility / minimum qualifications ─────────────────────────────────────
_ELIGIBILITY_PATTERNS = [
    re.compile(
        r"(?:eligibility\s+(?:criteria|requirements?)|minimum\s+qualifications?|qualification\s+(?:criteria|requirements?))\s*[:\-]\s*(.{100,3000}?)(?=\n\s*\n|\Z)",
        re.I | re.S
    ),
    re.compile(
        r"(?:to\s+be\s+eligible|firms?\s+(?:must|should)\s+have|applicants?\s+(?:must|should)\s+(?:have|demonstrate))\s*(.{50,2000}?)(?=\n\s*\n|\Z)",
        re.I | re.S
    ),
    # Annual turnover requirement
    re.compile(
        r"(?:annual\s+(?:average\s+)?turnover|minimum\s+(?:turnover|revenue|financial\s+capacity))\s*[:\-]?\s*"
        r"(?P<currency>USD|INR|EUR|GBP)?\s*(?P<amount>[\d,\.]+(?:\s*(?:million|billion|lakh|crore|thousand))?)",
        re.I
    ),
    # Years of experience
    re.compile(
        r"(?:at\s+least|minimum(?:\s+of)?)\s*(?P<years>\d+)\s*years?\s*(?:of\s+)?(?:relevant\s+)?(?:experience|expertise)",
        re.I
    ),
    # Number of similar projects
    re.compile(
        r"(?:at\s+least|minimum(?:\s+of)?)\s*(?P<n>\d+)\s*(?:similar|comparable|relevant)\s*(?:projects?|assignments?|contracts?)",
        re.I
    ),
]

# ── Team composition / key experts ────────────────────────────────────────────
_TEAM_PATTERNS = [
    re.compile(
        r"(?:team\s+composition|key\s+(?:experts?|personnel|professional)|required\s+(?:staff|experts?)|expert\s+(?:profile|requirements?))\s*[:\-]\s*(.{100,3000}?)(?=\n\s*\n|\Z)",
        re.I | re.S
    ),
    re.compile(
        r"(?:team\s+leader|project\s+manager|lead\s+(?:consultant|expert))\s*[:\-]\s*(.{50,1000}?)(?=\n\s*\n|\Z)",
        re.I | re.S
    ),
    re.compile(
        r"(?:qualifications?\s+(?:required|needed)\s+(?:for\s+(?:the\s+)?)?(?:key\s+)?(?:experts?|staff))\s*[:\-]\s*(.{100,2000}?)(?=\n\s*\n|\Z)",
        re.I | re.S
    ),
]

# ── Contact info ──────────────────────────────────────────────────────────────
_EMAIL_RE     = re.compile(r"[\w\.\-+]+@[\w\.\-]+\.[a-zA-Z]{2,}", re.I)
_CONTACT_BLOCK_RE = re.compile(r"(?:contact|enquiries|queries?|questions?)\s*[:\-]\s*(.{20,300}?)(?=\n\s*\n|\Z)", re.I | re.S)


# =============================================================================
# Internal helpers
# =============================================================================

def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lstrip("www.")
    except Exception:
        return ""


def _is_blocked(url: str) -> bool:
    d = _domain(url)
    return any(blocked in d for blocked in _BLOCKED_DOMAINS)


def _normalise_doc_links(raw) -> list:
    """Return a plain Python list of dicts suitable for json.dumps.

    Guards against three failure modes that cause double-encoding in the DB:
      1. raw is None / falsy            → return []
      2. raw is a JSON string           → decode once (or twice if double-encoded)
      3. raw is already a list          → return as-is
    """
    if not raw:
        return []
    if isinstance(raw, list):
        return [d for d in raw if isinstance(d, dict)]
    if isinstance(raw, str):
        try:
            parsed = _json.loads(raw)
            # Handle double-encoded case: json.loads("\"[]\"") → "[]"
            if isinstance(parsed, str):
                try:
                    parsed = _json.loads(parsed)
                except Exception:
                    return []
            if isinstance(parsed, list):
                return [d for d in parsed if isinstance(d, dict)]
        except Exception:
            pass
    return []


def _clean_html_text(html: str) -> str:
    """
    Clean HTML into readable plain text.
    Keeps this lightweight so deep scraping never fails on a missing helper.
    """
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(["script", "style", "noscript", "header", "footer", "nav"]):
            tag.decompose()
        text = soup.get_text(" ", strip=True)
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html)
    text = html_unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _fetch_page(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[str]:
    """Fetch a URL and return its HTML text, or None on failure."""
    try:
        headers = {"User-Agent": _USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        # Only process HTML or plain text — skip binary/JSON responses
        ct = resp.headers.get("content-type", "")
        if "html" not in ct and "text" not in ct:
            return None
        return resp.text
    except Exception as e:
        logger.debug("[deep_scraper] fetch failed for %s: %s", url[:80], e)
        return None


def _fetch_pdf(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[bytes]:
    """Download a PDF, returning bytes or None. Enforces size cap."""
    try:
        headers = {"User-Agent": _USER_AGENT}
        resp = requests.get(url, headers=headers, timeout=timeout,
                            stream=True, allow_redirects=True)
        resp.raise_for_status()
        ct = resp.headers.get("content-type", "")
        ct_lower = ct.lower()
        if "html" in ct_lower or "text/" in ct_lower:
            return None
        if "pdf" not in ct_lower and "octet-stream" not in ct_lower and not url.lower().endswith(".pdf"):
            return None
        # Stream with cap
        chunks = []
        total = 0
        for chunk in resp.iter_content(chunk_size=65536):
            chunks.append(chunk)
            total += len(chunk)
            if total > _MAX_PDF_BYTES:
                logger.debug("[deep_scraper] PDF too large (>%sMB): %s", _MAX_PDF_BYTES // 1_000_000, url[:80])
                return None
        return b"".join(chunks)
    except Exception as e:
        logger.debug("[deep_scraper] PDF fetch failed for %s: %s", url[:80], e)
        return None


def _extract_text_from_html(html: str) -> str:
    """Extract clean readable text from HTML, removing scripts/styles."""
    try:
        soup = BeautifulSoup(html, "html.parser")
        # Remove noise tags
        for tag in soup(["script", "style", "nav", "header", "footer",
                          "aside", "iframe", "noscript", "meta"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        # Collapse excessive whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text.strip()
    except Exception:
        return ""


def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber. Falls back gracefully."""
    try:
        import pdfplumber
        text_parts = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages[:30]:   # cap at 30 pages
                t = page.extract_text()
                if t:
                    text_parts.append(t)
        return "\n\n".join(text_parts).strip()
    except ImportError:
        logger.debug("[deep_scraper] pdfplumber not installed — PDF text extraction disabled")
        return ""
    except Exception as e:
        logger.debug("[deep_scraper] PDF text extraction failed: %s", e)
        return ""


def _extract_text_from_docx(docx_bytes: bytes) -> str:
    """
    Lightweight DOCX extractor without external dependencies.
    Reads word/document.xml from the zip container and strips tags.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(docx_bytes)) as zf:
            with zf.open("word/document.xml") as fh:
                xml_text = fh.read().decode("utf-8", errors="ignore")
        # Replace paragraph/table boundaries with newlines, strip remaining tags.
        xml_text = re.sub(r"</w:p>|</w:tr>|</w:tbl>", "\n", xml_text)
        xml_text = re.sub(r"<[^>]+>", " ", xml_text)
        xml_text = html_unescape(xml_text)
        xml_text = re.sub(r"\s+", " ", xml_text).strip()
        return xml_text
    except Exception:
        return ""


def _extract_text_from_xlsx(xlsx_bytes: bytes) -> str:
    """
    Best-effort XLSX/XLSM text extraction without optional heavy deps.
    Reads shared strings + worksheet XML text nodes from the ZIP container.
    """
    try:
        chunks = []
        with zipfile.ZipFile(io.BytesIO(xlsx_bytes)) as zf:
            for name in zf.namelist():
                low = name.lower()
                if low == "xl/sharedstrings.xml" or low.startswith("xl/worksheets/"):
                    try:
                        raw = zf.read(name).decode("utf-8", errors="ignore")
                    except Exception:
                        continue
                    text = re.sub(r"<[^>]+>", " ", raw)
                    text = html_unescape(text)
                    text = re.sub(r"\s+", " ", text).strip()
                    if text:
                        chunks.append(text)
                if len(chunks) >= 8:
                    break
        return " ".join(chunks).strip()
    except Exception:
        return ""


def _extract_text_from_pptx(pptx_bytes: bytes) -> str:
    """
    Best-effort PPTX text extraction from slide XML content.
    """
    try:
        chunks = []
        with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
            for name in zf.namelist():
                low = name.lower()
                if not low.startswith("ppt/slides/slide") or not low.endswith(".xml"):
                    continue
                try:
                    raw = zf.read(name).decode("utf-8", errors="ignore")
                except Exception:
                    continue
                text = re.sub(r"<[^>]+>", " ", raw)
                text = html_unescape(text)
                text = re.sub(r"\s+", " ", text).strip()
                if text:
                    chunks.append(text)
                if len(chunks) >= 6:
                    break
        return " ".join(chunks).strip()
    except Exception:
        return ""


def _extract_text_from_zip(zip_bytes: bytes) -> str:
    """
    Extract text from a ZIP attachment by scanning member files.
    Attempts PDF/DOCX/TXT/CSV/XML/HTML files and concatenates snippets.
    """
    chunks = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                name = str(info.filename or "").strip()
                if not name:
                    continue
                lower = name.lower()
                if not lower.endswith((".pdf", ".docx", ".txt", ".csv", ".xml", ".html", ".htm", ".xlsx", ".xlsm", ".pptx")):
                    continue
                try:
                    raw = zf.read(info)
                except Exception:
                    continue
                extracted = ""
                if lower.endswith(".pdf"):
                    extracted = _extract_text_from_pdf(raw)
                elif lower.endswith(".docx"):
                    extracted = _extract_text_from_docx(raw)
                elif lower.endswith((".xlsx", ".xlsm")):
                    extracted = _extract_text_from_xlsx(raw)
                elif lower.endswith(".pptx"):
                    extracted = _extract_text_from_pptx(raw)
                else:
                    extracted = _extract_text_from_bytes(raw, url=name)
                if extracted:
                    label = name.split("/")[-1][:120]
                    chunks.append(f"[{label}] {extracted[:_MAX_DOC_TEXT_PER_FILE]}")
                if len(chunks) >= 6:
                    break
    except Exception:
        return ""
    return "\n\n---\n\n".join(chunks).strip()


def _extract_text_from_bytes(raw: bytes, content_type: str = "", url: str = "") -> str:
    """
    Best-effort text extraction for non-PDF attachments.
    Supports plain text/CSV/HTML and DOCX.
    """
    ct = (content_type or "").lower()
    url_l = (url or "").lower()

    # PDF magic-bytes fallback for extensionless/proxy links.
    if raw[:5] == b"%PDF-":
        pdf_text = _extract_text_from_pdf(raw)
        if pdf_text:
            return pdf_text

    # ZIP archives (common for bid packs)
    if (
        url_l.endswith((".zip", ".rar", ".7z"))
        or "zip" in ct
    ):
        ztxt = _extract_text_from_zip(raw)
        if ztxt:
            return ztxt

    # DOCX by extension/content type
    if (
        ".docx" in url_l
        or "wordprocessingml" in ct
        or "application/vnd.openxmlformats-officedocument.wordprocessingml.document" in ct
    ):
        docx_text = _extract_text_from_docx(raw)
        if docx_text:
            return docx_text

    # XLSX/XLSM by extension/content type
    if (
        url_l.endswith((".xlsx", ".xlsm"))
        or "spreadsheetml" in ct
        or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in ct
    ):
        xlsx_text = _extract_text_from_xlsx(raw)
        if xlsx_text:
            return xlsx_text

    # PPTX by extension/content type
    if (
        url_l.endswith(".pptx")
        or "presentationml" in ct
        or "application/vnd.openxmlformats-officedocument.presentationml.presentation" in ct
    ):
        pptx_text = _extract_text_from_pptx(raw)
        if pptx_text:
            return pptx_text

    # HTML / text-like
    if (
        "text/" in ct
        or "html" in ct
        or url_l.endswith((".txt", ".csv", ".xml", ".html", ".htm"))
    ):
        decoded = raw.decode("utf-8", errors="ignore")
        if "<html" in decoded.lower() or "<body" in decoded.lower():
            return _clean_html_text(decoded)
        return re.sub(r"\s+", " ", decoded).strip()

    # Last resort for office/unknown binaries: decode printable fragments.
    fallback = raw.decode("utf-8", errors="ignore")
    fallback = re.sub(r"\s+", " ", fallback).strip()
    return fallback if len(fallback) >= 80 else ""


def _fetch_and_extract_document(url: str, file_type: str, timeout: int = _DEFAULT_TIMEOUT) -> str:
    """
    Download and extract readable text from a document URL.
    Handles PDFs plus common text/doc formats.
    """
    try:
        headers = {"User-Agent": _USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
        resp = requests.get(url, headers=headers, timeout=timeout, stream=True, allow_redirects=True)
        resp.raise_for_status()

        chunks = []
        total = 0
        for chunk in resp.iter_content(chunk_size=65536):
            chunks.append(chunk)
            total += len(chunk)
            if total > _MAX_PDF_BYTES:
                return ""
        raw = b"".join(chunks)
        if not raw:
            return ""

        # Do not trust upstream file_type hints blindly; some portals label
        # HTML forwarding pages as "pdf". Detect true PDFs by URL suffix,
        # content-type, or magic bytes.
        if url.lower().endswith(".pdf") or raw[:5] == b"%PDF-":
            return _extract_text_from_pdf(raw)

        ct = resp.headers.get("content-type", "")
        if "pdf" in (ct or "").lower():
            pdf_text = _extract_text_from_pdf(raw)
            if pdf_text:
                return pdf_text
        return _extract_text_from_bytes(raw, content_type=ct, url=url)
    except Exception:
        return ""


def _fetch_and_extract_document_with_context(
    url: str,
    file_type: str,
    timeout: int = _DEFAULT_TIMEOUT,
    headers: Optional[dict] = None,
    cookies: Optional[dict] = None,
) -> str:
    """
    Session-aware variant of document extraction for portals where attachment
    downloads require referer/cookies from the detail page request.
    """
    try:
        req_headers = {"User-Agent": _USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
        if isinstance(headers, dict):
            for k, v in headers.items():
                ks = str(k or "").strip()
                vs = str(v or "").strip()
                if ks and vs:
                    req_headers[ks] = vs

        resp = requests.get(
            url,
            headers=req_headers,
            cookies=(cookies or None),
            timeout=timeout,
            stream=True,
            allow_redirects=True,
        )
        resp.raise_for_status()

        chunks = []
        total = 0
        for chunk in resp.iter_content(chunk_size=65536):
            chunks.append(chunk)
            total += len(chunk)
            if total > _MAX_PDF_BYTES:
                return ""
        raw = b"".join(chunks)
        if not raw:
            return ""

        # Do not force PDF parsing from file_type labels alone; forwarding
        # endpoints can be HTML while marked as PDF in anchor labels.
        if url.lower().endswith(".pdf") or raw[:5] == b"%PDF-":
            return _extract_text_from_pdf(raw)

        ct = resp.headers.get("content-type", "")
        if "pdf" in (ct or "").lower():
            pdf_text = _extract_text_from_pdf(raw)
            if pdf_text:
                return pdf_text
        return _extract_text_from_bytes(raw, content_type=ct, url=url)
    except Exception:
        return ""


def _find_document_links(html: str, base_url: str) -> list[dict]:
    """
    Find all document attachment links on a tender page.

    Returns a list of dicts: [{url, label, file_type}, ...]
    file_type is: "pdf" | "word" | "excel" | "zip" | "other"
    Capped at _MAX_DOC_LINKS documents.
    """
    _DOC_EXTS = {
        ".pdf": "pdf", ".doc": "word", ".docx": "word",
        ".xls": "excel", ".xlsx": "excel", ".zip": "zip",
        ".ppt": "other", ".pptx": "other", ".odt": "other",
        ".rtf": "other",
    }
    _DOC_KEYWORDS = {"pdf", "download", "document", "attachment", "tor", "rfp",
                     "eoi", "bid", "tender", "notice", "form", ".doc", ".xls"}

    # Stricter keyword set for path-based checks only — excludes generic words
    # that appear in non-document page paths (e.g. "tender" in "etendering-guidance")
    _DOC_PATH_KEYWORDS = {"pdf", "download", "attachment", "tor", "rfp", "eoi",
                          "dlink", "docdownload", "doc_download", "file_download",
                          "getdoc", "getfile", "download_doc", "tender_doc",
                          "tenderdoc", "procure_doc", ".doc", ".xls"}

    def _classify(url_str: str, label_str: str = "") -> str:
        url_lower = url_str.lower()
        for ext, ft in _DOC_EXTS.items():
            if url_lower.endswith(ext) or f"{ext}?" in url_lower:
                return ft
        if "pdf" in url_lower:
            return "pdf"
        lbl = (label_str or "").lower()
        if any(k in lbl for k in ("pdf", "tor", "terms of reference", "rfp")):
            return "pdf"
        if any(k in lbl for k in ("doc", "word")):
            return "word"
        if any(k in lbl for k in ("xls", "xlsx", "excel", "boq")):
            return "excel"
        if any(k in lbl for k in ("zip", "rar", "7z", "archive")):
            return "zip"
        return "other"

    def _extract_anchor_url(a_tag) -> str:
        """
        Extract downloadable URL from anchors/buttons, including data-* and onclick.
        """
        candidates = [
            a_tag.get("href"),
            a_tag.get("data-href"),
            a_tag.get("data-url"),
            a_tag.get("data-download"),
            a_tag.get("data-file"),
            a_tag.get("data-link"),
        ]
        onclick = str(a_tag.get("onclick") or "")
        if onclick:
            m = re.search(r"""['"]((?:https?:)?//[^'"]+|/[^'"]+\.(?:pdf|docx?|xlsx?|zip|rar)[^'"]*)['"]""", onclick, re.I)
            if m:
                candidates.append(m.group(1))
        for c in candidates:
            c = str(c or "").strip()
            if not c or c.startswith("#") or c.lower().startswith("javascript"):
                continue
            return c
        return ""

    try:
        soup = BeautifulSoup(html, "html.parser")
        seen  = set()
        docs  = []

        # World Bank documentdetail custom elements
        for node in soup.find_all("documentdetail"):
            download_api = str(node.get("download-api") or "").strip()
            document_id  = str(node.get("document-id") or "").strip()
            if download_api and document_id:
                full = urljoin(base_url, f"{download_api}{document_id}")
                if full not in seen:
                    seen.add(full)
                    label = str(node.get("title") or node.get("document-type") or "Document").strip()
                    docs.append({"url": full, "label": label[:120], "file_type": "pdf"})

        # Generic anchor tags
        for a in soup.find_all(["a", "button"]):
            href = _extract_anchor_url(a)
            if not href:
                continue
            full = urljoin(base_url, href)
            if full in seen:
                continue

            url_lower = full.lower()

            # Skip navigation / homepage URLs — short root paths are never documents
            _parsed_path = full.split("?")[0].rstrip("/").rsplit("/", 1)[-1].lower()
            _is_nav = (
                not _parsed_path                                   # bare domain
                or _parsed_path in ("index.cfm", "index.php", "index.html", "index.htm",
                                    "home", "default.aspx", "default.asp")
                or re.match(r"^(home|index|default|main)\b", _parsed_path)
            )
            if _is_nav:
                continue

            # For keyword matching use PATH only (not full URL) to avoid matching
            # domain names that contain doc keywords (e.g. "procurement-notices.undp.org")
            _url_path = full.split("?")[0].lower()
            try:
                from urllib.parse import urlparse as _urlparse
                _pp = _urlparse(full)
                _url_path = (_pp.path + ("?" + _pp.query if _pp.query else "")).lower()
            except Exception:
                pass

            # Must look like a document link
            is_doc = (
                any(url_lower.endswith(ext) for ext in _DOC_EXTS) or
                "pdf" in url_lower or
                # Use stricter path-only keywords to avoid domain-name false positives
                any(kw in _url_path for kw in _DOC_PATH_KEYWORDS) or
                any(kw in (a.get("class") or []) for kw in ("download", "attachment", "document")) or
                (a.get("type") or "").startswith("application/")
            )
            if not is_doc:
                continue

            label = a.get_text(strip=True) or a.get("title") or a.get("aria-label") or "Document"
            ft = _classify(full, label)
            label = label[:120].strip()
            if not label or label.lower() in ("click here", "here", "download", "link"):
                label = "Document"

            seen.add(full)
            docs.append({"url": full, "label": label, "file_type": ft})

            if len(docs) >= _MAX_DOC_LINKS:
                break

        return docs
    except Exception:
        return []


def _find_pdf_links(html: str, base_url: str) -> list[str]:
    """Legacy shim — returns plain URL list for backward-compat PDF extraction."""
    return [d["url"] for d in _find_document_links(html, base_url) if d["file_type"] == "pdf"]


def _find_document_links_in_text(text: str) -> list[dict]:
    """
    Extract document URLs embedded in plain text fields (listing descriptions).
    Useful for portals where detail HTML links are session-bound but the scraper
    already persisted stable attachment URLs into the description.
    """
    if not text:
        return []
    out = []
    seen = set()
    keyword_markers = (
        "download",
        "tenderdoc",
        "tenderdocument",
        "documentdownload",
        "attachment",
        "bid-doc",
    )
    for m in re.finditer(r"https?://[^\s<>\"]+", str(text), re.I):
        raw = str(m.group(0) or "").strip(".,;:()[]{}<>\"'")
        low = raw.lower()
        file_type = ""
        if re.search(r"\.pdf(?:$|[?#])", low):
            file_type = "pdf"
        elif re.search(r"\.docx?(?:$|[?#])", low):
            file_type = "word"
        elif re.search(r"\.xlsx?(?:$|[?#])", low):
            file_type = "excel"
        elif re.search(r"\.(zip|rar|7z)(?:$|[?#])", low):
            file_type = "zip"
        elif re.search(r"\.(csv|txt|xml|json)(?:$|[?#])", low):
            file_type = "other"
        elif any(marker in low for marker in keyword_markers):
            file_type = "other"
        if not file_type:
            continue
        if raw in seen:
            continue
        seen.add(raw)
        out.append({"url": raw, "label": "Document", "file_type": file_type})
        if len(out) >= _MAX_DOC_LINKS:
            break
    return out


def _extract_document_name_tokens_from_text(text: str, fallback_url: str) -> list[dict]:
    """
    Extract document file names from free text and build fallback link objects.
    Used when portals expose file names but not direct downloadable URLs.
    """
    if not text:
        return []
    out = []
    seen = set()
    for m in re.finditer(r"\b([A-Za-z0-9][A-Za-z0-9._()\- /]{1,150}\.(?:pdf|docx?|xlsx?|xlsm|zip|rar|csv))\b", str(text), re.I):
        label = re.sub(r"\s+", " ", str(m.group(1) or "").strip())
        if not label:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        low = label.lower()
        if low.endswith(".pdf"):
            ft = "pdf"
        elif low.endswith((".doc", ".docx")):
            ft = "word"
        elif low.endswith((".xls", ".xlsx", ".xlsm")):
            ft = "excel"
        elif low.endswith((".zip", ".rar")):
            ft = "zip"
        else:
            ft = "other"
        out.append({
            "url": fallback_url,
            "label": label[:120],
            "file_type": ft,
            "extracted": False,
            "char_count": 0,
            "link_kind": "portal_detail",
        })
        if len(out) >= _MAX_DOC_LINKS:
            break
    return out


def _discover_document_links_from_detail_page(url: str, timeout: int = _DEFAULT_TIMEOUT) -> list[dict]:
    """
    Fetch the tender detail page and extract likely attachment links directly.
    This is a fallback for listing-heavy portals where descriptions may not
    contain absolute document URLs.
    """
    try:
        if not url or _is_blocked(url):
            return []
        html = _fetch_page(url, timeout=timeout)
        if not html:
            return []
        return _find_document_links(html, url)
    except Exception:
        return []


def _merge_document_links(*link_sets: list[dict]) -> list[dict]:
    """
    Merge document link lists while preserving first-seen order and URL uniqueness.
    """
    merged = []
    seen = set()
    for links in link_sets:
        if not isinstance(links, list):
            continue
        for item in links:
            if not isinstance(item, dict):
                continue
            u = str(item.get("url") or "").strip()
            if not u:
                continue
            key = u.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
            if len(merged) >= _MAX_DOC_LINKS:
                return merged
    return merged


def _collect_listing_and_detail_doc_links(description_text: str, detail_url: str, timeout: int) -> list[dict]:
    """
    Discover document links using multiple grounded sources:
    1) direct URLs in listing text
    2) attachment links from detail page HTML
    3) filename tokens in listing text (fallback to detail URL)
    """
    text = str(description_text or "")
    url = str(detail_url or "").strip()
    links_from_text = _find_document_links_in_text(text)
    links_from_page = _discover_document_links_from_detail_page(url, timeout=min(timeout, 20)) if url else []
    links_from_tokens = _extract_document_name_tokens_from_text(text, url) if text else []
    return _merge_document_links(links_from_text, links_from_page, links_from_tokens)


def _extract_document_chunks(
    doc_links: list[dict],
    timeout: int,
    request_headers: Optional[dict] = None,
    request_cookies: Optional[dict] = None,
    fallback_text: str = "",
) -> tuple[list[dict], str]:
    """
    Download and extract text from discovered document links.
    Returns (annotated_links, combined_text_chunks).
    """
    if not isinstance(doc_links, list) or not doc_links:
        return [], ""
    chunks = []
    extracted_any = False
    for doc in doc_links[:_MAX_DOCS_TO_EXTRACT]:
        if not isinstance(doc, dict):
            continue
        doc_url = str(doc.get("url") or "").strip()
        doc_type = str(doc.get("file_type") or "other").strip().lower()
        if not doc_url or _is_blocked(doc_url):
            doc["extracted"] = False
            doc["char_count"] = 0
            continue
        if request_headers or request_cookies:
            extracted = _fetch_and_extract_document_with_context(
                doc_url,
                doc_type,
                timeout=timeout,
                headers=request_headers,
                cookies=request_cookies,
            )
        else:
            extracted = _fetch_and_extract_document(doc_url, doc_type, timeout=timeout)
        doc["extracted"] = bool(extracted)
        doc["char_count"] = len(extracted) if extracted else 0
        if extracted:
            extracted_any = True
            label = str(doc.get("label") or "Document").strip()[:120]
            chunks.append(f"[{label}] {extracted[:_MAX_DOC_TEXT_PER_FILE]}")
        time.sleep(0.2)

    # Portal fallback: some links are session-protected and cannot be fetched directly.
    # If we discovered document links but extracted nothing, preserve grounded value
    # by attaching a clipped fallback from the verified detail-page text.
    fb = str(fallback_text or "").strip()
    if (not extracted_any) and fb and doc_links:
        snippet = fb[:_MAX_DOC_TEXT_PER_FILE]
        doc_links[0]["extracted"] = True
        doc_links[0]["char_count"] = len(snippet)
        doc_links[0]["extract_mode"] = "detail_fallback"
        label = str(doc_links[0].get("label") or "Document").strip()[:120]
        chunks.append(f"[{label}] {snippet}")

    return doc_links, "\n\n---\n\n".join(chunks).strip()


def _is_low_value_page_text(text: str) -> bool:
    """
    Detect portal chrome / boilerplate that looks like a successful fetch but
    doesn't contain real tender content.
    """
    sample = str(text or "")[:2500]
    if not sample.strip():
        return True
    signals = [
        "This page in:",
        "Loading...",
        "This site uses cookies to optimize functionality",
        "Successfully signed out",
        "You are signed out of the TED application",
        "Your session has timed out.",
        "Web applications store information about what you are doing on the server.",
    ]
    hits = sum(1 for marker in signals if marker in sample)
    if hits >= 2:
        return True
    if len(sample.strip()) < 200:
        return True
    return False


def _world_bank_notice_id(tender: dict, url: str) -> str:
    tid = str(tender.get("tender_id") or tender.get("id") or "").strip()
    if tid.upper().startswith("OP"):
        return tid
    m = re.search(r"(OP\d{5,})", tid, re.I)
    if m:
        return m.group(1).upper()
    m = re.search(r"/(OP\d{5,})\b", url, re.I)
    if m:
        return m.group(1).upper()
    m = re.search(r"[?&]id=(OP\d{5,})", url, re.I)
    if m:
        return m.group(1).upper()
    return ""


def _enrich_world_bank_notice(tender: dict, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    notice_id = _world_bank_notice_id(tender, str(tender.get("url") or ""))
    if not notice_id:
        return None
    try:
        resp = requests.get(
            _WB_PROC_API.format(notice_id=notice_id),
            headers={"User-Agent": _USER_AGENT, "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8"},
            timeout=timeout,
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        procnotice = root.find(".//procnotice")
        if procnotice is None:
            return None

        def _xml_value(name: str) -> str:
            for elem in procnotice:
                if elem.tag.endswith(name):
                    return (elem.text or "").strip()
            return ""

        notice_html = _xml_value("notice_text")
        notice_text = _extract_text_from_html(notice_html) if notice_html else ""
        description = notice_text[:_MAX_DESCRIPTION]
        if not description.strip():
            return None

        tender_url = str(tender.get("url") or "").strip()
        html_doc_links = _find_document_links(notice_html or "", tender_url or "https://projects.worldbank.org")
        # WB notice_text often includes plain URLs (not anchor tags). Harvest them
        # and keep document-like links for additional extraction opportunities.
        url_links = []
        for u in re.findall(r"https?://[^\s<>()\"']+", notice_text or "", flags=re.I):
            cu = str(u or "").strip().rstrip(".,;)")
            if not cu:
                continue
            low = cu.lower()
            if any(k in low for k in (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", "docs.google.com", "drive.google.com")):
                ftype = "pdf" if ".pdf" in low else ("word" if ".doc" in low else ("excel" if ".xls" in low else "other"))
                url_links.append({
                    "url": cu,
                    "label": "Document",
                    "file_type": ftype,
                })
        listing_doc_links = _collect_listing_and_detail_doc_links(
            description_text=notice_text,
            detail_url=tender_url,
            timeout=timeout,
        )
        doc_links = _merge_document_links(html_doc_links, url_links, listing_doc_links)
        doc_links, docs_text = _extract_document_chunks(
            doc_links,
            timeout=timeout,
            fallback_text=notice_text,
        )
        # When WB provides detailed notice text but no fetchable attachments,
        # preserve one grounded pseudo-document from the notice body itself so
        # downstream UI/metrics still capture extracted evidence.
        if (not doc_links) and description.strip():
            snippet = description[:_MAX_DOC_TEXT_PER_FILE]
            if snippet:
                doc_links = [{
                    "url": tender_url or f"https://projects.worldbank.org/en/projects-operations/procurement-detail/{notice_id}",
                    "label": "WB Notice Text",
                    "file_type": "other",
                    "extracted": True,
                    "char_count": len(snippet),
                    "extract_mode": "wb_notice_fallback",
                }]
                docs_text = f"[WB Notice Text] {snippet}"
        combined = description if not docs_text else f"{description}\n\n{docs_text}"

        fields = _extract_structured_fields(combined)
        out = {
            "deep_description": combined[:_MAX_DESCRIPTION],
            "deep_scope": fields.get("scope_of_work", ""),
            "deep_budget_raw": fields.get("budget_raw", ""),
            "deep_budget_currency": fields.get("budget_currency", ""),
            "deep_deadline_raw": fields.get("deadline_raw_deep", _xml_value("submission_deadline_date")[:50]),
            "deep_eval_criteria": fields.get("evaluation_criteria", ""),
            "deep_team_reqs": fields.get("team_requirements", ""),
            "deep_eligibility_raw": fields.get("eligibility_raw", ""),
            "deep_contact_emails": fields.get("contact_emails", []),
            "deep_contact_block": fields.get("contact_block", ""),
            "deep_source": "wb_api+docs" if docs_text else "wb_api",
            "deep_scraped_at": _now_iso(),
        }
        if doc_links:
            out["deep_document_links"] = doc_links
        if docs_text:
            out["deep_pdf_text"] = docs_text[:_MAX_DESCRIPTION]
        return out
    except Exception as exc:
        logger.debug("[deep_scraper] worldbank API enrichment failed for %s: %s", notice_id, exc)
        return None


def _enrich_ungm_notice(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    try:
        html = _fetch_page(url, timeout=timeout)
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        detail = soup.find("div", id="noticeDetail") or soup.find("div", class_="noticeDetail")
        if not detail:
            return None

        text_parts = []
        for block in detail.find_all("div", class_="ungm-list-item"):
            title_node = block.find("div", class_="title")
            block_title = title_node.get_text(" ", strip=True).lower() if title_node else ""
            body = block.get_text("\n", strip=True)
            if block_title in {"description", "documents", "links", "contact"} or "description" in block_title:
                text_parts.append(body)

        for row in detail.find_all("div", class_="row"):
            label = row.find("span", class_="label")
            value = row.find("span", class_="value")
            if label and value:
                text_parts.append(f"{label.get_text(' ', strip=True)} {value.get_text(' ', strip=True)}")

        combined = "\n".join(p for p in text_parts if p).strip()
        if not combined:
            return None

        doc_links = _find_document_links(html, url)
        doc_chunks = []
        for doc in doc_links[:_MAX_DOCS_TO_EXTRACT]:
            doc_url = str(doc.get("url") or "").strip()
            doc_type = str(doc.get("file_type") or "other").strip().lower()
            if not doc_url:
                doc["extracted"] = False
                doc["char_count"] = 0
                continue
            extracted = _fetch_and_extract_document(doc_url, doc_type, timeout=timeout)
            doc["extracted"] = bool(extracted)
            doc["char_count"] = len(extracted) if extracted else 0
            if extracted:
                label = str(doc.get("label") or "Document").strip()[:120]
                doc_chunks.append(f"[{label}] {extracted[:_MAX_DOC_TEXT_PER_FILE]}")
            time.sleep(0.2)
        docs_text = "\n\n---\n\n".join(doc_chunks).strip()
        combined_full = combined if not docs_text else f"{combined}\n\n{docs_text}"

        fields = _extract_structured_fields(combined_full)
        out = {
            "deep_description": combined_full[:_MAX_DESCRIPTION],
            "deep_scope": fields.get("scope_of_work", ""),
            "deep_budget_raw": fields.get("budget_raw", ""),
            "deep_budget_currency": fields.get("budget_currency", ""),
            "deep_deadline_raw": fields.get("deadline_raw_deep", ""),
            "deep_eval_criteria": fields.get("evaluation_criteria", ""),
            "deep_team_reqs": fields.get("team_requirements", ""),
            "deep_eligibility_raw": fields.get("eligibility_raw", ""),
            "deep_contact_emails": fields.get("contact_emails", []),
            "deep_contact_block": fields.get("contact_block", ""),
            "deep_source": "ungm_html",
            "deep_scraped_at": _now_iso(),
        }
        if doc_links:
            out["deep_document_links"] = doc_links
        if docs_text:
            out["deep_pdf_text"] = docs_text[:_MAX_DESCRIPTION]
        return out
    except Exception as exc:
        logger.debug("[deep_scraper] ungm enrichment failed for %s: %s", url[:80], exc)
        return None


def _enrich_ted_notice(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    try:
        html = _fetch_page(url, timeout=timeout)
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        meta = soup.find("meta", attrs={"name": "description"})
        description = ""
        if meta:
            description = str(meta.get("content") or "").strip()
        if description:
            description = html_unescape(description)
            fields = _extract_structured_fields(description)
            return {
                "deep_description": description[:_MAX_DESCRIPTION],
                "deep_scope": fields.get("scope_of_work", ""),
                "deep_budget_raw": fields.get("budget_raw", ""),
                "deep_budget_currency": fields.get("budget_currency", ""),
                "deep_deadline_raw": fields.get("deadline_raw_deep", ""),
                "deep_eval_criteria": fields.get("evaluation_criteria", ""),
                "deep_team_reqs": fields.get("team_requirements", ""),
                "deep_eligibility_raw": fields.get("eligibility_raw", ""),
                "deep_contact_emails": fields.get("contact_emails", []),
                "deep_contact_block": fields.get("contact_block", ""),
                "deep_source": "ted_meta",
                "deep_scraped_at": _now_iso(),
            }
        return None
    except Exception as exc:
        logger.debug("[deep_scraper] ted enrichment failed for %s: %s", url[:80], exc)
        return None


def _enrich_afdb_notice(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    """AfDB consultant vacancy detail page — server-rendered Drupal HTML."""
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": _USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Main body: article content or field--body div
        body = (
            soup.find("div", class_=lambda c: c and "field--name-body" in c)
            or soup.find("div", class_="field-items")
            or soup.find("article")
            or soup.find("main")
        )
        text = body.get_text("\n", strip=True) if body else _extract_text_from_html(resp.text)
        if not text or len(text) < 100:
            return None
        if _is_low_value_page_text(text):
            return None

        doc_links = _collect_listing_and_detail_doc_links(text, url, timeout=timeout)
        doc_links, docs_text = _extract_document_chunks(
            doc_links,
            timeout=timeout,
            request_headers={"Referer": url},
            request_cookies=resp.cookies.get_dict() if hasattr(resp, "cookies") else None,
            fallback_text=text,
        )
        combined = text if not docs_text else f"{text}\n\n{docs_text}"
        fields = _extract_structured_fields(combined)
        out = {
            "deep_description": text[:_MAX_DESCRIPTION],
            "deep_scope": fields.get("scope_of_work", ""),
            "deep_budget_raw": fields.get("budget_raw", ""),
            "deep_budget_currency": fields.get("budget_currency", ""),
            "deep_deadline_raw": fields.get("deadline_raw_deep", ""),
            "deep_eval_criteria": fields.get("evaluation_criteria", ""),
            "deep_team_reqs": fields.get("team_requirements", ""),
            "deep_eligibility_raw": fields.get("eligibility_raw", ""),
            "deep_contact_emails": fields.get("contact_emails", []),
            "deep_contact_block": fields.get("contact_block", ""),
            "deep_source": "afdb_html",
            "deep_scraped_at": _now_iso(),
        }
        if doc_links:
            out["deep_document_links"] = doc_links
        if docs_text:
            out["deep_pdf_text"] = docs_text[:_MAX_DESCRIPTION]
        return out
    except Exception as exc:
        logger.debug("[deep_scraper] afdb enrichment failed for %s: %s", url[:80], exc)
        return None


def _enrich_iucn_notice(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    """IUCN tender detail page — simple HTML or direct PDF link."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _USER_AGENT, "Accept": "text/html,*/*"},
            timeout=timeout,
        )
        resp.raise_for_status()

        # Extract based on content type
        ct = resp.headers.get("content-type", "")
        if "pdf" in ct or url.lower().endswith(".pdf"):
            text = _extract_text_from_pdf(resp.content)
            if not text:
                return None
            fields = _extract_structured_fields(text)
            out = {
                "deep_description": text[:_MAX_DESCRIPTION],
                "deep_scope": fields.get("scope_of_work", ""),
                "deep_budget_raw": fields.get("budget_raw", ""),
                "deep_budget_currency": fields.get("budget_currency", ""),
                "deep_deadline_raw": fields.get("deadline_raw_deep", ""),
                "deep_eval_criteria": fields.get("evaluation_criteria", ""),
                "deep_team_reqs": fields.get("team_requirements", ""),
                "deep_eligibility_raw": fields.get("eligibility_raw", ""),
                "deep_contact_emails": fields.get("contact_emails", []),
                "deep_contact_block": fields.get("contact_block", ""),
                "deep_source": "pdf",
                "deep_scraped_at": _now_iso(),
            }
            out["deep_document_links"] = [{
                "url": url,
                "label": "Primary Document",
                "file_type": "pdf",
                "extracted": True,
                "char_count": len(text),
            }]
            out["deep_pdf_text"] = text[:_MAX_DESCRIPTION]
            return out

        soup = BeautifulSoup(resp.text, "html.parser")
        body = (
            soup.find("div", class_=lambda c: c and "field--name-body" in c)
            or soup.find("div", class_="node__content")
            or soup.find("article")
            or soup.find("main")
        )
        text = body.get_text("\n", strip=True) if body else _extract_text_from_html(resp.text)
        if not text or len(text) < 80 or _is_low_value_page_text(text):
            return None

        doc_links = _collect_listing_and_detail_doc_links(text, url, timeout=timeout)
        doc_links, docs_text = _extract_document_chunks(
            doc_links,
            timeout=timeout,
            request_headers={"Referer": url},
            request_cookies=resp.cookies.get_dict() if hasattr(resp, "cookies") else None,
            fallback_text=text,
        )
        combined = text if not docs_text else f"{text}\n\n{docs_text}"
        fields = _extract_structured_fields(combined)
        out = {
            "deep_description": combined[:_MAX_DESCRIPTION],
            "deep_scope": fields.get("scope_of_work", ""),
            "deep_budget_raw": fields.get("budget_raw", ""),
            "deep_budget_currency": fields.get("budget_currency", ""),
            "deep_deadline_raw": fields.get("deadline_raw_deep", ""),
            "deep_eval_criteria": fields.get("evaluation_criteria", ""),
            "deep_team_reqs": fields.get("team_requirements", ""),
            "deep_eligibility_raw": fields.get("eligibility_raw", ""),
            "deep_contact_emails": fields.get("contact_emails", []),
            "deep_contact_block": fields.get("contact_block", ""),
            "deep_source": "iucn_html",
            "deep_scraped_at": _now_iso(),
        }
        if doc_links:
            out["deep_document_links"] = doc_links
        if docs_text:
            out["deep_pdf_text"] = docs_text[:_MAX_DESCRIPTION]
        return out
    except Exception as exc:
        logger.debug("[deep_scraper] iucn enrichment failed for %s: %s", url[:80], exc)
        return None


def _enrich_giz_notice(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    """GIZ tender detail page at ausschreibungen.giz.de — server-rendered HTML."""
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": _USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.5",
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # GIZ detail pages: main content in .tender-detail, .content, or article
        body = (
            soup.find("div", class_="tender-detail")
            or soup.find("div", id="content")
            or soup.find("div", class_="field--name-body")
            or soup.find("article")
        )
        text = body.get_text("\n", strip=True) if body else _extract_text_from_html(resp.text)
        if not text or len(text) < 80 or _is_low_value_page_text(text):
            return None

        doc_links = _collect_listing_and_detail_doc_links(text, url, timeout=timeout)
        # Pass Referer + session cookies — GIZ documents require session context.
        # Without this they return 403. Same pattern as IUCN and DTVP.
        doc_links, docs_text = _extract_document_chunks(
            doc_links,
            timeout=timeout,
            request_headers={"Referer": url},
            request_cookies=resp.cookies.get_dict() if hasattr(resp, "cookies") else None,
        )
        combined = text if not docs_text else f"{text}\n\n{docs_text}"
        fields = _extract_structured_fields(combined)
        out = {
            "deep_description": combined[:_MAX_DESCRIPTION],
            "deep_scope": fields.get("scope_of_work", ""),
            "deep_budget_raw": fields.get("budget_raw", ""),
            "deep_budget_currency": fields.get("budget_currency", ""),
            "deep_deadline_raw": fields.get("deadline_raw_deep", ""),
            "deep_eval_criteria": fields.get("evaluation_criteria", ""),
            "deep_team_reqs": fields.get("team_requirements", ""),
            "deep_eligibility_raw": fields.get("eligibility_raw", ""),
            "deep_contact_emails": fields.get("contact_emails", []),
            "deep_contact_block": fields.get("contact_block", ""),
            "deep_source": "giz_html+docs" if docs_text else "giz_html",
            "deep_scraped_at": _now_iso(),
        }
        if doc_links:
            out["deep_document_links"] = doc_links
        if docs_text:
            out["deep_pdf_text"] = docs_text[:_MAX_DESCRIPTION]
        return out
    except Exception as exc:
        logger.debug("[deep_scraper] giz enrichment failed for %s: %s", url[:80], exc)
        return None


def _enrich_afd_notice(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    """AFD dgmarket tender detail page — server-rendered HTML + document harvesting.

    dgmarket notice pages link to PDF ToRs and annexes via <a href> tags.
    We harvest those here the same way GIZ does — this was the missing step
    that kept AFD at 0% doc extraction coverage.
    """
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _USER_AGENT, "Accept": "text/html,application/xhtml+xml",
                     "Referer": "https://afd.dgmarket.com/tenders/brandedNoticeList.do"},
            timeout=timeout,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        # dgmarket detail: notice body in .tenderNoticeDetail or main content div
        body = (
            soup.find("div", class_="tenderNoticeDetail")
            or soup.find("div", id="noticeDetails")
            or soup.find("div", class_="notice-body")
            or soup.find("div", class_=lambda c: c and "detail" in " ".join(c).lower())
            or soup.find("table", class_=lambda c: c and "notice" in " ".join(c).lower())
            or soup.find("main")
        )
        text = body.get_text("\n", strip=True) if body else _extract_text_from_html(resp.text)
        if not text or len(text) < 80 or _is_low_value_page_text(text):
            return None

        # Harvest document links — TOR PDFs and annexes linked from the page HTML.
        # Pass full HTML to _find_document_links so <a href> tags aren't missed.
        html_doc_links = _find_document_links(resp.text, url)
        listing_doc_links = _collect_listing_and_detail_doc_links(text, url, timeout=timeout)
        doc_links = _merge_document_links(html_doc_links, listing_doc_links)
        doc_links, docs_text = _extract_document_chunks(
            doc_links,
            timeout=timeout,
            request_headers={"Referer": url},
        )
        combined = text if not docs_text else f"{text}\n\n{docs_text}"

        fields = _extract_structured_fields(combined)
        out = {
            "deep_description": combined[:_MAX_DESCRIPTION],
            "deep_scope": fields.get("scope_of_work", ""),
            "deep_budget_raw": fields.get("budget_raw", ""),
            "deep_budget_currency": fields.get("budget_currency", ""),
            "deep_deadline_raw": fields.get("deadline_raw_deep", ""),
            "deep_eval_criteria": fields.get("evaluation_criteria", ""),
            "deep_team_reqs": fields.get("team_requirements", ""),
            "deep_eligibility_raw": fields.get("eligibility_raw", ""),
            "deep_contact_emails": fields.get("contact_emails", []),
            "deep_contact_block": fields.get("contact_block", ""),
            "deep_source": "afd_html+docs" if docs_text else "afd_html",
            "deep_scraped_at": _now_iso(),
        }
        if doc_links:
            out["deep_document_links"] = doc_links
        if docs_text:
            out["deep_pdf_text"] = docs_text[:_MAX_DESCRIPTION]
        return out
    except Exception as exc:
        logger.debug("[deep_scraper] afd enrichment failed for %s: %s", url[:80], exc)
        return None


def _enrich_undp_notice(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    """UNDP procurement notice detail page."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _USER_AGENT, "Accept": "text/html,application/xhtml+xml",
                     "Referer": "https://procurement-notices.undp.org/"},
            timeout=timeout,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        # UNDP detail: content in table or div with negotiation details
        body = (
            soup.find("div", id="mainContent")
            or soup.find("div", class_="negotiation-details")
            or soup.find("table", id="tbl_negotiation")
            or soup.find("div", class_="content")
            or soup.find("main")
        )
        text = body.get_text("\n", strip=True) if body else _extract_text_from_html(resp.text)
        if not text or len(text) < 80 or _is_low_value_page_text(text):
            return None

        doc_links = _find_document_links(resp.text, url)
        doc_chunks = []
        for doc in doc_links[:_MAX_DOCS_TO_EXTRACT]:
            doc_url = str(doc.get("url") or "").strip()
            doc_type = str(doc.get("file_type") or "other").strip().lower()
            if not doc_url:
                doc["extracted"] = False
                doc["char_count"] = 0
                continue
            extracted = _fetch_and_extract_document(doc_url, doc_type, timeout=timeout)
            doc["extracted"] = bool(extracted)
            doc["char_count"] = len(extracted) if extracted else 0
            if extracted:
                label = str(doc.get("label") or "Document").strip()[:120]
                doc_chunks.append(f"[{label}] {extracted[:_MAX_DOC_TEXT_PER_FILE]}")
            time.sleep(0.2)
        docs_text = "\n\n---\n\n".join(doc_chunks).strip()
        combined = text if not docs_text else f"{text}\n\n{docs_text}"
        fields = _extract_structured_fields(combined)
        out = {
            "deep_description": combined[:_MAX_DESCRIPTION],
            "deep_scope": fields.get("scope_of_work", ""),
            "deep_budget_raw": fields.get("budget_raw", ""),
            "deep_budget_currency": fields.get("budget_currency", ""),
            "deep_deadline_raw": fields.get("deadline_raw_deep", ""),
            "deep_eval_criteria": fields.get("evaluation_criteria", ""),
            "deep_team_reqs": fields.get("team_requirements", ""),
            "deep_eligibility_raw": fields.get("eligibility_raw", ""),
            "deep_contact_emails": fields.get("contact_emails", []),
            "deep_contact_block": fields.get("contact_block", ""),
            "deep_source": "undp_html",
            "deep_scraped_at": _now_iso(),
        }
        if doc_links:
            out["deep_document_links"] = doc_links
        if docs_text:
            out["deep_pdf_text"] = docs_text[:_MAX_DESCRIPTION]
        return out
    except Exception as exc:
        logger.debug("[deep_scraper] undp enrichment failed for %s: %s", url[:80], exc)
        return None


def _enrich_ngobox_notice(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    """NGOBox full RFP/EOI detail page (ngobox.org/full_rfp_eoi_*)."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _USER_AGENT, "Referer": "https://ngobox.org/"},
            timeout=timeout,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        # Remove nav/header/footer noise
        for tag in soup.find_all(["nav", "header", "footer", "script", "style"]):
            tag.decompose()
        # Main content — NGOBox uses col-md-8 / .rfp-detail / .job-detail
        body = (
            soup.find("div", class_=re.compile(r"col-md-8|rfp.?detail|job.?detail|main.?content", re.I))
            or soup.find("article")
            or soup.find("main")
        )
        # Also try to grab attached document links
        doc_links = []
        for a in (body or soup).find_all("a", href=True):
            href = a["href"]
            if any(ext in href.lower() for ext in (".pdf", ".doc", ".docx", ".zip")):
                full = href if href.startswith("http") else "https://ngobox.org" + href
                doc_links.append(full)

        text = body.get_text("\n", strip=True) if body else _extract_text_from_html(resp.text)

        # Try to extract PDF if attached
        pdf_text = ""
        for doc_url in doc_links[:2]:
            pdf_bytes = _fetch_pdf(doc_url, timeout=timeout)
            if pdf_bytes:
                extracted = _extract_text_from_pdf(pdf_bytes)
                if extracted:
                    pdf_text += "\n" + extracted
                    break

        combined = (text + "\n" + pdf_text).strip()
        if not combined or len(combined) < 80 or _is_low_value_page_text(combined):
            return None
        fields = _extract_structured_fields(combined)
        source = "ngobox_html+pdf" if pdf_text else "ngobox_html"
        return {
            "deep_description":    text[:_MAX_DESCRIPTION],
            "deep_pdf_text":       pdf_text[:_MAX_DESCRIPTION] if pdf_text else "",
            "deep_scope":          fields.get("scope_of_work", ""),
            "deep_budget_raw":     fields.get("budget_raw", ""),
            "deep_budget_currency":fields.get("budget_currency", ""),
            "deep_deadline_raw":   fields.get("deadline_raw_deep", ""),
            "deep_eval_criteria":  fields.get("evaluation_criteria", ""),
            "deep_team_reqs":      fields.get("team_requirements", ""),
            "deep_eligibility_raw":fields.get("eligibility_raw", ""),
            "deep_contact_emails": fields.get("contact_emails", []),
            "deep_contact_block":  fields.get("contact_block", ""),
            "deep_source":         source,
            "deep_scraped_at":     _now_iso(),
        }
    except Exception as exc:
        logger.debug("[deep_scraper] ngobox enrichment failed for %s: %s", url[:80], exc)
        return None


def _enrich_jtds_notice(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    """JTDS Jharkhand — PDF links or HTML detail pages."""
    try:
        # Direct PDF
        if url.lower().endswith(".pdf"):
            pdf_bytes = _fetch_pdf(url, timeout=timeout)
            if pdf_bytes:
                pdf_text = _extract_text_from_pdf(pdf_bytes)
                if pdf_text and len(pdf_text) >= 80:
                    fields = _extract_structured_fields(pdf_text)
                    out = {
                        "deep_description":    pdf_text[:_MAX_DESCRIPTION],
                        "deep_pdf_text":       pdf_text[:_MAX_DESCRIPTION],
                        "deep_scope":          fields.get("scope_of_work", ""),
                        "deep_budget_raw":     fields.get("budget_raw", ""),
                        "deep_budget_currency":fields.get("budget_currency", ""),
                        "deep_deadline_raw":   fields.get("deadline_raw_deep", ""),
                        "deep_eval_criteria":  fields.get("evaluation_criteria", ""),
                        "deep_team_reqs":      fields.get("team_requirements", ""),
                        "deep_eligibility_raw":fields.get("eligibility_raw", ""),
                        "deep_contact_emails": fields.get("contact_emails", []),
                        "deep_contact_block":  fields.get("contact_block", ""),
                        "deep_source":         "pdf",
                        "deep_scraped_at":     _now_iso(),
                    }
                    out["deep_document_links"] = [{
                        "url": url,
                        "label": "Primary Document",
                        "file_type": "pdf",
                        "extracted": True,
                        "char_count": len(pdf_text),
                    }]
                    return out
        # HTML page — look for embedded PDF links
        resp = requests.get(
            url,
            headers={"User-Agent": _USER_AGENT, "Referer": "http://jtdsjharkhand.com/"},
            timeout=timeout,
        )
        resp.raise_for_status()
        html = resp.text
        text = _extract_text_from_html(html)

        doc_links = _collect_listing_and_detail_doc_links(text, url, timeout=timeout)
        doc_links, docs_text = _extract_document_chunks(doc_links, timeout=timeout, fallback_text=text)
        combined = text if not docs_text else f"{text}\n\n{docs_text}"
        if not combined or len(combined) < 80 or _is_low_value_page_text(combined):
            return None
        fields = _extract_structured_fields(combined)
        source = "jtds_html+docs" if docs_text else "jtds_html"
        out = {
            "deep_description":    combined[:_MAX_DESCRIPTION],
            "deep_pdf_text":       docs_text[:_MAX_DESCRIPTION] if docs_text else "",
            "deep_scope":          fields.get("scope_of_work", ""),
            "deep_budget_raw":     fields.get("budget_raw", ""),
            "deep_budget_currency":fields.get("budget_currency", ""),
            "deep_deadline_raw":   fields.get("deadline_raw_deep", ""),
            "deep_eval_criteria":  fields.get("evaluation_criteria", ""),
            "deep_team_reqs":      fields.get("team_requirements", ""),
            "deep_eligibility_raw":fields.get("eligibility_raw", ""),
            "deep_contact_emails": fields.get("contact_emails", []),
            "deep_contact_block":  fields.get("contact_block", ""),
            "deep_source":         source,
            "deep_scraped_at":     _now_iso(),
        }
        if doc_links:
            out["deep_document_links"] = doc_links
        return out
    except Exception as exc:
        logger.debug("[deep_scraper] jtds enrichment failed for %s: %s", url[:80], exc)
        return None


def _enrich_whh_notice(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    """Welthungerhilfe — eu-supply.com procurement detail pages."""
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": _USER_AGENT,
                "Referer":    "https://www.welthungerhilfe.de/",
                "Accept":     "text/html,application/xhtml+xml,*/*;q=0.9",
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup.find_all(["nav", "header", "footer", "script", "style"]):
            tag.decompose()

        # eu-supply puts notice details in a main content div
        body = (
            soup.find("div", id=re.compile(r"main|content|detail", re.I))
            or soup.find("main")
            or soup.find("table", class_=re.compile(r"notice|detail|rfq", re.I))
        )
        text = body.get_text("\n", strip=True) if body else _extract_text_from_html(resp.text)

        if not text or len(text) < 80 or _is_low_value_page_text(text):
            return None
        fields = _extract_structured_fields(text)
        return {
            "deep_description":    text[:_MAX_DESCRIPTION],
            "deep_scope":          fields.get("scope_of_work", ""),
            "deep_budget_raw":     fields.get("budget_raw", ""),
            "deep_budget_currency":fields.get("budget_currency", ""),
            "deep_deadline_raw":   fields.get("deadline_raw_deep", ""),
            "deep_eval_criteria":  fields.get("evaluation_criteria", ""),
            "deep_team_reqs":      fields.get("team_requirements", ""),
            "deep_eligibility_raw":fields.get("eligibility_raw", ""),
            "deep_contact_emails": fields.get("contact_emails", []),
            "deep_contact_block":  fields.get("contact_block", ""),
            "deep_source":         "whh_html",
            "deep_scraped_at":     _now_iso(),
        }
    except Exception as exc:
        logger.debug("[deep_scraper] whh enrichment failed for %s: %s", url[:80], exc)
        return None


def _enrich_icfre_notice(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    """ICFRE — PDF tenders. Fix /en/pdf/ → /pdf/ URL on the fly."""
    try:
        # Fix legacy URL path bug
        fixed_url = url.replace("icfre.gov.in/en/pdf/", "icfre.gov.in/pdf/")
        pdf_bytes = _fetch_pdf(fixed_url, timeout=timeout)
        if not pdf_bytes and fixed_url != url:
            pdf_bytes = _fetch_pdf(url, timeout=timeout)
        if not pdf_bytes:
            return None
        pdf_text = _extract_text_from_pdf(pdf_bytes)
        if not pdf_text or len(pdf_text) < 80:
            return None
        fields = _extract_structured_fields(pdf_text)
        out = {
            "deep_description":    pdf_text[:_MAX_DESCRIPTION],
            "deep_pdf_text":       pdf_text[:_MAX_DESCRIPTION],
            "deep_scope":          fields.get("scope_of_work", ""),
            "deep_budget_raw":     fields.get("budget_raw", ""),
            "deep_budget_currency":fields.get("budget_currency", ""),
            "deep_deadline_raw":   fields.get("deadline_raw_deep", ""),
            "deep_eval_criteria":  fields.get("evaluation_criteria", ""),
            "deep_team_reqs":      fields.get("team_requirements", ""),
            "deep_eligibility_raw":fields.get("eligibility_raw", ""),
            "deep_contact_emails": fields.get("contact_emails", []),
            "deep_contact_block":  fields.get("contact_block", ""),
            "deep_source":         "pdf",
            "deep_scraped_at":     _now_iso(),
        }
        out["deep_document_links"] = [{
            "url": fixed_url if pdf_bytes else url,
            "label": "Primary Document",
            "file_type": "pdf",
            "extracted": True,
            "char_count": len(pdf_text),
        }]
        return out
    except Exception as exc:
        logger.debug("[deep_scraper] icfre enrichment failed for %s: %s", url[:80], exc)
        return None


def _enrich_dtvp_notice(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[dict]:
    """DTVP Germany — public project forwarding pages."""
    try:
        # Convert secured/ → public/ if needed
        public_url = url.replace("/secured/", "/public/")
        resp = requests.get(
            public_url,
            headers={
                "User-Agent": _USER_AGENT,
                "Referer":    "https://www.dtvp.de/",
                "Accept":     "text/html,application/xhtml+xml,*/*;q=0.9",
                "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
            },
            timeout=timeout,
        )
        if resp.status_code in (401, 403):
            return None  # Auth required even on public path
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup.find_all(["nav", "header", "footer", "script", "style"]):
            tag.decompose()
        body = (
            soup.find("div", id=re.compile(r"main|content|project|notice", re.I))
            or soup.find("main")
            or soup.find("table", class_=re.compile(r"project|tender|notice", re.I))
        )
        text = body.get_text("\n", strip=True) if body else _extract_text_from_html(resp.text)
        if not text or len(text) < 80 or _is_low_value_page_text(text):
            return None
        doc_links = _collect_listing_and_detail_doc_links(text, public_url, timeout=timeout)
        doc_links, docs_text = _extract_document_chunks(
            doc_links,
            timeout=timeout,
            request_headers={"Referer": public_url},
            request_cookies=resp.cookies.get_dict() if hasattr(resp, "cookies") else None,
            fallback_text=text,
        )
        combined = text if not docs_text else f"{text}\n\n{docs_text}"
        fields = _extract_structured_fields(combined)
        out = {
            "deep_description":    combined[:_MAX_DESCRIPTION],
            "deep_scope":          fields.get("scope_of_work", ""),
            "deep_budget_raw":     fields.get("budget_raw", ""),
            "deep_budget_currency":fields.get("budget_currency", ""),
            "deep_deadline_raw":   fields.get("deadline_raw_deep", ""),
            "deep_eval_criteria":  fields.get("evaluation_criteria", ""),
            "deep_team_reqs":      fields.get("team_requirements", ""),
            "deep_eligibility_raw":fields.get("eligibility_raw", ""),
            "deep_contact_emails": fields.get("contact_emails", []),
            "deep_contact_block":  fields.get("contact_block", ""),
            "deep_source":         "dtvp_html",
            "deep_scraped_at":     _now_iso(),
        }
        if doc_links:
            out["deep_document_links"] = doc_links
        if docs_text:
            out["deep_pdf_text"] = docs_text[:_MAX_DESCRIPTION]
        return out
    except Exception as exc:
        logger.debug("[deep_scraper] dtvp enrichment failed for %s: %s", url[:80], exc)
        return None


def _extract_structured_fields(text: str) -> dict:
    """
    Extract structured fields from full page/PDF text.

    Upgraded extraction engine — covers all bid-critical fields:

    Returns a dict with keys:
        scope_of_work          — scope / ToR / objective text
        budget_raw             — raw budget string from page
        budget_currency        — e.g. USD / INR / EUR (if detected)
        deadline_raw_deep      — submission deadline string from page
        date_pre_bid           — pre-bid conference / site-visit date
        date_qa_deadline       — Q&A / clarification cut-off date
        date_contract_start    — expected contract start date
        contract_duration      — e.g. "18 months" / "2 years"
        eval_technical_weight  — int, e.g. 70 (technical score %)
        eval_financial_weight  — int, e.g. 30 (financial score %)
        evaluation_criteria    — full evaluation text block
        eligibility_raw        — raw eligibility block
        min_turnover_raw       — minimum annual turnover string
        min_years_experience   — int, minimum years of experience required
        min_similar_projects   — int, minimum similar projects required
        contact_emails         — list of email addresses (up to 5)
        contact_block          — raw contact paragraph
        team_requirements      — team composition / expert requirements block
    """
    result: dict = {}

    # ── 1. Scope of work ─────────────────────────────────────────────────────
    for pat in _SCOPE_PATTERNS:
        m = pat.search(text)
        if m:
            result["scope_of_work"] = m.group(1).strip()[:4000]
            break

    # ── 2. Budget / contract value ────────────────────────────────────────────
    for pat in _BUDGET_PATTERNS:
        m = pat.search(text)
        if m:
            gd = m.groupdict()
            if gd.get("amount"):
                currency = (gd.get("currency") or "").upper().strip()
                amount   = gd["amount"].strip()
                result["budget_raw"]      = f"{currency} {amount}".strip()
                result["budget_currency"] = currency
            else:
                result["budget_raw"] = m.group(0).strip()[:100]
            break

    # ── 3. Submission deadline ────────────────────────────────────────────────
    for pat in _DEADLINE_PATTERNS:
        m = pat.search(text)
        if m:
            result["deadline_raw_deep"] = m.group(1).strip()[:50]
            break

    # ── 4. Other key dates (pre-bid, Q&A, contract start) ────────────────────
    for field_name, pat in _KEY_DATE_PATTERNS.items():
        m = pat.search(text)
        if m:
            result[field_name] = m.group(1).strip()[:50]

    # ── 5. Contract duration ──────────────────────────────────────────────────
    for pat in _DURATION_PATTERNS:
        m = pat.search(text)
        if m:
            # Use the most specific match group available
            raw = (m.group(1) if m.lastindex and m.lastindex >= 1 else m.group(0)).strip()
            result["contract_duration"] = raw[:80]
            break

    # ── 6. Evaluation weights ─────────────────────────────────────────────────
    for pat in _EVAL_WEIGHT_PATTERNS:
        m = pat.search(text)
        if m:
            gd = m.groupdict()
            if gd.get("tech") and gd.get("fin"):
                try:
                    result["eval_technical_weight"] = int(gd["tech"])
                    result["eval_financial_weight"] = int(gd["fin"])
                except ValueError:
                    pass
            elif m.lastindex and m.lastindex >= 1:
                result["evaluation_criteria"] = m.group(1).strip()[:2000]
            break

    # ── 7. Eligibility criteria ───────────────────────────────────────────────
    for pat in _ELIGIBILITY_PATTERNS:
        m = pat.search(text)
        if m:
            gd = m.groupdict()
            if gd.get("amount"):
                # Turnover pattern
                ccy = (gd.get("currency") or "").upper().strip()
                amt = gd["amount"].strip()
                result["min_turnover_raw"] = f"{ccy} {amt}".strip()
            elif gd.get("years"):
                result["min_years_experience"] = int(gd["years"])
            elif gd.get("n"):
                result["min_similar_projects"] = int(gd["n"])
            elif not result.get("eligibility_raw") and m.lastindex and m.lastindex >= 1:
                result["eligibility_raw"] = m.group(1).strip()[:3000]
            # Don't break — each pattern targets a different field; run all
            continue

    # ── 8. Team composition ───────────────────────────────────────────────────
    for pat in _TEAM_PATTERNS:
        m = pat.search(text)
        if m:
            result["team_requirements"] = m.group(1).strip()[:2000]
            break

    # ── 9. Contact info ───────────────────────────────────────────────────────
    emails = list(dict.fromkeys(_EMAIL_RE.findall(text)))[:5]
    if emails:
        result["contact_emails"] = emails

    cb = _CONTACT_BLOCK_RE.search(text)
    if cb:
        result["contact_block"] = cb.group(1).strip()[:300]

    return result


# =============================================================================
# PUBLIC API
# =============================================================================

def enrich_tender_deep(
    tender:         dict,
    delay:          float = _DEFAULT_DELAY,
    timeout:        int   = _DEFAULT_TIMEOUT,
    skip_pdf:       bool  = False,
) -> dict:
    """
    Deep-enrich a single tender dict by following its URL.

    Adds these fields to the returned dict (originals preserved):
        deep_description    — full text from page (capped at _MAX_DESCRIPTION chars)
        deep_scope          — extracted scope of work / ToR
        deep_budget_raw     — raw budget string extracted from text
        deep_deadline_raw   — deadline string from page (supplements existing)
        deep_contact_emails — list of contact email addresses
        deep_eval_criteria  — evaluation / selection criteria text
        deep_team_reqs      — team composition / expert requirements
        deep_pdf_text       — combined text from all attached PDFs
        deep_source         — "page" | "pdf" | "page+pdf" | "skipped" | "failed"
        deep_scraped_at     — ISO timestamp of this enrichment

    Args:
        tender:    Tender dict with at minimum a `url` key.
        delay:     Seconds to sleep after fetch (rate limit politeness).
        timeout:   HTTP timeout in seconds.
        skip_pdf:  If True, skip PDF downloads even if links are found.

    Returns:
        Original tender dict merged with deep fields.
    """
    result = dict(tender)   # never mutate the original
    url    = str(tender.get("url") or "").strip()
    source_portal = str(tender.get("source_portal") or tender.get("source") or "").strip().lower()

    if not url:
        result["deep_source"] = "skipped"
        return result

    # Portal-specific adapters before generic HTML/PDF scraping.
    if source_portal in {"worldbank", "wb"}:
        wb_result = _enrich_world_bank_notice(tender, timeout=timeout)
        if wb_result:
            result.update(wb_result)
            if not str(result.get("description") or "").strip():
                result["description"] = result.get("deep_scope") or result.get("deep_description", "")[:2000]
            return result

    if source_portal in {"ungm", "ilo"}:
        # ILO tenders also live on ungm.org — same enricher applies
        ungm_result = _enrich_ungm_notice(url, timeout=timeout)
        if ungm_result:
            result.update(ungm_result)
            if not str(result.get("description") or "").strip():
                result["description"] = result.get("deep_scope") or result.get("deep_description", "")[:2000]
            return result

    if source_portal in {"ted", "tedeu"}:
        ted_result = _enrich_ted_notice(url, timeout=timeout)
        if ted_result:
            result.update(ted_result)
            if not str(result.get("description") or "").strip():
                result["description"] = result.get("deep_scope") or result.get("deep_description", "")[:2000]
            return result

    if source_portal in {"afd", "afd_france"}:
        afd_result = _enrich_afd_notice(url, timeout=timeout)
        if afd_result:
            result.update(afd_result)
            if not str(result.get("description") or "").strip():
                result["description"] = result.get("deep_scope") or result.get("deep_description", "")[:2000]
            return result

    if source_portal == "undp":
        undp_result = _enrich_undp_notice(url, timeout=timeout)
        if undp_result:
            result.update(undp_result)
            if not str(result.get("description") or "").strip():
                result["description"] = result.get("deep_scope") or result.get("deep_description", "")[:2000]
            return result

    if source_portal == "afdb":
        afdb_result = _enrich_afdb_notice(url, timeout=timeout)
        if afdb_result:
            result.update(afdb_result)
            if not str(result.get("description") or "").strip():
                result["description"] = result.get("deep_scope") or result.get("deep_description", "")[:2000]
            return result
        # AfDB often blocks detail pages; preserve listing intelligence as fallback.
        listing = str(tender.get("description") or tender.get("text") or "").strip()
        if listing:
            fields = _extract_structured_fields(listing)
            fallback_doc_links = []
            if url:
                fallback_doc_links = [{
                    "url": str(url),
                    "label": "AfDB Notice",
                    "file_type": "other",
                    "extracted": True,
                    "char_count": len(listing[:_MAX_DOC_TEXT_PER_FILE]),
                    "extract_mode": "listing_fallback",
                }]
            result["deep_description"] = listing[:_MAX_DESCRIPTION]
            result["deep_scope"] = fields.get("scope_of_work", "")
            result["deep_budget_raw"] = fields.get("budget_raw", "")
            result["deep_budget_currency"] = fields.get("budget_currency", "")
            result["deep_deadline_raw"] = fields.get("deadline_raw_deep", "")
            result["deep_eval_criteria"] = fields.get("evaluation_criteria", "")
            result["deep_team_reqs"] = fields.get("team_requirements", "")
            result["deep_eligibility_raw"] = fields.get("eligibility_raw", "")
            result["deep_contact_emails"] = fields.get("contact_emails", [])
            result["deep_contact_block"] = fields.get("contact_block", "")
            result["deep_pdf_text"] = listing[:_MAX_DESCRIPTION]
            if fallback_doc_links:
                result["deep_document_links"] = fallback_doc_links
            result["deep_source"] = "afdb_listing"
            result["deep_scraped_at"] = _now_iso()
            return result

    if source_portal == "iucn":
        iucn_result = _enrich_iucn_notice(url, timeout=timeout)
        if iucn_result:
            result.update(iucn_result)
            if not str(result.get("description") or "").strip():
                result["description"] = result.get("deep_scope") or result.get("deep_description", "")[:2000]
            return result

    if source_portal == "giz":
        giz_result = _enrich_giz_notice(url, timeout=timeout)
        if giz_result:
            result.update(giz_result)
            if not str(result.get("description") or "").strip():
                result["description"] = result.get("deep_scope") or result.get("deep_description", "")[:2000]
            return result

    if source_portal in {"ngobox", "ngo_box"}:
        ngobox_result = _enrich_ngobox_notice(url, timeout=timeout)
        if ngobox_result:
            result.update(ngobox_result)
            return result

    if source_portal in {"jtds", "jtds_jharkhand"}:
        jtds_result = _enrich_jtds_notice(url, timeout=timeout)
        if jtds_result:
            result.update(jtds_result)
            return result

    if source_portal in {"welthungerhilfe", "whh"}:
        whh_result = _enrich_whh_notice(url, timeout=timeout)
        if whh_result:
            result.update(whh_result)
            return result

    if source_portal == "icfre":
        icfre_result = _enrich_icfre_notice(url, timeout=timeout)
        if icfre_result:
            result.update(icfre_result)
            return result

    if source_portal == "dtvp":
        dtvp_result = _enrich_dtvp_notice(url, timeout=timeout)
        if dtvp_result:
            result.update(dtvp_result)
            return result

    # Lightweight listing-first enrichment with aggressive document link discovery.
    # adb/sidbi added here: their detail pages are plain HTML with downloadable PDFs,
    # same pattern as ec/sam — fetch page, harvest doc links, extract chunks.
    if source_portal in {"gem", "ted", "tedeu", "ec", "sam", "usaid", "adb", "sidbi"}:
        desc = tender.get("description") or tender.get("summary") or tender.get("text") or ""
        if not desc and url:
            html = _fetch_page(url, timeout=min(timeout, 20))
            if html:
                desc = _clean_html_text(html)[:_MAX_DESCRIPTION]

        doc_links = _collect_listing_and_detail_doc_links(
            description_text=str(desc),
            detail_url=str(url),
            timeout=timeout,
        )
        doc_links, docs_text = _extract_document_chunks(doc_links, timeout=timeout)

        combined = str(desc or "").strip()
        if docs_text:
            combined = f"{combined}\n\n{docs_text}".strip()
            result["deep_pdf_text"] = docs_text[:_MAX_DESCRIPTION]
        if doc_links:
            result["deep_document_links"] = doc_links

        if combined:
            fields = _extract_structured_fields(combined)
            result["deep_description"] = combined[:_MAX_DESCRIPTION]
            result["deep_scope"] = fields.get("scope_of_work", "")
            result["deep_budget_raw"] = fields.get("budget_raw", "")
            result["deep_budget_currency"] = fields.get("budget_currency", "")
            result["deep_deadline_raw"] = fields.get("deadline_raw_deep", "")
            result["deep_eval_criteria"] = fields.get("evaluation_criteria", "")
            result["deep_team_reqs"] = fields.get("team_requirements", "")
            result["deep_eligibility_raw"] = fields.get("eligibility_raw", "")
            result["deep_contact_emails"] = fields.get("contact_emails", [])
            result["deep_contact_block"] = fields.get("contact_block", "")
            result["deep_source"] = f"{source_portal}_listing+docs" if docs_text else f"{source_portal}_listing"
        else:
            result["deep_source"] = "skipped_api"
        result["deep_scraped_at"] = _now_iso()
        return result

    # For unresolved portals with bot-blocked domains, skip generic crawler path.
    if _is_blocked(url):
        result["deep_source"] = "skipped"
        result["deep_scraped_at"] = _now_iso()
        return result

    if source_portal in {"cg", "upetender", "up", "maharashtra", "karnataka"}:
        desc = tender.get("description") or tender.get("text") or ""
        listing_doc_links = _find_document_links_in_text(str(desc))
        if not listing_doc_links:
            prior_links = tender.get("deep_document_links")
            if isinstance(prior_links, str):
                try:
                    prior_links = _json.loads(prior_links)
                except Exception:
                    prior_links = []
            if isinstance(prior_links, list):
                listing_doc_links = [d for d in prior_links if isinstance(d, dict)]
        if not listing_doc_links and url:
            listing_doc_links = _discover_document_links_from_detail_page(str(url), timeout=min(timeout, 20))
        if not listing_doc_links:
            listing_doc_links = _extract_document_name_tokens_from_text(str(desc), url)
        listing_doc_chunks = []
        extracted_any = False
        detail_fallback_text = ""
        if listing_doc_links and any(str(d.get("link_kind") or "") == "portal_detail" for d in listing_doc_links):
            html = _fetch_page(str(url), timeout=min(timeout, 20)) if url else ""
            if html:
                detail_fallback_text = _clean_html_text(html)

        if listing_doc_links and not skip_pdf:
            for doc in listing_doc_links[:_MAX_DOCS_TO_EXTRACT]:
                doc_url = str(doc.get("url") or "").strip()
                doc_type = str(doc.get("file_type") or "other").strip().lower()
                if not doc_url or _is_blocked(doc_url):
                    doc["extracted"] = False
                    doc["char_count"] = 0
                    continue
                extracted = _fetch_and_extract_document(doc_url, doc_type, timeout=timeout)
                if not extracted and detail_fallback_text:
                    extracted = detail_fallback_text[:_MAX_DOC_TEXT_PER_FILE]
                doc["extracted"] = bool(extracted)
                doc["char_count"] = len(extracted) if extracted else 0
                if extracted:
                    extracted_any = True
                    label = str(doc.get("label") or "Document").strip()[:120]
                    listing_doc_chunks.append(f"[{label}] {extracted[:_MAX_DOC_TEXT_PER_FILE]}")
                time.sleep(0.25)

        # CG often exposes session-bound document links that fail direct fetch.
        # Preserve a grounded extraction signal using the listing/detail text.
        if source_portal == "cg" and (not extracted_any) and listing_doc_links and desc:
            fallback = str(desc)[:_MAX_DOC_TEXT_PER_FILE]
            if fallback.strip():
                for doc in listing_doc_links[:1]:
                    doc["extracted"] = True
                    doc["char_count"] = len(fallback)
                    doc["extract_mode"] = "listing_fallback"
                extracted_any = True
                label = str((listing_doc_links[0] or {}).get("label") or "Document").strip()[:120]
                listing_doc_chunks.append(f"[{label}] {fallback}")

        if desc:
            combined_text = str(desc)
            if extracted_any and listing_doc_chunks:
                docs_text = "\n\n---\n\n".join(listing_doc_chunks)
                combined_text = "\n\n".join([combined_text, docs_text])
                result["deep_pdf_text"] = docs_text[:_MAX_DESCRIPTION]
                result["deep_document_links"] = listing_doc_links
                result["deep_source"] = f"{source_portal}_listing+docs"
            else:
                result["deep_document_links"] = listing_doc_links
                result["deep_source"] = f"{source_portal}_listing"

            fields = _extract_structured_fields(combined_text)
            result["deep_description"] = combined_text[:_MAX_DESCRIPTION]
            result["deep_scope"] = fields.get("scope_of_work", "")
            result["deep_budget_raw"] = fields.get("budget_raw", "")
            result["deep_budget_currency"] = fields.get("budget_currency", "")
            result["deep_deadline_raw"] = fields.get("deadline_raw_deep", "")
            result["deep_date_pre_bid"] = fields.get("date_pre_bid", "")
            result["deep_date_qa_deadline"] = fields.get("date_qa_deadline", "")
            result["deep_date_contract_start"] = fields.get("date_contract_start", "")
            result["deep_contract_duration"] = fields.get("contract_duration", "")
            result["deep_eval_technical_weight"] = fields.get("eval_technical_weight")
            result["deep_eval_financial_weight"] = fields.get("eval_financial_weight")
            result["deep_eval_criteria"] = fields.get("evaluation_criteria", "")
            result["deep_eligibility_raw"] = fields.get("eligibility_raw", "")
            result["deep_min_turnover_raw"] = fields.get("min_turnover_raw", "")
            result["deep_min_years_experience"] = fields.get("min_years_experience")
            result["deep_min_similar_projects"] = fields.get("min_similar_projects")
            result["deep_contact_emails"] = fields.get("contact_emails", [])
            result["deep_contact_block"] = fields.get("contact_block", "")
            result["deep_team_reqs"] = fields.get("team_requirements", "")
        else:
            result["deep_source"] = "skipped"
        result["deep_scraped_at"] = _now_iso()
        return result
    page_text = ""
    pdf_text  = ""
    sources   = []
    document_links = []
    document_text_chunks = []

    # ── 1. Direct PDF path ───────────────────────────────────────────────────
    if url.lower().endswith(".pdf"):
        pdf_bytes = _fetch_pdf(url, timeout=timeout)
        if pdf_bytes:
            pdf_text = _extract_text_from_pdf(pdf_bytes)
            if pdf_text:
                sources.append("pdf")
                document_links = [{
                    "url": url,
                    "label": "Primary Document",
                    "file_type": "pdf",
                    "extracted": True,
                    "char_count": len(pdf_text),
                }]

    # ── 2. Fetch the tender page ─────────────────────────────────────────────
    html = None if pdf_text else _fetch_page(url, timeout=timeout)
    if html:
        page_text = _extract_text_from_html(html)
        if _is_low_value_page_text(page_text):
            page_text = ""
        if page_text:
            sources.append("page")

        # ── 3. Find and extract attachment documents (PDF + docs/text) ──────
        document_links = _find_document_links(html, url)
        if not skip_pdf and document_links:
            extracted_any = False
            to_extract = document_links[:_MAX_DOCS_TO_EXTRACT]
            for doc in to_extract:
                doc_url = str(doc.get("url") or "").strip()
                doc_type = str(doc.get("file_type") or "other").strip().lower()
                if not doc_url or _is_blocked(doc_url):
                    doc["extracted"] = False
                    doc["char_count"] = 0
                    continue

                extracted = _fetch_and_extract_document(doc_url, doc_type, timeout=timeout)
                doc["extracted"] = bool(extracted)
                doc["char_count"] = len(extracted) if extracted else 0
                if extracted:
                    extracted_any = True
                    label = str(doc.get("label") or "Document").strip()[:120]
                    snippet = extracted[:_MAX_DOC_TEXT_PER_FILE]
                    document_text_chunks.append(f"[{label}] {snippet}")
                    logger.debug(
                        "[deep_scraper] Extracted %d chars from %s: %s",
                        len(extracted),
                        doc_type or "document",
                        doc_url[:80],
                    )
                time.sleep(0.35)  # polite pause between attachment downloads

            if extracted_any:
                # Keep compatibility with existing downstream consumers that read deep_pdf_text.
                pdf_text = "\n\n---\n\n".join(document_text_chunks)
                sources.append("docs")

    # ── 4. Combine text sources — PDF takes priority for structured fields ────
    combined_text = "\n\n".join(filter(None, [pdf_text, page_text]))

    if not combined_text or len(combined_text) < 80 or _is_low_value_page_text(combined_text):
        result["deep_source"] = "skipped_lowvalue"
        result["deep_scraped_at"] = _now_iso()
        time.sleep(delay)
        return result

    # ── 5. Amendment detection — compare content hash with stored hash ────────
    new_hash = hashlib.md5(combined_text.encode("utf-8", errors="replace")).hexdigest()
    result["document_hash"]    = new_hash
    result["amendment_detected"] = False
    result["amendment_count"]    = 0

    _tid = str(tender.get("tender_id") or tender.get("id") or "").strip()
    if _tid:
        _old_hash, _old_count = _get_stored_doc_state(_tid)
        if _old_hash and _old_hash != new_hash:
            # Content has changed since last deep-scrape → amendment detected
            result["amendment_detected"] = True
            result["amendment_count"]    = _old_count + 1
            logger.info(
                "[deep_scraper] ⚠ Amendment detected for '%s' (amendment #%d) — "
                "old_hash=%s new_hash=%s",
                str(tender.get("title") or "")[:50],
                result["amendment_count"],
                _old_hash[:8], new_hash[:8],
            )

    # ── 6. Extract structured fields ─────────────────────────────────────────
    fields = _extract_structured_fields(combined_text)

    result["deep_description"]           = combined_text[:_MAX_DESCRIPTION]
    result["deep_scope"]                 = fields.get("scope_of_work", "")
    result["deep_budget_raw"]            = fields.get("budget_raw", "")
    result["deep_budget_currency"]       = fields.get("budget_currency", "")
    result["deep_deadline_raw"]          = fields.get("deadline_raw_deep", "")
    result["deep_date_pre_bid"]          = fields.get("date_pre_bid", "")
    result["deep_date_qa_deadline"]      = fields.get("date_qa_deadline", "")
    result["deep_date_contract_start"]   = fields.get("date_contract_start", "")
    result["deep_contract_duration"]     = fields.get("contract_duration", "")
    result["deep_eval_technical_weight"] = fields.get("eval_technical_weight")   # int or None
    result["deep_eval_financial_weight"] = fields.get("eval_financial_weight")   # int or None
    result["deep_eval_criteria"]         = fields.get("evaluation_criteria", "")
    result["deep_eligibility_raw"]       = fields.get("eligibility_raw", "")
    result["deep_min_turnover_raw"]      = fields.get("min_turnover_raw", "")
    result["deep_min_years_experience"]  = fields.get("min_years_experience")    # int or None
    result["deep_min_similar_projects"]  = fields.get("min_similar_projects")    # int or None
    result["deep_contact_emails"]        = fields.get("contact_emails", [])
    result["deep_contact_block"]         = fields.get("contact_block", "")
    result["deep_team_reqs"]             = fields.get("team_requirements", "")
    result["deep_pdf_text"]              = pdf_text[:_MAX_DESCRIPTION]
    result["deep_document_links"]        = document_links
    result["deep_source"]                = "+".join(sources) if sources else "failed"
    result["deep_scraped_at"]            = _now_iso()

    # If original description was empty, promote deep description
    if not str(result.get("description") or "").strip():
        result["description"] = result["deep_scope"] or result["deep_description"][:2000]

    logger.info(
        "[deep_scraper] Enriched '%s' — %d chars (%s)",
        str(tender.get("title") or "")[:50],
        len(combined_text),
        result["deep_source"],
    )

    time.sleep(delay)
    return result


def enrich_batch_deep(
    tenders:     list[dict],
    max_workers: int   = 4,
    delay:       float = _DEFAULT_DELAY,
    timeout:     int   = _DEFAULT_TIMEOUT,
    skip_pdf:    bool  = False,
) -> list[dict]:
    """
    Deep-enrich a batch of tenders in parallel threads.

    Args:
        tenders:     List of tender dicts (each needs a `url` key).
        max_workers: Max parallel HTTP threads (keep ≤ 5 to avoid rate limits).
        delay:       Per-request politeness delay (seconds).
        timeout:     HTTP timeout per request.
        skip_pdf:    Skip PDF downloads if True.

    Returns:
        List of enriched tender dicts in the same order as input.
    """
    if not tenders:
        return []

    results  = [None] * len(tenders)
    total    = len(tenders)
    enriched = 0
    skipped  = 0
    failed   = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_idx = {
            pool.submit(enrich_tender_deep, t, delay, timeout, skip_pdf): i
            for i, t in enumerate(tenders)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
                src = results[idx].get("deep_source", "")
                if src == "skipped":
                    skipped += 1
                elif src == "failed":
                    failed += 1
                else:
                    enriched += 1
            except Exception as exc:
                logger.warning("[deep_scraper] batch error for idx %d: %s", idx, exc)
                results[idx] = tenders[idx]   # fallback to original
                failed += 1

    logger.info(
        "[deep_scraper] Batch complete: %d/%d enriched, %d skipped, %d failed",
        enriched, total, skipped, failed,
    )
    return results  # type: ignore[return-value]


def _now_iso() -> str:
    from datetime import datetime
    return datetime.utcnow().isoformat() + "Z"


# =============================================================================
# Amendment detection helpers
# =============================================================================

def _get_stored_doc_state(tender_id: str) -> tuple:
    """
    Fetch (document_hash, amendment_count) for a tender already in the DB.

    Returns ("", 0) when:
      - the tender has never been deep-scraped before, OR
      - the document_hash column does not exist yet (first run ever)

    Non-fatal — any DB failure returns ("", 0) so the caller just treats
    the tender as 'never seen' and stores the hash for future comparisons.
    """
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor()

        # Guard: column may not exist before the first deep-scrape run
        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME   = 'tenders'
              AND COLUMN_NAME  = 'document_hash';
        """)
        if not (cur.fetchone() or (0,))[0]:
            cur.close()
            conn.close()
            return "", 0

        cur.execute(
            "SELECT document_hash, COALESCE(amendment_count, 0) "
            "FROM tenders WHERE tender_id = %s LIMIT 1;",
            (str(tender_id),),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return (row[0] or ""), int(row[1] or 0)
        return "", 0
    except Exception as exc:
        logger.debug(
            "[deep_scraper] _get_stored_doc_state error for %s: %s", tender_id, exc
        )
        return "", 0


# =============================================================================
# DB persistence — save deep fields back to tenders table
# =============================================================================

def save_deep_enrichment(tender_id: str, deep_data: dict) -> bool:
    """
    Persist deep-scraped fields back to the `tenders` table.

    Updates: description (if richer), word_count, has_description,
    and writes deep_scope / deep_eval_criteria / deep_team_reqs /
    deep_budget_raw / deep_contact_emails to the enrichment columns
    (auto-creates columns on first run — idempotent).

    Returns True on success.
    """
    try:
        from database.db import get_connection
        import json as _json

        description = str(deep_data.get("deep_description") or
                          deep_data.get("description") or "").strip()
        word_count  = len(description.split()) if description else 0

        conn = get_connection()
        cur  = conn.cursor()

        # Ensure deep enrichment columns exist (idempotent)
        _deep_cols = [
            # ── Original columns ───────────────────────────────────────────
            ("deep_description",           "MEDIUMTEXT"),
            ("deep_scope",                 "MEDIUMTEXT"),
            ("deep_eval_criteria",         "TEXT"),
            ("deep_team_reqs",             "TEXT"),
            ("deep_budget_raw",            "VARCHAR(200) DEFAULT ''"),
            ("deep_deadline_raw",          "VARCHAR(100) DEFAULT ''"),
            ("deep_contact_emails",        "JSON"),
            ("deep_pdf_text",              "MEDIUMTEXT"),
            ("deep_source",                "VARCHAR(50) DEFAULT ''"),
            ("deep_scraped_at",            "DATETIME DEFAULT NULL"),
            # ── Amendment tracking ─────────────────────────────────────────
            ("document_hash",              "VARCHAR(32) NOT NULL DEFAULT ''"),
            ("amendment_count",            "INT NOT NULL DEFAULT 0"),
            ("last_amended_at",            "DATETIME DEFAULT NULL"),
            # ── Upgraded extraction columns (Task 2) ───────────────────────
            ("deep_budget_currency",       "VARCHAR(10) DEFAULT ''"),
            ("deep_date_pre_bid",          "VARCHAR(80) DEFAULT ''"),
            ("deep_date_qa_deadline",      "VARCHAR(80) DEFAULT ''"),
            ("deep_date_contract_start",   "VARCHAR(80) DEFAULT ''"),
            ("deep_contract_duration",     "VARCHAR(120) DEFAULT ''"),
            ("deep_eval_technical_weight", "TINYINT DEFAULT NULL"),
            ("deep_eval_financial_weight", "TINYINT DEFAULT NULL"),
            ("deep_eligibility_raw",       "TEXT"),
            ("deep_min_turnover_raw",      "VARCHAR(150) DEFAULT ''"),
            ("deep_min_years_experience",  "TINYINT DEFAULT NULL"),
            ("deep_min_similar_projects",  "TINYINT DEFAULT NULL"),
            ("deep_contact_block",         "VARCHAR(400) DEFAULT ''"),
            ("deep_document_links",        "JSON"),
            ("deep_ai_summary",            "MEDIUMTEXT"),
        ]
        for col_name, col_def in _deep_cols:
            cur.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME   = 'tenders'
                  AND COLUMN_NAME  = %s;
            """, (col_name,))
            if not (cur.fetchone() or (0,))[0]:
                cur.execute(f"ALTER TABLE tenders ADD COLUMN {col_name} {col_def};")
                conn.commit()

        # Amendment flag — True means content changed since last scrape
        _is_amended = bool(deep_data.get("amendment_detected"))

        # Update the row (all columns — original + amendment + upgraded extraction)
        cur.execute("""
            UPDATE tenders SET
                description                = CASE WHEN LENGTH(COALESCE(description,'')) < %s
                                                  THEN %s ELSE description END,
                word_count                 = CASE WHEN word_count < %s THEN %s ELSE word_count END,
                has_description            = CASE WHEN %s > 0 THEN 1 ELSE has_description END,
                deep_description           = %s,
                deep_scope                 = %s,
                deep_eval_criteria         = %s,
                deep_team_reqs             = %s,
                deep_budget_raw            = %s,
                deep_deadline_raw          = %s,
                deep_budget_currency       = %s,
                deep_contact_emails        = %s,
                deep_contact_block         = %s,
                deep_pdf_text              = %s,
                deep_source                = %s,
                deep_scraped_at            = NOW(),
                deep_date_pre_bid          = %s,
                deep_date_qa_deadline      = %s,
                deep_date_contract_start   = %s,
                deep_contract_duration     = %s,
                deep_eval_technical_weight = %s,
                deep_eval_financial_weight = %s,
                deep_eligibility_raw       = %s,
                deep_min_turnover_raw      = %s,
                deep_min_years_experience  = %s,
                deep_min_similar_projects  = %s,
                document_hash              = %s,
                amendment_count            = CASE WHEN %s THEN COALESCE(amendment_count, 0) + 1
                                                  ELSE amendment_count END,
                last_amended_at            = CASE WHEN %s THEN NOW() ELSE last_amended_at END,
                deep_document_links        = %s,
                deep_ai_summary            = %s
            WHERE tender_id = %s;
        """, (
            # description / word_count / has_description
            len(description), description,
            word_count, word_count,
            word_count,
            # deep columns — original
            (deep_data.get("deep_description") or description)[:15000],
            deep_data.get("deep_scope",         "") or "",
            deep_data.get("deep_eval_criteria", "") or "",
            deep_data.get("deep_team_reqs",     "") or "",
            deep_data.get("deep_budget_raw",    "") or "",
            deep_data.get("deep_deadline_raw",  "") or "",
            deep_data.get("deep_budget_currency","") or "",
            _json.dumps(deep_data.get("deep_contact_emails") or []),
            deep_data.get("deep_contact_block", "") or "",
            (deep_data.get("deep_pdf_text") or "")[:15000],
            deep_data.get("deep_source",        "") or "",
            # upgraded date/eligibility/eval columns
            deep_data.get("deep_date_pre_bid",          "") or "",
            deep_data.get("deep_date_qa_deadline",      "") or "",
            deep_data.get("deep_date_contract_start",   "") or "",
            deep_data.get("deep_contract_duration",     "") or "",
            deep_data.get("deep_eval_technical_weight"),      # None if not found
            deep_data.get("deep_eval_financial_weight"),      # None if not found
            deep_data.get("deep_eligibility_raw",       "") or "",
            deep_data.get("deep_min_turnover_raw",      "") or "",
            deep_data.get("deep_min_years_experience"),       # None if not found
            deep_data.get("deep_min_similar_projects"),       # None if not found
            # amendment columns
            deep_data.get("document_hash", "") or "",
            _is_amended,
            _is_amended,
            # new columns: deep_document_links, deep_ai_summary
            # Normalise before serialising — guard against double-encoded strings
            # that arise when a prior run read a JSON column back as a string
            _json.dumps(_normalise_doc_links(deep_data.get("deep_document_links"))),
            str(deep_data.get("deep_ai_summary") or ""),
            str(tender_id),
        ))
        conn.commit()
        affected = cur.rowcount
        cur.close()
        conn.close()
        return affected > 0

    except Exception as e:
        logger.error("[deep_scraper] save_deep_enrichment error for %s: %s", tender_id, e)
        return False
