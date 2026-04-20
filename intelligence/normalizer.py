# =============================================================================
# intelligence/normalizer.py — Deterministic Tender Field Normalizer
#
# Converts a raw scraper dict (with inconsistent field names, date formats,
# and encoding) into a canonical NormalizedTender dataclass.
#
# Guarantees:
#   - Never calls any API or external service
#   - Never raises — all failures produce clean empty/None defaults
#   - Idempotent — normalizing the same dict twice gives the same result
#   - Works on ANY pipeline dict (handles all field name variants seen across
#     25+ scrapers: "Title", "title", "Tender Title", "Description", etc.)
#
# Key outputs:
#   title_clean   — title stripped of reference numbers, codes, excess whitespace
#   deadline_date — parsed to datetime.date, or None
#   country       — inferred from source_portal or raw text
#   content_hash  — MD5 of normalized title for cross-portal dedup fingerprint
#   source_portal — normalized source name (e.g. "NGO Box" → "ngobox")
#
# Called by: intelligence_layer.process_batch() before any API calls
# =============================================================================

import hashlib
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.expanduser("~/tender_system"))
logger = logging.getLogger("tenderradar.normalizer")


# ── Source portal → country mapping ──────────────────────────────────────────
# Used when the raw row doesn't explicitly state country.
_PORTAL_COUNTRY: Dict[str, str] = {
    "worldbank":        "India",        # we filter to IN projects
    "gem":              "India",
    "devnet":           "India",
    "cg":               "India",
    "up":               "India",
    "upetender":        "India",
    "giz":              "India",
    "undp":             "Global",       # UNDP is global; IND filtered separately
    "meghalaya":        "India",
    "ngobox":           "India",
    "iucn":             "Global",
    "welthungerhilfe":  "Global",
    "whh":              "Global",
    "ungm":             "Global",
    "sidbi":            "India",
    "afdb":             "Africa",
    "afd":              "Global",
    "icfre":            "India",
    "phfi":             "India",
    "jtds":             "India",
    "ted":              "Europe",
    "sam":              "USA",
    "karnataka":        "India",
    "usaid":            "Global",
    "dtvp":             "Germany",
    "taneps":           "Tanzania",
    "sikkim":           "India",
    "nic":              "India",
    "ec":               "European Union",
    "ilo":              "Global",
    "maharashtra":      "India",
}

_PORTAL_ORGANIZATION: Dict[str, str] = {
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
    "ilo": "ILO",
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

_TENDER_ID_ORG_HINTS: Dict[str, str] = {
    "MSRTC": "Maharashtra State Road Transport Corporation",
    "MBDA": "Meghalaya Basin Development Authority",
    "SIDBI": "SIDBI",
    "ICFRE": "ICFRE",
    "PHFI": "PHFI",
    "NHM": "NHM India",
}

# Reference number patterns to strip from titles
_REF_PATTERNS = [
    re.compile(r'\b[A-Z]{2,6}[-/]\d{4}[-/]\d+\b'),         # WB-2025-001
    re.compile(r'\bRFP[-/]\s*No\.?\s*\S+', re.I),           # RFP No. 2025/001
    re.compile(r'\bTender\s+No\.?\s*\S+', re.I),
    re.compile(r'\bRef\.?\s*No\.?\s*\S+', re.I),
    re.compile(r'\bNotice\s+No\.?\s*\S+', re.I),
    re.compile(r'\bITB[-/]\w+\b', re.I),                    # ITB/2025/001
    re.compile(r'\bIC[-/]\d+[-/]\w*\b', re.I),              # IC-2025-034
    re.compile(r'\[\s*[A-Z0-9/_-]{3,20}\s*\]'),             # [REF-2025-001]
    re.compile(r'^\s*\d{4,}/\d+[-/]\d*\s*[–:-]?\s*'),      # 2025/001: at start
    re.compile(r'\(\s*[A-Z]{2,6}\s*[-/]\s*\d{4,}\s*\)'),   # (RFP-2025)
]

# Date format strings tried in order (most specific first)
_DATE_FORMATS = [
    "%Y-%m-%d",       # 2026-04-15  (ISO — most reliable, try first)
    "%d-%b-%Y %I:%M %p",  # 13-Apr-2026 12:00 PM
    "%d-%B-%Y %I:%M %p",  # 13-April-2026 12:00 PM
    "%d/%m/%Y %I:%M %p",  # 13/04/2026 12:00 PM
    "%Y-%m-%d %H:%M:%S",  # 2026-04-13 14:30:00
    "%d-%m-%Y",       # 15-04-2026
    "%d/%m/%Y",       # 15/04/2026
    "%m/%d/%Y",       # 04/15/2026
    "%d.%m.%Y",       # 15.04.2026
    "%d %B %Y",       # 15 April 2026
    "%d %b %Y",       # 15 Apr 2026
    "%B %d, %Y",      # April 15, 2026
    "%b %d, %Y",      # Apr 15, 2026
    "%d %b. %Y",      # 15 Apr. 2026
    "%Y/%m/%d",       # 2026/04/15
    "%d-%b-%Y",       # 15-Apr-2026
    "%d-%B-%Y",       # 15-April-2026
]

# Field name aliases: canonical name → list of raw field names to check
_FIELD_ALIASES: Dict[str, List[str]] = {
    "title":       ["Title", "title", "Tender Title", "tender_title",
                    "NAME", "Name", "subject", "Subject", "Description_Short"],
    "description": ["Description", "description", "Body", "body",
                    "Details", "detail", "Scope", "scope", "Summary", "summary",
                    "text", "content", "Content", "Work Description"],
    "organization": ["Organisation", "Organization", "Authority",
                     "Entity", "Agency", "Client", "org", "Org",
                     "contracting_authority", "buyer", "Buyer", "Owner",
                     "Organisation Chain"],
    "deadline":    ["Deadline", "deadline", "Last Date", "last_date",
                    "Response Deadline", "Closing Date", "closing_date",
                    "Submission Deadline", "Bid Closing", "end_date",
                    "Due Date", "Due", "Expiry"],
    "country":     ["Country", "country", "Nation", "Location"],
    "url":         ["Detail Link", "detail_url", "URL", "url", "link",
                    "Link", "Url", "href", "source_url"],
    "value":       ["Value", "value", "Budget", "budget",
                    "Estimated Amount (US$)", "Amount", "amount",
                    "Contract Value", "Bid Value", "Tender Value in ₹",
                    "Tender Value", "Est. Value (₹)"],
    "source":      ["source", "Source", "source_site", "portal",
                    "SourceSite", "source_portal"],
}


# ── NormalizedTender dataclass ────────────────────────────────────────────────

@dataclass
class NormalizedTender:
    """
    Canonical representation of a tender after field normalization.
    All fields have clean types — no raw strings with "N/A" or mixed formats.
    """
    # ── Identity ──────────────────────────────────────────────────────────────
    tender_id:     str            # stable ID from pipeline (e.g. "NGOBOX_xxx")
    content_hash:  str            # MD5 of normalized title (cross-portal dedup key)
    source_portal: str            # normalized portal slug e.g. "ngobox"

    # ── Core metadata ─────────────────────────────────────────────────────────
    title:         str            # raw title as scraped
    title_clean:   str            # title with ref numbers stripped
    organization:  str            # hiring organization (empty string if unknown)
    country:       str            # inferred country (empty string if unknown)
    deadline:      Optional[date] # parsed to datetime.date, or None
    deadline_raw:  str            # original deadline string before parsing
    url:           str            # canonical source URL
    description:   str            # body text (may be empty)
    value_raw:     str            # original budget/value string (unparsed)
    scraped_at:    Optional[datetime]

    # ── Derived ───────────────────────────────────────────────────────────────
    is_expired:    bool           # True if deadline < today
    word_count:    int            # word count of description
    has_description: bool         # True if description has >50 chars

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dict safe for MySQL storage."""
        return {
            "tender_id":      self.tender_id,
            "content_hash":   self.content_hash,
            "source_portal":  self.source_portal,
            "title":          self.title[:500],
            "title_clean":    self.title_clean[:500],
            "organization":   self.organization[:255],
            "country":        self.country[:100],
            "deadline":       self.deadline.isoformat() if self.deadline else None,
            "deadline_raw":   self.deadline_raw[:100],
            "url":            self.url[:2000],
            "description":    self.description[:10000],
            "value_raw":      self.value_raw[:200],
            "scraped_at":     (self.scraped_at.isoformat()
                               if self.scraped_at else datetime.utcnow().isoformat()),
            "is_expired":     int(self.is_expired),
            "word_count":     self.word_count,
            "has_description": int(self.has_description),
        }


# ── Public entry point ────────────────────────────────────────────────────────

def normalize_tender(raw: Dict[str, Any], tender_id: str = "") -> NormalizedTender:
    """
    Convert a raw scraper dict into a NormalizedTender.

    Args:
        raw:       Any dict produced by a pipeline scraper.
        tender_id: Optional stable ID; if blank, derived from URL hash.

    Returns:
        NormalizedTender — never raises, falls back to safe defaults.
    """
    # ── Extract raw field values ───────────────────────────────────────────────
    title        = _pick(raw, "title")         or ""
    description  = _pick(raw, "description")   or ""
    organization = _pick(raw, "organization")  or ""
    deadline_raw = _pick(raw, "deadline")       or ""
    country_raw  = _pick(raw, "country")       or ""
    url          = _pick(raw, "url")           or ""
    value_raw    = _pick(raw, "value")         or ""
    source_raw   = (_pick(raw, "source") or raw.get("source_site", "")
                    or raw.get("source_name", "") or "unknown")

    # ── Normalize ─────────────────────────────────────────────────────────────
    source_portal = _normalize_source(source_raw, url=url, tender_id=tender_id)
    title_clean   = _clean_title(title)
    content_hash  = _content_hash(title_clean)
    deadline      = _parse_date(deadline_raw)
    country       = _infer_country(country_raw, description, source_portal)
    organization  = _infer_organization(raw, organization, source_portal, tender_id)
    description   = _clean_text(description)
    if not description and title_clean:
        # Safe fallback so downstream intelligence isn't starved of context
        description = f"{title_clean}. {organization}".strip().strip(".") + "."
    url           = url.strip()
    value_raw     = _clean_value(value_raw)

    if not tender_id:
        tender_id = _derive_id(source_portal, url, title_clean)

    today      = date.today()
    is_expired = (deadline < today) if deadline else False
    words      = len(description.split()) if description else 0

    return NormalizedTender(
        tender_id     = tender_id,
        content_hash  = content_hash,
        source_portal = source_portal,
        title         = title[:500],
        title_clean   = title_clean[:500],
        organization  = organization[:255],
        country       = country[:100],
        deadline      = deadline,
        deadline_raw  = deadline_raw[:100],
        url           = url[:2000],
        description   = description,
        value_raw     = value_raw[:200],
        scraped_at    = datetime.utcnow(),
        is_expired    = is_expired,
        word_count    = words,
        has_description = (len(description) >= 50),
    )


# ── Field extraction helpers ─────────────────────────────────────────────────

def _pick(raw: Dict, canonical: str) -> str:
    """
    Try all known aliases for `canonical` field name and return the first
    non-empty string value found.
    """
    for alias in _FIELD_ALIASES.get(canonical, [canonical]):
        v = raw.get(alias)
        if v and str(v).strip() and str(v).strip().lower() not in ("n/a", "none", "-", "–"):
            return str(v).strip()
    return ""


def _infer_organization(
    raw: Dict[str, Any],
    organization: str,
    source_portal: str,
    tender_id: str,
) -> str:
    """
    Fill organization with safe fallbacks when the raw row is thin.
    """
    cleaned = _clean_text(organization)
    if cleaned:
        return cleaned[:255]

    for key in [
        "Organisation Name", "organisation_name", "department", "Department",
        "ministry", "Ministry", "agency", "Agency", "authority", "Authority",
        "issuer", "Issuer", "organizationName", "organizationHierarchy",
        "financing_agency", "Financing Agency",
    ]:
        value = raw.get(key)
        if value and str(value).strip():
            cleaned = _clean_text(str(value))
            if cleaned:
                return cleaned[:255]

    upper_tid = str(tender_id or "").upper()
    for token, org_name in _TENDER_ID_ORG_HINTS.items():
        if token in upper_tid:
            return org_name

    return _PORTAL_ORGANIZATION.get(source_portal, "")[:255]


# ── Title cleaning ────────────────────────────────────────────────────────────

def _clean_title(title: str) -> str:
    """Remove reference numbers, codes, and normalize whitespace."""
    t = title
    for pat in _REF_PATTERNS:
        t = pat.sub(" ", t)
    # Collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()
    # Strip leading punctuation left by reference removal
    t = re.sub(r"^[\s\-–:,|]+", "", t).strip()
    return t or title   # fallback to original if we stripped too much


def _content_hash(title_clean: str) -> str:
    """
    Produce a stable MD5 fingerprint for cross-portal dedup.
    Normalises to lowercase, strips all non-alphanumeric chars.
    Two tenders with the same meaningful words will produce the same hash
    even if punctuation or reference numbers differ.
    """
    normalized = re.sub(r"[^a-z0-9 ]", "", title_clean.lower())
    # Collapse stop-word variations: "the", "a", "and", "of", "for", "in"
    tokens = [w for w in normalized.split()
              if w not in ("the", "a", "an", "and", "of", "for", "in",
                           "to", "on", "at", "by", "with", "from", "is")]
    key = " ".join(sorted(tokens))   # sort so "A B" == "B A" variants match
    return hashlib.md5(key.encode()).hexdigest()


# ── Date parsing ─────────────────────────────────────────────────────────────

def _parse_date(raw: str) -> Optional[date]:
    """
    Try every known date format; return datetime.date or None.
    Handles partial matches: extracts the date-like substring first.
    """
    if not raw or raw.strip().lower() in ("n/a", "none", "-", "–", "tbd", "open"):
        return None

    # Try to extract a date-looking substring
    clean = raw.strip()

    # Remove common prefixes: "Deadline:", "Due:", "by", "before"
    clean = re.sub(r"^(deadline|due date|due|closing|submission|by|before)\s*:?\s*",
                   "", clean, flags=re.I).strip()

    # Try each format
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(clean[:len(fmt) + 4], fmt).date()
        except (ValueError, IndexError):
            pass

    # Regex extraction fallback: find YYYY-MM-DD anywhere in string
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", clean)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # Regex for "DD Mon YYYY" or "DD Month YYYY"
    m = re.search(r"(\d{1,2})\s+([A-Za-z]{3,9})\.?\s+(\d{4})", clean)
    if m:
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(
                    f"{m.group(1)} {m.group(2).rstrip('.')} {m.group(3)}", fmt
                ).date()
            except ValueError:
                pass

    logger.debug(f"[normalizer] Could not parse date: {raw!r}")
    return None


# ── Source / country helpers ──────────────────────────────────────────────────

def _normalize_source(raw: str, url: str = "", tender_id: str = "") -> str:
    """Map any source string to a lowercase slug matching _PORTAL_COUNTRY keys."""
    s = raw.lower().strip()
    # Exact known mapping
    mapping = {
        "world bank":              "worldbank",
        "worldbank":               "worldbank",
        "gem":                     "gem",
        "gem bidplus":             "gem",
        "devnet":                  "devnet",
        "devnet india":            "devnet",
        "cg":                      "cg",
        "cg eprocurement":         "cg",
        "giz":                     "giz",
        "giz india":               "giz",
        "undp":                    "undp",
        "undp procurement":        "undp",
        "meghalaya":               "meghalaya",
        "meghalaya mbda":          "meghalaya",
        "ngobox":                  "ngobox",
        "ngo box":                 "ngobox",
        "iucn":                    "iucn",
        "iucn procurement":        "iucn",
        "welthungerhilfe":         "welthungerhilfe",
        "deutsche welthungerhilfe":"welthungerhilfe",
        "whh":                     "welthungerhilfe",
        "ungm":                    "ungm",
        "ilo":                     "ilo",
        "ilo procurement":         "ilo",
        "international labour organization": "ilo",
        "sidbi":                   "sidbi",
        "afdb":                    "afdb",
        "afdb consultants":        "afdb",
        "afd":                     "afd",
        "afd france":              "afd",
        "icfre":                   "icfre",
        "phfi":                    "phfi",
        "jtds":                    "jtds",
        "jtds jharkhand":          "jtds",
        "ted":                     "ted",
        "ted eu":                  "ted",
        "sam":                     "sam",
        "sam.gov":                 "sam",
        "karnataka":               "karnataka",
        "karnataka eprocure":      "karnataka",
        "usaid":                   "usaid",
        "dtvp":                    "dtvp",
        "dtvp germany":            "dtvp",
        "taneps":                  "taneps",
        "taneps tanzania":         "taneps",
        "sikkim":                  "sikkim",
        "nic":                     "nic",
        "nic state portals":       "nic",
    }
    normalized = mapping.get(s, re.sub(r"[^a-z0-9]", "", s) or "unknown")
    return _infer_source_from_context(normalized, url=url, tender_id=tender_id)


def _infer_source_from_context(source: str, url: str = "", tender_id: str = "") -> str:
    """
    Use URL and tender_id hints to collapse legacy aliases and generic
    placeholders into the canonical source slugs.
    """
    source = (source or "").strip().lower() or "unknown"
    url_l = (url or "").strip().lower()
    tid_u = (tender_id or "").strip().upper()

    if "worldbank.org" in url_l:
        return "worldbank"
    if "dtvp.de" in url_l:
        return "dtvp"
    if "etender.up.nic.in" in url_l:
        return "upetender"
    if "mahatenders.gov.in" in url_l:
        return "maharashtra"
    if "eprocure.gov.in" in url_l:
        return "cg"
    if "icfre.gov.in" in url_l or source in {"archive", "current"}:
        return "icfre"
    if "ungm.org" in url_l:
        # ILO tenders live on ungm.org but should keep their own portal slug
        if tid_u.startswith("ILO/") or tid_u.startswith("ILO_"):
            return "ilo"
        return "ungm"
    if "ted.europa.eu" in url_l:
        return "ted"
    if source == "wb":
        return "worldbank"
    if source == "up":
        return "upetender"
    if source == "unknown":
        if tid_u.startswith("DTVP_"):
            return "dtvp"
        if tid_u.startswith("WB_") or tid_u.startswith("OP"):
            return "worldbank"
        if tid_u.startswith("UNGM_"):
            return "ungm"
        if tid_u.startswith("TED_"):
            return "ted"
    return source


# India state / city names for country inference
_INDIA_SIGNALS = re.compile(
    r"\b(india|indian|bihar|uttar pradesh|rajasthan|madhya pradesh|"
    r"chhattisgarh|jharkhand|odisha|orissa|assam|meghalaya|nagaland|"
    r"manipur|tripura|mizoram|sikkim|arunachal|kerala|tamil\s*nadu|"
    r"andhra|telangana|karnataka|gujarat|maharashtra|punjab|haryana|"
    r"himachal|jammu|kashmir|uttarakhand|delhi|goa|new delhi|mumbai|"
    r"bengaluru|hyderabad|chennai|kolkata|ahmedabad)\b",
    re.I,
)

def _infer_country(raw: str, description: str, source: str) -> str:
    """
    Determine the country for a tender.
    Priority: explicit field → description signal → portal default.
    """
    if raw and raw.strip().lower() not in ("n/a", "none", "-", "global", ""):
        return raw.strip()

    # Check description for geographic signals
    text = (description or "")[:500]
    if _INDIA_SIGNALS.search(text):
        return "India"

    return _PORTAL_COUNTRY.get(source, "")


def _clean_text(text: str) -> str:
    """Normalize encoding, collapse whitespace, strip HTML artifacts."""
    if not text:
        return ""
    # Remove common HTML entities
    t = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">") \
            .replace("&nbsp;", " ").replace("&quot;", '"')
    # Collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _clean_value(raw: str) -> str:
    """Normalize value/budget strings to remove 'N/A', 'None', 'See listing'."""
    if not raw:
        return ""
    if raw.strip().lower() in ("n/a", "none", "-", "–", "see listing",
                               "as per requirement", "tbd", "not disclosed"):
        return ""
    return raw.strip()


def _derive_id(source: str, url: str, title_clean: str) -> str:
    """Generate a stable tender_id from URL or title when pipeline doesn't provide one."""
    key = url or title_clean
    slug = re.sub(r"[^a-zA-Z0-9]", "_",
                  (url.split("/")[-1] or title_clean)[:60])
    return f"{source.upper()}_{slug}"
