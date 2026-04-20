# =============================================================================
# core/base_scraper.py — IntelligentBaseScraper
#
# Abstract base class for every TenderRadar portal scraper.
# Enforces a fixed 8-step pipeline; portals override only what is unique
# to them — everything shared is inherited automatically.
#
# ── Pipeline (enforced in run()) ─────────────────────────────────────────────
#   1. fetch_data()         → raw data dict/list (MUST override)
#   2. validate_schema()    → bool; logs SCHEMA CHANGE if structure drifts
#   3. extract_rows()       → list[dict] of raw rows (MUST override)
#   4. quality filter+score → via quality_engine.apply_intelligence_filter()
#   5. enrich_fields()      → inject sector/type/geography/score (can extend)
#   6. deduplicate()        → stable key-based dedup (can override key)
#   7. to_standard_format() → TenderResult for new_tenders (MUST override)
#   8. monitoring           → via ScraperMonitor (fully shared)
#
# ── Minimal contract for a new portal ───────────────────────────────────────
#
#   class MyPortalScraper(IntelligentBaseScraper):
#       SOURCE_NAME  = "My Portal"
#       SOURCE_URL   = "https://myportal.org/tenders"
#       EXCEL_PATH   = config.MY_EXCEL_PATH
#
#       def fetch_data(self):
#           r = requests.get(self.SOURCE_URL, timeout=30)
#           return r.json()
#
#       def extract_rows(self, raw_data):
#           return [{"title": t["name"], "deadline": t["due"]}
#                   for t in raw_data.get("tenders", [])]
#
#       def to_standard_format(self, row):
#           return make_tender_result(
#               title   = row.get("title", ""),
#               url     = row.get("url", self.SOURCE_URL),
#               source  = self.SOURCE_NAME,
#               **row,
#           )
#
#   # Module-level shim so registry can call run()
#   def run():
#       return MyPortalScraper().run()
#
# =============================================================================

from __future__ import annotations

import logging
import re
import time
from abc import ABC, abstractmethod
from typing import Any

from core.quality_engine import (
    TenderResult,
    apply_intelligence_filter,
    make_tender_result,
    QUALITY_THRESHOLD,
    TIER_LABELS,
)
from core.scraper_monitor import ScraperMonitor
from database.db import check_if_new, mark_as_seen


logger = logging.getLogger("tenderradar.base")

# Global debug flag — can also be set per-instance via run(debug=True)
_DEBUG: bool = False

def set_debug(flag: bool) -> None:
    global _DEBUG
    _DEBUG = flag


class IntelligentBaseScraper(ABC):
    """
    Abstract base class for the TenderRadar intelligent scraper pipeline.

    Subclasses MUST implement:
        fetch_data()         → Any
        extract_rows(data)   → list[dict]
        to_standard_format(row) → TenderResult

    Subclasses MAY override:
        validate_schema(data)    → bool
        deduplicate(rows)        → list[dict]
        get_tender_id(row)       → str
        enrich_fields(rows)      → list[dict]   (post-filter hook)
        on_run_start()           → None          (pre-fetch hook)
        on_run_end(all_rows)     → None          (post-pipeline hook, e.g. save Excel)
    """

    # ── Required class attributes — set in every subclass ─────────────────────
    SOURCE_NAME: str = ""          # Human-readable  e.g. "World Bank"
    SOURCE_URL:  str = ""          # Primary portal URL
    EXCEL_PATH:  str = ""          # Absolute path to output .xlsx (or "")

    # ── Optional overrides ────────────────────────────────────────────────────
    QUALITY_THRESHOLD: int   = QUALITY_THRESHOLD
    DELAY_BETWEEN_REQUESTS: float = 0.5    # polite crawl delay (seconds)

    # ── Schema expectations — override per portal ─────────────────────────────
    # List of (field_name, expected_python_type) pairs used by validate_schema()
    EXPECTED_SCHEMA_FIELDS: list[tuple[str, type]] = []

    # ─────────────────────────────────────────────────────────────────────────
    def __init__(self):
        self._debug:   bool         = _DEBUG
        self._monitor: ScraperMonitor = ScraperMonitor(
            self.SOURCE_NAME or self.__class__.__name__
        )
        self._log = logging.getLogger(
            f"tenderradar."
            f"{re.sub(r'[^a-z0-9]', '_', (self.SOURCE_NAME or 'scraper').lower())}"
        )

    # =========================================================================
    # STEP 1 — Fetch raw data  (MUST override)
    # =========================================================================

    @abstractmethod
    def fetch_data(self) -> Any:
        """
        Retrieve raw data from the portal.

        Return any structure (dict, list, bytes, str, …) that
        extract_rows() knows how to process.

        Return None to abort the run gracefully (logged as 'no data').
        Raise any exception to abort and mark the run as failed.
        """

    # =========================================================================
    # STEP 2 — Schema validation  (default: field-type check via monitor)
    # =========================================================================

    def validate_schema(self, raw_data: Any) -> bool:
        """
        Validate raw data structure.  Default: check EXPECTED_SCHEMA_FIELDS
        against a sample of records.

        Override for portal-specific deep validation.
        Returns True when schema is healthy.
        """
        if not self.EXPECTED_SCHEMA_FIELDS:
            return True  # no spec → skip validation

        # Resolve sample: support dict-of-dicts, list-of-dicts, or plain dict
        sample = _to_sample(raw_data, size=20)
        if not sample:
            return True

        violations = 0
        for rec in sample:
            for field, expected_type in self.EXPECTED_SCHEMA_FIELDS:
                val = rec.get(field)
                if val is not None and not isinstance(val, expected_type):
                    self._log.debug(
                        f"Schema: field '{field}' has type "
                        f"{type(val).__name__}, expected {expected_type.__name__}"
                    )
                    violations += 1
                    break

        rate = violations / len(sample)
        self._monitor.check_schema_change(
            rate,
            raw_data=raw_data,
            checked_fields=[f for f, _ in self.EXPECTED_SCHEMA_FIELDS],
        )
        return rate <= 0.20

    # =========================================================================
    # STEP 3 — Extract rows  (MUST override)
    # =========================================================================

    @abstractmethod
    def extract_rows(self, raw_data: Any) -> list[dict]:
        """
        Convert raw portal data into a flat list of row dicts.

        Each dict should have at minimum a meaningful text field:
            "title" / "Title" / "Description" / "description"

        The quality engine will work with any field-naming convention.
        """

    # =========================================================================
    # STEP 4 — Quality filter + score (shared — do NOT override)
    # =========================================================================
    # Handled by quality_engine.apply_intelligence_filter() inside run().
    # Portal scrapers cannot and should not modify filtering logic here.
    # To adjust threshold: set QUALITY_THRESHOLD on the subclass.

    # =========================================================================
    # STEP 5 — Enrich fields  (optional post-filter hook)
    # =========================================================================

    def enrich_fields(self, rows: list[dict]) -> list[dict]:
        """
        Post-filter enrichment hook.

        Default: passthrough (signals already injected by quality_engine).
        Override to add portal-specific computed fields AFTER quality filtering,
        e.g. building a canonical URL from a reference number.
        """
        return rows

    # =========================================================================
    # STEP 6 — Deduplication  (can override get_tender_id)
    # =========================================================================

    def get_tender_id(self, row: dict) -> str:
        """
        Generate a stable unique key for DB deduplication.

        Default: SOURCE_NAME prefix + slug of url or title.
        Override for portals with natural IDs (notice numbers, ref codes, …).
        """
        prefix = re.sub(r"[^A-Z0-9]", "_",
                        (self.SOURCE_NAME or "UNK").upper())[:12]
        url   = str(row.get("url") or row.get("detail_url")
                    or row.get("Detail Link") or "")
        title = str(row.get("title") or row.get("Title")
                    or row.get("Description") or "")
        slug  = re.sub(r"[^a-zA-Z0-9]", "_",
                       (url.split("/")[-1] or title)[:80])
        return f"{prefix}_{slug}"

    def deduplicate(self, rows: list[dict]) -> list[dict]:
        """
        Remove rows with duplicate tender IDs within this run.
        Stable: first occurrence wins.
        """
        seen:   set[str]  = set()
        unique: list[dict] = []
        for row in rows:
            tid = self.get_tender_id(row)
            if tid in seen:
                if self._debug:
                    self._log.debug(f"Dedup drop: {tid[:70]}")
                continue
            seen.add(tid)
            unique.append(row)
        return unique

    # =========================================================================
    # STEP 7 — Standard output format  (MUST override)
    # =========================================================================

    @abstractmethod
    def to_standard_format(self, row: dict) -> TenderResult:
        """
        Map an enriched row dict to the canonical TenderResult format.

        Minimum implementation:
            return make_tender_result(
                title  = row.get("title", ""),
                url    = row.get("url", self.SOURCE_URL),
                source = self.SOURCE_NAME,
                **row,
            )
        """

    # =========================================================================
    # STEP 8 — Lifecycle hooks  (optional)
    # =========================================================================

    def on_run_start(self) -> None:
        """Called at the start of run(), before fetch_data(). Override for setup."""

    def on_run_end(self, all_rows: list[dict]) -> None:
        """
        Called at the end of run(), after monitoring.
        Override to save portal-specific Excel output, update dashboards, etc.
        """

    def on_filter_complete(
        self,
        scraped_total: int,
        accepted:      list[dict],
        rejected:      list[dict],
        reasons:       list[str],
    ) -> None:
        """
        Called immediately after Step 4 quality filter, before enrich_fields().

        Override to capture rejected rows for portal-specific debug summaries
        (e.g. UNGM's Task 6 validation output).  Default: no-op.

        Parameters
        ----------
        scraped_total : total rows before filter
        accepted      : rows that passed all quality gates
        rejected      : rows that failed (same order as reasons)
        reasons       : human-readable rejection reason per rejected row
        """

    # =========================================================================
    # MAIN PIPELINE  (do not override — override individual steps instead)
    # =========================================================================

    def run(self, debug: bool = False) -> tuple[list[TenderResult], list[dict]]:
        """
        Execute the full 8-step intelligence pipeline.

        Returns:
            new_tenders — list[TenderResult] for notification (new tenders only)
            all_rows    — list[dict] for Excel/export (all accepted rows)
        """
        self._debug = debug or self._debug or _DEBUG

        portal = self.SOURCE_NAME or self.__class__.__name__
        print(f"\n{'='*65}")
        print(f"[{portal}] Pipeline starting…")
        if self._debug:
            print(f"[{portal}] DEBUG MODE ACTIVE")
        print(f"{'='*65}")

        self.on_run_start()

        # ── Step 1: Fetch ─────────────────────────────────────────────────────
        try:
            raw_data = self.fetch_data()
        except Exception as exc:
            print(f"[{portal}] FATAL — fetch_data() failed: {exc}")
            return [], []

        if raw_data is None:
            print(f"[{portal}] fetch_data() returned None — no data available")
            self._monitor.check_structure_change(0, raw_data=None)
            return [], []

        # ── Step 2: Schema validation ─────────────────────────────────────────
        try:
            self.validate_schema(raw_data)
        except Exception as exc:
            print(f"[{portal}] WARNING — validate_schema() error: {exc} (continuing)")

        # ── Step 3: Extract rows ──────────────────────────────────────────────
        try:
            raw_rows = self.extract_rows(raw_data)
        except Exception as exc:
            print(f"[{portal}] FATAL — extract_rows() failed: {exc}")
            self._monitor.check_structure_change(0, raw_data)
            return [], []

        scraped_total = len(raw_rows)
        print(f"[{portal}] Extracted {scraped_total} raw rows")

        # ── Step 4: Quality filter + score (shared) ────────────────────────────
        accepted, rejected, reasons = apply_intelligence_filter(
            raw_rows, threshold=self.QUALITY_THRESHOLD
        )
        print(f"[{portal}] Quality filter: "
              f"{len(accepted)}/{scraped_total} passed")

        # ── Step 4b: Filter-complete hook (portal can capture rejected rows) ───
        try:
            self.on_filter_complete(scraped_total, accepted, rejected, reasons)
        except Exception as exc:
            print(f"[{portal}] WARNING — on_filter_complete() error: {exc}")

        # ── Step 5: Enrich (portal hook, runs after filter) ────────────────────
        try:
            accepted = self.enrich_fields(accepted)
        except Exception as exc:
            print(f"[{portal}] WARNING — enrich_fields() error: {exc} (continuing)")

        # ── Step 6: Deduplicate ────────────────────────────────────────────────
        all_rows = self.deduplicate(accepted)

        # ── Step 7: DB check + build new_tenders list ─────────────────────────
        new_tenders: list[TenderResult] = []
        for row in all_rows:
            tid = self.get_tender_id(row)
            try:
                if check_if_new(tid):
                    title = str(
                        row.get("title") or row.get("Title")
                        or row.get("Description") or ""
                    )
                    url   = str(
                        row.get("url") or row.get("detail_url")
                        or row.get("Detail Link") or self.SOURCE_URL
                    )
                    mark_as_seen(
                        tender_id   = tid,
                        title       = title[:300],
                        source_site = self.SOURCE_NAME,
                        url         = url,
                    )
                    std = self.to_standard_format(row)
                    new_tenders.append(std)
            except Exception as exc:
                self._log.warning(f"DB dedup error for {tid[:60]}: {exc}")

        # ── Step 8: Monitoring ────────────────────────────────────────────────
        self._monitor.run_quality_monitoring(
            scraped_total, all_rows, reasons, raw_data
        )
        self._monitor.check_structure_change(len(all_rows), raw_data)

        # ── Debug metrics ─────────────────────────────────────────────────────
        if self._debug:
            self._print_debug(scraped_total, all_rows, rejected)

        # ── Lifecycle hook: Excel / post-processing ───────────────────────────
        try:
            self.on_run_end(all_rows)
        except Exception as exc:
            print(f"[{portal}] WARNING — on_run_end() error: {exc}")

        print(f"[{portal}] Done — {len(all_rows)} rows, "
              f"{len(new_tenders)} NEW")
        return new_tenders, all_rows

    # =========================================================================
    # Debug output
    # =========================================================================

    def _print_debug(
        self,
        scraped_total: int,
        accepted_rows: list[dict],
        rejected_rows: list[dict],
    ) -> None:
        portal = self.SOURCE_NAME or self.__class__.__name__

        scores      = [int(r.get("quality_score")     or r.get("Quality Score")     or 0) for r in accepted_rows]
        raw_scores  = [int(r.get("raw_quality_score") or r.get("Raw Quality Score") or r.get("quality_score") or 0) for r in accepted_rows]
        confidences = [float(r.get("consulting_confidence") or 0.0) for r in accepted_rows]

        avg_qs   = sum(scores)      / len(scores)      if scores      else 0.0
        avg_raw  = sum(raw_scores)  / len(raw_scores)  if raw_scores  else 0.0
        avg_conf = sum(confidences) / len(confidences) if confidences else 0.0

        # Decision tier breakdown
        tier_counts: dict[str, int] = {
            "BID_NOW": 0, "STRONG_CONSIDER": 0, "WEAK_CONSIDER": 0, "IGNORE": 0
        }
        for r in accepted_rows:
            tier = r.get("decision_tag") or r.get("Decision") or "IGNORE"
            if tier not in tier_counts:
                tier = "IGNORE"
            tier_counts[tier] += 1

        ctype_counts: dict[str, int] = {}
        for r in accepted_rows:
            ct = r.get("consulting_type") or r.get("Consulting Type") or "Unknown"
            ctype_counts[ct] = ctype_counts.get(ct, 0) + 1

        print(f"\n[{portal}/debug] ══ Pipeline Metrics ════════════════════════")
        print(f"[{portal}/debug]   Scraped (pre-filter)       : {scraped_total}")
        print(f"[{portal}/debug]   Rejected                   : {len(rejected_rows)}")
        print(f"[{portal}/debug]   Accepted                   : {len(accepted_rows)}")
        print(f"[{portal}/debug]   Avg raw score              : {avg_raw:.1f}")
        print(f"[{portal}/debug]   Avg confidence             : {avg_conf:.2f}  ({avg_conf*100:.0f}%)")
        print(f"[{portal}/debug]   Avg quality score (final)  : {avg_qs:.1f}")
        print(f"[{portal}/debug]   ── Decision tier breakdown ─────────────────")
        print(f"[{portal}/debug]   🔥 BID_NOW         : {tier_counts['BID_NOW']}")
        print(f"[{portal}/debug]   ⭐ STRONG_CONSIDER  : {tier_counts['STRONG_CONSIDER']}")
        print(f"[{portal}/debug]   📌 WEAK_CONSIDER    : {tier_counts['WEAK_CONSIDER']}")
        print(f"[{portal}/debug]   🔇 IGNORE           : {tier_counts['IGNORE']}")
        if ctype_counts:
            print(f"[{portal}/debug]   ── Consulting types ────────────────────────")
            for ct, n in sorted(ctype_counts.items(), key=lambda x: -x[1]):
                print(f"[{portal}/debug]     {ct:<28} : {n}")
        # Top 3 BID_NOW first, then fill from highest score
        bid_now_rows = sorted(
            [r for r in accepted_rows if (r.get("decision_tag") or "") == "BID_NOW"],
            key=lambda r: -(int(r.get("quality_score") or r.get("Quality Score") or 0)),
        )
        if bid_now_rows:
            print(f"[{portal}/debug]   ── Top 3 BID NOW tenders ───────────────────")
            for i, r in enumerate(bid_now_rows[:3], 1):
                qs   = r.get("quality_score") or r.get("Quality Score") or 0
                raw  = r.get("raw_quality_score") or r.get("Raw Quality Score") or qs
                conf = r.get("consulting_confidence") or 0.0
                ct   = r.get("consulting_type") or r.get("Consulting Type") or "?"
                ttl  = (r.get("Description") or r.get("title") or r.get("Title") or "")[:45]
                print(f"[{portal}/debug]     #{i}  raw={raw:>3}×{conf:.2f}=QS{qs:>3}  "
                      f"{ct:<22}  {ttl}")
        elif accepted_rows:
            top3 = sorted(
                accepted_rows,
                key=lambda r: -(int(r.get("quality_score") or r.get("Quality Score") or 0)),
            )[:3]
            print(f"[{portal}/debug]   ── Top 3 rows by score ─────────────────────")
            for i, r in enumerate(top3, 1):
                qs   = r.get("quality_score") or r.get("Quality Score") or 0
                raw  = r.get("raw_quality_score") or r.get("Raw Quality Score") or qs
                conf = r.get("consulting_confidence") or 0.0
                ct   = r.get("consulting_type") or r.get("Consulting Type") or "?"
                tier = r.get("decision_tag") or "?"
                ttl  = (r.get("Description") or r.get("title") or r.get("Title") or "")[:45]
                print(f"[{portal}/debug]     #{i}  raw={raw:>3}×{conf:.2f}=QS{qs:>3}  "
                      f"[{tier}]  {ct:<18}  {ttl}")
        print(f"[{portal}/debug] ═══════════════════════════════════════════════")


# =============================================================================
# Helper utilities
# =============================================================================

def _to_sample(data: Any, size: int = 20) -> list[dict]:
    """
    Extract a list of up to `size` record dicts from any raw-data shape:
      - dict-of-dicts  (WB API: {"projects": {"P001": {...}, ...}})
      - list-of-dicts  (most HTML scrapers)
      - dict with a 'results' / 'data' / 'items' key
      - plain dict treated as a single record
    """
    if isinstance(data, list):
        return [r for r in data[:size] if isinstance(r, dict)]
    if isinstance(data, dict):
        # Try common wrapper keys
        for key in ("results", "data", "items", "tenders", "projects",
                    "documents", "opportunities"):
            val = data.get(key)
            if isinstance(val, list):
                return [r for r in val[:size] if isinstance(r, dict)]
            if isinstance(val, dict):
                return list(val.values())[:size]
        # If values are dicts it IS a dict-of-records
        vals = list(data.values())
        if vals and isinstance(vals[0], dict):
            return vals[:size]
        # Last resort: treat the dict itself as a single record
        return [data]
    return []
