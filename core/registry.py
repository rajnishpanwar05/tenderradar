# =============================================================================
# core/registry.py — Plugin-Based Scraper Registry  (Phase 4)
#
# ADDING A NEW PORTAL (zero-touch workflow):
#   1. Create  scrapers/portals/{name}_scraper.py
#   2. Define  SCRAPER_META  at the top of that file (see format below)
#   3. Add the flag to  config/enabled_portals.json  (or omit — defaults True)
#   That's it.  No other file needs touching.
#
# SCRAPER_META format (put this near the top of any new scraper):
#   SCRAPER_META = {
#       "flag":        "my_portal",        # CLI flag, unique, lowercase
#       "label":       "My Portal Name",   # human-readable
#       "group":       "requests",         # api | requests | selenium | captcha
#       "timeout":     120,                # per-run timeout in seconds
#       "max_retries": 1,                  # retries on crash / zero-row result
#       "auto":        True,               # False = explicit --portal only
#   }
#
# ENABLING / DISABLING PORTALS:
#   Edit  config/enabled_portals.json:
#       { "wb": true, "sidbi": false, ... }
#   A disabled portal is excluded from ALL runs (default and explicit).
#   File is auto-created (all enabled) if it does not exist.
#
# BACKWARD COMPATIBILITY:
#   Scrapers that do NOT define SCRAPER_META continue to work via the
#   _STATIC_REGISTRY fallback below.  No scraper modifications required.
#
# PERFORMANCE:
#   Discovery uses a fast text-scan (~4 KB per file) before importing.
#   Only files containing "SCRAPER_META" are imported.
#   Startup overhead: < 30 ms for 50 portals.
#
# ScraperJob fields:
#   flag          CLI flag without "--"
#   module        Fully-qualified Python module (e.g. scrapers.portals.wb)
#   label         Human-readable name for logs and reports
#   group         "api" | "requests" | "selenium" | "captcha"
#   timeout       Per-run timeout in seconds
#   max_retries   Retries on crash or zero-row result (default 2)
#   auto          Included in default all-portal run (False = explicit only)
# =============================================================================

import importlib
import json
import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger("tenderradar.registry")

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE_DIR     = os.path.expanduser("~/tender_system")
_PORTALS_DIR  = os.path.join(_BASE_DIR, "scrapers", "portals")
_PORTALS_PKG  = "scrapers.portals"
_ENABLED_JSON = os.path.join(_BASE_DIR, "config", "enabled_portals.json")


# =============================================================================
# ScraperJob — unchanged dataclass (runner depends on this exact shape)
# =============================================================================

@dataclass
class ScraperJob:
    flag:        str
    module:      str
    label:       str
    group:       str        # "api" | "requests" | "selenium" | "captcha"
    timeout:     int        # seconds
    max_retries: int  = 2
    auto:        bool = True   # included in default run?

    @property
    def needs_captcha(self) -> bool:
        return self.group == "captcha"


# =============================================================================
# SECTION 1 — Static fallback registry
#
# Existing scrapers without SCRAPER_META are covered here.
# New scrapers should use SCRAPER_META instead.
# Ordered fast → slow so the parallel pool finishes evenly.
# =============================================================================

_STATIC_REGISTRY: List[ScraperJob] = [

    # ── API-based (fastest — pure JSON) ──────────────────────────────────────
    ScraperJob("wb",       "scrapers.portals.worldbank_scraper",       "World Bank",         "api",      timeout=720, max_retries=1),
    ScraperJob("ted",      "scrapers.portals.ted_scraper",             "TED EU",             "api",      timeout=180),
    ScraperJob("sam",      "scrapers.portals.sam_scraper",             "SAM.gov",            "api",      timeout=240),
    ScraperJob("afdb",     "scrapers.portals.afdb_scraper",            "AfDB Consultants",   "api",      timeout=120),
    ScraperJob("afd",      "scrapers.portals.afd_scraper",             "AFD France",         "requests", timeout=420, max_retries=1),
    ScraperJob("ungm",     "scrapers.portals.ungm_scraper",            "UNGM",               "selenium", timeout=600, max_retries=1),
    ScraperJob("usaid",    "scrapers.portals.usaid_scraper",           "USAID",              "api",      timeout=120),

    # ── requests + BeautifulSoup (medium speed) ───────────────────────────────
    ScraperJob("gem",      "scrapers.portals.gem_scraper",             "GeM BidPlus",        "requests", timeout=300),
    ScraperJob("devnet",   "scrapers.portals.devnet_scraper",          "DevNet India",       "requests", timeout=180),
    ScraperJob("cg",       "scrapers.portals.cg_scraper",              "CG eProcurement",    "requests", timeout=300),
    ScraperJob("undp",     "scrapers.portals.undp_scraper",            "UNDP Procurement",   "requests", timeout=240),
    ScraperJob("meghalaya","scrapers.portals.meghalaya_scraper",       "Meghalaya MBDA",     "requests", timeout=5400),
    ScraperJob("iucn",     "scrapers.portals.iucn_scraper",            "IUCN Procurement",   "requests", timeout=120),
    ScraperJob("sidbi",    "scrapers.portals.sidbi_scraper",           "SIDBI Tenders",      "requests", timeout=180),
    ScraperJob("icfre",    "scrapers.portals.icfre_scraper",           "ICFRE Tenders",      "requests", timeout=120),
    ScraperJob("phfi",     "scrapers.portals.phfi_scraper",            "PHFI Tenders",       "requests", timeout=60,  max_retries=0),
    ScraperJob("jtds",        "scrapers.portals.jtds_scraper",          "JTDS Jharkhand",     "requests", timeout=120),
    ScraperJob("maharashtra", "scrapers.portals.maharashtra_scraper",  "Maharashtra Tenders","requests", timeout=600),
    # up_scraper.py defines SCRAPER_META with flag="upetender" — auto-discovered.
    # Static entry removed to prevent double-running the same scraper.
    ScraperJob("taneps",   "scrapers.portals.taneps_scraper",          "TANEPS Tanzania",    "requests", timeout=120),

    # ── Selenium / headless Chrome (slowest) ──────────────────────────────────
    ScraperJob("giz",      "scrapers.portals.giz_scraper",             "GIZ India",          "selenium", timeout=300),
    ScraperJob("ngobox",   "scrapers.portals.ngobox_scraper",          "NGO Box",            "selenium", timeout=300),
    ScraperJob("whh",      "scrapers.portals.welthungerhilfe_scraper", "Welthungerhilfe",    "requests", timeout=60),
    ScraperJob("karnataka","scrapers.portals.karnataka_scraper",       "Karnataka eProcure", "requests", timeout=300),
    ScraperJob("dtvp",     "scrapers.portals.dtvp_scraper",            "DTVP Germany",       "selenium", timeout=240),

    # ── Manual CAPTCHA (excluded from default run, always sequential) ─────────
    ScraperJob("sikkim",   "scrapers.portals.sikkim_scraper",          "Sikkim eProcure",    "captcha",  timeout=600, max_retries=0, auto=False),
    ScraperJob("nic",      "scrapers.portals.nic_states_scraper",      "NIC State Portals",  "captcha",  timeout=900, max_retries=0, auto=False),
]


# =============================================================================
# SECTION 2 — Auto-discovery: scan scrapers/portals/ for SCRAPER_META
# =============================================================================

def _quick_has_meta(filepath: str) -> bool:
    """
    Fast text probe — read first 4 KB of a scraper file and check for the
    string 'SCRAPER_META'.  Avoids importing files that don't define it.
    Keeps startup overhead well under 30 ms regardless of portal count.
    """
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as fh:
            return "SCRAPER_META" in fh.read(4096)
    except OSError:
        return False


def _discover_scrapers() -> List[ScraperJob]:
    """
    Scan scrapers/portals/ for *_scraper.py files containing SCRAPER_META.
    Returns ScraperJob objects built from the metadata dicts.

    Files that pass the text probe but fail to import are logged as warnings
    and skipped — they never crash the registry build.
    """
    discovered: List[ScraperJob] = []

    if not os.path.isdir(_PORTALS_DIR):
        logger.debug("[registry] Portals directory not found: %s", _PORTALS_DIR)
        return discovered

    for filename in sorted(os.listdir(_PORTALS_DIR)):
        if not filename.endswith("_scraper.py"):
            continue

        filepath = os.path.join(_PORTALS_DIR, filename)
        if not _quick_has_meta(filepath):
            continue   # skip — no SCRAPER_META declaration

        modname = f"{_PORTALS_PKG}.{filename[:-3]}"   # strip .py
        try:
            mod  = importlib.import_module(modname)
            meta = getattr(mod, "SCRAPER_META", None)

            if not isinstance(meta, dict) or "flag" not in meta:
                logger.debug(
                    "[registry] %s has SCRAPER_META but is not a valid dict — skipped",
                    modname,
                )
                continue

            group = meta.get("group", "requests")
            if group not in ("api", "requests", "selenium", "captcha"):
                logger.warning(
                    "[registry] %s: unknown group '%s' — defaulting to 'requests'",
                    modname, group,
                )
                group = "requests"

            job = ScraperJob(
                flag        = str(meta["flag"]).lower().strip(),
                module      = modname,
                label       = str(meta.get("label", meta["flag"])),
                group       = group,
                timeout     = int(meta.get("timeout",     120)),
                max_retries = int(meta.get("max_retries",   1)),
                auto        = bool(meta.get("auto",        True)),
            )
            discovered.append(job)
            logger.debug("[registry] Auto-discovered: %s (%s)", job.flag, modname)

        except Exception as exc:
            logger.warning(
                "[registry] Could not load '%s' during discovery: %s", modname, exc
            )

    if discovered:
        logger.info(
            "[registry] Auto-discovered %d portal(s) via SCRAPER_META",
            len(discovered),
        )
    return discovered


# =============================================================================
# SECTION 3 — enabled_portals.json  (toggle system)
# =============================================================================

def load_enabled_portals(all_flags: Optional[List[str]] = None) -> Dict[str, bool]:
    """
    Load config/enabled_portals.json and return a {flag: bool} map.

    Rules:
      • Any flag absent from the JSON defaults to True (enabled).
      • If the file doesn't exist it is created with all portals enabled.
      • JSON parse errors → all portals treated as enabled (fail open).

    Parameters
    ----------
    all_flags : optional list of known flags used when auto-creating the file
    """
    try:
        with open(_ENABLED_JSON, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            raise ValueError("enabled_portals.json must be a JSON object")
        return {str(k).lower(): bool(v) for k, v in data.items()}

    except FileNotFoundError:
        defaults: Dict[str, bool] = (
            {f: True for f in sorted(all_flags)} if all_flags else {}
        )
        try:
            os.makedirs(os.path.dirname(_ENABLED_JSON), exist_ok=True)
            with open(_ENABLED_JSON, "w", encoding="utf-8") as fh:
                json.dump(defaults, fh, indent=2)
            logger.info("[registry] Created %s (all portals enabled)", _ENABLED_JSON)
        except OSError as exc:
            logger.warning(
                "[registry] Could not create enabled_portals.json: %s", exc
            )
        return defaults

    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(
            "[registry] enabled_portals.json parse error (%s) — "
            "treating all portals as enabled",
            exc,
        )
        return {}


def save_enabled_portals(enabled: Dict[str, bool]) -> None:
    """
    Persist an updated {flag: bool} map to enabled_portals.json.
    Raises OSError on write failure.
    """
    os.makedirs(os.path.dirname(_ENABLED_JSON), exist_ok=True)
    with open(_ENABLED_JSON, "w", encoding="utf-8") as fh:
        json.dump({k: enabled[k] for k in sorted(enabled)}, fh, indent=2)


# =============================================================================
# SECTION 4 — Registry build  (static + discovered + enabled filter)
# =============================================================================

def _build_registry() -> List[ScraperJob]:
    """
    Build the live, filtered registry used by the runner.

    Algorithm
    ---------
    1. Start with _STATIC_REGISTRY (existing scrapers without SCRAPER_META).
    2. Run auto-discovery — scrapers with SCRAPER_META *override* matching
       static entries (module-defined metadata is the authoritative source).
    3. Append newly discovered portals not present in the static list.
    4. Load enabled_portals.json — drop disabled entries entirely.
    5. Preserve ordering: static order first, new portals at the end.
    """
    # Step 1: index static entries by flag
    static_by_flag: Dict[str, ScraperJob] = {j.flag: j for j in _STATIC_REGISTRY}
    static_order:   List[str]             = [j.flag for j in _STATIC_REGISTRY]

    # Steps 2–3: merge discovered, track new flags
    registry:   Dict[str, ScraperJob] = dict(static_by_flag)
    new_flags:  List[str]             = []
    for job in _discover_scrapers():
        if job.flag not in registry:
            new_flags.append(job.flag)
        registry[job.flag] = job   # meta always wins over static entry

    # Step 4: load enabled map (auto-creates JSON with all flags if absent)
    all_flags = static_order + new_flags
    enabled   = load_enabled_portals(all_flags)

    # Step 5: build ordered, filtered list
    active: List[ScraperJob] = []
    disabled_count = 0
    for flag in all_flags:
        if flag not in registry:
            continue
        if enabled.get(flag, True):       # absent key → enabled by default
            active.append(registry[flag])
        else:
            disabled_count += 1
            logger.debug("[registry] Portal '%s' disabled — excluded", flag)

    logger.info(
        "[registry] Built: %d active portal(s)  |  %d static  |  "
        "%d auto-discovered  |  %d disabled",
        len(active),
        len([f for f in static_order if f in {j.flag for j in active}]),
        len(new_flags),
        disabled_count,
    )
    return active


# ── Build once at import time ─────────────────────────────────────────────────
_REGISTRY: List[ScraperJob] = _build_registry()


# =============================================================================
# SECTION 5 — Public API  (identical surface to Phase 3 registry)
# =============================================================================

def all_jobs() -> List[ScraperJob]:
    """Return all enabled jobs (static + auto-discovered, filtered)."""
    return list(_REGISTRY)


def auto_jobs() -> List[ScraperJob]:
    """Jobs included in the default (no explicit flag) invocation."""
    return [j for j in _REGISTRY if j.auto]


def get_job(flag: str) -> Optional[ScraperJob]:
    """
    Look up one job by its CLI flag.
    Returns None if the flag is unknown or the portal is disabled.
    """
    flag = flag.lower().lstrip("-")
    for j in _REGISTRY:
        if j.flag == flag:
            return j
    return None


def jobs_for_flags(flags: List[str]) -> List[ScraperJob]:
    """
    Return ScraperJob list for the given flags, in registry order.
    Unknown or disabled flags are silently ignored.
    """
    flag_set = {f.lower().lstrip("-") for f in flags}
    return [j for j in _REGISTRY if j.flag in flag_set]


def resolve_run_list(args) -> List[ScraperJob]:
    """
    Determine the jobs to run from a parsed argparse Namespace.

    Priority order
    --------------
    1. ``--portal <flag>``  single-portal debug mode  (new in Phase 4)
    2. ``--<flag>``         legacy per-portal flags   (backward compatible)
    3. No flags             → run all auto=True jobs
    """
    # ── Priority 1: --portal <flag> ───────────────────────────────────────
    portal_flag = getattr(args, "portal", None)
    if portal_flag:
        flag = str(portal_flag).lower().lstrip("-")
        job  = get_job(flag)
        if job:
            return [job]
        valid = ", ".join(j.flag for j in _REGISTRY)
        raise SystemExit(
            f"[registry] Unknown or disabled portal: '{flag}'\n"
            f"Valid flags : {valid}\n"
            f"Tip         : python3 scripts/list_portals.py"
        )

    # ── Priority 2: legacy --<flag> explicit flags ─────────────────────────
    explicit: List[str] = []
    for j in _REGISTRY:
        attr = j.flag.replace("-", "_")
        if getattr(args, attr, False):
            explicit.append(j.flag)

    if explicit:
        return jobs_for_flags(explicit)

    # ── Priority 3: default all-portals run ───────────────────────────────
    return auto_jobs()


# =============================================================================
# SECTION 6 — Portal info helper  (used by scripts/list_portals.py)
# =============================================================================

def portal_info_table() -> List[Dict]:
    """
    Return a list of dicts describing EVERY known portal — enabled and disabled.

    Each dict contains:
        flag, label, group, auto, enabled, source
    where source is 'meta' (auto-discovered via SCRAPER_META) or 'static'.

    Used by scripts/list_portals.py to print the full portal inventory
    without importing every scraper a second time.
    """
    static_by_flag: Dict[str, ScraperJob] = {j.flag: j for j in _STATIC_REGISTRY}
    discovered:     List[ScraperJob]      = _discover_scrapers()
    merged:         Dict[str, ScraperJob] = dict(static_by_flag)
    for job in discovered:
        merged[job.flag] = job

    all_flags   = [j.flag for j in _STATIC_REGISTRY] + [
        j.flag for j in discovered if j.flag not in static_by_flag
    ]
    enabled_map  = load_enabled_portals(all_flags)
    meta_flags   = {j.flag for j in discovered}

    rows = []
    for flag in all_flags:
        job = merged[flag]
        rows.append({
            "flag":    flag,
            "label":   job.label,
            "group":   job.group,
            "auto":    job.auto,
            "enabled": enabled_map.get(flag, True),
            "source":  "meta" if flag in meta_flags else "static",
        })
    return rows
