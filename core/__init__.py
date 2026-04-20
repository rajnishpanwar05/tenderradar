# core/__init__.py — TenderRadar orchestration package
from core.registry       import ScraperJob, all_jobs, resolve_run_list  # noqa: F401
from core.runner         import JobRunner, JobResult                     # noqa: F401
from core.base_scraper   import IntelligentBaseScraper, set_debug        # noqa: F401
from core.quality_engine import (                                        # noqa: F401
    TenderResult,
    make_tender_result,
    compute_quality_score,
    passes_quality_filter,
    detect_consulting_signals,
    apply_intelligence_filter,
    QUALITY_THRESHOLD,
    CONSULTING_WHITELIST,
)
from core.scraper_monitor import ScraperMonitor                          # noqa: F401
