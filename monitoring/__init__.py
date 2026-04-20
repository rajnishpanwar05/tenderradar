# monitoring/__init__.py — re-export key symbols for backward compatibility
from monitoring.logs         import setup_logging   # noqa: F401
from monitoring.health_report import get_broken_scrapers, get_health_summary, log_scraper_health  # noqa: F401
