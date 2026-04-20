# =============================================================================
# config.py — Central configuration for the Tender Monitoring System
#
# Credentials are loaded from .env (never hardcoded here).
# Fallback defaults are used only for non-sensitive settings.
# .env must NOT be committed to version control — see .gitignore.
# =============================================================================

import os


def _env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return default


def _env_bool(*names: str, default: bool) -> bool:
    raw = _env_first(*names, default="")
    if raw == "":
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


def _env_int(*names: str, default: int) -> int:
    raw = _env_first(*names, default=str(default))
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return default

# ── Manual .env loader (python-dotenv not installed) ─────────────────────────
def _load_env(path: str = None) -> str:
    """
    Parse KEY=VALUE lines from .env and set them as environment variables.
    - Ignores blank lines and lines starting with #
    - Does NOT override variables already set in the environment
    - Strips inline comments (# ...) after the value
    - Handles values with = signs (only splits on the first =)

    Search order (first found wins):
      1. Explicit path argument
      2. ~/tender_system/.env  (project root — most intuitive)
      3. config/.env           (legacy fallback)
    """
    if path is None:
        # Project root is one directory up from this config/ directory
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        root_env     = os.path.join(project_root, ".env")
        config_env   = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        path = root_env if os.path.exists(root_env) else config_env
    if not os.path.exists(path):
        return ""
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, raw_val = line.partition("=")
            key = key.strip()
            # Strip inline comment
            val = raw_val.split("#")[0].strip()
            if key and key not in os.environ:
                os.environ[key] = val
    return path


LOADED_ENV_FILE = _load_env()

# ── MySQL ─────────────────────────────────────────────────────────────────────
DB_HOST      = _env_first("DB_HOST", default="127.0.0.1")
DB_PORT_RAW  = _env_first("DB_PORT", default="3306")
DB_PORT      = _env_int("DB_PORT", default=3306)
DB_USER      = _env_first("DB_USER", default="root")
DB_PASSWORD  = _env_first("DB_PASSWORD", "DB_PASS")
DB_PASS      = DB_PASSWORD  # legacy alias kept for existing imports
DB_NAME      = _env_first("DB_NAME", default="tender_monitor")
# Pool size per process — keep small when running multiple uvicorn workers.
# 4 workers × 10 = 40 connections; well within MySQL's default max_connections=151.
DB_POOL_SIZE = _env_int("DB_POOL_SIZE", default=10)

# ── API Authentication ────────────────────────────────────────────────────────
API_SECRET_KEY = os.environ.get("API_SECRET_KEY", "")

# ── Telegram group bot ────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── Email notifications (primary delivery) ───────────────────────────────────
SMTP_HOST = _env_first("SMTP_HOST")
SMTP_PORT = _env_int("SMTP_PORT", default=587)
SMTP_USERNAME = _env_first("SMTP_USERNAME", "SMTP_USER")
SMTP_PASSWORD = _env_first("SMTP_PASSWORD", "SMTP_PASS")
SMTP_USER = SMTP_USERNAME   # legacy alias kept for existing imports
SMTP_PASS = SMTP_PASSWORD   # legacy alias kept for existing imports
SMTP_USE_TLS = _env_bool("SMTP_USE_TLS", default=True)
SMTP_FROM_EMAIL = _env_first("SMTP_FROM_EMAIL", "EMAIL_FROM", default=SMTP_USERNAME or "")
SMTP_FROM_NAME = _env_first("SMTP_FROM_NAME", default="TenderRadar")
EMAIL_FROM = SMTP_FROM_EMAIL  # legacy alias kept for existing imports
NOTIFY_EMAIL_TO = [e.strip() for e in _env_first("NOTIFY_EMAIL_TO", "EMAIL_TO").split(",") if e.strip()]
EMAIL_TO = NOTIFY_EMAIL_TO    # legacy alias kept for existing imports
EMAIL_SUBJECT_PREFIX = _env_first("EMAIL_SUBJECT_PREFIX", default="TenderRadar")
AUTO_DAILY_DIGEST = _env_bool("AUTO_DAILY_DIGEST", default=False)
DAILY_DIGEST_DRY_RUN = _env_bool("DAILY_DIGEST_DRY_RUN", default=True)
DAILY_DIGEST_ATTACH_PACKAGES = _env_bool("DAILY_DIGEST_ATTACH_PACKAGES", default=True)
DAILY_DIGEST_MAX_PACKAGES = int(_env_first("DAILY_DIGEST_MAX_PACKAGES", default="5"))

# ── WhatsApp via Twilio ───────────────────────────────────────────────────────
TWILIO_ACCOUNT_SID   = os.environ.get("TWILIO_ACCOUNT_SID",   "YOUR_ACCOUNT_SID")
TWILIO_AUTH_TOKEN    = os.environ.get("TWILIO_AUTH_TOKEN",    "YOUR_AUTH_TOKEN")
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
WHATSAPP_NUMBERS     = [n.strip() for n in
                        os.environ.get("WHATSAPP_NUMBERS", "+918570910083").split(",")
                        if n.strip()]

# ── Output paths ──────────────────────────────────────────────────────────────
BASE_DIR          = os.path.expanduser("~/tender_system")
OUTPUT_DIR        = os.path.join(BASE_DIR, "output")
PORTAL_EXCELS_DIR = os.path.join(OUTPUT_DIR, "portal_excels")

# Per-portal Excel files — ALL written to portal_excels/ (never to output/ root).
# Each pipeline writes its own rich, portal-specific Excel via save_xxx_excel().
# Filenames here must match what each pipeline passes to openpyxl / xlsxwriter.
def _pe(name): return os.path.join(PORTAL_EXCELS_DIR, name)

WB_EXCEL_PATH        = _pe("WorldBank_Tenders_Master.xlsx")
WB_EARLY_EXCEL_PATH  = WB_EXCEL_PATH   # Early Pipeline is a sheet inside the same WB Excel
GEM_EXCEL_PATH       = _pe("GeM_BidPlus_Tenders_Master.xlsx")
DEVNET_EXCEL_PATH    = _pe("DevNet_India_Tenders_Master.xlsx")
CG_EXCEL_PATH        = _pe("CG_eProcurement_Tenders_Master.xlsx")
GIZ_EXCEL_PATH       = _pe("GIZ_India_Tenders_Master.xlsx")
SIKKIM_EXCEL_PATH    = _pe("Sikkim_Tenders_Master.xlsx")
UNDP_EXCEL_PATH      = _pe("UNDP_Procurement_Tenders_Master.xlsx")
MEGHALAYA_EXCEL_PATH = _pe("Meghalaya_MBDA_Tenders_Master.xlsx")
NGOBOX_EXCEL_PATH    = _pe("NGO_Box_Tenders_Master.xlsx")
IUCN_EXCEL_PATH      = _pe("IUCN_Procurement_Tenders_Master.xlsx")
WHH_EXCEL_PATH       = _pe("Welthungerhilfe_Tenders_Master.xlsx")
UNGM_EXCEL_PATH      = _pe("UNGM_Tenders_Master.xlsx")
SIDBI_EXCEL_PATH     = _pe("SIDBI_Tenders_Master.xlsx")
NIC_EXCEL_PATH       = _pe("NIC_States_Tenders_Master.xlsx")
SAM_EXCEL_PATH       = _pe("SAM_Tenders_Master.xlsx")
KARNATAKA_EXCEL_PATH = _pe("Karnataka_eProcure_Tenders_Master.xlsx")
USAID_EXCEL_PATH     = _pe("USAID_Tenders_Master.xlsx")
DTVP_EXCEL_PATH      = _pe("DTVP_Germany_Tenders_Master.xlsx")
TANEPS_EXCEL_PATH    = _pe("TANEPS_Tanzania_Tenders_Master.xlsx")
AFDB_EXCEL_PATH      = _pe("AfDB_Consultants_Tenders_Master.xlsx")
AFD_EXCEL_PATH       = _pe("AFD_France_Tenders_Master.xlsx")
ICFRE_EXCEL_PATH     = _pe("ICFRE_Tenders_Master.xlsx")
JTDS_EXCEL_PATH      = _pe("JTDS_Jharkhand_Tenders_Master.xlsx")
PHFI_EXCEL_PATH        = _pe("PHFI_Tenders_Master.xlsx")
DEVBUSINESS_EXCEL_PATH = _pe("DevBusiness_UN_Tenders_Master.xlsx")

# Unified master — stays at output/ root (single entry point for the dashboard)
UNIFIED_EXCEL_PATH = os.path.join(OUTPUT_DIR, "Tender_Monitor_Master.xlsx")
OPPORTUNITY_SIGNALS_EXCEL_PATH = os.path.join(OUTPUT_DIR, "Opportunity_Signals.xlsx")

# Dashboard intelligence file — BCG/Bain level strategic overview
DASHBOARD_EXCEL_PATH = os.path.join(OUTPUT_DIR, "IDCG_TenderRadar_Dashboard.xlsx")

LOG_FILE = os.path.join(BASE_DIR, "run.log")

# ── Notifications toggle ──────────────────────────────────────────────────────
# Toggle via .env: NOTIFICATIONS_ENABLED=false  (default: true)
NOTIFICATIONS_ENABLED = os.getenv("NOTIFICATIONS_ENABLED", "true").strip().lower() not in ("false", "0", "no", "off")

# ── API keys ──────────────────────────────────────────────────────────────────
# SAM.gov: free key at https://open.gsa.gov/api/get-started/
# OpenAI:  https://platform.openai.com/api-keys
SAM_API_KEY    = os.environ.get("SAM_API_KEY",    "YOUR_SAM_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "YOUR_OPENAI_API_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
EC_API_KEY     = os.environ.get("EC_API_KEY",     "")

# ── Portal credentials (loaded from .env — NOT hardcoded here) ───────────────

# World Bank eConsultant (wbgeprocure-rfxnow.worldbank.org)
WB_USER = os.environ.get("WB_USER", "")
WB_PASS = os.environ.get("WB_PASS", "")

# UNDP Procurement Notices (procurement-notices.undp.org)
UNDP_USER = os.environ.get("UNDP_USER", "")
UNDP_PASS = os.environ.get("UNDP_PASS", "")

# UNGM — UN Global Marketplace (ungm.org)
UNGM_USER = os.environ.get("UNGM_USER", "")
UNGM_PASS = os.environ.get("UNGM_PASS", "")

# GeM — Government e-Marketplace (gem.gov.in)
GEM_USER = os.environ.get("GEM_USER", "")
GEM_PASS = os.environ.get("GEM_PASS", "")

# SAM.gov — IDCG LLC entity
SAM_LLC_USER   = os.environ.get("SAM_LLC_USER",   "")
SAM_LLC_PASS   = os.environ.get("SAM_LLC_PASS",   "")
SAM_LLC_ENTITY = os.environ.get("SAM_LLC_ENTITY", "")

# SAM.gov — IDCG India entity
SAM_INDIA_USER   = os.environ.get("SAM_INDIA_USER",   "")
SAM_INDIA_PASS   = os.environ.get("SAM_INDIA_PASS",   "")
SAM_INDIA_ENTITY = os.environ.get("SAM_INDIA_ENTITY", "")

# USAID (workwithusaid.org)
USAID_USER = os.environ.get("USAID_USER", "")
USAID_PASS = os.environ.get("USAID_PASS", "")

# DTVP — Deutsches Vergabeportal (en.dtvp.de)
DTVP_USER = os.environ.get("DTVP_USER", "")
DTVP_PASS = os.environ.get("DTVP_PASS", "")

# AfDB — African Development Bank (afdb.org)
AFDB_USER = os.environ.get("AFDB_USER", "")
AFDB_PASS = os.environ.get("AFDB_PASS", "")

# CG eProcurement (eprocure.gov.in)
CG_USER = os.environ.get("CG_USER", "")
CG_PASS = os.environ.get("CG_PASS", "")

# Karnataka eProcurement (eproc.karnataka.gov.in)
KARNATAKA_USER = os.environ.get("KARNATAKA_USER", "")
KARNATAKA_PASS = os.environ.get("KARNATAKA_PASS", "")

# Tanzania NeST (nest.go.tz) — National e-Procurement System
# Register free at https://nest.go.tz/tenderer_registration (TENDERER account)
TANEPS_USER = os.environ.get("TANEPS_USER", "")
TANEPS_PASS = os.environ.get("TANEPS_PASS", "")
NEST_USER   = os.environ.get("NEST_USER",   "")
NEST_PASS   = os.environ.get("NEST_PASS",   "")

# UN Dev Business (devbusiness.un.org)
DEVBUSINESS_USER = os.environ.get("DEVBUSINESS_USER", "")
DEVBUSINESS_PASS = os.environ.get("DEVBUSINESS_PASS", "")

# UNIDO Procurement (procurement.unido.org)
UNIDO_USER = os.environ.get("UNIDO_USER", "")
UNIDO_PASS = os.environ.get("UNIDO_PASS", "")

# IOM (Oracle Cloud procurement)
IOM_USER = os.environ.get("IOM_USER", "")
IOM_PASS = os.environ.get("IOM_PASS", "")

# ── ChromaDB path (local persistent vector store for semantic memory) ─────────
CHROMA_DB_PATH = os.path.join(BASE_DIR, "chroma_db")
CHROMA_DB_PATH = os.environ.get("CHROMA_DB_PATH", CHROMA_DB_PATH)
CHROMA_HOST = os.environ.get("CHROMA_HOST", "")
CHROMA_PORT = _env_int("CHROMA_PORT", default=8000)

# Optional external error tracking (Sentry)
SENTRY_DSN = os.environ.get("SENTRY_DSN", "")
SENTRY_ENVIRONMENT = os.environ.get("SENTRY_ENVIRONMENT", "development")
SENTRY_TRACES_SAMPLE_RATE = float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.0"))

# ── Pipeline settings ─────────────────────────────────────────────────────────
WB_MAX_PROJECTS = 40     # max World Bank projects to scan per run (keep low — PDF scanning is slow)
GEM_MAX_PAGES   = 15     # max GeM listing pages per run (15 × 10 = 150 bids; was 50)
GEM_BID_TYPE    = "service"
GEM_BID_STATUS  = "ongoing_bids"

# Create output dirs if they don't exist
os.makedirs(OUTPUT_DIR,        exist_ok=True)
os.makedirs(PORTAL_EXCELS_DIR, exist_ok=True)


# =============================================================================
# Startup validation — call once at process entry point (main.py, api/app.py)
# =============================================================================


def get_email_config_errors() -> list[str]:
    missing: list[str] = []
    if not SMTP_HOST:
        missing.append("SMTP_HOST")
    if not SMTP_FROM_EMAIL:
        missing.append("SMTP_FROM_EMAIL")
    if not NOTIFY_EMAIL_TO:
        missing.append("NOTIFY_EMAIL_TO")
    if SMTP_USERNAME and not SMTP_PASSWORD:
        missing.append("SMTP_PASSWORD")
    return missing


def email_configured() -> bool:
    return not get_email_config_errors()


def get_db_config() -> dict[str, object]:
    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "database": DB_NAME,
        "env_file": LOADED_ENV_FILE,
    }


def get_db_config_errors() -> list[str]:
    missing: list[str] = []
    if not DB_HOST:
        missing.append("DB_HOST")
    if not str(DB_PORT_RAW).strip():
        missing.append("DB_PORT")
    else:
        try:
            port = int(str(DB_PORT_RAW).strip())
            if port <= 0 or port > 65535:
                missing.append("DB_PORT")
        except ValueError:
            missing.append("DB_PORT")
    if not DB_USER:
        missing.append("DB_USER")
    if not DB_PASSWORD:
        missing.append("DB_PASSWORD")
    if not DB_NAME:
        missing.append("DB_NAME")
    return missing


def log_optional_service_status(logger) -> None:
    if not NOTIFICATIONS_ENABLED:
        print("[TenderRadar] Notifications: DISABLED (NOTIFICATIONS_ENABLED=false)")
        return
    email_missing = get_email_config_errors()
    if email_missing:
        logger.warning(
            "[config] Email notifications disabled: missing %s. "
            "Set the SMTP vars in .env to enable email delivery.",
            ", ".join(email_missing),
        )
        # Print clearly to stdout so it appears even when log level is high
        print(
            f"[TenderRadar] Email: DISABLED — set {', '.join(email_missing)} in .env\n"
            f"  Example for Google Workspace / Gmail:\n"
            f"    SMTP_HOST=smtp.gmail.com\n"
            f"    SMTP_PORT=587\n"
            f"    SMTP_USERNAME=you@yourdomain.com\n"
            f"    SMTP_PASSWORD=<app-password from Google Account → Security → App passwords>\n"
            f"    SMTP_FROM_EMAIL=you@yourdomain.com\n"
            f"    NOTIFY_EMAIL_TO=recipient@yourdomain.com"
        )
    else:
        to_list = ", ".join(NOTIFY_EMAIL_TO)
        dry_label = "DRY-RUN" if DAILY_DIGEST_DRY_RUN else "LIVE"
        auto_label = "AUTO" if AUTO_DAILY_DIGEST else "MANUAL"
        print(
            f"[TenderRadar] Email: ENABLED ({dry_label}) — "
            f"from {SMTP_FROM_EMAIL} → {to_list} "
            f"[digest: {auto_label}]"
        )

def validate(raise_on_error: bool = True) -> list[str]:
    """
    Check that required config values are set and not placeholder strings.

    Args:
        raise_on_error: If True (default), raises RuntimeError on any critical
                        missing value. Set False to get the list without raising.

    Returns:
        List of warning strings (non-critical issues). Empty = all good.

    Raises:
        RuntimeError: If any critical required value is missing.
    """
    import logging
    _vlog = logging.getLogger("tenderradar.config")

    errors   = []   # critical — will raise
    warnings = []   # non-critical — logged only

    # ── Critical: database ────────────────────────────────────────────────────
    db_errors = get_db_config_errors()
    if db_errors:
        errors.append(
            "Database config is incomplete or invalid — set DB_HOST, DB_PORT, "
            "DB_USER, DB_PASSWORD, DB_NAME in .env "
            f"(missing/invalid: {', '.join(db_errors)})"
        )

    # ── Critical: API security ────────────────────────────────────────────────
    if not API_SECRET_KEY:
        errors.append("API_SECRET_KEY is not set — add it to .env to protect the API")

    # ── Warnings: AI features degrade without these ───────────────────────────
    _placeholders = {"YOUR_OPENAI_API_KEY", "YOUR_SAM_API_KEY", ""}
    if OPENAI_API_KEY in _placeholders and not GEMINI_API_KEY:
        warnings.append("Neither OPENAI_API_KEY nor GEMINI_API_KEY is set — AI enrichment will be disabled")
    # ── Report ────────────────────────────────────────────────────────────────
    for w in warnings:
        _vlog.warning("[config] %s", w)

    if errors:
        msg = "Configuration errors — fix .env before starting:\n  " + "\n  ".join(
            f"• {e}" for e in errors
        )
        _vlog.critical("[config] %s", msg)
        if raise_on_error:
            raise RuntimeError(msg)

    return warnings
