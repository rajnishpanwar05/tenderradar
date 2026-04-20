# =============================================================================
# api/app.py — TenderRadar FastAPI Application
#
# Launch:
#   python run_api.py                          # dev
#   uvicorn api.app:app --host 0.0.0.0 --port 8000 --workers 4   # prod
#
# Docs:
#   http://localhost:8000/docs    (Swagger UI)
#   http://localhost:8000/redoc   (ReDoc)
# =============================================================================

from __future__ import annotations

import collections
import logging
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from monitoring.sentry import init_sentry

logger = logging.getLogger("tenderradar.api")


# =============================================================================
# Rate limiter — sliding-window, in-memory, zero external dependencies.
#
# Rules (per client IP):
#   /api/v1/search*  and  /api/v1/copilot*  → 30 requests / 60 seconds
#   Everything else under /api/v1            → 200 requests / 60 seconds
#
# In a multi-worker deployment each worker has its own counter (no shared Redis).
# Limits are intentionally generous — this protects against runaway clients,
# not against determined attackers (use a reverse-proxy rate limiter for that).
# =============================================================================

_RATE_WINDOW   = 60          # sliding window in seconds
_RATE_STRICT   = 30          # max requests in window for heavy endpoints
_RATE_STANDARD = 200         # max requests in window for light endpoints

_rate_lock:   threading.Lock = threading.Lock()
_rate_store:  dict[str, collections.deque] = {}   # ip → deque of request timestamps


def _rate_check(ip: str, limit: int) -> bool:
    """
    Return True if request is allowed, False if rate limit exceeded.
    Uses a sliding-window counter (deque of timestamps per IP).
    Thread-safe via a single module-level lock.
    """
    now = time.monotonic()
    cutoff = now - _RATE_WINDOW
    with _rate_lock:
        dq = _rate_store.setdefault(ip, collections.deque())
        # Evict timestamps older than the window
        while dq and dq[0] < cutoff:
            dq.popleft()
        if len(dq) >= limit:
            return False
        dq.append(now)
        return True


# =============================================================================
# Lifespan — startup / shutdown hooks
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Runs once at startup and once at shutdown.
    Startup: initialise DB tables, warm sentence-transformer model.
    Shutdown: nothing required (connections are per-request in mysql-connector).
    """
    logger.info("[api] Starting TenderRadar API...")
    init_sentry(service_name="tenderradar-api")

    # ── Validate configuration — fail fast with clear error ──────────────────
    try:
        from config.config import log_optional_service_status, validate as _validate_config
        warnings = _validate_config(raise_on_error=True)
        if warnings:
            for w in warnings:
                logger.warning("[api] Config warning: %s", w)
        log_optional_service_status(logger)
        logger.info("[api] Configuration validated OK")
    except RuntimeError as exc:
        logger.critical("[api] STARTUP ABORTED — %s", exc)
        raise

    # ── Initialise database tables ────────────────────────────────────────────
    try:
        from database.db import DatabasePreflightError, init_db, preflight_db_connection
        db_status = preflight_db_connection(debug=False)
        if db_status["database_exists"]:
            logger.info(
                "[api] DB preflight OK — MySQL reachable at %s:%s (database '%s' ready)",
                db_status["host"], db_status["port"], db_status["database"],
            )
        else:
            logger.info(
                "[api] DB preflight OK — MySQL reachable at %s:%s (database '%s' will be created if needed)",
                db_status["host"], db_status["port"], db_status["database"],
            )
        init_db()
        logger.info("[api] Database tables verified/created OK")
    except DatabasePreflightError as exc:
        logger.warning("[api] DB preflight failed: %s", exc)
    except Exception as exc:
        logger.warning(f"[api] DB init failed (non-fatal, API will start): {exc}")

    # ── Vector store / DB divergence check ───────────────────────────────────
    # Non-blocking background check — logs warning if ChromaDB is empty/stale.
    try:
        import threading
        def _sync_check():
            try:
                from intelligence.vector_store import check_vector_db_sync
                result = check_vector_db_sync(warn_threshold=0.5)
                if result["status"] not in ("ok", "unavailable"):
                    logger.warning(
                        "[api] Vector store sync issue (%s): %s",
                        result["status"], result["message"],
                    )
            except Exception as _exc:
                logger.debug("[api] Vector store sync check skipped: %s", _exc)
        threading.Thread(target=_sync_check, daemon=True).start()
    except Exception:
        pass

    logger.info("[api] TenderRadar API ready")
    yield

    logger.info("[api] TenderRadar API shutting down")


# =============================================================================
# Application factory
# =============================================================================

app = FastAPI(
    title        = "TenderRadar API",
    description  = (
        "REST API for the TenderRadar procurement intelligence platform.\n\n"
        "Provides filtered, paginated access to normalised tender data "
        "scraped from 25+ procurement portals (World Bank, UNDP, GeM, GIZ, etc.), "
        "enriched with sector classification, AI fit scoring, and deduplication.\n\n"
        "**Base URL:** `/api/v1`"
    ),
    version      = "1.0.0",
    lifespan     = lifespan,
    docs_url     = "/docs",
    redoc_url    = "/redoc",
    openapi_tags = [
        {
            "name": "tenders",
            "description": "Retrieve, list, and search individual tenders.",
        },
        {
            "name": "search",
            "description": "Advanced search with sector/country/score filters.",
        },
        {
            "name": "stats",
            "description": "System statistics, portal health, and coverage metrics.",
        },
        {
            "name": "system",
            "description": "Health check and API metadata.",
        },
    ],
)


# =============================================================================
# Middleware
# =============================================================================

import os as _os
_cors_env = _os.getenv(
    "CORS_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000,https://tenderradar.idcg.in",
)
_CORS_ORIGINS = [o.strip() for o in _cors_env.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins     = _CORS_ORIGINS,
    allow_credentials = True,
    allow_methods     = ["GET", "POST", "OPTIONS"],
    allow_headers     = ["*", "X-API-Key"],
)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """
    Sliding-window rate limiter.  Applied only to /api/v1/* routes.
    Returns 429 Too Many Requests when limit is exceeded.
    """
    path = request.url.path
    if path.startswith("/api/v1/"):
        ip = request.client.host if request.client else "unknown"
        is_heavy = any(seg in path for seg in ("/search", "/copilot", "/chat"))
        limit = _RATE_STRICT if is_heavy else _RATE_STANDARD
        if not _rate_check(ip, limit):
            kind = "search/copilot" if is_heavy else "standard"
            return JSONResponse(
                status_code=429,
                content={
                    "error":  "Too Many Requests",
                    "detail": f"Rate limit exceeded ({limit} requests/{_RATE_WINDOW}s for {kind} endpoints). "
                              "Please slow down.",
                },
                headers={"Retry-After": str(_RATE_WINDOW)},
            )
    return await call_next(request)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Attach X-Process-Time header to every response (ms)."""
    t0       = time.perf_counter()
    response = await call_next(request)
    elapsed  = round((time.perf_counter() - t0) * 1000, 1)
    response.headers["X-Process-Time"] = f"{elapsed}ms"
    return response


# =============================================================================
# Error handlers
# =============================================================================

@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    return JSONResponse(
        status_code = 404,
        content     = {
            "error":   "Not Found",
            "detail":  str(exc.detail) if hasattr(exc, "detail") else "Resource not found",
            "path":    str(request.url.path),
        },
    )


@app.exception_handler(500)
async def server_error_handler(request: Request, exc):
    logger.error(f"[api] Unhandled 500 on {request.url.path}: {exc}")
    return JSONResponse(
        status_code = 500,
        content     = {
            "error":  "Internal Server Error",
            "detail": "An unexpected error occurred. Check server logs.",
        },
    )


# =============================================================================
# Mount versioned router
# =============================================================================

from api.routes import router as v1_router   # noqa: E402 — after app creation

app.include_router(v1_router, prefix="/api/v1")


# =============================================================================
# Root routes (outside /api/v1 prefix)
# =============================================================================

@app.get("/", include_in_schema=False)
def root():
    """Redirect root to Swagger UI docs."""
    return RedirectResponse(url="/docs")


@app.get("/health", tags=["system"], summary="API health check")
def health_check():
    """
    Returns 200 if the API is running.
    Does **not** check database connectivity (use `/api/v1/stats` for that).
    """
    return {
        "status":     "ok",
        "version":    "1.0.0",
        "timestamp":  datetime.utcnow().isoformat() + "Z",
        "service":    "TenderRadar API",
    }
