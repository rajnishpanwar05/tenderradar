# =============================================================================
# api/routes/__init__.py — Aggregates all versioned sub-routers
# =============================================================================

from fastapi import APIRouter, Depends

from api.dependencies       import verify_api_key
from api.routes.tenders     import router as tenders_router
from api.routes.stats       import router as stats_router
from api.routes.pipeline    import router as pipeline_router
from api.routes.summary     import router as summary_router
from api.routes.search      import router as search_router
from api.routes.copilot     import router as copilot_router
from api.routes.health      import router as health_router
from api.routes.performance import router as performance_router
from api.routes.chat        import router as chat_router

# All /api/v1/* routes require a valid X-API-Key header.
# /health (root level) and /docs /redoc /openapi.json are outside this router — stay public.
router = APIRouter(dependencies=[Depends(verify_api_key)])

# Mount sub-routers with their path prefixes
router.include_router(tenders_router,     prefix="/tenders",  tags=["tenders"])
router.include_router(pipeline_router,    prefix="/pipeline", tags=["pipeline"])
router.include_router(summary_router,     prefix="",          tags=["summary"])
router.include_router(stats_router,       prefix="",          tags=["stats"])
router.include_router(search_router,      prefix="",          tags=["search"])
router.include_router(copilot_router,     prefix="",          tags=["copilot"])
router.include_router(health_router,      prefix="",          tags=["health"])
router.include_router(performance_router, prefix="",          tags=["performance"])
router.include_router(chat_router,        prefix="/chat",     tags=["chat"])
