"""
monitoring/sentry.py — Optional Sentry bootstrap (fail-open).

If SENTRY_DSN is unset or sentry-sdk is not installed, this module does nothing.
"""

from __future__ import annotations

import logging
from typing import Optional

from config.config import (
    SENTRY_DSN,
    SENTRY_ENVIRONMENT,
    SENTRY_TRACES_SAMPLE_RATE,
)

logger = logging.getLogger("tenderradar.sentry")
_INITIALIZED = False


def init_sentry(service_name: str = "tenderradar", release: Optional[str] = None) -> bool:
    global _INITIALIZED
    if _INITIALIZED:
        return True
    if not SENTRY_DSN:
        logger.info("[sentry] SENTRY_DSN not set — external error tracking disabled")
        return False
    try:
        import sentry_sdk  # type: ignore
    except Exception as exc:
        logger.warning("[sentry] sentry_sdk unavailable: %s", exc)
        return False

    try:
        sentry_sdk.init(
            dsn=SENTRY_DSN,
            environment=SENTRY_ENVIRONMENT,
            traces_sample_rate=max(0.0, min(1.0, float(SENTRY_TRACES_SAMPLE_RATE))),
            release=release,
            server_name=service_name,
        )
        _INITIALIZED = True
        logger.info(
            "[sentry] initialized (service=%s, env=%s, traces=%.3f)",
            service_name,
            SENTRY_ENVIRONMENT,
            SENTRY_TRACES_SAMPLE_RATE,
        )
        return True
    except Exception as exc:
        logger.warning("[sentry] init failed: %s", exc)
        return False
