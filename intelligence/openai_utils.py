"""
Shared OpenAI helpers: client factory + lightweight global rate throttle.
Keeps a minimum interval between requests to reduce 429s.
"""

import os
import threading
import time
import logging
from typing import Optional

logger = logging.getLogger("tenderradar.openai_utils")

_lock = threading.Lock()
_last_call_ts = 0.0
_MIN_INTERVAL = float(os.environ.get("OPENAI_MIN_INTERVAL", "0.6"))  # seconds

_redis = None
_redis_key = "tenderradar:openai:token"
_gemini_cooldown_key = "tenderradar:gemini:cooldown_until"
_openai_cooldown_key = "tenderradar:openai:cooldown_until"
_gemini_local_cooldown_until = 0.0
_openai_local_cooldown_until = 0.0
_GEMINI_COOLDOWN_SEC = int(os.environ.get("GEMINI_COOLDOWN_SEC", "3600"))
_OPENAI_COOLDOWN_SEC = int(os.environ.get("OPENAI_COOLDOWN_SEC", "1800"))


def _get_redis():
    global _redis
    if _redis is not None:
        return _redis
    url = os.environ.get("REDIS_URL")
    if not url:
        return None
    try:
        import redis
        _redis = redis.Redis.from_url(url, decode_responses=True)
        _redis.ping()
        return _redis
    except Exception as exc:
        logger.debug(f"[openai_utils] Redis unavailable: {exc}")
        _redis = None
        return None


def _now_ts() -> float:
    return time.time()


def is_gemini_cooldown_active() -> bool:
    """Return True when Gemini is temporarily disabled due to quota exhaustion."""
    global _gemini_local_cooldown_until
    now = _now_ts()
    r = _get_redis()
    if r:
        try:
            v = r.get(_gemini_cooldown_key)
            if v:
                redis_until = float(v)
                return max(_gemini_local_cooldown_until, redis_until) > now
            return _gemini_local_cooldown_until > now
        except Exception:
            pass
    return _gemini_local_cooldown_until > now


def is_openai_cooldown_active() -> bool:
    """Return True when OpenAI is temporarily disabled due to quota exhaustion."""
    global _openai_local_cooldown_until
    now = _now_ts()
    r = _get_redis()
    if r:
        try:
            v = r.get(_openai_cooldown_key)
            if v:
                redis_until = float(v)
                return max(_openai_local_cooldown_until, redis_until) > now
            return _openai_local_cooldown_until > now
        except Exception:
            pass
    return _openai_local_cooldown_until > now


def mark_gemini_cooldown(seconds: Optional[int] = None) -> None:
    """Set a cooldown window to skip Gemini and avoid repeated 429 storms."""
    global _gemini_local_cooldown_until
    ttl = max(60, int(seconds or _GEMINI_COOLDOWN_SEC))
    until = _now_ts() + ttl
    _gemini_local_cooldown_until = max(_gemini_local_cooldown_until, until)
    r = _get_redis()
    if r:
        try:
            r.set(_gemini_cooldown_key, str(until), ex=ttl)
        except Exception:
            pass


def mark_openai_cooldown(seconds: Optional[int] = None) -> None:
    """Set a cooldown window to skip OpenAI after quota/rate-limit errors."""
    global _openai_local_cooldown_until
    ttl = max(60, int(seconds or _OPENAI_COOLDOWN_SEC))
    until = _now_ts() + ttl
    _openai_local_cooldown_until = max(_openai_local_cooldown_until, until)
    r = _get_redis()
    if r:
        try:
            r.set(_openai_cooldown_key, str(until), ex=ttl)
        except Exception:
            pass


def note_llm_error(exc: Exception, model: Optional[str] = None) -> None:
    """
    Inspect LLM errors and trigger Gemini cooldown on quota/rate-limit failures.
    """
    model_s = (model or "").lower()
    msg = str(exc).lower()
    is_gemini = "gemini" in model_s or "generativelanguage.googleapis.com" in msg
    is_openai = ("gpt-" in model_s) or ("openai" in msg and not is_gemini)
    quotaish = (
        "resource_exhausted" in msg
        or "quota exceeded" in msg
        or "rate limit" in msg
        or "error code: 429" in msg
        or "429" in msg
    )
    key_invalid = (
        "api_key_invalid" in msg
        or "api key expired" in msg
        or "invalid api key" in msg
    )
    if is_gemini and (quotaish or key_invalid):
        cooldown = 86400 if key_invalid else _GEMINI_COOLDOWN_SEC
        mark_gemini_cooldown(cooldown)
        logger.warning("[openai_utils] Gemini cooldown activated for %ss", cooldown)
        return
    if is_openai and quotaish:
        mark_openai_cooldown()
        logger.warning("[openai_utils] OpenAI cooldown activated for %ss", _OPENAI_COOLDOWN_SEC)


def throttle_openai() -> None:
    """
    Enforce a minimum gap between OpenAI calls.
    Uses Redis token if available for multi-process safety; otherwise per-process lock.
    """
    r = _get_redis()
    if r:
        while True:
            now = time.time()
            ok = r.set(_redis_key, now, nx=True, ex=max(1, int(_MIN_INTERVAL + 1)))
            if ok:
                break
            time.sleep(_MIN_INTERVAL)
        return

    # Fallback: process-local throttle
    global _last_call_ts
    with _lock:
        now = time.time()
        wait = _MIN_INTERVAL - (now - _last_call_ts)
        if wait > 0:
            time.sleep(wait)
            now = time.time()
        _last_call_ts = now


def get_openai_client():
    """Return OpenAI client or None if key not configured."""
    try:
        from openai import OpenAI
        from config.config import OPENAI_API_KEY
        if is_openai_cooldown_active():
            return None
        if not OPENAI_API_KEY or OPENAI_API_KEY in ("YOUR_OPENAI_API_KEY", ""):
            return None
        return OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        logger.debug(f"[openai_utils] OpenAI client unavailable: {e}")
        return None


# ── Gemini / unified LLM client ───────────────────────────────────────────────
_GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
_GEMINI_MODEL    = "gemini-2.0-flash"
_OPENAI_MODEL    = "gpt-4o"


def get_llm_client():
    """
    Return (client, model_name), preferring Gemini when GEMINI_API_KEY is set.
    Falls back to OpenAI if Gemini key is absent.
    Returns (None, None) if neither key is configured.

    The returned client is an openai.OpenAI instance (Gemini uses an
    OpenAI-compatible endpoint), so all existing chat.completions.create()
    calls work without modification.

    NOTE: Gemini does NOT support beta.chat.completions.parse() — callers
    must use response_format={"type": "json_object"} + manual JSON parsing.
    """
    try:
        gemini_key = os.environ.get("GEMINI_API_KEY", "")
        if not gemini_key:
            try:
                from config.config import GEMINI_API_KEY as CFG_GEMINI_API_KEY
                gemini_key = CFG_GEMINI_API_KEY or ""
            except Exception:
                gemini_key = ""
        if gemini_key and gemini_key not in ("", "YOUR_GEMINI_API_KEY") and not is_gemini_cooldown_active():
            from openai import OpenAI
            client = OpenAI(api_key=gemini_key, base_url=_GEMINI_BASE_URL)
            logger.debug("[openai_utils] LLM: Gemini (%s)", _GEMINI_MODEL)
            return client, _GEMINI_MODEL
    except Exception as e:
        logger.debug("[openai_utils] Gemini init failed, falling back to OpenAI: %s", e)

    client = get_openai_client()
    if client:
        logger.debug("[openai_utils] LLM: OpenAI (%s)", _OPENAI_MODEL)
        return client, _OPENAI_MODEL

    return None, None
