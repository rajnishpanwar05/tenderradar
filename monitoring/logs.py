# =============================================================================
# pipeline/logger.py — Structured, Rotating Log Setup
#
# Call setup_logging() once at the top of main.py, before importing anything
# else. After that, all logging.getLogger("tenderradar.*") calls automatically
# write to both the console and the rotating JSON log file.
#
# Two output streams:
#   Console        → human-readable, coloured by level (INFO/WARNING/ERROR)
#   File (JSON)    → machine-readable, rotating 10 MB × 5 files
#                    path: monitoring/tenderradar.log
#
# JSON record format:
#   {
#     "ts":     "2026-03-13T15:00:01",
#     "level":  "INFO",
#     "logger": "tenderradar.ngobox",
#     "msg":    "[NGOBox] Found 32 listings",
#     "source": "ngobox",          ← extracted from logger name
#     "exc":    null               ← traceback string if level=ERROR
#   }
#
# Existing print() calls in Phase 1 pipeline files are NOT captured here
# (they still appear in stdout → run.log via cron redirection). That's fine —
# this logger targets the new framework layers (runner, reporter, base_scraper).
# =============================================================================

import json
import logging
import logging.handlers
import os
import sys
import traceback
from datetime import datetime
from typing import Optional


# ── JSON formatter ─────────────────────────────────────────────────────────

class _JsonFormatter(logging.Formatter):
    """Emit one compact JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        # Extract optional source name from logger hierarchy
        # e.g. "tenderradar.ngobox" → "ngobox"
        parts  = record.name.split(".")
        source = parts[-1] if len(parts) > 1 else ""

        doc: dict = {
            "ts":     datetime.fromtimestamp(record.created).isoformat(timespec="seconds"),
            "level":  record.levelname,
            "logger": record.name,
            "msg":    record.getMessage(),
            "source": source,
            "exc":    None,
        }
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, ensure_ascii=False)


# ── Console formatter ───────────────────────────────────────────────────────

class _ConsoleFormatter(logging.Formatter):
    """
    Human-readable format with optional ANSI colour for terminals.
    Degrades gracefully when stderr is redirected (e.g. cron).
    """
    _COLOURS = {
        "DEBUG":    "\033[36m",    # cyan
        "INFO":     "\033[32m",    # green
        "WARNING":  "\033[33m",    # yellow
        "ERROR":    "\033[31m",    # red
        "CRITICAL": "\033[35m",    # magenta
    }
    _RESET = "\033[0m"

    def __init__(self, use_colour: bool = True):
        super().__init__()
        self._use_colour = use_colour and sys.stderr.isatty()

    def format(self, record: logging.LogRecord) -> str:
        ts    = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        level = record.levelname[0]   # D / I / W / E / C
        msg   = record.getMessage()

        if self._use_colour:
            colour = self._COLOURS.get(record.levelname, "")
            line   = f"{colour}[{ts}] {level} {msg}{self._RESET}"
        else:
            line   = f"[{ts}] {level} {msg}"

        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


# ── Public setup function ───────────────────────────────────────────────────

def setup_logging(
    log_dir:     Optional[str] = None,
    console:     bool          = True,
    console_level: str         = "INFO",
    file_level:  str           = "DEBUG",
) -> logging.Logger:
    """
    Configure the root "tenderradar" logger.

    Call once at the top of main.py:
        from monitoring.logs import setup_logging
        setup_logging()

    Args:
        log_dir       : Directory for the rotating JSON log file.
                        Defaults to {BASE_DIR}/monitoring/
        console       : Emit human-readable messages to stderr (default True).
        console_level : Minimum level for console output (default "INFO").
        file_level    : Minimum level for file output (default "DEBUG").

    Returns the configured "tenderradar" Logger instance.
    """
    if log_dir is None:
        log_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "monitoring",
        )
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "tenderradar.log")

    root = logging.getLogger("tenderradar")
    root.setLevel(logging.DEBUG)   # handlers filter their own levels

    # Avoid duplicate handlers on re-import (e.g. in tests)
    if root.handlers:
        return root

    # ── Rotating JSON file ─────────────────────────────────────────────────
    fh = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes    = 10 * 1024 * 1024,   # 10 MB
        backupCount = 5,
        encoding    = "utf-8",
    )
    fh.setLevel(getattr(logging, file_level.upper(), logging.DEBUG))
    fh.setFormatter(_JsonFormatter())
    root.addHandler(fh)

    # ── Console (stderr) ───────────────────────────────────────────────────
    if console:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(getattr(logging, console_level.upper(), logging.INFO))
        ch.setFormatter(_ConsoleFormatter())
        root.addHandler(ch)

    root.info(
        f"[logger] Logging initialised — "
        f"file={log_path} (rotating 10MB×5), "
        f"console={'yes' if console else 'no'}"
    )
    return root


def get_logger(name: str) -> logging.Logger:
    """
    Convenience wrapper: returns logging.getLogger(f"tenderradar.{name}").
    Usage inside pipeline modules:
        from monitoring.logs import get_logger
        log = get_logger("mymodule")
    """
    return logging.getLogger(f"tenderradar.{name}")
