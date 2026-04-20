#!/usr/bin/env python3
"""
cron_notify.py — Telegram-aware cron wrapper
=============================================
Wraps any shell command, sends Telegram start + finish/fail notifications.

Usage (in crontab):
    30 */6 * * * python3 ~/tender_system/scripts/cron_notify.py \
        --job "dashboard-refresh" \
        -- python3 ~/tender_system/dashboard_data_loader.py

On success: sends  ✅ [job] finished in Xs
On failure: sends  ❌ [job] FAILED (exit N) — last 20 log lines

Telegram is fire-and-forget: if the bot token is not set the job still runs,
notification is skipped silently.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

BASE = os.path.expanduser("~/tender_system")
if BASE not in sys.path:
    sys.path.insert(0, BASE)


def _telegram(msg: str) -> None:
    """Best-effort Telegram message — never raises."""
    try:
        from config.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return
        import urllib.request, urllib.parse
        payload = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data=payload,
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Cron wrapper that sends Telegram start/finish/fail alerts"
    )
    ap.add_argument("--job", required=True, help="Human-readable job name for alerts")
    ap.add_argument("--log", default=None,
                    help="Path to the job log file (last lines included on failure)")
    ap.add_argument("--no-start", action="store_true",
                    help="Suppress the start notification (reduces noise for frequent jobs)")
    ap.add_argument("cmd", nargs=argparse.REMAINDER,
                    help="Command to run (everything after --)")

    args = ap.parse_args()

    # Strip leading "--" separator if present
    cmd = args.cmd
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]

    if not cmd:
        ap.error("No command provided after --")

    job = args.job

    if not args.no_start:
        _telegram(f"⏳ <b>[cron]</b> {job} started")

    t0 = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = round(time.time() - t0, 1)

    if result.returncode == 0:
        _telegram(f"✅ <b>[cron]</b> {job} — finished in {elapsed}s")
        # Print stdout so it goes to the cron log file
        if result.stdout:
            print(result.stdout, end="")
        return 0

    # ── Failure path ──────────────────────────────────────────────────────────
    # Collect tail of log file + stderr for the alert
    tail_lines: list[str] = []

    stderr_tail = (result.stderr or "").strip().splitlines()[-15:]
    if stderr_tail:
        tail_lines.extend(stderr_tail)

    if args.log:
        log_path = Path(args.log)
        if log_path.exists():
            try:
                log_tail = log_path.read_text(errors="replace").splitlines()[-10:]
                tail_lines.extend(log_tail)
            except Exception:
                pass

    tail_str = "\n".join(tail_lines[-20:])
    msg = (
        f"❌ <b>[cron] {job} FAILED</b> (exit {result.returncode}, {elapsed}s)\n"
        f"<pre>{tail_str[:1800]}</pre>"
    )
    _telegram(msg)

    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)

    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
