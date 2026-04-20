# =============================================================================
# pipeline/reporter.py — Run Diagnostics Reporter
#
# Builds the post-run summary: ASCII table for run.log + optional
# Telegram health message so you can see portal status without SSHing in.
#
# Called once at the end of main.py after all JobRunner results are collected.
#
# ── Summary table (written to run.log) ──────────────────────────────────────
#
#   PORTAL                 ROWS   NEW   STATUS    TIME     ATTEMPTS
#   ──────────────────────────────────────────────────────────────────
#   World Bank               42     3   ✅ OK      4m12s        1
#   UNDP Procurement         18     1   ✅ OK      1m08s        1
#   NGO Box                   0     0   ⚠ WARN    0m34s        2  ← zero-row
#   GIZ India                 –     –   ❌ FAIL    0m15s        2  ← crashed
#   TED EU                    –     –   ⏱ TIMEOUT  –            1  ← stuck
#   ──────────────────────────────────────────────────────────────────
#   TOTAL                   847    62               18m04s
#
# ── Telegram health message (optional) ──────────────────────────────────────
#
# Sent after every run IF any portal is WARN/FAIL/TIMEOUT, or at most once
# per day when everything is OK. Uses a simple direct POST (no notifier.py
# import to avoid circular deps).
#
# Controlled by config.NOTIFICATIONS_ENABLED and TELEGRAM_BOT_TOKEN.
# =============================================================================

import logging
import os
import time
from datetime import datetime
from typing import List

from core.runner import JobResult
from monitoring.health_report import (
    get_broken_scrapers, get_health_summary, get_consecutive_zero_runs,
)

logger = logging.getLogger("tenderradar.reporter")


class RunReporter:
    """
    Assemble and deliver the post-run diagnostics report.

    Usage:
        reporter = RunReporter(results, run_start_ts)
        reporter.log_summary()           # writes ASCII table to logger
        reporter.send_health_telegram()  # sends Telegram status (if configured)
    """

    STATUS_ICON = {
        "ok":      "✅",
        "warn":    "⚠",
        "fail":    "❌",
        "timeout": "⏱",
        "skip":    "–",
        "skipped": "–",
    }

    # Human-readable labels for the summary table (ISSUE 9)
    STATUS_DISPLAY = {
        "ok":      "OK",
        "warn":    "WARN",
        "fail":    "FAILED",
        "timeout": "TIMEOUT",
        "skip":    "SKIPPED",
        "skipped": "SKIPPED",
    }

    def __init__(self, results: List[JobResult], run_start: float):
        self.results   = results
        self.run_start = run_start
        self.elapsed   = time.time() - run_start
        self.run_at    = datetime.now()

    # ── Public methods ────────────────────────────────────────────────────────

    def log_summary(self) -> str:
        """
        Build and log the ASCII summary table.
        Returns the table string (useful for testing).
        """
        table = self._build_table()
        # Log each line so it appears in both run.log and tenderradar.log
        for line in table.split("\n"):
            logger.info(line)
        return table

    def send_health_telegram(self, total_new: int = 0) -> None:
        """
        Send an email health status message at the end of each run.

        Always sent if any portal is WARN/FAIL/TIMEOUT.
        Suppressed when all portals are OK and total_new > 0
        (the tender alert email covers that case already).
        """
        try:
            from config.config import (
                NOTIFICATIONS_ENABLED,
                email_configured,
            )
        except Exception:
            return

        if not NOTIFICATIONS_ENABLED:
            return
        if not email_configured():
            return

        has_issues = any(
            r.status in ("warn", "fail", "timeout")
            for r in self.results
        )
        # Suppress clean-run health pings when tender alert already sent
        if not has_issues and total_new > 0:
            logger.debug("[reporter] All portals OK + tender alert sent — skipping health ping")
            return

        html = self._build_telegram_html(total_new, has_issues)
        self._send_health_email(html)

    @staticmethod
    def _send_health_email(message: str) -> None:
        try:
            from notifier.email_notifier import _send_email as _email_send
            ok = _email_send(
                subject="TenderRadar | Run Health Report",
                body=message.replace("<b>", "").replace("</b>", "")
                .replace("<i>", "").replace("</i>", ""),
                attachment_path="",
            )
            if ok:
                logger.info("[reporter] Health report sent via email OK")
        except Exception as exc:
            logger.warning("[reporter] Health email failed (non-fatal): %s", exc)

    def get_broken_portals(self, min_prev_rows: int = 5) -> List[dict]:
        """
        Return portals that previously had rows but returned zero in the last run.
        Cross-checks live results with the historical health DB.
        Portals with 3+ consecutive zero-row runs are flagged as
        POSSIBLE SCRAPER BREAK instead of a plain WARN.
        """
        broken = []
        # From current run
        for r in self.results:
            if r.status == "warn" and r.all_rows == []:
                consec = get_consecutive_zero_runs(r.label)
                flag = (
                    "POSSIBLE SCRAPER BREAK" if consec >= 3
                    else f"WARN — zero rows (run {consec} consecutive)"
                )
                broken.append({
                    "source":  r.label,
                    "status":  flag,
                    "elapsed": r.elapsed,
                })
        # From historical health DB (may flag portals not in this run)
        for entry in get_broken_scrapers(min_prev_rows=min_prev_rows):
            if not any(b["source"] == entry["source"] for b in broken):
                consec = get_consecutive_zero_runs(entry["source"])
                flag = (
                    "POSSIBLE SCRAPER BREAK" if consec >= 3
                    else f"WARN — was {entry['prev_avg_rows']:.0f} avg, now 0"
                )
                broken.append({
                    "source":  entry["source"],
                    "status":  flag,
                    "elapsed": 0.0,
                })
        return broken

    # ── Table builder ─────────────────────────────────────────────────────────

    def _build_table(self) -> str:
        if not self.results:
            return "(no jobs ran)"

        W_PORTAL   = 22
        W_ROWS     = 6
        W_NEW      = 5
        W_STATUS   = 11
        W_TIME     = 8
        W_ATTEMPTS = 8

        hdr = (
            f"{'PORTAL':<{W_PORTAL}} "
            f"{'ROWS':>{W_ROWS}} "
            f"{'NEW':>{W_NEW}} "
            f"{'STATUS':<{W_STATUS}} "
            f"{'TIME':>{W_TIME}} "
            f"{'TRIES':>{W_ATTEMPTS}}"
        )
        sep = "─" * len(hdr)

        lines = [
            sep,
            f"TenderRadar Run Summary — {self.run_at.strftime('%Y-%m-%d %H:%M')}",
            sep,
            hdr,
            sep,
        ]

        total_rows = 0
        total_new  = 0

        for r in self.results:
            icon = self.STATUS_ICON.get(r.status, "?")
            rows_s = str(len(r.all_rows))   if r.all_rows    else "–"
            new_s  = str(len(r.new_tenders)) if r.all_rows   else "–"
            time_s = _fmt_elapsed(r.elapsed) if r.elapsed    else "–"
            tries_s = str(r.attempts)        if r.attempts   else "–"

            label_s  = self.STATUS_DISPLAY.get(r.status, r.status.upper())
            status_s = f"{icon} {label_s}"
            lines.append(
                f"{r.label:<{W_PORTAL}} "
                f"{rows_s:>{W_ROWS}} "
                f"{new_s:>{W_NEW}} "
                f"{status_s:<{W_STATUS}} "
                f"{time_s:>{W_TIME}} "
                f"{tries_s:>{W_ATTEMPTS}}"
            )
            total_rows += len(r.all_rows)
            total_new  += len(r.new_tenders)

        lines.append(sep)
        lines.append(
            f"{'TOTAL':<{W_PORTAL}} "
            f"{total_rows:>{W_ROWS}} "
            f"{total_new:>{W_NEW}} "
            f"{'':>{W_STATUS}} "
            f"{_fmt_elapsed(self.elapsed):>{W_TIME}}"
        )
        lines.append(sep)

        # Broken portal warnings
        broken = self.get_broken_portals()
        if broken:
            lines.append("")
            lines.append("⚠  PORTALS NEEDING ATTENTION:")
            for b in broken:
                lines.append(f"   {b['source']}: {b['status']}")

        return "\n".join(lines)

    # ── Telegram health message ───────────────────────────────────────────────

    def _build_telegram_html(self, total_new: int, has_issues: bool) -> str:
        ok_count      = sum(1 for r in self.results if r.status == "ok")
        warn_count    = sum(1 for r in self.results if r.status == "warn")
        fail_count    = sum(1 for r in self.results if r.status in ("fail", "timeout"))
        total_portals = len(self.results)
        total_rows    = sum(len(r.all_rows) for r in self.results)

        status_line = (
            "🔴 <b>Issues Detected</b>" if has_issues
            else "🟢 <b>All Portals OK</b>"
        )

        lines = [
            "🏥 <b>TenderRadar — Run Report</b>",
            f"🕐 {self.run_at.strftime('%d %b %Y  •  %H:%M')}",
            f"⏱ Total runtime: {_fmt_elapsed(self.elapsed)}",
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"{status_line}",
            f"📊 {total_portals} portals — {ok_count} OK / {warn_count} WARN / {fail_count} FAIL",
            f"📋 {total_rows} rows scraped  |  🆕 {total_new} new tenders",
            "",
        ]

        # List any problem portals
        problems = [r for r in self.results if r.status in ("warn", "fail", "timeout")]
        if problems:
            lines.append("⚠ <b>Problem Portals:</b>")
            for r in problems:
                icon = self.STATUS_ICON.get(r.status, "?")
                err  = f" — {r.error[:60]}" if r.error else ""
                lines.append(f"  {icon} <b>{r.label}</b>{err}")
            lines.append("")

        # Historical broken scrapers (if any)
        broken = [b for b in self.get_broken_portals()
                  if not any(r.label == b["source"] for r in problems)]
        if broken:
            lines.append("🔍 <b>Possible Selector Drift (historical):</b>")
            for b in broken:
                lines.append(f"  ⚠ <b>{b['source']}</b> — {b['status']}")
            lines.append("")

        lines.append("⚡ <i>Powered by TenderRadar</i>")
        return "\n".join(lines)

    @staticmethod
    def _post_telegram(html: str, token: str, chat_id: str) -> None:
        """Direct Telegram POST — no dependency on notifier.py."""
        import requests as _req
        try:
            resp = _req.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={
                    "chat_id":                  chat_id,
                    "text":                     html[:4000],
                    "parse_mode":               "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            if resp.status_code == 200 and resp.json().get("ok"):
                logger.info("[reporter] Health report sent to Telegram OK")
            else:
                logger.warning(
                    f"[reporter] Telegram health report failed: "
                    f"HTTP {resp.status_code} {resp.text[:100]}"
                )
        except Exception as exc:
            logger.warning(f"[reporter] Telegram post error (non-fatal): {exc}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt_elapsed(seconds: float) -> str:
    """Format seconds → '4m12s' or '34s'."""
    if seconds <= 0:
        return "–"
    m = int(seconds) // 60
    s = int(seconds) % 60
    return f"{m}m{s:02d}s" if m else f"{s}s"
