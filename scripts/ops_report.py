#!/usr/bin/env python3
"""
ops_report.py — TenderRadar Ops Status Report
==============================================
Publishes a single ops snapshot covering:
  • Portal scraper status (last run, rows, stability)
  • Deep extraction success by source (doc link + extracted coverage)
  • Cron job health (last run time per log file)
  • Signal-portal quality metrics (excl. state-infra noise)

Usage:
    python3 scripts/ops_report.py              # print to stdout
    python3 scripts/ops_report.py --telegram   # also send to Telegram
    python3 scripts/ops_report.py --json       # raw JSON output
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

BASE = os.path.expanduser("~/tender_system")
if BASE not in sys.path:
    sys.path.insert(0, BASE)


# ── Cron log health ───────────────────────────────────────────────────────────

_CRON_LOGS: Dict[str, str] = {
    "main-scraper":       f"{BASE}/run.log",
    "dashboard-refresh":  f"{BASE}/dashboard.log",
    "deep-backfill":      f"{BASE}/deep_backfill.log",
    "rescore":            f"{BASE}/rescore.log",
    "insights-backfill":  f"{BASE}/insights.log",
    "daily-report":       f"{BASE}/daily_report.log",
    "chat-smoke":         f"{BASE}/chat_smoke.log",
}


def _cron_health() -> List[Dict[str, Any]]:
    rows = []
    for job, log_path in _CRON_LOGS.items():
        p = Path(log_path)
        if not p.exists():
            rows.append({"job": job, "status": "never_run", "last_run": None, "size_kb": 0})
            continue
        stat = p.stat()
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
        age_h = round((datetime.now(tz=timezone.utc).timestamp() - stat.st_mtime) / 3600, 1)
        # Read last 5 lines to detect recent errors
        try:
            tail = p.read_text(errors="replace").splitlines()[-5:]
        except Exception:
            tail = []
        has_error = any(
            kw in line.lower() for line in tail
            for kw in ("error", "traceback", "exception", "failed", "critical")
        )
        rows.append({
            "job": job,
            "last_run": mtime,
            "age_hours": age_h,
            "size_kb": round(stat.st_size / 1024, 1),
            "status": "WARN" if has_error else ("STALE" if age_h > 25 else "OK"),
            "last_lines": tail,
        })
    return rows


# ── Portal scraper status ──────────────────────────────────────────────────────

def _portal_status() -> List[Dict[str, Any]]:
    try:
        from monitoring.scraper_health_manager import get_all_health
        raw = get_all_health(window=5)
        portals = raw.get("portals", []) or []
        return sorted(portals, key=lambda p: (p.get("stability", ""), p.get("source", "")))
    except Exception as exc:
        return [{"error": str(exc)}]


# ── Deep extraction by source ──────────────────────────────────────────────────

def _extraction_by_source() -> Dict[str, Any]:
    try:
        from scripts.backend_maintenance import _deep_document_quality_by_source, _INFRA_PORTALS
        all_src  = _deep_document_quality_by_source(limit=30, min_total=10)
        # Signal-only: filter out infra portals
        signal_weakest  = [x for x in all_src["weakest"]
                           if x["source_portal"] not in _INFRA_PORTALS]
        signal_strongest = [x for x in all_src["strongest"]
                            if x["source_portal"] not in _INFRA_PORTALS]
        return {
            "signal_weakest":   signal_weakest[:8],
            "signal_strongest": signal_strongest[:8],
        }
    except Exception as exc:
        return {"error": str(exc)}


# ── Signal-portal quality summary ─────────────────────────────────────────────

def _quality_summary() -> Dict[str, Any]:
    try:
        from scripts.backend_maintenance import audit_backend
        audit = audit_backend()
        rates = audit["rates"]
        return {
            "signal_tenders":                    rates["signal_tenders_count"],
            "infra_tenders_excluded":            rates["infra_tenders_count"],
            "signal_deep_description_pct":       rates["signal_deep_description_coverage_pct"],
            "signal_deep_pdf_pct":               rates["signal_deep_pdf_coverage_pct"],
            "signal_deep_doc_link_pct":          rates["signal_deep_doc_link_coverage_pct"],
            "signal_deep_doc_extracted_pct":     rates["signal_deep_doc_extracted_coverage_pct"],
            "doc_link_extraction_success_pct":   rates["doc_link_extraction_success_pct"],
            "priority_nonzero_pct":              rates["priority_nonzero_pct"],
        }
    except Exception as exc:
        return {"error": str(exc)}


def _chat_smoke_summary() -> Dict[str, Any]:
    """
    Load latest chat smoke result artifact if available.
    """
    candidates = [
        f"{BASE}/artifacts/live_chat_smoke_last.json",
        f"{BASE}/artifacts/live_chat_smoke_notify.json",
        f"{BASE}/artifacts/live_chat_smoke_maint.json",
    ]
    for p in candidates:
        fp = Path(p)
        if not fp.exists():
            continue
        try:
            raw = json.loads(fp.read_text(encoding="utf-8"))
            summary = raw.get("summary") or {}
            return {
                "ok": bool(summary.get("ok")),
                "pass_rate_pct": summary.get("pass_rate_pct"),
                "cases": summary.get("cases"),
                "passed": summary.get("passed"),
                "failed": summary.get("failed"),
                "failed_case_ids": summary.get("failed_case_ids") or [],
                "generated_at": summary.get("generated_at"),
            }
        except Exception:
            continue
    return {"ok": None, "status": "not_run"}


# ── Report assembly ────────────────────────────────────────────────────────────

def build_report() -> Dict[str, Any]:
    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "cron_health":  _cron_health(),
        "portal_status": _portal_status(),
        "extraction_by_source": _extraction_by_source(),
        "quality_summary": _quality_summary(),
        "chat_smoke": _chat_smoke_summary(),
    }


def _fmt_report(report: Dict[str, Any]) -> str:
    """Format as a readable plain-text ops report."""
    lines = [
        "=" * 60,
        f"  TenderRadar Ops Report  —  {report['generated_at'][:19]}Z",
        "=" * 60,
    ]

    # ── Cron health ───────────────────────────────────────────────
    lines += ["", "── CRON JOB HEALTH ──────────────────────────────────────"]
    for j in report["cron_health"]:
        status = j.get("status", "?")
        icon = "✅" if status == "OK" else ("⚠️ " if status == "WARN" else ("🔴" if status == "STALE" else "❓"))
        age = f"  ({j['age_hours']}h ago)" if j.get("age_hours") is not None else ""
        lines.append(f"  {icon} {j['job']:<22} {status}{age}")

    # ── Quality summary ───────────────────────────────────────────
    qs = report.get("quality_summary", {})
    if "error" not in qs:
        lines += ["", "── SIGNAL-PORTAL QUALITY (excl. state-infra) ────────────"]
        lines.append(f"  Signal tenders:    {qs.get('signal_tenders', '?'):>6}   (excluded {qs.get('infra_tenders_excluded', '?')} infra)")
        lines.append(f"  Deep description:  {qs.get('signal_deep_description_pct', '?'):>6}%")
        lines.append(f"  Deep PDF text:     {qs.get('signal_deep_pdf_pct', '?'):>6}%")
        lines.append(f"  Doc links found:   {qs.get('signal_deep_doc_link_pct', '?'):>6}%")
        lines.append(f"  Docs extracted:    {qs.get('signal_deep_doc_extracted_pct', '?'):>6}%")
        lines.append(f"  Extract success:   {qs.get('doc_link_extraction_success_pct', '?'):>6}%")
        lines.append(f"  Priority nonzero:  {qs.get('priority_nonzero_pct', '?'):>6}%")

    # ── Chat smoke summary ───────────────────────────────────────
    cs = report.get("chat_smoke", {})
    if cs and cs.get("ok") is not None:
        status = "✅" if cs.get("ok") else "❌"
        lines += ["", "── CHAT SMOKE (live guardrail checks) ───────────────────"]
        lines.append(
            f"  {status} Pass rate: {cs.get('pass_rate_pct', '?')}% "
            f"({cs.get('passed', '?')}/{cs.get('cases', '?')})"
        )
        if cs.get("failed_case_ids"):
            lines.append("  Failed: " + ", ".join(str(x) for x in cs.get("failed_case_ids", [])[:8]))

    # ── Portal status ─────────────────────────────────────────────
    lines += ["", "── PORTAL SCRAPER STATUS ────────────────────────────────"]
    portals = report.get("portal_status", [])
    if portals and "error" not in (portals[0] if portals else {}):
        for p in portals:
            stab = str(p.get("stability") or "?")
            icon = "✅" if stab == "stable" else ("⚠️ " if stab == "partial" else "🔴")
            src = str(p.get("source") or "?")[:22]
            rows = p.get("average_rows") or 0
            cov  = p.get("coverage_pct") or 0
            deep = p.get("deep_enriched") or 0
            lines.append(f"  {icon} {src:<24} rows≈{rows:<5} cov={cov}%  deep={deep}")
    else:
        lines.append("  (portal health unavailable)")

    # ── Extraction by source ──────────────────────────────────────
    ext = report.get("extraction_by_source", {})
    if "error" not in ext:
        weakest = ext.get("signal_weakest", [])
        if weakest:
            lines += ["", "── DOC EXTRACTION — WEAKEST SIGNAL PORTALS ─────────────"]
            for row in weakest:
                src = str(row["source_portal"])[:22]
                lines.append(
                    f"  {src:<24} total={row['total_tenders']:<5} "
                    f"links={row['doc_link_coverage_pct']}%  "
                    f"extracted={row['doc_extracted_coverage_pct']}%"
                )

        strongest = ext.get("signal_strongest", [])
        if strongest:
            lines += ["", "── DOC EXTRACTION — STRONGEST SIGNAL PORTALS ───────────"]
            for row in strongest:
                src = str(row["source_portal"])[:22]
                lines.append(
                    f"  {src:<24} total={row['total_tenders']:<5} "
                    f"links={row['doc_link_coverage_pct']}%  "
                    f"extracted={row['doc_extracted_coverage_pct']}%"
                )

    lines.append("")
    lines.append("=" * 60)
    return "\n".join(lines)


def _send_telegram(text: str) -> bool:
    try:
        from config.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return False
        import urllib.request, urllib.parse
        payload = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text[:4000],
            "disable_web_page_preview": "true",
        }).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data=payload,
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception:
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="TenderRadar ops status report")
    ap.add_argument("--telegram", action="store_true", help="Send report to Telegram")
    ap.add_argument("--json", action="store_true", help="Output raw JSON")
    args = ap.parse_args()

    report = build_report()

    if args.json:
        print(json.dumps(report, indent=2, default=str))
        return 0

    text = _fmt_report(report)
    print(text)

    if args.telegram:
        ok = _send_telegram(text)
        print(f"\nTelegram: {'sent' if ok else 'not sent (check token/chat_id)'}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
