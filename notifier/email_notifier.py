"""
Email notifier that sends run notifications with a ZIP package containing:
- Unified master Excel
- Portal-wise Excel files
"""

from __future__ import annotations

import logging
import os
import smtplib
import zipfile
from datetime import datetime
from email.message import EmailMessage
from email.utils import formataddr
from typing import Dict, List

from config.config import (
    EMAIL_SUBJECT_PREFIX,
    OUTPUT_DIR,
    PORTAL_EXCELS_DIR,
    SMTP_FROM_EMAIL,
    SMTP_FROM_NAME,
    UNIFIED_EXCEL_PATH,
    email_configured,
    get_email_config_errors,
    NOTIFY_EMAIL_TO,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_USE_TLS,
    SMTP_USERNAME,
)

logger = logging.getLogger("tenderradar.email_notifier")


def _enabled() -> bool:
    return email_configured()


def _collect_excel_paths() -> List[str]:
    files: List[str] = []
    if os.path.exists(UNIFIED_EXCEL_PATH):
        files.append(UNIFIED_EXCEL_PATH)

    if os.path.isdir(PORTAL_EXCELS_DIR):
        for name in sorted(os.listdir(PORTAL_EXCELS_DIR)):
            if name.lower().endswith(".xlsx"):
                files.append(os.path.join(PORTAL_EXCELS_DIR, name))
    return files


def _build_zip(files: List[str], total_new: int) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    out_dir = os.path.join(OUTPUT_DIR, "notifications")
    os.makedirs(out_dir, exist_ok=True)
    zip_path = os.path.join(out_dir, f"tenderradar_package_{ts}_{total_new}new.zip")

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in files:
            if not os.path.exists(p):
                continue
            arcname = (
                os.path.basename(p)
                if os.path.dirname(p) != PORTAL_EXCELS_DIR
                else os.path.join("portal_excels", os.path.basename(p))
            )
            zf.write(p, arcname=arcname)

    return zip_path


def _send_email(subject: str, body: str, attachment_path: str) -> bool:
    if not _enabled():
        logger.info(
            "[email_notify] Email skipped because SMTP delivery is disabled (%s)",
            ", ".join(get_email_config_errors()) or "missing config",
        )
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr((SMTP_FROM_NAME, SMTP_FROM_EMAIL))
    msg["To"] = ", ".join(NOTIFY_EMAIL_TO)
    msg.set_content(body)

    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as fh:
            data = fh.read()
        msg.add_attachment(
            data,
            maintype="application",
            subtype="zip",
            filename=os.path.basename(attachment_path),
        )

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=40) as server:
            if SMTP_USE_TLS:
                server.starttls()
            if SMTP_USERNAME:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
        logger.info("[email_notify] Email sent to %s", ", ".join(NOTIFY_EMAIL_TO))
        return True
    except Exception as exc:
        logger.warning("[email_notify] send failed: %s", exc)
        return False


def _send_run_package(total_new: int, context: str) -> bool:
    files = _collect_excel_paths()
    if not files:
        logger.warning("[email_notify] No excel files found; email skipped")
        return False

    zip_path = _build_zip(files, total_new=total_new)
    subject = f"{EMAIL_SUBJECT_PREFIX} | {context} | {total_new} new tenders"
    body = (
        f"TenderRadar run completed.\n\n"
        f"Context: {context}\n"
        f"New tenders in this run: {total_new}\n"
        f"Attached ZIP contains:\n"
        f"- Tender_Monitor_Master.xlsx\n"
        f"- Portal-wise Excel files\n"
        f"- Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    return _send_email(subject=subject, body=body, attachment_path=zip_path)


def test_email() -> bool:
    """
    Send a test email to verify SMTP configuration.
    Run from the project root:
        python -c "from notifier.email_notifier import test_email; test_email()"
    """
    if not _enabled():
        errors = get_email_config_errors()
        print(f"[email_test] Cannot send — SMTP not configured. Missing: {', '.join(errors)}")
        return False
    subject = f"{EMAIL_SUBJECT_PREFIX} | Test — configuration OK"
    body = (
        "This is a test email from TenderRadar.\n\n"
        f"SMTP host: {SMTP_HOST}:{SMTP_PORT}\n"
        f"From: {SMTP_FROM_NAME} <{SMTP_FROM_EMAIL}>\n"
        f"To: {', '.join(NOTIFY_EMAIL_TO)}\n"
        f"Sent at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    result = _send_email(subject=subject, body=body, attachment_path="")
    if result:
        print(f"[email_test] Test email sent to {', '.join(NOTIFY_EMAIL_TO)}")
    else:
        print("[email_test] Test email FAILED — check logs for details")
    return result


def notify_all(**portal_tenders) -> bool:
    total = 0
    for rows in portal_tenders.values():
        if isinstance(rows, list):
            total += len(rows)
    return _send_run_package(total_new=total, context="Standard Alert")


def send_rich_alert(enriched_tenders: list) -> bool:
    return _send_run_package(total_new=len(enriched_tenders or []), context="Rich Alert")


def send_amendment_alert(amended_tenders: List[Dict]) -> bool:
    n = len(amended_tenders or [])
    subject = f"{EMAIL_SUBJECT_PREFIX} | Amendment Alert | {n} tenders changed"
    titles = []
    for t in amended_tenders[:20]:
        title = str(t.get("title") or "").strip()
        if title:
            titles.append(f"- {title[:140]}")
    body = (
        "Amendment alert from TenderRadar.\n\n"
        f"Tenders with content changes: {n}\n\n"
        + ("\n".join(titles) if titles else "No tender titles available.")
    )
    return _send_email(subject=subject, body=body, attachment_path="")
