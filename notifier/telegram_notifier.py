# =============================================================================
# notifier/telegram_notifier.py — Clean score-ranked opportunity alerts
# Primary   : Telegram bot     (free, instant, rich HTML)
# Secondary : Twilio WhatsApp  (paid, fallback)
#
# Format (each tender):
#   🔥 #1 · Score 87 · 7 Apr
#   GIZ India
#   Climate Policy Consulting in India  ← tappable link
#
# Tiers by score:  🔥 ≥75   ⭐ 60–74   📌 <60
# =============================================================================

import logging
import requests
from datetime import datetime
from html import escape as _html_escape
from typing import Any, Dict, List, Optional

from config.config import (
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN,
    TWILIO_WHATSAPP_FROM, WHATSAPP_NUMBERS,
)

logger = logging.getLogger(__name__)

MAX_SEND      = 10    # top N tenders shown per alert
MAX_MSG_CHARS = 3900  # Telegram safe limit (hard cap 4096)
WA_MAX_CHARS  = 1500  # WhatsApp safe limit


# =============================================================================
# HELPERS
# =============================================================================

def _esc(text: Any) -> str:
    return _html_escape(str(text or ""), quote=False)


def _score_emoji(score: int) -> str:
    if score >= 75: return "🔥"
    if score >= 60: return "⭐"
    return "📌"


def _format_deadline(dl: Any) -> str:
    """Return deadline as '7 Apr' (short) or '' if blank/unparseable."""
    if not dl:
        return ""
    s = str(dl).strip()
    if s in ("", "N/A", "None", "none", "No deadline set", "—"):
        return ""
    # Already in '7 Apr 2026' or '07 Apr 2026' form — shorten to '7 Apr'
    for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s[:11].strip(), fmt)
            return dt.strftime("%-d %b")
        except ValueError:
            continue
    # Fallback: return first 10 chars
    return s[:10]


def _resolve_tid(t: Dict) -> str:
    return str(t.get("tender_id") or t.get("id") or "").strip()


def _fetch_intel_by_tender_ids(tenders: List[Dict]) -> Dict[str, Dict]:
    tids = [_resolve_tid(t) for t in tenders]
    tids = [tid for tid in tids if tid]
    if not tids:
        return {}
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        placeholders = ",".join(["%s"] * len(tids))
        cur.execute(
            f"""
            SELECT tender_id, sector, priority_score, relevance_score AS bid_fit_score,
                   competition_level, opportunity_size, opportunity_insight,
                   organization AS client_org, decision_tag
            FROM tender_structured_intel
            WHERE tender_id IN ({placeholders})
            """,
            tids,
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {str(r["tender_id"]): r for r in rows}
    except Exception as e:
        logger.warning("[notify] intel fetch failed: %s", e)
        return {}


def _fetch_intel_by_urls(urls: List[str]) -> Dict[str, Dict]:
    urls = [u for u in urls if u]
    if not urls:
        return {}
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        placeholders = ",".join(["%s"] * len(urls))
        cur.execute(
            f"SELECT id, url FROM seen_tenders WHERE url IN ({placeholders})", urls
        )
        id_map = {r["url"]: str(r["id"]) for r in cur.fetchall()}
        if not id_map:
            cur.close(); conn.close()
            return {}
        tids = list(id_map.values())
        tid_placeholders = ",".join(["%s"] * len(tids))
        cur.execute(
            f"""
            SELECT tender_id, sector, priority_score, relevance_score AS bid_fit_score,
                   competition_level, opportunity_size, opportunity_insight,
                   organization AS client_org, decision_tag
            FROM tender_structured_intel
            WHERE tender_id IN ({tid_placeholders})
            """,
            tids,
        )
        tid_intel = {str(r["tender_id"]): r for r in cur.fetchall()}
        cur.close(); conn.close()
        result = {}
        for url, tid in id_map.items():
            if tid in tid_intel:
                result[url] = tid_intel[tid]
        return result
    except Exception as e:
        logger.warning("[notify] intel fetch by URLs failed: %s", e)
        return {}


# =============================================================================
# MESSAGE BUILDERS
# =============================================================================

def _build_tender_block(idx: int, t: Dict) -> tuple:
    """
    Build one tender entry. Returns (html_block, plain_lines) or ("", []).

    Visual format:
        🔥 #1 · Score 87 · 7 Apr
        GIZ India
        Climate Policy Consulting in India   ← hyperlinked title
    """
    title = (t.get("title") or "").strip()
    if not title:
        return "", []

    url      = (t.get("url") or t.get("link") or "").strip()
    deadline = _format_deadline(t.get("deadline") or t.get("end_date") or "")
    score    = int(t.get("priority_score") or t.get("quality_score") or 0)
    source   = (
        t.get("client") or t.get("organization") or
        t.get("source_site") or t.get("source") or ""
    ).strip()

    emoji      = _score_emoji(score)
    title_s    = title[:80] + ("…" if len(title) > 80 else "")
    source_s   = source[:50] if source else ""

    # ── Score line ────────────────────────────────────────────────────────────
    meta_parts = [f"#{idx}"]
    if score:    meta_parts.append(f"Score {score}")
    if deadline: meta_parts.append(deadline)
    meta_str = " · ".join(meta_parts)

    # ── Title line (linked if URL available) ─────────────────────────────────
    if url:
        title_html = f'<a href="{_esc(url)}">{_esc(title_s)}</a>'
    else:
        title_html = f"<b>{_esc(title_s)}</b>"

    # ── HTML block ────────────────────────────────────────────────────────────
    lines = [f"{emoji} <b>{_esc(meta_str)}</b>"]
    if source_s:
        lines.append(f"<i>{_esc(source_s)}</i>")
    lines.append(title_html)
    html_block = "\n".join(lines)

    # ── Plain text (for WhatsApp) ─────────────────────────────────────────────
    plain = [f"{emoji} {meta_str}"]
    if source_s: plain.append(f"  {source_s}")
    plain.append(f"  {title_s}")
    if url:      plain.append(f"  {url}")
    plain.append("")

    return html_block, plain


def _build_opportunity_message(
    ranked_tenders: List[Dict],
    total_found: int,
    fallback: bool = False,   # kept for API compat
) -> tuple:
    """
    Build (plain_text, html_pages) for the opportunity alert.

    Layout:
        🎯 IDCG TenderRadar  •  21 Mar 2026  18:30
        12 new tenders — top 10 shown
        ─────────────────────
        🔥 #1 · Score 87 · 7 Apr
        GIZ India
        Climate Policy Consulting in India
        ─────────────────────
        ⭐ #2 · Score 74 · 15 Apr
        ...
        ─────────────────────
        ⚡ Next scan ~6 hrs
    """
    date_str   = datetime.now().strftime("%-d %b %Y  %H:%M")
    divider    = "─────────────────────"
    footer_html = f"\n{divider}\n⚡ <i>Next scan ~6 hrs  •  TenderRadar</i>"

    tender_count = len(ranked_tenders)
    shown_count  = min(tender_count, MAX_SEND)

    # ── Header ────────────────────────────────────────────────────────────────
    sub = f"{total_found} new tender{'s' if total_found != 1 else ''}"
    if total_found > MAX_SEND:
        sub += f" — top {MAX_SEND} shown"

    header_html = (
        f"🎯 <b>IDCG TenderRadar</b>  •  <i>{_esc(date_str)}</i>\n"
        f"{_esc(sub)}"
    )

    plain_lines = [
        f"🎯 IDCG TenderRadar  •  {date_str}",
        sub,
        "",
    ]

    # ── Build blocks ──────────────────────────────────────────────────────────
    all_blocks: List[str] = [divider]

    for idx, t in enumerate(ranked_tenders[:MAX_SEND], 1):
        html_block, plain = _build_tender_block(idx, t)
        if html_block:
            all_blocks.append(html_block)
            all_blocks.append(divider)
            plain_lines.extend(plain)

    # ── Paginate (Telegram 4096 char limit) ───────────────────────────────────
    html_pages: List[str] = []
    current    = header_html

    for block in all_blocks:
        sep       = "\n"
        candidate = current + sep + block
        if len(candidate) + len(footer_html) + 4 > MAX_MSG_CHARS:
            html_pages.append(current.strip())
            current = block
        else:
            current = candidate

    # Attach footer to final page
    if current.strip():
        # If current ends with a divider already, don't add another
        final = current.rstrip()
        if not final.endswith(divider):
            final += footer_html
        else:
            final += f"\n⚡ <i>Next scan ~6 hrs  •  TenderRadar</i>"
        html_pages.append(final.strip())

    if not html_pages:
        html_pages = [(header_html + "\n" + divider + footer_html).strip()]

    plain_lines.append("⚡ Next scan ~6 hrs — TenderRadar")
    return "\n".join(plain_lines), html_pages


def _build_zero_tenders_message() -> tuple:
    text = "No new tenders detected in this cycle."
    return text, [text]


# =============================================================================
# TRANSPORT LAYER
# =============================================================================

def _send_telegram(html_pages: list) -> bool:
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN in ("YOUR_BOT_TOKEN_HERE", ""):
        logger.warning("[notify] Telegram token not configured — skipping.")
        return False

    if isinstance(html_pages, str):
        html_pages = [html_pages]

    any_ok = False
    for i, page in enumerate(html_pages, 1):
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id":                  TELEGRAM_CHAT_ID,
                    "text":                     page,
                    "parse_mode":               "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=20,
            )
            if resp.status_code == 200 and resp.json().get("ok"):
                logger.info("[notify] Telegram — page %d/%d sent OK", i, len(html_pages))
                any_ok = True
            else:
                logger.error("[notify] Telegram page %d — HTTP %d: %s",
                             i, resp.status_code, resp.text[:200])
        except Exception as e:
            logger.error("[notify] Telegram page %d error: %s", i, e)

    return any_ok


def _send_whatsapp_twilio(plain_text: str) -> bool:
    if not TWILIO_ACCOUNT_SID or TWILIO_ACCOUNT_SID in ("YOUR_ACCOUNT_SID", ""):
        logger.warning("[notify] Twilio not configured — skipping WhatsApp.")
        return False

    try:
        from twilio.rest import Client
    except ImportError:
        logger.warning("[notify] twilio not installed — run: pip3 install twilio")
        return False

    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    any_ok = False

    for number in (WHATSAPP_NUMBERS or []):
        for chunk in _chunk_message(plain_text, WA_MAX_CHARS):
            try:
                msg = client.messages.create(
                    body  = chunk,
                    from_ = TWILIO_WHATSAPP_FROM,
                    to    = f"whatsapp:{number}",
                )
                logger.info("[notify] WhatsApp → %s SID: %s", number, msg.sid)
                any_ok = True
            except Exception as e:
                logger.error("[notify] WhatsApp → %s FAILED: %s", number, e)

    return any_ok


def _chunk_message(text: str, max_chars: int) -> list:
    if len(text) <= max_chars:
        return [text]
    lines, chunks, current = text.split("\n"), [], ""
    for line in lines:
        if len(current) + len(line) + 1 > max_chars:
            if current:
                chunks.append(current.strip())
            current = line + "\n"
        else:
            current += line + "\n"
    if current.strip():
        chunks.append(current.strip())
    return chunks


# =============================================================================
# PORTAL BALANCING  (max 3 per source to avoid one portal dominating)
# =============================================================================

def _balance_portals(
    tenders: List[Dict],
    max_per_source: int = 3,
    total_cap: int = MAX_SEND,
) -> List[Dict]:
    source_buckets: Dict[str, List[Dict]] = {}
    for t in tenders:
        src = str(
            t.get("source_site") or t.get("source") or
            t.get("source_portal") or "unknown"
        ).lower()
        source_buckets.setdefault(src, []).append(t)

    ordered_sources = sorted(
        source_buckets.keys(),
        key=lambda s: -int(source_buckets[s][0].get("priority_score") or 0),
    )

    result:   List[Dict] = []
    pointers: Dict[str, int] = {s: 0 for s in ordered_sources}

    while len(result) < total_cap:
        added = False
        for src in ordered_sources:
            if len(result) >= total_cap:
                break
            already = sum(
                1 for t in result
                if str(t.get("source_site") or t.get("source") or
                       t.get("source_portal") or "unknown").lower() == src
            )
            if already >= max_per_source:
                continue
            idx = pointers[src]
            if idx < len(source_buckets[src]):
                result.append(source_buckets[src][idx])
                pointers[src] = idx + 1
                added = True
        if not added:
            break

    return result


# =============================================================================
# PUBLIC INTERFACE
# =============================================================================

def notify_all(**portal_tenders) -> bool:
    """
    Primary entry point. Accepts new_<flag>=list_of_tender_dicts kwargs.
    Fires Telegram (primary) + WhatsApp/Twilio (if configured).
    Returns True if at least one channel succeeded.
    """
    all_tenders: List[Dict] = []
    for key, val in portal_tenders.items():
        if isinstance(val, list):
            all_tenders.extend(val)

    total = len(all_tenders)

    if total == 0:
        logger.info("[notify] No new tenders — nothing to send.")
        return True

    logger.info("[notify] %d new tender(s) — building alert...", total)

    # Enrich from DB
    intel_map = _fetch_intel_by_tender_ids(all_tenders)
    enriched: List[Dict] = []
    for t in all_tenders:
        tid   = _resolve_tid(t)
        intel = intel_map.get(tid, {})
        row   = dict(t)
        if intel:
            row["priority_score"]      = intel.get("priority_score") or row.get("priority_score") or 0
            row["bid_fit_score"]       = intel.get("bid_fit_score")  or row.get("relevance_score") or 0
            row["opportunity_insight"] = intel.get("opportunity_insight", "")
            if intel.get("client_org") and not row.get("client"):
                row["client"] = intel["client_org"]
            if intel.get("decision_tag") and not row.get("decision_tag"):
                row["decision_tag"] = intel["decision_tag"]
        enriched.append(row)

    # Sort by score descending
    _tier_order = {"BID_NOW": 0, "STRONG_CONSIDER": 1, "WEAK_CONSIDER": 2}
    enriched.sort(key=lambda x: (
        _tier_order.get(x.get("decision_tag", ""), 3),
        -int(x.get("priority_score") or x.get("quality_score") or 0),
        -int(x.get("bid_fit_score")  or x.get("relevance_score") or 0),
    ))

    balanced = _balance_portals(enriched, max_per_source=3, total_cap=MAX_SEND)

    plain_text, html_pages = _build_opportunity_message(balanced, total_found=total)

    any_ok = False
    if _send_telegram(html_pages):   any_ok = True
    if _send_whatsapp_twilio(plain_text): any_ok = True
    return any_ok


def send_rich_alert(enriched_tenders: list) -> bool:
    """Entry point from intelligence layer (EnrichedTender objects)."""
    if not enriched_tenders:
        return True

    raw: List[Dict] = []
    for t in enriched_tenders:
        url    = getattr(t, "url", "") or ""
        title  = getattr(t, "title", "") or ""
        source = getattr(t, "source", "") or ""
        dl     = getattr(t, "deadline", "") or ""
        fit    = getattr(t, "fit_score", 0) or 0
        client = ""
        ext    = getattr(t, "extraction", None)
        if ext:
            client = getattr(ext, "client_org", "") or ""
            if not dl:
                dl = getattr(ext, "deadline", "") or ""
        raw.append({
            "title":          title,
            "url":            url,
            "client":         client or source.replace("_", " ").title(),
            "deadline":       dl,
            "priority_score": 0,
            "bid_fit_score":  fit,
        })

    total     = len(raw)
    urls      = [r["url"] for r in raw if r["url"]]
    intel_map = _fetch_intel_by_urls(urls)

    enriched: List[Dict] = []
    for row in raw:
        intel = intel_map.get(row["url"], {})
        if intel:
            row["priority_score"] = intel.get("priority_score") or row["priority_score"]
            row["bid_fit_score"]  = intel.get("bid_fit_score")  or row["bid_fit_score"]
            if intel.get("client_org") and not row.get("client"):
                row["client"] = intel["client_org"]
        enriched.append(row)

    enriched.sort(key=lambda x: (
        -int(x.get("priority_score") or 0),
        -int(x.get("bid_fit_score")  or 0),
    ))

    balanced = _balance_portals(enriched, max_per_source=3, total_cap=MAX_SEND)
    _, html_pages = _build_opportunity_message(balanced, total_found=total)

    logger.info("[notify] send_rich_alert — %d tenders, %d page(s)", len(balanced), len(html_pages))
    return _send_telegram(html_pages)


# =============================================================================
# AMENDMENT ALERT — sent when a tracked tender changes its document content
# =============================================================================

def send_amendment_alert(amended_tenders: List[Dict]) -> bool:
    """
    Send a Telegram alert when deep-scraped tenders detect document changes
    (scope, budget, deadline extensions, addenda, etc.).

    Called automatically from main.py after every deep-enrichment cycle
    whenever amendment_detected=True appears in the batch results.

    Format:
        📝 AMENDMENT ALERT  •  1 Apr 2026  18:30
        2 tender(s) updated their documents — review before bidding
        ─────────────────────
        📝 #1  Amendment #2  •  7 Apr
        GIZ India
        Climate Policy Consulting ← linked title
        ─────────────────────
        ⚠️ Review document changes before your next bid decision

    Args:
        amended_tenders: list of deep-data dicts where amendment_detected=True.
                         Each dict needs: title, url, deadline, organization /
                         source_site / source (for label), amendment_count.

    Returns:
        True if Telegram accepted at least one message.
    """
    if not amended_tenders:
        return True

    date_str = datetime.now().strftime("%-d %b %Y  %H:%M")
    divider  = "─────────────────────"
    n        = len(amended_tenders)

    html_lines: List[str] = [
        f"📝 <b>AMENDMENT ALERT</b>  •  <i>{_esc(date_str)}</i>",
        f"{n} tender{'s' if n != 1 else ''} updated their documents — "
        f"review before bidding",
        divider,
    ]

    for idx, t in enumerate(amended_tenders, 1):
        title    = (t.get("title")        or "").strip()[:80]
        url      = (t.get("url")          or "").strip()
        deadline = _format_deadline(
            t.get("deadline") or t.get("deadline_raw") or
            t.get("deep_deadline_raw") or ""
        )
        source   = (
            t.get("organization")  or
            t.get("client")        or
            t.get("source_site")   or
            t.get("source_portal") or
            t.get("source")        or ""
        ).strip()[:60]
        count    = int(t.get("amendment_count") or 1)

        # ── Meta line ─────────────────────────────────────────────────────────
        meta_parts = [f"#{idx}", f"Amendment #{count}"]
        if deadline:
            meta_parts.append(deadline)
        meta_str = " · ".join(meta_parts)

        # ── Title link ────────────────────────────────────────────────────────
        title_esc  = _esc(title) if title else "(no title)"
        title_html = (f'<a href="{_esc(url)}">{title_esc}</a>'
                      if url else f"<b>{title_esc}</b>")

        html_lines.append(f"📝 <b>{_esc(meta_str)}</b>")
        if source:
            html_lines.append(f"<i>{_esc(source)}</i>")
        html_lines.append(title_html)
        html_lines.append(divider)

    html_lines.append(
        "⚠️ <i>Document content has changed — check for scope, "
        "budget, deadline or eligibility updates before bidding.</i>"
    )

    html = "\n".join(html_lines)

    # Respect 4096-char limit — paginate if somehow over (edge case only)
    pages = _chunk_message(html, MAX_MSG_CHARS)
    sent  = _send_telegram(pages)

    if sent:
        logger.info(
            "[notify] Amendment alert sent for %d tender(s): %s",
            n,
            ", ".join((t.get("title") or "")[:30] for t in amended_tenders),
        )
    else:
        logger.warning("[notify] Amendment alert FAILED to send")

    return sent


# =============================================================================
# SELF-TEST  —  run:  python3 -m notifier.telegram_notifier
# =============================================================================

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print("=== TenderRadar Notifier — self-test ===\n")

    fake = [
        {
            "tender_id": "9001",
            "title": "Hiring of M&E Consultant for PMGSY Road Project — Phase IV",
            "deadline": "2026-04-15",
            "url": "https://projects.worldbank.org/en/projects-operations/project-detail/P123456",
            "client": "World Bank",
            "priority_score": 88,
            "bid_fit_score": 90,
        },
        {
            "tender_id": "9002",
            "title": "RFP — Impact Evaluation of Swachh Bharat Mission (Gramin)",
            "deadline": "2026-04-25",
            "url": "https://ngobox.org/full_rfp_eoi_12345.php",
            "client": "Ministry of Jal Shakti",
            "priority_score": 72,
            "bid_fit_score": 75,
        },
        {
            "tender_id": "9003",
            "title": "Technical Assistance for Climate Finance in South Asia",
            "deadline": "2026-05-10",
            "url": "https://www.giz.de/en/tenders/12345.html",
            "client": "GIZ India",
            "priority_score": 65,
            "bid_fit_score": 68,
        },
        {
            "tender_id": "9004",
            "title": "Supply of Stationery Items — District Office Procurement",
            "deadline": "2026-04-10",
            "url": "https://example.gov.in/tender/789",
            "client": "District Collectorate",
            "priority_score": 18,
            "bid_fit_score": 20,
        },
    ]

    plain, pages = _build_opportunity_message(fake, total_found=4)
    print(pages[0])
    print(f"\nPages: {len(pages)} | Plain chars: {len(plain)}")
    print("\n--- Plain text preview ---")
    print(plain)
