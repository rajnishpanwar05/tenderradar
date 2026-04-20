# =============================================================================
# intelligence/fuzzy_dedup.py — Cross-Portal Fuzzy Deduplication & Merging
#
# Problem the old hash dedup had:
#   MD5(normalised_title + org) = same portal, same wording → dedup
#   But the SAME tender on World Bank + UNGM + AfDB has slightly different
#   titles → 3 separate records, 2 marked duplicate but NOT merged.
#
# What this module does:
#   1. DETECT — fuzzy title similarity (≥85%) + same org + deadline within 7d
#      → identifies the same tender across portals
#   2. MERGE — build ONE canonical record by picking the richest field from
#      each source (longest description, earliest deadline, first budget, etc.)
#   3. TRACK — store all source portals/URLs so the UI can deep-link to each
#   4. MARK — keep one canonical row in `tenders`, mark others as duplicate_of
#
# Algorithm:
#   - Levenshtein distance (via difflib) for similarity — no extra deps
#   - Optional: sentence-transformers cosine similarity as a second pass
#     for tenders with very different surface forms but identical meaning
#
# DB tables used:
#   tenders                — canonical record updated in-place
#   tender_cross_sources   — new table: all portal URLs per canonical tender
#
# =============================================================================

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from typing import Optional

logger = logging.getLogger("tenderradar.fuzzy_dedup")

# ── Tuning knobs ─────────────────────────────────────────────────────────────
_TITLE_SIM_THRESHOLD     = 0.82   # SequenceMatcher ratio — 0.82 ≈ 82% similar
_DEADLINE_WINDOW_DAYS    = 7      # deadlines within 7 days = probably the same tender
_MIN_TITLE_LEN           = 15     # shorter titles are too generic to deduplicate on
_EMBEDDING_THRESHOLD     = 0.90   # cosine similarity threshold (optional second pass)

_GENERIC_TITLE_PHRASES = {
    "construction work",
    "civil works",
    "consultancy services",
    "selection of agency",
}

_SOURCE_FAMILY = {
    "worldbank": "worldbank",
    "wb": "worldbank",
    "undp": "un",
    "ungm": "un",
    "giz": "giz",
    "dtvp": "giz",
    "ec": "eu",
    "tedeu": "eu",
}


# =============================================================================
# Schema bootstrap — called from db.init_db() at API/runner startup
# =============================================================================

def init_cross_sources_schema() -> None:
    """
    Ensure the tender_cross_sources table exists.
    Idempotent — safe to call on every startup.
    Called automatically from database.db.init_db() so the table is always
    available before any scraper run or API request touches it.
    """
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tender_cross_sources (
                id          INT AUTO_INCREMENT PRIMARY KEY,
                tender_id   VARCHAR(255) NOT NULL,
                portal      VARCHAR(100) DEFAULT '',
                url         TEXT,
                added_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_cs_tender_id (tender_id)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        # Backward/forward-compatible richer columns for source-level evidence.
        for col_name, col_def in [
            ("source_portal", "VARCHAR(100) DEFAULT ''"),
            ("source_url", "TEXT"),
            ("unique_fields", "JSON"),
            ("detected_at", "DATETIME DEFAULT CURRENT_TIMESTAMP"),
        ]:
            cur.execute(
                """
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME   = 'tender_cross_sources'
                  AND COLUMN_NAME  = %s
                """,
                (col_name,),
            )
            if not (cur.fetchone() or (0,))[0]:
                cur.execute(f"ALTER TABLE tender_cross_sources ADD COLUMN {col_name} {col_def};")
        conn.commit()
        cur.close()
        conn.close()
        logger.debug("[fuzzy_dedup] tender_cross_sources table ready.")
    except Exception as e:
        logger.warning("[fuzzy_dedup] init_cross_sources_schema warning (non-fatal): %s", e)


# =============================================================================
# Text normalisation helpers
# =============================================================================

def _normalise_title(title: str) -> str:
    """
    Normalise a tender title for comparison:
    - lowercase
    - remove reference numbers (e.g. "IND/2024/001", "P123456")
    - remove common noise words
    - collapse whitespace
    """
    t = str(title or "").lower()
    # Remove reference numbers
    t = re.sub(r"\b[a-z]{0,4}\d{3,}[\w/\-]*\b", " ", t)
    # Remove common noise phrases
    noise = [
        r"\brequest for (proposal|quotation|tender|eoi|information)\b",
        r"\bexpression of interest\b",
        r"\bsolicitation no\.?",
        r"\bnotice of\b",
        r"\bprocurement of\b",
        r"\bservices for\b",
        r"\bassignment\b",
    ]
    for pattern in noise:
        t = re.sub(pattern, " ", t, flags=re.IGNORECASE)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _normalise_org(org: str) -> str:
    """Normalise organisation name for comparison."""
    o = str(org or "").lower()
    o = re.sub(r"\b(ltd|limited|inc|corp|pvt|private|the|of|and)\b", " ", o)
    o = re.sub(r"[^\w\s]", " ", o)
    o = re.sub(r"\s+", " ", o).strip()
    return o


def _source_family(portal: str) -> str:
    return _SOURCE_FAMILY.get(str(portal or "").strip().lower(), str(portal or "").strip().lower())


def _is_generic_title(title: str) -> bool:
    norm = _normalise_title(title)
    if not norm:
        return True
    if norm in _GENERIC_TITLE_PHRASES:
        return True
    tokens = norm.split()
    return len(tokens) <= 2 and any(tok in {"work", "works", "consultancy", "service", "services"} for tok in tokens)


def _blocking_key(title: str, n_words: int = 3) -> str:
    """
    Return first n_words of normalised title for use as a blocking key.
    Buckets tenders by shared prefix so we only run SequenceMatcher within
    the same bucket — reduces O(n*m) to O(n * avg_bucket_size).
    Returns "" for titles too short/generic to block on.
    """
    norm = _normalise_title(title)
    if len(norm) < _MIN_TITLE_LEN:
        return ""
    tokens = norm.split()
    return " ".join(tokens[:n_words])


def _title_similarity(a: str, b: str) -> float:
    """
    Compute fuzzy similarity between two normalised titles.
    Returns a float in [0.0, 1.0].
    """
    na = _normalise_title(a)
    nb = _normalise_title(b)
    if not na or not nb:
        return 0.0
    if len(na) < _MIN_TITLE_LEN or len(nb) < _MIN_TITLE_LEN:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


def _deadline_close(d1, d2, window_days: int = _DEADLINE_WINDOW_DAYS) -> bool:
    """
    Return True if two deadlines are within `window_days` of each other,
    or if either is None (unknown deadline is treated as non-conflicting).
    """
    if d1 is None or d2 is None:
        return True
    try:
        if not isinstance(d1, datetime):
            d1 = datetime.strptime(str(d1)[:10], "%Y-%m-%d")
        if not isinstance(d2, datetime):
            d2 = datetime.strptime(str(d2)[:10], "%Y-%m-%d")
        return abs((d1 - d2).days) <= window_days
    except Exception:
        return True   # unparseable deadline = non-conflicting


def _is_same_tender(a: dict, b: dict) -> bool:
    """
    Decide if two tender dicts refer to the same real-world opportunity.

    Criteria (ALL must pass):
      1. Title similarity ≥ _TITLE_SIM_THRESHOLD
      2. Same organisation (fuzzy, ≥ 0.70) OR one is unknown
      3. Deadline within _DEADLINE_WINDOW_DAYS (or either unknown)
    """
    if _is_generic_title(a.get("title", "")) or _is_generic_title(b.get("title", "")):
        return False

    sim = _title_similarity(a.get("title", ""), b.get("title", ""))
    if sim < _TITLE_SIM_THRESHOLD:
        return False

    org_a = _normalise_org(a.get("organization") or a.get("org") or "")
    org_b = _normalise_org(b.get("organization") or b.get("org") or "")
    family_a = _source_family(a.get("source_portal") or a.get("source_site") or "")
    family_b = _source_family(b.get("source_portal") or b.get("source_site") or "")
    if org_a and org_b:
        org_sim = SequenceMatcher(None, org_a, org_b).ratio()
        if org_sim < 0.65:
            return False
    elif family_a != family_b:
        return False

    deadline_a = a.get("deadline") or a.get("deadline_raw")
    deadline_b = b.get("deadline") or b.get("deadline_raw")
    if not _deadline_close(deadline_a, deadline_b):
        return False

    return True


def _extract_unique_fields(row: dict) -> dict:
    """
    Keep a compact set of source-specific fields so GPT/exports can surface
    unique evidence across portals without rereading the original portal pages.
    """
    keep = {}
    for key in (
        "organization", "country", "deadline", "deadline_raw", "description",
        "estimated_budget_usd", "budget", "notice_type", "source_portal",
        "deep_scope", "deep_eval_criteria", "deep_team_reqs",
    ):
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, (datetime, date)):
            value = value.isoformat()
        text = str(value).strip() if not isinstance(value, (dict, list)) else value
        if text not in ("", [], {}, "unknown", "Unknown"):
            keep[key] = value
    return keep


def _insert_cross_source(cur, tender_id: str, portal: str, url: str, row: Optional[dict] = None) -> None:
    if not tender_id or not url:
        return
    cur.execute(
        """
        SELECT 1 FROM tender_cross_sources
        WHERE tender_id = %s AND portal = %s AND url = %s
        LIMIT 1
        """,
        (tender_id, portal, url),
    )
    if cur.fetchone():
        return
    unique_fields = json.dumps(_extract_unique_fields(row or {}), ensure_ascii=False)
    cur.execute(
        """
        INSERT INTO tender_cross_sources
            (tender_id, portal, url, source_portal, source_url, unique_fields, detected_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """,
        (tender_id, portal, url, portal, url, unique_fields),
    )


# =============================================================================
# Merge strategy — pick the richest field from multiple sources
# =============================================================================

def _pick_best(values: list, prefer_longer: bool = True):
    """
    From a list of candidate values, return the 'best' non-null one.
    prefer_longer=True: pick the longest non-empty string.
    prefer_longer=False: pick the first non-null value.
    """
    non_null = [v for v in values if v is not None and str(v).strip() not in ("", "unknown", "Unknown")]
    if not non_null:
        return None
    if prefer_longer:
        return max(non_null, key=lambda x: len(str(x)))
    return non_null[0]


def _earliest_deadline(values: list):
    """Return the earliest parseable deadline from a list."""
    dates = []
    for v in values:
        if v is None:
            continue
        try:
            d = datetime.strptime(str(v)[:10], "%Y-%m-%d")
            dates.append(d)
        except Exception:
            pass
    return min(dates).date().isoformat() if dates else None


def merge_tender_group(tenders: list[dict]) -> dict:
    """
    Merge a group of duplicate tenders into one canonical record.

    Strategy:
      - title:         longest / most descriptive
      - description:   longest
      - deadline:      earliest confirmed
      - organization:  most specific (longest non-generic)
      - budget_usd:    first non-null
      - country:       first non-null
      - sectors:       UNION of all sector lists
      - service_types: UNION of all service_type lists
      - sources:       list of all {portal, url} entries
      - canonical_id:  tender_id of the record that was seen FIRST

    Returns the merged dict.
    """
    if not tenders:
        return {}
    if len(tenders) == 1:
        merged = dict(tenders[0])
        merged["sources"] = [{
            "portal": merged.get("source_portal") or merged.get("source_site", ""),
            "url":    merged.get("url", ""),
        }]
        return merged

    # Sort by date_first_seen ascending — the oldest is the canonical record
    def _seen_key(t):
        raw = t.get("date_first_seen") or t.get("scraped_at") or ""
        try:
            return datetime.fromisoformat(str(raw)[:19])
        except Exception:
            return datetime(2099, 1, 1)

    tenders_sorted = sorted(tenders, key=_seen_key)
    canonical = tenders_sorted[0]

    # Collect field candidates
    titles       = [t.get("title")        for t in tenders]
    descriptions = [t.get("description")  for t in tenders]
    orgs         = [t.get("organization") or t.get("org") for t in tenders]
    deadlines    = [t.get("deadline")     for t in tenders]
    budgets      = [t.get("estimated_budget_usd") for t in tenders]
    countries    = [t.get("country")      for t in tenders]

    # Union sector + service_type arrays
    all_sectors: set = set()
    all_service_types: set = set()
    for t in tenders:
        for s in _parse_json_list(t.get("sectors")):
            if s and s.lower() not in ("unknown", ""):
                all_sectors.add(s)
        for s in _parse_json_list(t.get("service_types")):
            if s and s.lower() not in ("unknown", ""):
                all_service_types.add(s)

    # Build source list
    sources = []
    seen_urls: set = set()
    for t in tenders:
        portal = t.get("source_portal") or t.get("source_site") or ""
        url    = t.get("url") or ""
        if url and url not in seen_urls:
            sources.append({"portal": portal, "url": url})
            seen_urls.add(url)

    merged = dict(canonical)
    merged["title"]               = _pick_best(titles, prefer_longer=True) or canonical.get("title", "")
    merged["description"]         = _pick_best(descriptions, prefer_longer=True) or ""
    merged["organization"]        = _pick_best(orgs, prefer_longer=True) or ""
    merged["deadline"]            = _earliest_deadline(deadlines)
    merged["estimated_budget_usd"]= _pick_best(budgets, prefer_longer=False)
    merged["country"]             = _pick_best(countries, prefer_longer=False)
    merged["sectors"]             = json.dumps(sorted(all_sectors))
    merged["service_types"]       = json.dumps(sorted(all_service_types))
    merged["sources"]             = sources
    merged["duplicate_count"]     = len(tenders) - 1
    merged["source_portals"]      = [s["portal"] for s in sources]

    logger.debug(
        "[fuzzy_dedup] Merged %d tenders into canonical '%s' (portals: %s)",
        len(tenders),
        str(merged.get("title", ""))[:50],
        ", ".join(merged["source_portals"]),
    )
    return merged


def _parse_json_list(val) -> list:
    if val is None:
        return []
    if isinstance(val, list):
        return val
    try:
        parsed = json.loads(val)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


# =============================================================================
# Cross-portal deduplication — main entry point
# =============================================================================

def deduplicate_batch(tenders: list[dict]) -> list[dict]:
    """
    Detect and merge duplicates within a batch of tenders.

    Uses O(n²) pairwise comparison — acceptable for batches ≤ 2000.
    For larger sets, call `deduplicate_against_db()` instead.

    Returns:
        List of canonical tenders with duplicates merged.
        Each canonical tender has a `sources` field listing all portals.
    """
    if not tenders:
        return []

    n         = len(tenders)
    used      = [False] * n
    groups: list[list[int]] = []

    # Safety cap: O(n²) is only tractable for small batches.
    # For large batches, skip intra-batch dedup — the DB dedup pass will catch
    # cross-run duplicates. Intra-run duplication at this scale is negligible.
    if n > 2000:
        logger.warning(
            "[fuzzy_dedup] deduplicate_batch: batch too large (%d > 2000) "
            "— skipping O(n²) intra-batch dedup to avoid hang. "
            "Use deduplicate_against_db() for large sets.", n
        )
        return tenders

    # Build blocking index so we only compare within the same 3-word bucket
    _idx: dict[str, list[int]] = defaultdict(list)
    _generic_idx: list[int]    = []
    for k, t in enumerate(tenders):
        bkey = _blocking_key(t.get("title", ""))
        if bkey:
            _idx[bkey].append(k)
        else:
            _generic_idx.append(k)

    for i in range(n):
        if used[i]:
            continue
        group = [i]
        used[i] = True
        bkey = _blocking_key(tenders[i].get("title", ""))
        candidates = (_idx.get(bkey, []) + _generic_idx) if bkey else _generic_idx
        for j in candidates:
            if j <= i or used[j]:
                continue
            if _is_same_tender(tenders[i], tenders[j]):
                group.append(j)
                used[j] = True
        groups.append(group)

    result = []
    merged_count = 0
    for group in groups:
        group_tenders = [tenders[idx] for idx in group]
        merged = merge_tender_group(group_tenders)
        result.append(merged)
        if len(group) > 1:
            merged_count += len(group) - 1

    if merged_count:
        logger.info(
            "[fuzzy_dedup] deduplicate_batch: %d→%d (eliminated %d cross-portal duplicates)",
            n, len(result), merged_count,
        )
    return result


def deduplicate_against_db(
    new_tenders: list[dict],
    lookback_days: int = 60,
) -> tuple[list[dict], list[dict]]:
    """
    Deduplicate `new_tenders` against recently seen tenders in the DB.

    For each new tender, checks against tenders seen in the last
    `lookback_days`. If a match is found, the DB record is updated with
    any richer fields from the new tender rather than creating a duplicate.

    Returns:
        (unique_tenders, merged_updates)
        - unique_tenders: tenders that are genuinely new (no DB match)
        - merged_updates: list of {tender_id, updates} for existing DB records
                          that got enriched by the new tender
    """
    if not new_tenders:
        return [], []

    # Fetch recent tenders from DB for comparison
    db_tenders: list[dict] = []
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        cur.execute("""
            SELECT tender_id, title, organization, deadline, source_portal,
                   url, description, estimated_budget_usd, country
            FROM tenders
            WHERE scraped_at >= %s
            ORDER BY scraped_at DESC
            LIMIT 5000
        """, (cutoff,))
        db_tenders = cur.fetchall() or []
        cur.close()
        conn.close()
        logger.debug("[fuzzy_dedup] Loaded %d DB tenders for comparison", len(db_tenders))
    except Exception as e:
        logger.warning("[fuzzy_dedup] Could not load DB tenders: %s — skipping DB dedup", e)
        return new_tenders, []

    # ── Build blocking index to avoid O(n*m) brute-force comparison ──────────
    # Group DB tenders by their 3-word normalised title prefix.
    # This reduces 11k × 5k = 55M comparisons to ~11k × avg_bucket_size (≈3-10).
    _db_index: dict[str, list] = defaultdict(list)
    _db_index_fallback: list   = []   # tenders whose title is too generic to bucket
    for db_t in db_tenders:
        bkey = _blocking_key(db_t.get("title", ""))
        if bkey:
            _db_index[bkey].append(db_t)
        else:
            _db_index_fallback.append(db_t)

    unique:  list[dict] = []
    updates: list[dict] = []

    for new_t in new_tenders:
        new_bkey   = _blocking_key(new_t.get("title", ""))
        candidates = _db_index.get(new_bkey, []) + _db_index_fallback if new_bkey else _db_index_fallback
        matched_db = None
        for db_t in candidates:
            if _is_same_tender(new_t, db_t):
                matched_db = db_t
                break

        if matched_db is None:
            unique.append(new_t)
        else:
            # Found a DB match — check if new tender has richer fields
            enrichments: dict = {}
            if (len(str(new_t.get("description") or ""))
                    > len(str(matched_db.get("description") or ""))):
                enrichments["description"] = new_t["description"]

            if new_t.get("estimated_budget_usd") and not matched_db.get("estimated_budget_usd"):
                enrichments["estimated_budget_usd"] = new_t["estimated_budget_usd"]

            if new_t.get("deadline") and not matched_db.get("deadline"):
                enrichments["deadline"] = new_t["deadline"]

            # Track new source portal
            enrichments["_new_source"] = {
                "portal": new_t.get("source_portal") or new_t.get("source_site", ""),
                "url":    new_t.get("url", ""),
            }

            updates.append({
                "tender_id":  matched_db["tender_id"],
                "updates":    enrichments,
                "matched_by": "fuzzy_title_org_deadline",
            })

            logger.debug(
                "[fuzzy_dedup] Matched '%s' → existing '%s' (portals: %s + %s)",
                str(new_t.get("title", ""))[:40],
                str(matched_db.get("title", ""))[:40],
                matched_db.get("source_portal", ""),
                new_t.get("source_portal") or new_t.get("source_site", ""),
            )

    if updates:
        logger.info(
            "[fuzzy_dedup] deduplicate_against_db: %d new, %d matched existing DB records",
            len(unique), len(updates),
        )

    return unique, updates


def apply_db_merges(merged_updates: list[dict]) -> int:
    """
    Write enrichments from deduplicate_against_db() back to the DB.

    For each matched tender:
    - Updates description, budget, deadline if richer in the new source
    - Appends the new source portal to tender_cross_sources table

    Returns count of successfully updated records.
    """
    if not merged_updates:
        return 0

    updated = 0
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor()

        # Ensure tender_cross_sources table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tender_cross_sources (
                id          INT AUTO_INCREMENT PRIMARY KEY,
                tender_id   VARCHAR(255) NOT NULL,
                portal      VARCHAR(100) DEFAULT '',
                url         TEXT,
                added_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_tender_id (tender_id)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        conn.commit()

        for item in merged_updates:
            tender_id = item["tender_id"]
            updates   = item.get("updates", {})
            new_src   = updates.pop("_new_source", None)

            # Apply field enrichments
            set_parts: list = []
            params:    list = []
            if updates.get("description"):
                set_parts.append("description = %s")
                params.append(updates["description"])
            if updates.get("estimated_budget_usd"):
                set_parts.append("estimated_budget_usd = %s")
                params.append(updates["estimated_budget_usd"])
            if updates.get("deadline"):
                set_parts.append("deadline = %s")
                params.append(updates["deadline"])

            if set_parts:
                params.append(tender_id)
                cur.execute(
                    f"UPDATE tenders SET {', '.join(set_parts)} WHERE tender_id = %s",
                    params,
                )

            # Record the new source portal
            if new_src and new_src.get("url"):
                _insert_cross_source(
                    cur,
                    tender_id,
                    new_src.get("portal", ""),
                    new_src.get("url", ""),
                    updates,
                )

            conn.commit()
            updated += 1

        cur.close()
        conn.close()

    except Exception as e:
        logger.error("[fuzzy_dedup] apply_db_merges error: %s", e)

    return updated


def backfill_cross_source_groups(limit_groups: int = 500) -> int:
    """
    Build high-confidence historical cross-source groups from existing tenders.
    """
    try:
        from database.db import get_connection
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT tender_id, title, title_clean, organization, deadline, deadline_raw,
                   source_portal, url, description, estimated_budget_usd, country,
                   sectors, service_types, scraped_at
            FROM tenders
            WHERE COALESCE(NULLIF(TRIM(title_clean), ''), NULLIF(TRIM(title), '')) IS NOT NULL
            ORDER BY scraped_at DESC
            """
        )
        rows = cur.fetchall() or []

        buckets: dict[str, list[dict]] = {}
        for row in rows:
            key = _normalise_title(row.get("title_clean") or row.get("title") or "")
            if not key or _is_generic_title(key):
                continue
            buckets.setdefault(key, []).append(row)

        groups_written = 0
        for bucket_rows in buckets.values():
            if groups_written >= limit_groups:
                break
            if len(bucket_rows) < 2:
                continue
            if len({str(r.get("source_portal") or "") for r in bucket_rows}) < 2:
                continue

            used = [False] * len(bucket_rows)
            for i in range(len(bucket_rows)):
                if used[i]:
                    continue
                group = [bucket_rows[i]]
                used[i] = True
                for j in range(i + 1, len(bucket_rows)):
                    if used[j]:
                        continue
                    if _is_same_tender(bucket_rows[i], bucket_rows[j]):
                        group.append(bucket_rows[j])
                        used[j] = True

                if len(group) < 2:
                    continue

                merged = merge_tender_group(group)
                canonical_id = str(merged.get("tender_id") or "")
                if not canonical_id:
                    continue

                merged_description = str(merged.get("description") or "")
                cur.execute(
                    """
                    UPDATE tenders
                    SET
                        description = CASE WHEN LENGTH(COALESCE(description, '')) < LENGTH(%s) THEN %s ELSE description END,
                        organization = CASE WHEN COALESCE(NULLIF(TRIM(organization), ''), '') = '' THEN %s ELSE organization END,
                        deadline = COALESCE(deadline, %s),
                        estimated_budget_usd = COALESCE(estimated_budget_usd, %s),
                        country = CASE WHEN COALESCE(NULLIF(TRIM(country), ''), '') = '' THEN %s ELSE country END
                    WHERE tender_id = %s
                    """,
                    (
                        merged_description,
                        merged_description,
                        merged.get("organization") or "",
                        merged.get("deadline"),
                        merged.get("estimated_budget_usd"),
                        merged.get("country") or "",
                        canonical_id,
                    ),
                )

                for row in group:
                    _insert_cross_source(
                        cur,
                        canonical_id,
                        str(row.get("source_portal") or ""),
                        str(row.get("url") or ""),
                        row,
                    )
                    row_tid = str(row.get("tender_id") or "")
                    if row_tid and row_tid != canonical_id:
                        cur.execute(
                            """
                            UPDATE tenders
                            SET is_duplicate = 1, duplicate_of = %s
                            WHERE tender_id = %s
                            """,
                            (canonical_id, row_tid),
                        )

                groups_written += 1

        conn.commit()
        cur.close()
        conn.close()
        return groups_written
    except Exception as exc:
        logger.error("[fuzzy_dedup] backfill_cross_source_groups error: %s", exc)
        return 0


# =============================================================================
# Cross-sources query helper — used by the detail page API
# =============================================================================

def get_cross_sources(tender_id: str) -> list[dict]:
    """
    Return all portal sources for a canonical tender_id.
    Combines the tender's own source with any cross-sources table entries.
    """
    sources = []
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        # Own source
        cur.execute(
            "SELECT source_portal AS portal, url FROM tenders WHERE tender_id = %s LIMIT 1;",
            (tender_id,)
        )
        own = cur.fetchone()
        if own:
            sources.append(own)

        # Cross-sources
        cur.execute("""
            SELECT
                COALESCE(source_portal, portal) AS portal,
                COALESCE(source_url, url) AS url,
                unique_fields
            FROM tender_cross_sources
            WHERE tender_id = %s
            ORDER BY COALESCE(detected_at, added_at) ASC;
        """, (tender_id,))
        for row in (cur.fetchall() or []):
            if row.get("url") not in {s.get("url") for s in sources}:
                sources.append(row)

        cur.close()
        conn.close()
    except Exception as e:
        logger.debug("[fuzzy_dedup] get_cross_sources error: %s", e)

    return sources
