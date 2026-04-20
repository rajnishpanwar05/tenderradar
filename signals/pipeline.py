from __future__ import annotations

from collections import Counter
import hashlib
from typing import Dict, List

from database.db import get_connection, init_db, init_opportunity_signals_schema
from exporters.opportunity_signals_exporter import export_opportunity_signals_excel
from signals.sources.aiib import fetch_signal_rows as fetch_aiib_signal_rows
from signals.sources.jica_india import fetch_signal_rows as fetch_jica_india_signal_rows
from signals.sources.world_bank import fetch_signal_rows as fetch_world_bank_signal_rows


_SOURCE_LOADERS = {
    "world_bank": fetch_world_bank_signal_rows,
    "aiib": fetch_aiib_signal_rows,
    "jica_india": fetch_jica_india_signal_rows,
}


def _signal_uid(row: Dict) -> str:
    source = str(row.get("source") or "unknown").strip().lower().replace(" ", "_")
    record_id = str(row.get("source_record_id") or "").strip()
    url = str(row.get("url") or "").strip()
    return f"{source}:{record_id or url}"


def _hash_text(value: str) -> str:
    value = str(value or "").strip().lower()
    if not value:
        return ""
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def _prepare_row(row: Dict) -> Dict:
    prepared = dict(row)
    prepared["signal_uid"] = _signal_uid(prepared)
    prepared["url_hash"] = _hash_text(str(prepared.get("url") or ""))
    prepared["title_hash"] = _hash_text(str(prepared.get("title") or ""))
    if not prepared.get("content_hash"):
        prepared["content_hash"] = _hash_text(
            "|".join([
                str(prepared.get("title") or ""),
                str(prepared.get("summary") or ""),
                str(prepared.get("signal_stage") or ""),
                str(prepared.get("confidence_score") or 0),
            ])
        )
    return prepared


def upsert_opportunity_signals(rows: List[Dict]) -> Dict[str, int]:
    if not rows:
        return {"inserted": 0, "updated": 0}

    conn = get_connection()
    cur = conn.cursor()
    inserted = 0
    updated = 0
    for row in rows:
        row = _prepare_row(row)
        signal_uid = row["signal_uid"]
        metadata_json = str(row.get("metadata_json") or "{}")
        cur.execute(
            """
            SELECT id
            FROM opportunity_signals
            WHERE signal_uid = %s
               OR (%s != '' AND url_hash = %s)
               OR (%s != '' AND content_hash = %s AND title_hash = %s)
            LIMIT 1
            """,
            (
                signal_uid,
                row.get("url_hash", ""),
                row.get("url_hash", ""),
                row.get("title_hash", ""),
                row.get("content_hash", ""),
                row.get("title_hash", ""),
            ),
        )
        existing = cur.fetchone()
        if existing is None:
            cur.execute(
                """
                INSERT INTO opportunity_signals (
                    signal_uid, source, source_record_id, title, organization, geography,
                    sector, summary, signal_stage, confidence_score, consulting_signal,
                    consulting_signal_reason, url, published_date, captured_at,
                    recommended_action, raw_stage, procurement_signal, url_hash, title_hash,
                    content_hash, metadata_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    signal_uid,
                    row.get("source"),
                    row.get("source_record_id"),
                    row.get("title"),
                    row.get("organization"),
                    row.get("geography"),
                    row.get("sector"),
                    row.get("summary"),
                    row.get("signal_stage"),
                    int(row.get("confidence_score") or 0),
                    int(row.get("consulting_signal") or 0),
                    row.get("consulting_signal_reason"),
                    row.get("url"),
                    row.get("published_date") or None,
                    row.get("recommended_action"),
                    row.get("raw_stage"),
                    int(row.get("procurement_signal") or 0),
                    row.get("url_hash"),
                    row.get("title_hash"),
                    row.get("content_hash"),
                    metadata_json,
                ),
            )
            inserted += 1
        else:
            cur.execute(
                """
                UPDATE opportunity_signals
                   SET signal_uid = %s,
                       source = %s,
                       source_record_id = %s,
                       title = %s,
                       organization = %s,
                       geography = %s,
                       sector = %s,
                       summary = %s,
                       signal_stage = %s,
                       confidence_score = %s,
                       consulting_signal = %s,
                       consulting_signal_reason = %s,
                       url = %s,
                       published_date = %s,
                       captured_at = NOW(),
                       recommended_action = %s,
                       raw_stage = %s,
                       procurement_signal = %s,
                       url_hash = %s,
                       title_hash = %s,
                       content_hash = %s,
                       metadata_json = %s
                 WHERE id = %s
                """,
                (
                    signal_uid,
                    row.get("source"),
                    row.get("source_record_id"),
                    row.get("title"),
                    row.get("organization"),
                    row.get("geography"),
                    row.get("sector"),
                    row.get("summary"),
                    row.get("signal_stage"),
                    int(row.get("confidence_score") or 0),
                    int(row.get("consulting_signal") or 0),
                    row.get("consulting_signal_reason"),
                    row.get("url"),
                    row.get("published_date") or None,
                    row.get("recommended_action"),
                    row.get("raw_stage"),
                    int(row.get("procurement_signal") or 0),
                    row.get("url_hash"),
                    row.get("title_hash"),
                    row.get("content_hash"),
                    metadata_json,
                    existing[0],
                ),
            )
            updated += 1
    conn.commit()
    cur.close()
    conn.close()
    return {"inserted": inserted, "updated": updated}


def fetch_export_rows(limit: int = 500) -> List[Dict]:
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT source, title, organization, geography, sector, signal_stage,
               consulting_signal, confidence_score, summary, url,
               recommended_action, published_date, captured_at,
               consulting_signal_reason
        FROM opportunity_signals
        ORDER BY confidence_score DESC, published_date DESC, captured_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def run_signal_pipeline(source: str = "world_bank", debug: bool = False) -> Dict:
    init_db()
    init_opportunity_signals_schema()

    source_names = list(_SOURCE_LOADERS) if source == "all" else [source]
    rows: List[Dict] = []
    per_source: Dict[str, int] = {}
    for source_name in source_names:
        loader = _SOURCE_LOADERS[source_name]
        source_rows = loader(debug=debug)
        per_source[source_name] = len(source_rows)
        rows.extend(source_rows)
    db_stats = upsert_opportunity_signals(rows)
    export_rows = fetch_export_rows()
    artifact_path = export_opportunity_signals_excel(export_rows)

    stage_counts = Counter(str(r.get("signal_stage") or "UNKNOWN") for r in rows)
    consulting_counts = Counter("YES" if int(r.get("consulting_signal") or 0) == 1 else "NO" for r in rows)

    return {
        "source": source,
        "captured": len(rows),
        "inserted": db_stats["inserted"],
        "updated": db_stats["updated"],
        "stage_counts": dict(stage_counts),
        "consulting_counts": dict(consulting_counts),
        "artifact_path": artifact_path,
        "sample_rows": rows[:5],
        "per_source_counts": per_source,
    }
