"""
intelligence/ai_quality_loop.py

Failure-driven improvement loop for chat quality:
1) Log every answer event with retrieval metadata.
2) Collect explicit human feedback labels.
3) Produce weekly analytics for tuning retrieval/prompts.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from database.db import get_connection

logger = logging.getLogger("tenderradar.ai_quality")

_FACT_QUERY_MARKERS = (
    "which", "what", "who", "when", "where", "deadline", "budget", "amount",
    "cost", "eligibility", "requirement", "documents", "show", "list",
    "top ", "best ", "give me",
)


def _now_utc() -> datetime:
    return datetime.utcnow()


def ensure_schema() -> None:
    """Create AI quality loop tables if absent (idempotent)."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_answer_events (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                route VARCHAR(64) NOT NULL,
                query_text TEXT NOT NULL,
                query_type VARCHAR(64) DEFAULT '',
                retrieval_count INT NOT NULL DEFAULT 0,
                llm_model VARCHAR(128) DEFAULT '',
                citations_json JSON NULL,
                source_tender_ids_json JSON NULL,
                answer_text MEDIUMTEXT,
                has_abstain BOOLEAN NOT NULL DEFAULT FALSE,
                is_validated BOOLEAN NOT NULL DEFAULT TRUE,
                latency_ms INT NOT NULL DEFAULT 0,
                INDEX idx_created_at (created_at),
                INDEX idx_route (route),
                INDEX idx_query_type (query_type)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_answer_feedback (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                answer_event_id BIGINT NOT NULL,
                rating TINYINT NULL,
                labels_json JSON NULL,
                note TEXT NULL,
                FOREIGN KEY (answer_event_id) REFERENCES llm_answer_events(id)
                  ON DELETE CASCADE,
                INDEX idx_answer_event_id (answer_event_id),
                INDEX idx_created_at (created_at)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def log_answer_event(
    route: str,
    query_text: str,
    query_type: str,
    retrieval_count: int,
    llm_model: str,
    citations: List[int],
    source_tender_ids: List[str],
    answer_text: str,
    has_abstain: bool,
    is_validated: bool,
    latency_ms: int,
) -> Optional[int]:
    """Persist one chat answer event and return event id."""
    try:
        ensure_schema()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO llm_answer_events (
                route, query_text, query_type, retrieval_count, llm_model,
                citations_json, source_tender_ids_json, answer_text,
                has_abstain, is_validated, latency_ms
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(route or "")[:64],
                str(query_text or ""),
                str(query_type or "")[:64],
                int(retrieval_count or 0),
                str(llm_model or "")[:128],
                json.dumps(citations or []),
                json.dumps(source_tender_ids or []),
                str(answer_text or ""),
                bool(has_abstain),
                bool(is_validated),
                int(latency_ms or 0),
            ),
        )
        conn.commit()
        event_id = int(cur.lastrowid or 0)
        cur.close()
        conn.close()
        return event_id if event_id > 0 else None
    except Exception as exc:
        logger.warning("[ai_quality] log_answer_event failed: %s", exc)
        return None


def log_feedback(
    answer_event_id: int,
    rating: Optional[int] = None,
    labels: Optional[List[str]] = None,
    note: Optional[str] = None,
) -> bool:
    """Store one feedback row for a previously logged answer event."""
    try:
        ensure_schema()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO llm_answer_feedback (
                answer_event_id, rating, labels_json, note
            )
            VALUES (%s, %s, %s, %s)
            """,
            (
                int(answer_event_id),
                (int(rating) if rating is not None else None),
                json.dumps(labels or []),
                (str(note or "")[:4000] if note else None),
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as exc:
        logger.warning("[ai_quality] log_feedback failed: %s", exc)
        return False


def weekly_report(days: int = 7) -> Dict[str, Any]:
    """
    Summarize last N days for failure-driven tuning.
    """
    ensure_schema()
    start = _now_utc() - timedelta(days=max(1, int(days)))
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
                COUNT(*) AS answers,
                AVG(latency_ms) AS avg_latency_ms,
                SUM(CASE WHEN has_abstain = 1 THEN 1 ELSE 0 END) AS abstains
            FROM llm_answer_events
            WHERE created_at >= %s
            """,
            (start,),
        )
        answers_row = cur.fetchone() or {}

        cur.execute(
            """
            SELECT
                f.rating,
                COUNT(*) AS n
            FROM llm_answer_feedback f
            JOIN llm_answer_events e ON e.id = f.answer_event_id
            WHERE f.created_at >= %s
            GROUP BY f.rating
            ORDER BY n DESC
            """,
            (start,),
        )
        ratings = cur.fetchall() or []

        cur.execute(
            """
            SELECT
                e.query_type,
                COUNT(*) AS n
            FROM llm_answer_feedback f
            JOIN llm_answer_events e ON e.id = f.answer_event_id
            WHERE f.created_at >= %s
              AND (f.rating IS NULL OR f.rating <= 2)
            GROUP BY e.query_type
            ORDER BY n DESC
            LIMIT 8
            """,
            (start,),
        )
        weak_query_types = cur.fetchall() or []

        cur.execute(
            """
            SELECT
                jt.label,
                COUNT(*) AS n
            FROM llm_answer_feedback f
            JOIN JSON_TABLE(
                COALESCE(f.labels_json, JSON_ARRAY()),
                '$[*]' COLUMNS(label VARCHAR(128) PATH '$')
            ) AS jt
            WHERE f.created_at >= %s
            GROUP BY jt.label
            ORDER BY n DESC
            LIMIT 12
            """,
            (start,),
        )
        label_counts = cur.fetchall() or []

        top_labels = [str(r.get("label") or "").strip() for r in label_counts if str(r.get("label") or "").strip()]
        actions: List[str] = []
        if any("wrong_retrieval" == x for x in top_labels):
            actions.append("Tune retrieval filters + reranker; inspect top-k misses for wrong_retrieval.")
        if any("hallucination" == x for x in top_labels):
            actions.append("Increase abstain strictness and enforce citation-only claim policy.")
        if any("missing_context" == x for x in top_labels):
            actions.append("Expand evidence pack size for synthesis queries and include deep document chunks.")
        if any("bad_ranking" == x for x in top_labels):
            actions.append("Retune composite score weights and apply portal/sector priors.")

        return {
            "window_days": int(days),
            "since_utc": start.isoformat() + "Z",
            "answers": int(answers_row.get("answers") or 0),
            "avg_latency_ms": round(float(answers_row.get("avg_latency_ms") or 0.0), 1),
            "abstains": int(answers_row.get("abstains") or 0),
            "ratings": ratings,
            "failure_labels": label_counts,
            "weak_query_types": weak_query_types,
            "recommended_actions": actions,
        }
    finally:
        cur.close()
        conn.close()


def _parse_json_list(raw: Any) -> List[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            v = json.loads(s)
            return v if isinstance(v, list) else []
        except Exception:
            return []
    return []


def _is_fact_query(text: str) -> bool:
    q = (text or "").lower()
    return any(m in q for m in _FACT_QUERY_MARKERS)


def _answer_has_numeric_claim(answer: str) -> bool:
    """
    Heuristic: answer contains percentages/currency/count claims.
    Useful for flagging uncited factual assertions.
    """
    a = answer or ""
    patterns = [
        r"\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b",   # 12,000 or 1,200,000.5
        r"\b\d+(?:\.\d+)?\s*%\b",              # 45% or 12.5%
        r"\b(?:usd|inr|eur|rs\.?)\s*\d",       # USD 1000
        r"\b\d+\s+(?:days|weeks|months|years)\b",
    ]
    return any(re.search(p, a, flags=re.IGNORECASE) for p in patterns)


def chat_grounding_report(days: int = 7, max_rows: int = 5000) -> Dict[str, Any]:
    """
    Grounding-focused metrics over recent chat events:
      - citation validity
      - fact-query grounding compliance (citation or abstain)
      - abstain quality on zero-retrieval cases
      - risky uncited numeric claims
    """
    ensure_schema()
    start = _now_utc() - timedelta(days=max(1, int(days)))
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
                id,
                created_at,
                query_text,
                query_type,
                retrieval_count,
                citations_json,
                answer_text,
                has_abstain,
                latency_ms
            FROM llm_answer_events
            WHERE created_at >= %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (start, int(max_rows)),
        )
        rows = cur.fetchall() or []

        total = len(rows)
        fact_queries = 0
        with_citations = 0
        valid_citations = 0
        grounding_compliant = 0
        zero_retrieval = 0
        zero_retrieval_abstain = 0
        uncited_numeric_claims = 0
        latency_sum = 0
        risky_examples: List[Dict[str, Any]] = []

        for r in rows:
            query = str(r.get("query_text") or "")
            answer = str(r.get("answer_text") or "")
            retrieval_count = int(r.get("retrieval_count") or 0)
            has_abstain = bool(r.get("has_abstain"))
            latency_sum += int(r.get("latency_ms") or 0)

            cites_raw = _parse_json_list(r.get("citations_json"))
            citations: List[int] = []
            for c in cites_raw:
                try:
                    i = int(c)
                except Exception:
                    continue
                if i > 0 and i not in citations:
                    citations.append(i)

            if citations:
                with_citations += 1

            if citations and all(i <= max(1, retrieval_count) for i in citations):
                valid_citations += 1
            elif not citations:
                # no citations is "valid" structurally only if abstained
                if has_abstain:
                    valid_citations += 1

            is_fact = _is_fact_query(query)
            if is_fact:
                fact_queries += 1
                if citations or has_abstain:
                    grounding_compliant += 1
                if (not citations) and (not has_abstain) and _answer_has_numeric_claim(answer):
                    uncited_numeric_claims += 1
                    if len(risky_examples) < 10:
                        risky_examples.append(
                            {
                                "event_id": int(r.get("id") or 0),
                                "query": query[:220],
                                "retrieval_count": retrieval_count,
                                "answer_preview": answer[:300],
                            }
                        )

            if retrieval_count == 0:
                zero_retrieval += 1
                if has_abstain:
                    zero_retrieval_abstain += 1

        def pct(n: int, d: int) -> float:
            return round((100.0 * n / d), 2) if d > 0 else 0.0

        avg_latency = round(latency_sum / total, 1) if total else 0.0
        return {
            "window_days": int(days),
            "since_utc": start.isoformat() + "Z",
            "events": total,
            "avg_latency_ms": avg_latency,
            "fact_queries": fact_queries,
            "with_citations": with_citations,
            "citation_presence_pct": pct(with_citations, total),
            "citation_validity_pct": pct(valid_citations, total),
            "fact_query_grounding_compliance_pct": pct(grounding_compliant, fact_queries),
            "zero_retrieval_cases": zero_retrieval,
            "zero_retrieval_abstain_pct": pct(zero_retrieval_abstain, zero_retrieval),
            "uncited_numeric_claims": uncited_numeric_claims,
            "uncited_numeric_claim_rate_pct": pct(uncited_numeric_claims, fact_queries),
            "risky_examples": risky_examples,
            "recommended_actions": [
                "Tighten abstain policy if fact_query_grounding_compliance_pct < 95.",
                "Inspect risky_examples and add citation-required guardrails for numeric claims.",
                "Increase retrieval evidence depth for query types with low citation presence.",
            ],
        }
    finally:
        cur.close()
        conn.close()


def weekly_combined_report(days: int = 7) -> Dict[str, Any]:
    """
    Unified AI quality report:
      - user feedback trends
      - grounding correctness metrics
    """
    return {
        "feedback_quality": weekly_report(days=days),
        "grounding_quality": chat_grounding_report(days=days),
    }
