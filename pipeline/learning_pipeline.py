from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

logger = logging.getLogger("tenderradar.learning_pipeline")

_BASE = os.path.expanduser("~/tender_system")
_ARTIFACT_DIR = os.path.join(_BASE, "artifacts")
_MODEL_PATH = os.path.join(_ARTIFACT_DIR, "feedback_model.json")
_METRICS_PATH = os.path.join(_ARTIFACT_DIR, "feedback_eval_metrics.json")
_STATE_PATH = os.path.join(_ARTIFACT_DIR, "learning_state.json")


def _ensure_artifact_dir() -> None:
    os.makedirs(_ARTIFACT_DIR, exist_ok=True)


def _fetch_feedback_rows(limit: int = 50000) -> List[Dict[str, Any]]:
    """Load supervised rows from bid_pipeline + structured intel."""
    try:
        from database.db import get_connection
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
                bp.tender_id,
                bp.model_decision_tag,
                bp.bid_decision,
                bp.outcome,
                bp.evaluated_at,
                si.relevance_score,
                si.priority_score,
                si.consulting_type,
                si.sector,
                si.region,
                si.organization,
                si.deadline_category,
                si.competition_level,
                si.opportunity_size,
                si.complexity_score
            FROM bid_pipeline bp
            LEFT JOIN tender_structured_intel si USING (tender_id)
            WHERE bp.bid_decision IS NOT NULL
              AND bp.bid_decision IN ('bid', 'no_bid', 'review_later')
            ORDER BY COALESCE(bp.evaluated_at, bp.updated_at, bp.created_at) DESC
            LIMIT %s
            """,
            (int(limit),),
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        return rows
    except Exception as exc:
        logger.warning("[learning_pipeline] Failed to fetch feedback rows: %s", exc)
        return []


def _label_bid(row: Dict[str, Any]) -> int:
    return 1 if str(row.get("bid_decision") or "").lower() == "bid" else 0


def _label_high_relevance(row: Dict[str, Any]) -> int:
    return 1 if int(row.get("relevance_score") or 0) >= 75 else 0


def _featurize(rows: List[Dict[str, Any]]) -> Tuple[List[List[float]], List[int], List[int]]:
    """Deterministic numeric features with lightweight categorical hashing."""
    def cat_hash(v: str, mod: int = 1000) -> float:
        return float(abs(hash((v or "").strip().lower())) % mod) / float(mod)

    X: List[List[float]] = []
    y_bid: List[int] = []
    y_rel: List[int] = []

    for r in rows:
        rel = float(r.get("relevance_score") or 0)
        pr = float(r.get("priority_score") or 0)
        cx = float(r.get("complexity_score") or 0)
        dc = str(r.get("deadline_category") or "unknown").lower()
        comp = str(r.get("competition_level") or "medium").lower()
        size = str(r.get("opportunity_size") or "medium").lower()
        vec = [
            rel / 100.0,
            pr / 100.0,
            cx / 100.0,
            1.0 if dc == "urgent" else 0.0,
            1.0 if dc == "soon" else 0.0,
            1.0 if comp == "high" else 0.0,
            1.0 if comp == "low" else 0.0,
            1.0 if size == "large" else 0.0,
            1.0 if size == "small" else 0.0,
            cat_hash(str(r.get("consulting_type") or "")),
            cat_hash(str(r.get("sector") or "")),
            cat_hash(str(r.get("region") or "")),
            cat_hash(str(r.get("organization") or "")),
            1.0 if str(r.get("model_decision_tag") or "") == "BID_NOW" else 0.0,
        ]
        X.append(vec)
        y_bid.append(_label_bid(r))
        y_rel.append(_label_high_relevance(r))
    return X, y_bid, y_rel


def _fit_logreg(X: List[List[float]], y: List[int]) -> Dict[str, Any]:
    """
    Try sklearn LogisticRegression; fallback to deterministic feature-weight baseline.
    """
    if len(X) < 25:
        return {"type": "insufficient_data", "weights": [], "intercept": 0.0}
    try:
        from sklearn.linear_model import LogisticRegression
        model = LogisticRegression(max_iter=500, random_state=42, class_weight="balanced")
        model.fit(X, y)
        return {
            "type": "logreg",
            "weights": model.coef_[0].tolist(),
            "intercept": float(model.intercept_[0]),
        }
    except Exception:
        # Deterministic fallback: handcrafted compact baseline
        fallback_weights = [0.8, 1.0, 0.3, 0.15, 0.1, -0.15, 0.1, 0.1, -0.05, 0.05, 0.05, 0.05, 0.05, 0.2]
        return {"type": "fallback_linear", "weights": fallback_weights, "intercept": -0.6}


def _predict_prob(model: Dict[str, Any], x: List[float]) -> float:
    import math
    w = model.get("weights") or []
    b = float(model.get("intercept") or 0.0)
    if not w:
        return 0.5
    z = b + sum((w[i] if i < len(w) else 0.0) * x[i] for i in range(len(x)))
    return 1.0 / (1.0 + math.exp(-max(-20.0, min(20.0, z))))


def _safe_div(a: float, b: float) -> float:
    return float(a) / float(b) if b else 0.0


def _dcg(rels: List[int], k: int) -> float:
    import math
    s = 0.0
    for i, rel in enumerate(rels[:k], start=1):
        s += (2 ** int(rel) - 1) / math.log2(i + 1)
    return s


def evaluate_ranking(rows: List[Dict[str, Any]], scores: List[float], k: int = 20) -> Dict[str, float]:
    pairs = list(zip(rows, scores))
    pairs.sort(key=lambda x: x[1], reverse=True)
    top = pairs[:k]
    rels = [1 if str(r.get("outcome") or "") == "won" else 0 for r, _ in top]
    all_wins = sum(1 for r in rows if str(r.get("outcome") or "") == "won")

    precision_k = _safe_div(sum(rels), k)
    recall_k = _safe_div(sum(rels), all_wins)
    ideal_rels = sorted([1 if str(r.get("outcome") or "") == "won" else 0 for r in rows], reverse=True)
    ndcg_k = _safe_div(_dcg(rels, k), _dcg(ideal_rels, k))
    return {
        "precision_at_k": round(precision_k, 4),
        "recall_at_k": round(recall_k, 4),
        "ndcg_at_k": round(ndcg_k, 4),
    }


def train_and_evaluate(limit: int = 50000, k: int = 20) -> Dict[str, Any]:
    rows = _fetch_feedback_rows(limit=limit)
    if not rows:
        return {"ok": False, "note": "No feedback rows available for training."}

    X, y_bid, y_rel = _featurize(rows)
    model_bid = _fit_logreg(X, y_bid)
    model_rel = _fit_logreg(X, y_rel)

    scores_bid = [_predict_prob(model_bid, x) for x in X]
    pred_bid = [1 if s >= 0.5 else 0 for s in scores_bid]
    decision_acc = _safe_div(sum(1 for a, b in zip(pred_bid, y_bid) if a == b), len(y_bid))

    ranking_metrics = evaluate_ranking(rows, scores_bid, k=k)
    result = {
        "ok": True,
        "generated_at": datetime.utcnow().isoformat(),
        "rows_used": len(rows),
        "decision_accuracy": round(decision_acc, 4),
        **ranking_metrics,
        "models": {
            "bid_model": model_bid,
            "relevance_model": model_rel,
        },
    }

    _ensure_artifact_dir()
    with open(_MODEL_PATH, "w", encoding="utf-8") as fh:
        json.dump(result["models"], fh, ensure_ascii=True, indent=2)
    with open(_METRICS_PATH, "w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=True, indent=2)
    return result


def maybe_run_weekly_learning(force: bool = False) -> Dict[str, Any]:
    """
    Run training + evaluation at most once every 7 days unless force=True.
    """
    _ensure_artifact_dir()
    now = datetime.utcnow()
    if not force and os.path.exists(_STATE_PATH):
        try:
            with open(_STATE_PATH, "r", encoding="utf-8") as fh:
                state = json.load(fh)
            last = datetime.fromisoformat(str(state.get("last_run")))
            if now - last < timedelta(days=7):
                return {"ok": True, "skipped": True, "note": "Weekly learning not due yet."}
        except Exception:
            pass

    result = train_and_evaluate()
    with open(_STATE_PATH, "w", encoding="utf-8") as fh:
        json.dump({"last_run": now.isoformat()}, fh, ensure_ascii=True, indent=2)
    return result


if __name__ == "__main__":
    out = maybe_run_weekly_learning(force=True)
    print(json.dumps(out, indent=2))
