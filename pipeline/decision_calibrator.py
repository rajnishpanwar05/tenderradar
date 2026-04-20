# =============================================================================
# pipeline/decision_calibrator.py — Decision Feedback & Learning Engine
#
# Closes the loop between TenderRadar's recommendations and real-world results.
# As the firm records outcomes (won / lost / no_bid), the calibrator measures
# how accurate the model's tier predictions were and suggests improvements.
#
# Architecture
# ─────────────────────────────────────────────────────────────────────────────
#   bid_pipeline.model_decision_tag  ← stored at discovery (what system said)
#   bid_pipeline.bid_decision        ← what firm actually decided
#   bid_pipeline.outcome             ← final result (won / lost / no_bid)
#   bid_pipeline.evaluated_at        ← when outcome was recorded
#
#   The calibrator reads these columns and computes:
#     • Per-tier metrics   (bid rate, win rate, false positive rate per tier)
#     • Overall metrics    (portfolio-level bid conversion, win rate)
#     • Signal correlation (which consulting_type / sector / org → wins)
#     • Threshold suggestions (rule-based + data-driven)
#
# Adaptive threshold tuning writes to calibration_config.json.
# quality_engine.py reads this file at import time and overrides constants.
#
# Public API:
#   compute_decision_accuracy(min_samples)  → dict   (Task 3)
#   suggest_threshold_adjustment(metrics)   → dict   (Task 4)
#   apply_threshold_adjustment(suggestion)  → None   (Task 4 — write config)
#   compute_winning_signals()               → dict   (Task 6)
#   get_performance_summary()              → dict   (for /api/v1/performance)
#   print_accuracy_report()                → None   (CLI --accuracy)
#
# Minimum data for reliable calibration:
#   MIN_SAMPLES = 20  (function returns low-confidence flag below this)
# =============================================================================

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("tenderradar.calibrator")

_TABLE       = "bid_pipeline"
_INTEL_TABLE = "tender_structured_intel"
_SEEN_TABLE  = "seen_tenders"

# Path to the adaptive calibration config file
_CALIBRATION_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "calibration_config.json"
)
_CALIBRATION_CONFIG_PATH = os.path.normpath(_CALIBRATION_CONFIG_PATH)

# Minimum evaluated rows for reliable calibration
MIN_SAMPLES = 20

# Tier ordering for display
_ALL_TIERS = ("BID_NOW", "STRONG_CONSIDER", "WEAK_CONSIDER", "IGNORE")

# False positive rate threshold: if BID_NOW bid_rate < this → raise threshold
_FP_RATE_THRESHOLD = 0.50   # 50 %: at least half of BID_NOW should be acted on
# Scarcity threshold: if BID_NOW count < this fraction of pipeline → lower threshold
_SCARCITY_FRACTION = 0.05   # 5 % of all discovered tenders should be BID_NOW


# =============================================================================
# SECTION 1 — Data retrieval
# =============================================================================

def _query_calibration_data() -> List[Dict]:
    """
    Fetch all bid_pipeline rows that have outcome recorded (evaluated_at IS NOT NULL).
    Joins with tender_structured_intel for signal data (sector, consulting_type, org).

    Returns list of dicts with keys:
        tender_id, model_decision_tag, bid_decision, outcome,
        evaluated_at, sector, consulting_type, organization, priority_score
    """
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT
                bp.tender_id,
                bp.model_decision_tag,
                bp.bid_decision,
                bp.outcome,
                bp.evaluated_at,
                COALESCE(tsi.sector,           'unknown')        AS sector,
                COALESCE(tsi.consulting_type,  'unknown')        AS consulting_type,
                COALESCE(tsi.organization,     'unknown')        AS organization,
                COALESCE(tsi.priority_score,    0)               AS priority_score
            FROM `{_TABLE}` bp
            LEFT JOIN `{_INTEL_TABLE}` tsi ON bp.tender_id = tsi.tender_id
            WHERE bp.evaluated_at IS NOT NULL
              AND bp.outcome      IS NOT NULL
            ORDER BY bp.evaluated_at DESC
        """)
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        return rows
    except Exception as exc:
        logger.warning("[calibrator] _query_calibration_data failed: %s", exc)
        return []


def _query_pipeline_counts() -> Dict[str, int]:
    """
    Return total count per model_decision_tag across ALL pipeline entries
    (not just evaluated ones) — used for scarcity detection.
    """
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT
                COALESCE(model_decision_tag, 'UNKNOWN') AS tag,
                COUNT(*) AS cnt
            FROM `{_TABLE}`
            GROUP BY model_decision_tag
        """)
        rows  = cur.fetchall() or []
        cur.close()
        conn.close()
        return {r["tag"]: int(r["cnt"]) for r in rows}
    except Exception as exc:
        logger.warning("[calibrator] _query_pipeline_counts failed: %s", exc)
        return {}


# =============================================================================
# SECTION 2 — Per-tier metrics
# =============================================================================

def _compute_tier_stats(rows: List[Dict], tier: str) -> Dict[str, Any]:
    """
    Compute accuracy metrics for a single decision tier.

    Metrics
    ───────
    total           : rows with this model_decision_tag
    bid_count       : how many the firm actually bid on
    no_bid_count    : how many were skipped (bid_decision = no_bid)
    win_count       : how many were won
    loss_count      : how many were lost
    no_bid_outcome  : no_bid outcome (firm never bid)
    bid_rate        : bid_count / total  (how often we acted on recommendation)
    win_rate        : win_count / total  (overall win rate incl. no-bids)
    win_from_bid    : win_count / bid_count  (win rate among bids placed)
    false_positive  : no_bid_count / total  (BID_NOW but chose not to bid)
    """
    tier_rows = [r for r in rows if (r.get("model_decision_tag") or "") == tier]
    total     = len(tier_rows)

    if total == 0:
        return {
            "total": 0, "bid_count": 0, "no_bid_count": 0,
            "win_count": 0, "loss_count": 0, "no_bid_outcome": 0,
            "bid_rate": None, "win_rate": None,
            "win_from_bid_rate": None, "false_positive_rate": None,
        }

    bid_count      = sum(1 for r in tier_rows if r.get("bid_decision") == "bid")
    no_bid_count   = sum(
        1 for r in tier_rows if r.get("bid_decision") in ("no_bid", "review_later")
    )
    win_count      = sum(1 for r in tier_rows if r.get("outcome") == "won")
    loss_count     = sum(1 for r in tier_rows if r.get("outcome") == "lost")
    no_bid_outcome = sum(
        1 for r in tier_rows if r.get("outcome") in ("no_submission", "pending")
    )

    def _pct(num: int, denom: int) -> Optional[float]:
        return round(num / denom, 3) if denom > 0 else None

    return {
        "total":              total,
        "bid_count":          bid_count,
        "no_bid_count":       no_bid_count,
        "win_count":          win_count,
        "loss_count":         loss_count,
        "no_bid_outcome":     no_bid_outcome,
        "bid_rate":           _pct(bid_count, total),
        "win_rate":           _pct(win_count, total),
        "win_from_bid_rate":  _pct(win_count, bid_count),
        "false_positive_rate": _pct(no_bid_count, total),
    }


# =============================================================================
# SECTION 3 — Main accuracy computation  (Task 3)
# =============================================================================

def compute_decision_accuracy(min_samples: int = MIN_SAMPLES) -> Dict[str, Any]:
    """
    Compute full calibration metrics from recorded outcomes.

    Returns a dict with:
      data_coverage      — how many rows have outcomes vs total pipeline
      by_model_tier      — per-tier stats dict (BID_NOW / STRONG / WEAK / IGNORE)
      overall            — portfolio-level bid conversion + win rate
      model_accuracy     — precision (BID_NOW bid rate) + recall (BID_NOW of all wins)
      threshold_suggestion — suggested adjustment (from suggest_threshold_adjustment)
      winning_signals    — top consulting_type / sector / org by win rate
      generated_at       — ISO timestamp

    If fewer than min_samples rows are evaluated, all metrics are still computed
    but the 'low_confidence' flag is set True and a warning is included.
    """
    rows          = _query_calibration_data()
    pipeline_cnts = _query_pipeline_counts()
    total_pipeline = sum(pipeline_cnts.values())
    evaluated      = len(rows)

    low_confidence = evaluated < min_samples
    confidence_note = (
        f"Only {evaluated} evaluated outcomes — need {min_samples} for reliable "
        "calibration. Record more outcomes with `--outcome` to improve accuracy."
        if low_confidence else
        f"{evaluated} evaluated outcomes — calibration confidence OK."
    )

    # ── Per-tier metrics ───────────────────────────────────────────────────────
    by_tier: Dict[str, Dict] = {
        tier: _compute_tier_stats(rows, tier) for tier in _ALL_TIERS
    }

    # ── Overall metrics ────────────────────────────────────────────────────────
    total_bids = sum(1 for r in rows if r.get("bid_decision") == "bid")
    total_wins = sum(1 for r in rows if r.get("outcome") == "won")

    def _pct(n: int, d: int) -> Optional[float]:
        return round(n / d, 3) if d > 0 else None

    overall = {
        "total_evaluated":      evaluated,
        "total_bids_placed":    total_bids,
        "total_wins":           total_wins,
        "bid_conversion_rate":  _pct(total_bids, evaluated),
        "win_rate":             _pct(total_wins, evaluated),
        "win_from_bid_rate":    _pct(total_wins, total_bids),
    }

    # ── Model accuracy ─────────────────────────────────────────────────────────
    bid_now_stats   = by_tier["BID_NOW"]
    bid_now_bids    = bid_now_stats["bid_count"]
    bid_now_wins    = bid_now_stats["win_count"]
    precision_bid_now = bid_now_stats["bid_rate"]    # % of BID_NOW we bid on
    recall_bid_now    = (                            # % of all wins that were BID_NOW
        round(bid_now_wins / total_wins, 3) if total_wins > 0 else None
    )
    model_accuracy = {
        "precision_bid_now": precision_bid_now,  # ideal: > 0.70
        "recall_bid_now":    recall_bid_now,      # ideal: > 0.50
        "note": (
            "precision = fraction of BID_NOW tenders we actually bid on. "
            "recall = fraction of all wins that were classified BID_NOW."
        ),
    }

    # ── Signal correlation (Task 6 preview in accuracy output) ─────────────────
    signals = compute_winning_signals(rows)

    # ── Threshold suggestion ───────────────────────────────────────────────────
    suggestion = suggest_threshold_adjustment(
        {"by_model_tier": by_tier, "overall": overall},
        pipeline_counts=pipeline_cnts,
    )

    return {
        "data_coverage": {
            "total_in_pipeline": total_pipeline,
            "evaluated":         evaluated,
            "coverage_pct":      round(evaluated / total_pipeline * 100, 1) if total_pipeline else 0.0,
            "low_confidence":    low_confidence,
            "note":              confidence_note,
        },
        "by_model_tier":      by_tier,
        "overall":            overall,
        "model_accuracy":     model_accuracy,
        "winning_signals":    signals,
        "threshold_suggestion": suggestion,
        "generated_at":       datetime.now().isoformat(timespec="seconds"),
    }


# =============================================================================
# SECTION 4 — Adaptive threshold tuning  (Task 4)
# =============================================================================

def suggest_threshold_adjustment(
    metrics:         Optional[Dict] = None,
    pipeline_counts: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Examine calibration metrics and suggest BID_NOW threshold adjustments.

    Rules (applied in priority order):
      1. BID_NOW bid_rate < 50 %   → raise score threshold by 5
         (model recommends BID_NOW but firm ignores > half — too permissive)
      2. BID_NOW win_from_bid < 20 % and ≥ 5 bids placed
         → raise confidence threshold by 0.05
         (even when bidding, win rate is poor — calibrate confidence gate)
      3. BID_NOW < 5 % of total pipeline
         → lower score threshold by 3
         (BID_NOW is too rare — likely threshold is too strict)
      4. No issues detected → no_change = True

    Returns dict:
      no_change             : bool
      reason                : str   (human-readable explanation)
      suggested_thresholds  : dict  (field → new value, empty if no_change)
      current_thresholds    : dict
      confidence            : 'low' | 'medium' | 'high'
    """
    # Always import current thresholds
    from core.quality_engine import (
        TIER_BID_NOW_SCORE, TIER_BID_NOW_CONFIDENCE,
        TIER_STRONG_MIN, TIER_WEAK_MIN,
    )
    # Also try to read any previously applied overrides
    current = _load_calibration_config().get("thresholds", {})
    bid_now_score = int(current.get("TIER_BID_NOW_SCORE", TIER_BID_NOW_SCORE))
    bid_now_conf  = float(current.get("TIER_BID_NOW_CONFIDENCE", TIER_BID_NOW_CONFIDENCE))
    strong_min    = int(current.get("TIER_STRONG_MIN", TIER_STRONG_MIN))
    weak_min      = int(current.get("TIER_WEAK_MIN", TIER_WEAK_MIN))

    current_thresholds = {
        "TIER_BID_NOW_SCORE":      bid_now_score,
        "TIER_BID_NOW_CONFIDENCE": bid_now_conf,
        "TIER_STRONG_MIN":         strong_min,
        "TIER_WEAK_MIN":           weak_min,
    }

    # Fetch metrics if not provided
    if metrics is None:
        metrics = compute_decision_accuracy()

    by_tier   = metrics.get("by_model_tier", {})
    overall   = metrics.get("overall", {})
    bn_stats  = by_tier.get("BID_NOW", {})
    evaluated = overall.get("total_evaluated", 0)

    confidence = "high" if evaluated >= MIN_SAMPLES else (
        "medium" if evaluated >= 10 else "low"
    )

    # ── Rule 1: bid_rate too low → raise score threshold ──────────────────────
    bid_rate = bn_stats.get("bid_rate")
    if bid_rate is not None and bid_rate < _FP_RATE_THRESHOLD and bn_stats.get("total", 0) >= 5:
        delta  = 5
        reason = (
            f"BID_NOW bid_rate={bid_rate:.0%} < {_FP_RATE_THRESHOLD:.0%} target. "
            f"System over-promotes tenders to BID_NOW — raising score threshold "
            f"{bid_now_score} → {bid_now_score + delta}."
        )
        return {
            "no_change":           False,
            "rule_triggered":      "low_bid_rate",
            "reason":              reason,
            "confidence":          confidence,
            "current_thresholds":  current_thresholds,
            "suggested_thresholds": {
                **current_thresholds,
                "TIER_BID_NOW_SCORE": bid_now_score + delta,
            },
        }

    # ── Rule 2: low win rate despite bidding → tighten confidence gate ────────
    wfb = bn_stats.get("win_from_bid_rate")
    if (wfb is not None and wfb < 0.20 and bn_stats.get("bid_count", 0) >= 5):
        delta  = 0.05
        new_c  = round(min(0.95, bid_now_conf + delta), 2)
        reason = (
            f"BID_NOW win_from_bid_rate={wfb:.0%} < 20% target on "
            f"{bn_stats['bid_count']} bids placed. "
            f"Raising confidence gate {bid_now_conf:.2f} → {new_c:.2f}."
        )
        return {
            "no_change":           False,
            "rule_triggered":      "low_win_rate",
            "reason":              reason,
            "confidence":          confidence,
            "current_thresholds":  current_thresholds,
            "suggested_thresholds": {
                **current_thresholds,
                "TIER_BID_NOW_CONFIDENCE": new_c,
            },
        }

    # ── Rule 3: BID_NOW too rare → lower threshold ────────────────────────────
    if pipeline_counts is None:
        pipeline_counts = _query_pipeline_counts()
    total_pipeline = sum(pipeline_counts.values()) or 1
    bid_now_total  = pipeline_counts.get("BID_NOW", 0)
    bid_now_pct    = bid_now_total / total_pipeline

    if bid_now_pct < _SCARCITY_FRACTION and total_pipeline >= 50:
        delta  = 3
        reason = (
            f"BID_NOW = {bid_now_total} ({bid_now_pct:.1%}) of {total_pipeline} "
            f"pipeline entries — below {_SCARCITY_FRACTION:.0%} target. "
            f"Threshold may be too strict. Lowering score threshold "
            f"{bid_now_score} → {bid_now_score - delta}."
        )
        return {
            "no_change":           False,
            "rule_triggered":      "bid_now_scarcity",
            "reason":              reason,
            "confidence":          confidence,
            "current_thresholds":  current_thresholds,
            "suggested_thresholds": {
                **current_thresholds,
                "TIER_BID_NOW_SCORE": bid_now_score - delta,
            },
        }

    # ── No adjustment needed ──────────────────────────────────────────────────
    return {
        "no_change":           True,
        "rule_triggered":      None,
        "reason":              (
            f"All metrics within target ranges. "
            f"BID_NOW bid_rate={bid_rate:.0%} ≥ {_FP_RATE_THRESHOLD:.0%}, "
            f"pipeline coverage={bid_now_pct:.1%} ≥ {_SCARCITY_FRACTION:.0%}."
        ) if bid_rate is not None else "Insufficient data to trigger adjustment.",
        "confidence":          confidence,
        "current_thresholds":  current_thresholds,
        "suggested_thresholds": {},
    }


def apply_threshold_adjustment(suggestion: Dict) -> None:
    """
    Write suggested thresholds to calibration_config.json.

    quality_engine.py reads this file at import and overrides its constants,
    making the system genuinely adaptive without code changes.

    Parameters
    ----------
    suggestion : return value of suggest_threshold_adjustment()
        Must have 'suggested_thresholds' key with at least one entry.

    Raises
    ------
    ValueError if suggestion has no_change=True or empty suggested_thresholds.
    """
    if suggestion.get("no_change"):
        raise ValueError("No adjustment to apply — suggestion has no_change=True.")

    new_thresholds = suggestion.get("suggested_thresholds", {})
    if not new_thresholds:
        raise ValueError("suggested_thresholds is empty — nothing to apply.")

    existing = _load_calibration_config()
    existing["thresholds"]  = new_thresholds
    existing["applied_at"]  = datetime.now().isoformat(timespec="seconds")
    existing["applied_rule"] = suggestion.get("rule_triggered", "unknown")
    existing["reason"]       = suggestion.get("reason", "")
    existing["version"]      = existing.get("version", 0) + 1

    with open(_CALIBRATION_CONFIG_PATH, "w") as fh:
        json.dump(existing, fh, indent=2)

    logger.info(
        "[calibrator] Thresholds written to %s: %s",
        _CALIBRATION_CONFIG_PATH, new_thresholds,
    )


def _load_calibration_config() -> Dict:
    """Load existing calibration config (returns empty dict if absent)."""
    if not os.path.exists(_CALIBRATION_CONFIG_PATH):
        return {}
    try:
        with open(_CALIBRATION_CONFIG_PATH) as fh:
            return json.load(fh)
    except Exception:
        return {}


# =============================================================================
# SECTION 5 — Signal correlation analysis  (Task 6)
# =============================================================================

def compute_winning_signals(
    rows: Optional[List[Dict]] = None,
    top_n: int = 5,
) -> Dict[str, Any]:
    """
    Identify which signal combinations correlate with wins vs losses.

    Signals analysed:
      consulting_type  (Evaluation, Technical Assistance, etc.)
      sector           (Health, Education, etc.)
      organization     (UNDP, World Bank, etc.)

    For each signal value, computes:
      win_rate   = won / (won + lost)   [excludes no_bid outcomes]
      sample     = total evaluated rows with this signal
      win_count  = number won
      loss_count = number lost

    Only signals with ≥ 3 evaluated bids are included (too few = noise).

    Returns dict:
      consulting_type : list[{value, win_rate, win_count, loss_count, sample}]
      sector          : same structure
      organization    : same structure
      note            : data-quality note
    """
    if rows is None:
        rows = _query_calibration_data()

    # Filter to rows where a bid was actually placed
    bid_rows = [r for r in rows if r.get("bid_decision") == "bid"]

    def _signal_stats(
        field: str,
        min_count: int = 3,
    ) -> List[Dict]:
        counts: Dict[str, Dict[str, int]] = {}
        for r in bid_rows:
            val = (r.get(field) or "unknown").strip()
            if val not in counts:
                counts[val] = {"won": 0, "lost": 0}
            if r.get("outcome") == "won":
                counts[val]["won"] += 1
            elif r.get("outcome") == "lost":
                counts[val]["lost"] += 1

        result = []
        for val, c in counts.items():
            total_bids = c["won"] + c["lost"]
            if total_bids < min_count:
                continue
            result.append({
                "value":      val,
                "win_rate":   round(c["won"] / total_bids, 3) if total_bids else None,
                "win_count":  c["won"],
                "loss_count": c["lost"],
                "sample":     total_bids,
            })

        return sorted(result, key=lambda x: -(x["win_rate"] or 0))

    top_by_type = _signal_stats("consulting_type")
    top_by_sec  = _signal_stats("sector")
    top_by_org  = _signal_stats("organization")

    note = (
        f"{len(bid_rows)} bids placed with outcomes recorded. "
        f"Signals with < 3 bids excluded (noise floor)."
        if bid_rows else
        "No bids placed with outcomes recorded yet — no signal data available."
    )

    return {
        "consulting_type": top_by_type[:top_n],
        "sector":          top_by_sec[:top_n],
        "organization":    top_by_org[:top_n],
        "note":            note,
    }


# =============================================================================
# SECTION 6 — Public summary (for API endpoint)
# =============================================================================

def get_performance_summary() -> Dict[str, Any]:
    """
    Public entry point for GET /api/v1/performance.

    Returns a fully-structured performance dict ready for JSON serialisation.
    Always succeeds — returns empty structure with error flag on DB failure.
    """
    try:
        metrics = compute_decision_accuracy()
        return {
            "ok":                True,
            "data_coverage":     metrics["data_coverage"],
            "overall":           metrics["overall"],
            "by_tier":           metrics["by_model_tier"],
            "model_accuracy":    metrics["model_accuracy"],
            "winning_signals":   metrics["winning_signals"],
            "threshold_suggestion": metrics["threshold_suggestion"],
            "generated_at":      metrics["generated_at"],
        }
    except Exception as exc:
        logger.warning("[calibrator] get_performance_summary failed: %s", exc)
        return {
            "ok":    False,
            "error": str(exc),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }


# =============================================================================
# SECTION 7 — CLI print report
# =============================================================================

_TIER_EMOJI = {
    "BID_NOW":         "🔥",
    "STRONG_CONSIDER": "⭐",
    "WEAK_CONSIDER":   "📌",
    "IGNORE":          "🔇",
}


def print_accuracy_report() -> None:
    """
    Human-readable accuracy report for CLI --accuracy flag.
    """
    metrics = compute_decision_accuracy()
    cov     = metrics["data_coverage"]
    overall = metrics["overall"]
    by_tier = metrics["by_model_tier"]
    acc     = metrics["model_accuracy"]
    signals = metrics["winning_signals"]
    sugg    = metrics["threshold_suggestion"]

    W = 62
    print(f"\n{'═' * W}")
    print(f"  TenderRadar — Decision Accuracy Report")
    print(f"  Generated: {metrics['generated_at']}")
    print(f"{'═' * W}")

    # ── Data coverage ──────────────────────────────────────────────────────────
    flag = "⚠  LOW CONFIDENCE" if cov["low_confidence"] else "✔  OK"
    print(f"\n  Data Coverage  [{flag}]")
    print(f"  {'─' * 56}")
    print(f"  Total in pipeline  : {cov['total_in_pipeline']:>6}")
    print(f"  Evaluated (outcomes): {cov['evaluated']:>6}  "
          f"({cov['coverage_pct']:.1f}% coverage)")
    print(f"  Note: {cov['note']}")

    # ── Overall metrics ───────────────────────────────────────────────────────
    print(f"\n  Overall Portfolio Metrics")
    print(f"  {'─' * 56}")
    print(f"  Bids placed        : {overall['total_bids_placed']:>6} / "
          f"{overall['total_evaluated']} evaluated")
    print(f"  Wins               : {overall['total_wins']:>6}")
    print(f"  Bid conversion rate: "
          f"  {_pct_str(overall['bid_conversion_rate'])}")
    print(f"  Win rate (overall) : "
          f"  {_pct_str(overall['win_rate'])}")
    print(f"  Win rate (of bids) : "
          f"  {_pct_str(overall['win_from_bid_rate'])}")

    # ── Model accuracy ────────────────────────────────────────────────────────
    print(f"\n  Model Accuracy")
    print(f"  {'─' * 56}")
    print(f"  BID_NOW precision  : "
          f"  {_pct_str(acc['precision_bid_now'])}  "
          f"(% of BID_NOW we acted on — target ≥ 70%)")
    print(f"  BID_NOW recall     : "
          f"  {_pct_str(acc['recall_bid_now'])}  "
          f"(% of all wins flagged BID_NOW — target ≥ 50%)")

    # ── Per-tier breakdown ────────────────────────────────────────────────────
    print(f"\n  Per-Tier Breakdown")
    print(f"  {'─' * 56}")
    hdr = f"  {'TIER':<18}  {'TOTAL':>5}  {'BID%':>6}  {'WIN%':>6}  {'WIN/BID':>7}  {'FP%':>6}"
    print(hdr)
    print(f"  {'─' * 56}")
    for tier in _ALL_TIERS:
        s    = by_tier.get(tier, {})
        em   = _TIER_EMOJI.get(tier, " ")
        row  = (
            f"  {em} {tier:<16}  "
            f"{s.get('total', 0):>5}  "
            f"{_pct_str(s.get('bid_rate')):>6}  "
            f"{_pct_str(s.get('win_rate')):>6}  "
            f"{_pct_str(s.get('win_from_bid_rate')):>7}  "
            f"{_pct_str(s.get('false_positive_rate')):>6}"
        )
        print(row)

    # ── Winning signals ───────────────────────────────────────────────────────
    if any(signals.get(k) for k in ("consulting_type", "sector", "organization")):
        print(f"\n  Top Winning Signals  (min 3 bids placed)")
        print(f"  {'─' * 56}")
        for field, label in [
            ("consulting_type", "By Consulting Type"),
            ("sector",          "By Sector"),
            ("organization",    "By Organization"),
        ]:
            items = signals.get(field, [])
            if items:
                print(f"  {label}:")
                for item in items[:3]:
                    print(
                        f"    {item['value']:<30}  "
                        f"win={_pct_str(item['win_rate'])}  "
                        f"({item['win_count']}W/{item['loss_count']}L, "
                        f"n={item['sample']})"
                    )

    # ── Threshold suggestion ──────────────────────────────────────────────────
    print(f"\n  Threshold Suggestion  "
          f"[confidence: {sugg.get('confidence','?')}]")
    print(f"  {'─' * 56}")
    if sugg.get("no_change"):
        print(f"  ✔  No adjustment needed.")
        print(f"  {sugg.get('reason', '')}")
    else:
        print(f"  ⚠  Adjustment recommended  [{sugg.get('rule_triggered')}]")
        print(f"  {sugg.get('reason', '')}")
        print(f"\n  Current → Suggested:")
        curr = sugg.get("current_thresholds", {})
        new  = sugg.get("suggested_thresholds", {})
        for k in new:
            if curr.get(k) != new.get(k):
                print(f"    {k:<32} {curr.get(k)} → {new.get(k)}")
        print(f"\n  To apply:  python3 pipeline/opportunity_pipeline.py --apply-tuning")

    print(f"\n{'═' * W}\n")


def _pct_str(val: Optional[float]) -> str:
    """Format a 0.0-1.0 float as '72%', or '—' if None."""
    if val is None:
        return "—"
    return f"{val*100:.0f}%"


# =============================================================================
# Self-test
# =============================================================================

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    print("=== decision_calibrator self-test ===\n")

    # Test with synthetic data (no DB needed)
    fake_rows = [
        {"model_decision_tag": "BID_NOW",         "bid_decision": "bid",    "outcome": "won",    "sector": "Health",     "consulting_type": "Evaluation",         "organization": "UNDP"},
        {"model_decision_tag": "BID_NOW",         "bid_decision": "bid",    "outcome": "won",    "sector": "Health",     "consulting_type": "Evaluation",         "organization": "UNDP"},
        {"model_decision_tag": "BID_NOW",         "bid_decision": "bid",    "outcome": "lost",   "sector": "Education",  "consulting_type": "Technical Assistance","organization": "World Bank"},
        {"model_decision_tag": "BID_NOW",         "bid_decision": "no_bid", "outcome": "no_bid", "sector": "Governance", "consulting_type": "Advisory/Policy",     "organization": "GIZ"},
        {"model_decision_tag": "STRONG_CONSIDER", "bid_decision": "bid",    "outcome": "won",    "sector": "Health",     "consulting_type": "Capacity Building",   "organization": "WHO"},
        {"model_decision_tag": "STRONG_CONSIDER", "bid_decision": "bid",    "outcome": "lost",   "sector": "Education",  "consulting_type": "Research/Study",      "organization": "UNICEF"},
        {"model_decision_tag": "STRONG_CONSIDER", "bid_decision": "no_bid", "outcome": "no_bid", "sector": "Urban",      "consulting_type": "Advisory/Policy",     "organization": "ADB"},
        {"model_decision_tag": "WEAK_CONSIDER",   "bid_decision": "no_bid", "outcome": "no_bid", "sector": "Agriculture","consulting_type": "General Consulting",  "organization": "FAO"},
        {"model_decision_tag": "IGNORE",          "bid_decision": "no_bid", "outcome": "no_bid", "sector": "unknown",    "consulting_type": "unknown",             "organization": "unknown"},
    ]

    # Tier stats
    for tier in _ALL_TIERS:
        stats = _compute_tier_stats(fake_rows, tier)
        print(f"  {tier:<18}: total={stats['total']}  "
              f"bid_rate={_pct_str(stats['bid_rate'])}  "
              f"win_rate={_pct_str(stats['win_rate'])}")

    # Signal correlation
    print("\n  Signal correlation:")
    sigs = compute_winning_signals(fake_rows)
    for field in ("consulting_type", "sector"):
        items = sigs.get(field, [])
        for item in items[:3]:
            print(f"    [{field}] {item['value']:<28} win={_pct_str(item['win_rate'])}  n={item['sample']}")

    # Suggestion with synthetic metrics (bid_rate=0.75, no trigger)
    fake_metrics = {
        "by_model_tier": {
            "BID_NOW": {"bid_rate": 0.75, "win_from_bid_rate": 0.67,
                        "total": 4, "bid_count": 3, "win_count": 2, "loss_count": 1, "no_bid_count": 1},
        },
        "overall": {"total_evaluated": 9},
    }
    suggest = suggest_threshold_adjustment(fake_metrics, pipeline_counts={"BID_NOW": 50, "STRONG_CONSIDER": 200, "WEAK_CONSIDER": 400, "IGNORE": 100})
    print(f"\n  Threshold suggestion: no_change={suggest['no_change']}  reason={suggest['reason'][:60]}…")

    print("\nSelf-test complete.")
