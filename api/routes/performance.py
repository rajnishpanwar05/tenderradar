# =============================================================================
# api/routes/performance.py — Decision accuracy & feedback-loop endpoints
#
# Routes (all under /api/v1):
#
#   GET  /performance          → full calibration dashboard
#   POST /pipeline/outcome     → record a real-world bid outcome
# =============================================================================

from __future__ import annotations

import logging
import time
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, status as http_status

from api.schemas import (
    PerformanceResponse,
    DataCoverage,
    OverallMetrics,
    TierStats,
    ModelAccuracy,
    WinningSignals,
    WinningSignalItem,
    ThresholdSuggestion,
    OutcomeRequest,
    OutcomeResponse,
)

logger = logging.getLogger("tenderradar.api.performance")
router = APIRouter()


# =============================================================================
# Helpers
# =============================================================================

def _coerce_tier_stats(raw: Dict[str, Any]) -> TierStats:
    """Convert a raw dict from _compute_tier_stats() into a TierStats model."""
    return TierStats(
        total               = raw.get("total",               0),
        bid_count           = raw.get("bid_count",           0),
        no_bid_count        = raw.get("no_bid_count",        0),
        win_count           = raw.get("win_count",           0),
        loss_count          = raw.get("loss_count",          0),
        no_bid_outcome      = raw.get("no_bid_outcome",      0),
        bid_rate            = raw.get("bid_rate"),
        win_rate            = raw.get("win_rate"),
        win_from_bid_rate   = raw.get("win_from_bid_rate"),
        false_positive_rate = raw.get("false_positive_rate"),
    )


def _coerce_signal_list(items: list) -> list:
    return [
        WinningSignalItem(
            value      = item.get("value",      ""),
            win_rate   = item.get("win_rate"),
            win_count  = item.get("win_count",  0),
            loss_count = item.get("loss_count", 0),
            sample     = item.get("sample",     0),
        )
        for item in (items or [])
    ]


def _build_performance_response(summary: Dict[str, Any]) -> PerformanceResponse:
    """
    Convert the raw dict from get_performance_summary() into the
    typed PerformanceResponse model.
    """
    if not summary.get("ok", True):
        return PerformanceResponse(
            ok          = False,
            error       = summary.get("error", "Unknown error"),
            generated_at= summary.get("generated_at", ""),
        )

    # ── data_coverage ─────────────────────────────────────────────────────────
    cov_raw = summary.get("data_coverage", {})
    data_coverage = DataCoverage(
        total_in_pipeline = cov_raw.get("total_in_pipeline", 0),
        evaluated         = cov_raw.get("evaluated",         0),
        coverage_pct      = cov_raw.get("coverage_pct",      0.0),
        low_confidence    = cov_raw.get("low_confidence",     True),
        note              = cov_raw.get("note",               ""),
    )

    # ── overall ───────────────────────────────────────────────────────────────
    ov_raw = summary.get("overall", {})
    overall = OverallMetrics(
        total_evaluated      = ov_raw.get("total_evaluated",     0),
        total_bids_placed    = ov_raw.get("total_bids_placed",   0),
        total_wins           = ov_raw.get("total_wins",          0),
        bid_conversion_rate  = ov_raw.get("bid_conversion_rate"),
        win_rate             = ov_raw.get("win_rate"),
        win_from_bid_rate    = ov_raw.get("win_from_bid_rate"),
    )

    # ── by_tier ───────────────────────────────────────────────────────────────
    by_tier_raw = summary.get("by_tier", {})
    by_tier = {
        tier: _coerce_tier_stats(stats)
        for tier, stats in by_tier_raw.items()
    }

    # ── model_accuracy ────────────────────────────────────────────────────────
    ma_raw = summary.get("model_accuracy", {})
    model_accuracy = ModelAccuracy(
        precision_bid_now = ma_raw.get("precision_bid_now"),
        recall_bid_now    = ma_raw.get("recall_bid_now"),
        note              = ma_raw.get("note", ""),
    )

    # ── winning_signals ───────────────────────────────────────────────────────
    ws_raw = summary.get("winning_signals", {})
    winning_signals = WinningSignals(
        consulting_type = _coerce_signal_list(ws_raw.get("consulting_type", [])),
        sector          = _coerce_signal_list(ws_raw.get("sector",          [])),
        organization    = _coerce_signal_list(ws_raw.get("organization",    [])),
        note            = ws_raw.get("note", ""),
    )

    # ── threshold_suggestion ──────────────────────────────────────────────────
    ts_raw = summary.get("threshold_suggestion", {})
    threshold_suggestion = ThresholdSuggestion(
        no_change            = ts_raw.get("no_change",            True),
        rule_triggered       = ts_raw.get("rule_triggered"),
        reason               = ts_raw.get("reason",               ""),
        confidence           = ts_raw.get("confidence",           "low"),
        current_thresholds   = ts_raw.get("current_thresholds",   {}),
        suggested_thresholds = ts_raw.get("suggested_thresholds", {}),
    )

    return PerformanceResponse(
        ok                   = True,
        data_coverage        = data_coverage,
        overall              = overall,
        by_tier              = by_tier,
        model_accuracy       = model_accuracy,
        winning_signals      = winning_signals,
        threshold_suggestion = threshold_suggestion,
        generated_at         = summary.get("generated_at", ""),
    )


# =============================================================================
# GET /performance
# =============================================================================

@router.get(
    "/performance",
    response_model = PerformanceResponse,
    summary        = "Decision accuracy dashboard",
    description    = (
        "Returns the full feedback-loop performance dashboard:\\n\\n"
        "- **data_coverage** — how many pipeline outcomes have been recorded\\n"
        "- **overall** — portfolio bid conversion rate and win rate\\n"
        "- **by_tier** — per-tier accuracy metrics "
        "(BID_NOW / STRONG_CONSIDER / WEAK_CONSIDER / IGNORE)\\n"
        "- **model_accuracy** — BID_NOW precision (% we acted on) "
        "and recall (% of wins flagged)\\n"
        "- **winning_signals** — which sectors / consulting types / organisations "
        "correlate with wins\\n"
        "- **threshold_suggestion** — adaptive tuning recommendation "
        "(apply with `--apply-tuning` CLI)\\n\\n"
        "> Requires at least 20 evaluated outcomes for reliable calibration. "
        "The `data_coverage.low_confidence` flag is `true` below that threshold."
    ),
    responses      = {
        503: {"description": "Calibrator or database unavailable"},
    },
)
def get_performance() -> PerformanceResponse:
    t0 = time.perf_counter()

    try:
        from pipeline.decision_calibrator import get_performance_summary
    except ImportError as exc:
        raise HTTPException(
            status_code = http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail      = f"Calibrator module unavailable: {exc}",
        )

    summary = get_performance_summary()

    response = _build_performance_response(summary)

    elapsed = round((time.perf_counter() - t0) * 1000, 1)
    logger.info(
        "[api] GET /performance  evaluated=%d  ok=%s  %.1fms",
        response.overall.total_evaluated,
        response.ok,
        elapsed,
    )

    return response


# =============================================================================
# POST /pipeline/outcome
# =============================================================================

@router.post(
    "/pipeline/outcome",
    response_model = OutcomeResponse,
    summary        = "Record a bid outcome",
    description    = (
        "Records the real-world result of a tender bid so TenderRadar can "
        "calibrate its decision-tier accuracy over time.\\n\\n"
        "**outcome** values:\\n"
        "- `won` — bid submitted and contract awarded\\n"
        "- `lost` — bid submitted but not selected\\n"
        "- `no_bid` — firm decided not to submit a bid\\n\\n"
        "**bid_decision** values:\\n"
        "- `bid` *(default)* — firm submitted a proposal\\n"
        "- `no_bid` — firm explicitly decided to skip\\n"
        "- `pending` — decision not yet finalised\\n\\n"
        "Calling this endpoint also advances the pipeline `status` to "
        "`won` or `lost` automatically when the outcome matches.\\n\\n"
        "**Example:**\\n"
        "```json\\n"
        '{\"tender_id\": \"abc123\", \"outcome\": \"won\", \"bid_decision\": \"bid\"}\\n'
        "```"
    ),
    responses      = {
        404: {"description": "Tender not found in pipeline"},
        422: {"description": "Validation error (invalid outcome / bid_decision)"},
        503: {"description": "Database unavailable"},
    },
)
def record_outcome(body: OutcomeRequest) -> OutcomeResponse:
    try:
        from pipeline.opportunity_pipeline import record_outcome as _record_outcome
    except ImportError as exc:
        raise HTTPException(
            status_code = http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail      = f"Pipeline module unavailable: {exc}",
        )

    success = _record_outcome(
        tender_id    = body.tender_id,
        outcome      = body.outcome,
        bid_decision = body.bid_decision,
    )

    if not success:
        raise HTTPException(
            status_code = http_status.HTTP_404_NOT_FOUND,
            detail      = (
                f"Tender '{body.tender_id}' not found in pipeline, "
                "or outcome was already recorded. "
                "Ensure the tender is registered with `pipeline --init` first."
            ),
        )

    logger.info(
        "[api] outcome recorded: tender_id=%s  outcome=%s  bid_decision=%s",
        body.tender_id, body.outcome, body.bid_decision,
    )

    outcome_labels = {"won": "Won 🏆", "lost": "Lost", "no_bid": "No bid recorded"}
    return OutcomeResponse(
        success   = True,
        tender_id = body.tender_id,
        outcome   = body.outcome,
        message   = (
            f"{outcome_labels.get(body.outcome, body.outcome)} — "
            f"outcome recorded for '{body.tender_id}'. "
            "Run GET /performance to see updated calibration metrics."
        ),
    )
