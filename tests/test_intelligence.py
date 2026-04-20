"""
test_intelligence.py — Unit tests for the intelligence layer fit scorer.

Tests keyword scoring, semantic scoring direction, red flag detection,
and the process_batch output contract.
"""
import pytest
from intelligence.intelligence_layer import (
    compute_keyword_score,
    score_tender_fit,
    detect_red_flags,
    process_batch,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

HIGH_RELEVANCE = {
    "title": "Hiring of Evaluation Firm for Impact Evaluation of Mid-Day Meal Scheme — Bihar",
    "description": (
        "The World Bank invites proposals from qualified consulting firms to conduct "
        "an independent impact evaluation of the mid-day meal program in Bihar. "
        "The assignment includes a baseline survey, endline survey, and process "
        "documentation. Firm eligibility: minimum 5 years in M&E."
    ),
    "url": "https://worldbank.org/tender/001",
    "source": "worldbank",
    "deadline": "2027-05-15",
}

GOODS_ONLY = {
    "title": "Supply of 500 Laptops and Peripherals to District Collectorate",
    "description": "Procurement of laptop computers, keyboards, and mice for government office use.",
    "url": "https://gem.gov.in/bid/002",
    "source": "gem",
    "deadline": "2027-04-10",
}

WASH_TENDER = {
    "title": "Third Party Monitoring of WASH Infrastructure under Jal Jeevan Mission — Rajasthan",
    "description": (
        "UNICEF seeks a third-party monitoring agency to assess implementation quality "
        "of piped water schemes and ODF behavior change in rural Rajasthan. "
        "12-month assignment, team of 3 consultants."
    ),
    "url": "https://unicef.org/tender/003",
    "source": "ungm",
    "deadline": "2027-06-01",
}

EXPIRED_TENDER = {
    "title": "Consultancy for Governance Reform and Digital Public Administration — Kenya",
    "description": "GIZ invites proposals for institutional reform in Kenya's public administration.",
    "url": "https://giz.de/tender/004",
    "source": "giz",
    "deadline": "2020-03-01",  # clearly in the past
}

CIVIL_WORKS = {
    "title": "Construction of Bridge over River Mahanadi — Odisha PWD",
    "description": "Civil works contract for construction of a 120-metre two-lane bridge.",
    "url": "https://odisha.gov.in/tender/005",
    "source": "nic",
    "deadline": "2027-07-30",
}


# ── Tests ────────────────────────────────────────────────────────────────────

def test_high_relevance_keyword_score():
    """M&E + education tender achieves a meaningful keyword score."""
    kw_score, kw_cats = compute_keyword_score(
        HIGH_RELEVANCE["title"], HIGH_RELEVANCE["description"]
    )
    assert kw_score >= 20, f"Expected keyword_score ≥ 20, got {kw_score:.1f}"


def test_goods_tender_fit_score_low():
    """Pure goods procurement scores below consulting tenders."""
    fit_goods, _, _, _ = score_tender_fit(GOODS_ONLY["title"], GOODS_ONLY["description"])
    fit_wash, _, _, _ = score_tender_fit(WASH_TENDER["title"], WASH_TENDER["description"])
    assert fit_goods < fit_wash, (
        f"Goods ({fit_goods:.1f}) should score below consulting WASH ({fit_wash:.1f})"
    )


def test_consulting_scores_above_civil_works():
    """WASH consulting tender outscores civil construction."""
    fit_wash, _, _, _ = score_tender_fit(WASH_TENDER["title"], WASH_TENDER["description"])
    fit_civil, _, _, _ = score_tender_fit(CIVIL_WORKS["title"], CIVIL_WORKS["description"])
    assert fit_wash > fit_civil, (
        f"WASH consulting ({fit_wash:.1f}) should beat civil works ({fit_civil:.1f})"
    )


def test_expired_deadline_flag_detected():
    """Red flag EXPIRED is raised for deadlines clearly in the past."""
    flags = detect_red_flags(EXPIRED_TENDER["title"], None, EXPIRED_TENDER["deadline"])
    assert "EXPIRED" in flags, f"EXPIRED flag missing. Got: {flags}"


def test_civil_works_fit_score_very_low():
    """Civil construction achieves a very low fit score (below goods threshold)."""
    fit, _, _, _ = score_tender_fit(CIVIL_WORKS["title"], CIVIL_WORKS["description"])
    assert fit <= 40, f"Civil works scored too high: {fit:.1f}"


def test_process_batch_output_contract():
    """process_batch returns enriched objects sorted by fit_score descending."""
    raw = [HIGH_RELEVANCE, GOODS_ONLY, WASH_TENDER, CIVIL_WORKS]
    results = process_batch(raw)

    assert len(results) == 4, f"Expected 4 results, got {len(results)}"

    scores = [r.fit_score for r in results]
    assert scores == sorted(scores, reverse=True), f"Results not sorted: {scores}"

    for r in results:
        assert hasattr(r, "fit_score")
        assert hasattr(r, "red_flags")
        assert hasattr(r, "top_reasons")
        assert hasattr(r, "processing_time_ms")
        assert r.processing_time_ms > 0


def test_me_tender_ranks_above_civil_works():
    """M&E / education tender always ranks above civil construction."""
    raw = [HIGH_RELEVANCE, CIVIL_WORKS]
    results = process_batch(raw)
    me_score = next(r.fit_score for r in results if "Mid-Day" in r.title)
    civil_score = next(r.fit_score for r in results if "Bridge" in r.title)
    assert me_score > civil_score, (
        f"M&E ({me_score:.1f}) should outrank Bridge ({civil_score:.1f})"
    )
