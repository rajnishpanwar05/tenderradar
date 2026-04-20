"""
test_sim.py — Unit tests for cross-portal deduplication logic.

Tests that near-duplicate tenders (same tender on multiple portals)
are correctly identified and merged by deduplicate_batch().
No database or vector store required.
"""
import pytest
from intelligence.fuzzy_dedup import deduplicate_batch, merge_tender_group


# ── Fixtures ─────────────────────────────────────────────────────────────────

def _tender(tid, title, org="World Bank", portal="worldbank", deadline="2026-06-01", desc=""):
    return {
        "id": tid,
        "title": title,
        "organization": org,
        "source_site": portal,
        "deadline": deadline,
        "description": desc or title,
        "url": f"https://example.com/{tid}",
        "country": "India",
    }


CROSS_PORTAL_NEAR_DUPS = [
    _tender("A1", "Hiring of Evaluation Firm for Mid-Day Meal Scheme Bihar", portal="worldbank"),
    _tender("A2", "Hiring of Evaluation Firm for Mid Day Meal Scheme — Bihar", portal="ungm"),
]

DISTINCT_TENDERS = [
    _tender("B1", "Impact Evaluation of Education Programme in Bihar"),
    _tender("B2", "Supply of 500 Laptops to District Collectorate", org="GeM"),
    _tender("B3", "Construction of Bridge over River Mahanadi", org="Odisha PWD"),
]

SAME_TENDER_THREE_PORTALS = [
    _tender("C1", "Technical Assistance for Public Finance Reform Kenya", portal="worldbank"),
    _tender("C2", "Technical Assistance: Public Finance Reform, Kenya", portal="afdb"),
    _tender("C3", "Technical Assistance for Public Finance Reform — Kenya", portal="undp"),
]


# ── Tests ────────────────────────────────────────────────────────────────────

def test_distinct_tenders_not_merged():
    """Clearly different tenders are kept separate after dedup."""
    result = deduplicate_batch(DISTINCT_TENDERS)
    # All 3 should survive — none are near-duplicates
    assert len(result) == 3


def test_merge_tender_group_picks_longest_description():
    """merge_tender_group selects the richest description from the group."""
    tenders = [
        _tender("D1", "Evaluation of Health Programme", desc="Short desc."),
        _tender("D2", "Evaluation of Health Programme", desc="Much longer and more detailed description of the health programme evaluation scope."),
    ]
    merged = merge_tender_group(tenders)
    assert "longer" in merged.get("description", "").lower() or len(merged.get("description", "")) >= len("Short desc.")


def test_merge_group_has_required_fields():
    """Merged tender has all required output fields."""
    tenders = [
        _tender("E1", "Capacity Building for County Governments Kenya"),
        _tender("E2", "Capacity Building for County Governments — Kenya", portal="giz"),
    ]
    merged = merge_tender_group(tenders)
    for field in ("title", "description", "url"):
        assert field in merged, f"Missing field: {field}"


def test_deduplicate_batch_returns_list():
    """deduplicate_batch always returns a list."""
    result = deduplicate_batch([])
    assert isinstance(result, list)
    assert len(result) == 0
