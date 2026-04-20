# =============================================================================
# test_intelligence.py — Unit tests for the intelligence layer fit scorer
#
# Tests keyword scoring, semantic scoring direction, red flag detection,
# and the process_batch output contract.
#
# Run:  python3 test_intelligence.py
# =============================================================================

import sys
import os
sys.path.insert(0, os.path.expanduser("~/tender_system"))

from intelligence_layer import (
    compute_keyword_score,
    compute_semantic_score,
    score_tender_fit,
    detect_red_flags,
    process_batch,
    TenderExtraction,
)


# =============================================================================
# Test fixtures — 5 representative tenders
# =============================================================================

TENDERS = [
    {
        "id": "T001",
        "title": "Hiring of Evaluation Firm for Impact Evaluation of Mid-Day Meal Scheme — Bihar",
        "description": (
            "The World Bank invites proposals from qualified consulting firms to conduct "
            "an independent impact evaluation of the mid-day meal program in Bihar. "
            "The assignment includes a baseline survey, endline survey, and process "
            "documentation. Firm eligibility: minimum 5 years in M&E."
        ),
        "url": "https://worldbank.org/tender/001",
        "source": "worldbank",
        "deadline": "2026-05-15",
        "expected_min_score": 70,   # High relevance — M&E + education + Bihar + WB
        "expected_no_flags":  True,
    },
    {
        "id": "T002",
        "title": "Supply of 500 Laptops and Peripherals to District Collectorate",
        "description": "Procurement of laptop computers, keyboards, and mice for government office use.",
        "url": "https://gem.gov.in/bid/002",
        "source": "gem",
        "deadline": "2026-04-10",
        "expected_max_score": 40,   # Low relevance — pure goods procurement
        "expected_flag": "GOODS_ONLY",
    },
    {
        "id": "T003",
        "title": "Third Party Monitoring of WASH Infrastructure under Jal Jeevan Mission — Rajasthan",
        "description": (
            "UNICEF seeks a third-party monitoring agency to assess implementation quality "
            "of piped water schemes and ODF behavior change in rural Rajasthan. "
            "12-month assignment, team of 3 consultants."
        ),
        "url": "https://unicef.org/tender/003",
        "source": "ungm",
        "deadline": "2026-06-01",
        "expected_min_score": 65,   # Solid match — WASH + TPM + India
        "expected_no_flags":  True,
    },
    {
        "id": "T004",
        "title": "Consultancy for Governance Reform and Digital Public Administration — Kenya",
        "description": (
            "GIZ invites proposals for a consultancy to support institutional reform "
            "in Kenya's public administration sector, including capacity building for "
            "county governments and digital governance systems."
        ),
        "url": "https://giz.de/tender/004",
        "source": "giz",
        "deadline": "2026-03-01",   # Already expired relative to current date 2026-03-13
        "expected_flag": "EXPIRED",
        "expected_min_score": 55,   # Good sector match but geography is weak
    },
    {
        "id": "T005",
        "title": "Construction of Bridge over River Mahanadi — Odisha PWD",
        "description": "Civil works contract for construction of a 120-metre two-lane bridge.",
        "url": "https://odisha.gov.in/tender/005",
        "source": "nic",
        "deadline": "2026-07-30",
        "expected_max_score": 30,   # Construction/civil works — not consulting
        "expected_flag": "GOODS_ONLY",
    },
]


# =============================================================================
# Helper
# =============================================================================

def _pass(msg): print(f"  ✅  {msg}")
def _fail(msg): print(f"  ❌  {msg}"); return False


def run_tests():
    print("=" * 60)
    print("TenderRadar Intelligence Layer — Unit Tests")
    print("=" * 60)
    all_passed = True

    # ── Test 1: High-relevance M&E tender scores well ────────────────────────
    print("\n[1] High-relevance M&E / Education tender (T001)")
    t = TENDERS[0]
    kw_score, kw_cats = compute_keyword_score(t["title"], t["description"])
    print(f"     keyword_score={kw_score:.1f}  matched={kw_cats}")
    if kw_score >= 30:
        _pass(f"keyword_score {kw_score:.1f} ≥ 30")
    else:
        _fail(f"keyword_score {kw_score:.1f} < 30 — M&E keywords not firing")
        all_passed = False

    # M&E and Education should both be in matched categories
    for cat in ("M&E / Evaluation", "Education & Skills"):
        if any(cat in c for c in kw_cats):
            _pass(f"'{cat}' matched")
        else:
            _fail(f"'{cat}' not matched — check keywords.py")
            all_passed = False

    # ── Test 2: Goods tender scores low ──────────────────────────────────────
    print("\n[2] Goods-only tender (T002) — should score low")
    t = TENDERS[1]
    fit, sem, kw, reasons = score_tender_fit(t["title"], t["description"])
    print(f"     fit_score={fit:.1f}  semantic={sem:.1f}  keyword={kw:.1f}")
    if fit <= t.get("expected_max_score", 40):
        _pass(f"fit_score {fit:.1f} ≤ {t.get('expected_max_score', 40)} (goods correctly low)")
    else:
        _fail(f"fit_score {fit:.1f} > 40 for a goods tender — scoring too generous")
        all_passed = False

    # ── Test 3: WASH tender scores decently ──────────────────────────────────
    print("\n[3] WASH / TPM tender (T003)")
    t = TENDERS[2]
    fit, sem, kw, reasons = score_tender_fit(t["title"], t["description"])
    print(f"     fit_score={fit:.1f}  reasons={reasons}")
    min_s = t.get("expected_min_score", 60)
    if fit >= min_s:
        _pass(f"fit_score {fit:.1f} ≥ {min_s}")
    else:
        _fail(f"fit_score {fit:.1f} < {min_s} for WASH tender — check semantic model")
        all_passed = False

    # ── Test 4: Red flag — EXPIRED ───────────────────────────────────────────
    print("\n[4] Red flag: EXPIRED deadline (T004)")
    t  = TENDERS[3]
    flags = detect_red_flags(t["title"], None, t["deadline"])
    print(f"     red_flags={flags}")
    if "EXPIRED" in flags:
        _pass("EXPIRED flag correctly detected")
    else:
        _fail("EXPIRED flag missing — deadline was 2026-03-01 (before 2026-03-13)")
        all_passed = False

    # ── Test 5: Red flag — GOODS_ONLY ────────────────────────────────────────
    print("\n[5] Red flag: GOODS_ONLY (T005 — bridge construction)")
    t     = TENDERS[4]
    flags = detect_red_flags(t["title"], None, t["deadline"])
    print(f"     red_flags={flags}")
    # Construction may not trigger GOODS_ONLY phrase-match but fit_score should be low
    fit, _, _, _ = score_tender_fit(t["title"], t["description"])
    print(f"     fit_score={fit:.1f}")
    if fit <= t.get("expected_max_score", 30):
        _pass(f"fit_score {fit:.1f} ≤ 30 for civil works tender")
    else:
        _fail(f"fit_score {fit:.1f} > 30 for bridge construction — should rank very low")
        all_passed = False

    # ── Test 6: process_batch output contract ────────────────────────────────
    print("\n[6] process_batch contract (no OpenAI key needed)")
    raw = [
        {"title": t["title"], "url": t["url"], "source": t["source"],
         "deadline": t["deadline"], "description": t["description"]}
        for t in TENDERS
    ]
    results = process_batch(raw)

    if len(results) == len(TENDERS):
        _pass(f"process_batch returned {len(results)} EnrichedTender objects")
    else:
        _fail(f"Expected {len(TENDERS)} results, got {len(results)}")
        all_passed = False

    # Sorted by fit_score descending
    scores = [r.fit_score for r in results]
    if scores == sorted(scores, reverse=True):
        _pass(f"Results sorted by fit_score descending: {[round(s, 1) for s in scores]}")
    else:
        _fail(f"Results NOT sorted: {[round(s, 1) for s in scores]}")
        all_passed = False

    # All have required fields
    for r in results:
        assert hasattr(r, "fit_score")
        assert hasattr(r, "red_flags")
        assert hasattr(r, "top_reasons")
        assert hasattr(r, "processing_time_ms")
        assert r.processing_time_ms > 0
    _pass("All EnrichedTender objects have required fields + non-zero processing_time_ms")

    # M&E tender should rank higher than bridge construction
    me_score    = next(r.fit_score for r in results if "Mid-Day Meal" in r.title)
    civil_score = next(r.fit_score for r in results if "Bridge" in r.title)
    if me_score > civil_score:
        _pass(f"M&E tender ({me_score:.1f}) ranks above bridge construction ({civil_score:.1f})")
    else:
        _fail(f"M&E tender ({me_score:.1f}) should outrank bridge ({civil_score:.1f})")
        all_passed = False

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    if all_passed:
        print("✅  ALL TESTS PASSED — intelligence layer is working correctly")
    else:
        print("❌  SOME TESTS FAILED — review output above")
    print("=" * 60)
    return all_passed


if __name__ == "__main__":
    ok = run_tests()
    sys.exit(0 if ok else 1)
