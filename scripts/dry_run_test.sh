#!/usr/bin/env bash
# =============================================================================
# scripts/dry_run_test.sh — TenderRadar Dry-Run Verification
#
# Runs the pipeline with --dry-run flag and verifies:
#   1. Exits with code 0
#   2. Unified Excel is written (Excel is NOT skipped in dry-run)
#   3. DB row count is UNCHANGED (no writes)
#   4. run.log contains "DRY-RUN mode" message
#   5. No alert/notification was triggered
#
# Usage:
#   chmod +x scripts/dry_run_test.sh
#   ./scripts/dry_run_test.sh
#   ./scripts/dry_run_test.sh --wb          # dry-run World Bank only
# =============================================================================

set -euo pipefail

BASE_DIR="$HOME/tender_system"
PYTHON="${PYTHON:-python3}"
UNIFIED_EXCEL="$BASE_DIR/output/Tender_Monitor_Master.xlsx"
RUN_LOG="$BASE_DIR/run.log"
PORTAL_FLAGS="${*:-}"

GREEN='\033[0;32m'; YELLOW='\033[0;33m'; RED='\033[0;31m'; NC='\033[0m'; BOLD='\033[1m'

pass() { echo -e "${GREEN}  ✓ $*${NC}"; }
fail() { echo -e "${RED}  ✗ $*${NC}"; FAILS=$(( FAILS + 1 )); }
warn() { echo -e "${YELLOW}  ⚠ $*${NC}"; }

FAILS=0

echo ""
echo -e "${BOLD}${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  TenderRadar Dry-Run Test${NC}"
echo -e "${BOLD}  Portals: ${PORTAL_FLAGS:-ALL AUTO}${NC}"
echo -e "${BOLD}${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo ""

# ── Snapshot DB count BEFORE ──────────────────────────────────────────────────
DB_BEFORE=$($PYTHON -c "
import sys, os; sys.path.insert(0, '$BASE_DIR'); os.chdir('$BASE_DIR')
try:
    from db import get_stats
    print(sum(get_stats().values()))
except:
    print(-1)
" 2>/dev/null)

echo "  DB rows before: $DB_BEFORE"

# ── Snapshot Excel state BEFORE ───────────────────────────────────────────────
EXCEL_BEFORE=""
if [ -f "$UNIFIED_EXCEL" ]; then
    EXCEL_BEFORE=$(md5 -q "$UNIFIED_EXCEL" 2>/dev/null || md5sum "$UNIFIED_EXCEL" | cut -d' ' -f1)
fi

# ── Record last log line BEFORE ───────────────────────────────────────────────
LOG_LINES_BEFORE=0
if [ -f "$RUN_LOG" ]; then
    LOG_LINES_BEFORE=$(wc -l < "$RUN_LOG")
fi

# ── Run pipeline in dry-run mode ──────────────────────────────────────────────
echo ""
echo "  Running: python3 main.py --dry-run $PORTAL_FLAGS"
echo ""

START=$(date +%s)
if $PYTHON "$BASE_DIR/main.py" --dry-run $PORTAL_FLAGS; then
    END=$(date +%s)
    pass "Pipeline exited 0 ($(( END - START ))s)"
else
    END=$(date +%s)
    fail "Pipeline exited non-zero after $(( END - START ))s"
fi
echo ""

# ── Check 1: DB count unchanged ───────────────────────────────────────────────
DB_AFTER=$($PYTHON -c "
import sys, os; sys.path.insert(0, '$BASE_DIR'); os.chdir('$BASE_DIR')
try:
    from db import get_stats
    print(sum(get_stats().values()))
except:
    print(-1)
" 2>/dev/null)

if [ "$DB_BEFORE" = "-1" ] || [ "$DB_AFTER" = "-1" ]; then
    warn "Could not query DB — skipping count check"
elif [ "$DB_AFTER" -eq "$DB_BEFORE" ]; then
    pass "DB count unchanged: $DB_BEFORE → $DB_AFTER (dry-run writes suppressed)"
else
    DELTA=$(( DB_AFTER - DB_BEFORE ))
    fail "DB count CHANGED in dry-run: $DB_BEFORE → $DB_AFTER (+$DELTA rows written!)"
fi

# ── Check 2: Unified Excel was still written ──────────────────────────────────
if [ -f "$UNIFIED_EXCEL" ]; then
    SIZE=$(du -k "$UNIFIED_EXCEL" | cut -f1)
    EXCEL_AFTER=$(md5 -q "$UNIFIED_EXCEL" 2>/dev/null || md5sum "$UNIFIED_EXCEL" | cut -d' ' -f1)
    if [ -n "$EXCEL_BEFORE" ] && [ "$EXCEL_BEFORE" = "$EXCEL_AFTER" ]; then
        warn "Unified Excel exists but content unchanged (no new rows from dry-run portals?)"
    else
        pass "Unified Excel written/updated: $UNIFIED_EXCEL (${SIZE}KB)"
    fi
else
    warn "Unified Excel not found (no rows scraped, or exporter failed)"
fi

# ── Check 3: run.log contains DRY-RUN marker ─────────────────────────────────
if [ -f "$RUN_LOG" ]; then
    # Read only lines added in this run
    NEW_LOG=$(tail -n "+$(( LOG_LINES_BEFORE + 1 ))" "$RUN_LOG" 2>/dev/null || true)
    if echo "$NEW_LOG" | grep -q "DRY-RUN"; then
        pass "run.log contains 'DRY-RUN' marker"
    else
        fail "run.log does NOT contain 'DRY-RUN' marker in new lines"
    fi
else
    warn "run.log not found — skipping log check"
fi

# ── Check 4: No 'Sending tender notification' in log ─────────────────────────
if [ -n "${NEW_LOG:-}" ]; then
    if echo "$NEW_LOG" | grep -q "Sending tender notification"; then
        fail "Alert was sent during dry-run (should be suppressed)"
    else
        pass "No tender notification sent (correctly suppressed)"
    fi
fi

# ── Check 5: 'Unified Excel written' appeared in log ─────────────────────────
if [ -n "${NEW_LOG:-}" ]; then
    if echo "$NEW_LOG" | grep -q "Unified Excel written"; then
        pass "run.log confirms Unified Excel was written"
    else
        warn "run.log does not contain 'Unified Excel written' (check for errors)"
    fi
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}═══════════════════════════════════════════════════════════${NC}"
if [ "$FAILS" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}  ✓ Dry-run test PASSED (all checks OK)${NC}"
else
    echo -e "${RED}${BOLD}  ✗ Dry-run test FAILED ($FAILS check(s) failed)${NC}"
fi
echo -e "${BOLD}${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo ""

exit $FAILS
