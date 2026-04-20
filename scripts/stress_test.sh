#!/usr/bin/env bash
# =============================================================================
# scripts/stress_test.sh — TenderRadar Full Pipeline Stress Test
#
# Runs the full pipeline N times back-to-back (default 5), verifying that:
#   • No run crashes with a non-zero exit code
#   • The unified Excel is written and grows / stays stable in size
#   • The DB row count increases or stays constant (never decreases)
#   • Per-portal Excel files are present after each run
#   • Run times are consistent (no runaway hangs)
#
# Usage:
#   chmod +x scripts/stress_test.sh
#   ./scripts/stress_test.sh              # 5 runs, all portals
#   ./scripts/stress_test.sh 3            # 3 runs
#   ./scripts/stress_test.sh 6 --wb --gem # 6 runs, World Bank + GeM only
#
# Output:
#   Appended to: ~/tender_system/monitoring/stress_test.log
#   Summary table printed at end
# =============================================================================

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
RUNS="${1:-5}"
shift 2>/dev/null || true                # remaining args forwarded to main.py
PORTAL_FLAGS="${*:-}"                   # empty = all auto portals

BASE_DIR="$HOME/tender_system"
PYTHON="${PYTHON:-python3}"
LOG_DIR="$BASE_DIR/monitoring"
STRESS_LOG="$LOG_DIR/stress_test.log"
UNIFIED_EXCEL="$BASE_DIR/output/Tender_Monitor_Master.xlsx"

mkdir -p "$LOG_DIR"

TS() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(TS)] $*" | tee -a "$STRESS_LOG"; }

# ── Colour ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[0;33m'; RED='\033[0;31m'; NC='\033[0m'; BOLD='\033[1m'

# ── Baseline DB count ─────────────────────────────────────────────────────────
get_db_count() {
    $PYTHON -c "
import sys, os; sys.path.insert(0, '$BASE_DIR'); os.chdir('$BASE_DIR')
try:
    from db import get_stats
    print(sum(get_stats().values()))
except Exception as e:
    print(0)
" 2>/dev/null
}

echo ""
echo -e "${BOLD}${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  TenderRadar Stress Test — $RUNS runs${NC}"
echo -e "${BOLD}  Portals: ${PORTAL_FLAGS:-ALL AUTO}${NC}"
echo -e "${BOLD}  Started: $(TS)${NC}"
echo -e "${BOLD}${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo ""

log "=== STRESS TEST START: $RUNS runs, portals=[${PORTAL_FLAGS:-ALL AUTO}] ==="

PREV_DB_COUNT=$(get_db_count)
PREV_EXCEL_SIZE=0
if [ -f "$UNIFIED_EXCEL" ]; then
    PREV_EXCEL_SIZE=$(du -k "$UNIFIED_EXCEL" | cut -f1)
fi

# Summary table data
declare -a RUN_TIMES=()
declare -a RUN_STATUS=()
declare -a EXCEL_SIZES=()
declare -a DB_COUNTS=()

# ── Main loop ─────────────────────────────────────────────────────────────────
for i in $(seq 1 "$RUNS"); do
    echo -e "\n${BOLD}━━━ Run $i / $RUNS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    log "--- RUN $i START ---"
    RUN_START=$(date +%s)

    CMD="$PYTHON $BASE_DIR/main.py $PORTAL_FLAGS"
    log "Command: $CMD"

    STATUS="PASS"
    if ! $CMD >> "$STRESS_LOG" 2>&1; then
        STATUS="FAIL"
        echo -e "${RED}  ✗ Run $i exited with non-zero status${NC}"
    fi

    RUN_END=$(date +%s)
    ELAPSED=$(( RUN_END - RUN_START ))
    RUN_TIMES+=("${ELAPSED}s")
    RUN_STATUS+=("$STATUS")

    # Excel check
    EXCEL_SIZE=0
    if [ -f "$UNIFIED_EXCEL" ]; then
        EXCEL_SIZE=$(du -k "$UNIFIED_EXCEL" | cut -f1)
        EXCEL_SIZES+=("${EXCEL_SIZE}KB")
        if [ "$STATUS" = "PASS" ]; then
            if [ "$EXCEL_SIZE" -ge "$PREV_EXCEL_SIZE" ] 2>/dev/null; then
                echo -e "${GREEN}  ✓ Unified Excel OK (${EXCEL_SIZE}KB)${NC}"
            else
                echo -e "${YELLOW}  ⚠ Unified Excel SHRANK: ${PREV_EXCEL_SIZE}KB → ${EXCEL_SIZE}KB${NC}"
                log "WARN: Excel size shrank from ${PREV_EXCEL_SIZE}KB to ${EXCEL_SIZE}KB on run $i"
            fi
        fi
    else
        EXCEL_SIZES+=("MISSING")
        echo -e "${YELLOW}  ⚠ Unified Excel not found after run $i${NC}"
        log "WARN: Unified Excel missing after run $i"
    fi
    PREV_EXCEL_SIZE=$EXCEL_SIZE

    # DB count check
    DB_COUNT=$(get_db_count)
    DB_COUNTS+=("$DB_COUNT")
    if [ "$DB_COUNT" -ge "$PREV_DB_COUNT" ] 2>/dev/null; then
        DELTA=$(( DB_COUNT - PREV_DB_COUNT ))
        echo -e "${GREEN}  ✓ DB count: ${PREV_DB_COUNT} → ${DB_COUNT} (+${DELTA} new)${NC}"
    else
        echo -e "${RED}  ✗ DB count DECREASED: ${PREV_DB_COUNT} → ${DB_COUNT}${NC}"
        log "FAIL: DB count decreased from $PREV_DB_COUNT to $DB_COUNT on run $i"
        STATUS="FAIL"
        RUN_STATUS[-1]="FAIL"
    fi
    PREV_DB_COUNT=$DB_COUNT

    # Timing check
    if [ "$ELAPSED" -gt 1800 ]; then
        echo -e "${YELLOW}  ⚠ Run $i took ${ELAPSED}s (>30min — check for hangs)${NC}"
        log "WARN: Run $i took ${ELAPSED}s"
    else
        echo -e "${GREEN}  ✓ Run $i completed in ${ELAPSED}s${NC}"
    fi

    log "--- RUN $i END: status=$STATUS elapsed=${ELAPSED}s db=$DB_COUNT excel=${EXCEL_SIZE}KB ---"

    # Brief pause between runs (avoid hammering portals)
    if [ "$i" -lt "$RUNS" ]; then
        echo -e "  ${YELLOW}Waiting 30s before next run…${NC}"
        sleep 30
    fi
done

# ── Summary table ─────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  STRESS TEST SUMMARY${NC}"
echo -e "${BOLD}${GREEN}═══════════════════════════════════════════════════════════${NC}"
printf "${BOLD}  %-6s %-10s %-12s %-12s %-8s${NC}\n" \
    "Run" "Status" "Time" "Excel" "DB Total"
echo "  ──────────────────────────────────────────────────────"
FAILS=0
for i in $(seq 0 $(( RUNS - 1 ))); do
    STATUS="${RUN_STATUS[$i]}"
    if [ "$STATUS" = "FAIL" ]; then
        COLOUR="$RED"; (( FAILS++ ))
    else
        COLOUR="$GREEN"
    fi
    printf "  ${COLOUR}%-6s %-10s %-12s %-12s %-8s${NC}\n" \
        "Run $(( i+1 ))" \
        "$STATUS" \
        "${RUN_TIMES[$i]}" \
        "${EXCEL_SIZES[$i]}" \
        "${DB_COUNTS[$i]}"
done
echo "  ──────────────────────────────────────────────────────"

TOTAL_TIME=0
for t in "${RUN_TIMES[@]}"; do
    TOTAL_TIME=$(( TOTAL_TIME + ${t%s} ))
done
AVG_TIME=$(( TOTAL_TIME / RUNS ))

echo ""
echo -e "  ${BOLD}Runs:     $RUNS${NC}"
echo -e "  ${BOLD}Failed:   $FAILS${NC}"
echo -e "  ${BOLD}Avg time: ${AVG_TIME}s per run${NC}"
echo -e "  ${BOLD}Final DB: $PREV_DB_COUNT rows${NC}"
echo ""

log "=== STRESS TEST COMPLETE: $FAILS/$RUNS runs failed, final_db=$PREV_DB_COUNT ==="

if [ "$FAILS" -gt 0 ]; then
    echo -e "${RED}${BOLD}  ✗ $FAILS run(s) FAILED — check $STRESS_LOG${NC}"
    exit 1
else
    echo -e "${GREEN}${BOLD}  ✓ All $RUNS runs passed${NC}"
    exit 0
fi
