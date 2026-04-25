# Complete Pipeline Execution Guide

## Quick Start Commands

```bash
# Run ALL portals (default - recommended for production)
python3 main.py

# Run specific portal
python3 main.py --wb              # World Bank
python3 main.py --gem             # GeM BidPlus
python3 main.py --undp            # UNDP Procurement
python3 main.py --cg              # CG eProcurement

# Run with options
python3 main.py --dry-run          # No DB writes, no alerts
python3 main.py --no-parallel      # Sequential (single threaded)
python3 main.py --debug            # Verbose logging
python3 main.py --wb --debug       # Single portal + debug

# Start API (separate terminal)
python3 run_api.py                 # Starts FastAPI on http://localhost:8000
```

---

## Complete Pipeline Workflow (What Happens Inside)

```
main.py
    ↓
[1] CONFIGURATION VALIDATION
    • Validate all .env credentials
    • Check DB connection
    • Verify service status (Telegram, Email, etc)
    ↓
[2] DATABASE INITIALIZATION
    • Create tables if missing:
      - seen_tenders (dedup)
      - tender_structured_intel (enrichment)
      - bid_pipeline (opportunity pipeline)
      - scraper_health (monitoring)
    ↓
[3] PORTAL RESOLUTION
    • Parse CLI args (--wb, --gem, --cg, etc)
    • Build job list from registry.py
    • If no flags → run all 16 active portals
    ↓
[4] PARALLEL SCRAPING (ThreadPoolExecutor, max 7 workers)
    • World Bank → fetch all tenders
    • TED EU → fetch EU tenders
    • AfDB → fetch Consultants RFPs
    • ... [16 portals in parallel]
    • Each scraper:
      - Returns (new_tenders, all_rows)
      - Uses check_if_new() for dedup
      - Marks rows as seen with mark_as_seen()
    ↓
[5] CANONICAL NORMALIZATION
    • Merge new_tenders + all_rows
    • Extract tender_id, title, url, org, deadline, value
    • Add source_portal tag
    • Normalize fields via normalizer.py
    ↓
[6] SCRAPER HEALTH TRACKING
    • Record success/fail per portal
    • Update confidence scores
    • Flag unstable scrapers (health.db)
    ↓
[7] BID PIPELINE REGISTRATION
    • INSERT IGNORE into bid_pipeline
    • Marks tenders as 'discovered' state
    • Allows downstream opportunity_engine to process
    ↓
[8] CROSS-PORTAL FUZZY DEDUPLICATION
    • Compare new tenders vs DB (60-day window)
    • Fuzzy match: title + org + deadline
    • Mark duplicates, merge unique fields
    • Populate tender_cross_sources (same tender from multiple portals)
    ↓
[9] NORMALIZED MATERIALIZATION
    • Save raw scraper output to `tenders` table
    • Preserves descriptions/URLs if AI stages fail
    • Backfill historical records from seen_tenders
    ↓
[10] PHASE 3 INTELLIGENCE (Synchronous)
     • Process each tender through:
       - Keyword relevance scoring
       - Sector classification
       - Opportunity insights generation
       - Vector DB indexing (ChromaDB)
     • Backfill 1000 un-enriched historical tenders
    ↓
[11] OPPORTUNITY INSIGHTS BACKFILL
     • Generate strategic insights (500 tenders max per run)
     • Written to tender_structured_intel.opportunity_insight
    ↓
[12] DATA CONFIDENCE SCORING
     • Calculate per-tender confidence scores based on:
       - Source success rate
       - Data completeness
       - Deadline freshness
       - Organization authority
       - Sector alignment
    ↓
[13] PHASE 1/2 INTELLIGENCE (Synchronous Deep Enrichment)
     • For each new tender:
       - Scrape detail pages (PDFs, descriptions)
       - Extract: scope, budget, procurement type
       - Amendment detection (compare vs previous scrape)
     • Send amendment alerts if content changed
    ↓
[14] WORLD BANK EARLY PIPELINE (Optional)
     • Scan pre-RFP project signals
     • 16 new projects, consulting opportunities
     • Run in isolation if --wb-early flag set
    ↓
[15] EXCEL FEEDBACK SYNC
     • Read user's "My Decision" + "Outcome" columns
     • Sync back to bid_pipeline for learning
     • Compute feedback metrics
    ↓
[16] WEEKLY LEARNING PIPELINE (Auto on Monday)
     • Train GBM classifier on historical feedback
     • Compute ranking metrics (nDCG, Precision, Recall)
     • Evaluate decision accuracy
    ↓
[17] UNIFIED EXCEL EXPORT
     • Create master workbook with all portals
     • Columns: Tender ID, Title, Organization, Deadline, Value, Score, Sector
     • Dark blue header (1F3864, Calibri)
     • ~86K tenders indexed
    ↓
[18] EVIDENCE PACKAGING
     • For shortlisted tenders, create per-tender packages
     • Include: detail pages, PDFs, evidence summary
     • Write to ~/output/packages/
    ↓
[19] NOTIFICATIONS (Conditional)
     ├─ IF NOTIFICATIONS_ENABLED = true
     │  ├─ Group tenders by portal
     │  ├─ Amendment alerts → Telegram (if changes detected)
     │  └─ Rich alerts → Email (if enriched) or notify_all()
     ├─ IF dry_run → log only
     └─ IF no new tenders → skip
    ↓
[20] DATABASE STATISTICS
     • Query seen_tenders for top 10 sources
     • Print summary to run.log
    ↓
[21] END-OF-RUN SUMMARY
     • Total in DB: X tenders
     • Scraped this run: Y tenders
     • New this run: Z tenders
     • Enrichment coverage: A/B (C%)
     • Top priority score: D
     • Notification status
     ↓
[22] QUALITY METRICS
     • Average priority score (exclude un-scored)
     • High priority count (>75)
     • Top 5 scored tenders
     ↓
[23] DIAGNOSTICS REPORT
     • ASCII table: Portal | Rows | Time | Status
     • Send health Telegram if non-dry-run
     ↓
[24] DAILY DIGEST (Optional)
     • IF AUTO_DAILY_DIGEST = true
     • Email with master Excel + evidence packages
     • Check for duplicates (don't send same twice)
     ↓
[25] CLEANUP & EXIT
     • Log total runtime
     • Close DB connections
     • Exit 0 (success) or 1 (fatal error)
```

---

## All Portal Flags

```
python3 main.py --<flag>

PRIMARY PORTALS:
  --wb                 World Bank
  --gem                GeM BidPlus  
  --devnet             DevNet India
  --cg                 CG eProcurement
  --giz                GIZ India
  --undp               UNDP Procurement
  --ungm               UNGM (UN Global Marketplace)
  --afdb               AfDB Consultants
  --afd                AFD France
  --icfre              ICFRE Tenders
  --phfi               PHFI Tenders
  --jtds               JTDS Jharkhand
  --ted                TED EU (Tenders Electronic Daily)
  --iucn               IUCN Procurement
  --whh                Deutsche Welthungerhilfe
  --dtvp               DTVP Germany
  --ngobox             NGO Box

SECONDARY (NOT IN DEFAULT RUN):
  --sidbi              SIDBI Tenders
  --sam                SAM.gov (US Federal)
  --karnataka          Karnataka eProcure
  --usaid              USAID Sub-opportunities
  --maharashtra        Maharashtra Tenders
  --up                 UP eTenders
  --taneps             TANEPS Tanzania
  --adb                ADB (Asian Development Bank)
  --ec                 EC (European Commission)
  --devbusiness        Dev Business UN

SPECIAL:
  --wb-early           World Bank Early Pipeline (pre-RFP signals)
  --sikkim             Sikkim (manual CAPTCHA required)
  --nic                NIC State Portals (manual CAPTCHA required)

RUNNER OPTIONS:
  --portal FLAG        Run single portal by flag
  --dry-run            Skip DB writes & notifications
  --no-parallel        Sequential mode (single thread)
  --debug              Verbose logging
```

---

## Configuration (`.env`)

```bash
# DATABASE
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=tender_db

# OPENAI
OPENAI_API_KEY=sk-...

# GEMINI (fallback)
GEMINI_API_KEY=...

# NOTIFICATIONS
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
NOTIFY_EMAIL_TO=your@email.com
NOTIFICATIONS_ENABLED=true

# PORTAL CREDENTIALS (examples)
WB_API_KEY=...
GEM_USERNAME=...
GEM_PASSWORD=...
UNDP_USERNAME=...
USAID_USERNAME=...

# LEARNING PIPELINE
AUTO_DAILY_DIGEST=true
DAILY_DIGEST_ATTACH_PACKAGES=true
DAILY_DIGEST_DRY_RUN=false
DAILY_DIGEST_MAX_PACKAGES=10

# LOGGING
LOG_FILE=~/tender_system/run.log
DEBUG_MODE=false
```

---

## Directory Structure

```
~/tender_system/
├── main.py                    # Main orchestrator (you are here)
├── run_api.py                 # Start FastAPI
├── .env                       # Credentials (NOT in git)
├── .env.example               # Template
├── config/
│   ├── config.py             # Central config loader
│   ├── enabled_portals.json  # Portal on/off toggles
│   └── firm_profile.json     # Sector/region preferences
├── scrapers/
│   └── portals/
│       ├── worldbank_scraper.py
│       ├── ungm_scraper.py
│       ├── gem_scraper.py
│       ├── ... [16 portal scrapers]
├── core/
│   ├── registry.py           # Portal job definitions
│   ├── runner.py             # ThreadPoolExecutor orchestration
│   ├── reporter.py           # Diagnostics reporting
│   └── base_scraper.py       # Shared base class
├── intelligence/
│   ├── classifier.py         # GBM scoring
│   ├── deduplicator.py       # Semantic dedup
│   ├── fuzzy_dedup.py        # Fuzzy title matching
│   ├── vector_store.py       # ChromaDB
│   ├── opportunity_insights.py
│   ├── tender_intelligence.py # Phase 3 pipeline
│   └── normalizer.py         # Field normalization
├── database/
│   ├── db.py                 # MySQL helpers
│   ├── models.py             # Schema definitions
│   └── [5 tables: seen_tenders, tender_structured_intel, bid_pipeline, etc]
├── pipeline/
│   ├── opportunity_pipeline.py # Bid opportunity tracking
│   ├── learning_pipeline.py    # Weekly retraining
│   └── decision_calibrator.py  # Accuracy tracking
├── monitoring/
│   ├── logs.py               # Structured logging
│   ├── health_report.py      # Scraper health
│   ├── sentry.py             # Error tracking
│   └── scraper_health_manager.py
├── notifier/
│   ├── telegram_notifier.py
│   ├── daily_digest.py
│   └── ... [WhatsApp, email notifiers]
├── exporters/
│   ├── excel_exporter.py     # Master workbook
│   ├── evidence_packager.py  # Per-tender packages
│   └── excel_feedback_sync.py
├── api/
│   ├── run_api.py            # FastAPI startup
│   └── routes/
│       ├── tenders.py        # /api/tenders
│       ├── search.py         # /api/search
│       ├── pipeline.py       # /api/pipeline
│       └── ... [9 routes total]
├── output/
│   ├── master_tenders.xlsx   # Main export
│   └── packages/             # Evidence per tender
└── logs/
    └── tenderradar.log       # Structured JSON logs
```

---

## Running Examples

### 1. **Production Full Run** (All Portals)
```bash
python3 main.py
# Runs: 16 active portals in parallel
# Output: master_tenders.xlsx, daily digest email, Telegram alerts
# Time: ~15-20 min
```

### 2. **World Bank Only (Testing)**
```bash
python3 main.py --wb --debug
# Output: console debug + run.log
```

### 3. **Parallel Subset**
```bash
python3 main.py --wb --gem --undp --cg
# Just these 4 portals in parallel
```

### 4. **Sequential (Low Memory)**
```bash
python3 main.py --no-parallel
# Single thread, useful for low-RAM servers
```

### 5. **Dry Run (No Side Effects)**
```bash
python3 main.py --dry-run
# Excel export works, but:
# • No DB writes
# • No alerts sent
# • Good for testing new scrapers
```

### 6. **Early Bird Signals (WB Pre-RFP)**
```bash
python3 main.py --wb-early
# Isolated run: scans for project signals before RFPs
# Exits after this stage (skips other portals)
```

### 7. **Cron (Every 6 Hours)**
```bash
# Add to crontab:
0 */6 * * * /usr/bin/python3 ~/tender_system/main.py >> ~/tender_system/run.log 2>&1

# Runs: 00:00, 06:00, 12:00, 18:00 every day
# All output appended to run.log
```

---

## Key Outputs

### 1. **Excel Master Workbook**
- Path: `output/master_tenders.xlsx`
- Rows: ~86K (all tenders indexed)
- Columns:
  - Tender ID, Title, Organization
  - Deadline, Value, Source Portal
  - Sector, Relevance Score, Priority Score
  - My Decision, Outcome (user fills these in)
- Dark blue header (Calibri, 1F3864)

### 2. **Run Log**
- Path: `run.log`
- Human-readable timestamps + status
- Read by cron to monitor pipeline health

### 3. **Structured JSON Logs**
- Path: `logs/tenderradar.log`
- Rotating: 10MB × 5 files
- Timestamp + severity + message

### 4. **Evidence Packages**
- Path: `output/packages/<portal>_<tender_id>/`
- Contains: PDFs, detail pages, screenshots
- One folder per shortlisted tender

### 5. **Telegram Alerts** (if enabled)
```
New Tender Alert
───────────────────
Portal: World Bank
New: 5, Total: 486

Sector Breakdown:
  Infrastructure: 3
  Education: 2

Top Priority: 95 pts
```

### 6. **Daily Digest Email** (if AUTO_DAILY_DIGEST=true)
- Recipients: NOTIFY_EMAIL_TO
- Attachment: master_tenders.xlsx
- Attachment: evidence packages (optional)
- One email per 24hr max (duplicate detection)

---

## Monitoring Pipeline Health

### 1. **Check Last Run**
```bash
tail -100 run.log
# Shows: timestamps, portal status, counts, errors
```

### 2. **Check Structured Logs**
```bash
tail -50 logs/tenderradar.log | jq .
# JSON format: timestamp, level, message
```

### 3. **View Scraper Health**
```bash
sqlite3 database/health.db "SELECT * FROM scraper_runs ORDER BY timestamp DESC LIMIT 10;"
# Shows: success rate, stability, last 10 runs
```

### 4. **DB Stats**
```bash
# Inside run.log:
# Database stats (seen_tenders):
#   World Bank      : 486
#   TED EU          :  21
#   ... [10 sources listed]
```

### 5. **Vector DB Sync Check**
```bash
python3 -c "from intelligence.vector_store import check_vector_db_sync; check_vector_db_sync()"
# Verifies ChromaDB matches MySQL
```

---

## Common Issues & Fixes

### Issue: "FATAL: Database connection failed"
```bash
# Check MySQL is running:
mysql -u root -p -e "SELECT 1;"

# Check .env credentials:
cat .env | grep DB_

# Fix: Update DB_HOST, DB_USER, DB_PASSWORD in .env
```

### Issue: "No new tenders" (0 every run)
```bash
# 1. Check portal credentials in .env
# 2. Manually test one scraper:
python3 -c "from scrapers.portals.worldbank_scraper import run; new, all = run(); print(f'Found: {len(all)} total')"

# 3. If returns 0, portal may have changed structure
```

### Issue: "WARNING: Phase 3 intelligence failed"
```bash
# Usually non-fatal, but check:
tail -20 logs/tenderradar.log | grep error

# If recurring: restart API and retry
python3 run_api.py &  # in background
python3 main.py       # run again
```

### Issue: Out of Memory (ThreadPoolExecutor)
```bash
# Use sequential mode:
python3 main.py --no-parallel

# Or limit workers in core/runner.py:
# JobRunner.MAX_WORKERS = 3  (default 7)
```

### Issue: Duplicate Tenders Across Portals
```bash
# Fuzzy dedup logs to run.log:
# "Fuzzy dedup: 12 duplicate record(s) merged; 234 unique new tender(s) remain"

# Check quality:
sqlite3 database/health.db "SELECT COUNT(*) FROM tenders WHERE tender_cross_sources != '';"
# If high: dedup working well
```

---

## Performance Tips

### 1. **Speed Up Scraping**
- Parallel scrapers enabled by default (max 7 workers)
- Selenium scrapers capped at 2 concurrent (slower, so limited)
- To run faster: just wait, parallelization is already active

### 2. **Speed Up Intelligence**
- Phase 3 runs synchronously (fast, ~1-2 sec per tender)
- Phase 1/2 (deep enrichment) is slower (~5-10 sec per tender)
- To limit: edit intelligence/tender_intelligence.py limit=1000

### 3. **Reduce DB Query Time**
```bash
# Add indexes (run once):
mysql> CREATE INDEX idx_tender_id ON seen_tenders(tender_id);
mysql> CREATE INDEX idx_deadline ON seen_tenders(deadline);
mysql> CREATE INDEX idx_source ON seen_tenders(source_portal);
```

### 4. **Skip Slow Stages (Dry Runs)**
```bash
python3 main.py --dry-run
# Skips: Deep enrichment, learning pipeline, daily digest
# Keeps: Scraping, Excel export, local calculations
# Time: ~10 min instead of 20
```

---

## Next Steps

1. **Fill `.env`** with your credentials
2. **Test single portal**: `python3 main.py --wb`
3. **Check output**: `master_tenders.xlsx`
4. **Verify alerts**: Check Telegram/email
5. **Set cron**: `0 */6 * * * python3 main.py >> run.log 2>&1`
6. **Monitor**: Watch `run.log` and `logs/tenderradar.log`

---

## Questions?

- Check `/SYSTEM_STATE.md` for current architecture details
- Check individual scraper files in `scrapers/portals/` for portal-specific logic
- Check `core/registry.py` for all registered portals and their config
- See `config/enabled_portals.json` to enable/disable portals without code changes
