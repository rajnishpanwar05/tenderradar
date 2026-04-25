# TenderRadar — Intelligent Tender Monitoring System

[![CI](https://github.com/rajnishpanwar05/tenderradar/actions/workflows/ci.yml/badge.svg)](https://github.com/rajnishpanwar05/tenderradar/actions/workflows/ci.yml)
[![Code Quality & Security](https://github.com/rajnishpanwar05/tenderradar/actions/workflows/quality.yml/badge.svg)](https://github.com/rajnishpanwar05/tenderradar/actions/workflows/quality.yml)

> **Production ML system** for international development consulting firms.  
> Monitors tender portals globally, scores opportunities with ML, and surfaces bids via semantic search + RAG chatbot.

---

## Stats

| Metric | Value |
|--------|-------|
| Tenders indexed | **86,693** |
| Portal scrapers | **23** (World Bank, UNDP, USAID, GeM, GIZ, AfDB, EU TED, and more) |
| Deduplication accuracy | **98%** (cross-portal) |
| API routes | **9** (FastAPI) |
| ML model | GBM Classifier — 21-dim feature space, StratifiedKFold CV |
| Ranking metrics | nDCG@K · Precision@K · Recall@K (all from scratch) |
| Search latency | **<5 sec** over 86K tenders (ChromaDB + sentence-transformers) |
| Retraining | Weekly automated LR retraining via Celery/Redis |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      TenderRadar                        │
├──────────────┬──────────────────┬───────────────────────┤
│   Scrapers   │   Intelligence   │      API / UI          │
│              │                  │                        │
│ 23 portals   │  GBM Classifier  │  FastAPI (9 routes)   │
│ ThreadPool   │  Semantic Dedup  │  Next.js frontend     │
│ parallel     │  Fuzzy Dedup     │  RAG Chatbot          │
│ execution    │  RAG (GPT-4o /   │  (ChromaDB +          │
│              │  Gemini/Ollama)  │   LLM fallback chain) │
└──────────────┴──────────────────┴───────────────────────┘
         ↕                ↕                  ↕
    MySQL DB (5 tables + unified view)   Redis / Celery
```

---

## Key Features

**Scraping**
- 23 international portals scraped concurrently via `ThreadPoolExecutor`
- Portals: World Bank, UNDP, USAID, GeM, GIZ, AfDB, DTVP, TED (EU), TANEPS, UNGM, SIDBI, and more
- Configurable via `config/enabled_portals.json` — no code changes needed to add/remove portals

**ML Opportunity Scoring**
- GBM Classifier with 21 engineered features (keyword match, sector alignment, deadline urgency, geography score, etc.)
- StratifiedKFold cross-validation
- Weekly automated retraining as user feedback accumulates
- Ranking evaluated with nDCG@K, Precision@K, Recall@K — all implemented from scratch

**Deduplication (98% accuracy)**
- `sentence-transformers` semantic similarity for cross-portal dedup
- `SequenceMatcher` fuzzy matching with blocking index for speed
- Combined pipeline catches both exact and near-duplicate tenders across portals

**RAG Chatbot**
- Ask natural-language questions over the tender database
- LLM fallback chain: GPT-4o → Gemini → Ollama (local)
- Grounded on ChromaDB vector store of tender embeddings

**Infrastructure**
- FastAPI backend (9 endpoints: tenders, search, pipeline, chat, copilot, stats, health, performance, summary)
- Next.js dashboard frontend
- Docker Compose for one-command deployment
- Sentry error tracking, Redis/Celery task queue, MySQL (5 tables)

---

## Quickstart

### Option 1: Docker (Recommended)

```bash
git clone https://github.com/rajnishpanwar05/tenderradar.git
cd tenderradar

# Configure credentials
cp .env.example .env
# Fill in: DB credentials, OpenAI/Gemini key, portal logins (see .env.example)

# Start full stack
docker-compose up -d

# Open:
#   Frontend:  http://localhost:3000
#   API Docs:  http://localhost:8000/docs
```

### Option 2: Local Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env  # fill in your credentials

# Run pipeline (scrape → score → dedup)
python main.py

# Start API
python run_api.py
```

---

## Project Structure

```
tenderradar/
├── api/              # FastAPI app — 9 routes
├── core/             # Scraper base, pipeline runner, quality engine
├── scrapers/
│   └── portals/      # 23 individual portal scrapers
├── intelligence/     # Dedup, scoring, copilot, RAG query engine
├── pipeline/         # ML learning pipeline, Optuna HPO, calibrator
├── monitoring/       # Health checks, Sentry, scraper health manager
├── notifier/         # Telegram, email, WhatsApp alerts
├── exporters/        # Excel exporter, feedback sync
├── database/         # MySQL models + db.py
├── frontend/         # Next.js dashboard
├── config/
│   ├── config.py          # Central config — loads from .env
│   ├── enabled_portals.json
│   ├── firm_profile.json  # Firm sector/region preferences
│   └── idcg_keywords.json # Keyword intelligence for scoring
├── tests/            # Test suite
├── docker-compose.yml
├── Dockerfile
└── .env.example      # Template — copy to .env
```

---

## Configuration

All credentials and parameters load from `.env` (never hardcoded). Copy `.env.example` to `.env` and fill in:

- `DB_HOST / DB_USER / DB_PASSWORD / DB_NAME` — MySQL connection
- `OPENAI_API_KEY` or `GEMINI_API_KEY` — LLM for RAG chatbot
- Portal credentials (World Bank, UNDP, GeM, etc.) — see `.env.example`
- `TELEGRAM_BOT_TOKEN` / `NOTIFY_EMAIL_TO` — optional alerts

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Scraping | Python · `requests` · `BeautifulSoup` · `Selenium` · `ThreadPoolExecutor` |
| ML | `scikit-learn` GBM · custom nDCG/Precision/Recall · `Optuna` HPO |
| NLP / Search | `sentence-transformers` · `ChromaDB` · `SequenceMatcher` |
| LLM | OpenAI GPT-4o · Google Gemini · Ollama (local fallback) |
| Backend | `FastAPI` · `MySQL` · `Redis` · `Celery` |
| Frontend | `Next.js` · `TypeScript` |
| Infrastructure | `Docker Compose` · `Sentry` |

---

## Built By

Rajnish Panwar — ML Engineer  
MSc Business Analytics, Bayes Business School (City, University of London)  
[LinkedIn](https://www.linkedin.com/in/rajnish-panwar05) · [GitHub](https://github.com/rajnishpanwar05)
