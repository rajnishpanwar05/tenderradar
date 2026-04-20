# =============================================================================
# Dockerfile — TenderRadar Full Stack
#
# Build: docker build -t tenderradar:latest .
# Run: docker run -p 8000:8000 -p 3000:3000 -v /path/to/.env:/app/.env tenderradar:latest
# =============================================================================

FROM python:3.11-slim

WORKDIR /app

# ── Install system dependencies ────────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    chromium-browser \
    chromium-chromedriver \
    mysql-client \
    postgresql-client \
    git \
    && rm -rf /var/lib/apt/lists/*

# ── Install Python dependencies ────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy application code ──────────────────────────────────────────────────
COPY . .

# ── Create required directories ────────────────────────────────────────────
RUN mkdir -p /app/output/portal_excels \
    && mkdir -p /app/logs \
    && mkdir -p /app/artifacts \
    && mkdir -p /app/chroma_db

# ── Copy environment template ──────────────────────────────────────────────
COPY .env.example .env.example

# ── Expose ports ───────────────────────────────────────────────────────────
# 8000 = FastAPI backend
# 3000 = Next.js frontend
EXPOSE 8000 3000

# ── Health check ───────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# ── Startup script ─────────────────────────────────────────────────────────
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
