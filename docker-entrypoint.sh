#!/bin/bash
set -e

# =============================================================================
# docker-entrypoint.sh — TenderRadar Container Startup
# =============================================================================

echo "=========================================="
echo "TenderRadar Container Startup"
echo "=========================================="

# Check if .env exists, if not, copy from .env.example
if [ ! -f /app/.env ]; then
    if [ -f /app/.env.example ]; then
        echo "⚠️  .env not found. Copying from .env.example"
        cp /app/.env.example /app/.env
        echo "⚠️  IMPORTANT: Edit .env with your actual credentials!"
    fi
fi

# Accept either the new DB_PASSWORD name or the legacy DB_PASS alias.
if [ -z "$DB_PASS" ] && [ -n "$DB_PASSWORD" ]; then
    export DB_PASS="$DB_PASSWORD"
fi

# Verify critical environment variables
if [ -z "$DB_HOST" ]; then
    echo "❌ ERROR: DB_HOST not set"
    exit 1
fi

if [ -z "$DB_USER" ]; then
    echo "❌ ERROR: DB_USER not set"
    exit 1
fi

if [ -z "$DB_PASS" ]; then
    echo "⚠️  WARNING: DB_PASS not set (using empty password)"
fi

echo "✓ Environment check passed"
echo "  DB_HOST: $DB_HOST"
echo "  DB_USER: $DB_USER"
echo "  DB_NAME: $DB_NAME"

# Wait for MySQL to be ready
echo "Waiting for MySQL to be ready..."
for i in {1..30}; do
    if nc -z $DB_HOST $DB_PORT 2>/dev/null; then
        echo "✓ MySQL is ready"
        break
    fi
    echo "  (attempt $i/30)"
    sleep 2
done

# Initialize database if needed
echo "Initializing database..."
python -c "from database.db import init_db; init_db()" 2>/dev/null || true

echo "=========================================="
echo "✓ Startup complete!"
echo "=========================================="

# Run the command passed to the container
exec "$@"
