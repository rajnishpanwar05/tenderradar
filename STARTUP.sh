#!/bin/bash
set -e

echo "=================================="
echo "   TenderRadar Auto Setup"
echo "=================================="
echo ""

cd ~/tender_system

# ============================================================================
# STEP 1: Create Virtual Environment
# ============================================================================
echo "[1/5] Creating virtual environment..."
if [ -d "venv" ]; then
    echo "      venv already exists, skipping..."
else
    python3 -m venv venv
    echo "      ✅ venv created"
fi

source venv/bin/activate
echo "      ✅ venv activated"
echo ""

# ============================================================================
# STEP 2: Install Dependencies
# ============================================================================
echo "[2/5] Installing dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q
echo "      ✅ All packages installed"
echo ""

# ============================================================================
# STEP 3: Create .env if missing
# ============================================================================
echo "[3/5] Configuring .env..."
if [ ! -f .env ]; then
    cat > .env << 'ENVFILE'
# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=root
DB_NAME=tender_db

# ============================================================================
# NOTIFICATIONS (Disabled for initial setup)
# ============================================================================
NOTIFICATIONS_ENABLED=false
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
NOTIFY_EMAIL_TO=

# ============================================================================
# API KEYS (Optional - add later)
# ============================================================================
OPENAI_API_KEY=
GEMINI_API_KEY=

# ============================================================================
# LEARNING & DIGEST
# ============================================================================
AUTO_DAILY_DIGEST=false
DAILY_DIGEST_DRY_RUN=true
DAILY_DIGEST_MAX_PACKAGES=5

# ============================================================================
# LOGGING
# ============================================================================
DEBUG_MODE=false
ENVFILE
    echo "      ✅ .env created with default values"
else
    echo "      .env already exists, skipping..."
fi
echo ""

# ============================================================================
# STEP 4: Start MySQL (if not running)
# ============================================================================
echo "[4/5] Checking MySQL..."

# Try to connect to MySQL
if mysql -h localhost -u root -p"root" -e "SELECT 1;" >/dev/null 2>&1; then
    echo "      ✅ MySQL already running"
else
    echo "      ⚠️  MySQL not running. Attempting to start..."

    # Try Docker first
    if command -v docker &> /dev/null; then
        # Check if container already exists
        if docker ps -a --format '{{.Names}}' | grep -q '^mysql_tender$'; then
            docker start mysql_tender >/dev/null 2>&1
            echo "      ✅ Started existing MySQL container"
        else
            docker run -d \
              --name mysql_tender \
              -p 3306:3306 \
              -e MYSQL_ROOT_PASSWORD=root \
              -e MYSQL_DATABASE=tender_db \
              mysql:8.0 >/dev/null 2>&1
            echo "      ⏳ Docker MySQL starting (wait 5 sec)..."
            sleep 5
        fi
    else
        echo "      ⚠️  Docker not found. Please start MySQL manually:"
        echo "         macOS: brew services start mysql@8.0"
        echo "         Linux: sudo systemctl start mysql"
        echo "         Or use Docker: docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root mysql:8.0"
    fi
fi

echo ""

# ============================================================================
# STEP 5: Initialize Database
# ============================================================================
echo "[5/5] Initializing database..."
python3 << 'PYEOF'
import sys
try:
    from database.db import init_db, preflight_db_connection
    print("      Checking DB connection...")
    status = preflight_db_connection()
    print(f"      ✅ Connected to {status['host']}:{status['port']}")
    print("      Initializing tables...")
    init_db()
    print("      ✅ Database ready!")
except Exception as e:
    print(f"      ❌ ERROR: {e}")
    print("      Make sure MySQL is running and credentials are correct in .env")
    sys.exit(1)
PYEOF

if [ $? -ne 0 ]; then
    exit 1
fi

echo ""
echo "=================================="
echo "   ✅ Setup Complete!"
echo "=================================="
echo ""
echo "Your project is ready to run!"
echo ""
echo "To start the pipeline, just run:"
echo ""
echo "  cd ~/tender_system"
echo "  source venv/bin/activate"
echo "  python3 main.py"
echo ""
echo "Or use these commands:"
echo ""
echo "  python3 main.py --gem --dry-run      # Test with GeM only"
echo "  python3 main.py --wb                  # World Bank only"
echo "  python3 main.py --dry-run             # Dry run (no DB writes)"
echo "  python3 main.py                       # Run all portals"
echo ""
echo "To start the API (separate terminal):"
echo ""
echo "  source venv/bin/activate"
echo "  python3 run_api.py"
echo ""
