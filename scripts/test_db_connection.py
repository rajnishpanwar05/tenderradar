#!/usr/bin/env python3
"""
Standalone DB connectivity check for local TenderRadar development.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import get_db_config, get_db_config_errors  # noqa: E402
from database.db import DatabasePreflightError, preflight_db_connection  # noqa: E402


def main() -> int:
    cfg = get_db_config()
    env_file = cfg.get("env_file") or "<not found>"

    print("TenderRadar DB connection test")
    print(f"  env file : {env_file}")
    print(f"  host     : {cfg['host']}")
    print(f"  port     : {cfg['port']}")
    print(f"  database : {cfg['database']}")
    print(f"  user     : {cfg['user']}")

    cfg_errors = get_db_config_errors()
    if cfg_errors:
        print(
            "ERROR: Database config is incomplete or invalid. "
            "Set DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME in .env "
            f"(missing/invalid: {', '.join(cfg_errors)})."
        )
        return 1

    try:
        result = preflight_db_connection(debug=True)
    except DatabasePreflightError as exc:
        print(f"ERROR: {exc}")
        if exc.debug_detail:
            print(f"DETAIL: {exc.debug_detail}")
        return 1
    except Exception as exc:
        print(f"ERROR: Unexpected DB test failure: {exc}")
        return 1

    if result["database_exists"]:
        print(
            f"SUCCESS: MySQL is reachable at {result['host']}:{result['port']} "
            f"and database '{result['database']}' is ready."
        )
    else:
        print(
            f"SUCCESS: MySQL is reachable at {result['host']}:{result['port']}. "
            f"Database '{result['database']}' does not exist yet, but TenderRadar can create it "
            "during startup if the configured user has permission."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
