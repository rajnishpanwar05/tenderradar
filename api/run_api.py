#!/usr/bin/env python3
# =============================================================================
# run_api.py — TenderRadar API Development Server
#
# Usage:
#   python run_api.py                    # dev (auto-reload, port 8000)
#   python run_api.py --port 9000        # custom port
#   python run_api.py --no-reload        # disable auto-reload
#   python run_api.py --workers 4        # multi-process (prod mode, disables reload)
#
# Production deployment:
#   gunicorn api.app:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
# =============================================================================

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(description="TenderRadar REST API server")
    parser.add_argument("--host",      default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port",      default=8000,       type=int,  help="Port (default: 8000)")
    parser.add_argument("--workers",   default=1,          type=int,  help="Worker processes (default: 1; disables reload)")
    parser.add_argument("--no-reload", action="store_true",           help="Disable auto-reload in dev mode")
    parser.add_argument("--log-level", default="info",
                        choices=["debug", "info", "warning", "error"],
                        help="Uvicorn log level (default: info)")
    args = parser.parse_args()

    try:
        import uvicorn
    except ImportError:
        print("ERROR: uvicorn is not installed.")
        print("Install it with:  pip install uvicorn[standard]")
        sys.exit(1)

    # Auto-reload only makes sense in single-worker dev mode
    reload = not args.no_reload and args.workers == 1

    print(f"")
    print(f"  ┌─────────────────────────────────────────────────────┐")
    print(f"  │         TenderRadar API  v1.0.0                     │")
    print(f"  │  Docs:  http://{args.host}:{args.port}/docs          │")
    print(f"  │  ReDoc: http://{args.host}:{args.port}/redoc         │")
    print(f"  │  Health:http://{args.host}:{args.port}/health        │")
    print(f"  └─────────────────────────────────────────────────────┘")
    print(f"")

    uvicorn.run(
        "api.app:app",
        host       = args.host,
        port       = args.port,
        reload     = reload,
        workers    = args.workers if not reload else None,
        log_level  = args.log_level,
        access_log = True,
    )


if __name__ == "__main__":
    main()
