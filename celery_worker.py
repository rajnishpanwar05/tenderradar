"""Celery worker + Beat entrypoint with a minimal health HTTP server on port 8001.

Set env var CELERY_BEAT=1 to also run the Beat scheduler embedded in the worker.
This way a single Railway service handles both scheduling and execution.
"""
import os
import threading
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status":"ok","service":"celery-worker"}')

    def log_message(self, *args):
        pass


def start_health_server():
    server = HTTPServer(("0.0.0.0", 8001), HealthHandler)
    server.serve_forever()


if __name__ == "__main__":
    t = threading.Thread(target=start_health_server, daemon=True)
    t.start()

    from celery.__main__ import main

    use_beat = os.environ.get("CELERY_BEAT", "0") == "1"

    if use_beat:
        # Run worker with embedded beat scheduler (--beat flag)
        sys.argv = [
            "celery", "-A", "core.tasks", "worker",
            "--loglevel=info", "-E", "--concurrency=2",
            "--beat", "--scheduler=celery.beat.PersistentScheduler",
        ]
    else:
        sys.argv = [
            "celery", "-A", "core.tasks", "worker",
            "--loglevel=info", "-E", "--concurrency=2",
        ]

    main()
