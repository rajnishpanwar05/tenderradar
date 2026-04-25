"""Celery worker entrypoint with a minimal health HTTP server on port 8001."""
import threading
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status":"ok","service":"celery-worker"}')
    def log_message(self, *args):
        pass  # suppress request logs

def start_health_server():
    server = HTTPServer(("0.0.0.0", 8001), HealthHandler)
    server.serve_forever()

if __name__ == "__main__":
    # Start health server in background thread
    t = threading.Thread(target=start_health_server, daemon=True)
    t.start()

    # Start Celery worker in foreground
    from celery.__main__ import main
    sys.argv = ["celery", "-A", "core.tasks", "worker",
                "--loglevel=info", "-E", "--concurrency=2"]
    main()
