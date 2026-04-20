import os
from celery import Celery

broker = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
app = Celery("tenderradar", broker=broker, backend=broker)
app.config_from_object("config.celeryconfig")

# Alias used by main.py and tasks to avoid import errors
celery_app = app

__all__ = ["app", "celery_app"]
