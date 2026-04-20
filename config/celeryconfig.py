import os

broker_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
result_backend = broker_url
task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]
timezone = "UTC"
enable_utc = True
worker_prefetch_multiplier = 1
broker_transport_options = {"visibility_timeout": 3600}
task_acks_late = True
task_default_queue = "tenderradar"

