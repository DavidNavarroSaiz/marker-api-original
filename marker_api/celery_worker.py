import os
from celery import Celery
from dotenv import load_dotenv
import multiprocessing

multiprocessing.set_start_method("fork", force=True)

load_dotenv(".env")

celery_app = Celery(
    "celery_app",
    broker=os.environ.get("REDIS_HOST", "redis://localhost:6379/0"),
    backend=os.environ.get("REDIS_HOST", "redis://localhost:6379/0"),
    include=["marker_api.celery_tasks"],
    broker_heartbeat=900,  # Increase heartbeat interval
    broker_connection_retry_on_startup=True,
    broker_connection_timeout=900,  # Increase connection timeout
    task_acks_late=True,  # Ensure tasks are acknowledged after completion
    
)

# # Adjust heartbeat and timeout settings
celery_app.conf.worker_heartbeat_interval = 900
celery_app.conf.worker_prefetch_multiplier = 1
celery_app.conf.result_expires = 900  # 1 hour
celery_app.conf.worker_max_tasks_per_child = 3
# Timeout settings
celery_app.conf.task_time_limit = 900  # 2 hours
celery_app.conf.task_soft_time_limit = 900  # Graceful exit before hard kill

@celery_app.task(name="celery.ping")
def ping():
    try:
        print("Ping task received!")
        return "pong"
    finally:
        import gc
        gc.collect()  # Force garbage collection to clear semaphores