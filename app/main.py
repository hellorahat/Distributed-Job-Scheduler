import logging
import threading
import time

from app.scheduler import scheduler_tick
from app.worker import worker_loop
from app.redis_client import get_redis
from app.transition import schedule_job
from app.model import SchedulerConfig, SchedulerContext

import structlog

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

ctx = SchedulerContext(
    redis=get_redis(),
    config=SchedulerConfig()
)


def run_scheduler(ctx: SchedulerContext):
    while True:
        scheduler_tick(ctx)
        time.sleep(0.2)


def run_worker():
    worker_loop(ctx, worker_id="worker-1")


if __name__ == "__main__":
    t1 = threading.Thread(target=run_scheduler, args=(ctx,), daemon=True)
    t2 = threading.Thread(target=run_worker, daemon=True)

    t1.start()
    t2.start()

    time.sleep(1)

    schedule_job(
        ctx,
        job_id="job-1",
        task="task.echo",
        payload={"msg": "hello world"},
    )

    while True:
        time.sleep(1)
