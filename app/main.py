import logging
import structlog
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.jobs import router as jobs_router
from app.redis_client import get_redis
from app.model import SchedulerConfig, SchedulerContext
from app.runtime import start_scheduler, start_worker

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    ctx = SchedulerContext(
        redis=get_redis(),
        config=SchedulerConfig(),
    )

    start_scheduler(ctx)
    start_worker(ctx, worker_id="worker-1")

    yield

app = FastAPI(
    title="Distributed Job Scheduler",
    lifespan=lifespan,
)

app.include_router(jobs_router)
