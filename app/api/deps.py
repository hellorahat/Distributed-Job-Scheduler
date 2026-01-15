from app.redis_client import get_redis
from app.model import SchedulerConfig, SchedulerContext


def get_scheduler_context() -> SchedulerContext:
    return SchedulerContext(
        redis=get_redis(),
        config=SchedulerConfig(),
    )
