from typing import TYPE_CHECKING

from app.utils.time import now_ms
from app.storage.redis_keys import RedisKeys
from app.transition import enqueue_job
from app.redis_client import get_redis


if TYPE_CHECKING:
    from redis import Redis


redis = get_redis()


def scheduler_tick(redis: 'Redis', limit: int = 100) -> None:
    now = now_ms()

    job_ids = redis.zrange(
        RedisKeys.JOBS_SCHEDULED,
        start=0,
        end=now,
        byscore=True,
        offset=0,
        num=limit,
    )

    for job_id in job_ids:
        enqueue_job(redis, job_id)
