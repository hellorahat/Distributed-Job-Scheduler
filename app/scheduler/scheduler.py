from typing import cast

from app.utils.time import now_ms
from app.storage.redis_keys import RedisKeys
from app.scheduler.transition import enqueue_job, retry_job, dlq_job
from app.model import SchedulerContext


def _decide_retry_or_dlq(ctx: SchedulerContext, job_id: str, now: int) -> None:
    redis = ctx.redis
    config = ctx.config

    attempts = redis.hget(
        RedisKeys.JOB.format(id=job_id),
        'attempts'
    )

    if attempts is None:
        return

    attempts = int(cast(str, attempts))

    if attempts < config.max_retries:
        delay = now + config.backoff_base_ms * 2 ** (attempts - 1)
        retry_job(ctx, job_id, delay)
    else:
        dlq_job(ctx, job_id)


def scheduler_tick(ctx: SchedulerContext, limit: int = 100) -> None:
    redis = ctx.redis
    now = now_ms()

    # Decide scheduled -> queued
    job_ids = redis.zrange(
        RedisKeys.JOBS_SCHEDULED,
        start=0,
        end=now,
        byscore=True,
        offset=0,
        num=limit,
    )
    assert isinstance(job_ids, list)

    for job_id in job_ids:
        enqueue_job(ctx, job_id)

    # Decide failed -> retry | failed -> DLQ
    cursor = 0
    while True:
        result = redis.sscan(
            RedisKeys.JOBS_FAILED,
            cursor=cursor,
            count=limit,
        )
        assert isinstance(result, tuple)

        cursor, job_ids = result
        for job_id in job_ids:
            _decide_retry_or_dlq(ctx, job_id, now)

        if cursor == 0:
            break
