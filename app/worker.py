import time
import json
from typing import Dict

from app.scheduler.transition import lease_job, complete_job, fail_job
from app.storage.redis_keys import RedisKeys
from app.task_registry import TASKS
from app.model import SchedulerContext

import app.tasks  # noqa: F401


def worker_loop(ctx: SchedulerContext,
                worker_id: str,
                poll_interval: float = 0.2) -> None:
    redis = ctx.redis
    while True:
        job_id = redis.spop(RedisKeys.JOBS_READY)

        if job_id is None:
            time.sleep(poll_interval)
            continue

        assert isinstance(job_id, str)

        try:
            lease_job(ctx, job_id, worker_id)
        except Exception:
            # Other worker got it first, or illegal transition
            redis.sadd(RedisKeys.JOBS_READY, job_id)
            continue

        job = redis.hgetall(RedisKeys.JOB.format(id=job_id))
        if not job:
            continue
        assert isinstance(job, Dict)

        task_name = job['task']
        payload = json.loads(job['payload'])

        task_fn = TASKS.get(task_name)
        if task_fn is None:
            fail_job(ctx, job_id, f"Unknown task: {task_name}")
            continue

        try:
            task_fn(payload)
        except Exception as e:
            fail_job(ctx, job_id, str(e))
            continue

        complete_job(ctx, job_id)
