import json
from typing import Optional
from app.model import JobRecord, SchedulerContext
from app.storage.redis_keys import RedisKeys

INT_FIELDS = {
    "attempts",
    "max_retries",
    "backoff_base_ms",
    "created_at_ms",
    "updated_at_ms",
    "run_at_ms",
    "lease_expires_at_ms",
}


def get_job(ctx: SchedulerContext, job_id: str) -> Optional[JobRecord]:
    data = ctx.redis.hgetall(RedisKeys.JOB.format(id=job_id))
    if not data:
        return None

    for field in INT_FIELDS:
        if field in data and data[field] != "":
            data[field] = int(data[field])

    if "payload" in data:
        data["payload"] = json.loads(data["payload"])

    return JobRecord.model_validate(data)
