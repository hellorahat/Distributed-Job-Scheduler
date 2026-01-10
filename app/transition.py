"""
State transition rules:

- job:{id} is the source of truth
- State transitions must be guarded by expected current state
- Job state is authoritative
- Transitions must be atomic
- At-least-once execution is expected
"""

from typing import Dict, Optional, Any, TYPE_CHECKING, cast

from model import JobState, JobRecord
from utils.time import now_ms

if TYPE_CHECKING:
    from redis import Redis
    from redis.client import Pipeline
    from storage.redis_keys import RedisKeys

ALLOWED_TRANSITIONS = {
    JobState.scheduled: {JobState.queued, JobState.canceled},
    JobState.queued: {JobState.running, JobState.canceled},
    JobState.running: {JobState.completed, JobState.failed},
    JobState.completed: set(),
    JobState.failed: set(),
    JobState.canceled: set(),
}
MAX_RETRIES = 5
BACKOFF_BASE_MS = 500
LEASE_DURATION_MS = 30000


def _get_job(redis: 'Redis', job_id: str) -> dict | None:
    data = redis.hgetall(f'job:{job_id}')
    if not data:
        return None

    data = cast(dict[str, str], data)
    data['state'] = JobState(data['state'])
    return data


def _assert_transition(cur_state: JobState, new_state: JobState):
    allowed = ALLOWED_TRANSITIONS.get(cur_state, set())
    if new_state not in allowed:
        raise ValueError(
            f'Illegal transition: {cur_state} -> {new_state}'
        )


def _clean_terminal_job(pipe: 'Pipeline', job_id: str) -> None:
    # remove execution-related metadata
    pipe.hdel(
        RedisKeys.JOB.format(id=job_id),
        'lease_expires_at_ms',
        'lease_owner'
    )

    # remove from all active indexes
    pipe.zrem(RedisKeys.JOBS_LEASE, job_id)
    pipe.srem(RedisKeys.JOBS_READY, job_id)
    pipe.zrem(RedisKeys.JOBS_SCHEDULED, job_id)


def schedule_job(redis: 'Redis',
                 job_id: str,
                 task: str,
                 payload: Dict[str, Any],
                 run_at_ms: Optional[int] = None
                 ) -> None:
    # Job creation is a special case: no prior state exists

    now = now_ms()
    run_at_ms = run_at_ms if run_at_ms is not None else now

    record = JobRecord(
        id=job_id,
        state=JobState.scheduled,
        task=task,
        payload=payload,

        run_at_ms=run_at_ms,
        created_at_ms=now,
        updated_at_ms=now,

        attempts=0,
        max_retries=MAX_RETRIES,
        backoff_base_ms=BACKOFF_BASE_MS,

        lease_owner=None,
        lease_expires_at_ms=None,
    )

    pipe = redis.pipeline()
    pipe.hset(RedisKeys.JOB.format(id=job_id),
              mapping=record.model_dump(mode="json"))
    pipe.zadd(RedisKeys.JOBS_SCHEDULED, {job_id: run_at_ms})
    pipe.execute()


def enqueue_job(redis: 'Redis', job_id: str) -> None:
    data = _get_job(redis, job_id)
    if not data:
        return

    _assert_transition(data['state'], JobState.queued)

    # scheduled -> queued
    pipe = redis.pipeline()
    pipe.hset(
        RedisKeys.JOB.format(id=job_id),
        mapping={
            'state': JobState.queued,
            'updated_at_ms': now_ms(),
        }
    )
    pipe.zrem(RedisKeys.JOBS_SCHEDULED, job_id)
    pipe.sadd(RedisKeys.JOBS_READY, job_id)
    pipe.execute()


def lease_job(redis: 'Redis', job_id: str, worker_id: str) -> None:
    data = _get_job(redis, job_id)
    if not data:
        return

    _assert_transition(data['state'], JobState.running)
    lease_expires_at_ms = now_ms() + LEASE_DURATION_MS

    # queued -> running
    pipe = redis.pipeline()
    pipe.hset(
        RedisKeys.JOB.format(id=job_id),
        mapping={
            'state': JobState.running,
            'updated_at_ms': now_ms(),
            'lease_expires_at_ms': lease_expires_at_ms,
            'lease_owner': worker_id,
        }
    )
    pipe.srem(RedisKeys.JOBS_READY, job_id)
    pipe.zadd(RedisKeys.JOBS_LEASE, {job_id: lease_expires_at_ms})
    pipe.execute()


def complete_job(redis: 'Redis', job_id: str) -> None:
    data = _get_job(redis, job_id)
    if not data:
        return

    _assert_transition(data['state'], JobState.completed)

    pipe = redis.pipeline()
    pipe.hset(
        RedisKeys.JOB.format(id=job_id),
        mapping={
            'state': JobState.completed,
            'updated_at_ms': now_ms(),
        }
    )
    _clean_terminal_job(pipe, job_id)
    pipe.execute()


def fail_job(redis: 'Redis', job_id: str, error: str) -> None:
    data = _get_job(redis, job_id)
    if not data:
        return

    _assert_transition(data['state'], JobState.failed)

    pipe = redis.pipeline()
    pipe.hset(
        RedisKeys.JOB.format(id=job_id),
        mapping={
            'state': JobState.failed,
            'updated_at_ms': now_ms(),
            'last_error': error
        }
    )
    _clean_terminal_job(pipe, job_id)
    pipe.execute()


def cancel_job(redis: 'Redis', job_id: str) -> None:
    data = _get_job(redis, job_id)
    if not data:
        return

    _assert_transition(data['state'], JobState.canceled)

    pipe = redis.pipeline()

    pipe.hset(
        RedisKeys.JOB.format(id=job_id),
        mapping={
            'state': JobState.canceled,
            'updated_at_ms': now_ms(),
        })
    _clean_terminal_job(pipe, job_id)
    pipe.execute()
