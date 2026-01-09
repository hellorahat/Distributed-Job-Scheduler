"""
State transition rules:

- job:{id} is the source of truth
- State transitions must be guarded by expected current state
- Job state is authoritative
- Transitions must be atomic
- At-least-once execution is expected
"""

from typing import Dict, Any, TYPE_CHECKING, cast

from model import JobState, JobRecord
from utils.time import now_ms

if TYPE_CHECKING:
    from redis import Redis

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


def schedule_job(redis: 'Redis',
                 job_id: str,
                 task: str,
                 payload: Dict[str, Any],
                 run_at_ms: int
                 ):

    now = now_ms()
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

        lease_owner="",
        lease_expires_at_ms=0,
    )

    redis.hset(f'job:{job_id}', mapping=record.model_dump())


def enqueue_job(redis: 'Redis', job_id: str):
    data = _get_job(redis, job_id)
    if not data:
        return
    _assert_transition(data['state'], JobState.queued)


def lease_job(redis: 'Redis', job_id: str):
    data = _get_job(redis, job_id)
    if not data:
        return
    _assert_transition(data['state'], JobState.running)


def complete_job(redis: 'Redis', job_id):
    data = _get_job(redis, job_id)
    if not data:
        return
    _assert_transition(data['state'], JobState.completed)


def fail_job(redis: 'Redis', job_id, error):
    data = _get_job(redis, job_id)
    if not data:
        return
    _assert_transition(data['state'], JobState.failed)


def cancel_job(redis: 'Redis', job_id):
    data = _get_job(redis, job_id)
    if not data:
        return
    _assert_transition(data['state'], JobState.canceled)
