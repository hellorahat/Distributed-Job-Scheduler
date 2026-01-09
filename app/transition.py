"""
State transition rules:

- job:{id} is the source of truth
- State transitions must be guarded by expected current state
- Job state is authoritative
- Transitions must be atomic
- At-least-once execution is expected
"""

from typing import Dict, Any, TYPE_CHECKING, cast

from model import JobState
from redis_client import get_redis

if TYPE_CHECKING:
    from redis.client import Redis

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
LEASE_EXPIRE_MS = 30000


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


def schedule_job(job_id: str,
                 task: str,
                 payload: Dict[str, Any],
                 run_at: str
                 ):
    pass



def enqueue_job(job_id):
    pass


def lease_job(job_id):
    pass


def complete_job(job_id):
    pass


def fail_job(job_id, error):
    pass


def cancel_job(job_id):
    pass
