"""
State transition rules:

- job:{id} is the source of truth
- State transitions must be guarded by expected current state
- Job state is authoritative
- Transitions must be atomic
- At-least-once execution is expected
"""

from model import JobState

ALLOWED_TRANSITIONS = {
    JobState.scheduled: {JobState.queued, JobState.canceled},
    JobState.queued: {JobState.running, JobState.canceled},
    JobState.running: {JobState.completed, JobState.failed},
    JobState.completed: set(),
    JobState.failed: set(),
    JobState.canceled: set(),
}


def _get_job(redis, job_id) -> dict | None:
    data = redis.hgetall(f'job:{job_id}')
    return data or None


def _assert_transition(cur_state: JobState, new_state: JobState):
    allowed = ALLOWED_TRANSITIONS.get(cur_state, set())
    if new_state not in allowed:
        raise ValueError(
            f'Illegal transition: {cur_state} -> {new_state}'
        )


def schedule_job(job_id):
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
