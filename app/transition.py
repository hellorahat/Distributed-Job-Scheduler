"""
State transition rules:

- job:{id} is the source of truth
- State transitions must be guarded by expected current state
- Job state is authoritative
- Transitions must be atomic
- At-least-once execution is expected
"""

from collections.abc import Callable
from typing import Dict, Optional, Any, TYPE_CHECKING, cast

from app.model import JobState, JobRecord
from app.utils.time import now_ms
from app.storage.redis_keys import RedisKeys

from redis.exceptions import WatchError

if TYPE_CHECKING:
    from redis import Redis
    from redis.client import Pipeline

STATE_MACHINE = {
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


def _assert_transition(cur_state: JobState, new_state: JobState):
    allowed = STATE_MACHINE.get(cur_state, set())
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


def _atomic_transition(
    redis: "Redis",
    job_id: str,
    expected: JobState,
    new_state: JobState,
    *,
    extra_hset: Optional[dict[str, Any]] = None,
    pre_index_ops: Optional[Callable[['Pipeline'], None]] = None,
    post_index_ops: Optional[Callable[['Pipeline'], None]] = None,
) -> None:
    key = RedisKeys.JOB.format(id=job_id)
    extra_hset = extra_hset or {}

    while True:
        pipe = redis.pipeline(transaction=True)
        try:
            pipe.watch(key)

            data = pipe.hgetall(key)
            if not data:
                pipe.unwatch()
                return

            data = cast(dict[str, str], data)
            cur = JobState(data["state"])

            _assert_transition(cur, new_state)

            if cur != expected:
                pipe.unwatch()
                return

            now = now_ms()

            pipe.multi()

            if pre_index_ops:
                pre_index_ops(pipe)

            pipe.hset(key, mapping={
                "state": new_state.value,
                "updated_at_ms": now,
                **{k: (v.value if isinstance(v, JobState) else v)
                   for k, v in extra_hset.items()},
            })

            if post_index_ops:
                post_index_ops(pipe)

            pipe.execute()
            return

        except WatchError:
            # state was modified between WATCH and EXEC, retry
            continue
        finally:
            pipe.reset()


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

    data = record.model_dump(mode="json")
    data = {k: v for k, v in data.items() if v is not None}
    pipe.hset(
        RedisKeys.JOB.format(id=job_id),
        mapping=data
    )

    pipe.zadd(RedisKeys.JOBS_SCHEDULED, {job_id: run_at_ms})

    pipe.execute()


def enqueue_job(redis: "Redis", job_id: str) -> None:
    def post(pipe: "Pipeline") -> None:
        pipe.zrem(RedisKeys.JOBS_SCHEDULED, job_id)
        pipe.sadd(RedisKeys.JOBS_READY, job_id)

    _atomic_transition(
        redis,
        job_id,
        expected=JobState.scheduled,
        new_state=JobState.queued,
        post_index_ops=post,
    )


def lease_job(redis: "Redis", job_id: str, worker_id: str) -> None:
    lease_expires_at_ms = now_ms() + LEASE_DURATION_MS

    def post(pipe: "Pipeline") -> None:
        pipe.srem(RedisKeys.JOBS_READY, job_id)
        pipe.zadd(RedisKeys.JOBS_LEASE, {job_id: lease_expires_at_ms})

    _atomic_transition(
        redis,
        job_id,
        expected=JobState.queued,
        new_state=JobState.running,
        extra_hset={
            "lease_expires_at_ms": lease_expires_at_ms,
            "lease_owner": worker_id,
        },
        post_index_ops=post,
    )


def complete_job(redis: "Redis", job_id: str) -> None:
    def post(pipe: "Pipeline") -> None:
        _clean_terminal_job(pipe, job_id)

    _atomic_transition(
        redis,
        job_id,
        expected=JobState.running,
        new_state=JobState.completed,
        post_index_ops=post,
    )


def fail_job(redis: "Redis", job_id: str, error: str) -> None:
    def post(pipe: "Pipeline") -> None:
        _clean_terminal_job(pipe, job_id)

    _atomic_transition(
        redis,
        job_id,
        expected=JobState.running,
        new_state=JobState.failed,
        extra_hset={"last_error": error},
        post_index_ops=post,
    )


def cancel_job(redis: "Redis", job_id: str) -> None:
    def post(pipe: "Pipeline") -> None:
        _clean_terminal_job(pipe, job_id)

    # try both transitions: scheduled -> canceled & queued -> canceled
    _atomic_transition(
        redis,
        job_id,
        expected=JobState.scheduled,
        new_state=JobState.canceled,
        post_index_ops=post,
    )

    _atomic_transition(
        redis,
        job_id,
        expected=JobState.queued,
        new_state=JobState.canceled,
        post_index_ops=post,
    )
