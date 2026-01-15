from dataclasses import dataclass
from enum import Enum
import json
from typing import Dict, Optional, Any, TYPE_CHECKING

from pydantic import BaseModel, Field, ConfigDict, field_serializer

if TYPE_CHECKING:
    from redis import Redis


class JobState(str, Enum):
    scheduled = "scheduled"
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"
    canceled = "canceled"
    dlq = "dlq"


class JobCreateRequest(BaseModel):
    task: str
    payload: Dict[str, Any] = Field(default_factory=dict)

    run_at_ms: Optional[int] = None


class JobCreateResponse(BaseModel):
    job_id: str
    state: JobState


class JobStatusResponse(BaseModel):
    id: str
    state: JobState

    task: str
    payload: Dict[str, Any]

    attempts: int
    max_retries: int

    run_at_ms: Optional[int] = None
    created_at_ms: int
    updated_at_ms: int

    lease_owner: Optional[str] = None
    lease_expires_at_ms: Optional[int] = None


class JobCancelResponse(BaseModel):
    job_id: str

    # Whether cancel request was accepted
    accepted: bool

    # State after attempting cancellation
    state: JobState

    message: Optional[str] = None


class JobRecord(BaseModel):
    """
    Immutable snapshot of job:{id} from Redis.
    """

    model_config = ConfigDict(frozen=True)

    id: str
    state: JobState

    task: str
    payload: Dict[str, Any]

    last_error: Optional[str] = None
    attempts: int
    max_retries: int
    backoff_base_ms: int

    run_at_ms: Optional[int] = None
    created_at_ms: int
    updated_at_ms: int

    lease_owner: Optional[str] = None
    lease_expires_at_ms: Optional[int] = None

    @field_serializer("payload", when_used="json")
    def serialize_payload(self, payload: Dict[str, Any]) -> str:
        return json.dumps(payload)


class SchedulerConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_retries: int = 3
    backoff_base_ms: int = 500
    lease_duration_ms: int = 30_000


@dataclass(frozen=True)
class SchedulerContext():
    redis: 'Redis'
    config: SchedulerConfig
