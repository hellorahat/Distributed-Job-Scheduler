from enum import Enum
import json
from typing import Dict, Optional, Any

from pydantic import BaseModel, Field, ConfigDict, field_serializer


class JobState(str, Enum):
    scheduled = "scheduled"
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"
    canceled = "canceled"


class JobCreateRequest(BaseModel):
    task: str
    payload: Dict[str, Any] = Field(default_factory=dict)

    run_at_ms: Optional[int] = None

    max_retries: int = Field(default=5, ge=0)
    backoff_base_ms: int = Field(default=500, ge=0)


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

    lease_owner: str
    lease_expires_at_ms: int


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
