from enum import Enum
from typing import Dict, Optional, Any

from pydantic import BaseModel, Field


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


class JobRecord(BaseModel):
    """
    Immutable snapshot of job:{id} from Redis.
    """

    id: str
    state: JobState

    task: str
    payload: Dict[str, Any]

    attempts: int
    max_retries: int
    backoff_base_ms: int

    run_at_ms: Optional[int] = None
    created_at_ms: int
    updated_at_ms: int

    lease_owner: str
    lease_expires_at_ms: int

    class Config:
        allow_mutation = False
