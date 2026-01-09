from enum import Enum
from typing import Dict, Optional, Any

from pydantic import BaseModel, Field


class JobCreateRequest(BaseModel):
    task: str
    payload: Dict[str, Any] = Field(default_factory=dict)

    # Scheduling
    run_at_ms: Optional[int] = None

    # Retry Config
    max_retries: int = Field(default=5, ge=0)
    backoff_base_ms: int = Field(default=500, ge=0)


class JobCreateResponse(BaseModel):
    pass


class JobStatusResponse(BaseModel):
    pass


class JobCancelResponse(BaseModel):
    pass


class JobState(str, Enum):
    scheduled = "scheduled"
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"
    canceled = "canceled"


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
