from fastapi import APIRouter, Depends, HTTPException


from app.api.deps import get_scheduler_context
from app.model import (
    JobCreateRequest,
    JobCreateResponse,
    JobStatusResponse,
    JobCancelResponse,
    SchedulerContext
)
from app.scheduler.transition import schedule_job, cancel_job
from app.scheduler.queries import get_job

router = APIRouter(prefix="/jobs")


@router.post("", response_model=JobCreateResponse)
def create_job_endpoint(
    req: JobCreateRequest,
    ctx: SchedulerContext = Depends(get_scheduler_context)
):
    try:
        job_id, state = schedule_job(
            ctx,
            task=req.task,
            payload=req.payload,
            run_at_ms=req.run_at_ms       
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return JobCreateResponse(job_id=job_id, state=state)


@router.get("/{job_id}", response_model=JobStatusResponse)
def get_job_endpoint(
    job_id: str,
    ctx: SchedulerContext = Depends(get_scheduler_context)
):
    record = get_job(ctx, job_id)
    if not record:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobStatusResponse(
        id=record.id,
        state=record.state,
        task=record.task,
        payload=record.payload,
        attempts=record.attempts,
        max_retries=record.max_retries,
        run_at_ms=record.run_at_ms,
        created_at_ms=record.created_at_ms,
        updated_at_ms=record.updated_at_ms,
        lease_owner=record.lease_owner,
        lease_expires_at_ms=record.lease_expires_at_ms
    )


@router.post("/{job_id}/cancel", response_model=JobCancelResponse)
def cancel_job_endpoint(
    job_id: str,
    ctx: SchedulerContext = Depends(get_scheduler_context),
):
    result = cancel_job(ctx, job_id)

    if not result:
        raise HTTPException(status_code=404, detail="Job not found")

    return result
