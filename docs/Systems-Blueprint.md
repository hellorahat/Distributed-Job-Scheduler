#### Requirements
1. FastAPI
2. Redis

#### Subsystems:
1. API Layer (FastAPI)
2. Job State machine
	1. Scheduled → Queued → Running → Completed / Failed
3. Persistence Layer (Redis)
4. Scheduler loop
	1. Internal system loop to move jobs from “scheduled” to “ready”
5. Worker pool (execution engine)
6. Retry and DLQ system
	1. After multiple failures, move job to DLQ for manual inspection.

# API Layer (FastAPI)
**Guarantees**:
1. Once the API returns success, the job **must not disappear**
	1. If the API crashes after responding, Redis must already contain enough information for the job to still run.

**Description**:
The purpose of the API is not for scheduling or executing. It will only record intent.

**Responsibilities**:
1. Validate input.
2. Assign job IDs.
3. Persist jobs immediately.

# Job State Machine
**Process**:
- Scheduled → Queued → Running → Completed / Failed


# Persistence Layer (Redis)
**Responsibilites**:
1. Database – Hold Job metadata (hash per job)
2. Queue – Ready-to-run jobs
3. Future jobs – Sorted set of future jobs

Redis will be used for atomic operations, fast leasing, simple failure recovery. This doesn’t need a relational database, but it does need coordination.

**Used Redis Structures**:
1. HASH: job metadata
2. ZSET: future jobs
3. LIST: ready queue
4. ZSET: active leases (visibility timeouts)
5. LIST: dead-letter queue

# Scheduler Loop
Internal system loop to move jobs who’s `run_at <= now` from “scheduled” to “ready”

This loop can crash and restart safely because Redis always contains the truth, and moving a job twice is prevented by state checks.

# Worker Pool (Execution Enginer)
Workers will:
1. Pull job IDs from Redis
2. Try to **lease** a job
3. Execute user code
4. Report success or failure

We will use at-lease-once delivery.
When a worker starts a job:
- It writes a lease with an expiration time
- If the worker dies, the lease expires
- Another worker can reclaim the job


# Retry and DLQ System
Retries:
- Increment attempt count
- Apply exponential backoff
- Reschedule job

Dead-letter queue
- Jobs that failed too many times get moved here.
- They typically need human inspection
