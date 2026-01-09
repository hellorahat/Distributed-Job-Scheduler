All keys:
- job:{id} – metadata
- jobs:scheduled – future jobs
- jobs:ready – jobs ready for execution
- jobs:lease – jobs currently owned by a worker
- jobs:dlq – jobs that have failed above a set threshold; manual review list

**job:{id} is the single source of truth, always verify state before carrying out state transition**

## Job Metadata
HASH: (FIELD: job:{id}, mapping: {
- id=”3tg0gh0ghg024bn2gl2k42ln”
- state=”scheduled” | “queued” | “running” | “completed” | “failed” | “canceled”
- task=”report.generate”
- payload=”{…}”

- run_at_ms=”1”
- created_at_ms=”1”
- updated_at_ms=”1”
	
- attempts=”1”
- max_retries=”5”
- backoff_base_ms=”500”

- lease_owner=”worker-3” (empty unless state == “running“)
- lease_expires_at_ms=”1

- last_error=””
})

#### Future Jobs
SORTED SET: (jobs:scheduled, {job1: time, job2: time})

#### Ready Jobs
LIST: (jobs:ready)

#### Active Leases
SORTED SET: (jobs:lease, {job1:expiretime, job2:expiretime})

#### Dead Letter Queue
List: (jobs:dlq)

