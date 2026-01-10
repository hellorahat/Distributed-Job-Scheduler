class RedisKeys:
    JOB = 'job:{id}'
    JOBS_SCHEDULED = 'jobs:scheduled'
    JOBS_READY = 'jobs:ready'
    JOBS_LEASE = 'jobs:lease'
    JOBS_DLQ = 'jobs:dlq'
