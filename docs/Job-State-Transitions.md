#### States
- scheduled
- queued
- running
- completed
- failed
- cancelled

#### Allowed transitions
- scheduled → queued
	- Trigger: scheduler loop
- queued → running
	- Trigger: worker lease acquisition
- running → completed
	- Trigger: worker success
- running → failed
	- Trigger: retry logic
- scheduled | queue → canceled
	- Trigger: API cancel

Should running → canceled be allowed?

#### Invariants
1. job:{id} is authoritative
	1. Every transition re-checks the job hash
2. State transitions are monotonic
	1. Only transition if the current state matches the expected prior state
3. A job may appear in multiple Redis structures temporarily
	1. Use validation for correctness, not cleanup
4. At-least-once is explicit
	1. Duplicate execution is allowed

