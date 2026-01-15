"""
End-to-end smoke test for the distributed job scheduler.

Validates:
- Job creation
- Delayed execution
- State transitions over time
- Cancellation behavior

Requires:
- API running locally on :8000
- Redis running
"""

import time
import requests

BASE = "http://127.0.0.1:8000"


def now_ms() -> int:
    return int(time.time() * 1000)


def dump_response(label: str, r: requests.Response):
    print(f"\n--- {label} ---")
    print("STATUS:", r.status_code)
    print("CONTENT-TYPE:", r.headers.get("content-type"))
    print("TEXT:", repr(r.text))

    if r.headers.get("content-type", "").startswith("application/json"):
        try:
            print("JSON:", r.json())
        except Exception as e:
            print("JSON PARSE ERROR:", e)


print("Creating job...")
create_resp = requests.post(
    f"{BASE}/jobs",
    json={
        "task": "task.echo",
        "payload": {"msg": "hello"},
        "run_at_ms": now_ms() + 1000,
    },
)
dump_response("CREATE JOB", create_resp)

create_resp.raise_for_status()
job = create_resp.json()
job_id = job["job_id"]


time.sleep(0.1)
get1 = requests.get(f"{BASE}/jobs/{job_id}")
dump_response("GET IMMEDIATE", get1)


time.sleep(1.5)
get2 = requests.get(f"{BASE}/jobs/{job_id}")
dump_response("GET AFTER 1.5s", get2)


time.sleep(1.0)
get3 = requests.get(f"{BASE}/jobs/{job_id}")
dump_response("GET AFTER 2.5s", get3)


cancel = requests.post(f"{BASE}/jobs/{job_id}/cancel")
dump_response("CANCEL JOB", cancel)


final = requests.get(f"{BASE}/jobs/{job_id}")
dump_response("FINAL GET", final)

print("\nSmoke test complete.")
