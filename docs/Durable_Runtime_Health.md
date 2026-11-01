# Durable job timeouts and runtime health

The business backend creates an immutable SQL job before calling `POST /jobs` with its ID. The inference gateway claims that row and creates one in-memory attempt. SQL remains authoritative across gateway restarts.

## Time budgets

| Setting | Current value | Meaning |
| --- | --- | --- |
| SQL `deadline_at` | Set by the business backend | Absolute total job deadline, including waiting and retries |
| Lease duration | 120 seconds | Maximum ownership interval, capped at the total deadline |
| Lease renewal interval | 30 seconds | Gateway renews while responsible for the attempt |
| Gateway polling interval | 0.5 seconds | Inspect results, renew leases, and claim due jobs |
| SQL retry delay | 5 seconds | Earliest next claim after a retryable failure |
| `SYNTHX_JOBS_MAX_INFLIGHT` | 1 | Per-gateway claimed attempts, including queued attempts |
| `SYNTHX_JOBS_RPC_TIMEOUT_SECONDS` | 300 seconds | Maximum duration of one audio inference RPC after dispatch |
| `LOOP_HEALTH_TIMEOUT_SECONDS` | 10 seconds | Maximum age of a successful scheduler/gateway cycle |
| Database statement timeout | 5 seconds | Database-side statement bound per connection |
| Database lock timeout | 1 second | Database-side lock wait bound per connection |

The RPC limit is configurable at startup and must be tuned using measured audio processing times. It does not change the job deadline. RPC duration includes network time and any remote waiting; it is not a measurement of model execution alone. For a shared batch, the RPC obeys the strictest configured attempt cap while each task retains its own total deadline. The simulator `/predict` keeps its existing 30-second total budget.

A lease is ownership, not proof of model progress. A queued attempt may be renewed while a healthy scheduler waits for worker capacity. Jobs still waiting in SQL without a claim do not need renewal.

## Recovery behavior

- `rpc_timeout`: retryable through SQL. A new attempt receives a new `attempt_id`; the original total deadline remains unchanged.
- `deadline_exceeded`: terminal. No renewal or retry extends the total budget.
- Transport failure: existing SQL retry policy applies; no independent direct worker handoff is introduced for durable audio jobs.
- Gateway exits or stops renewing: another healthy gateway can claim the row after lease expiry.
- Scheduler stops progressing: the gateway stops claiming and renewing. It invalidates local attempts and leaves their SQL leases intact. They become recoverable after expiry, not immediately.
- Scheduler stalls during a SQL claim: that claimed row is not placed into the local queue; its lease is left to expire.
- SQL temporarily fails: the gateway preserves local state and retries its poll. SQL completion and renewal still require the current attempt, a live lease, and an unexpired deadline.

Ownership is fenced at the SQL result boundary. A timed-out RPC does not prove the old model process stopped. Physical computation can overlap during recovery; worker-side job identity, immutable specs, and artifact handling remain necessary. The platform does not claim exactly-once execution or guarantee that a retry selects a different worker when no alternative exists.

## Loop health

The scheduler records completed dispatch cycles. An empty queue and normal capacity deferral are healthy. Repeated dispatch exceptions do not refresh progress. The gateway records successful polls; a thread blocked in SQL or on its work lock becomes unhealthy even if it is still alive.

`GET /health/runtime` returns loop age, timeout, and health, with HTTP 503 if a required loop is missing, stopped, or stale. This endpoint does not acquire scheduler/gateway work locks or query SQL. `/status` includes the same runtime snapshot. `/readyz` also rejects stale runtime loops before its existing worker readiness check.

Prometheus exports:

- `inference_runtime_loop_healthy{loop="scheduler"|"gateway"}`
- `inference_runtime_loop_last_cycle_age_seconds{loop="scheduler"|"gateway"}`

Prometheus collection runs outside the API event loop, allows one collector at a time, and returns HTTP 503 after a 0.5-second wait. A timed-out collector retains its slot until it actually finishes, preventing repeated scrapes from accumulating blocked collector threads. Use `/health/runtime` as the independent loop probe if collection is blocked. Existing `/status` and worker membership checks may still acquire their own work locks.

A healthy loop does not certify model progress or an acceptable queue delay. This round does not add remote stage/progress events, a queue-wait SLO detector, or a supervisor that restarts stuck processes. The new probe supports external monitoring/restart policy; deployment wiring must be configured separately. If the only scheduler is stuck, persisted jobs need that instance restarted or another healthy gateway before they can complete.

## Verification

Regression tests cover idle versus blocked loops, repeated dispatch errors, waiting under capacity pressure, stalled scheduler ownership, a stall during SQL claim, nonblocking health probing, bounded scrapes, RPC cap versus total deadline, and database statement cancellation.

A local integration test uses real PostgreSQL and two gRPC servers: the first attempt exceeds its RPC cap; SQL defers and reclaims the job; the second worker completes; the original deadline is unchanged; a stale first-attempt result is rejected. Another integration test verifies that scheduler failure cannot release an unexpired lease early.

These are local asset-free integration tests, not a Vast model performance benchmark. No remote service was restarted for this implementation round.
