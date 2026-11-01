# Durable job timeout and runtime health implementation plan

User approved implementation before a detail-by-detail review; no production rollout or push.

## Design
Keep SQL deadline_at as the immutable total budget and lease_until as ownership. Add a per-attempt RPC cap (default 300 seconds for durable audio jobs), distinguish rpc_timeout from total deadline exhaustion, and retry only through SQL. Cap RPC by remaining total budget. No remote RUNNING event is fabricated: PROCESSING means client-side dispatch, not confirmed remote execution.

Track completed scheduler and gateway cycles with monotonic heartbeats (default 10 seconds maximum age, configurable). Expose independent runtime health/readiness and Prometheus age/healthy gauges. Idle and busy deferral count as healthy completed cycles. A stale scheduler stops gateway claims and renewals; abandon local attempts as terminal lease_lost and leave SQL leases to expire before recovery. Do not restart threads in place or reset deadlines. Bound database statement/lock waits. Health probing must not take scheduler/gateway work locks.

## Tasks
1. Write failing tests for loop progress, idle/stalled scheduler, readiness, stale scheduler claim/renew gating, RPC cap/error classification and SQL timeout/fencing. Run RED.
2. Implement health heartbeats, gateway fail-closed ownership, durable RPC cap, bounded SQL waits and API/metrics configuration. Run focused tests GREEN.
3. Run full suite against disposable PostgreSQL; review diff, document defaults and limitations; copy only changed files back to repository, verify copied code. No live Vast fault injection in this code-only round.

## Review focus
- A live but blocked thread must be unhealthy; an idle thread must stay healthy.
- Stalled gateway must not allow completed late results to bypass SQL fencing.
- Stale scheduler must not immediately release SQL ownership to overlapping attempts.
- RPC timeout differs from overall expiry and never resets the original deadline.
- Health endpoints cannot block on work/SQL locks; missing worker capacity is not a dead loop.
