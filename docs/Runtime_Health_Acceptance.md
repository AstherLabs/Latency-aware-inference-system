# Runtime health recovery acceptance results

Date: 2026-11-01 UTC. Local disposable Docker environment; no Vast model or production service was used.

## Outcome

All 13 acceptance checks passed. Four immutable SQL jobs completed: one baseline and three submitted concurrently during the injected dispatch stall. The stalled job completed on its second leased attempt; its original deadline did not change.

| Observation | Measured value |
| --- | --- |
| Runtime health detects stalled dispatch | 10.054 seconds after stall observation |
| Prometheus sustained-failure alert fires | 46.493 seconds after stall observation |
| Replacement API reports healthy | 5.987 seconds after restart request |
| All jobs complete | 120.700 seconds after stall observation |

The 120-second default ownership lease was not shortened. Recovery completion time is dominated by waiting for that lease, not slow detection. The restarted gateway completed other queued jobs while waiting to reclaim the held job.

## Ownership evidence

The original lease expired at `2026-11-01 09:07:15.902769+00:00`. The database timestamp of the replacement claim was `2026-11-01T09:07:16.002067+00:00`.

The replacement RPC was deliberately held open. While SQL still showed the replacement attempt running with a live lease, submitting the original attempt result returned false and left the row unchanged. The replacement was then released and its result persisted. This checks attempt fencing independently of terminal-state rejection.

Docker changed from healthy to unhealthy and returned to healthy after the API process restart. The Prometheus scheduler alert resolved. A restart was explicitly performed by the test harness, not by a production auto-restart controller.

## Scope

The run used real API subprocesses, HTTP job submission/query, PostgreSQL claims/renewals, the scheduler and gRPC transport. The remote endpoint was a deterministic fixture worker. This proves the tested recovery path; it does not measure audio inference latency, verify audio artifacts, or certify real Vast model recovery.

Raw report, claim trace, Docker health transitions, logs and image identity are retained in `experiments/results/health-alerts-20261101/`. Configuration, source hashes and CI evidence are saved alongside the report.
