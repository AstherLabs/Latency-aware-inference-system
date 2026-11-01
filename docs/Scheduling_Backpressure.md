# Scheduling, backpressure, and worker recovery

The API admits requests into a bounded queue. A busy worker is not an admission failure: the scheduler retains accepted work until compatible capacity becomes available or the original deadline expires.

## Dispatch and ordering

- Idle compatible workers are preferred, followed by busy compatible workers with a free outstanding-batch slot. Selection within the preferred group uses round robin.
- Normal waiting tasks retain FIFO order within each task kind. A blocked model kind does not prevent another kind from using its compatible workers. This is not global FIFO or tenant-level fair scheduling.
- Accepted retries receive scheduling priority. They are retained separately from new admissions, so a full admission queue cannot discard a failed attempt.
- The admission queue remains bounded. Scheduler head batches and the bounded latency-aware buffer are additional in-memory work; retry retention contains already accepted work only.
- Unsupported task kinds fail validation instead of creating unbounded scheduler head batches.

Worker capacity counts batches, not tasks, model threads, GPU executions, or user requests. With a batch size of four, three outstanding batches can contain up to twelve tasks. Increasing this limit increases prefetch and queueing, not execution throughput.

## Configuration

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `WORKER_MAX_OUTSTANDING_BATCHES` | `3` | Per proxy: one active RPC plus up to two queued batches. Read when a proxy is created. |
| `MAX_TASK_RETRIES` | `3` | Maximum additional simulation attempts accepted after initial dispatch. Waiting does not consume an attempt. |
| `WORKER_RECOVERY_BACKOFF_SECONDS` | `1` | Initial transport quarantine interval; failed probes back off up to 30 seconds. |

The existing global queue size and task deadlines still apply. Queue exhaustion can return HTTP 503; deadline exhaustion returns HTTP 504. Unbounded queues and unlimited retries are intentionally avoided. The benchmark can vary capacity independently using `--worker-capacity`.

## Failure path

1. A recoverable gRPC transport failure quarantines that proxy and detaches its queued batches.
2. Simulation tasks return to the central scheduler with the same task ID and deadline. Worker removal can also hand off queued and active tasks.
3. The retry counter advances atomically with acceptance into a worker's bounded queue, rather than when waiting begins.
4. A recovered gRPC channel makes the proxy eligible again. This tests transport connectivity; it does not certify model warmup or application health.
5. Exhausted deadlines, exhausted retry budgets, non-retryable errors, and platform shutdown can still terminate tasks. This mechanism does not promise every request succeeds during arbitrary outages.

Real SynthX audio/text tasks preserve the existing SQL-backed job recovery path: the attempt reports `transport_error`, and the durable gateway defers/reclaims the SQL job with its attempt/lease fencing. It is not directly resubmitted as a second unfenced model execution. SQL remains the job authority; the in-memory simulation retry limit is not a replacement for SQL job policy.

The API creates `Scheduler(queue, pool.submit)`; scheduler binding selects the pool's deferred-admission interface. Direct legacy `pool.submit()` callers retain fail-fast semantics and do not acquire a central backlog automatically.

## Synchronization and visibility

Queue extraction, scheduler backlog ownership, and proxy capacity each have their own lock. No network call runs under the scheduler backlog lock. Worker RPC execution runs outside its queue lock. Capacity and discovery events wake the scheduler; a 50 ms fallback also checks deadlines.

`inference_scheduler_pending_tasks` exposes scheduler waiting work, including retry handoff and the latency-aware buffer. Existing worker queue/active RPC gauges remain available. Worker routing state now includes `UNAVAILABLE` alongside lifecycle states. This gauge is in-memory visibility, not a durable job count.

## Verification

`tests/test_backpressure.py` covers busy-worker waiting, per-kind bypass, original deadlines, retry ownership during immediate repeated failures, full-queue handoff, temporary loss of all workers, invalid kinds, and transport recovery. Existing SQL tests exercise durable lease recovery independently.

Load-test results distinguish success from latency: a queue can absorb a burst with no failed requests while P95 exceeds its SLO. Consult the recorded arrival rate, generator drops, queue depth, completion throughput, and P95 together. A short simulation run does not establish real-model production capacity.
