# Completion notifications and synchronous admission

`/predict` and `/text/tasks` now await terminal task notifications instead of repeatedly polling task state. SQL-backed `/jobs` remains asynchronous and is excluded from synchronous admission and disconnect cancellation.

## Completion across threads

`Task.wait_finished()` creates an asyncio future owned by the caller's event loop. Registration and the terminal-state check share the existing task lock, so completion before or during registration cannot lose the notification. `mark_done()` and `mark_failed()` publish a terminal state once, detach waiters under the lock, then call `loop.call_soon_threadsafe()` outside the lock. Cancelled or timed-out futures are unregistered, and late worker results cannot overwrite a terminal task.

The HTTP waiter races task completion against an ASGI disconnect message. Its timeout is the task's remaining original budget; it does not restart the deadline. A socket close or handler cancellation marks the ephemeral task cancelled and purges waiting entries. No background thread blocks the asyncio loop waiting on a threading.Event.

## Admission and cleanup

`MAX_SYNC_REQUESTS` defaults to 1024 per API process, shared by `/predict` and `/text/tasks`. Excess requests receive HTTP 503 and `Retry-After: 1` before body parsing. Slots are released in a finally block, including validation errors and cancellation. Health, metrics, deployment operations, and durable jobs do not consume these slots.

This is a handler limit, not a TCP connection or ingress body-size limit. Multiple API processes each own a limit. Set the limit to measured memory and latency budgets; a larger value does not increase model throughput.

Cancelled or expired tasks are removed by identity from the central task queue, scheduler pending/retry lists, every latency-aware strategy buffer, and local/gRPC proxy waiting batches. Buffer cleanup is synchronized with batch extraction. A task moving between owners during cleanup remains terminal and is skipped at admission/execution. Local workers also skip terminal or expired batches before simulated computation.

An active RPC is not closed just because one member of its batch is cancelled. It may serve other requests. Computation already running can therefore continue; its late result is ignored. This change does not promise forceful cancellation of GPU kernels or exactly-once execution.

## Metrics

- `inference_sync_requests_active`: admitted synchronous handlers in this process.
- `inference_sync_admission_rejections_total`: requests refused at this limit.
- `inference_sync_cancellations_total`: HTTP waiters abandoned before terminal completion.

Existing API queue, scheduler pending, worker queued-batch, and active-RPC metrics remain available.

## A/B experiment

`experiments/k6/compare_waiting.py` builds runtime images from explicit working-tree snapshots and records a SHA-256 manifest. The baseline is the pre-change working tree, including the previous backpressure changes, rather than old committed HEAD. Both variants use the same harness, cached dependency image, workers, queue size, batching, and offered arrival rates.

Three repetitions alternate A/B, B/A, A/B. Each fresh stack runs normal traffic at 10 RPS, overload at 50 RPS, and recovery at 10 RPS. Two simulated workers execute one task per batch, with a nominal 72 ms simulated execution time. k6 preallocates 512 clients; dropped iterations invalidate a stage. A separate candidate-only run uses a limit of 32 to verify controlled refusal. It must not be presented as an equal-load latency improvement.

API CPU is read from `/proc/1/stat`; resident memory, thread and file-descriptor counts are sampled from `/proc`. CPU seconds cover the complete stage resource window, including idle checks, draining, and telemetry. Figures normalize by completed requests and retain window duration. Memory peak is the maximum sample, not an exact high-water measurement. Per-stage reports and samples are retained.

The real-socket check closes 80 connections after admission. Passing requires both queue cleanup and a cancellation-counter increase, so ordinary task completion alone cannot pass. Deterministic regression tests additionally exercise deadline expiry, cancelled ASGI handlers, background-thread completion races, and all waiting queue types.

The generator and containers share a local machine. This short A/B experiment measures scheduling overhead and bounded admission; it is not a production hardware benchmark, real-model test, or long-duration stability certification.

## Reproduction

Keep the baseline snapshot before applying changes. Use a local dependency image built with this repository's production requirements; the script records immutable image IDs.

```bash
python3 experiments/k6/compare_waiting.py \
  --baseline-source /path/to/pre-change-snapshot \
  --candidate-source /path/to/candidate \
  --runtime-image local-inference-runtime:dependencies \
  --output /tmp/waiting-comparison \
  --repeats 3 --duration 15 --execute

python3 experiments/k6/plot_waiting.py /tmp/waiting-comparison
```

Plot generation requires matplotlib (and its numpy dependency). Images are exported as standalone PNG and PDF files; comparison.csv retains individual measurements. No production URL or credentials are accepted by this runner.
