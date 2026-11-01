# Current-runtime sustained-overload acceptance

The current notification-based platform is the only runtime under test. Obsolete A/B source build contexts and the dedicated old polling image were removed at the user's request; historical raw results, hashes, reports and figures remain. Production code and user files were not deleted.

## Workload

- Four simulated gRPC workers; nominal single-task batch execution is 72 ms, capacity is three outstanding batches per proxy.
- API admission queue 1000; MAX_SYNC_REQUESTS=1024; 2048 preallocated k6 clients to avoid confusing generator exhaustion with service capacity.
- Warmup, then 30-second steps at 40, 100 and 400 RPS.
- Sustained 400 RPS for 900 seconds. This deliberately exceeds execution capacity; it is a stability and backpressure test, not a claim of 400 successful inferences/second.
- Kill one worker at observed-load +300 seconds and restart at +330. Kill all four at +600 and restart at +610. Actual command times are retained.
- Return to 40 RPS for 60 seconds and check queue cleanup after real client disconnects.

## Evaluation

Load must be delivered without dropped iterations. Invalid result bodies are prohibited by the k6 mismatch threshold. Functional checks require completed fault commands, no unexpected HTTP/transport errors, available telemetry, healthy recovery, and verified disconnect cancellation.

HTTP 503 controlled refusal and HTTP 504 original-deadline expiry are counted explicitly. They must not be called successful requests or silently omitted from availability reporting. Passing stability checks does not mean the overload workload meets a latency or success-rate SLO.

Resource evaluation is separate: inspect sampled RSS, early-versus-late steady memory, RSS slope, thread and descriptor counts, and generator memory. A positive short-run memory slope is not by itself proof of a leak; a flat short run is not proof that no leak exists. Resource data includes sampling overhead.

All tests run in a unique loopback-only Docker Compose project with simulation tasks, no production database, credentials, model service, or GPU. Only this project's workers are killed. Cleanup removes its containers and network, preserving measured evidence. This is local shared-host evidence and does not certify real-model deployment stability.

Exact runtime and harness hashes, immutable image ID, k6 version, per-stage summaries, metrics, API/generator samples, and actual fault events are retained. Use stress_current.py for execution and plot_stress.py for the outcome figure and assessment.
