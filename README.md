# Latency-Aware Inference System

An inference-serving testbed for studying batching, queueing latency, and worker failure recovery. A FastAPI gateway dispatches batches over gRPC; ZooKeeper tracks worker membership; Prometheus and Grafana expose request behavior.

The default local backend simulates inference latency and returns `x * x`; this mode does not load an ML model or use a GPU. Production SynthX adapters use separate model workers and SQL-backed jobs; see [deployment integration](docs/Inference_CICD.md).

## Architecture

```text
HTTP client
    |
    v
FastAPI -> bounded task queue -> scheduler -> gRPC worker pool
                                               |
                                               v
                                    remote worker processes
                                               |
                                       simulated inference

ZooKeeper: worker registration -> membership watch -> worker pool
Prometheus: API /metrics -> Grafana
```

The API owns task state and the request deadline. Each gRPC worker proxy has a private batch queue and a serial dispatch thread. Remote workers receive serialized inputs and remaining time budgets, then return one result per processed task.

Separate locks protect queue extraction, scheduler backlog ownership, pool membership, proxy capacity, and task terminal transitions.

## Run with Docker Compose

Requirements: Docker Engine or Docker Desktop with Compose, and Python 3 for the standard-library load generator.

From the repository root:

```bash
docker compose up -d --build
docker compose ps
```

The stack starts the API, three workers, ZooKeeper, Prometheus, and Grafana. ZooKeeper must pass its health check before the API and workers start. Rebuild after changing Python code or the protobuf schema.

Send a request:

```bash
curl -sS -w '\nHTTP %{http_code}\n' \
  -X POST http://localhost:8000/predict \
  -H 'Content-Type: application/json' \
  -d '{"x": 3}'
```

A successful response contains a task ID, a prediction of `9`, the worker ID, and end-to-end task latency in milliseconds.

| Service | Local address |
| --- | --- |
| API documentation | http://localhost:8000/docs |
| API metrics | http://localhost:8000/metrics |
| Grafana dashboard | http://localhost:3000/d/inference-overview/inference-service-overview |
| Prometheus | http://localhost:9090 |

Grafana allows anonymous viewing. Prometheus and Grafana bind to loopback; the API port binds to all host interfaces. ZooKeeper and gRPC workers are reachable on the Compose network, without published host ports. This stack is intended for local development and has no authentication or transport encryption.

Stop containers while retaining them and their volumes:

```bash
docker compose stop
```

Remove containers and the Compose network, retaining named volumes:

```bash
docker compose down
```

## Request lifecycle and failure handling

A task progresses from `pending` to `processing`, then to `done` or `failed`. Terminal updates are protected by a task lock, so a late response cannot reopen a failed task.

- **Admission:** the API queue holds up to 10,000 tasks by default. A full queue is rejected immediately. Each gRPC proxy accepts three outstanding batches by default, counting its active RPC and queued work; `WORKER_MAX_OUTSTANDING_BATCHES` changes the limit. Accepted tasks wait centrally when compatible workers are full, rather than failing immediately. Waiting tasks retain order within a kind while other kinds can use compatible capacity. Routing considers only READY proxies, preferring idle workers before trying busy workers with available capacity.
- **Deadline:** each HTTP prediction gets a 30-second budget, including queueing. Retries keep that deadline. The API uses a monotonic clock and sends a relative remaining budget; remote workers build deadlines using their own clocks. Network transit is not subtracted from the remote relative budget, so the API deadline remains authoritative.
- **RPC timeout:** a batch call uses the largest remaining task budget, or 35 seconds when no task has a deadline. The remote worker checks individual deadlines independently.
- **Cancellation:** simulated computation waits in intervals of at most 10 ms, checking task expiry and RPC activity. This is cooperative cancellation of the simulator, not cancellation of a model or GPU kernel.
- **Retry:** `UNAVAILABLE`, and `CANCELLED` caused by worker removal, return eligible simulation tasks to central scheduling. `MAX_TASK_RETRIES` defaults to three additional accepted attempts; waiting for capacity consumes no attempt and never resets the deadline. Failed transports are quarantined and probed with bounded backoff. Timeout and unrelated cancellation are not retried. Real SynthX jobs retain SQL lease-based recovery instead of unfenced direct handoff. See [scheduling and recovery](docs/Scheduling_Backpressure.md).
- **Discovery:** workers register ephemeral ZooKeeper nodes. Session expiry removes a worker from membership; detection time depends on session and connection timing. Removed proxies reject new batches and attempt handoff for eligible queued and active tasks.
- **Registration recovery:** workers restore registration after reconnecting with a new session. At startup, a conflicting node is left untouched and registration is retried in the background. ZooKeeper does not restart crashed processes; Compose currently has no restart policy.

On SIGTERM or SIGINT, a worker publishes DRAINING through its ZooKeeper registration. Discovery watches membership and node data; the pool keeps the draining proxy but stops routing new tasks to it. State changes alone do not cancel accepted work. Publication gets up to two seconds of foreground waiting within a shared 30-second shutdown budget; gRPC uses the remaining budget to finish active calls. Compose allows 35 seconds before forceful termination. Watch propagation is asynchronous, so calls racing with shutdown can still require handoff. Registration state is not a remote health probe.

Retries can repeat computation when the first worker's outcome is unknown. The service has no remote deduplication or exactly-once guarantee. Synchronous `/predict` task state and queues are in memory and are lost when the API process restarts. Durable SynthX job ownership and attempt fencing remain in SQL.

### HTTP responses

| Status | Meaning |
| --- | --- |
| `200` | Inference completed successfully |
| `422` | Request validation failed |
| `503` | API admission queue full; busy workers alone do not reject accepted predictions |
| `504` | Task deadline or RPC timeout exceeded |
| `500` | Other inference failures, including unsuccessful handoff |

`/readyz` requires an initialized pool with at least one READY proxy and no pool shutdown in progress. It excludes locally quarantined transports but is not an active model-health or free-capacity check; busy workers do not make the API unready. Readiness currently supports only gRPC mode.

A rejected or failed HTTP request is not saved for later processing. Clients must decide whether a new request is appropriate for their own deadline and retry policy.

## API and scheduling

| Method | Endpoint | Payload or purpose |
| --- | --- | --- |
| POST | `/predict` | `{"x": 3}` |
| POST | `/config/batch_size` | `{"batch_size": 8}` |
| POST | `/config/scheduler` | `{"scheduler": "batching"}` |
| POST | `/config/failure_rate` | `{"failure_rate": 0.1}` |
| GET | `/status` | Configuration and API queue counters; not a readiness probe |
| GET | `/readyz` | gRPC routing readiness: 200 with at least one READY worker; 503 before initialization, during pool shutdown, with no READY workers, or in unsupported local mode |
| GET | `/metrics` | Prometheus exposition |

| Strategy | Current behavior |
| --- | --- |
| `fifo` | Reads tasks in arrival order, up to the configured batch size; each empty-queue read can wait 50 ms |
| `batching` | Collects a batch until it fills or a 100 ms fill window expires |
| `latency_aware` | Uses a bounded local buffer and dispatches older tasks first |

For homogeneous requests, the age-based strategy can behave similarly to FIFO. It does not estimate model execution cost or guarantee an SLO. The default batch size is 4; selecting a strategy changes the next scheduler iteration.

## Monitoring

Prometheus scrapes the API every 15 seconds. The provisioned Grafana dashboard shows request rate, successful request rate, response ratios, successful P95 latency, requests since API startup, accepted/completed retry activity, discovered worker count, API queue depth, and per-worker queued batches, active inference RPCs, and routing lifecycle flags.

| Metric | Meaning |
| --- | --- |
| `predict_requests_total` | Prediction requests received by the API |
| `predict_responses_total{status_code}` | Completed HTTP responses by status |
| `predict_failures_total` | Prediction responses with errors, including unhandled exceptions |
| `predict_duration_seconds{status_code}` | Server-side handling duration histogram, excluding client network time |
| `inference_retry_submissions_total` | Retry tasks accepted by another proxy |
| `inference_retry_completions_total` | Retried tasks that completed successfully |
| `inference_registered_workers` | Workers currently discovered by the API, including DRAINING workers; not a health or idle count |
| `inference_api_queue_depth` | Tasks in the API queue; excludes scheduler buffers and worker-side work |
| `inference_worker_queued_batches{worker_id}` | Batches waiting in each API-side proxy, excluding the active batch |
| `inference_worker_active_rpcs{worker_id}` | In-flight inference calls from each API-side proxy; 0 or 1 per proxy |
| `inference_worker_routing_state{worker_id,state}` | READY and DRAINING flags; 1 for the observed state, 0 otherwise |

Retry counters are exposed in gRPC mode. The retry panels show per-second activity and estimated increases over the selected dashboard range. These counters reset when the API process restarts. A submitted retry may still be running or may later fail, so submission and completion counts are different events.

Example PromQL for retries submitted over five minutes:

```promql
sum(increase(inference_retry_submissions_total[5m]))
```

Grafana's latency quantiles are estimates from histogram buckets. Use the load generator's measured percentiles when comparing short runs, and report request errors alongside latency.

## Load and fault experiments

The paced load generator uses only the Python standard library:

```bash
python3 experiments/sustained_load_test.py --rates 10 30 60 --duration 30
```

It schedules arrivals independently of response completion and does not retry requests. Each stage prints the actual send rate, status counts, successful P50/P95/P99 latency, and skipped send slots. Skipped slots indicate generator limits or missed timing; they must not be counted as server rejections.

To exercise handoff, run a sustained stage in one terminal:

```bash
python3 experiments/sustained_load_test.py --rates 10 --duration 60
```

During the stage, use a second terminal:

```bash
docker compose kill worker-2
docker compose start worker-2
docker compose logs --since 2m api worker-2
```

Look for `Retrying task`, `Completed retried task`, and worker registration/discovery messages. If no task was active or queued when the worker stopped, this run does not prove handoff. Capacity loss can produce `503`; success under one load level does not establish a capacity guarantee.

The broader experiment runner requires the Python dependencies below. Its scheduler, batching, failure, burst, and scaling scenarios write CSVs and plots under `experiments/`. Existing result files are historical runs, not performance guarantees for the current revision. Record worker count, batch size, simulator settings, actual load, error rates, and hardware when reporting results.

## Development and tests

Use Python 3.11 or later and a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt 'grpcio>=1.80.0' 'protobuf>=6.31.1'
python -m unittest discover -s tests -v
```

Registration and discovery tests use fake ZooKeeper clients. Handoff, retry-counter, and routing tests use real loopback gRPC servers. Shutdown tests send SIGTERM to worker subprocesses and cover normal, blocked, and failed state publication. Readiness tests distinguish READY, DRAINING, and busy workers. Deadline tests cover expired tasks, mixed budgets, cancellation, and RPC timeout mapping. The unit suite does not require a running ZooKeeper service.

To use the container's dependency environment instead:

```bash
docker compose run --rm --no-deps \
  -v "$PWD/system:/app/system:ro" \
  -v "$PWD/tests:/app/tests:ro" \
  api python -m unittest discover -s tests -v
```

Run the local thread-based simulator without ZooKeeper:

```bash
USE_GRPC=0 python -m uvicorn system.api_server:app --host 127.0.0.1 --port 8000
```

This mode has different queue and failure behavior: local worker queues are unbounded, and gRPC handoff and cooperative remote deadline handling do not apply.

Per-worker gauges describe the API-side proxy, not a queue or utilization measurement inside the remote process. Queued batches plus active inference RPCs show the proxy's outstanding batch load. Worker removal and pool shutdown remove their label series; rejoining with the same ID starts with fresh values. Prometheus retains historical samples, and the 15-second scrape interval can miss brief load or lifecycle transitions.

### Protobuf generation

`system/inference.proto` defines the gRPC methods and request/response fields. After editing it, regenerate both Python files from the repository root:

```bash
python -m grpc_tools.protoc \
  -I . \
  --python_out=. \
  --grpc_python_out=. \
  system/inference.proto
```

Do not edit generated files manually. The installed gRPC and protobuf runtimes must satisfy the versions recorded in their generated headers. Rebuild the API and worker images after generation.

## Repository layout

```text
system/                       API, schedulers, tasks, worker proxies, and gRPC server
  inference.proto             RPC contract
  metrics.py                  Retry counters
  zookeeper_registry.py       Registration recovery and membership discovery
experiments/                  Load generators, experiment runner, results, and plots
tests/                        Registration, handoff, deadline, and retry-counter tests
monitoring/                   Prometheus config and provisioned Grafana dashboard
docker-compose.yml            Local service stack
Dockerfile                    Shared API/worker image
```

## Scope and next work

The next steps are a real model backend and local-mode readiness. Durable asynchronous jobs, authentication, multi-API coordination, worker supervision, and GPU scheduling require additional design. vLLM or SGLang would provide the model execution layer; this project currently provides neither integration.

See [open-source architectural references](docs/Open_Source_References.md) for relevant Ray Serve, Triton, and Celery mechanisms.

Synchronous predictions and text stages use completion notifications and a shared per-process `MAX_SYNC_REQUESTS` limit (default 1024). See [completion, cancellation, and A/B methodology](docs/Completion_Notifications.md).

[Measured completion-notification A/B results](docs/Completion_AB_Results.md) include raw-data provenance and exported figures.

[Current-runtime sustained-overload results](docs/Stress_Results.md) cover 15 minutes at 400 offered RPS, worker outages, resource trends, and recovery.

[Durable job timeouts and runtime health](docs/Durable_Runtime_Health.md) documents per-attempt RPC budgets, SQL lease recovery, independent loop probes, and bounded metrics collection.

[Runtime health checks and alerts](docs/Runtime_Health_Alerts.md) covers Docker probes, sustained Prometheus alerts and production monitoring configuration. [Fault recovery acceptance](docs/Runtime_Health_Acceptance.md) records process restart, lease expiry and stale-result fencing.
