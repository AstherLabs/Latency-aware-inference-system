# Scheduler overload experiments

This experiment measures the real FastAPI admission path, task queue, scheduler,
worker proxies, gRPC transport, deadlines and retry implementation. The model is
replaced by the platform's existing simulated worker. It does not call a cloud
model, Supabase, the product backend, or any production container.

The simulation adds one measurement field to successful worker results. It does
not replace the scheduler or manufacture successful replies. The default worker
cost is the existing formula `60 + batch_size * 12` milliseconds per batch: at
batch size one the nominal work is 72 ms, not 60 ms.

## Run

Requirements: Docker Desktop or Docker Engine with Compose, Python 3.12+, and k6.
Run commands from the platform repository root. All new code and comments use
English. No npm packages, k6 plugins, database credentials or GPU are needed.

```bash
# Show configuration without starting anything.
python3 experiments/k6/run.py

# Short protocol smoke test. This is not a capacity measurement.
python3 experiments/k6/run.py --execute --smoke

# Full four-worker overload and crash experiment.
python3 experiments/k6/run.py --execute --workers 4

# Worker-count comparison, preserving all other settings.
python3 experiments/k6/run.py --execute --workers 1 2 4 8
```

Each worker count takes approximately 25 minutes plus build/start/drain time
with the default nine two-minute pressure stages. A full matrix can take around
two hours. The runner prints its unique `REPORT_PATH`. Results stay in the
ignored `experiments/k6/results/` directory; do not commit raw telemetry.

The default is four workers. The matrix command above is required to compare
1/2/4/8; a four-worker run cannot establish scaling efficiency. For independent
repeats, run the same command at least three times and compare variability.

## Load sequence

1. Snapshot committed `HEAD`, build a dedicated runtime image, record its image
   ID, installed dependencies, source revision and harness hashes. Uncommitted
   source and untracked files are not included. Commit intended runtime changes
   before measuring them; the report records the working tree differences.
2. Start API and workers in a uniquely named Compose project on its own network.
   Only the API's loopback port `18180` is published. There are no production
   mounts, environment files or databases. Verify a unique benchmark identity
   and exercise actual gRPC predictions before sending pressure.
3. Warm up at 10 requests/second for 20 seconds. Exclude this phase from capacity.
4. Run independent 120-second constant-arrival-rate stages at
   **10, 25, 50, 100, 200, 400, 800, 1600 and 3200 requests/second**. The client
   does not wait for responses before scheduling new arrivals and never retries
   failed HTTP requests. There is no early abort simply because an SLO fails.
5. Between stages, stop offering traffic and require API and worker queues/RPCs
   to drain. This prevents a previous stage's backlog from contaminating the
   next stage's P95. Each stage still sustains overload throughout its full send
   window. After the sweep, explicitly run 60 seconds of low-rate recovery load.
6. Select the highest tested healthy pressure rate. Keep that rate for a separate
   130-second fault stage. At 30 seconds send **SIGKILL** to `worker-0`, then start
   that same worker at 60 seconds. Record requested/returned action timestamps,
   retries and individual completion samples. No healthy baseline means the
   fault phase is marked unrun and the experiment exits nonzero.
7. Save logs and remove only this Compose project's containers/network, including
   on failure. Images/build cache and result files are retained as evidence;
   there is no global Docker prune or deletion of other projects.

The runner does not stop at the first latency failure. If 3200 RPS is still
healthy, increase `--rates`; a last healthy point is a tested lower bound, not a
measured maximum. If the generator drops iterations, increase `--max-vus` or move
the generator to a stronger machine before claiming the target RPS was tested.

```bash
python3 experiments/k6/run.py --execute --workers 4 \
  --rates 10 25 50 100 200 400 800 1600 3200 6400 \
  --duration 180 --max-vus 2048 --slo-ms 250
```

CPU-heavy or more variable real model workloads need a later, separate benchmark.
Use `--latency-ms`, `--batch-size`, `--scheduler` and `--queue-size` to compare
controlled simulation configurations. Change one dimension at a time. The API's
existing 30-second request deadline and bounded worker-proxy admission remain
unchanged. The default API queue size is explicitly 1000.

## Reading results

`report.json` contains all phases, configuration, capacity observations and
fault timing. `summary.csv` provides one row per phase/rate/worker count. Each
phase directory contains:

- `summary.json`: complete k6 aggregate metrics, including success-only
  P50/P95/P99, all-attempt latency, 503/504/transport failures and result mismatches.
- `telemetry.jsonl`: API/worker queue depths, active RPCs, routing states, retries
  and prediction counters sampled about every two seconds; Docker CPU/memory
  snapshots about every ten seconds. Blocking sampling calls add jitter.
- `k6.log`: generator warnings and interrupted-iteration information.
- Fault phases additionally have `samples.json.gz` with raw timestamped k6
  points. Other phases retain aggregates to keep disk usage bounded.

A tested rate is healthy only when all of these hold:

- No `dropped_iterations`, at least 99% of planned arrivals started, and all
  started requests finished. Lost/interrupted requests never disappear from
  the verdict. k6 can start an extra boundary iteration in short stages.
- At least 99% of started requests return a valid, correlated model result.
- At least 100 successful samples exist and successful-request P95 is at most
  the configured 250 ms budget. This is a configurable experiment SLO, not an
  industry standard or a pre-existing product promise. P99 is reported, but
  100 samples are not sufficient for a stable P99 estimate.

Capacity breach requires adequate delivered load and at least 100 completions,
then either an error-budget breach or a measured P95 breach. Small samples are
inconclusive, not overload. Generator-limited rates are listed separately.
Fast 503 responses may keep all-request P95 low while success throughput
collapses; always read success fraction and throughput beside latency.

`completion_rps_over_execution` uses the actual k6 execution duration, including
request drain, not just the offered-load window or monitoring cleanup time.
`queue_dispatch_overhead_ms` subtracts measured worker batch time from API task
latency. It includes queueing, scheduling, serialization, RPC and handoff overhead;
it is **not an exact queue-wait metric**. Transport/setup overhead is visible in
the difference between client latency and API latency.

Fault recovery requires three consecutive 10-second completion windows with
at least 90% of the target completion count, at least 99% valid successes and
P95 within budget. Each window needs at least 20 completions. Qualified recovery
is suppressed if the overall fault load was not delivered. Missing observations
remain `null`; no time is fabricated. First failed completion and first sampled
retry are observations after the kill signal, not precise failure-detector
latencies or proof of causality. First success from worker `1000` after restart
records when that worker actually served a response again.

## What this does not prove

The real deployment currently uses static worker addresses, so this experiment
also uses static routing. Killing a worker does not remove its proxy from the
registered-worker gauge. Transport failures now quarantine its routing state as
UNAVAILABLE until a connectivity probe succeeds; advertised lifecycle metadata
alone does not override quarantine. The fault experiment measures transport
recovery rather than pretending ZooKeeper removed the worker. Discovery/session expiry requires a separate
ZooKeeper experiment. Raising the simulated worker count does not change the
production worker count or GPU parallelism.

The test targets `/predict`. Production audio jobs also traverse PostgreSQL,
durable leases, object storage and model adapters; those stages are not included
in this core-scheduler measurement. Keep the separate real-audio acceptance and
lease-recovery tests. There is no claim about GPU/model latency, audio quality,
authentication, WAN/TLS, or durable job capacity here.

Local runs place k6 on the host and the server containers in that host's Docker
environment. They share physical CPU/memory. Such results are valid for debugging
and regression comparisons, but not final production capacity claims. For a
final benchmark, place this isolated stack on representative server hardware and
run k6 from another machine with network access to its benchmark endpoint;
retain the identity guard and collect generator resource usage as well. This
runner's automated orchestration uses local loopback; a remote topology needs
an explicit host/transport adaptation rather than targeting production by URL.

`experiment_completed: true` means all requested phases ran and reports were
saved. It does not mean all rates met the SLO: intentional overload is expected
to fail it. Read `capacity`, `valid_load`, `service_slo_breached`, recovery fields,
and `errors`. A smoke run sets `smoke_only: true` and is never capacity evidence.

## Offline checks

```bash
python3 -m unittest discover -s experiments/k6/tests -v
node --test experiments/k6/tests/config.test.mjs
```

No third-party JavaScript modules are downloaded by the script. The k6 script
refuses traffic unless `/benchmark/identity` matches `RUN_ID`. It also validates
the predicted value, task identity, worker namespace, timing fields and runtime
marker on every successful response.


## Backpressure controls

`--worker-capacity 3` controls outstanding batches per proxy (active plus queued).
Accepted tasks wait centrally when all compatible proxies are busy. Queue size
and original deadlines still bound admission and waiting; this does not raise
model execution throughput. See [scheduling behavior](../../docs/Scheduling_Backpressure.md).

For long queue waits, preallocate sufficient k6 clients before traffic starts:

```bash
python3 experiments/k6/run.py --execute --workers 2 --rates 10 50 \
  --duration 10 --queue-size 1000 --worker-capacity 3 \
  --max-vus 512 --preallocated-vus 512
```

Each synchronous request occupies a VU until its response returns. A rough sizing
starting point is arrival rate times the longest expected response time. Dynamic
VU allocation can lag a rapidly growing queue; `dropped_iterations` invalidates
that stage even if every request actually sent succeeds. Preallocation also uses
memory; observe generator resource limits and retain validity checks.

The first result mismatch per VU now records sanitized diagnostic fields in
`k6.log`. This helps diagnose an invalid HTTP 200 response without silently
counting it as a success. Earlier unexplained mismatches are not retroactively
classified as resolved.
