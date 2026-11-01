# Runtime health checks, alerts, and recovery acceptance

## Deployment wiring

The local API and production gateway now run `python -m system.health_probe` as their Docker health check. It calls `/health/runtime` with a two-second HTTP timeout and requires a healthy JSON response. Docker probes every five seconds, uses a three-second command timeout, requires three failures and gives startup twenty seconds of grace.

Worker availability is deliberately excluded from liveness. A busy or disconnected model worker must not cause a healthy gateway to restart repeatedly. `/readyz` separately checks runtime progress and routing availability.

Docker marks a container unhealthy; `restart: unless-stopped` does not restart a still-running unhealthy container. There is no Docker socket mount, privileged recovery sidecar, or automatic host restart policy in this change. The acceptance harness explicitly restarts the API after observing the alert.

## Prometheus alerts

Both monitoring configurations scrape and evaluate every five seconds. The following conditions must remain active for thirty seconds before firing:

| Alert | Condition |
| --- | --- |
| `InferenceRuntimeLoopStalled` | An exported scheduler or gateway loop reports unhealthy |
| `InferenceApiScrapeFailed` | The configured API/gateway metrics target fails scraping |
| `InferenceSchedulerHealthMissing` | A successful scrape omits scheduler health |
| `InferenceGatewayHealthMissing` | The durable production gateway omits recovery-loop health |

Transient failures shorter than the alert window do not fire these alerts. A ten-second runtime threshold plus scrape/evaluation alignment and the thirty-second alert window means alerting takes longer than the initial health failure. Missing scrape data is not treated as healthy.

Alerts appear in Prometheus `/alerts` and the Grafana dashboard. External email, chat and pager delivery is not configured: a receiver and credentials must be provided separately. Do not interpret an alert as proof that a model process died; investigate loop age, API reachability, SQL availability and leases.

The dashboard includes loop health, cycle age and firing inference alerts. Existing simulation panels remain explicitly labeled: `/predict` request counts and simulation retries are not durable `/jobs` completion metrics.

## Local monitoring

The existing root Compose stack mounts the alert rules and provisions the updated dashboard. Its ports remain loopback-only for Prometheus and Grafana.

## Optional production monitoring stack

The production gateway health check is included in `deploy/ci/compose.platform.yml`. Prometheus and Grafana are separate from the two services managed by platform CI/CD. The optional stack uses the existing `deploy_default` network and targets `gateway:8000`:

```sh
export GRAFANA_ADMIN_PASSWORD='set-a-private-password'
docker compose -p synthx-monitoring -f monitoring/compose.production.yml up -d
```

Supply the password securely rather than storing it in source control. Grafana anonymous access is disabled in this production configuration. Access the loopback-bound ports through an approved SSH tunnel or reverse proxy. Do not start a duplicate stack on ports already occupied by existing monitoring services. This change prepares the configuration; it does not deploy it to the remote host.

## Reproduce fault acceptance

Use a local platform/CI image with the production Python dependencies. The runner installs the test HTTP client inside its disposable container. Docker and locally available `postgres:16-alpine` and `prom/prometheus:v3.5.0` images are required.

```sh
python3 experiments/run_runtime_health_acceptance.py \
  --image YOUR_LOCAL_PLATFORM_CI_IMAGE \
  --output /tmp/runtime-health-acceptance-unique
```

The output directory must not already exist. The runner creates isolated containers with no host ports, mounts source read-only, and removes its containers on exit. It never uses a production DSN or contacts Vast.

The acceptance uses the actual API, SQL store, scheduler and gRPC proxy. A separate fixture process stalls dispatch; no fault-injection endpoint or file gate is added to production code. A simple gRPC worker supplies deterministic results rather than running the audio model.

The scenario:

1. Complete a baseline task through HTTP `/jobs`, SQL, scheduling and gRPC.
2. Submit three more SQL-backed jobs concurrently and stall the scheduler after it takes a task.
3. Observe runtime HTTP 503, readiness refusal, failed probe, Docker unhealthy state and a firing Prometheus alert.
4. Verify the held SQL lease is unchanged and remains live, then restart the actual API process.
5. Observe the replacement waiting for ownership expiry. Trace successful claims using database-issued timestamps and assert takeover occurs after the old lease expires.
6. Hold the replacement RPC open with its new attempt and live lease. Submit the old attempt result and assert SQL rejects it while the new attempt is still running.
7. Release the replacement RPC; verify all jobs succeed, deadlines remain unchanged, alerts resolve and Docker health recovers.

The default 120-second SQL lease, 30-second renewal interval, ten-second runtime threshold and thirty-second alert duration are retained. The elapsed recovery time includes intentionally waiting for ownership expiry. It is not a throughput or real-model latency result.

Reports retain event timestamps, claim traces, API/container logs, Docker health transitions, image identity and final checks. Prometheus rule tests and the full platform CI suite run separately.
