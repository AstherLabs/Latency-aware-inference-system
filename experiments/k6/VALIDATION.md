# Harness validation, 2026-10-30

## Executed checks

- Nine Python verdict/isolation/recovery checks passed.
- Two Node configuration checks passed.
- Final published entry point smoke completed with valid results, no missing
  requests and no generator drops. Report: `results/bench-43f1a4207f84/report.json`.
- A shortened real Docker/API/gRPC overload and crash experiment completed with
  no harness errors. Two simulated workers, 60 ms base cost, batch size one, FIFO,
  three 12-second pressure stages, and a separate 130-second crash phase.
- This is local validation, not a server capacity benchmark. Other test work
  shared the machine. The full default sweep and 1/2/4/8 matrix were not run.

| Offered rate | Sent | Successful | Success fraction | Successful P95 |
| --- | --- | --- | --- | --- |
| 10/s | 121 | 121 | 100% | 83 ms |
| 50/s | 601 | 324 | 53.91% | 151 ms |
| 200/s | 2400 | 324 | 13.50% | 154 ms |

There were no generator drops in these stages. The nearly unchanged successful
count between 50/s and 200/s while success fraction collapsed demonstrates why
P95 alone is inadequate. One extra boundary iteration in short stages is possible.
The 12-second stages are too short to establish stable production capacity.

## Crash observations

The runner sent SIGKILL to one simulated worker and restarted that same service
30 seconds later. The running workload continued without failed HTTP completions.
A retry-counter increase was first sampled approximately 21.81 seconds after
the kill request. This includes detector/transport behavior and sampling delay;
it is not an exact failure-detector latency. The restarted worker first returned
a valid result approximately 1.56 seconds after the start command returned.

Three consecutive ten-second healthy completion windows were confirmed 30
seconds after restart. This is a confirmation interval, not evidence of a
30-second outage: those windows were healthy from their start. No precise
recovery onset can be inferred from this coarse window definition.

Raw report and fault samples: `results/bench-fc52169161c4/`. This early integration
report retains its original monitoring-walltime throughput field. The published
runner corrected that field to use k6 execution duration, exercised again by the
final smoke. Read each phase's original k6 summary for measured execution time.

## Existing platform regressions

An ephemeral image of committed platform HEAD ran the existing `tests/` suite:
118 discovered, 105 passed, 13 skipped. All skipped tests were
`test_job_gateway.DurableGatewayTests` because no explicitly disposable
PostgreSQL URL was configured. No production database was used. Thus SQL lease
fencing/recovery integration was not revalidated in this run.

No production service, model, checkpoint or cloud worker was restarted. Only
the uniquely named local benchmark containers and network were removed.
