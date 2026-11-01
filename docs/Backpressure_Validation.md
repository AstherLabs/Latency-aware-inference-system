# Backpressure change verification

Verified October 30, 2026 (America/Chicago). All load traffic targeted disposable local Docker benchmark stacks. No production services were restarted or deployed.

## Regression coverage

The candidate suite discovered 130 tests: 117 passed and 13 PostgreSQL-dependent tests were skipped in the no-database run. Those 13 tests were then run separately against a disposable PostgreSQL 16 database with the CI schema; all passed. The benchmark helper suite passed nine Python tests and three Node tests.

The final repository suite was rerun directly from the working tree: 117 tests passed and the same 13 database tests were skipped (already passed separately against PostgreSQL). This caught a pre-existing default-version mapping issue: switching the text default to v1.2.3 removed the v1.2.2 dictionary entry. Explicit version mappings restore both pinned versions while preserving the v1.2.3 defaults.

The candidate was reviewed for retry ownership races, boundedness, lock ordering, and mixed-kind head-of-line blocking. Regression tests reproduce the fast repeated-failure race before the fix.

## Isolated load results

Two simulation workers, batch size one, 60 ms base simulated batch latency (72 ms nominal per single-task batch), three outstanding batches per proxy, queue size 1,000, host-local k6. These are scheduler checks, not real-model or production capacity claims.

| Phase | Target and duration | Completed / successful | Generator drops | Success P95 |
| --- | --- | --- | --- | --- |
| Baseline, preallocated clients | 10 RPS, 10 seconds | 101 / 101 | 0 | 83 ms |
| Overload, 512 preallocated clients | 50 RPS, 10 seconds | 501 / 501 | 0 | 8,354 ms |
| Recovery after overload | 10 RPS, 10 seconds | 100 / 100 | 0 | 83.05 ms |
| Worker kill/restart | 10 RPS, 130 seconds | 1,301 / 1,301 | 0 | 83 ms |

The corrected overload stage exceeded the configured 250 ms P95 SLO despite no failed requests. Completion throughput over execution, including draining, was about 26.63 RPS. Queueing absorbed the burst; it did not increase execution capacity.

The fault stage killed one worker around 30 seconds and restarted it around 60 seconds. The restarted worker first returned a successful response about 2.15 seconds after the restart command returned. No failed completion was observed. The 30-second stable-recovery value reflects the predefined three-window confirmation rule, not a 30-second outage. Retry observation is sampled and is not an exact failure-detection measurement.

An earlier 50 RPS stage used dynamic VU allocation and dropped 101 iterations. Its 400 sent requests all succeeded, but it is explicitly invalid as a 50 RPS result. The corrected run used preallocated clients and delivered the requested load. Earlier unrelated invalid-result failures remain unexplained; sanitized mismatch diagnostics were added rather than suppressing their threshold.

## Evidence and provenance

Reports are retained under `experiments/k6/results/backpressure-20261030/` (ignored generated artifacts): `fault-report.json` and `overload-report.json`.

The runner builds committed source only. To verify uncommitted changes without committing or deploying the user repository, disposable local candidate snapshots were created in `/tmp/inference-backpressure-work`. The fault report records candidate `876452b4a84ba98031725c3420f3f39cff52c920`; the overload report records `9cda155c02ed1536bf4c5f0d5de9a5e59e2bf52f`, which additionally handles retry-limit reduction while waiting. The baseline repository revision was `52b7ac4b5ab8c927cfb4c0486319a5376036ae9d`. The existing v1.2.3 defaults in `system/worker_text.py` were preserved and excluded from candidate load snapshots; the final repository additionally restores its explicit v1.2.2 compatibility mapping.

Full raw stage artifacts remain in the temporary paths recorded in these reports. Keep or export them before clearing temporary storage if detailed request-window analysis is needed.
