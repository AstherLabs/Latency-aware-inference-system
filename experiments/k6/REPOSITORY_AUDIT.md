# Repository audit, 2026-10-30

The repository was reviewed for redundant experiment entry points, generated
files, and non-English source comments. No existing product code was deleted or
rewritten as part of this benchmark.

- `system/worker_text.py` has a pre-existing uncommitted change. It is retained
  untouched and is excluded from the benchmark's committed-source snapshot.
- Nine untracked PNG files in the repository root appear unrelated to inference
  serving. Their ownership/purpose cannot be established from filenames. They
  are retained; they are not copied into the benchmark image.
- Tracked CSV files and plots in `experiments/results/` and `experiments/plots/`
  are historical experiment artifacts, not runtime dependencies. They remain
  available for old result reproduction and are not included in the image.
- `load_test.py`, `metrics.py`, `plot_results.py` and `run_experiments.py` form a
  connected historical experiment toolchain. Deleting one as apparently unused
  would break the others. Some old fault scenarios have manual prerequisites
  and TODO notes. Use `experiments/k6/run.py` for the automated overload/crash
  protocol; old files are not current production-capacity evidence.
- `sustained_load_test.py` is still a useful dependency-free HTTP arrival-rate
  probe. It does not replace the new isolated environment or failure timeline.
- `system/worker.py` is the supported non-gRPC runtime selected by `USE_GRPC=0`;
  it is not dead code merely because the cloud deployment uses gRPC.
- Generated protobuf Python files are imported runtime dependencies and are
  intentionally retained beside their `.proto` source.
- Ignored `__pycache__`/`.pyc` files are disposable, not source. Only directories
  containing exclusively bytecode may be removed during cleanup. Benchmark
  output is ignored separately, so new raw telemetry does not pollute Git.
- The scanned Python/JavaScript source under `system/`, `experiments/` and
  `tests/` contained no Chinese text before this change. New harness code,
  commands, comments and documentation are English-only.

This audit is not a whole-program proof that every legacy function is reachable.
No source or user-created file is classified as safe to delete from a name alone.
