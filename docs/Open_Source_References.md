# Open-source mechanisms worth borrowing

Reviewed against upstream documentation on October 30, 2026. These are architectural references, not newly installed dependencies. No upstream source was copied in this change.

## Ray Serve: router and replica admission

- Repository: https://github.com/ray-project/ray
- Router implementation: https://github.com/ray-project/ray/tree/master/python/ray/serve/_private/request_router
- Production guidance: https://docs.ray.io/en/latest/serve/production-guide/best-practices.html

Ray Serve is the closest reference for resident custom Python inference workers. Its separate `max_ongoing_requests` and `max_queued_requests` controls are useful models for separating execution capacity from admission capacity.

Our current change applies that separation without adopting Ray: bounded admission, central waiting, and bounded per-worker outstanding work. Next candidates are load-aware placement using estimated work, independent queues per deployment/model version, and autoscaling driven by queued work and measured service time. These features need workload measurements before implementation. Ray's private router modules are implementation references, not stable extension APIs to import into this service.

## NVIDIA Triton: model batching and queue policy

- Repository: https://github.com/triton-inference-server/server
- Core scheduler source: https://github.com/triton-inference-server/core
- Batching documentation: https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/batcher.html

Triton's dynamic batcher exposes preferred batch sizes, bounded batching delay, and queue policies. Borrow the distinction between time spent waiting to form a batch and time allowed to wait for execution. A later model adapter could use Triton as an execution backend while this platform retains business jobs, authorization, SQL fencing, and result storage.

Audio preprocessing, retrieval, model prediction, and Vital rendering should not be assumed to form one tensor batch. Measure compatibility and resource demand for each stage before enabling dynamic batching. CPU execution remains a valid deployment option.

## Celery: redelivery, prefetch, and idempotency

- Repository: https://github.com/celery/celery
- Worker-loss policy: https://docs.celeryq.dev/en/stable/userguide/configuration.html#task-reject-on-worker-lost
- Prefetch tuning: https://docs.celeryq.dev/en/stable/userguide/optimizing.html

Celery is useful for studying finite prefetch, acknowledgment timing, and redelivery after worker loss. Its worker-loss redelivery option also warns about repeated delivery loops. Redelivery is not exactly-once execution: stable job identity, attempt fencing, and idempotent result publication remain necessary.

Our durable job path already uses SQL claims and leases. Adding Celery or Redis solely to obtain a second retry mechanism would create overlapping ownership. Consider adopting a broker only when independent API replicas, durable high-volume queueing, or operational requirements justify it.

## Recommended sequence

1. Validate the current admission, recovery, and deadline behavior with overload and fault tests.
2. Measure real audio/text execution time, memory, CPU/GPU use, and queue wait by model kind.
3. Add model-aware queue policies and tenant fairness if measured contention warrants them.
4. Add application readiness checks and load-aware routing; channel connectivity alone is insufficient for model health.
5. Evaluate Triton for compatible tensor inference stages, or Ray Serve for broader Python deployment/autoscaling, against measured requirements.

A framework migration is a separate decision from borrowing its queueing mechanisms. Preserve durable job ownership and external API contracts during any future integration. Check the pinned upstream version and license before incorporating source code.
