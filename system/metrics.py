"""Process-local counters and gauges for API and worker-proxy activity."""

from prometheus_client import Counter, Gauge

RETRY_SUBMISSIONS = Counter(
    "inference_retry_submissions_total",
    "Number of retry submissions accepted by another worker",
)

RETRY_COMPLETIONS = Counter(
    "inference_retry_completions_total",
    "Number of retry submissions completed successfully by another worker",
)

WORKER_COUNT = Gauge(
    "inference_registered_workers",
    "Number of workers currently discovered by the API",
)

QUEUE_DEPTH = Gauge(
    "inference_api_queue_depth",
    "Number of tasks waiting in the API queue",
)

SCHEDULER_PENDING = Gauge(
    'inference_scheduler_pending_tasks',
    'Accepted tasks waiting in scheduler buffers, including retry handoff',
)

WORKER_QUEUED_BATCHES = Gauge(
    "inference_worker_queued_batches",
    "Batches waiting in the API-side worker proxy, excluding the active batch",
    ["worker_id"],
)

WORKER_ACTIVE_RPCS = Gauge(
    "inference_worker_active_rpcs",
    "Inference RPC calls currently in flight from the API-side worker proxy",
    ["worker_id"],
)

WORKER_ROUTING_STATE = Gauge(
    "inference_worker_routing_state",
    "One when the API-side worker proxy has the indicated lifecycle state; zero otherwise",
    ["worker_id", "state"],
)


SYNC_ACTIVE = Gauge(
    "inference_sync_requests_active",
    "Synchronous request handlers admitted in this API process",
)
SYNC_REJECTED = Counter(
    "inference_sync_admission_rejections_total",
    "Synchronous requests refused before body parsing at the process admission limit",
)

SYNC_CANCELLATIONS = Counter(
    "inference_sync_cancellations_total",
    "Synchronous HTTP waiters abandoned before task completion",
)

LOOP_HEALTHY = Gauge(
    "inference_runtime_loop_healthy",
    "One when a live loop recently completed a cycle; not model health",
    ["loop"],
)
LOOP_AGE = Gauge(
    "inference_runtime_loop_last_cycle_age_seconds",
    "Monotonic seconds since the loop last completed a cycle",
    ["loop"],
)
