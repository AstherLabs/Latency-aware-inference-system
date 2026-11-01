"""
api_server.py
=============
FastAPI server for the latency-aware inference system.

Endpoints:
  POST /predict                 – Submit an inference request (blocks until done)
  POST /config/batch_size       – Change batch size at runtime
  POST /config/scheduler        – Switch scheduler strategy at runtime
  POST /config/failure_rate     – Inject worker failures at runtime
  GET  /status                  – System health and queue stats

Usage:
    python -m system.api_server
    # or
    uvicorn system.api_server:app --host 0.0.0.0 --port 8000
"""

import asyncio
import logging
import hmac
import threading
import json
import time
from contextlib import asynccontextmanager
from typing import Any, Literal
from queue import Full

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from uuid import UUID

from .metrics import QUEUE_DEPTH, LOOP_HEALTHY, LOOP_AGE
from fastapi import Request, Response
from prometheus_client import (
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

import os

from .deployment import DeploymentGate, DeploymentMiddleware
from .config import config
from .queue import TaskQueue
from .scheduler import Scheduler
from .task import Task
from .sync_requests import SyncAdmissionMiddleware, wait_for_task
from .worker import WorkerPool
from system import task

JOBS_ENABLED = os.environ.get("SYNTHX_JOBS_ENABLED", "0") == "1"
USE_GRPC = os.environ.get("USE_GRPC", "0") == "1" or JOBS_ENABLED
if USE_GRPC:
    from .grpc_worker_pool import GrpcWorkerPool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Application state (initialised in lifespan)
# ---------------------------------------------------------------------------

_task_queue: TaskQueue
_worker_pool = None
_scheduler: Scheduler
_job_gateway = None
_jobs_token = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the queue, workers, and scheduler; stop them when the API shuts down."""
    global _task_queue, _worker_pool, _scheduler, _job_gateway, _jobs_token

    job_store = None
    if JOBS_ENABLED:
        from .job_gateway import JobGateway
        from .job_store import JobStore
        _jobs_token = os.environ.get("INFERENCE_SERVICE_TOKEN")
        if not _jobs_token:
            raise RuntimeError("INFERENCE_SERVICE_TOKEN is required for durable jobs")
        job_store = JobStore(os.environ.get("DATABASE_URL"))
        config.update(batch_size=1)

    _task_queue = TaskQueue(maxsize=config.max_queue_size)
    QUEUE_DEPTH.set_function(_task_queue.qsize)
    job_gateway = None
    if job_store is not None:
        job_gateway = JobGateway(job_store, _task_queue,
            max_inflight=int(os.environ.get("SYNTHX_JOBS_MAX_INFLIGHT", "1")),
            rpc_timeout_seconds=float(os.environ.get("SYNTHX_JOBS_RPC_TIMEOUT_SECONDS", "300")),
            health_timeout_seconds=config.loop_health_timeout_seconds)
    if USE_GRPC:
        _worker_pool = GrpcWorkerPool(zk_hosts=os.environ.get("ZK_HOSTS", "localhost:2181"))
    else:
        _worker_pool = WorkerPool()
    _worker_pool.start()

    _scheduler = Scheduler(_task_queue, _worker_pool.submit)
    for name, component in (("scheduler", _scheduler), ("gateway", job_gateway)):
        if component is not None:
            LOOP_HEALTHY.labels(loop=name).set_function(component.is_healthy)
            LOOP_AGE.labels(loop=name).set_function(
                lambda component=component: component.progress.snapshot()["last_cycle_age_seconds"] or 0.)
    _scheduler.start()
    if job_gateway is not None:
        job_gateway.scheduler_healthy = _scheduler.is_healthy
        _job_gateway = job_gateway
        _job_gateway.start()

    logger.info("System started — host=%s port=%d", config.host, config.port)
    try:
        yield
    finally:
        if _job_gateway is not None:
            _job_gateway.stop()
            _job_gateway = None
        _jobs_token = None
        _scheduler.stop()
        _worker_pool.stop()
        logger.info("System shutdown complete")


app = FastAPI(
    title="Latency-Aware Inference System",
    version="1.0.0",
    lifespan=lifespan,
)

_deployment_gate = DeploymentGate()
app.add_middleware(DeploymentMiddleware, gate=_deployment_gate)
app.add_middleware(SyncAdmissionMiddleware)


@app.get('/health/deployment')
async def deployment_status():
    return _deployment_gate.snapshot()


PREDICT_REQUESTS = Counter(
    "predict_requests_total",
    "Total prediction requests",
)

PREDICT_FAILURES = Counter(
    "predict_failures_total",
    "Prediction requests that failed",
)

PREDICT_RESPONSES = Counter(
    "predict_responses_total",
    "Completed prediction requests by HTTP status code",
    ["status_code"],
)
for status_code in ("200", "422", "500", "503", "504"):
    PREDICT_RESPONSES.labels(status_code=status_code)

PREDICT_DURATION = Histogram(
    "predict_duration_seconds",
    "Prediction request duration in seconds",
    ["status_code"],
    buckets=(0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60),
)
for status_code in ("200", "422", "500", "503", "504"):
    PREDICT_DURATION.labels(status_code=status_code)


@app.middleware("http")
async def record_metrics(request: Request, call_next):
    # Exclude monitoring traffic from prediction request metrics.
    """Count prediction responses and observe their full HTTP request duration."""
    if request.method != "POST" or request.url.path != "/predict":
        return await call_next(request)

    PREDICT_REQUESTS.inc()
    start = time.perf_counter()
    status_code = "500"

    try:
        response = await call_next(request)
    except Exception:
        PREDICT_FAILURES.inc()
        raise
    else:
        status_code = str(response.status_code)
        if response.status_code >= 400:
            PREDICT_FAILURES.inc()
        return response
    finally:
        PREDICT_RESPONSES.labels(status_code=status_code).inc()
        PREDICT_DURATION.labels(status_code=status_code).observe(
            time.perf_counter() - start
        )


_metrics_collection_lock = threading.Lock()


@app.get("/metrics", include_in_schema=False)
async def metrics():
    """Bound scraping without blocking the event loop or accumulating collectors."""
    if not _metrics_collection_lock.acquire(blocking=False):
        raise HTTPException(status_code=503, detail="metrics collection already in progress")

    def collect():
        try:
            return generate_latest()
        finally:
            # A timed-out HTTP waiter cannot release a still-running collector.
            _metrics_collection_lock.release()

    collector = asyncio.create_task(asyncio.to_thread(collect))
    # Retrieve errors even if the HTTP waiter times out or disconnects first.
    collector.add_done_callback(lambda future: future.exception() if not future.cancelled() else None)
    try:
        content = await asyncio.wait_for(asyncio.shield(collector), timeout=.5)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=503, detail="metrics collection timed out")
    return Response(content=content, headers={"Content-Type": CONTENT_TYPE_LATEST})

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class PredictRequest(BaseModel):
    x: Any = Field(..., description="Input value for inference")


class PredictResponse(BaseModel):
    task_id: str
    result: Any
    latency_ms: float


class BatchSizeConfig(BaseModel):
    batch_size: int = Field(..., ge=1, le=512)


class SchedulerConfig(BaseModel):
    scheduler: str = Field(..., pattern="^(fifo|batching|latency_aware)$")


class FailureRateConfig(BaseModel):
    failure_rate: float = Field(..., ge=0.0, le=1.0)


class JobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    job_id: UUID


async def require_jobs_auth(request: Request):
    if _job_gateway is None or not _jobs_token:
        raise HTTPException(status_code=503, detail="durable jobs are not enabled")
    supplied = request.headers.get("Authorization", "")
    expected = "Bearer " + _jobs_token
    if not hmac.compare_digest(supplied.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(status_code=401, detail="invalid service authorization",
                            headers={"WWW-Authenticate": "Bearer"})


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/jobs", status_code=202, dependencies=[Depends(require_jobs_auth)])
async def submit_job(body: JobRequest):
    """Poke durable admission using only the authoritative database job ID."""
    try:
        row = await asyncio.to_thread(_job_gateway.submit, str(body.job_id))
    except KeyError:
        raise HTTPException(status_code=404, detail="job not found")
    except Exception:
        logger.error("Durable job admission unavailable")
        raise HTTPException(status_code=503, detail="durable job admission unavailable")
    return {"job_id": row["job_id"], "status": row["status"]}


@app.get("/jobs/{job_id}", dependencies=[Depends(require_jobs_auth)])
async def get_job(job_id: UUID):
    try:
        row = await asyncio.to_thread(_job_gateway.store.get, str(job_id))
    except Exception:
        logger.error("Durable job lookup unavailable")
        raise HTTPException(status_code=503, detail="durable job lookup unavailable")
    if row is None:
        raise HTTPException(status_code=404, detail="job not found")
    return row

class TextTaskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    model: Literal["text_v1.2.3", "text_v1.2_refinement"]
    operation: Literal["retrieve", "rerank"]
    payload: dict[str, Any]


def _discard_abandoned(task):
    """Release waiting slots without cancelling other tasks sharing an active RPC."""
    _task_queue.discard(task)
    for owner in (_scheduler if '_scheduler' in globals() else None, _worker_pool):
        discard = getattr(owner, 'discard_pending', None)
        if discard is not None:
            discard(task)


@app.post("/text/tasks", dependencies=[Depends(require_jobs_auth)])
async def text_task(body: TextTaskRequest, request: Request):
    """Schedule one read-only text stage; this endpoint is not a durable job."""
    try:
        encoded = json.dumps(body.payload, allow_nan=False).encode()
    except (ValueError, TypeError):
        raise HTTPException(status_code=422, detail="payload must be finite JSON")
    if len(encoded) > 256 * 1024:
        raise HTTPException(status_code=413, detail="text payload exceeds 256 KiB")
    if body.operation == "retrieve" and body.payload.get("pipeline_version") != body.model:
        raise HTTPException(status_code=422, detail="retrieval version does not match worker")
    if body.operation == "retrieve" and body.model == "text_v1.2_refinement" and (
            body.payload.get("operation") != "retrieve_similar" or not all(
                isinstance(body.payload.get(key), str) and body.payload[key] for key in
                ("anchor_candidate_id", "anchor_native_sha256", "anchor_audio_sha256"))):
        raise HTTPException(status_code=422, detail="MFCC refinement requires a verified bank anchor")
    if body.operation == "retrieve" and body.model == "text_v1.2.3" and body.payload.get("operation") == "retrieve_similar":
        raise HTTPException(status_code=422, detail="anchor retrieval requires the refinement pipeline")
    budget = 600 if body.operation == "retrieve" else 180
    kind = "synthx_text_refinement" if body.model == "text_v1.2_refinement" else "synthx_text_v123"
    task = Task(input_data={"kind":kind, **body.model_dump()},
                deadline=time.monotonic() + budget)
    try:
        _task_queue.put(task, timeout=0)
    except Full:
        raise HTTPException(status_code=503, detail="task queue is full")
    await wait_for_task(task, request, _discard_abandoned)
    if task.status.value == "failed":
        status = {"deadline_exceeded":504, "overloaded":503, "busy":503,
                  "transport_error":503, "invalid_spec":422, "cancelled":499}.get(task.error_code,502)
        raise HTTPException(status_code=status, detail={"code":task.error_code, "message":task.error})
    return task.result


@app.post("/predict", response_model=PredictResponse)
async def predict(request: PredictRequest, http_request: Request):
    """
    Submit an inference request and wait synchronously for the result.

    The client blocks here until a worker completes the task (or fails it).
    This mirrors typical synchronous inference APIs used in benchmarks.
    """
    task = Task(
        deadline=time.monotonic() + 30.0,
        input_data={"x": request.x}
    )

    try:
        _task_queue.put(task, timeout=0.0)
    except Full:
        raise HTTPException(status_code=503, detail="task queue is full")

    await wait_for_task(task, http_request, _discard_abandoned)

    if task.is_expired():
        task.mark_failed(
            "request deadline exceeded",
            error_code="deadline_exceeded",
        )

    if task.status.value == "failed":
        if task.error_code == "deadline_exceeded":
            status_code = 504
        elif task.error_code == "overloaded":
            status_code = 503
        else:
            status_code = 500

        raise HTTPException(
            status_code=status_code,
            detail=task.error or "inference failed",
        )

    if task.status.value != "done":
        raise HTTPException(status_code=504, detail="inference timed out")

    return PredictResponse(
        task_id=task.task_id,
        result=task.result,
        latency_ms=round((task.end_time - task.creation_time) * 1000, 2),
    )


@app.post("/config/batch_size")
async def set_batch_size(body: BatchSizeConfig):
    """Set the maximum number of tasks assembled into a scheduler batch."""
    config.update(batch_size=body.batch_size)
    logger.info("batch_size updated to %d", body.batch_size)
    return {"status": "ok", "batch_size": config.batch_size}


@app.post("/config/scheduler")
async def set_scheduler(body: SchedulerConfig):
    """Select the strategy used on the next scheduler iteration."""
    config.update(scheduler_strategy=body.scheduler)
    logger.info("scheduler_strategy updated to %s", body.scheduler)
    return {"status": "ok", "scheduler": config.scheduler_strategy}


@app.post("/config/failure_rate")
async def set_failure_rate(body: FailureRateConfig):
    """Update failure injection locally and push it to connected gRPC workers."""
    config.update(failure_rate=body.failure_rate)
    _worker_pool.push_config(config.failure_rate, config.inference_latency_ms)
    logger.info("failure_rate updated to %.3f", body.failure_rate)
    return {"status": "ok", "failure_rate": config.failure_rate}


@app.get("/status")
async def status():
    """Return configuration and API queue statistics; this is not a readiness probe."""
    return {
        "status": "running",
        "config": config.snapshot(),
        "queue": _task_queue.stats,
        "runtime": runtime_snapshot(),
    }

def runtime_snapshot():
    """Read progress without acquiring dispatch, queue, or database work locks."""
    loops = {}
    for name, component in (("scheduler", globals().get("_scheduler")),
                            ("gateway", _job_gateway)):
        if component is None:
            if name == "scheduler" or JOBS_ENABLED:
                loops[name] = {"healthy": False, "last_cycle_age_seconds": None}
            continue
        loops[name] = component.progress.snapshot()
        loops[name]["healthy"] = component.is_healthy()
    return {"healthy": bool(loops) and all(item["healthy"] for item in loops.values()),
            "loops": loops}


@app.get("/health/runtime")
async def runtime_health():
    """Return 503 for stalled/stopped loops; lack of worker capacity is separate."""
    from fastapi.responses import JSONResponse
    snapshot = runtime_snapshot()
    return JSONResponse(snapshot, status_code=200 if snapshot["healthy"] else 503)


@app.get("/readyz")
async def readiness():
    """Report whether the API has a worker pool ready to accept routing."""
    if _worker_pool is None:
        raise HTTPException(status_code=503, detail="worker pool not initialized")
    if not USE_GRPC:
        raise HTTPException(status_code=503, detail="gRPC worker pool not enabled")
    if not runtime_snapshot()["healthy"]:
        raise HTTPException(status_code=503, detail="runtime loop progress unavailable")
    if not _worker_pool.has_workers():
        raise HTTPException(status_code=503, detail="no workers available")
    else:
        return {"status": "ready"}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "system.api_server:app",
        host=config.host,
        port=config.port,
        log_level="info",
    )
