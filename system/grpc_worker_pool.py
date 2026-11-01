"""
grpc_worker_pool.py
===================
GrpcWorkerPool discovers live workers from ZooKeeper and dispatches
inference batches to them via gRPC.

Each remote worker is wrapped in a GrpcWorker object that owns a
dedicated thread + internal queue, mirroring the design of the original
thread-based Worker.  Batches submitted to the pool are round-robined
across idle workers exactly as before.
"""

import json
import logging
import os
import threading
import time
from typing import Callable, Dict, List, Optional

import grpc

from . import inference_pb2, inference_pb2_grpc
from .task import Task, TaskStatus
from .config import config
from .zookeeper_registry import WorkerDiscovery, WorkerInfo
from .metrics import (
    RETRY_COMPLETIONS, RETRY_SUBMISSIONS, WORKER_COUNT,
    WORKER_QUEUED_BATCHES, WORKER_ACTIVE_RPCS, WORKER_ROUTING_STATE,
)
logger = logging.getLogger(__name__)

Batch = List[Task]


class GrpcWorker:
    """
    Client-side proxy for one remote gRPC worker process.
    Maintains its own queue + thread so submit() is non-blocking,
    matching the behaviour of the original Worker class.
    """

    def __init__(self, worker_id: str, address: str,
                 retry_handler: Callable[[Task, str], bool], capacity_event=None):
        """Create a channel and start the serial dispatch thread for one remote worker."""
        self.worker_id = worker_id
        self.address = address
        self._retry_handler = retry_handler
        self._capacity_event = capacity_event or threading.Event()
        self.max_outstanding_batches = config.worker_max_outstanding_batches
        self._unavailable = False
        self._recover_after = 0.0
        self._recovery_failures = 0
        self._channel = grpc.insecure_channel(address, options=[
            ("grpc.max_send_message_length", 16 * 1024 * 1024),
            ("grpc.max_receive_message_length", 16 * 1024 * 1024),
            ('grpc.initial_reconnect_backoff_ms', 100),
            ('grpc.min_reconnect_backoff_ms', 100),
            ('grpc.max_reconnect_backoff_ms', 1000),
        ])
        self._stub = inference_pb2_grpc.WorkerServiceStub(self._channel)
        self._batch_queue: List[Batch] = []
        self._lock = threading.Lock()
        self._has_work = threading.Event()
        self._stop = threading.Event()
        self._retry_on_stop = False
        self._processing = False
        self._routing_state = "READY"
        # Keep child handles: a retired dispatch thread cannot recreate removed labels
        # or overwrite the metrics of a replacement with the same worker identity.
        self._queued_batches_metric = WORKER_QUEUED_BATCHES.labels(worker_id)
        self._active_rpcs_metric = WORKER_ACTIVE_RPCS.labels(worker_id)
        self._state_metrics = {
            state: WORKER_ROUTING_STATE.labels(worker_id, state)
            for state in ("READY", "DRAINING", "UNAVAILABLE")
        }
        self._metrics_removed = False
        self._queued_batches_metric.set(0)
        self._active_rpcs_metric.set(0)
        self._update_state_metrics()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name=f"GrpcWorker-{worker_id}"
        )
        self._thread.start()

    def submit(self, batch: Batch) -> None:
        """Enqueue a batch or mark its tasks overloaded if the proxy rejects it."""
        if not self.try_submit(batch):
            for task in batch:
                task.mark_failed("worker unavailable or at capacity", "overloaded")

    def try_submit(self, batch: Batch) -> bool:
        """Reserve configured capacity and retry budget together before enqueueing."""
        with self._lock:
            if self._stop.is_set() or self._routing_state != "READY" or self._unavailable:
                return False
            outstanding = self._processing + len(self._batch_queue)
            if outstanding >= self.max_outstanding_batches:
                return False
            live = []
            for task in batch:
                retry = task.retry_pending
                if not task.accept_retry(config.max_task_retries):
                    if task.is_expired():
                        task.mark_failed('request deadline exceeded', 'deadline_exceeded')
                    elif task.status not in (TaskStatus.DONE, TaskStatus.FAILED):
                        task.mark_failed('retry budget exhausted', 'retry_exhausted')
                    continue
                if retry:
                    RETRY_SUBMISSIONS.inc()
                live.append(task)
            if not live:
                return True
            batch = live
            self._batch_queue.append(batch)
            self._queued_batches_metric.set(len(self._batch_queue))
        self._has_work.set()
        return True

    def discard_pending(self, task):
        """Remove queued work only; an active batch may serve other requests."""
        with self._lock:
            batches = [[item for item in batch if item is not task] for batch in self._batch_queue]
            self._batch_queue = [batch for batch in batches if batch]
            self._queued_batches_metric.set(len(self._batch_queue))
        self._capacity_event.set()

    def _mark_unavailable(self):
        """Quarantine failed transport and return queued work without holding locks."""
        with self._lock:
            self._unavailable = True
            self._recovery_failures += 1
            self._recover_after = time.monotonic() + min(30, config.worker_recovery_backoff_seconds * 2 ** min(self._recovery_failures - 1, 5))
            pending, self._batch_queue = self._batch_queue, []
            self._queued_batches_metric.set(0)
            self._update_state_metrics()
        self._capacity_event.set()
        for batch in pending:
            for task in batch:
                self._finish_or_retry(task, 'worker transport unavailable', True)

    def _probe_recovery(self):
        with self._lock:
            if not self._unavailable:
                return True
            if self._stop.is_set() or time.monotonic() < self._recover_after:
                return False
        # Channel connectivity is a transport probe, not model execution readiness.
        probe = None
        try:
            probe = grpc.channel_ready_future(self._channel)
            probe.result(timeout=.2)
        except (grpc.FutureTimeoutError, grpc.FutureCancelledError, ValueError):
            if probe is not None:
                probe.cancel()
            with self._lock:
                self._recovery_failures += 1
                self._recover_after = time.monotonic() + min(30, config.worker_recovery_backoff_seconds * 2 ** min(self._recovery_failures - 1, 5))
            return False
        with self._lock:
            if self._stop.is_set():
                return False
            self._unavailable = False
            self._recovery_failures = 0
            self._update_state_metrics()
        self._capacity_event.set()
        return True

    def busy(self) -> bool:
        """Report whether this proxy has an active RPC or a queued batch."""
        with self._lock:
            return self._processing or bool(self._batch_queue)

    def begin_stop(self, retry_pending: bool = False) -> None:
        """Reject new submissions before the pool releases its membership lock."""
        with self._lock:
            if not self._stop.is_set():
                self._retry_on_stop = retry_pending
                self._stop.set()
                self._update_state_metrics()
        self._has_work.set()
        self._capacity_event.set()

    def stop(self, retry_pending: bool = False) -> None:
        """Cancel the active RPC and hand off queued tasks only during worker removal."""
        self.begin_stop(retry_pending)
        with self._lock:
            pending = self._batch_queue
            self._batch_queue = []
            self._queued_batches_metric.set(0)
        # Cancel the active RPC promptly; its exception handler transfers its
        # own tasks. Never wait for that handler while holding the pool lock.
        self._channel.close()
        for batch in pending:
            for task in batch:
                self._finish_or_retry(task, "worker removed", self._retry_on_stop)
        if threading.current_thread() is not self._thread:
            self._thread.join(timeout=3)
        with self._lock:
            if not self._metrics_removed:
                WORKER_QUEUED_BATCHES.remove(self.worker_id)
                WORKER_ACTIVE_RPCS.remove(self.worker_id)
                for state in self._state_metrics:
                    WORKER_ROUTING_STATE.remove(self.worker_id, state)
                self._metrics_removed = True

    def _finish_or_retry(self, task: Task, error: str, retry_allowed: bool) -> None:
        """Preserve terminal states, enforce deadlines, and attempt eligible handoff."""
        if task.status in (TaskStatus.DONE, TaskStatus.FAILED):
            return
        if task.is_expired():
            task.mark_failed("request deadline exceeded", "deadline_exceeded")
            return
        if isinstance(task.input_data, dict) and task.input_data.get("kind") in ("synthx_v73", "synthx_text_v123", "synthx_text_refinement"):
            task.mark_failed(error, "transport_error")
            return
        if retry_allowed:
            try:
                if self._retry_handler(task, self.worker_id):
                    return
            except Exception:
                logger.exception("Retry dispatch failed for task %s", task.task_id)
        task.mark_failed(error)

    def update_config(self, failure_rate: float, inference_latency_ms: float) -> None:
        """Send runtime simulation settings to the remote worker."""
        try:
            self._stub.UpdateConfig(
                inference_pb2.ConfigUpdate(
                    failure_rate=failure_rate,
                    inference_latency_ms=inference_latency_ms,
                ),
                timeout=5,
            )
        except Exception as exc:
            logger.warning("UpdateConfig failed for %s: %s", self.worker_id, exc)

    def _loop(self) -> None:
        """Dispatch queued batches serially without holding the queue lock during RPCs."""
        while not self._stop.is_set():
            self._has_work.wait(timeout=0.1)
            self._has_work.clear()
            if not self._probe_recovery():
                continue
            while True:
                with self._lock:
                    if self._stop.is_set() or not self._batch_queue:
                        break
                    batch = self._batch_queue.pop(0)
                    self._queued_batches_metric.set(len(self._batch_queue))
                    self._processing = True

                try:
                    self._process(batch)
                finally:
                    with self._lock:
                        self._processing = False
                    self._capacity_event.set()

    def _process(self, batch: Batch) -> None:
        """Send live tasks with their budgets and reconcile results or eligible RPC failures."""
        active_batch = []
        for task in batch:
            if task.status in (TaskStatus.DONE, TaskStatus.FAILED):

                continue
            if task.is_expired():
                task.mark_failed("request deadline exceeded", error_code="deadline_exceeded")
            else:
                active_batch.append(task)

        if not active_batch:
            return

        batch = active_batch

        if self._stop.is_set():
            for task in batch:
                self._finish_or_retry(task, "worker removed", self._retry_on_stop)
            return

        for task in batch:
            task.mark_processing()

        task_map = {task.task_id: task for task in batch}
        requests = []
        remaining_times = []
        for task in batch:
            request = inference_pb2.TaskRequest(
                task_id=task.task_id,
                input_json=json.dumps(task.input_data),
            )

            remaining_time = task.remaining_seconds()

            if remaining_time is not None:
                request.remaining_seconds = remaining_time
                remaining_times.append(remaining_time)

            requests.append(request)
        rpc_timeout = max(remaining_times) if remaining_times else 35.0
        attempt_caps = [task.rpc_timeout_seconds for task in batch
                        if task.rpc_timeout_seconds is not None]
        if attempt_caps:
            rpc_timeout = min(rpc_timeout, min(attempt_caps))

        if rpc_timeout <= 0:
            for task in batch:
                task.mark_failed(
                    "request deadline exceeded",
                    error_code="deadline_exceeded",
                )
            return
        try:
            self._active_rpcs_metric.set(1)
            try:
                response = self._stub.ProcessBatch(
                    inference_pb2.BatchRequest(tasks=requests),
                    timeout=rpc_timeout,
                )
            finally:
                self._active_rpcs_metric.set(0)
            for result in response.results:
                task = task_map.get(result.task_id)
                if task is None:
                    continue
                if result.success:
                    task.mark_done(json.loads(result.result_json))
                    if task.retry_count and task.status == TaskStatus.DONE:
                        RETRY_COMPLETIONS.inc()
                        logger.info("Completed retried task %s on %s",
                                    task.task_id, self.worker_id)
                else:
                    task.mark_failed(result.error, error_code=result.error_code or None)
            for task in batch:
                if (isinstance(task.input_data, dict) and task.input_data.get("kind") in ("synthx_v73", "synthx_text_v123", "synthx_text_refinement")
                        and task.status not in (TaskStatus.DONE, TaskStatus.FAILED)):
                    task.mark_failed("worker omitted task result", "transport_error")

        except grpc.RpcError as exc:
            logger.warning(
                "Worker %s RPC failed: %s",
                self.worker_id,
                exc.code(),
            )

            if exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                for task in batch:
                    expired = task.is_expired()
                    task.mark_failed(
                        "request deadline exceeded" if expired else "inference RPC timed out",
                        error_code="deadline_exceeded" if expired else "rpc_timeout",
                    )
                return

            if exc.code() == grpc.StatusCode.UNAVAILABLE and not self._stop.is_set():
                self._mark_unavailable()

            retiring = self._stop.is_set() and self._retry_on_stop
            retry_allowed = (
                exc.code() == grpc.StatusCode.UNAVAILABLE
                and (not self._stop.is_set() or retiring)
            ) or (exc.code() == grpc.StatusCode.CANCELLED and retiring)
            for task in batch:
                self._finish_or_retry(task, str(exc), retry_allowed)

        except Exception as exc:
            logger.error("gRPC call failed: %s", exc)
            for task in batch:
                # Removal may close the channel between the stop check and
                # invocation, producing a local error instead of RpcError.
                self._finish_or_retry(
                    task, str(exc),
                    self._stop.is_set() and self._retry_on_stop
                    and isinstance(exc, ValueError),
                )
    def set_routing_state(self, state: str) -> None:
        """Update admission state without cancelling accepted work."""
        with self._lock:
            self._routing_state = state
            self._update_state_metrics()

    def _update_state_metrics(self) -> None:
        """Publish lifecycle flags while holding the proxy lock (or during initialization)."""
        effective = 'UNAVAILABLE' if self._unavailable and self._routing_state == 'READY' else self._routing_state
        for state, metric in self._state_metrics.items():
            metric.set(int(not self._stop.is_set() and effective == state))

    def is_ready(self) -> bool:
        """Report routing eligibility independently of current load."""
        with self._lock:
            return not self._stop.is_set() and self._routing_state == "READY" and not self._unavailable

class GrpcWorkerPool:
    """
    Worker pool that discovers live workers from ZooKeeper and routes
    batches to them via gRPC.  Worker join/leave is handled automatically
    through ZooKeeper watches — no manual reconfiguration needed.
    """

    def __init__(self, zk_hosts: str = "localhost:2181", static_workers=None):
        """Initialize membership and routing state; discovery starts in start()."""
        self._zk_hosts = zk_hosts
        self._workers: Dict[str, GrpcWorker] = {}
        self._lock = threading.Lock()
        self._rr_index = 0
        self._discovery: Optional[WorkerDiscovery] = None
        self._stopping = False
        self._central_retry = None
        self._capacity_event = threading.Event()
        raw_static = os.environ.get("STATIC_WORKERS")
        if static_workers is None and raw_static is not None:
            try:
                static_workers = json.loads(raw_static)
            except (ValueError, TypeError) as exc:
                raise ValueError("STATIC_WORKERS must be a JSON object") from exc
            if static_workers is None:
                raise ValueError("STATIC_WORKERS must be a JSON object")
        if static_workers is not None and (
            not isinstance(static_workers, dict) or any(
                not isinstance(wid, str) or not wid.strip()
                or not isinstance(address, str) or not address.strip()
                for wid, address in static_workers.items()
            )
        ):
            raise ValueError("STATIC_WORKERS must map nonempty worker IDs to addresses")
        raw_capabilities = os.environ.get("WORKER_CAPABILITIES")
        self._capabilities = None
        if raw_capabilities is not None:
            try:
                capabilities = json.loads(raw_capabilities)
                if not isinstance(capabilities, dict) or any(
                    not isinstance(wid, str) or not wid or not isinstance(kinds, list) or not kinds
                    or any(kind not in ("simulation", "synthx_v73", "synthx_text_v123", "synthx_text_refinement") for kind in kinds)
                    for wid, kinds in capabilities.items()
                ):
                    raise ValueError("invalid worker capabilities")
                self._capabilities = {wid: set(kinds) for wid, kinds in capabilities.items()}
            except (ValueError, TypeError) as exc:
                raise ValueError("WORKER_CAPABILITIES must map worker IDs to supported task kinds") from exc
        self._static_workers = static_workers

    def start(self) -> None:
        """Watch ZooKeeper membership and create proxies for registered workers."""
        if self._static_workers is not None:
            self._update_workers({wid: WorkerInfo(address=address, state="READY")
                                  for wid, address in self._static_workers.items()})
            logger.info("GrpcWorkerPool started with %d static workers", len(self._workers))
            return
        self._discovery = WorkerDiscovery(
            zk_hosts=self._zk_hosts,
            on_change=self._update_workers,
        )
        logger.info("GrpcWorkerPool started with %d workers", len(self._workers))

    def stop(self) -> None:
        """Reject retries, stop discovery, and close proxies outside the membership lock."""
        with self._lock:
            self._stopping = True
            workers = list(self._workers.values())
            self._workers.clear()
            WORKER_COUNT.set(0)
            for worker in workers:
                worker.begin_stop()
        if self._discovery:
            self._discovery.stop()
        for worker in workers:
            worker.stop()
        logger.info("GrpcWorkerPool stopped")

    def push_config(self, failure_rate: float, inference_latency_ms: float) -> None:
        """Snapshot membership before sending configuration RPCs outside the pool lock."""
        with self._lock:
            workers = list(self._workers.values())
        for w in workers:
            w.update_config(failure_rate, inference_latency_ms)

    @staticmethod
    def _kind(task):
        return task.input_data.get("kind", "simulation") if isinstance(task.input_data, dict) else "simulation"

    def _supports(self, worker, task):
        kinds = {"simulation", "synthx_v73"} if self._capabilities is None else self._capabilities.get(worker.worker_id, set())
        kind = self._kind(task)
        return isinstance(kind, str) and kind in kinds

    def submit(self, batch: Batch) -> None:
        """Split heterogeneous batches before selecting a compatible worker."""
        groups = {}
        for task in batch:
            kind = self._kind(task)
            if not isinstance(kind, str):
                task.mark_failed("invalid task kind", "invalid_spec")
                continue
            groups.setdefault(kind, []).append(task)
        for group in groups.values():
            self._submit_compatible(group)

    def discard_pending(self, task):
        """Purge abandoned proxy queue entries without closing shared channels."""
        with self._lock:
            workers = list(self._workers.values())
        for worker in workers:
            worker.discard_pending(task)

    def bind_scheduler(self, retry_handler, wakeup):
        """Use one central scheduler for accepted retries and capacity notifications."""
        self._central_retry = retry_handler
        self._capacity_event = wakeup
        with self._lock:
            for worker in self._workers.values():
                worker._capacity_event = wakeup

    def try_submit(self, batch: Batch) -> Batch:
        """Return unscheduled tasks to the caller without marking them overloaded."""
        groups = {}
        for task in batch:
            if task.status in (TaskStatus.DONE, TaskStatus.FAILED):
                continue
            if task.is_expired():
                task.mark_failed('request deadline exceeded', 'deadline_exceeded')
                continue
            kind = self._kind(task)
            if not isinstance(kind, str) or kind not in ('simulation', 'synthx_v73', 'synthx_text_v123', 'synthx_text_refinement'):
                task.mark_failed('invalid task kind', 'invalid_spec')
                continue
            groups.setdefault(kind, []).append(task)
        deferred = []
        for group in groups.values():
            if not self._try_submit_compatible(group):
                deferred.extend(group)
        return deferred

    def _submit_compatible(self, batch: Batch) -> None:
        if not self._try_submit_compatible(batch):
            for task in batch:
                task.mark_failed('all workers at capacity or unavailable', 'overloaded')

    def _try_submit_compatible(self, batch: Batch) -> bool:
        """Prefer idle compatible proxies, then try their remaining capacity."""
        with self._lock:
            ready = [w for w in self._workers.values() if w.is_ready() and self._supports(w, batch[0])]
            if not ready:
                return False

            idle = [w for w in ready if not w.busy()]
            pool = idle if idle else ready
            worker = pool[self._rr_index % len(pool)]
            self._rr_index += 1

        if worker.try_submit(batch):
            return True
        else:
            with self._lock:
                for w in self._workers.values():
                    if w is worker or not w.is_ready() or not self._supports(w, batch[0]):
                        continue
                    if w.try_submit(batch):
                        return True
            return False



    def _update_workers(self, workers: Dict[str, WorkerInfo]) -> None:
        """Apply membership changes and retire removed proxies outside the pool lock."""
        removed = []
        with self._lock:
            if self._stopping:
                return
            # Add proxies before removing stale members so handoff sees current candidates.
            for wid, info in workers.items():
                if wid not in self._workers:
                    self._workers[wid] = GrpcWorker(wid, info.address, self.try_resubmit, self._capacity_event)
                    logger.info("Connected to %s at %s", wid, info.address)
                self._workers[wid].set_routing_state(info.state)
            # Mark removed proxies stopped before releasing the membership lock.
            gone = [wid for wid in self._workers if wid not in workers]
            for wid in gone:
                worker = self._workers.pop(wid)
                worker.begin_stop(retry_pending=True)
                removed.append(worker)
                logger.info("Worker %s removed", wid)
            WORKER_COUNT.set(len(self._workers))
        for worker in removed:
            worker.stop(retry_pending=True)
        self._capacity_event.set()


    def try_resubmit(self, task: Task, failed_worker_id: str) -> bool:
        """Retain retries centrally when bound; otherwise use the legacy direct handoff."""
        if self._central_retry is not None:
            with self._lock:
                if self._stopping:
                    return False
            return self._central_retry(task, failed_worker_id)
        with self._lock:
            if self._stopping:
                return False
            candidates = [
                w for w in self._workers.values()
                if w.worker_id != failed_worker_id and w.is_ready() and self._supports(w, task)
            ]
        if not candidates:
            return False

        if not task.try_reserve_retry():
            return False

        idle = []
        busy = []

        for w in candidates:
            if w.busy():
                busy.append(w)
            else:
                idle.append(w)

        for w in idle + busy:
            if w.try_submit([task]):
                RETRY_SUBMISSIONS.inc()
                logger.info("Retrying task %s from %s to %s (retry=%d)",
                            task.task_id, failed_worker_id, w.worker_id, task.retry_count)

                return True
        return False

    def has_workers(self) -> bool:
        """Report whether at least one proxy remains eligible for routing."""
        with self._lock:
            if self._stopping:
                return False
            return any(w.is_ready() for w in self._workers.values())
