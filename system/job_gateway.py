"""Dispatch immutable SQL jobs through the existing queue and scheduler.

The SQL row is the authority; an in-memory task is only one leased attempt.
RPC disconnects are retried through SQL, never handed straight to another worker.
"""
import hashlib
import json
import logging
import math
import threading
import time
from datetime import datetime, timezone
from queue import Full

from .task import Task, TaskStatus
from .health import LoopProgress

logger = logging.getLogger(__name__)

RETRYABLE_ERRORS = frozenset({
    "rpc_timeout", "overloaded", "worker_busy", "already_running", "transport_error",
    "in_progress", "storage_unavailable", "socket_unavailable",
    "storage_error", "worker_unavailable", "worker_error",
})


class JobGateway:
    def __init__(self, store, queue, *, lease_seconds=120, renew_interval=30,
                 retry_seconds=5, poll_interval=.5, max_inflight=1,
                 rpc_timeout_seconds=300, scheduler_healthy=None, health_timeout_seconds=10):
        if lease_seconds <= 0 or renew_interval < 0 or renew_interval >= lease_seconds:
            raise ValueError("renew_interval must be nonnegative and shorter than lease_seconds")
        if max_inflight < 1 or retry_seconds <= 0 or poll_interval <= 0:
            raise ValueError("invalid job admission settings")
        if not math.isfinite(rpc_timeout_seconds) or rpc_timeout_seconds <= 0:
            raise ValueError("rpc_timeout_seconds must be positive and finite")
        self.rpc_timeout_seconds = rpc_timeout_seconds
        self.scheduler_healthy = scheduler_healthy or (lambda: True)
        self.progress = LoopProgress(health_timeout_seconds)
        self.store = store
        self.queue = queue
        self.lease_seconds = lease_seconds
        self.renew_interval = renew_interval
        self.retry_seconds = retry_seconds
        self.poll_interval = poll_interval
        self.max_inflight = max_inflight
        self._active = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._stop.clear()
        self.progress.start()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="DurableJobs")
        self._thread.start()

    def stop(self):
        # Leave accepted leases in SQL. Recovery takes over only after expiry.
        self._stop.set()
        self.progress.stop()
        if self._thread:
            self._thread.join(timeout=5)

    def submit(self, job_id):
        with self._lock:
            row = self.store.get(job_id)
            if row is None:
                raise KeyError(job_id)
            if (len(self._active) < self.max_inflight and not self._stop.is_set()
                    and self.scheduler_healthy()):
                claimed = self.store.claim(job_id=job_id, lease_seconds=self.lease_seconds)
                if claimed:
                    self._enqueue(claimed)
            return self.store.get(job_id)

    def _enqueue(self, row):
        if not self.scheduler_healthy():
            # Claim may have waited on SQL while the scheduler stopped progressing.
            # Leave the new SQL lease untouched for expiry-based recovery.
            return
        job_id, attempt_id = row["job_id"], row["attempt_id"]
        fingerprint = hashlib.sha256(json.dumps(row["spec"], sort_keys=True,
            separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()
        if fingerprint != row["spec_sha256"]:
            self.store.complete(job_id, attempt_id, error={"code": "invalid_spec", "message": "spec fingerprint mismatch"})
            return
        remaining = (row["deadline_at"] - datetime.now(timezone.utc)).total_seconds()
        task = Task(input_data={"kind": "synthx_v73", "spec": row["spec"], "attempt_id": attempt_id},
                    deadline=time.monotonic() + max(0, remaining),
                    rpc_timeout_seconds=self.rpc_timeout_seconds)
        try:
            self.queue.put(task, timeout=0)
        except Full:
            self.store.defer(job_id, attempt_id, {"code": "overloaded", "message": "task queue is full"}, self.retry_seconds)
            return
        self._active[job_id] = (task, attempt_id, time.monotonic())

    def poll(self):
        with self._lock:
            self.store.expire_deadlines()
            for job_id, (task, attempt_id, renewed) in list(self._active.items()):
                if not self.scheduler_healthy():
                    # Do not release SQL ownership early: old remote work may still run.
                    # Expiry plus attempt fencing governs takeover by another gateway.
                    logger.error("Scheduler progress lost; job %s awaits lease recovery", job_id)
                    task.mark_failed("scheduler progress lost; awaiting lease recovery", "lease_lost")
                    del self._active[job_id]
                    continue
                if task.is_expired():
                    task.mark_failed("job deadline exceeded", "deadline_exceeded")
                if task.status == TaskStatus.DONE:
                    self.store.complete(job_id, attempt_id, result=task.result)
                    del self._active[job_id]
                elif task.status == TaskStatus.FAILED:
                    error = {"code": task.error_code or "worker_error", "message": task.error or "inference failed"}
                    if error["code"] in RETRYABLE_ERRORS:
                        self.store.defer(job_id, attempt_id, error, self.retry_seconds)
                    else:
                        self.store.complete(job_id, attempt_id, error=error)
                    del self._active[job_id]
                elif time.monotonic() - renewed >= self.renew_interval:
                    if self.store.renew(job_id, attempt_id, self.lease_seconds):
                        self._active[job_id] = (task, attempt_id, time.monotonic())
                    else:
                        # Prevent a task waiting in the queue from executing a lost lease.
                        task.mark_failed("execution lease lost", "lease_lost")
                        del self._active[job_id]
            for _ in range(self.max_inflight - len(self._active)):
                if self._stop.is_set() or not self.scheduler_healthy():
                    break
                row = self.store.claim(lease_seconds=self.lease_seconds)
                if row is None:
                    break
                self._enqueue(row)

    def is_healthy(self):
        """Never acquire the SQL/work lock while serving a health probe."""
        return bool(self._thread and self._thread.is_alive()
                    and self.progress.snapshot()["healthy"])

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.poll()
                self.progress.beat()
            except Exception:
                # No task/result is discarded if SQL is temporarily unavailable.
                logger.exception("Durable job recovery failed; will retry")
            self._stop.wait(self.poll_interval)
