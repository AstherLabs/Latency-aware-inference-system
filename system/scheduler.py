"""
scheduler.py
============
Scheduler strategies for the latency-aware inference system.

Three strategies ship out of the box:

  fifo          – Process tasks strictly in arrival order.
  batching      – Collect up to batch_size tasks before dispatching.
  latency_aware – Prefer older buffered tasks; this does not guarantee a latency bound.

The Scheduler class wraps all three behind a single interface so the
API server can switch strategies at runtime via /config/scheduler.
"""

import logging
import threading
import time
from typing import List
from collections import deque

from .config import config
from .queue import TaskQueue
from .task import Task, TaskStatus
from .metrics import SCHEDULER_PENDING
from .health import LoopProgress

logger = logging.getLogger(__name__)

Batch = List[Task]


# ---------------------------------------------------------------------------
# Strategy implementations
# ---------------------------------------------------------------------------

class FIFOScheduler:
    """
    Emit one batch per tick containing up to batch_size tasks in
    the order they arrived.
    """

    def next_batch(self, queue: TaskQueue) -> Batch:
        """Read up to batch_size tasks in FIFO order, stopping on an empty-queue timeout."""
        batch: Batch = []
        while len(batch) < config.batch_size:
            task = queue.get(timeout=0.05)
            if task is None:
                break
            batch.append(task)
        return batch


class BatchingScheduler:
    """
    Block until a full batch is available (or a short timeout expires),
    then return the batch. Larger simulated batches amortize overhead at the
    cost of added queueing latency; GPU utilization is not measured.
    """
    _FILL_TIMEOUT = 0.1  # seconds to wait for a full batch

    def next_batch(self, queue: TaskQueue) -> Batch:
        """Collect a batch until it fills or the 100 ms fill window expires."""
        deadline = time.time() + self._FILL_TIMEOUT
        batch: Batch = []

        while len(batch) < config.batch_size and time.time() < deadline:
            task = queue.get(timeout=0.01)
            if task is not None:
                batch.append(task)

        return batch


class LatencyAwareScheduler:
    """
    Prioritise tasks that have been waiting longer than
    config.latency_threshold_ms.  Uses a local buffer so tasks are never
    re-enqueued (which would push them behind newly arriving tasks and cause
    starvation).  The buffer is always sorted oldest-first; stale tasks at
    the front are dispatched before fresh ones.
    """

    def __init__(self):
        """Keep a bounded local buffer of tasks awaiting dispatch."""
        self._buffer: List[Task] = []
        self._buffer_lock = threading.Lock()

    def discard(self, task):
        with self._buffer_lock:
            self._buffer = [item for item in self._buffer if item is not task]

    def discard_all(self):
        with self._buffer_lock:
            items, self._buffer = self._buffer, []
            return items

    def pending_count(self):
        with self._buffer_lock:
            return len(self._buffer)

    def take_matching(self, predicate, limit):
        with self._buffer_lock:
            items = [task for task in self._buffer if predicate(task)][:limit]
            selected = {id(task) for task in items}
            self._buffer = [task for task in self._buffer if id(task) not in selected]
            return items

    def next_batch(self, queue: TaskQueue) -> Batch:
        with self._buffer_lock:
            batch = self._next_batch(queue)
        if not batch:
            time.sleep(.01)
        return batch

    def _next_batch(self, queue: TaskQueue) -> Batch:
        # Keep buffer capped: only drain enough to top it up to batch_size*4
        """Drain into the local buffer and dispatch older tasks before newer ones."""
        cap = config.batch_size * 4
        if len(self._buffer) < cap:
            new_tasks = queue.drain(max_items=cap - len(self._buffer))
            self._buffer.extend(new_tasks)

        if not self._buffer:
            return []

        threshold = config.latency_threshold_ms
        old = [t for t in self._buffer if t.age_ms >= threshold]
        fresh = [t for t in self._buffer if t.age_ms < threshold]

        old.sort(key=lambda t: t.creation_time)
        fresh.sort(key=lambda t: t.creation_time)

        batch = (old + fresh)[: config.batch_size]

        batch_ids = {id(t) for t in batch}
        self._buffer = [t for t in self._buffer if id(t) not in batch_ids]

        return batch


# ---------------------------------------------------------------------------
# Unified Scheduler facade
# ---------------------------------------------------------------------------

class Scheduler:
    """
    Runs a background loop that continuously pulls tasks from the queue and
    forwards batches to a registered batch_handler callable.

    The active strategy is re-read from config on every tick, so live
    reconfiguration via /config/scheduler takes effect immediately.
    """

    def __init__(self, queue: TaskQueue, batch_handler):
        """Bind the shared queue to a dispatch callback and create scheduler strategies."""
        self._queue = queue
        self._handler = batch_handler
        self._owner = getattr(batch_handler, '__self__', None)
        self._pending = []
        self._retries = deque()
        self._retry_ids = set()
        self._pending_lock = threading.Lock()
        self._wakeup = threading.Event()
        if self._owner is not None and hasattr(self._owner, 'bind_scheduler'):
            self._owner.bind_scheduler(self.enqueue_retry, self._wakeup)
            self._handler = self._owner.try_submit
        SCHEDULER_PENDING.set_function(self.pending_count)
        self.progress = LoopProgress(config.loop_health_timeout_seconds)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        self._strategies = {
            "fifo": FIFOScheduler(),
            "batching": BatchingScheduler(),
            "latency_aware": LatencyAwareScheduler(),
        }

    def start(self) -> None:
        """Start the background batch assembly and dispatch loop."""
        self._stop.clear()
        self.progress.start()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="Scheduler"
        )
        self._thread.start()
        logger.info("Scheduler started")

    def stop(self, timeout: float = 5.0) -> None:
        """Signal shutdown and wait up to timeout seconds for dispatch to stop."""
        self._stop.set()
        self.progress.stop()
        self._wakeup.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        logger.info("Scheduler stopped")
        with self._pending_lock:
            abandoned = self._pending + list(self._retries)
            self._pending = []
            self._retries.clear()
            self._retry_ids.clear()
            for strategy in self._strategies.values():
                if hasattr(strategy, 'discard_all'):
                    abandoned.extend(strategy.discard_all())
        for task in abandoned:
            task.mark_failed('scheduler stopped', 'cancelled')

    def is_healthy(self):
        """Require a live thread and recently completed dispatch cycles."""
        return bool(self._thread and self._thread.is_alive()
                    and self.progress.snapshot()["healthy"])

    def discard_pending(self, task):
        """Release scheduler-owned waiting slots for a terminal ephemeral task."""
        with self._pending_lock:
            self._pending = [item for item in self._pending if item is not task]
            self._retries = deque(item for item in self._retries if item is not task)
            self._retry_ids.discard(task.task_id)
            for strategy in self._strategies.values():
                if hasattr(strategy, 'discard'):
                    strategy.discard(task)
        self._wakeup.set()

    def pending_count(self):
        with self._pending_lock:
            return (len(self._pending) + len(self._retries)
                    + sum(strategy.pending_count() for strategy in self._strategies.values()
                          if hasattr(strategy, 'pending_count')))

    def enqueue_retry(self, task, failed_worker_id):
        """Retain an already accepted task; this backlog admits no new requests."""
        with self._pending_lock:
            if self._stop.is_set():
                return False
            if task.task_id in self._retry_ids:
                return True
            if not task.prepare_retry(config.max_task_retries):
                return False
            self._retries.append(task)
            self._retry_ids.add(task.task_id)
        self._wakeup.set()
        return True

    @staticmethod
    def _kind(task):
        kind = task.input_data.get('kind', 'simulation') if isinstance(task.input_data, dict) else 'simulation'
        return kind if isinstance(kind, str) else '__invalid__'

    def _loop(self) -> None:
        """Read the configured strategy each iteration and submit nonempty batches."""
        while not self._stop.is_set():
            cycle_succeeded = True
            strategy = self._strategies.get(config.scheduler_strategy)
            if strategy is None:
                logger.warning("Unknown scheduler strategy: %s", config.scheduler_strategy)
                time.sleep(0.1)
                continue

            with self._pending_lock:
                batch = self._pending
                self._pending = []
                # Older accepted retries run before newly arrived work. A blocked
                # normal head is retained rather than repeatedly sent to the tail.
                while self._retries and len(batch) < config.batch_size:
                    batch.append(self._retries.popleft())
                blocked = {self._kind(task) for task in batch}
                selected = []
                for task in self._retries:
                    if self._kind(task) not in blocked and len(selected) < config.batch_size:
                        selected.append(task)
                selected_ids = {id(task) for task in selected}
                self._retries = deque(task for task in self._retries if id(task) not in selected_ids)
                batch.extend(selected)
            if batch:
                # Keep at most one waiting head batch per kind. Other model kinds
                # can use compatible workers while this head has no capacity.
                blocked_kinds = {self._kind(task) for task in batch}
                eligible = lambda task: self._kind(task) not in blocked_kinds
                extras = (strategy.take_matching(eligible, config.batch_size)
                          if hasattr(strategy, 'take_matching') else [])
                while len(extras) < config.batch_size:
                    task = self._queue.get_matching(eligible)
                    if task is None:
                        break
                    extras.append(task)
                batch.extend(extras)
            else:
                batch = strategy.next_batch(self._queue)
            # Deduplication belongs only to scheduler-owned work. Release it
            # before dispatch so a fast failing attempt can enqueue its retry.
            with self._pending_lock:
                for task in batch:
                    self._retry_ids.discard(task.task_id)
            if batch:
                live = []
                for task in batch:
                    if task.is_expired():
                        task.mark_failed('request deadline exceeded', 'deadline_exceeded')
                    if task.status not in (TaskStatus.DONE, TaskStatus.FAILED):
                        live.append(task)
                try:
                    deferred = self._handler(live) if live else []
                except Exception:
                    logger.exception("batch_handler raised an exception")
                    cycle_succeeded = False
                    deferred = live
                if deferred is None:
                    deferred = []  # Local WorkerPool accepts its complete batch.
                with self._pending_lock:
                    self._pending = deferred
                    self._retry_ids.update(task.task_id for task in deferred)
                if deferred:
                    # A capacity event avoids spinning; the timeout also handles
                    # expiry or discovery transitions with no capacity signal.
                    self._wakeup.wait(.05)
                    self._wakeup.clear()
            if cycle_succeeded:
                self.progress.beat()
