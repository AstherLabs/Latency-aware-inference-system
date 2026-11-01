"""
worker.py
=========
Worker pool for processing inference batches.

Each Worker runs in its own thread and simulates model inference with a
configurable per-item latency.  A failure_rate in (0, 1] causes the worker
to raise an exception on that fraction of batches, enabling the failure
injection experiments.

The WorkerPool manages a fixed number of workers and exposes a single
submit(batch) method that the scheduler calls.
"""

import logging
import random
import threading
import time
from typing import List

from .config import config
from .task import Task, TaskStatus

logger = logging.getLogger(__name__)

Batch = List[Task]


class Worker:
    """
    Processes one batch at a time in a dedicated thread.

    Inference uses a fixed simulated batch overhead plus a small per-item cost.
    Each task returns x squared and the worker identity.
    """

    def __init__(self, worker_id: int):
        """Create the local simulation worker and its private batch queue."""
        self.worker_id = worker_id
        self._batch_queue: List[Batch] = []
        self._lock = threading.Lock()
        self._has_work = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name=f"Worker-{worker_id}"
        )
        self._processed = 0

    def start(self) -> None:
        """Start the worker's background processing thread."""
        self._stop.clear()
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Wake the thread and wait up to timeout seconds for it to stop."""
        self._stop.set()
        self._has_work.set()
        self._thread.join(timeout=timeout)

    def submit(self, batch: Batch) -> None:
        """Append a batch to this local worker's unbounded queue."""
        with self._lock:
            self._batch_queue.append(batch)
        self._has_work.set()

    def discard_pending(self, task):
        """Release abandoned local queue entries without interrupting active work."""
        with self._lock:
            batches = [[item for item in batch if item is not task] for batch in self._batch_queue]
            self._batch_queue = [batch for batch in batches if batch]

    def busy(self) -> bool:
        """Report queued work; unlike the gRPC proxy, this excludes active processing."""
        with self._lock:
            return bool(self._batch_queue)

    def _loop(self) -> None:
        """Process queued batches serially in the local simulation thread."""
        while not self._stop.is_set():
            self._has_work.wait(timeout=0.1)
            self._has_work.clear()

            while True:
                with self._lock:
                    if not self._batch_queue:
                        break
                    batch = self._batch_queue.pop(0)
                self._process(batch)

    def _process(self, batch: Batch) -> None:
        """Apply batch failure injection and calculate the simulated x-squared output."""
        live = []
        for task in batch:
            if task.status in (TaskStatus.DONE, TaskStatus.FAILED):
                continue
            if task.is_expired():
                task.mark_failed('request deadline exceeded', 'deadline_exceeded')
                continue
            live.append(task)
        batch = live
        if not batch:
            return
        for task in batch:
            task.mark_processing()

        # Simulate failure injection
        if config.failure_rate > 0 and random.random() < config.failure_rate:
            error_msg = f"Worker-{self.worker_id}: injected failure"
            logger.debug(error_msg)
            for task in batch:
                task.mark_failed(error_msg)
            return

        # Fixed overhead (GPU setup) + small per-item cost
        # This makes larger batches more efficient, like real ML inference
        per_item_ms = config.inference_latency_ms * 0.05
        total_ms = config.inference_latency_ms + len(batch) * per_item_ms
        time.sleep(total_ms / 1000.0)

        for task in batch:
            try:
                x = task.input_data.get("x", 0) if isinstance(task.input_data, dict) else 0
                task.mark_done({"prediction": x * x, "worker_id": self.worker_id})
            except Exception as exc:
                task.mark_failed(str(exc))

        self._processed += len(batch)


class WorkerPool:
    """
    Fixed-size pool of Worker instances.

    submit(batch) selects a worker round-robin, preferring empty queues; if all workers are busy the current worker still accepts
    it (workers queue internally).
    """

    def __init__(self):
        """Initialize the fixed local worker pool and round-robin index."""
        self._workers: List[Worker] = []
        self._rr_index = 0
        self._lock = threading.Lock()

    def start(self) -> None:
        """Create and start the configured number of local workers."""
        n = config.num_workers
        self._workers = [Worker(i) for i in range(n)]
        for w in self._workers:
            w.start()
        logger.info("WorkerPool started with %d workers", n)

    def stop(self, timeout: float = 5.0) -> None:
        """Stop each local worker with a bounded join."""
        for w in self._workers:
            w.stop(timeout=timeout)
        logger.info("WorkerPool stopped")

    def discard_pending(self, task):
        """Purge cancelled work in the local simulator as well as gRPC mode."""
        with self._lock:
            workers = list(self._workers)
        for worker in workers:
            worker.discard_pending(task)

    def submit(self, batch: Batch) -> None:
        """Route a batch round-robin, preferring workers without queued work."""
        if not self._workers:
            for task in batch:
                task.mark_failed("no workers available")
            return

        with self._lock:
            # Round-robin across idle workers; fall back to all workers
            idle = [w for w in self._workers if not w.busy()]
            pool = idle if idle else self._workers
            worker = pool[self._rr_index % len(pool)]
            self._rr_index += 1

        worker.submit(batch)
