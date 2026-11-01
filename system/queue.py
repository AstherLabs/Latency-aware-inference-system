"""
queue.py
========
Thread-safe task queue for the inference system.

The TaskQueue sits between the API server (producers) and the scheduler
(consumer). Callers choose a put timeout; the HTTP API uses zero so a full queue
is rejected immediately with HTTP 503.
"""

import threading
import time
from queue import Queue, Full, Empty
from typing import List, Optional

from .task import Task


class _SelectiveQueue(Queue):
    def take_matching(self, predicate):
        """Remove the first eligible item without moving blocked items to the tail."""
        with self.mutex:
            for index, task in enumerate(self.queue):
                if predicate(task):
                    del self.queue[index]
                    self.not_full.notify()
                    return task
        return None


class TaskQueue:
    """
    Bounded FIFO queue with blocking put/get and bulk-drain support.

    The latency-aware strategy uses drain() to retrieve currently available
    tasks without waiting for a full batch.
    """

    def __init__(self, maxsize: int = 10_000):
        """Create a bounded FIFO queue and separate accounting for enqueued tasks."""
        self._q: Queue = _SelectiveQueue(maxsize=maxsize)
        self._total_enqueued = 0
        self._total_dequeued = 0
        self._lock = threading.Lock()

    def put(self, task: Task, timeout: float = 5.0) -> None:
        """
        Add a task to the queue.

        Raises:
            Full: If the queue is at capacity after `timeout` seconds.
        """
        self._q.put(task, block=True, timeout=timeout)
        with self._lock:
            self._total_enqueued += 1

    def get(self, timeout: float = 1.0) -> Optional[Task]:
        """
        Remove and return the next task.

        Returns None on timeout instead of raising Empty, so callers can
        loop cleanly with a stop-event check.
        """
        try:
            task = self._q.get(block=True, timeout=timeout)
            with self._lock:
                self._total_dequeued += 1
            return task
        except Empty:
            return None

    def get_matching(self, predicate) -> Optional[Task]:
        """Take one eligible task immediately, preserving FIFO order within each kind."""
        task = self._q.take_matching(predicate)
        if task is not None:
            with self._lock:
                self._total_dequeued += 1
        return task

    def discard(self, task: Task) -> None:
        """Remove an abandoned task by identity, releasing its admission queue slot."""
        self.get_matching(lambda candidate: candidate is task)

    def drain(self, max_items: int = 256) -> List[Task]:
        """
        Non-blocking bulk retrieval: return up to `max_items` tasks that
        are already in the queue right now.

        Used by the scheduler to assemble batches without blocking.
        """
        items: List[Task] = []
        for _ in range(max_items):
            try:
                items.append(self._q.get_nowait())
            except Empty:
                break
        with self._lock:
            self._total_dequeued += len(items)
        return items

    def qsize(self) -> int:
        """Return the queue's approximate current size."""
        return self._q.qsize()

    def empty(self) -> bool:
        """Return a point-in-time emptiness check, not a reservation."""
        return self._q.empty()

    @property
    def stats(self) -> dict:
        """Read queue size and cumulative enqueue/dequeue counters."""
        with self._lock:
            return {
                "queue_size": self._q.qsize(),
                "total_enqueued": self._total_enqueued,
                "total_dequeued": self._total_dequeued,
            }
