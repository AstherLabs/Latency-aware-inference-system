"""
task.py
=======
Core Task data structure for the inference system.

A Task is the unit of work: it carries the client's input, tracks its
lifecycle (pending → processing → done/failed), and stores the result
so the API server can return it to the caller.
"""

import time
import asyncio
import uuid
import threading
from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum


class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


@dataclass
class Task:
    """
    Represents a single inference request travelling through the system.

    Attributes:
        task_id:       Unique identifier (UUID4 string).
        input_data:    Raw payload from the client (arbitrary dict).
        creation_time: Unix timestamp of when the task entered the queue.
        status:        Current lifecycle stage.
        result:        Inference output; populated by the worker on success.
        error:         Error message; populated by the worker on failure.
        error_code:    Failure category used by the API to select an HTTP status.
        deadline:      API-process monotonic deadline; never sent as an absolute value.
        retry_count:   Accepted central retries; legacy handoff reserves before admission.
        start_time:    Unix timestamp when a worker began processing.
        end_time:      Unix timestamp when processing completed (success or fail).
    """
    input_data: Any
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    creation_time: float = field(default_factory=time.time)
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[Any] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    deadline: Optional[float] = None
    rpc_timeout_seconds: Optional[float] = None
    retry_count: int = 0
    retry_pending: bool = False
    _lock: Any = field(
        default_factory=threading.Lock,
        init=False,
        repr=False,
        compare=False,
    )

    _waiters: list = field(default_factory=list, init=False, repr=False, compare=False)

    async def wait_finished(self, timeout=None) -> None:
        """Await a terminal state without polling; unregister on timeout or cancellation."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        entry = (loop, future)
        with self._lock:
            if self.status in (TaskStatus.DONE, TaskStatus.FAILED):
                return
            self._waiters.append(entry)
        try:
            await asyncio.wait_for(future, timeout)
        finally:
            with self._lock:
                if entry in self._waiters:
                    self._waiters.remove(entry)

    @staticmethod
    def _notify_waiters(waiters) -> None:
        """Schedule loop-owned future updates outside the task state lock."""
        def resolve(future):
            if not future.done():
                future.set_result(None)
        for loop, future in waiters:
            try:
                loop.call_soon_threadsafe(resolve, future)
            except RuntimeError:
                # A caller's event loop may have closed during process shutdown.
                pass

    @property
    def age_ms(self) -> float:
        """Milliseconds since the task was created."""
        return (time.time() - self.creation_time) * 1000

    @property
    def processing_latency_ms(self) -> Optional[float]:
        """Elapsed time from first processing start to completion, including any retry wait."""
        if self.start_time is None or self.end_time is None:
            return None
        return (self.end_time - self.start_time) * 1000

    def remaining_seconds(self) -> Optional[float]:
        """Return the remaining monotonic budget, or None for an unbounded task."""
        if self.deadline is None:
            return None
        return max(0.0, self.deadline - time.monotonic())

    def is_expired(self) -> bool:
        """Check whether the task has exhausted its original time budget."""
        remaining = self.remaining_seconds()
        return remaining is not None and remaining <= 0

    def mark_processing(self) -> None:
        """Record the first start time without reopening a terminal task."""
        with self._lock:
            if self.status in (TaskStatus.DONE, TaskStatus.FAILED):
                return

            if self.start_time is None:
                self.start_time = time.time()

            self.status = TaskStatus.PROCESSING

    def mark_done(self, result: Any) -> None:
        """Publish a successful result atomically; ignore late terminal updates."""
        with self._lock:
            if self.status in (TaskStatus.DONE, TaskStatus.FAILED):
                return

            self.result = result
            self.end_time = time.time()
            self.status = TaskStatus.DONE
            waiters, self._waiters = self._waiters, []
        self._notify_waiters(waiters)

    def mark_failed(self, error: str, error_code: Optional[str] = None) -> None:
        """Publish an error atomically; preserve any existing terminal result."""
        with self._lock:
            if self.status in (TaskStatus.DONE, TaskStatus.FAILED):
                return
            self.error = error
            self.error_code = error_code
            self.end_time = time.time()
            self.status = TaskStatus.FAILED
            waiters, self._waiters = self._waiters, []
        self._notify_waiters(waiters)

    def to_dict(self) -> dict:
        """Build a task summary for inspection and experiment output."""
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "age_ms": round(self.age_ms, 2),
            "processing_latency_ms": (
                round(self.processing_latency_ms, 2)
                if self.processing_latency_ms is not None
                else None
            ),
        }

    def try_reserve_retry(self) -> bool:
        """Reserve at most one retry for a nonterminal task with time remaining."""
        with self._lock:
            if self.status in (TaskStatus.DONE, TaskStatus.FAILED):
                return False
            if self.is_expired() or self.retry_count >= 1:
                return False
            self.retry_count += 1
            return True

    def prepare_retry(self, max_retries: int) -> bool:
        """Return accepted work to central scheduling without spending an attempt."""
        with self._lock:
            if self.status in (TaskStatus.DONE, TaskStatus.FAILED) or self.is_expired():
                return False
            if self.retry_count >= max_retries:
                return False
            self.retry_pending = True
            self.status = TaskStatus.PENDING
            return True

    def accept_retry(self, max_retries: int) -> bool:
        """Consume retry budget only inside an accepting worker's capacity lock."""
        with self._lock:
            if self.status in (TaskStatus.DONE, TaskStatus.FAILED) or self.is_expired():
                return False
            if self.retry_pending:
                if self.retry_count >= max_retries:
                    return False
                self.retry_count += 1
                self.retry_pending = False
            return True
