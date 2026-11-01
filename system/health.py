"""Monotonic loop progress, independent of scheduler and gateway work locks."""
import math
import threading
import time


class LoopProgress:
    """A successful cycle proves progress; thread existence alone does not."""
    def __init__(self, timeout_seconds=10):
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("loop health timeout must be positive and finite")
        self.timeout_seconds = timeout_seconds
        self._lock = threading.Lock()
        self._last = None
        self._running = False

    def start(self):
        with self._lock:
            self._running = True
            self._last = time.monotonic()

    def stop(self):
        with self._lock:
            self._running = False

    def beat(self):
        with self._lock:
            self._last = time.monotonic()

    def snapshot(self):
        with self._lock:
            age = None if self._last is None else max(0., time.monotonic() - self._last)
            return {"healthy": self._running and age is not None and age < self.timeout_seconds,
                    "last_cycle_age_seconds": age, "timeout_seconds": self.timeout_seconds}
