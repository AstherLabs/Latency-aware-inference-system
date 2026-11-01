"""
config.py
=========
System-wide configuration for the latency-aware inference system.
All tuneable parameters live here so experiments can override them
via the /config/* API endpoints without touching worker or scheduler code.
"""

import threading
import os
import math
from dataclasses import dataclass, field
from typing import Literal

SchedulerStrategy = Literal["fifo", "batching", "latency_aware"]


@dataclass
class SystemConfig:
    """
    Runtime-mutable configuration shared across server, scheduler, and workers.

    Updates and snapshots use a lock. Scheduler and worker code also read
    individual fields directly; a multi-field update is not an atomic snapshot
    for those readers.
    """
    # Inference settings
    batch_size: int = 4
    num_workers: int = 4
    inference_latency_ms: float = 60.0   # simulated base batch latency

    # Scheduling strategy: "fifo", "batching", or "latency_aware"
    scheduler_strategy: SchedulerStrategy = "fifo"

    # Failure injection (0.0 = never fail, 1.0 = always fail)
    failure_rate: float = 0.0

    # Latency-aware scheduler threshold: tasks older than this are expedited
    latency_threshold_ms: float = 150.0

    # Queue
    max_queue_size: int = 10_000
    max_sync_requests: int = int(os.getenv("MAX_SYNC_REQUESTS", "1024"))
    worker_max_outstanding_batches: int = int(os.getenv('WORKER_MAX_OUTSTANDING_BATCHES', '3'))
    max_task_retries: int = int(os.getenv('MAX_TASK_RETRIES', '3'))
    worker_recovery_backoff_seconds: float = float(os.getenv('WORKER_RECOVERY_BACKOFF_SECONDS', '1'))

    loop_health_timeout_seconds: float = float(os.getenv("LOOP_HEALTH_TIMEOUT_SECONDS", "10"))

    # Server
    host: str = "0.0.0.0"
    port: int = 8000

    # Internal lock — not part of the dataclass value
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False, compare=False)

    @staticmethod
    def _validate(values):
        for key in ('worker_max_outstanding_batches', 'max_task_retries', 'max_sync_requests'):
            if key in values and (type(values[key]) is not int or values[key] < 1):
                raise ValueError(f'{key} must be a positive integer')
        if 'loop_health_timeout_seconds' in values:
            value = values['loop_health_timeout_seconds']
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError('loop_health_timeout_seconds must be positive and finite')
        if 'worker_recovery_backoff_seconds' in values:
            value = values['worker_recovery_backoff_seconds']
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError('worker_recovery_backoff_seconds must be positive and finite')

    def __post_init__(self):
        self._validate(vars(self))

    def update(self, **kwargs) -> None:
        """Apply known configuration fields under the configuration lock."""
        with self._lock:
            self._validate(kwargs)
            for key, value in kwargs.items():
                if not hasattr(self, key):
                    raise ValueError(f"Unknown config key: {key}")
                setattr(self, key, value)

    def snapshot(self) -> dict:
        """Return a consistent copy of the exposed configuration fields."""
        with self._lock:
            return {
                "batch_size": self.batch_size,
                "num_workers": self.num_workers,
                "inference_latency_ms": self.inference_latency_ms,
                "scheduler_strategy": self.scheduler_strategy,
                "failure_rate": self.failure_rate,
                "latency_threshold_ms": self.latency_threshold_ms,
                "max_queue_size": self.max_queue_size,
                "max_sync_requests": self.max_sync_requests,
                "worker_max_outstanding_batches": self.worker_max_outstanding_batches,
                "max_task_retries": self.max_task_retries,
                "worker_recovery_backoff_seconds": self.worker_recovery_backoff_seconds,
                "loop_health_timeout_seconds": self.loop_health_timeout_seconds,
                "host": self.host,
                "port": self.port,
            }


# Module-level singleton shared by all components
config = SystemConfig()
