"""Bound ephemeral HTTP waiters and bridge worker completion into asyncio."""
import asyncio
import threading

from fastapi import HTTPException
from starlette.responses import JSONResponse

from .config import config
from .metrics import SYNC_ACTIVE, SYNC_REJECTED, SYNC_CANCELLATIONS
from .task import TaskStatus


class SyncAdmissionMiddleware:
    """Reject excess synchronous handlers before parsing request bodies.

    The limit is per API process, shared by prediction and read-only text routes.
    It does not limit TCP connections or replace ingress body/time limits.
    Durable job submission and health/metrics routes are excluded.
    """
    def __init__(self, app):
        self.app = app
        self._active = 0
        self._lock = threading.Lock()

    @property
    def active(self):
        with self._lock:
            return self._active

    async def __call__(self, scope, receive, send):
        if (scope['type'] != 'http' or scope['method'] != 'POST'
                or scope['path'] not in ('/predict', '/text/tasks')):
            return await self.app(scope, receive, send)
        with self._lock:
            admitted = self._active < config.max_sync_requests
            if admitted:
                self._active += 1
                SYNC_ACTIVE.inc()
        if not admitted:
            SYNC_REJECTED.inc()
            response = JSONResponse({'detail': 'synchronous request capacity reached'},
                                    status_code=503, headers={'Retry-After': '1'})
            return await response(scope, receive, send)
        try:
            return await self.app(scope, receive, send)
        finally:
            with self._lock:
                self._active -= 1
                SYNC_ACTIVE.dec()


async def wait_for_task(task, request, cleanup):
    """Race completion/deadline against disconnect, without periodic status polling."""
    async def disconnected():
        # FastAPI has consumed the JSON body before entering the endpoint.
        while True:
            message = await request.receive()
            if message['type'] == 'http.disconnect':
                return

    finished = asyncio.create_task(task.wait_finished(timeout=task.remaining_seconds()))
    disconnect = asyncio.create_task(disconnected())
    try:
        done, _ = await asyncio.wait((finished, disconnect), return_when=asyncio.FIRST_COMPLETED)
        if finished in done:
            await finished
        else:
            await disconnect
            if task.status not in (TaskStatus.DONE, TaskStatus.FAILED):
                SYNC_CANCELLATIONS.inc()
            task.mark_failed('caller disconnected', 'cancelled')
            raise HTTPException(status_code=499, detail='caller disconnected')
    except TimeoutError:
        task.mark_failed('request deadline exceeded', 'deadline_exceeded')
    except asyncio.CancelledError:
        if task.status not in (TaskStatus.DONE, TaskStatus.FAILED):
            SYNC_CANCELLATIONS.inc()
        task.mark_failed('caller cancelled', 'cancelled')
        raise
    finally:
        finished.cancel()
        disconnect.cancel()
        if task.status == TaskStatus.FAILED:
            cleanup(task)
        await asyncio.gather(finished, disconnect, return_exceptions=True)
