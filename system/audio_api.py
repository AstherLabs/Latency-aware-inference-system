"""Authenticated asynchronous HTTP compatibility surface for scheduled audio."""
from itertools import chain
import uuid

import anyio

from fastapi import APIRouter, Depends, Request
from starlette.responses import JSONResponse, StreamingResponse

from .audio_jobs import AudioJobError
from .audio_protocol import AudioWorkerError



def _cancel_artifact(source):
    close = getattr(source, 'cancel', None) or getattr(source, 'close', None)
    if close:
        close()


async def _first_artifact_chunk(source, request):
    disconnected, failure, first = False, None, None
    async with anyio.create_task_group() as group:
        async def disconnect():
            nonlocal disconnected
            while (await request.receive())['type'] != 'http.disconnect':
                pass
            disconnected = True
            _cancel_artifact(source)
        group.start_soon(disconnect)
        try:
            first = await anyio.to_thread.run_sync(lambda: next(source, None))
        except AudioWorkerError as exc:
            failure = exc
        finally:
            group.cancel_scope.cancel()
    if disconnected:
        raise AudioJobError(499, 'client_disconnected', 'Artifact client disconnected')
    if failure:
        raise failure
    return first


class _ArtifactResponse(StreamingResponse):
    """Cancel the gRPC read before waiting for its blocked iterator thread."""
    def __init__(self, content, *, source, **kwargs):
        super().__init__(content, **kwargs)
        self._source = source
        self._closed = False

    def _close_source(self):
        if self._closed:
            return
        self._closed = True
        _cancel_artifact(self._source)

    async def __call__(self, scope, receive, send):
        try:
            async with anyio.create_task_group() as group:
                async def disconnect():
                    await self.listen_for_disconnect(receive)
                    self._close_source()
                    group.cancel_scope.cancel()
                group.start_soon(disconnect)
                try:
                    await self.stream_response(send)
                except AudioWorkerError:
                    if not self._closed:
                        raise
                except OSError:
                    # ASGI 2.4 servers can report a closed connection via send.
                    self._close_source()
                finally:
                    self._close_source()
                    group.cancel_scope.cancel()
        finally:
            self._close_source()
        if self.background is not None:
            await self.background()


def create_router(manager_provider, authorize_dependency):
    router = APIRouter(prefix='/audio', dependencies=[Depends(authorize_dependency)])

    def manager():
        value = manager_provider()
        if value is None:
            raise AudioJobError(503, 'unavailable', 'Audio gateway is disabled')
        return value

    def error_response(exc):
        if isinstance(exc, AudioJobError):
            return JSONResponse({'code': exc.code, 'message': str(exc), 'error': str(exc)},
                                status_code=exc.status_code, headers=exc.headers)
        return JSONResponse({'code': exc.code, 'message': str(exc), 'error': str(exc)},
                            status_code=503 if exc.retryable else 502,
                            headers={'Retry-After': '2'} if exc.retryable else {})

    async def submit(request, model, request_id):
        try:
            accepted = await manager().submit(model, request_id, request.stream())
            return JSONResponse(accepted, status_code=202, headers={'Retry-After': '1'})
        except (AudioJobError, AudioWorkerError) as exc:
            return error_response(exc)
        except OSError:
            return error_response(AudioJobError(503, 'storage_unavailable', 'Audio upload storage is temporarily unavailable', {'Retry-After': '2'}))

    @router.post('/requests/{request_id}/{model}')
    async def submit_request(request: Request, request_id: str, model: str):
        return await submit(request, model, request_id)

    @router.post('/jobs/{model}')
    async def submit_job(request: Request, model: str):
        return await submit(request, model, uuid.uuid4().hex)

    @router.get('/jobs/{request_id}')
    def status(request_id: str):
        try:
            return manager().status(request_id)
        except AudioJobError as exc:
            return error_response(exc)

    @router.get('/jobs/{request_id}/result')
    def result(request_id: str):
        try:
            return manager().result(request_id)
        except AudioJobError as exc:
            return error_response(exc)

    @router.get('/jobs/{request_id}/artifacts/{kind}')
    async def artifact(request: Request, request_id: str, kind: str):
        chunks = None
        try:
            selected = manager()
            chunks = iter(selected.artifact(request_id, kind))
            # Resolve upstream errors before sending successful HTTP headers.
            first = await _first_artifact_chunk(chunks, request)
            if first is None:
                raise AudioWorkerError('invalid_artifact', 'Audio artifact is empty')
            result = selected.result(request_id)
            headers = {'Content-Disposition': f'attachment; filename="{request_id}.{kind}"'}
            if result.get(kind + '_sha256'):
                headers['X-Artifact-SHA256'] = result[kind + '_sha256']
            return _ArtifactResponse(chain([first], chunks), source=chunks, headers=headers,
                                     media_type='audio/wav' if kind == 'wav' else 'application/octet-stream')
        except (AudioJobError, AudioWorkerError) as exc:
            if chunks is not None:
                _cancel_artifact(chunks)
            return error_response(exc)

    @router.get('/health')
    def health():
        try:
            return manager().health()
        except AudioJobError as exc:
            return error_response(exc)

    return router
