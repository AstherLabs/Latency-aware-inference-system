import asyncio
import hashlib

import pytest
from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient

from system.audio_jobs import AudioJobManager
from system.queue import TaskQueue
from tests.test_audio_jobs import ID, MODELS, completion, wait_state


def application(manager):
    from system.audio_api import create_router
    app = FastAPI()
    def authorize(authorization: str = Header(default='')):
        if authorization != 'Bearer service-secret':
            raise HTTPException(401, 'Service authentication required')
    app.include_router(create_router(lambda: manager, authorize))
    return app


@pytest.fixture
def manager(tmp_path):
    manager = AudioJobManager(tmp_path, TaskQueue(), 'worker:50061')
    manager.start()
    yield manager
    manager.stop()


@pytest.mark.parametrize('path,method', [('/audio/health','get'),
    ('/audio/requests/' + ID + '/' + MODELS[0], 'post'),
    ('/audio/jobs/' + MODELS[0], 'post'), ('/audio/jobs/' + ID, 'get'),
    ('/audio/jobs/' + ID + '/result', 'get'), ('/audio/jobs/' + ID + '/artifacts/wav', 'get')])
def test_all_routes_require_service_auth(manager, path, method):
    with TestClient(application(manager)) as client:
        assert getattr(client, method)(path).status_code == 401
    assert manager.queue.empty()


def test_submit_status_result_and_identity_conflict(manager):
    with TestClient(application(manager)) as client:
        headers = {'Authorization': 'Bearer service-secret', 'Content-Type': 'application/octet-stream'}
        route = '/audio/requests/' + ID + '/' + MODELS[0]
        response = client.post(route, content=b'audio', headers=headers)
        assert response.status_code == 202
        assert response.json()['job_id'] == ID
        assert response.headers['Retry-After'] == '1'
        assert client.get('/audio/jobs/' + ID, headers=headers).json()['state'] == 'queued'
        assert client.get('/audio/jobs/' + ID + '/result', headers=headers).status_code == 409
        task = manager.queue.get(timeout=2)
        task.mark_done(completion(task))
        wait_state(manager, ID, 'completed')
        assert client.get('/audio/jobs/' + ID + '/result', headers=headers).json() == completion(task)['result']
        assert client.post(route, content=b'other', headers=headers).status_code == 409
        generated = client.post('/audio/jobs/' + MODELS[1], content=b'new', headers=headers)
        assert generated.status_code == 202
        assert len(generated.json()['job_id']) == 32
        assert generated.json()['job_id'] != ID


def test_artifact_streams_protocol_bytes(manager, monkeypatch):
    import system.audio_jobs as jobs
    headers = {'Authorization': 'Bearer service-secret'}
    with TestClient(application(manager)) as client:
        client.post('/audio/requests/' + ID + '/' + MODELS[0], content=b'audio', headers=headers)
        task = manager.queue.get(timeout=2)
        task.mark_done(completion(task))
        wait_state(manager, ID, 'completed')
        calls = []
        def artifact(channel, model, request_id, kind):
            calls.append((model, request_id, kind))
            yield b'RIFF'
            yield b'wav-bytes'
        monkeypatch.setattr(jobs.audio_protocol, 'remote_artifact', artifact)
        response = client.get('/audio/jobs/' + ID + '/artifacts/wav', headers=headers)
        assert response.status_code == 200
        assert response.content == b'RIFFwav-bytes'
        assert response.headers['Content-Type'] == 'audio/wav'
        assert calls == [(MODELS[0], ID, 'wav')]


def test_api_drain_returns_retry_header_and_authenticated_get_remains(tmp_path):
    from system.deployment import DeploymentGate
    gate = DeploymentGate(tmp_path / 'drain')
    manager = AudioJobManager(tmp_path / 'jobs', TaskQueue(), 'worker:50061', deployment_gate=gate)
    manager.start()
    try:
        with TestClient(application(manager)) as client:
            headers = {'Authorization': 'Bearer service-secret'}
            response = client.post('/audio/requests/' + ID + '/' + MODELS[0], content=b'audio', headers=headers)
            assert response.status_code == 202
            gate.marker.write_text('drain')
            blocked = client.post('/audio/jobs/' + MODELS[0], content=b'new', headers=headers)
            assert blocked.status_code == 503
            assert blocked.headers['Retry-After'] == '30'
            assert blocked.json()['code'] == 'DEPLOYMENT_DRAINING'
            assert client.get('/audio/jobs/' + ID, headers=headers).status_code == 200
    finally:
        manager.stop()


def test_failed_storage_publication_returns_retryable_http_error(manager):
    import httpx
    state = manager.root / ID / 'state.json'
    receipt = manager.root / ID / 'original-upload-state.json'
    async def disk_fault():
        yield b'audio'
        state.rename(receipt)
        state.mkdir()
    async def run():
        transport = httpx.ASGITransport(app=application(manager), raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url='http://gateway') as client:
            response = await client.post('/audio/requests/' + ID + '/' + MODELS[0],
                content=disk_fault(), headers={'Authorization': 'Bearer service-secret'})
            assert response.status_code == 503
            assert response.headers['Retry-After'] == '2'
            assert response.json()['code'] == 'storage_unavailable'
            assert manager.status(ID)['durable'] is False
    try:
        asyncio.run(run())
    finally:
        if state.is_dir():
            state.rmdir()
            receipt.rename(state)


@pytest.mark.parametrize('before_first_chunk', [False, True])
def test_real_http_disconnect_cancels_blocked_grpc_artifact(tmp_path, before_first_chunk):
    from concurrent.futures import ThreadPoolExecutor
    import socket
    import threading
    import time
    import grpc
    import httpx
    import uvicorn
    from system.audio_protocol import CHECKPOINTS, execute_remote
    from system.audio_worker import AudioWorker
    from tests.test_audio_worker import ModelHTTP
    from tests.test_audio_jobs import submit

    disconnected, handler_finished, entered = threading.Event(), threading.Event(), threading.Event()
    class StallAfterFirstChunk(grpc.ServerInterceptor):
        def intercept_service(self, continuation, details):
            handler = continuation(details)
            if not details.method.endswith('/Artifact'):
                return handler
            def stream(request, context):
                context.add_callback(disconnected.set)
                source = handler.unary_stream(request, context)
                try:
                    entered.set()
                    if not before_first_chunk:
                        yield next(source)
                    while context.is_active():
                        disconnected.wait(.01)
                finally:
                    source.close()
                    handler_finished.set()
            return grpc.unary_stream_rpc_method_handler(stream,
                request_deserializer=handler.request_deserializer, response_serializer=handler.response_serializer)

    model = ModelHTTP()
    worker = AudioWorker(tmp_path / 'worker', origins={m:model.origin for m in CHECKPOINTS}, poll_interval=.01)
    rpc_server = grpc.server(ThreadPoolExecutor(8), interceptors=[StallAfterFirstChunk()])
    worker.add_to_server(rpc_server)
    port = rpc_server.add_insecure_port('127.0.0.1:0')
    rpc_server.start()
    channel = grpc.insecure_channel(f'127.0.0.1:{port}')
    manager = AudioJobManager(tmp_path / 'gateway', TaskQueue(), f'127.0.0.1:{port}')
    manager.start()
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    http_port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(application(manager), lifespan='off', log_level='critical', ws='none'))
    thread = threading.Thread(target=server.run, kwargs={'sockets':[listener]}, daemon=True)
    try:
        submit(manager)
        task = manager.queue.get(timeout=2)
        task.mark_done(execute_remote(channel, task.input_data, 3))
        wait_state(manager, ID, 'completed')
        thread.start()
        until = time.monotonic() + 3
        while not server.started:
            assert time.monotonic() < until
            threading.Event().wait(.01)
        if before_first_chunk:
            connection = socket.create_connection(('127.0.0.1', http_port), timeout=3)
            connection.sendall((f'GET /audio/jobs/{ID}/artifacts/wav HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer service-secret\r\n\r\n').encode())
            assert entered.wait(2)
            connection.close()
        else:
            with httpx.Client(timeout=3) as client:
                with client.stream('GET', f'http://127.0.0.1:{http_port}/audio/jobs/{ID}/artifacts/wav',
                        headers={'Authorization':'Bearer service-secret'}) as response:
                    assert response.status_code == 200
                    assert next(response.iter_bytes()) == b'RIFFfake'
        assert disconnected.wait(2), 'HTTP disconnect left the gRPC artifact RPC active'
        assert handler_finished.wait(2), 'Worker artifact generator retained its resources'
    finally:
        channel.close()
        rpc_server.stop(0).wait()
        server.should_exit = True
        if thread.is_alive():
            thread.join(timeout=3)
        listener.close()
        manager.stop()
        worker.close()
        model.close()
