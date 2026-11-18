"""Real gRPC, local HTTP and durable files; no GPU/model imports."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time

import grpc
import pytest


def sha(data):
    return hashlib.sha256(data).hexdigest()


class ModelHTTP:
    def __init__(self):
        self.calls = []
        self.jobs = {}
        self.release = threading.Event()
        self.release.set()
        self.corrupt = False
        self.bad_identity = False
        self.drop_submission = False
        self.submissions = 0
        self.post_status = None
        self.bad_job_id = False
        self.status_override = None
        self.result_override = {}
        self.health_checkpoints = None
        self.health_omit_checkpoints = False
        self.nonfinite_result = False
        self.artifact_stall = False
        self.artifact_started = threading.Event()
        self.artifact_release = threading.Event()
        self.artifact_disconnected = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def send(self, document, code=200):
                data = document if isinstance(document, bytes) else json.dumps(document, separators=(',', ':')).encode()
                self.send_response(code)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                from system.audio_protocol import CHECKPOINTS
                owner.submissions += 1
                if owner.post_status:
                    self.rfile.read(int(self.headers['Content-Length']))
                    return self.send({'detail': 'rejected'}, owner.post_status)
                assert self.path.startswith('/requests/'), self.path
                _, _, key, model = self.path.split('/')
                data = self.rfile.read(int(self.headers['Content-Length']))
                identity = (model, sha(data))
                if key in owner.jobs and owner.jobs[key]['identity'] != identity:
                    return self.send({'detail': 'conflict'}, 409)
                if key not in owner.jobs:
                    owner.calls.append((key, model))
                    owner.jobs[key] = {'identity': identity, 'result': {
                        'model': model, 'input_sha256': sha(data),
                        'checkpoint_sha256': CHECKPOINTS[model],
                        'document': {'_schema': 'unified-synth-preset-v3', 'origin': {'synth': 'vital'}},
                        'vital_sha256': sha(b'{"settings":{}}'), 'wav_sha256': sha(b'RIFFfake'),
                        'extra': {'preserve': True}}}
                if owner.drop_submission:
                    owner.drop_submission = False
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                self.send({'job_id': 'e' * 32 if owner.bad_job_id else key, 'model': model, 'state': 'running'}, 202)

            def do_GET(self):
                from system.audio_protocol import CHECKPOINTS
                if self.path == '/health':
                    health = {'models': list(CHECKPOINTS)}
                    if not owner.health_omit_checkpoints:
                        health['checkpoints'] = CHECKPOINTS if owner.health_checkpoints is None else owner.health_checkpoints
                    return self.send(health)
                parts = self.path.split('/')
                key = parts[2]
                if key not in owner.jobs:
                    return self.send({'detail': 'missing'}, 404)
                job = owner.jobs[key]
                if len(parts) == 3:
                    return self.send({'job_id': key, 'model': job['identity'][0],
                                      'state': owner.status_override or ('completed' if owner.release.is_set() else 'running')})
                if parts[3] == 'result':
                    result = dict(job['result'])
                    if owner.bad_identity:
                        result['checkpoint_sha256'] = 'f' * 64
                    result.update(owner.result_override)
                    if owner.nonfinite_result:
                        return self.send(json.dumps(result, separators=(',', ':'))[:-1].encode() + b',"number":1e9999}')
                    return self.send(result)
                data = b'{"settings":{}}' if parts[4] == 'vital' else b'RIFFfake'
                if owner.artifact_stall:
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(data)))
                    self.end_headers()
                    self.wfile.write(data[:1]); self.wfile.flush()
                    owner.artifact_started.set()
                    self.connection.settimeout(.05)
                    while not owner.artifact_release.is_set():
                        try:
                            if not self.connection.recv(1):
                                owner.artifact_disconnected.set()
                                return
                        except TimeoutError:
                            pass
                    try:
                        self.wfile.write(data[1:])
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    return
                self.send(b'corrupt' if owner.corrupt else data)

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.origin = 'http://127.0.0.1:' + str(self.server.server_port)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


@pytest.fixture
def http_model():
    model = ModelHTTP()
    yield model
    model.close()


def start_worker(root, http_model):
    from system.audio_protocol import CHECKPOINTS
    from system.audio_worker import AudioWorker
    worker = AudioWorker(root, origins={m: http_model.origin for m in CHECKPOINTS}, poll_interval=.02, http_timeout=1)
    server = grpc.server(ThreadPoolExecutor(max_workers=8))
    worker.add_to_server(server)
    port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    channel = grpc.insecure_channel(f'127.0.0.1:{port}')
    grpc.channel_ready_future(channel).result(timeout=3)
    return worker, server, channel


def finish(running):
    worker, server, channel = running
    channel.close()
    server.stop(0).wait()
    worker.close()


def payload(tmp_path, model='vital_v7_3', key='a' * 32, data=b'audio' * 20000):
    from system.audio_protocol import CHECKPOINTS, KIND
    path = tmp_path / (key + '.audio')
    path.write_bytes(data)
    return dict(kind=KIND, model=model, request_id=key, input_path=str(path), input_sha256=sha(data),
                input_size=len(data), checkpoint_sha256=CHECKPOINTS[model])


@pytest.mark.parametrize('model', ['vital_v7_3', 'vital_v7_5', 'vital_v7_5_chord'])
def test_streams_all_current_models_and_replays_without_compute(tmp_path, http_model, model):
    from system.audio_protocol import execute_remote, remote_artifact, probe_remote
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        task = payload(tmp_path, model)
        result = execute_remote(running[2], task, 5)
        assert result['model'] == model and result['state'] == 'completed'
        assert result['result']['extra'] == {'preserve': True}
        assert execute_remote(running[2], task, 5) == result
        assert http_model.calls == [('a' * 32, model)]
        assert b''.join(remote_artifact(running[2], model, task['request_id'], 'wav')) == b'RIFFfake'
        assert probe_remote(running[2])['ready'] is True
    finally:
        finish(running)


def wire_execute(channel, header, chunks=()):
    from system.audio_protocol import SERVICE
    return json.loads(channel.stream_unary('/' + SERVICE + '/Execute')(iter([header, *chunks]), timeout=3))


def wire_metadata(tmp_path):
    task = payload(tmp_path, data=b'audio')
    return {k: v for k, v in task.items() if k not in {'input_path', 'kind'}} | {'version': 1}


@pytest.mark.parametrize('change,code', [
    ({'model': 'legacy'}, 'unknown_model'),
    ({'model': []}, 'unknown_model'),
    ({'request_id': '../evil'}, 'invalid_metadata'),
    ({'version': True}, 'invalid_metadata'),
    ({'input_size': 209715201}, 'invalid_size'),
    ({'input_size': True}, 'invalid_size'),
    ({'checkpoint_sha256': '0' * 64}, 'checkpoint_mismatch'),
    ({'input_path': '/etc/passwd'}, 'invalid_metadata'),
])
def test_rejects_bad_metadata_without_http(tmp_path, http_model, change, code):
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        metadata = wire_metadata(tmp_path) | change
        response = wire_execute(running[2], json.dumps(metadata).encode(), [b'audio'])
        assert response['error']['code'] == code
        assert not http_model.calls
    finally:
        finish(running)


@pytest.mark.parametrize('header,chunks,code', [
    (b'{', [], 'invalid_metadata'),
    (b' ' * 8193, [], 'invalid_metadata'),
    (None, [b'other'], 'input_mismatch'),
    (None, [b'audio!'], 'invalid_size'),
    (None, [b'x' * 65537], 'invalid_chunk'),
])
def test_rejects_malformed_and_changed_streams(tmp_path, http_model, header, chunks, code):
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        response = wire_execute(running[2], header if header is not None else json.dumps(wire_metadata(tmp_path)).encode(), chunks)
        assert response['error']['code'] == code
        assert not http_model.calls
        assert not list((tmp_path / 'worker').glob('upload-*'))
    finally:
        finish(running)


def test_cross_model_and_input_replay_rejected(tmp_path, http_model):
    from system.audio_protocol import AudioWorkerError, execute_remote
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        execute_remote(running[2], payload(tmp_path), 3)
        for arguments in [{'model': 'vital_v7_5'}, {'data': b'changed'}]:
            task = payload(tmp_path, **arguments)
            with pytest.raises(AudioWorkerError) as error:
                execute_remote(running[2], task, 3)
            assert error.value.code == 'identity_conflict'
        assert len(http_model.calls) == 1
    finally:
        finish(running)


def wait_until(predicate, timeout=3):
    end = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < end, 'condition did not become true'
        time.sleep(.01)


def test_caller_loss_and_worker_restart_keep_previous_gpu_slot(tmp_path, http_model):
    from system.audio_protocol import AudioWorkerError, execute_remote
    http_model.release.clear()
    root = tmp_path / 'worker'
    first = payload(tmp_path)
    second = payload(tmp_path, model='vital_v7_5_chord', key='b' * 32)
    running = start_worker(root, http_model)
    with pytest.raises(AudioWorkerError):
        execute_remote(running[2], first, .15)
    wait_until(lambda: len(http_model.calls) == 1)
    finish(running)
    running = start_worker(root, http_model)
    pool = ThreadPoolExecutor(2)
    try:
        replay = pool.submit(execute_remote, running[2], first, 5)
        following = pool.submit(execute_remote, running[2], second, 5)
        time.sleep(.2)
        assert len(http_model.calls) == 1
        assert not replay.done() and not following.done()
        http_model.release.set()
        assert replay.result(3)['model'] == 'vital_v7_3'
        assert following.result(3)['model'] == 'vital_v7_5_chord'
        assert http_model.calls == [('a' * 32, 'vital_v7_3'), ('b' * 32, 'vital_v7_5_chord')]
    finally:
        http_model.release.set()
        pool.shutdown()
        finish(running)


def test_artifact_corruption_never_yields_bytes(tmp_path, http_model):
    from system.audio_protocol import AudioWorkerError, execute_remote, remote_artifact
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        execute_remote(running[2], payload(tmp_path), 3)
        http_model.corrupt = True
        received = []
        with pytest.raises(AudioWorkerError) as error:
            for chunk in remote_artifact(running[2], 'vital_v7_3', 'a' * 32, 'vital'):
                received.append(chunk)
        assert received == []
        assert error.value.code == 'artifact_mismatch'
        assert error.value.retryable is False
    finally:
        finish(running)


def test_result_checkpoint_mismatch_fails_after_actual_completion(tmp_path, http_model):
    from system.audio_protocol import AudioWorkerError, execute_remote
    running = start_worker(tmp_path / 'worker', http_model)
    http_model.bad_identity = True
    try:
        with pytest.raises(AudioWorkerError) as error:
            execute_remote(running[2], payload(tmp_path), 3)
        assert error.value.code == 'result_mismatch'
    finally:
        finish(running)


def test_shared_client_returns_structured_hash_error(tmp_path, http_model):
    from system.audio_protocol import AudioWorkerError, execute_remote
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        task = payload(tmp_path)
        task['input_sha256'] = '0' * 64
        with pytest.raises(AudioWorkerError) as error:
            execute_remote(running[2], task, 3)
        assert error.value.code == 'input_mismatch'
        assert error.value.retryable is False
        assert not http_model.calls
    finally:
        finish(running)


def test_startup_removes_only_abandoned_partial_uploads(tmp_path, http_model):
    root = tmp_path / 'worker'
    root.mkdir()
    partial = root / 'upload-interrupted'
    partial.write_bytes(b'partial' * 100)
    unrelated = root / 'operator-receipt.txt'
    unrelated.write_text('keep')
    running = start_worker(root, http_model)
    try:
        assert not partial.exists()
        assert unrelated.read_text() == 'keep'
    finally:
        finish(running)


def test_lost_http_submission_response_reuses_only_stable_request_route(tmp_path, http_model):
    from system.audio_protocol import execute_remote
    http_model.drop_submission = True
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        response = execute_remote(running[2], payload(tmp_path), 3)
        assert response['state'] == 'completed'
        assert http_model.submissions == 2
        assert http_model.calls == [('a' * 32, 'vital_v7_3')]
    finally:
        finish(running)


def test_request_routes_to_its_model_origin(tmp_path, http_model):
    from system.audio_protocol import CHECKPOINTS, execute_remote
    from system.audio_worker import AudioWorker
    other = ModelHTTP()
    origins = {model: other.origin for model in CHECKPOINTS}
    origins['vital_v7_3'] = http_model.origin
    worker = AudioWorker(tmp_path / 'worker', origins=origins, poll_interval=.01)
    server = grpc.server(ThreadPoolExecutor(4))
    worker.add_to_server(server)
    port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    channel = grpc.insecure_channel(f'127.0.0.1:{port}')
    try:
        for i, model in enumerate(CHECKPOINTS):
            execute_remote(channel, payload(tmp_path, model=model, key=f'{i:032x}'), 3)
        assert [model for _, model in http_model.calls] == ['vital_v7_3']
        assert [model for _, model in other.calls] == ['vital_v7_5', 'vital_v7_5_chord']
    finally:
        finish((worker, server, channel))
        other.close()


def test_durable_root_has_one_process_owner(tmp_path, http_model):
    from system.audio_worker import AudioWorker
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        with pytest.raises(RuntimeError, match='already owned'):
            AudioWorker(tmp_path / 'worker')
    finally:
        finish(running)


def test_health_does_not_claim_ready_for_missing_model(tmp_path, http_model):
    from system.audio_protocol import probe_remote
    from system.audio_worker import AudioWorker
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        # A refused local connection is a genuine service boundary failure.
        missing = socket.socket()
        missing.bind(('127.0.0.1', 0))
        port = missing.getsockname()[1]
        missing.close()
        running[0].origins['vital_v7_5'] = f'http://127.0.0.1:{port}'
        assert probe_remote(running[2])['ready'] is False
    finally:
        finish(running)


@pytest.mark.parametrize('status', [400, 404, 405, 413, 422])
def test_known_preaccept_rejection_fails_and_releases_slot(tmp_path, http_model, status):
    from system.audio_protocol import AudioWorkerError, execute_remote
    http_model.post_status = status
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        with pytest.raises(AudioWorkerError) as error:
            execute_remote(running[2], payload(tmp_path), 2)
        assert error.value.code == 'submission_rejected'
        assert error.value.retryable is False
        assert http_model.submissions == 1
        http_model.post_status = None
        response = execute_remote(running[2], payload(tmp_path, key='b' * 32), 2)
        assert response['state'] == 'completed'
    finally:
        finish(running)


@pytest.mark.parametrize('broken,expected', [('bad_job_id', 'result_mismatch'), ('status_override', 'invalid_status')])
def test_invalid_acceptance_quarantines_slot_but_returns_explicit_error(tmp_path, http_model, broken, expected):
    from system.audio_protocol import AudioWorkerError, execute_remote
    http_model.release.clear()
    setattr(http_model, broken, True if broken == 'bad_job_id' else 'unknown')
    running = start_worker(tmp_path / 'worker', http_model)
    pool = ThreadPoolExecutor(1)
    try:
        with pytest.raises(AudioWorkerError) as error:
            execute_remote(running[2], payload(tmp_path), 2)
        assert error.value.code == expected
        from system.audio_protocol import probe_remote
        health = probe_remote(running[2])
        assert health['inflight'] == 1
        assert health['ready'] is False
        assert health['quarantined'][0]['error']['code'] == expected
        next_job = pool.submit(execute_remote, running[2], payload(tmp_path, key='b' * 32), 3)
        time.sleep(.15)
        assert not next_job.done()
        assert len(http_model.calls) == 1
        setattr(http_model, broken, False if broken == 'bad_job_id' else None)
        http_model.release.set()
        assert next_job.result(2)['state'] == 'completed'
    finally:
        http_model.release.set()
        setattr(http_model, broken, False if broken == 'bad_job_id' else None)
        pool.shutdown()
        finish(running)


@pytest.mark.parametrize('document', [None, [], {'_schema': 'unified-synth-preset-v3', 'origin': []}])
def test_malformed_terminal_document_fails_without_killing_coordinator(tmp_path, http_model, document):
    from system.audio_protocol import AudioWorkerError, execute_remote
    http_model.result_override = {'document': document}
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        with pytest.raises(AudioWorkerError) as error:
            execute_remote(running[2], payload(tmp_path), 2)
        assert error.value.code == 'invalid_result'
        http_model.result_override = {}
        assert execute_remote(running[2], payload(tmp_path, key='b' * 32), 2)['state'] == 'completed'
    finally:
        finish(running)


def test_completion_is_not_published_until_atomic_state_write_succeeds(tmp_path, http_model):
    from system.audio_protocol import AudioWorkerError, execute_remote
    http_model.release.clear()
    root = tmp_path / 'worker'
    running = start_worker(root, http_model)
    pool = ThreadPoolExecutor(1)
    task = payload(tmp_path)
    blocker = root / ('a' * 32 + '.tmp')
    try:
        call = pool.submit(execute_remote, running[2], task, .5)
        wait_until(lambda: (root / ('a' * 32 + '.json')).exists()
                   and json.loads((root / ('a' * 32 + '.json')).read_text())['state'] == 'running')
        blocker.mkdir()
        http_model.release.set()
        with pytest.raises(AudioWorkerError):
            call.result(2)
        assert json.loads((root / ('a' * 32 + '.json')).read_text())['state'] == 'running'
        blocker.rmdir()
        assert execute_remote(running[2], task, 3)['state'] == 'completed'
        assert json.loads((root / ('a' * 32 + '.json')).read_text())['state'] == 'completed'
    finally:
        if blocker.is_dir():
            blocker.rmdir()
        http_model.release.set()
        pool.shutdown()
        finish(running)


@pytest.mark.parametrize('missing', [False, True])
def test_upstream_readiness_requires_verified_checkpoint_metadata(tmp_path, http_model, missing):
    from system.audio_protocol import CHECKPOINTS, probe_remote
    http_model.health_omit_checkpoints = missing
    http_model.health_checkpoints = {**CHECKPOINTS, 'vital_v7_3': 'f' * 64}
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        health = probe_remote(running[2])
        assert health['ready'] is False
        assert health['checkpoints'].get('vital_v7_3') != CHECKPOINTS['vital_v7_3']
    finally:
        finish(running)


def test_manifest_cli_is_mandatory(tmp_path):
    result = subprocess.run([sys.executable, '-m', 'system.audio_worker', '--root', str(tmp_path)],
                            capture_output=True, timeout=2)
    assert result.returncode == 2
    assert b'--checkpoint-manifest' in result.stderr


def test_actual_checkpoint_files_are_revalidated_after_change(tmp_path, http_model, monkeypatch):
    from system.audio_protocol import CHECKPOINTS, probe_remote
    from system.audio_worker import AudioWorker
    paths = {}
    for index, model in enumerate(CHECKPOINTS):
        path = tmp_path / (model + '.pt')
        path.write_bytes(f'checkpoint-{index}'.encode())
        paths[model] = str(path)
        monkeypatch.setitem(CHECKPOINTS, model, sha(path.read_bytes()))
    worker = AudioWorker(tmp_path / 'worker', origins={m: http_model.origin for m in CHECKPOINTS},
                         checkpoint_paths=paths)
    server = grpc.server(ThreadPoolExecutor(4))
    worker.add_to_server(server)
    port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    channel = grpc.insecure_channel(f'127.0.0.1:{port}')
    try:
        # Local file evidence is sufficient even when HTTP services omit their pins.
        http_model.health_omit_checkpoints = True
        assert probe_remote(channel)['checkpoints'] == CHECKPOINTS
        assert probe_remote(channel)['ready'] is True
        path = Path(paths['vital_v7_3'])
        path.write_bytes(b'changed-checkpoint')
        assert probe_remote(channel)['ready'] is False
        assert 'vital_v7_3' not in probe_remote(channel)['checkpoints']
        path.unlink()
        assert probe_remote(channel)['ready'] is False
    finally:
        finish((worker, server, channel))


@pytest.mark.parametrize('paths', [{}, {'vital_v7_3': '/missing'},
                                  {'vital_v7_3': 'relative.pt', 'vital_v7_5': '/a', 'vital_v7_5_chord': '/a'}])
def test_checkpoint_manifest_requires_exact_models_and_absolute_paths(tmp_path, paths):
    from system.audio_worker import AudioWorker
    with pytest.raises(ValueError, match='checkpoint|Checkpoint'):
        AudioWorker(tmp_path / 'worker', checkpoint_paths=paths)


def test_nonfinite_http_result_fails_terminal_job_without_losing_coordinator(tmp_path, http_model):
    from system.audio_protocol import AudioWorkerError, execute_remote
    http_model.nonfinite_result = True
    running = start_worker(tmp_path / 'worker', http_model)
    try:
        with pytest.raises(AudioWorkerError) as error:
            execute_remote(running[2], payload(tmp_path), 2)
        assert error.value.code == 'invalid_metadata'
        assert error.value.retryable is False
        assert running[0]._thread.is_alive()
        http_model.nonfinite_result = False
        assert execute_remote(running[2], payload(tmp_path, key='b' * 32), 2)['state'] == 'completed'
    finally:
        finish(running)


@pytest.mark.parametrize('headroom,completed', [(200, True), (10, False)])
def test_result_size_boundary_preserves_durable_completion_or_terminal_failure(tmp_path, http_model, headroom, completed):
    from system.audio_protocol import AudioWorkerError, MAX_JSON_BYTES, execute_remote
    http_model.release.clear()
    root = tmp_path / 'worker'
    running = start_worker(root, http_model)
    pool = ThreadPoolExecutor(1)
    task = payload(tmp_path)
    try:
        call = pool.submit(execute_remote, running[2], task, 5)
        wait_until(lambda: task['request_id'] in http_model.jobs)
        original = http_model.jobs[task['request_id']]['result']
        base = len(json.dumps({**original, 'padding': ''}, separators=(',', ':')).encode())
        http_model.result_override = {'padding': 'x' * (MAX_JSON_BYTES - headroom - base)}
        http_model.release.set()
        if completed:
            result = call.result(5)
            assert result['state'] == 'completed'
            assert len((root / (task['request_id'] + '.json')).read_bytes()) > MAX_JSON_BYTES
            finish(running)
            running = start_worker(root, http_model)
            assert execute_remote(running[2], task, 5) == result
        else:
            with pytest.raises(AudioWorkerError) as error:
                call.result(5)
            assert error.value.retryable is False
            assert json.loads((root / (task['request_id'] + '.json')).read_text())['state'] == 'failed'
        http_model.result_override = {}
        assert execute_remote(running[2], payload(tmp_path, key='b' * 32), 3)['state'] == 'completed'
    finally:
        http_model.release.set()
        pool.shutdown()
        finish(running)


def test_checkpoint_manifest_can_share_durable_directory(tmp_path, http_model):
    from system.audio_protocol import CHECKPOINTS
    from system.audio_worker import AudioWorker
    root = tmp_path / 'worker'
    root.mkdir()
    paths = {model: str(tmp_path / (model + '.pt')) for model in CHECKPOINTS}
    (root / 'checkpoint-manifest.json').write_text(json.dumps(paths))
    worker = AudioWorker(root, origins={model: http_model.origin for model in CHECKPOINTS},
                         checkpoint_paths=paths)
    try:
        assert worker._verified_checkpoint_files() == {}
    finally:
        worker.close()


def test_artifact_cancel_interrupts_a_blocked_rpc_from_another_thread():
    from system.audio_protocol import AudioWorkerError, SERVICE, remote_artifact
    started, stopped = threading.Event(), threading.Event()

    def blocked_artifact(request, context):
        started.set()
        while context.is_active():
            time.sleep(.01)
        stopped.set()
        return
        yield b''

    server = grpc.server(ThreadPoolExecutor(2))
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(SERVICE, {
        'Artifact': grpc.unary_stream_rpc_method_handler(blocked_artifact)}),))
    port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    channel = grpc.insecure_channel(f'127.0.0.1:{port}')
    pool = ThreadPoolExecutor(1)
    stream = remote_artifact(channel, 'vital_v7_3', 'a' * 32, 'wav', timeout=3)
    try:
        assert callable(getattr(stream, 'cancel', None))
        assert callable(getattr(stream, 'close', None))
        waiting = pool.submit(next, iter(stream))
        assert started.wait(2)
        stream.cancel()
        with pytest.raises(AudioWorkerError) as error:
            waiting.result(1)
        assert error.value.code == 'transport'
        assert stopped.wait(1)
        stream.close()
    finally:
        channel.close()
        server.stop(0).wait()
        pool.shutdown()


def test_artifact_cancellation_interrupts_stalled_http_prefetch_and_closes_tempfile(tmp_path, http_model, monkeypatch):
    import tempfile
    from system.audio_protocol import AudioWorkerError, execute_remote, remote_artifact
    running = start_worker(tmp_path / 'worker', http_model)
    pool = ThreadPoolExecutor(1)
    opened = []
    real_temporary_file = tempfile.TemporaryFile
    def observed_file(*args, **kwargs):
        stream = real_temporary_file(*args, **kwargs)
        opened.append(stream)
        return stream
    monkeypatch.setattr(tempfile, 'TemporaryFile', observed_file)
    try:
        execute_remote(running[2], payload(tmp_path), 3)
        running[0].http_timeout = 10
        http_model.artifact_stall = True
        stream = remote_artifact(running[2], 'vital_v7_3', 'a' * 32, 'wav', timeout=15)
        blocked = pool.submit(next, iter(stream))
        assert http_model.artifact_started.wait(2)
        wait_until(lambda: bool(opened))
        stream.cancel()
        with pytest.raises(AudioWorkerError):
            blocked.result(1)
        assert http_model.artifact_disconnected.wait(1), 'upstream HTTP socket retained after RPC cancellation'
        wait_until(lambda: all(file.closed for file in opened), timeout=1)
        assert not list((tmp_path / 'worker').glob('upload-*'))
    finally:
        http_model.artifact_release.set()
        pool.shutdown()
        finish(running)
