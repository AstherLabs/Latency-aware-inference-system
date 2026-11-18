"""A gateway-only profile preserves the unavailable text process and default checks."""
import copy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from deploy.ci import deploy_platform as platform
from deploy.ci.tests import test_platform_deployment as existing
from deploy.ci.tests.test_platform_deployment import HostBoundary, OLD, NEW, SHA


class AudioHost(HostBoundary):
    def prepare(self, sha):
        return {'sha': sha, 'compose': 'new.json', 'audio_enabled': True, 'audio_gateway_only': True}
    def configure_audio_rollout(self, previous): self.baseline = copy.deepcopy(previous)


def test_gateway_only_deployment_preserves_the_previous_text_image():
    fixture = existing.DeploymentTests(); fixture.setUp()
    try:
        host = AudioHost()
        images = platform.Deployment(host).run(SHA, fixture.archive, fixture.digest)
        assert images == {'gateway': NEW, 'text-worker': OLD['text-worker']}
        assert host.backend_pins['text-worker'] == OLD['text-worker']
        assert host.baseline['images'] == OLD
        assert host.state['audio_gateway_only'] is True
    finally:
        fixture.doCleanups()


def runtime(tmp_path):
    host = object.__new__(platform.Runtime)
    host.target = platform.Environment('staging')
    host.root = tmp_path
    host._audio_previous = {'compose': '/old/compose.platform.yml', 'images': dict(OLD)}
    host.assert_platform_environment = lambda: None
    host.assert_protected = lambda: None
    host.assert_audio_text_unchanged = lambda: None
    calls = []
    def docker(*args, **kwargs):
        calls.append(args)
        return 'gateway-id' if args[:2] == ('container', 'ls') else ''
    host.docker = docker
    return host, calls


def test_stop_touches_only_gateway_in_the_narrow_profile(tmp_path):
    host, calls = runtime(tmp_path)
    host.stop()
    assert len([args for args in calls if args[0] == 'stop']) == 1
    assert all('text-worker' not in ' '.join(args) for args in calls)


def test_start_preserves_text_process_and_uses_only_gateway_compose_up(tmp_path, monkeypatch):
    host, calls = runtime(tmp_path)
    directory = tmp_path / 'release'; directory.mkdir()
    release = {'compose': str(directory / 'compose.platform.yml'), 'audio_enabled': True, 'audio_gateway_only': True}
    host.compose = lambda release, *args: calls.append(args) or '{}'
    monkeypatch.setattr(platform, 'validate_layout', lambda *args: None)
    host.start(release, {'gateway': NEW, 'text-worker': OLD['text-worker']})
    assert calls[-1] == ('up', '-d', '--no-deps', '--no-build', 'gateway')
    assert host._audio_rollback is False
    with pytest.raises(platform.DeployError, match='text'):
        host.start(release, {'gateway': NEW, 'text-worker': NEW})


def container():
    return {'Id': 'stable-container', 'Image': OLD['text-worker'],
            'State': {'Running': True, 'Pid': 123, 'StartedAt': 'original-start'},
            'Config': {'Env': ['INFERENCE_BACKEND=text_v1.2.3', 'TEXT_SERVICE_URL=http://model-tunnel:18083',
                               'TEXT_REFINEMENT_SERVICE_URL=http://model-tunnel:18080'], 'Cmd': ['worker']},
            'HostConfig': {'NetworkMode': 'synthx-staging_default'},
            'NetworkSettings': {'Networks': {'synthx-staging_default': {}}}, 'Mounts': []}


def test_text_snapshot_detects_restart_configuration_or_image_mutation(tmp_path):
    host, _ = runtime(tmp_path)
    host.assert_audio_text_unchanged = platform.Runtime.assert_audio_text_unchanged.__get__(host)
    state = container()
    host.inspect = lambda name, expr: json.dumps(state)
    host._audio_text_baseline = host.audio_container_snapshot(host.target.containers['text-worker'], include_process=True)
    host.assert_audio_text_unchanged()
    for change in (lambda c: c.update(Image=NEW), lambda c: c['State'].update(Pid=456),
                   lambda c: c['State'].update(StartedAt='restarted'),
                   lambda c: c['Config']['Env'].append('TEXT_SERVICE_URL=http://foreign:18083')):
        state = container(); change(state)
        with pytest.raises(platform.DeployError, match='text'):
            host.assert_audio_text_unchanged()


def test_new_profile_requires_staging_and_audio_boolean_opt_in(tmp_path):
    host, _ = runtime(tmp_path)
    host.config = {'audio_gateway_only': True}
    with pytest.raises(platform.DeployError): host.audio_rollout_settings()
    host.config['audio_enabled'] = True
    assert host.audio_rollout_settings() == (True, True)
    host.target = platform.Environment('dev')
    with pytest.raises(platform.DeployError): host.audio_rollout_settings()
    host.target = platform.Environment('staging')
    for value in ('true', 1, None):
        host.config['audio_gateway_only'] = value
        with pytest.raises(platform.DeployError): host.audio_rollout_settings()


@pytest.fixture
def health_host(tmp_path):
    host, calls = runtime(tmp_path)
    host.platform_status = lambda: True
    host.gateway_readiness = lambda: {'status': 'ready'}
    host.inspect = lambda name, expression: 'true' if expression == '{{.State.Running}}' else NEW
    host._audio_rollback = False
    return host


def test_forward_health_executes_authenticated_pinned_audio_check(health_host, monkeypatch):
    import os
    import subprocess
    import sys
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from concurrent.futures import ThreadPoolExecutor
    import grpc
    from system.audio_protocol import CHECKPOINTS
    seen = []
    health = {'ready': True, 'worker': {'ready': True, 'models': list(CHECKPOINTS), 'checkpoints': dict(CHECKPOINTS)}}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_GET(self):
            seen.append((self.path, self.headers.get('Authorization')))
            self.send_response(200); self.end_headers(); self.wfile.write(json.dumps(health).encode())
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    grpc_server = grpc.server(ThreadPoolExecutor(max_workers=1))
    port = grpc_server.add_insecure_port('127.0.0.1:0'); grpc_server.start()
    def docker(*args, **kwargs):
        code = args[-1].replace('text-worker:50062', f'127.0.0.1:{port}').replace('127.0.0.1:8000', f'127.0.0.1:{server.server_port}')
        result = subprocess.run([sys.executable, '-c', code], env={**os.environ, 'INFERENCE_SERVICE_TOKEN': 'private-service-token'}, capture_output=True, text=True, timeout=20)
        if result.returncode: raise platform.DeployError('transport verification failed')
        return result.stdout.strip()
    health_host.docker = docker
    try:
        assert health_host.healthy({'gateway': NEW, 'text-worker': OLD['text-worker']}) is True
        assert seen == [('/audio/health', 'Bearer private-service-token')]
        # A reachable worker with a wrong checkpoint must not pass the same executed check.
        health['worker']['checkpoints']['vital_v7_3'] = '0' * 64
        moments = iter([0, 1, 100]); monkeypatch.setattr(platform.time, 'monotonic', lambda: next(moments))
        monkeypatch.setattr(platform.time, 'sleep', lambda seconds: None)
        assert health_host.healthy({'gateway': NEW, 'text-worker': OLD['text-worker']}) is False
    finally:
        server.shutdown(); server.server_close(); thread.join()
        grpc_server.stop(0).wait()


def test_rollback_checks_original_readiness_and_layout_without_new_audio_condition(health_host, monkeypatch):
    health_host._audio_rollback = True
    health_host._audio_previous_readiness = {'status': 'ready'}
    health_host._audio_previous_layout = {'image': OLD['gateway'], 'layout_sha256': 'original'}
    health_host.audio_container_snapshot = lambda name: dict(health_host._audio_previous_layout)
    health_host.inspect = lambda name, expression: 'true' if expression == '{{.State.Running}}' else OLD['gateway']
    probes = []
    health_host.docker = lambda *args, **kwargs: probes.append(args[-1]) or 'text-adapter-unchanged'
    assert health_host.healthy(OLD) is True
    assert '/audio/health' not in probes[0]
    health_host.audio_container_snapshot = lambda name: {'image': OLD['gateway'], 'layout_sha256': 'changed'}
    moments = iter([0, 1, 100]); monkeypatch.setattr(platform.time, 'monotonic', lambda: next(moments))
    monkeypatch.setattr(platform.time, 'sleep', lambda seconds: None)
    assert health_host.healthy(OLD) is False


def test_narrow_receipt_reports_the_unchanged_text_outage(tmp_path):
    host, _ = runtime(tmp_path)
    host.shared = {'dependency_images': {'protected-backend': 'same'}}
    host.shared_path = tmp_path / 'shared.json'
    host._audio_text_baseline = {'image': OLD['text-worker'], 'layout_sha256': 'text', 'container_id': 'same', 'pid': 123, 'started_at': 'before'}
    host._audio_previous_readiness = {'status': 'ready'}
    host._audio_previous_layout = {'image': OLD['gateway'], 'layout_sha256': 'gateway'}
    host.record({'compose': '/new/compose.yml', 'audio_enabled': True, 'audio_gateway_only': True},
                {'gateway': NEW, 'text-worker': OLD['text-worker']}, host._audio_previous)
    receipt = json.loads((tmp_path / 'current.json').read_text())
    assert receipt['text_model_health'] == 'preexisting_unavailable_not_changed'
    assert receipt['protected_text_worker']['pid'] == 123
    assert receipt['images']['text-worker'] == OLD['text-worker']


def test_recorded_gateway_profile_requires_its_audio_enable_flag(tmp_path):
    host, _ = runtime(tmp_path)
    for release in ({'compose': str(tmp_path / 'compose.yml'), 'audio_gateway_only': True},
                    {'compose': str(tmp_path / 'compose.yml'), 'audio_enabled': True, 'audio_gateway_only': 'true'}):
        with pytest.raises(platform.DeployError):
            host.compose(release, 'config')


def test_transaction_durably_records_the_rollback_and_text_baselines(tmp_path):
    host, _ = runtime(tmp_path)
    host.shared = {'dependency_images': {'backend': 'unchanged'}}
    host._audio_text_baseline = {'image': OLD['text-worker'], 'layout_sha256': 'text', 'container_id': 'same', 'pid': 123, 'started_at': 'before'}
    host._audio_previous_readiness = {'status': 'ready'}
    host._audio_previous_layout = {'image': OLD['gateway'], 'layout_sha256': 'old-gateway'}
    host.begin_transaction(host._audio_previous)
    transaction = json.loads((tmp_path / 'transaction.json').read_text())
    assert transaction['audio_gateway_only'] is True
    assert transaction['protected_text_worker'] == host._audio_text_baseline
    assert transaction['previous_gateway_readiness'] == {'status': 'ready'}
    assert transaction['previous_gateway_layout']['image'] == OLD['gateway']


def test_configure_profile_captures_immutable_baselines_before_mutation(tmp_path):
    host, _ = runtime(tmp_path)
    del host._audio_previous
    host.protected = {'backend': 'unchanged'}
    def inspect(name, expression):
        state = container()
        if name == host.target.containers['gateway']:
            state['Image'] = OLD['gateway']
        return json.dumps(state)
    host.inspect = inspect
    host.gateway_readiness = lambda: {'status': 'ready'}
    host.configure_audio_rollout({'compose': '/old/compose.platform.yml', 'images': dict(OLD)})
    assert host._audio_previous_readiness == {'status': 'ready'}
    assert host._audio_text_baseline['pid'] == 123
    assert host._audio_previous_layout['image'] == OLD['gateway']
    assert 'INFERENCE_BACKEND' not in json.dumps(host.audio_rollout_receipt())
    assert host.protected['backend'] == 'unchanged'
    assert host.protected[host.target.containers['text-worker']] == OLD['text-worker']


def test_default_profile_still_requires_remote_text_model_health(tmp_path, monkeypatch):
    import io
    host, _ = runtime(tmp_path)
    del host._audio_previous
    host.platform_status = lambda: True
    host.inspect = lambda name, expression: json.dumps(container()['Config']['Env']) if expression == '{{json .Config.Env}}' else ('true' if expression == '{{.State.Running}}' else NEW)
    monkeypatch.setattr(platform.urllib.request, 'urlopen', lambda *a, **k: io.StringIO('{"status":"ready"}'))
    probes = []
    def unavailable(*args, **kwargs):
        probes.append(args)
        raise platform.DeployError('existing text HTTP model is unavailable')
    host.docker = unavailable
    moments = iter([0, 1, 100]); monkeypatch.setattr(platform.time, 'monotonic', lambda: next(moments))
    monkeypatch.setattr(platform.time, 'sleep', lambda seconds: None)
    assert host.healthy({'gateway': NEW, 'text-worker': NEW}) is False
    assert probes[0][-4:] == ('http://model-tunnel:18083', 'text_v1.2.3', 'http://model-tunnel:18080', 'model-tunnel')
