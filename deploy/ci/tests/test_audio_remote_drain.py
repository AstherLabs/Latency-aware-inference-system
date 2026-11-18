"""A locally terminal request may still own durable inference on Vast."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from deploy.ci import deploy_platform as platform
from deploy.ci.tests import test_platform_deployment as existing


@pytest.fixture
def runtime(monkeypatch):
    host = object.__new__(platform.Runtime)
    host.target = platform.Environment('staging')
    host.inspect = lambda *args: '["SYNTHX_AUDIO_ENABLED=1"]'
    metrics = existing.SafetyBoundaryTests().metrics().encode()
    monkeypatch.setattr(platform.urllib.request, 'urlopen', lambda *args, **kwargs: io.BytesIO(metrics))
    health = {'ready': True, 'accepting': False, 'worker': {'ready': True, 'inflight': 1, 'quarantined': []}}
    seen = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_GET(self):
            seen.append((self.path, self.headers.get('Authorization')))
            self.send_response(200); self.end_headers(); self.wfile.write(json.dumps(health).encode())
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    def docker(*args, **kwargs):
        code = args[-1].replace('127.0.0.1:8000', f'127.0.0.1:{server.server_port}')
        result = subprocess.run([sys.executable, '-c', code], env={**os.environ, 'INFERENCE_SERVICE_TOKEN': 'private-token'},
                                capture_output=True, text=True, timeout=10)
        if result.returncode: raise platform.DeployError('remote state was not confirmed idle')
        return result.stdout.strip()
    host.docker = docker
    try:
        yield host, health, seen
    finally:
        server.shutdown(); server.server_close(); thread.join()


def test_idle_metrics_do_not_hide_remote_inflight_or_quarantined_work(runtime):
    host, health, seen = runtime
    assert host.gateway_quiet() is False
    health['worker'].update(inflight=0, quarantined=[{'request_id': 'a' * 32, 'error': 'execution uncertain'}])
    assert host.gateway_quiet() is False
    health['worker']['quarantined'] = []
    assert host.gateway_quiet() is True
    assert seen == [('/audio/health', 'Bearer private-token')] * 3


@pytest.mark.parametrize('worker', [{'ready': True, 'inflight': 0},
    {'ready': True, 'quarantined': []}, {'ready': False, 'inflight': 0, 'quarantined': []},
    {'ready': True, 'inflight': False, 'quarantined': []},
    {'ready': True, 'inflight': 0, 'quarantined': None}])
def test_unconfirmed_remote_execution_state_is_never_idle(runtime, worker):
    host, health, seen = runtime
    health['worker'] = worker
    assert host.gateway_quiet() is False


def test_disabled_previous_gateway_keeps_original_metrics_idle_contract(runtime):
    host, health, seen = runtime
    host.inspect = lambda *args: '["SYNTHX_AUDIO_ENABLED=0"]'
    assert host.gateway_quiet() is True
    assert seen == []
    host.inspect = lambda *args: '[]'
    assert host.gateway_quiet() is True
    assert seen == []


def test_wait_idle_times_out_without_stopping_or_fencing_text_when_remote_is_uncertain(runtime, tmp_path, monkeypatch):
    from types import SimpleNamespace
    from system.deployment import DeploymentGate
    host, health, seen = runtime
    host.root = tmp_path
    (tmp_path / 'control').mkdir()
    host.config = {'drain_timeout_seconds': .1}
    host.platform_token = 'window'
    host.marker_names = set()
    host.backend = SimpleNamespace(drain_token='backend-window')
    gateway_gate = DeploymentGate(tmp_path / 'control/gateway-drain')
    host.backend_call = lambda name: {'draining': True, 'drain_id': 'backend-window', 'active_requests': 0, 'active_operations': 0}
    host.gateway_status = gateway_gate.snapshot
    host.worker_status = lambda: {'protocol': 1, 'draining': True, 'drain_id': 'window', 'active': 0}
    host.busy = lambda: False
    host.assert_protected = lambda: None
    moments = iter([0, 1])
    monkeypatch.setattr(platform.time, 'monotonic', lambda: next(moments))
    with pytest.raises(platform.DeployError, match='did not become idle'):
        host.wait_idle()
    assert gateway_gate.snapshot()['active'] == 0
    assert 'worker-drain' not in host.marker_names
    assert seen == [('/audio/health', 'Bearer private-token')]


def test_expired_gateway_job_with_no_local_hold_still_blocks_on_remote_ownership(runtime, tmp_path):
    import asyncio
    from system.audio_jobs import AudioJobManager
    from system.deployment import DeploymentGate
    from system.queue import TaskQueue
    host, health, seen = runtime
    gate = DeploymentGate(tmp_path / 'gateway-drain')
    manager = AudioJobManager(tmp_path / 'jobs', TaskQueue(), 'worker:50061', deployment_gate=gate, job_timeout=1700)
    manager.start()
    async def source(): yield b'audio'
    try:
        asyncio.run(manager.submit('vital_v7_3', 'a' * 32, source()))
        task = manager.queue.get(timeout=5)
        assert task is not None
        task.mark_processing()
        # Expire the persisted deadline only after dispatch, avoiding a race with fsync/poll latency.
        with manager._condition:
            manager._write(dict(manager._records['a' * 32], deadline_at=0))
            assert manager._condition.wait_for(lambda: manager.status('a' * 32)['state'] == 'failed', timeout=3)
        assert gate.snapshot()['active'] == 0
        assert manager.status('a' * 32)['error']['code'] == 'deadline_exceeded'
        assert host.gateway_quiet() is False
        health['worker']['inflight'] = 0
        assert host.gateway_quiet() is True
    finally:
        manager.stop()
