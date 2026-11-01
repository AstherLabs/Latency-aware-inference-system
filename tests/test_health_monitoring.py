"""Runtime probes and monitoring contracts without model assets."""
import json
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]

class HealthProbeTests(unittest.TestCase):
    def test_healthy_response_passes_but_http_success_with_unhealthy_body_fails(self):
        from system.health_probe import check
        class Response:
            def __init__(self, healthy): self.healthy = healthy
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, size=-1): return json.dumps({'healthy': self.healthy}).encode()
        with patch('system.health_probe.urlopen', return_value=Response(True)):
            self.assertTrue(check('http://127.0.0.1:8000/health/runtime'))
        with patch('system.health_probe.urlopen', return_value=Response(False)):
            self.assertFalse(check('http://127.0.0.1:8000/health/runtime'))

    def test_network_failure_and_invalid_payload_fail_closed(self):
        from system.health_probe import check
        with patch('system.health_probe.urlopen', side_effect=TimeoutError):
            self.assertFalse(check('http://127.0.0.1:8000/health/runtime'))
        class Response:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, size=-1): return b'[]'
        with patch('system.health_probe.urlopen', return_value=Response()):
            self.assertFalse(check('http://127.0.0.1:8000/health/runtime'))

    def test_compose_gateway_and_local_api_use_runtime_probe(self):
        for name in ('docker-compose.yml', 'deploy/ci/compose.platform.yml'):
            text=(ROOT/name).read_text()
            self.assertIn('system.health_probe',text)
            self.assertIn('start_period: 20s',text)
            self.assertNotIn('/var/run/docker.sock',text)

    def test_dashboard_exposes_runtime_health_and_alerts(self):
        dashboard=json.loads((ROOT/'monitoring/grafana/dashboards/inference.json').read_text())
        exprs=[target.get('expr','') for panel in dashboard['panels'] for target in panel.get('targets',[])]
        self.assertTrue(any('inference_runtime_loop_healthy' in expr for expr in exprs))
        self.assertTrue(any('inference_runtime_loop_last_cycle_age_seconds' in expr for expr in exprs))
        self.assertTrue(any('ALERTS' in expr for expr in exprs))

if __name__=='__main__': unittest.main()
