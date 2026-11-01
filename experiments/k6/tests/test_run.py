import importlib.util
import gzip
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('runner', Path(__file__).parents[1] / 'run.py')
runner = importlib.util.module_from_spec(spec)
if Path(spec.origin).exists():
    spec.loader.exec_module(runner)


def summary(started=1000, finished=1000, successes=1000, dropped=0, p95=100):
    return {'metrics': {
        'requests_started': {'values': {'count': started}},
        'requests_finished': {'values': {'count': finished}},
        'valid_successes': {'values': {'count': successes}},
        'dropped_iterations': {'values': {'count': dropped}},
        'successful_latency_ms': {'values': {'p(95)': p95}},
        'attempt_latency_ms': {'values': {'p(95)': 5}},
    }}


class VerdictTests(unittest.TestCase):
    def test_fast_rejections_do_not_look_healthy(self):
        v = runner.verdict(summary(successes=100), 10, 100, 250)
        self.assertFalse(v['healthy'])
        self.assertTrue(v['valid_load'])

    def test_generator_drops_invalidate_requested_load(self):
        v = runner.verdict(summary(started=500, finished=500, successes=500, dropped=500), 10, 100, 250)
        self.assertFalse(v['valid_load'])
        self.assertFalse(v['healthy'])

    def test_unfinished_requests_cannot_pass(self):
        self.assertFalse(runner.verdict(summary(finished=999), 10, 100, 250)['healthy'])

    def test_high_tail_latency_and_missing_samples_fail(self):
        for data in [summary(p95=900), summary(started=0, finished=0, successes=0),
                     summary(started=10, finished=10, successes=10)]:
            self.assertFalse(runner.verdict(data, 10, 100, 250)['healthy'])

    def test_complete_low_latency_stage_passes(self):
        self.assertTrue(runner.verdict(summary(), 10, 100, 250)['healthy'])

    def test_insufficient_samples_are_not_called_a_capacity_breach(self):
        result = runner.verdict(summary(started=10, finished=10, successes=10), 10, 1, 250)
        self.assertFalse(result['service_slo_breached'])

    def test_recovery_at_requested_rate_requires_valid_load_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'samples.json.gz'
            with gzip.open(path, 'wt') as output:
                for second in range(30):
                    for _ in range(10):
                        row = {'type':'Point','metric':'attempt_latency_ms','data':{
                            'time':runner.datetime.fromtimestamp(100 + second, runner.timezone.utc).isoformat(),
                            'value':20,'tags':{'outcome':'success','worker':'1000'}}}
                        output.write(json.dumps(row) + '\n')
            good = runner.fault_windows(path, 90, 100, 10, 250, load_valid=True)
            bad = runner.fault_windows(path, 90, 100, 10, 250, load_valid=False)
            self.assertEqual(good['stable_recovery_after_restart_s'], 30)
            self.assertIsNone(bad['stable_recovery_after_restart_s'])

    def test_identity_guard_rejects_real_or_other_run(self):
        for value in [{}, {'benchmark': 'simulation-only', 'run_id': 'other'}]:
            with self.assertRaises(ValueError):
                runner.verify_identity(value, 'expected')

    def test_compose_has_no_production_services_or_host_worker_ports(self):
        config = runner.compose_config(4, 18180, 'test-run', 'test-image')
        self.assertEqual(len(config['services']), 5)
        self.assertNotIn('env_file', config['services']['api'])
        for name, service in config['services'].items():
            self.assertNotIn('container_name', service)
            if name != 'api':
                self.assertNotIn('ports', service)
        self.assertEqual(config['services']['api']['ports'], ['127.0.0.1:18180:8000'])


if __name__ == '__main__':
    unittest.main()
