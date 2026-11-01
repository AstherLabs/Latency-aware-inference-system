#!/usr/bin/env python3
"""Reproducible overload and crash experiments against an isolated Docker stack."""
from __future__ import annotations
import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import shutil
import subprocess
import tarfile
import threading
import time
import urllib.request
import uuid

HERE = Path(__file__).resolve().parent


def verify_identity(value, run_id):
    if value.get('benchmark') != 'simulation-only' or value.get('run_id') != run_id:
        raise ValueError('Refusing traffic or configuration changes against a non-benchmark target')


def verdict(summary, rate, duration, slo):
    """Keep load delivery, service capacity, and sample sufficiency independent."""
    metrics = summary.get('metrics', {})
    count = lambda name: metrics.get(name, {}).get('values', {}).get('count', 0)
    sent, finished, good = count('requests_started'), count('requests_finished'), count('valid_successes')
    dropped = count('dropped_iterations')
    p95 = metrics.get('successful_latency_ms', {}).get('values', {}).get('p(95)')
    valid = dropped == 0 and sent >= math.floor(rate * duration * .99) and sent == finished
    success_rate = good / sent if sent else 0
    enough = good >= 100
    breach = valid and finished >= 100 and (success_rate < .99 or (enough and p95 is not None and p95 > slo))
    return {'target_rps': rate, 'planned': rate * duration, 'sent': sent, 'finished': finished,
            'successes': good, 'failures': finished - good, 'unfinished': sent - finished,
            'generator_dropped': dropped, 'valid_load': valid, 'success_rate': success_rate,
            'rejected_503': count('rejections_503'), 'deadline_504': count('deadlines_504'),
            'transport_failures': count('transport_failures'), 'other_service_failures': count('other_service_failures'),
            'enough_tail_samples': enough, 'success_p95_ms': p95, 'service_slo_breached': breach,
            'healthy': valid and enough and success_rate >= .99 and p95 is not None and p95 <= slo,
            'metrics': metrics}


def compose_config(workers, port, run_id, image):
    addresses = {str(1000 + i): f'worker-{i}:50051' for i in range(workers)}
    services = {'api': {'image': image, 'command': ['python', '/harness/runtime.py', 'api'],
        'ports': [f'127.0.0.1:{port}:8000'], 'restart': 'no',
        'environment': {'USE_GRPC': '1', 'SYNTHX_JOBS_ENABLED': '0',
            'STATIC_WORKERS': json.dumps(addresses), 'BENCH_RUN_ID': run_id,
            'WORKER_CAPABILITIES': json.dumps({k: ['simulation'] for k in addresses})}}}
    for i in range(workers):
        services[f'worker-{i}'] = {'image': image, 'restart': 'no',
            'command': ['python', '/harness/runtime.py', 'worker', '--worker-id', str(1000 + i)],
            'environment': {'BENCH_RUN_ID': run_id, 'INFERENCE_BACKEND': 'simulation'}}
    return {'services': services}


def call(args, *, timeout=90, log=None, cwd=None):
    result = subprocess.run(args, cwd=cwd, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=timeout)
    if log:
        Path(log).write_text(result.stdout)
    if result.returncode:
        raise RuntimeError(f'Command failed ({result.returncode}): {args[0]}; see {log or result.stdout[-800:]}')
    return result.stdout.strip()


def request(base, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, headers={'Content-Type': 'application/json'})
    client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with client.open(req, timeout=5) as response:
        value = response.read().decode()
    return value if path == '/metrics' else json.loads(value)


def gauges(text, prefix):
    return sum(float(line.rsplit(' ', 1)[1]) for line in text.splitlines()
               if line.startswith(prefix + ' ') or line.startswith(prefix + '{'))


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def fault_windows(path, killed, restored, target, slo, *, load_valid=True):
    """Use actual completion samples; never invent missing detection/recovery times."""
    samples = []
    returned_worker = []
    with gzip.open(path, 'rt') as stream:
        for line in stream:
            row = json.loads(line)
            if row.get('type') == 'Point' and row.get('metric') == 'attempt_latency_ms':
                data = row['data']
                samples.append((datetime.fromisoformat(data['time'].replace('Z', '+00:00')).timestamp(),
                                data['value'], data.get('tags', {}).get('outcome') == 'success'))
                if data.get('tags', {}).get('worker') == '1000' and samples[-1][2] and samples[-1][0] >= restored:
                    returned_worker.append(samples[-1][0])
    first = min((t for t, _, ok in samples if t >= killed and not ok), default=None)
    windows = []
    for i in range(6):
        start = restored + i * 10
        rows = [(ms, ok) for t, ms, ok in samples if start <= t < start + 10]
        good = [ms for ms, ok in rows if ok]
        p95 = percentile(good, .95)
        healthy = (len(rows) >= max(20, target * 10 * .9) and len(good) / max(1, len(rows)) >= .99
                   and p95 is not None and p95 <= slo)
        windows.append({'start_epoch_s': start, 'completed': len(rows), 'successes': len(good),
                        'success_p95_ms': p95, 'healthy': healthy})
    recovered = next((windows[i + 2]['start_epoch_s'] + 10 for i in range(4)
                      if all(w['healthy'] for w in windows[i:i + 3])), None)
    return {'first_failed_completion_after_signal_s': first - killed if first else None,
            'stable_recovery_after_restart_s': recovered - restored if recovered and load_valid else None,
            'first_success_from_restarted_worker_s': min(returned_worker) - restored if returned_worker else None,
            'windows': windows, 'rule': 'three consecutive 10-second healthy completion windows'}


class Experiment:
    def __init__(self, args):
        self.args = args
        self.run_id = 'bench-' + uuid.uuid4().hex[:12]
        self.base = f'http://127.0.0.1:{args.port}'
        self.out = args.output.resolve() / self.run_id
        self.out.mkdir(parents=True, exist_ok=False)
        self.report = {'run_id': self.run_id, 'started_at': datetime.now(timezone.utc).isoformat(),
                       'smoke_only': args.smoke, 'measurements': [], 'errors': [],
                       'topology': 'host k6 and Docker services share this machine; not an isolated load generator',
                       'slo': {'success_p95_ms': args.slo_ms, 'success_rate': .99, 'minimum_success_samples': 100},
                       'configuration': {k: v for k, v in vars(args).items() if isinstance(v, (str, int, float, bool, list))}}
        self.compose = None

    def save(self):
        path = self.out / 'report.json'
        temp = path.with_suffix('.tmp')
        temp.write_text(json.dumps(self.report, indent=2) + '\n')
        temp.replace(path)
        fields = ['workers', 'phase', 'target_rps', 'sent', 'successes', 'failures', 'rejected_503',
                  'deadline_504', 'transport_failures', 'success_rate', 'success_p95_ms',
                  'generator_dropped', 'valid_load', 'healthy', 'completion_rps_over_execution']
        with (self.out / 'summary.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(self.report['measurements'])

    def docker(self, *args, **kwargs):
        return call(['docker', 'compose', '-p', self.run_id, '-f', str(self.compose), *args], **kwargs)

    def prepare_image(self):
        repo = self.args.repo.resolve()
        revision = call(['git', 'rev-parse', 'HEAD'], cwd=repo)
        self.report['source_revision'] = revision
        self.report['working_tree_status'] = call(['git', 'status', '--short'], cwd=repo)
        self.report['source_policy'] = 'committed HEAD only; uncommitted edits and untracked files excluded'
        context = self.out / 'context'
        context.mkdir()
        # Only the committed production runtime is used, not unrelated working tree edits.
        archive = subprocess.check_output(['git', 'archive', revision, 'system', 'requirements-production.txt'], cwd=repo)
        with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
            for member in bundle.getmembers():
                if not (member.isfile() or member.isdir()) or '..' in Path(member.name).parts or member.name.startswith('/'):
                    raise ValueError('Unexpected member in source snapshot')
            bundle.extractall(context, filter='data')
        shutil.copy(HERE / 'runtime.py', context / 'runtime.py')
        (context / 'Dockerfile').write_text('FROM python:3.12-slim\nWORKDIR /app\n'
            'ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app\n'
            'COPY requirements-production.txt .\nRUN pip install --no-cache-dir -r requirements-production.txt\n'
            'COPY system ./system\nCOPY runtime.py /harness/runtime.py\n')
        self.image = 'inference-benchmark:' + self.run_id
        call(['docker', 'build', '-t', self.image, str(context)], timeout=900, log=self.out / 'build.log')
        self.report['image_id'] = call(['docker', 'image', 'inspect', '--format', '{{.Id}}', self.image])
        call(['docker', 'run', '--rm', self.image, 'pip', 'freeze'], log=self.out / 'dependencies.txt')
        self.report['harness_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                        for p in HERE.iterdir() if p.is_file()}
        self.report['k6_version'] = call([self.args.k6, 'version'])
        self.save()

    def up(self, workers):
        config = compose_config(workers, self.args.port, self.run_id, self.image)
        for name, service in config['services'].items():
            service['environment'].update({'BENCH_LATENCY_MS': str(self.args.latency_ms),
                'BENCH_BATCH': str(self.args.batch_size), 'BENCH_QUEUE': str(self.args.queue_size),
                'BENCH_SCHEDULER': self.args.scheduler,
                'WORKER_MAX_OUTSTANDING_BATCHES': str(self.args.worker_capacity)})
        self.compose = self.out / f'compose-{workers}.json'
        self.compose.write_text(json.dumps(config, indent=2))
        self.docker('up', '-d', timeout=120, log=self.out / f'up-{workers}.log')
        limit = time.monotonic() + 90
        while time.monotonic() < limit:
            try:
                verify_identity(request(self.base, '/benchmark/identity'), self.run_id)
                request(self.base, '/readyz')
                # An actual prediction exercises the gRPC route, not only proxy readiness.
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    replies = list(pool.map(lambda i: request(self.base, '/predict', {'x': i}), range(1, workers * 3 + 1)))
                if not all(r['result']['prediction'] == i * i and r['result']['benchmark']['run_id'] == self.run_id
                           for i, r in enumerate(replies, 1)):
                    raise RuntimeError('Warmup result mismatch')
                return
            except Exception:
                time.sleep(1)
        raise TimeoutError('Isolated stack did not become ready within 90 seconds')

    def idle(self):
        deadline = time.monotonic() + 60
        stable = 0
        while time.monotonic() < deadline:
            metrics = request(self.base, '/metrics')
            active = sum(gauges(metrics, key) for key in ('inference_api_queue_depth',
                         'inference_scheduler_pending_tasks',
                         'inference_worker_queued_batches', 'inference_worker_active_rpcs'))
            stable = stable + 1 if active == 0 else 0
            if stable >= 2:
                return
            time.sleep(1)
        raise TimeoutError('Queues did not drain; later stages would be contaminated')

    def stage(self, workers, rate, duration, phase, fault=False):
        verify_identity(request(self.base, '/benchmark/identity'), self.run_id)
        self.idle()
        folder = self.out / f'w{workers}-{len(self.report["measurements"]):02d}-{phase}-{rate}rps'
        folder.mkdir()
        env = {'RUN_ID': self.run_id, 'BASE_URL': self.base, 'RATE': str(rate), 'DURATION': str(duration),
               'MAX_VUS': str(self.args.max_vus), 'PHASE': phase, 'SUMMARY_PATH': str(folder / 'summary.json')}
        if self.args.preallocated_vus is not None:
            env['PREALLOCATED_VUS'] = str(self.args.preallocated_vus)
        cmd = [self.args.k6, 'run', '--quiet', '--no-usage-report']
        for key, value in env.items():
            cmd += ['-e', f'{key}={value}']
        if fault:
            cmd += ['--out', 'json=' + str(folder / 'samples.json.gz')]
        cmd.append(str(HERE / 'load-test.js'))
        timeline = []
        telemetry_errors = []
        stop = threading.Event()
        container_ids = self.docker('ps', '-q').split()

        def sample():
            with (folder / 'telemetry.jsonl').open('w') as output:
                n = 0
                while not stop.is_set():
                    row = {'epoch_s': time.time()}
                    try:
                        row['metrics'] = request(self.base, '/metrics')
                        if n % 5 == 0:
                            row['resources'] = call(['docker', 'stats', '--no-stream', '--format', '{{json .}}', *container_ids], timeout=10)
                    except Exception as exc:
                        telemetry_errors.append(str(exc))
                        row['error'] = str(exc)
                    output.write(json.dumps(row) + '\n')
                    output.flush()
                    n += 1
                    stop.wait(2)

        monitor = threading.Thread(target=sample, daemon=True)
        monitor.start()
        began = time.monotonic()
        killed = restored = None
        retries_before = 0
        with (folder / 'k6.log').open('w') as log:
            process = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
            try:
                while process.poll() is None:
                    elapsed = time.monotonic() - began
                    if fault and killed is None and elapsed >= 30:
                        verify_identity(request(self.base, '/benchmark/identity'), self.run_id)
                        retries_before = gauges(request(self.base, '/metrics'), 'inference_retry_submissions_total')
                        killed = time.time()
                        self.docker('kill', '-s', 'SIGKILL', 'worker-0')
                        timeline.append({'event': 'kill_worker', 'requested_epoch_s': killed, 'returned_epoch_s': time.time()})
                    if fault and killed is not None and restored is None and elapsed >= 60:
                        restore_requested = time.time()
                        self.docker('start', 'worker-0')
                        restored = time.time()
                        timeline.append({'event': 'restart_worker', 'requested_epoch_s': restore_requested, 'returned_epoch_s': restored})
                    if elapsed > duration + 60:
                        process.terminate()
                        raise TimeoutError('k6 exceeded stage duration plus drain allowance')
                    time.sleep(.2)
                code = process.wait()
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                if fault and killed is not None and restored is None:
                    self.docker('start', 'worker-0')
                stop.set()
                monitor.join(timeout=20)
        if code != 0:
            raise RuntimeError(f'k6 failed with exit {code}; inspect {folder / "k6.log"}')
        summary = json.loads((folder / 'summary.json').read_text())
        result = verdict(summary, rate, duration, self.args.slo_ms)
        result.update(workers=workers, phase=phase, elapsed_s=time.monotonic() - began,
                      timeline=timeline, telemetry_errors=telemetry_errors, path=str(folder))
        result['k6_execution_s'] = summary.get('state', {}).get('testRunDurationMs', 0) / 1000
        result['completion_rps_over_execution'] = (result['successes'] / result['k6_execution_s']
                                                  if result['k6_execution_s'] > 0 else None)
        if fault:
            if killed is None or restored is None:
                raise RuntimeError('Fault phase ended before kill/restart occurred')
            result['fault'] = fault_windows(folder / 'samples.json.gz', killed, restored, rate, self.args.slo_ms,
                                            load_valid=result['valid_load'])
            result['fault']['load_valid'] = result['valid_load']
            observations = [json.loads(line) for line in (folder / 'telemetry.jsonl').read_text().splitlines()]
            retry_at = next((row['epoch_s'] for row in observations if row['epoch_s'] >= killed
                             and gauges(row.get('metrics', ''), 'inference_retry_submissions_total') > retries_before), None)
            result['fault']['retry_observation_delay_s'] = retry_at - killed if retry_at else None
        self.report['measurements'].append(result)
        self.save()
        print(json.dumps({k: v for k, v in result.items() if k not in ('metrics', 'telemetry_errors')}), flush=True)
        return result

    def execute(self):
        self.prepare_image()
        for workers in self.args.workers:
            try:
                self.up(workers)
                self.stage(workers, self.args.rates[0], self.args.warmup, 'warmup')
                stages = [self.stage(workers, rate, self.args.duration, 'pressure') for rate in self.args.rates]
                # Recovery is explicit; the next pressure stage never inherits an old queue.
                recovery = self.stage(workers, self.args.rates[0], self.args.recovery_duration, 'recovery')
                healthy = [s for s in stages if s['healthy']]
                if not self.args.skip_fault:
                    if not healthy:
                        self.report['errors'].append(f'No qualified healthy baseline for {workers} workers; fault phase not run')
                    else:
                        self.stage(workers, max(s['target_rps'] for s in healthy), 130, 'fault', fault=True)
                self.report.setdefault('capacity', []).append({
                    'workers': workers, 'max_tested_healthy_rps': max((s['target_rps'] for s in healthy), default=None),
                    'first_valid_unhealthy_rps': next((s['target_rps'] for s in stages if s['service_slo_breached']), None),
                    'overload_observed': any(s['service_slo_breached'] for s in stages),
                    'recovery_healthy': recovery['healthy'],
                    'generator_limited_rates': [s['target_rps'] for s in stages if not s['valid_load']]})
            finally:
                if self.compose:
                    try:
                        self.docker('logs', '--no-color', timeout=30, log=self.out / f'services-{workers}.log')
                    except Exception as exc:
                        self.report['errors'].append('Log collection failed: ' + str(exc))
                    try:
                        self.docker('down', '--remove-orphans', timeout=90)
                    except Exception as exc:
                        self.report['errors'].append('Cleanup failed: ' + str(exc))
            self.save()
        self.report['experiment_completed'] = not self.report['errors']
        self.report['finished_at'] = datetime.now(timezone.utc).isoformat()
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=HERE.parents[1])
    parser.add_argument('--workers', nargs='+', type=int, default=[4])
    parser.add_argument('--rates', nargs='+', type=int, default=[10, 25, 50, 100, 200, 400, 800, 1600, 3200])
    parser.add_argument('--duration', type=int, default=120)
    parser.add_argument('--warmup', type=int, default=20)
    parser.add_argument('--recovery-duration', type=int, default=60)
    parser.add_argument('--slo-ms', type=float, default=250)
    parser.add_argument('--latency-ms', type=float, default=60)
    parser.add_argument('--batch-size', type=int, default=1)
    parser.add_argument('--queue-size', type=int, default=1000)
    parser.add_argument('--worker-capacity', type=int, default=3)
    parser.add_argument('--scheduler', choices=['fifo', 'batching', 'latency_aware'], default='fifo')
    parser.add_argument('--max-vus', type=int, default=1024)
    parser.add_argument('--preallocated-vus', type=int)
    parser.add_argument('--port', type=int, default=18180)
    parser.add_argument('--output', type=Path, default=HERE / 'results')
    parser.add_argument('--k6', default=shutil.which('k6') or '/opt/homebrew/bin/k6')
    parser.add_argument('--skip-fault', action='store_true')
    parser.add_argument('--execute', action='store_true', help='Build and run the isolated experiment')
    parser.add_argument('--smoke', action='store_true', help='Short protocol check; not a capacity claim')
    args = parser.parse_args()
    if (any(n < 1 or n > 32 for n in args.workers) or len(set(args.workers)) != len(args.workers)
            or args.rates != sorted(set(args.rates)) or any(r < 1 for r in args.rates)
            or min(args.duration, args.warmup, args.recovery_duration, args.max_vus, args.batch_size, args.queue_size, args.worker_capacity) < 1
            or (args.preallocated_vus is not None and not 1 <= args.preallocated_vus <= args.max_vus)
            or not 1024 <= args.port <= 65535 or not math.isfinite(args.slo_ms) or args.slo_ms <= 0
            or not math.isfinite(args.latency_ms) or args.latency_ms <= 0):
        parser.error('Invalid experiment parameters')
    if args.smoke:
        args.workers, args.rates, args.duration, args.warmup, args.recovery_duration, args.skip_fault = [2], [2, 8], 3, 3, 3, True
    if not args.execute:
        print(json.dumps({**vars(args), 'note': 'Plan only. Add --execute to run. No production services are targeted.'}, default=str, indent=2))
        return 0
    experiment = Experiment(args)
    try:
        experiment.execute()
    except (Exception, KeyboardInterrupt) as exc:
        experiment.report['errors'].append(type(exc).__name__ + ': ' + str(exc))
        experiment.save()
        print(f'Experiment incomplete: {exc}', flush=True)
        return 1
    finally:
        print('REPORT_PATH=' + str(experiment.out / 'report.json'), flush=True)
    return 0 if experiment.report.get('experiment_completed') else 1


if __name__ == '__main__':
    raise SystemExit(main())
