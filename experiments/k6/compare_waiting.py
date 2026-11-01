#!/usr/bin/env python3
"""Compare hashed working-tree snapshots in isolated local benchmark stacks."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import socket
import threading
import time
from types import SimpleNamespace

from run import Experiment, call, gauges, request

HERE = Path(__file__).resolve().parent
PROC_SAMPLE = """import json,os,time
from pathlib import Path
stat=Path('/proc/1/stat').read_text().split(') ',1)[1].split()
status=dict(line.split(':',1) for line in Path('/proc/1/status').read_text().splitlines() if ':' in line)
print(json.dumps({'epoch_s':time.time(),'cpu_seconds':(int(stat[11])+int(stat[12]))/os.sysconf('SC_CLK_TCK'),'rss_mib':int(status['VmRSS'].split()[0])/1024,'threads':int(status['Threads']),'fds':len(list(Path('/proc/1/fd').iterdir()))}))
"""


def snapshot(source, destination):
    """Copy runtime source only; retain every byte's hash, including uncommitted edits."""
    destination.mkdir(parents=True)
    shutil.copytree(source / 'system', destination / 'system', ignore=shutil.ignore_patterns('__pycache__'))
    shutil.copy2(source / 'requirements-production.txt', destination)
    shutil.copy2(HERE / 'runtime.py', destination)
    manifest = {str(p.relative_to(destination)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(destination.rglob('*')) if p.is_file()}
    return manifest


class MeasuredExperiment(Experiment):
    def docker(self, *args, **kwargs):
        if args and args[0] == 'up':
            document = json.loads(self.compose.read_text())
            document['services']['api']['environment']['MAX_SYNC_REQUESTS'] = str(self.args.sync_limit)
            self.compose.write_text(json.dumps(document, indent=2))
        return super().docker(*args, **kwargs)

    def resources(self):
        row = json.loads(self.docker('exec', '-T', 'api', 'python', '-c', PROC_SAMPLE, timeout=10))
        metrics = request(self.base, '/metrics')
        for name in ('inference_sync_requests_active', 'inference_api_queue_depth',
                     'inference_scheduler_pending_tasks', 'inference_worker_queued_batches',
                     'inference_sync_cancellations_total'):
            row[name] = gauges(metrics, name)
        return row

    def stage(self, *args, **kwargs):
        before = self.resources()
        rows, errors, stop = [before], [], threading.Event()
        def sample():
            while not stop.wait(2):
                try: rows.append(self.resources())
                except Exception as error: errors.append(str(error))
        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            result = super().stage(*args, **kwargs)
        finally:
            stop.set()
            sampler.join(timeout=15)
        after = self.resources()
        rows.append(after)
        duration = after['epoch_s'] - before['epoch_s']
        result.update(api_cpu_seconds=after['cpu_seconds']-before['cpu_seconds'],
                      resource_window_seconds=duration,
                      api_peak_rss_mib=max(row['rss_mib'] for row in rows),
                      api_max_threads=max(row['threads'] for row in rows),
                      resource_errors=errors)
        Path(result['path'], 'api-resources.json').write_text(json.dumps(rows, indent=2))
        self.save()
        return result


def disconnect_check(experiment):
    """Close real sockets after admission; require pending ephemeral work to disappear."""
    host, port = '127.0.0.1', experiment.args.port
    sockets = []
    try:
        body = b'{"x":3}'
        wire = (b'POST /predict HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n'
                + f'Content-Length: {len(body)}\r\n\r\n'.encode() + body)
        for _ in range(80):
            connection = socket.create_connection((host, port), timeout=2)
            connection.sendall(wire)
            sockets.append(connection)
        time.sleep(.25)
        before = experiment.resources()
    finally:
        began = time.monotonic()
        for connection in sockets:
            connection.close()
    observations = []
    while time.monotonic()-began < 5:
        row = experiment.resources()
        observations.append(row)
        if (row['inference_sync_requests_active'] == 0 and row['inference_api_queue_depth'] == 0
                and row['inference_scheduler_pending_tasks'] == 0 and row['inference_worker_queued_batches'] == 0):
            cancelled = row['inference_sync_cancellations_total'] - before['inference_sync_cancellations_total']
            proved_cancellation = cancelled >= max(1, before['inference_sync_requests_active'] / 2)
            return {'passed':proved_cancellation,'opened':len(sockets),'before':before,'cancellation_delta':cancelled,
                    'observed_cleanup_seconds':time.monotonic()-began,'samples':observations}
        time.sleep(.1)
    return {'passed':False,'opened':len(sockets),'before':before,'samples':observations}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-source', required=True, type=Path)
    parser.add_argument('--candidate-source', type=Path, default=HERE.parents[1])
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--runtime-image', required=True, help='Local image with production dependencies; no production containers are used')
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--duration', type=int, default=15)
    parser.add_argument('--port', type=int, default=18191)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    if args.repeats < 1 or args.duration < 1 or not 1024 <= args.port <= 65535:
        parser.error('Invalid experiment parameters')
    if not args.execute:
        print(json.dumps(vars(args), default=str, indent=2))
        return
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    report = {'complete':False, 'scope':'local simulation; shared host generator and services',
              'variants':{}, 'runs':[], 'errors':[]}
    def save(): (root/'comparison.json').write_text(json.dumps(report, indent=2))
    images = {}
    for variant, source in [('polling',args.baseline_source), ('notification',args.candidate_source)]:
        context = root / ('context-'+variant)
        manifest = snapshot(source.resolve(), context)
        digest = hashlib.sha256(json.dumps(manifest,sort_keys=True).encode()).hexdigest()
        image = 'inference-wait-ab:' + variant + '-' + digest[:12]
        (context/'Dockerfile').write_text(f'FROM {args.runtime_image}\nCOPY system /app/system\nCOPY runtime.py /harness/runtime.py\n')
        call(['docker','build','-t',image,str(context)],timeout=300,log=root/(variant+'-build.log'))
        images[variant] = image
        report['variants'][variant] = {'manifest':manifest,'sha256':digest,
            'image_id':call(['docker','image','inspect','--format','{{.Id}}',image])}
        save()
    configurations = []
    for repeat in range(args.repeats):
        order = ['polling','notification'] if repeat % 2 == 0 else ['notification','polling']
        configurations.extend((variant,repeat,1024,'comparison') for variant in order)
    configurations.append(('notification',0,32,'admission_guard'))
    for variant, repeat, limit, purpose in configurations:
        settings = SimpleNamespace(repo=args.candidate_source, workers=[2], rates=[10,50],
            port=args.port, output=root/'runs', k6=shutil.which('k6') or '/opt/homebrew/bin/k6',
            slo_ms=250, latency_ms=60, batch_size=1, queue_size=1000, worker_capacity=3,
            scheduler='fifo', max_vus=512, preallocated_vus=512, smoke=False, sync_limit=limit)
        experiment = MeasuredExperiment(settings)
        experiment.image = images[variant]
        experiment.report.update(variant=variant,repeat=repeat,purpose=purpose,source=report['variants'][variant])
        try:
            experiment.up(2)
            experiment.stage(2,10,3,'warmup')
            if purpose == 'comparison': experiment.stage(2,10,10,'normal')
            experiment.stage(2,50,args.duration,'overload')
            experiment.stage(2,10,10,'recovery')
            if purpose == 'admission_guard':
                experiment.report['disconnect_check'] = disconnect_check(experiment)
                if not experiment.report['disconnect_check']['passed']:
                    raise RuntimeError('Real socket disconnect cleanup failed')
            experiment.report['experiment_completed'] = True
        except Exception as error:
            experiment.report['errors'].append(str(error))
            report['errors'].append(f'{variant}/{repeat}/{purpose}: {error}')
            raise
        finally:
            experiment.save()
            if experiment.compose:
                try: experiment.docker('logs','--no-color',log=experiment.out/'services.log')
                finally: experiment.docker('down','--remove-orphans',timeout=90)
            report['runs'].append({'variant':variant,'repeat':repeat,'purpose':purpose,
                                   'report':str(experiment.out/'report.json')})
            save()
        print(json.dumps({'completed':variant,'repeat':repeat,'purpose':purpose,
                          'measurements':experiment.report['measurements']},default=str),flush=True)
    report['complete'] = not report['errors']
    save()
    print('COMPARISON_PATH='+str(root/'comparison.json'),flush=True)


if __name__ == '__main__': main()
