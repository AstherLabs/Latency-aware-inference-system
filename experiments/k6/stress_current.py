#!/usr/bin/env python3
"""Sustained overload and worker outages against the current isolated runtime."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import threading
import time
from types import SimpleNamespace

from compare_waiting import MeasuredExperiment, snapshot, disconnect_check
from run import call, gauges, request, verify_identity


class StressExperiment(MeasuredExperiment):
    def resources(self):
        row = super().resources()
        if self.generator is not None and self.generator.poll() is None:
            try:
                raw = call(['ps','-p',str(self.generator.pid),'-o','rss=,pcpu='],timeout=3).split()
                row['generator_rss_mib'] = int(raw[0])/1024
                row['generator_cpu_percent'] = float(raw[1])
            except Exception as error:
                row['generator_resource_error'] = str(error)
        return row

    def faults(self, stop, initial_requests):
        """Use actual observed arrivals and actual command times, not assumed clocks."""
        while not stop.wait(.2):
            if gauges(request(self.base,'/metrics'),'predict_requests_total') > initial_requests:
                began = time.monotonic()
                self.report['soak_first_observed_arrival_epoch_s'] = time.time()
                break
        else: return
        events=[(300,'kill',['worker-0']), (330,'start',['worker-0']),
                (600,'kill',[f'worker-{i}' for i in range(4)]),
                (610,'start',[f'worker-{i}' for i in range(4)])]
        for offset,action,workers in events:
            if stop.wait(max(0,offset-(time.monotonic()-began))): return
            verify_identity(request(self.base,'/benchmark/identity'),self.run_id)
            row={'action':action,'workers':workers,'requested_epoch_s':time.time(),
                 'offset_from_first_observed_arrival_s':time.monotonic()-began}
            try:
                if action=='kill': self.docker('kill','-s','SIGKILL',*workers,timeout=30)
                else: self.docker('start',*workers,timeout=30)
                row['returned_epoch_s']=time.time()
            except Exception as error:
                row['error']=str(error)
            self.report.setdefault('fault_timeline',[]).append(row)
            self.save()
            print(json.dumps({'fault_event':row}),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument('--runtime-image',required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--duration',type=int,default=900)
    parser.add_argument('--port',type=int,default=18195)
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args()
    if args.duration<650 or not 1024<=args.port<=65535: parser.error('Duration must cover all fault events; invalid port')
    if not args.execute:
        print(json.dumps(vars(args),default=str,indent=2));return
    settings=SimpleNamespace(repo=args.source,workers=[4],rates=[40,100,400],port=args.port,
        output=args.output,k6=shutil.which('k6') or '/opt/homebrew/bin/k6',slo_ms=250,
        latency_ms=60,batch_size=1,queue_size=1000,worker_capacity=3,scheduler='fifo',
        max_vus=2048,preallocated_vus=2048,smoke=False,sync_limit=1024)
    experiment=StressExperiment(settings)
    experiment.generator=None
    # Preserve existing stage orchestration while observing the real k6 PID.
    original_popen=subprocess.Popen
    def observed_popen(command,*positional,**named):
        process=original_popen(command,*positional,**named)
        if isinstance(command,list) and command and command[0]==settings.k6:
            experiment.generator=process
        return process
    subprocess.Popen=observed_popen
    context=experiment.out/'context-current'
    manifest=snapshot(args.source.resolve(),context)
    digest=hashlib.sha256(json.dumps(manifest,sort_keys=True).encode()).hexdigest()
    experiment.image='inference-current-stress:'+digest[:12]
    (context/'Dockerfile').write_text(f'FROM {args.runtime_image}\nCOPY system /app/system\nCOPY runtime.py /harness/runtime.py\n')
    call(['docker','build','-t',experiment.image,str(context)],timeout=300,log=experiment.out/'build.log')
    experiment.report.update(source_sha256=digest,source_manifest=manifest,
        image_id=call(['docker','image','inspect','--format','{{.Id}}',experiment.image]),
        k6_version=call([settings.k6,'version']),
        scope='current runtime only; four simulated workers; local shared-host sustained overload',
        soak_target_rps=400,soak_duration_s=args.duration)
    stop=threading.Event();controller=None
    try:
        experiment.up(4)
        experiment.stage(4,40,10,'warmup')
        for rate in [40,100,400]: experiment.stage(4,rate,30,'ramp')
        initial=gauges(request(experiment.base,'/metrics'),'predict_requests_total')
        controller=threading.Thread(target=experiment.faults,args=(stop,initial),daemon=True)
        controller.start()
        experiment.stage(4,400,args.duration,'soak')
        stop.set();controller.join(timeout=35)
        experiment.stage(4,40,60,'recovery')
        experiment.report['disconnect_check']=disconnect_check(experiment)
        if not experiment.report['disconnect_check']['passed']: raise RuntimeError('Disconnect cleanup failed')
        events=experiment.report.get('fault_timeline',[])
        rows=experiment.report['measurements']
        experiment.report['acceptance']={
            'all_load_delivered':all(row['valid_load'] for row in rows),
            'fault_commands_completed':len(events)==4 and not any('error' in row for row in events),
            'no_unexpected_request_errors':all(row['transport_failures']==0 and row['other_service_failures']==0 for row in rows),
            'observability_available':all(not row['telemetry_errors'] and not row['resource_errors'] for row in rows),
            'recovery_meets_slo':rows[-1]['healthy'],
            'disconnect_cleanup':experiment.report['disconnect_check']['passed'],
        }
        experiment.report['platform_acceptance_passed']=all(experiment.report['acceptance'].values())
        experiment.report['experiment_completed']=True
    except (Exception,KeyboardInterrupt) as error:
        experiment.report['errors'].append(type(error).__name__+': '+str(error))
        raise
    finally:
        stop.set()
        if controller: controller.join(timeout=35)
        experiment.save()
        if experiment.compose:
            try: experiment.docker('logs','--no-color',log=experiment.out/'services.log',timeout=45)
            finally: experiment.docker('down','--remove-orphans',timeout=90)
        print('REPORT_PATH='+str(experiment.out/'report.json'),flush=True)


if __name__=='__main__': main()
