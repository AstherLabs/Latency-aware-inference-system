#!/usr/bin/env python3
"""Asset-free recovery acceptance inside a disposable Docker network namespace.

Requires a disposable PostgreSQL DSN and Prometheus on loopback:9090. Starts real
API subprocesses and a gRPC fixture, never a production model or public service.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid
from concurrent import futures
from datetime import datetime, timedelta, timezone

import grpc
import httpx
import psycopg
from psycopg.types.json import Jsonb
from system import inference_pb2, inference_pb2_grpc
from system.job_store import JobStore


def main():
    dsn = os.environ['SYNTHX_TEST_DATABASE_URL']
    if '127.0.0.1' not in dsn or '/synthx_platform_ci' not in dsn:
        raise ValueError('acceptance requires an explicitly disposable loopback database')
    output = Path(os.environ.get('ACCEPTANCE_RESULTS', '/results'))
    output.mkdir(parents=True, exist_ok=True)
    work = output / 'control'
    work.mkdir(exist_ok=True)
    stall, entered = work/'stall', work/'entered'
    report = {'scenario':'scheduler_stall_process_restart_sql_recovery',
              'model':'asset-free gRPC fixture', 'events':[], 'checks':{}}
    start = time.monotonic()
    def event(name, **values):
        record={'event':name,'elapsed_seconds':round(time.monotonic()-start,3),
                'utc':datetime.now(timezone.utc).isoformat(),**values}
        report['events'].append(record)
        (output/'progress.json').write_text(json.dumps(report,indent=2,default=str))
        print(json.dumps(record,default=str),flush=True)
    def wait_for(fn, timeout, label):
        until=time.monotonic()+timeout
        while time.monotonic()<until:
            result=fn()
            if result:return result
            time.sleep(.2)
        raise TimeoutError(label)
    client=httpx.Client(timeout=2,trust_env=False)
    store=JobStore(dsn)
    calls=[]
    held_job_target = [None]
    recovered_rpc_started, release_recovered_rpc = threading.Event(), threading.Event()
    calls_lock=threading.Lock()
    class Echo(inference_pb2_grpc.WorkerServiceServicer):
        def ProcessBatch(self, request, context):
            results=[]
            for task in request.tasks:
                payload=json.loads(task.input_json)
                with calls_lock:calls.append({'job_id':payload['spec']['job_id'],'attempt_id':payload['attempt_id'],
                                              'received_at':datetime.now(timezone.utc).isoformat()})
                if payload['spec']['job_id'] == held_job_target[0]:
                    recovered_rpc_started.set()
                    if not release_recovered_rpc.wait(15):
                        context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, 'acceptance did not release recovered call')
                results.append(inference_pb2.TaskResult(task_id=task.task_id,success=True,
                    result_json=json.dumps({'job_id':payload['spec']['job_id'],'from_rpc':True})))
            return inference_pb2.BatchResponse(results=results)
    server=grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    inference_pb2_grpc.add_WorkerServiceServicer_to_server(Echo(),server)
    port=server.add_insecure_port('127.0.0.1:0');server.start()
    token=uuid.uuid4().hex
    headers={'Authorization':'Bearer '+token}
    claim_log=output/'claims.jsonl'
    env=dict(os.environ,SYNTHX_JOBS_ENABLED='1',USE_GRPC='1',DATABASE_URL=dsn,
             INFERENCE_SERVICE_TOKEN=token,STATIC_WORKERS=json.dumps({'fixture':f'127.0.0.1:{port}'}),
             WORKER_CAPABILITIES=json.dumps({'fixture':['synthx_v73']}),SYNTHX_JOBS_MAX_INFLIGHT='1',
             LOOP_HEALTH_TIMEOUT_SECONDS='10',ACCEPTANCE_STALL_FILE=str(stall),
             ACCEPTANCE_STALL_ENTERED_FILE=str(entered),ACCEPTANCE_CLAIM_LOG=str(claim_log))
    process=None
    handles=[]
    def boot(number):
        log=open(output/f'api-{number}.log','w');handles.append(log)
        child=subprocess.Popen([sys.executable,'/app/experiments/fixtures/runtime_health_api.py'],env=env,stdout=log,stderr=subprocess.STDOUT)
        event('api_started',generation=number,pid=child.pid)
        return child
    def health():
        try:
            r=client.get('http://127.0.0.1:8000/health/runtime')
            return r.status_code,r.json()
        except (httpx.HTTPError,ValueError):return None,None
    def alerts():
        r=client.get('http://127.0.0.1:9090/api/v1/alerts');r.raise_for_status()
        return r.json()['data']['alerts']
    def scheduler_firing():
        return [a for a in alerts() if a['labels'].get('alertname')=='InferenceRuntimeLoopStalled'
                and a['labels'].get('loop')=='scheduler' and a['state']=='firing']
    project=str(uuid.uuid4());jobs=[]
    def create_job():
        job=str(uuid.uuid4());deadline=datetime.now(timezone.utc)+timedelta(seconds=600)
        spec={'job_id':job,'project_id':project,'user_id':'health-acceptance','model':'v7_3_full'}
        digest=hashlib.sha256(json.dumps(spec,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        with psycopg.connect(dsn) as conn:
            conn.execute("INSERT INTO synthx.inference_jobs(job_id,project_id,user_id,model,spec,spec_sha256,status,deadline_at) VALUES (%s,%s,'health-acceptance','v7_3_full',%s,%s,'queued',%s)",(job,project,Jsonb(spec),digest,deadline))
        jobs.append((job,deadline))
        return job
    def submit(job):
        response=client.post('http://127.0.0.1:8000/jobs',headers=headers,json={'job_id':job})
        assert response.status_code==202,response.text
    try:
        with psycopg.connect(dsn) as conn:
            conn.execute("INSERT INTO synthx.projects(id,user_id,title,created_at) VALUES (%s,'health-acceptance','disposable runtime acceptance',now())",(project,))
        process=boot(1)
        wait_for(lambda:health()[0]==200,15,'initial API health')
        normal=create_job();submit(normal)
        wait_for(lambda:store.get(normal)['status']=='succeeded',10,'normal completion')
        event('baseline_completed',job_id=normal)
        stall.write_text('stall next dispatch\n')
        fault_jobs=[create_job() for _ in range(3)]
        with futures.ThreadPoolExecutor(max_workers=3) as submitters:
            list(submitters.map(submit,fault_jobs))
        wait_for(entered.exists,5,'scheduler entered controlled stall')
        held=next(store.get(job) for job in fault_jobs if store.get(job)['status']=='running')
        old_attempt,held_job,old_lease=held['attempt_id'],held['job_id'],held['lease_until']
        held_job_target[0]=held_job
        event('scheduler_stalled',job_id=held_job,attempt_id=old_attempt,lease_until=old_lease)
        probe_start=time.monotonic()
        status,snapshot=wait_for(lambda:health() if health()[0]==503 else None,20,'runtime health detects stall')
        event('runtime_unhealthy',detection_seconds=round(time.monotonic()-probe_start,3),snapshot=snapshot)
        ready=client.get('http://127.0.0.1:8000/readyz')
        assert ready.status_code==503
        firing=wait_for(scheduler_firing,50,'sustained scheduler alert fires')
        event('alert_firing',alerts=firing)
        probe=subprocess.run([sys.executable,'-m','system.health_probe'],capture_output=True)
        assert probe.returncode==1
        current=store.get(held_job)
        assert current['attempt_id']==old_attempt and current['lease_until']==old_lease
        assert current['status']=='running' and current['attempts']==1
        assert current['lease_until']>datetime.now(timezone.utc)
        report['checks'].update(runtime_detected=True,readiness_rejected=True,alert_fired=True,
            probe_failed=True,no_renewal_after_stall=True,no_early_lease_release=True)
        event('restart_requested')
        process.terminate()
        try:process.wait(timeout=12);mode='sigterm'
        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5);mode='sigkill_after_grace'
        event('api_stopped',mode=mode,returncode=process.returncode)
        stall.unlink()
        process=boot(2)
        wait_for(lambda:health()[0]==200,15,'replacement API health')
        current=store.get(held_job)
        assert current['attempt_id']==old_attempt and current['attempts']==1
        event('replacement_healthy_waiting_for_lease',lease_until=current['lease_until'])
        def takeover_started():
            row=store.get(held_job)
            if datetime.now(timezone.utc) < old_lease:
                assert row['attempt_id']==old_attempt and row['attempts']==1, 'live lease reclaimed early'
            return recovered_rpc_started.is_set()
        wait_for(takeover_started,135,'replacement dispatch after lease expiry')
        running=store.get(held_job)
        assert running['status']=='running' and running['lease_until']>datetime.now(timezone.utc)
        assert running['attempt_id']!=old_attempt and running['attempts']==2
        claims=[json.loads(line) for line in claim_log.read_text().splitlines()]
        claim=next(item for item in claims if item['attempt_id']==running['attempt_id'])
        assert datetime.fromisoformat(claim['claimed_at'])>=old_lease, 'claim preceded original lease expiry'
        assert not store.complete(held_job,old_attempt,result={'stale':True})
        after_stale=store.get(held_job)
        assert after_stale['status']=='running' and after_stale['attempt_id']==running['attempt_id']
        assert after_stale['result'] is None
        report['claims']=claims
        event('stale_result_rejected_while_replacement_running',claimed_at=claim['claimed_at'],old_lease=old_lease)
        release_recovered_rpc.set()
        wait_for(lambda:all(store.get(job)['status']=='succeeded' for job in fault_jobs),10,'all durable jobs recover')
        recovered=store.get(held_job)
        assert store.get(held_job)['result']=={'job_id':held_job,'from_rpc':True}
        for job,deadline in jobs:
            row=store.get(job)
            assert row['deadline_at']==deadline
            assert row['status']=='succeeded'
            response=client.get('http://127.0.0.1:8000/jobs/'+job,headers=headers)
            assert response.status_code==200 and response.json()['status']=='succeeded'
        wait_for(lambda:not scheduler_firing(),15,'scheduler alert resolves')
        report['checks'].update(restart_completed=True,live_lease_not_reclaimed=True,
            jobs_completed=True,total_deadlines_preserved=True,stale_result_fenced=True,alert_resolved=True)
        report['jobs']=[{**store.get(job),'expected_deadline':deadline} for job,deadline in jobs]
        report['rpc_calls']=calls
        report['success']=True
        event('acceptance_passed',jobs=len(jobs),recovered_attempts=recovered['attempts'])
    except BaseException as exc:
        report['success']=False;report['error']=repr(exc);event('acceptance_failed',error=repr(exc));raise
    finally:
        release_recovered_rpc.set()
        if process and process.poll() is None:
            process.terminate()
            try:process.wait(timeout=12)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
        server.stop(0).wait();client.close()
        for handle in handles:handle.close()
        (output/'report.json').write_text(json.dumps(report,indent=2,default=str)+'\n')

if __name__=='__main__':main()
