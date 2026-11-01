#!/usr/bin/env python3
"""Run runtime recovery acceptance in disposable local Docker containers."""
import argparse
import json
from pathlib import Path
import subprocess
import time
import uuid


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image',required=True,help='Local platform/CI image with production dependencies')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[1]
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=False)
    suffix=uuid.uuid4().hex[:10]
    sql,prom,runner=['health-acceptance-'+suffix+'-'+name for name in ('sql','prom','runner')]
    def docker(*values,check=True,**kwargs):
        return subprocess.run(['docker',*values],check=check,text=True,**kwargs)
    def capture(*values):return docker(*values,capture_output=True).stdout.strip()
    states=[]
    config=output/'prometheus.yml'
    config.write_text('''global:
  scrape_interval: 5s
  scrape_timeout: 3s
  evaluation_interval: 5s
rule_files:
  - /etc/prometheus/rules/runtime.yml
scrape_configs:
  - job_name: inference-gateway
    static_configs:
      - targets: ['127.0.0.1:8000']
''')
    log=None
    try:
        metadata={'image':args.image,'image_id':capture('image','inspect',args.image,'--format','{{.Id}}'),
                  'defaults':{'loop_timeout_seconds':10,'alert_for_seconds':30,'lease_seconds':120,'renew_interval_seconds':30},
                  'scope':'local disposable SQL, API subprocesses, fixture gRPC worker and real Prometheus'}
        (output/'environment.json').write_text(json.dumps(metadata,indent=2)+'\n')
        docker('run','-d','--name',sql,'-e','POSTGRES_PASSWORD=disposable-health','-e','POSTGRES_DB=synthx_platform_ci_health','postgres:16-alpine',stdout=subprocess.DEVNULL)
        for _ in range(100):
            if docker('exec',sql,'pg_isready','-h','127.0.0.1','-U','postgres',check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:break
            time.sleep(.3)
        else:raise TimeoutError('disposable PostgreSQL readiness')
        with (root/'deploy/ci/schema.sql').open() as fixture:
            docker('exec','-i',sql,'psql','-U','postgres','-d','synthx_platform_ci_health','-v','ON_ERROR_STOP=1',stdin=fixture,stdout=subprocess.DEVNULL)
        docker('run','-d','--name',prom,'--network','container:'+sql,
               '-v',str(config)+':/etc/prometheus/prometheus.yml:ro',
               '-v',str(root/'monitoring/rules')+':/etc/prometheus/rules:ro',
               'prom/prometheus:v3.5.0','--config.file=/etc/prometheus/prometheus.yml',stdout=subprocess.DEVNULL)
        command=['docker','run','--name',runner,'--network','container:'+sql,
                 '--health-cmd','python -m system.health_probe','--health-interval','5s',
                 '--health-timeout','3s','--health-retries','3','--health-start-period','20s',
                 '-v',str(root)+':/app:ro','-v',str(output)+':/results',
                 '-e','PYTHONPATH=/app:/app/tests','-e','PYTHONDONTWRITEBYTECODE=1',
                 '-e','SYNTHX_TEST_DATABASE_URL=postgresql://postgres:disposable-health@127.0.0.1:5432/synthx_platform_ci_health',
                 args.image,'sh','-c','pip install --quiet httpx && python /app/experiments/runtime_health_acceptance.py']
        log=(output/'runner.log').open('w')
        process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
        until=time.monotonic()+260
        while process.poll() is None:
            if time.monotonic()>until:
                docker('kill',runner,check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                raise TimeoutError('acceptance exceeded 260 seconds')
            result=docker('inspect',runner,'--format','{{json .State.Health}}',check=False,capture_output=True)
            if result.returncode==0:
                health=json.loads(result.stdout or 'null')
                status=health.get('Status') if health else None
                if not states or states[-1]['status']!=status:
                    states.append({'epoch':time.time(),'status':status,'health':health})
                    (output/'container-health.json').write_text(json.dumps(states,indent=2)+'\n')
            time.sleep(1)
        if process.returncode:raise RuntimeError('acceptance failed; inspect runner.log')
        report=json.loads((output/'report.json').read_text())
        assert report['success']
        assert any(item['status']=='unhealthy' for item in states),'Docker never observed unhealthy state'
        first_bad=next(i for i,item in enumerate(states) if item['status']=='unhealthy')
        assert any(item['status']=='healthy' for item in states[first_bad+1:]),'Docker health never recovered'
        report['checks']['docker_health_failed_and_recovered']=True
        (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps({'success':True,'output':str(output),'checks':report['checks']},indent=2))
    finally:
        if log:log.close()
        for name in (prom,runner):
            result=docker('logs',name,check=False,capture_output=True)
            (output/(name.rsplit('-',1)[-1]+'.container.log')).write_text(result.stdout+result.stderr)
        for name in (runner,prom,sql):
            docker('rm','-f',name,check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

if __name__=='__main__':main()
