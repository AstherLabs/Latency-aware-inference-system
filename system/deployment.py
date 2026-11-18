"""Process-local admission and active handler accounting for approved rollouts."""
import json
import os
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

DRAIN_FILE = Path(os.environ.get('PLATFORM_DRAIN_FILE', '/run/synthx-deploy/drain'))


class DeploymentGate:
    def __init__(self, marker=DRAIN_FILE):
        self.marker=Path(marker)
        self._active=0
        self._lock=threading.Lock()

    def marker_state(self):
        try:return True,self.marker.read_text().strip()
        except FileNotFoundError:return False,None
        except (OSError,UnicodeError):return True,None

    def enter(self):
        with self._lock:
            if self.marker_state()[0]:return False
            self._active+=1
            return True

    def leave(self):
        with self._lock:self._active-=1

    def enter_existing(self):
        """Count already admitted durable work, including recovery during drain."""
        with self._lock:
            self._active += 1

    def snapshot(self):
        with self._lock:
            draining,token=self.marker_state()
            return {'protocol':1,'draining':draining,'drain_id':token,'active':self._active}


class DeploymentMiddleware:
    def __init__(self,app,gate):self.app,self.gate=app,gate

    async def __call__(self,scope,receive,send):
        if scope['type']!='http' or scope['method'] in {'GET','HEAD','OPTIONS'}:
            return await self.app(scope,receive,send)
        if not self.gate.enter():
            from starlette.responses import JSONResponse
            response=JSONResponse({'code':'DEPLOYMENT_DRAINING','message':'Inference service is updating. Retry shortly.'},
                                  status_code=503,headers={'Retry-After':'30'})
            return await response(scope,receive,send)
        try:return await self.app(scope,receive,send)
        finally:self.gate.leave()


def start_worker_health(gate,port):
    """Enabled only for an EC2 adapter; never publishes a host-facing port."""
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            if self.path!='/health/deployment':
                self.send_error(404);return
            body=json.dumps(gate.snapshot()).encode()
            self.send_response(200);self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    server=ThreadingHTTPServer(('127.0.0.1',port),Handler)
    server.daemon_threads=True
    threading.Thread(target=server.serve_forever,daemon=True,name='AdapterDeploymentHealth').start()
    return server
