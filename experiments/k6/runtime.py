"""Experiment-only wrappers around the actual API and simulated gRPC worker."""
import argparse
import json
import os
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('role', choices=['api', 'worker'])
    parser.add_argument('--worker-id', type=int)
    args = parser.parse_args()
    run_id = os.environ['BENCH_RUN_ID']
    if args.role == 'api':
        from system.config import config
        config.update(batch_size=int(os.environ.get('BENCH_BATCH', '1')),
                      max_queue_size=int(os.environ.get('BENCH_QUEUE', '1000')),
                      scheduler_strategy=os.environ.get('BENCH_SCHEDULER', 'fifo'))
        from system.api_server import app
        import uvicorn

        @app.get('/benchmark/identity')
        def identity():
            return {'benchmark': 'simulation-only', 'run_id': run_id}

        uvicorn.run(app, host='0.0.0.0', port=8000, access_log=False)
    else:
        from system import grpc_worker_server as worker
        worker._inference_latency_ms = float(os.environ.get('BENCH_LATENCY_MS', '60'))
        original = worker.WorkerServicer

        class TimedWorker(original):
            def _process_batch(self, request, context):
                tick = time.perf_counter()
                response = super()._process_batch(request, context)
                elapsed = (time.perf_counter() - tick) * 1000
                for result in response.results:
                    if result.success:
                        value = json.loads(result.result_json)
                        value['benchmark'] = {'run_id': run_id, 'batch_ms': elapsed}
                        result.result_json = json.dumps(value)
                return response

        worker.WorkerServicer = TimedWorker
        worker.serve(args.worker_id, 50051, 'unused:2181', register=False, bind='0.0.0.0')


if __name__ == '__main__':
    main()
