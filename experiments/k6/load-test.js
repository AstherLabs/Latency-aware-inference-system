import http from 'k6/http';
import exec from 'k6/execution';
import {Counter, Trend} from 'k6/metrics';
import {configuration} from './config.mjs';

const cfg = configuration(__ENV);
export const options = cfg.options;
const started = new Counter('requests_started');
const finished = new Counter('requests_finished');
const successes = new Counter('valid_successes');
const mismatches = new Counter('result_mismatches');
const statuses = new Counter('response_statuses');
const rejections = new Counter('rejections_503');
const deadlines = new Counter('deadlines_504');
const transports = new Counter('transport_failures');
const otherFailures = new Counter('other_service_failures');
const attempts = new Trend('attempt_latency_ms', true);
const successful = new Trend('successful_latency_ms', true);
const server = new Trend('server_latency_ms', true);
const processing = new Trend('worker_batch_ms', true);
const overhead = new Trend('queue_dispatch_overhead_ms', true);
let mismatchLogged = false;

export function setup() {
  const identity = http.get(`${cfg.base}/benchmark/identity`, {timeout: '5s'});
  const body = identity.status === 200 ? identity.json() : {};
  if (body.benchmark !== 'simulation-only' || body.run_id !== cfg.runID) {
    throw new Error('Refusing traffic: isolated benchmark identity does not match RUN_ID');
  }
}

export default function () {
  const x = 1 + (exec.scenario.iterationInTest % 1000000);
  const tick = Date.now();
  started.add(1);
  // No sleep and no client retry: overload remains visible at the requested arrival rate.
  const response = http.post(`${cfg.base}/predict`, JSON.stringify({x}), {
    headers: {'Content-Type': 'application/json'}, timeout: '35s', tags: {name: 'POST /predict'},
  });
  const elapsed = Date.now() - tick;
  let valid = false;
  let payload;
  if (response.status === 200) {
    try {
      payload = response.json();
      valid = typeof payload.task_id === 'string' && payload.task_id.length > 0 &&
        payload.result.prediction === x * x && Number.isInteger(payload.result.worker_id) &&
        payload.result.worker_id >= 1000 && payload.result.benchmark.run_id === cfg.runID &&
        Number.isFinite(payload.latency_ms) && payload.latency_ms >= 0 &&
        Number.isFinite(payload.result.benchmark.batch_ms) && payload.result.benchmark.batch_ms >= 0;
    } catch (_) { valid = false; }
  }
  const outcome = valid ? 'success' : (response.status === 200 ? 'invalid_result' : `http_${response.status}`);
  if (response.status === 200 && !valid && !mismatchLogged) {
    mismatchLogged = true;
    console.error(JSON.stringify({event: 'invalid_result', expected_prediction: x * x,
      task_id: payload?.task_id, actual_prediction: payload?.result?.prediction,
      worker_id: payload?.result?.worker_id, benchmark: payload?.result?.benchmark,
      server_latency_ms: payload?.latency_ms, parse_failed: payload === undefined}));
  }
  attempts.add(elapsed, {outcome, worker: valid ? String(payload.result.worker_id) : 'none'});
  finished.add(1);
  statuses.add(1, {code: String(response.status)});
  rejections.add(response.status === 503 ? 1 : 0);
  deadlines.add(response.status === 504 ? 1 : 0);
  transports.add(response.status === 0 ? 1 : 0);
  otherFailures.add(response.status !== 0 && response.status !== 200 &&
    response.status !== 503 && response.status !== 504 ? 1 : 0);
  mismatches.add(response.status === 200 && !valid ? 1 : 0);
  successes.add(valid ? 1 : 0);
  if (valid) {
    successful.add(elapsed);
    server.add(payload.latency_ms);
    processing.add(payload.result.benchmark.batch_ms);
    // Includes queueing, dispatch, serialization and RPC overhead; not exact queue time.
    overhead.add(Math.max(0, payload.latency_ms - payload.result.benchmark.batch_ms));
  }
}

export function handleSummary(data) {
  return {[cfg.summary]: JSON.stringify({target_rps: cfg.rate, duration_s: cfg.duration,
    run_id: cfg.runID, ...data}, null, 2),
    stdout: `Saved stage summary: ${cfg.summary}\n`};
}
