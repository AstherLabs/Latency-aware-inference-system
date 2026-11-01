// Pure configuration, shared by k6 and the offline Node test suite.
export function configuration(env) {
  const integer = (name, fallback, minimum) => {
    const value = Number(env[name] ?? fallback);
    if (!Number.isSafeInteger(value) || value < minimum) throw new Error(`Invalid ${name}`);
    return value;
  };
  if (!env.RUN_ID) throw new Error('RUN_ID is required; use the isolated experiment runner');
  const base = env.BASE_URL || 'http://127.0.0.1:18180';
  if (!/^https?:\/\/[^/]+$/.test(base)) throw new Error('BASE_URL must be an origin without a path');
  const rate = integer('RATE', 25, 1);
  const duration = integer('DURATION', 120, 1);
  const maxVUs = integer('MAX_VUS', 1024, 1);
  const preAllocatedVUs = integer('PREALLOCATED_VUS', Math.min(maxVUs, Math.max(32, Math.ceil(rate / 2))), 1);
  if (preAllocatedVUs > maxVUs) throw new Error('PREALLOCATED_VUS exceeds MAX_VUS');
  return {base, runID: env.RUN_ID, rate, duration, summary: env.SUMMARY_PATH || 'summary.json',
    options: {
      scenarios: {pressure: {executor: 'constant-arrival-rate', rate, timeUnit: '1s',
        duration: `${duration}s`, preAllocatedVUs,
        maxVUs, gracefulStop: '40s'}},
      summaryTrendStats: ['count', 'avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
      thresholds: {result_mismatches: ['count==0']},
      systemTags: ['status', 'method', 'name', 'scenario', 'error_code'],
      tags: {run_id: env.RUN_ID, phase: env.PHASE || 'pressure'},
    }};
}
