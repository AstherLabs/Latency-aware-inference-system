import {test} from 'node:test';
import assert from 'node:assert/strict';
import {configuration} from '../config.mjs';

test('arrival rate stays independent from response time', () => {
  const c = configuration({RUN_ID:'test', RATE:'3200', DURATION:'120', MAX_VUS:'1024'});
  assert.equal(c.options.scenarios.pressure.executor, 'constant-arrival-rate');
  assert.equal(c.options.scenarios.pressure.rate, 3200);
  assert.equal(c.options.scenarios.pressure.duration, '120s');
});

test('invalid configuration fails before traffic', () => {
  for (const e of [{}, {RUN_ID:'x',RATE:'0'}, {RUN_ID:'x',DURATION:'NaN'},
                   {RUN_ID:'x',BASE_URL:'https://example.com/path'},
                   {RUN_ID:'x',MAX_VUS:'1.5'}]) {
    assert.throws(() => configuration(e));
  }
});


test('preallocated clients cover long queue waits without runtime allocation', () => {
  const c = configuration({RUN_ID:'test', RATE:'50', MAX_VUS:'512', PREALLOCATED_VUS:'512'});
  assert.equal(c.options.scenarios.pressure.preAllocatedVUs, 512);
  assert.throws(() => configuration({RUN_ID:'test', MAX_VUS:'32', PREALLOCATED_VUS:'33'}));
});
