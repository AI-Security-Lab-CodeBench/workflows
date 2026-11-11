import assert from 'node:assert/strict';
import {afterEach, test} from 'node:test';
import {ApiError, request} from '../src/api';
import {batchSchema, completeSchema, dataSchema, reviewStagesSchema, savedSchema, stageResponses} from '../src/models';
import {retractionBody} from '../src/decisions';
import {reviewPath} from '../src/reviewStages';

const originalFetch = globalThis.fetch;
afterEach(() => {globalThis.fetch = originalFetch;});

for (const {operation, method} of [
  {operation: 'decision', method: 'PUT'},
  {operation: 'retract', method: 'POST'},
] as const) {
  test(`a stale ${operation} response surfaces conflict without retrying the write`, async () => {
    let calls = 0;
    globalThis.fetch = async () => {
      calls += 1;
      return Response.json({error: {code: 'stale', message: 'Changed'}}, {status: 409});
    };
    await assert.rejects(request(`${reviewPath('model-selection')}/${operation}`, savedSchema, method, {}),
      (error: unknown) => error instanceof ApiError && error.status === 409 && error.code === 'stale');
    assert.equal(calls, 1);
  });
}

test('malformed successful data is rejected at the transport boundary', async () => {
  globalThis.fetch = async () => Response.json({
    kind: 'table', dataset_id: 'one', stage: 'raw', snapshot_id: 'not-a-digest',
    page: {columns: [], total_rows: 0, matched_rows: 0, offset: 0, limit: 2, rows: []},
  });
  await assert.rejects(request('/data/raw', dataSchema, 'POST', {}),
    (error: unknown) => error instanceof ApiError && error.code === 'invalid_response');
});

test('rules responses retain nested typed taxonomy without table assumptions', async () => {
  globalThis.fetch = async () => Response.json({
    kind: 'rules', dataset_id: 'one', stage: 'category_rules', snapshot_id: 'a'.repeat(64),
    rules: {classes: ['allow', 'ask', 'deny'], revision: 1},
  });
  const data = await request('/data/category_rules', dataSchema, 'POST', {});
  assert.equal(data.kind, 'rules');
  if (data.kind === 'rules') {
    assert.deepEqual(data.rules, {classes: ['allow', 'ask', 'deny'], revision: 1});
  }
});

test('caller cancellation reaches the HTTP transport', async () => {
  const controller = new AbortController();
  controller.abort();
  globalThis.fetch = async (_input, options) => {
    options?.signal?.throwIfAborted();
    return Response.json({});
  };
  await assert.rejects(request('/data/raw', dataSchema, 'POST', {}, controller.signal),
    (error: unknown) => error instanceof DOMException && error.name === 'AbortError');
});

function completionFixture(reviewStage?: string) {
  const completion = {
    batch_id: 'batch-1', generation: 1, batch_revision: 2, dataset_id: 'dataset-1',
    snapshot_id: 'a'.repeat(64), decision_digest: 'b'.repeat(64), token: 'completion-1',
    ...(reviewStage === undefined ? {} : {review_stage: reviewStage}),
  };
  return {
    batch: {
      batch_id: 'batch-1', generation: 1, batch_revision: 2, state: 'completed',
      snapshot: {dataset_id: 'dataset-1', snapshot_id: 'a'.repeat(64),
        candidate_ids: ['model-a'], eligible_candidate_ids: ['model-a'],
        ...(reviewStage === undefined ? {} : {review_stage: reviewStage})},
      created_at: '2026-11-10', updated_at: '2026-11-10', reviewer: 'one', completion,
    },
    counts: {pending: 0, approve: 1, reject: 0}, completion, publication: null, run_id: null,
  };
}

test('generic completion accepts saved review without publication or a Dagster run', async () => {
  let requestedPath: unknown;
  globalThis.fetch = async (path) => {
    requestedPath = path;
    return Response.json(completionFixture('model/selection'));
  };
  const result = await request(`${reviewPath('model/selection')}/complete`,
    stageResponses('model/selection').complete, 'POST', {});
  assert.equal(requestedPath, '/codebench/api/reviews/model%2Fselection/complete');
  assert.deepEqual([result.batch.state, result.completion.review_stage, result.publication, result.run_id],
    ['completed', 'model/selection', null, null]);
});

test('historical snapshot and completion tokens default only to annotations', () => {
  const result = completeSchema.parse(completionFixture());
  assert.deepEqual([result.batch.snapshot.review_stage, result.batch.completion?.review_stage,
    result.completion.review_stage], ['annotations', 'annotations', 'annotations']);
  assert.equal(stageResponses('model-selection').complete.safeParse(completionFixture()).success, false);
  assert.equal(batchSchema.safeParse({...result.batch,
    snapshot: {...result.batch.snapshot, review_stage: ''},
  }).success, false);
});

test('completion rejects a token issued for a different review stage', () => {
  const response = completionFixture('model-selection');
  assert.equal(stageResponses('model-selection').complete.safeParse({...response,
    completion: {...response.completion, review_stage: 'annotations'},
  }).success, false);
});

test('retraction posts revision-bound JSON and accepts a reopened generation with a pending row', async () => {
  const completed = completionFixture('sources');
  const decision = {
    batch_id: 'batch-1', candidate_id: 'model-a', decision: 'approve' as const,
    notes: '', reviewer: 'one', row_revision: 4, created_at: '2026-11-10', updated_at: '2026-11-10',
  };
  const batch = batchSchema.parse(completed.batch);
  let posted: unknown;
  globalThis.fetch = async (path, options) => {
    posted = {path, method: options?.method, body: JSON.parse(String(options?.body))};
    return Response.json({
      batch: {...batch, state: 'open', generation: 2, batch_revision: 3, completion: null},
      decision: {...decision, decision: 'pending', row_revision: 5},
      counts: {pending: 1, approve: 0, reject: 0},
    });
  };
  const result = await request(`${reviewPath('sources')}/retract`, stageResponses('sources').saved,
    'POST', retractionBody(batch, decision, 'two'));
  assert.deepEqual(posted, {
    path: '/codebench/api/reviews/sources/retract', method: 'POST',
    body: {dataset_id: 'dataset-1', snapshot_id: 'a'.repeat(64),
      expected: {batch_id: 'batch-1', generation: 1, batch_revision: 2},
      candidate_id: 'model-a', expected_row_revision: 4, reviewer: 'two'},
  });
  assert.deepEqual([result.batch.state, result.batch.generation, result.decision.decision,
    result.decision.row_revision], ['open', 2, 'pending', 5]);
});

test('continuation and reloaded batch parsing retain run identity without CSV publication', async () => {
  const completed = completionFixture('sources');
  globalThis.fetch = async () => Response.json({...completed, run_id: 'run-2',
    batch: {...completed.batch, continuation_run_id: 'run-2'}});
  const result = await request(`${reviewPath('sources')}/continue`, stageResponses('sources').continue,
    'POST', {});
  const reloaded = batchSchema.parse(JSON.parse(JSON.stringify(result.batch)));
  assert.deepEqual([result.run_id, result.publication, reloaded.continuation_run_id], ['run-2', null, 'run-2']);
});

test('unchanged continuation permits null completion and run while rejecting foreign stage tokens', () => {
  const completed = completionFixture('sources');
  const schema = stageResponses('sources').continue;
  const unchanged = schema.parse({...completed, completion: null});
  assert.deepEqual([unchanged.completion, unchanged.run_id], [null, null]);
  assert.equal(schema.safeParse({...completed,
    completion: {...completed.completion, review_stage: 'another'},
  }).success, false);
});

test('continuation failure surfaces server code without retrying the effect', async () => {
  let calls = 0;
  globalThis.fetch = async () => {
    calls += 1;
    return Response.json({error: {code: 'continuation_failed', message: 'Failed'}}, {status: 500});
  };
  await assert.rejects(request(`${reviewPath('sources')}/continue`,
    stageResponses('sources').continue, 'POST', {}),
  (error: unknown) => error instanceof ApiError && error.code === 'continuation_failed');
  assert.equal(calls, 1);
});

test('historical descriptors and batches receive conservative continuation defaults', () => {
  const parsed = reviewStagesSchema.parse({stages: [{
    stage_id: 'arbitrary', title: 'Review', asset_keys: [['custom', 'asset']],
    completion_label: 'Complete', min_approvals: 0, max_approvals: null,
  }]});
  assert.deepEqual(parsed.stages.map(({can_continue, continuation_label}) =>
    [can_continue, continuation_label]), [[false, 'Continue pipeline']]);
  assert.equal(batchSchema.parse(completionFixture().batch).continuation_run_id, null);
});

test('stale decision and downstream tables remain inspectable without a published state', () => {
  const data = dataSchema.parse({
    kind: 'table', dataset_id: 'one', stage: 'human_source_decisions',
    snapshot_id: 'a'.repeat(64), publication_state: 'stale',
    page: {columns: ['id'], total_rows: 1, matched_rows: 1, offset: 0, limit: 25,
      rows: [{row_index: 0, values: {id: 'source-1'}}]},
  });
  assert.equal(data.kind, 'table');
  if (data.kind === 'table') {
    assert.equal(data.publication_state, 'stale');
    assert.deepEqual(data.page.rows.map((row) => row.values), [{id: 'source-1'}]);
  }
});
