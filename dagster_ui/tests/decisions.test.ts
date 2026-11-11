import assert from 'node:assert/strict';
import {test} from 'node:test';
import {batchBody, decisionBody, decisionDraft, draftScopeKey, mayComplete, mayContinue, mayRetract, nextPending, retractionBody} from '../src/decisions';
import {reviewSchema, reviewStagesSchema, stageResponses} from '../src/models';

const stage = reviewStagesSchema.parse({stages: [{
  stage_id: 'annotations', title: 'Dataset inclusion', asset_keys: [['datasets', 'reviewed_annotations']],
  completion_label: 'Complete review and publish', min_approvals: 0, max_approvals: null,
}]}).stages[0];
assert.ok(stage);

function fixture() {
  return reviewSchema.parse({
    batch: {
      batch_id: 'batch-1', generation: 3, batch_revision: 8, state: 'open',
      snapshot: {dataset_id: 'dataset-1', snapshot_id: 'a'.repeat(64),
        candidate_ids: ['a', 'b', 'c', 'd'], eligible_candidate_ids: ['a', 'b']},
      created_at: '2026-11-10', updated_at: '2026-11-10', reviewer: '', completion: null,
    },
    counts: {pending: 2, approve: 1, reject: 1},
    decisions: [
      {batch_id: 'batch-1', candidate_id: 'a', decision: 'approve', notes: '', reviewer: 'one',
        row_revision: 7, created_at: '2026-11-10', updated_at: '2026-11-10'},
      {batch_id: 'batch-1', candidate_id: 'b', decision: 'pending', notes: '', reviewer: '',
        row_revision: 0, created_at: '2026-11-10', updated_at: '2026-11-10'},
      {batch_id: 'batch-1', candidate_id: 'c', decision: 'reject', notes: '', reviewer: 'one',
        row_revision: 2, created_at: '2026-11-10', updated_at: '2026-11-10'},
    ],
    page: {columns: ['model_id', 'candidate_id'], rows: [
      {row_index: 0, review_record_id: 'b', values: {model_id: 'b', candidate_id: 'unrelated-source-id'}},
    ], total_rows: 4, matched_rows: 1, offset: 0, limit: 2},
    publication: null,
  });
}

test('decision writes bind snapshot generation and persisted row revision', () => {
  const review = fixture();
  const draft = {...decisionDraft(review.decisions[0]), decision: 'reject', notes: '  retain\n'};
  const body = decisionBody(review.batch, 'a', {...draft, decision: 'reject'}, 'two');
  assert.deepEqual(body, {
    dataset_id: 'dataset-1', snapshot_id: 'a'.repeat(64),
    change: {batch_id: 'batch-1', generation: 3, candidate_id: 'a', expected_row_revision: 7,
      decision: 'reject', notes: '  retain\n', reviewer: 'two'},
  });
});

test('completion binds batch revision rather than a row revision', () => {
  assert.deepEqual(batchBody(fixture().batch, 'two').expected,
    {batch_id: 'batch-1', generation: 3, batch_revision: 8});
});

test('next pending scans the full batch beyond the filtered page and wraps', () => {
  const review = fixture();
  const selected = review.page.rows[0];
  assert.ok(selected);
  assert.deepEqual([
    nextPending(review, selected.review_record_id, 'approve'), nextPending(review, 'd', 'reject'),
  ], ['d', 'b']);
});

test('completion requires zero pending rows an open batch and no unsaved work', () => {
  const review = fixture();
  const decided = {...review, counts: {pending: 0, approve: 2, reject: 2}};
  assert.deepEqual([
    mayComplete(review, stage, 'one', false, false),
    mayComplete(decided, stage, '  ', false, false),
    mayComplete(decided, stage, 'one', true, false),
    mayComplete(decided, stage, 'one', false, true),
    mayComplete({...decided, batch: {...decided.batch, state: 'completed'}}, stage, 'one', false, false),
    mayComplete(decided, stage, 'one', false, false),
  ], [false, false, false, false, false, true]);
});

test('completion enforces inclusive minimum and maximum approval counts', () => {
  const review = fixture();
  const selection = {...stage, min_approvals: 1, max_approvals: 1};
  assert.deepEqual([0, 1, 2].map((approve) => mayComplete({
    ...review, counts: {pending: 0, approve, reject: 4 - approve},
  }, selection, 'one', false, false)), [false, true, false]);
  assert.equal(mayComplete({...review, counts: {pending: 0, approve: 0, reject: 4}},
    stage, 'one', false, false), true);
});

test('stage identity isolates drafts and rejects approvals from another stage', () => {
  const review = fixture();
  const other = {...review, batch: {...review.batch,
    snapshot: {...review.batch.snapshot, review_stage: 'model-selection'}}};
  assert.notEqual(draftScopeKey(review.batch), draftScopeKey(other.batch));
  assert.equal(stageResponses('model-selection').review.safeParse(review).success, false);
  assert.equal(stageResponses('model-selection').review.safeParse(other).success, true);
  assert.equal(mayComplete({...other, counts: {pending: 0, approve: 1, reject: 3}},
    stage, 'one', false, false), false);
});

test('review accepts only its declared automatic publication companion', () => {
  const original = fixture();
  const review = {...original, batch: {...original.batch,
    snapshot: {...original.batch.snapshot, review_stage: 'documentation_annotations'}},
    publication: {
      payload_key: 'objects/release.parquet', payload_hash: 'a'.repeat(64),
      csv_reference: 'release/commands.csv', bundle_reference: 'release',
      dagster_run_id: 'run', publication_id: 'publication', created_at: '2026-11-11',
      authorization: {
        batch_id: 'automatic', generation: 1, batch_revision: 1, dataset_id: 'dataset-1',
        snapshot_id: 'a'.repeat(64), decision_digest: 'b'.repeat(64), token: 'token',
        review_stage: 'documentation_automatic',
      },
    }};
  const schema = stageResponses('documentation_annotations', ['documentation_automatic']).review;
  assert.equal(schema.safeParse(review).success, true);
  assert.equal(stageResponses('documentation_annotations').review.safeParse(review).success, false);
  assert.equal(schema.safeParse({...review, publication: {...review.publication,
    authorization: {...review.publication.authorization, review_stage: 'automatic'},
  }}).success, false);
});

test('review rows require normalized identity independent of source columns', () => {
  const review = fixture();
  const row = review.page.rows[0];
  assert.ok(row);
  const generic = reviewSchema.parse({...review, page: {...review.page,
    rows: [{...row, values: {model_id: 'b'}}],
  }});
  assert.deepEqual(generic.page.rows.map((entry) => entry.review_record_id), ['b']);
  assert.equal(reviewSchema.safeParse({...review, page: {...review.page,
    rows: [{row_index: row.row_index, values: row.values}],
  }}).success, false);
});

test('retraction binds both batch and saved row revisions without changing the decision payload', () => {
  const review = fixture();
  const decision = review.decisions[0];
  assert.ok(decision);
  assert.deepEqual(retractionBody(review.batch, decision, 'two'), {
    dataset_id: 'dataset-1', snapshot_id: 'a'.repeat(64),
    expected: {batch_id: 'batch-1', generation: 3, batch_revision: 8},
    candidate_id: 'a', expected_row_revision: 7, reviewer: 'two',
  });
});

test('saved decisions can be retracted from open and completed but not superseded batches', () => {
  const review = fixture();
  const state = {reviewer: 'two', dirty: false, unavailable: false, rowRevision: 7};
  assert.deepEqual((['open', 'completed', 'superseded'] as const).map((batchState) =>
    mayRetract({...review, batch: {...review.batch, state: batchState}}, 'a', state)),
  [true, true, false]);
  assert.equal(mayRetract(review, 'c', {...state, rowRevision: 2}), true);
  assert.equal(mayRetract(review, 'b', {...state, rowRevision: 0}), false);
  assert.equal(mayRetract(review, 'missing', state), false);
});

test('retraction blocks dirty drafts missing reviewer unavailable state and stale row revision', () => {
  const review = fixture();
  const state = {reviewer: 'two', dirty: false, unavailable: false, rowRevision: 7};
  assert.deepEqual([
    {...state, dirty: true},
    {...state, reviewer: '  '},
    {...state, unavailable: true},
    {...state, rowRevision: 6},
  ].map((blocked) => mayRetract(review, 'a', blocked)), [false, false, false, false]);
});

test('continuation requires advertised capability completed matching stage and clean writable state', () => {
  const review = fixture();
  const completed = {...review, batch: {...review.batch, state: 'completed' as const}};
  const continuable = {...stage, can_continue: true};
  const state = {reviewer: 'one', dirty: false, unavailable: false};
  assert.deepEqual([
    mayContinue(completed, continuable, state),
    mayContinue(review, continuable, state),
    mayContinue(completed, stage, state),
    mayContinue(completed, {...continuable, stage_id: 'other'}, state),
    mayContinue(completed, continuable, {...state, reviewer: ''}),
    mayContinue(completed, continuable, {...state, dirty: true}),
    mayContinue(completed, continuable, {...state, unavailable: true}),
  ], [true, false, false, false, false, false, false]);
});
