import assert from 'node:assert/strict';
import {test} from 'node:test';
import {batchBody, changeEdit, changeEditValue, decisionBody, decisionDraft, draftScopeKey, mayComplete, mayContinue, mayRetract, retractionBody} from '../src/decisions';
import {decisionSchema, reviewSchema, reviewStagesSchema, stageResponses} from '../src/models';
import type {JsonValue} from '../src/models';
import {parseEdit, reviewControls} from '../src/labelCorrections';

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
        candidate_ids: [], eligible_candidate_ids: []},
      created_at: '2026-11-10', updated_at: '2026-11-10', reviewer: '', completion: null,
    },
    counts: {pending: 2, approve: 1, reject: 1, blocked: 0, approved_occurrences: 1},
    review_columns: ['command', 'category_id', 'risk_id', 'severity', 'class'],
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
      {row_index: 1, review_record_id: 'a', values: {model_id: 'a'}},
      {row_index: 2, review_record_id: 'c', blocked_reason: 'Machine ineligible', values: {model_id: 'c'}},
    ], total_rows: 4, matched_rows: 3, offset: 0, limit: 25},
    publication: null,
  });
}

test('decision writes bind snapshot generation and persisted row revision', () => {
  const review = fixture();
  const draft = {...decisionDraft(review.decisions[0]), decision: 'reject', notes: '  retain\n'};
  const body = decisionBody(review.batch, 'a', {...draft, decision: 'reject'}, 'two', review.review_columns);
  assert.deepEqual(body, {
    dataset_id: 'dataset-1', snapshot_id: 'a'.repeat(64),
    change: {batch_id: 'batch-1', generation: 3, candidate_id: 'a', expected_row_revision: 7,
      decision: 'reject', notes: '  retain\n', reviewer: 'two', edits: {}},
  });
});

test('completion binds batch revision rather than a row revision', () => {
  assert.deepEqual(batchBody(fixture().batch, 'two').expected,
    {batch_id: 'batch-1', generation: 3, batch_revision: 8});
});

test('compact review metadata preserves page decisions and duplicate multiplicity', () => {
  const review = fixture();
  const page = reviewSchema.parse({...review, decisions: [review.decisions[1]],
    page: {...review.page, rows: [{...review.page.rows[0], occurrence_count: 6}]}});
  assert.deepEqual(page.decisions.map((entry) => entry.candidate_id), ['b']);
  assert.deepEqual(page.batch.snapshot.candidate_ids, []);
  assert.equal(page.page.rows[0]?.occurrence_count, 6);
  assert.equal(review.page.rows[0]?.occurrence_count, 1);
  assert.equal(review.page.rows[0]?.blocked_reason, null);
});

test('completion requires zero pending rows an open batch and no unsaved work', () => {
  const review = fixture();
  const decided = {...review, counts: {pending: 0, approve: 2, reject: 2, blocked: 0, approved_occurrences: 2}};
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
    ...review, counts: {pending: 0, approve, reject: 4 - approve, blocked: 0, approved_occurrences: approve},
  }, selection, 'one', false, false)), [false, true, false]);
  assert.equal(mayComplete({...review, counts: {pending: 0, approve: 0, reject: 4, blocked: 0, approved_occurrences: 0}},
    stage, 'one', false, false), true);
});

test('stage identity isolates drafts and rejects approvals from another stage', () => {
  const review = fixture();
  const other = {...review, batch: {...review.batch,
    snapshot: {...review.batch.snapshot, review_stage: 'model-selection'}}};
  assert.notEqual(draftScopeKey(review.batch), draftScopeKey(other.batch));
  assert.equal(stageResponses('model-selection').review.safeParse(review).success, false);
  assert.equal(stageResponses('model-selection').review.safeParse(other).success, true);
  assert.equal(mayComplete({...other, counts: {pending: 0, approve: 1, reject: 3, blocked: 0, approved_occurrences: 1}},
    stage, 'one', false, false), false);
});

test('completion counts approved occurrences and rejects blocked cached approvals', () => {
  const review = fixture();
  const counts = {pending: 0, approve: 1, reject: 0, blocked: 0, approved_occurrences: 2};
  const selection = {...stage, min_approvals: 2, max_approvals: 2};
  assert.equal(mayComplete({...review, counts}, selection, 'one', false, false), true);
  assert.equal(mayComplete({...review, counts: {...counts, blocked: 1}}, selection, 'one', false, false), false);
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

test('off-page saved decisions can be retracted without full-batch candidate metadata', () => {
  const review = fixture();
  const saved = review.decisions[0];
  assert.ok(saved);
  const pageLocal = {...review, decisions: []};
  const state = {reviewer: 'two', dirty: false, unavailable: false, rowRevision: 7};
  assert.equal(mayRetract(pageLocal, 'a', state, saved), true);
  assert.equal(mayRetract(pageLocal, 'other', state, saved), false);
  assert.equal(mayRetract(pageLocal, 'a', state, {...saved, batch_id: 'foreign'}), false);
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

test('saved generic edits initialize independent drafts at the persisted revision', () => {
  const review = fixture();
  const saved = decisionSchema.parse({...review.decisions[0], edits: {command: 'new command', payload: {a: [true, null]}}});
  const draft = changeEdit(decisionDraft(saved), 'command', 'original command', '');
  assert.deepEqual(draft.edits, {command: '', payload: {a: [true, null]}});
  assert.deepEqual(decisionDraft(saved).edits, {command: 'new command', payload: {a: [true, null]}});
  assert.equal(draft.rowRevision, 7);
});

test('writes include only configured edits on approval and resets remove overrides', () => {
  const review = fixture();
  const draft = changeEditValue(changeEditValue(changeEditValue(
    {...decisionDraft(review.decisions[0]), notes: 'retain notes'}, 'command', ''), 'metadata', null), 'hidden', true);
  const cleared = changeEditValue(draft, 'command', undefined);
  assert.deepEqual(decisionBody(review.batch, 'a', cleared, 'two', ['command', 'metadata']).change, {
    batch_id: 'batch-1', generation: 3, candidate_id: 'a', expected_row_revision: 7,
    decision: 'approve', notes: 'retain notes', reviewer: 'two', edits: {metadata: null},
  });
  for (const decision of ['pending', 'reject'] as const) {
    assert.deepEqual(decisionBody(review.batch, 'a', {...draft, decision}, 'two', ['command', 'metadata']).change.edits, {});
  }
  assert.deepEqual(decisionBody(review.batch, 'a', draft, 'two', ['command', 'metadata']).change.edits,
    {command: '', metadata: null});
});

test('historical corrections are readable but generic edits take precedence', () => {
  const review = fixture();
  const saved = review.decisions[0];
  assert.deepEqual(decisionDraft(saved).edits, {});
  assert.deepEqual(decisionDraft(decisionSchema.parse({...saved, label_corrections: {severity: 'high'}})).edits,
    {severity: 'high'});
  assert.deepEqual(decisionDraft(decisionSchema.parse({...saved, edits: {}, label_corrections: {severity: 'high'}})).edits, {});
});

test('configured controls preserve originals without confidence or taxonomy restrictions', () => {
  const values = {command: '  original\n', class: 'ask', class_confidence: 'high', hidden: true, extra: {count: 2}};
  assert.deepEqual(reviewControls(values, ['command', 'class', 'extra']), [
    {field: 'command', original: '  original\n'},
    {field: 'class', original: 'ask'},
    {field: 'extra', original: {count: 2}},
  ]);
  assert.deepEqual(changeEdit(decisionDraft(undefined), 'class', 'ask', 'modify').edits, {class: 'modify'});
});

test('generic edits preserve JSON types and retain invalid input until reset', () => {
  const cases: readonly {readonly original: JsonValue; readonly input: string; readonly value: JsonValue}[] = [
    {original: 2, input: '0', value: 0},
    {original: true, input: 'false', value: false},
    {original: [1], input: '[null,{"nested":true}]', value: [null, {nested: true}]},
    {original: {a: 1}, input: '{"b":[]}', value: {b: []}},
    {original: null, input: '"now a string"', value: 'now a string'},
    {original: null, input: 'null', value: null},
  ];
  for (const {original, input, value} of cases) {
    assert.deepEqual(parseEdit(original, input), {value});
  }
  for (const input of ['{', '"not a number"', '1e999']) {
    assert.ok(parseEdit(2, input).error);
  }
  const draft = changeEdit(decisionDraft(undefined), 'quantity', 2, '-');
  assert.deepEqual(draft.invalidEdits, {quantity: '-'});
  assert.deepEqual(changeEditValue(draft, 'quantity', undefined).invalidEdits, {});
  assert.deepEqual(changeEdit(draft, 'quantity', 2, '3').edits, {quantity: 3});
});
