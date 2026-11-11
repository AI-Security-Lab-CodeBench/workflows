import assert from 'node:assert/strict';
import {test} from 'node:test';
import {cellText, panelLink} from '../src/api';
import {initialQuery, querySchema, reviewStagesSchema} from '../src/models';
import {toggleSort} from '../src/query';
import {assetKeyFromPath, reviewScopeKey, reviewStageLink, stagesForAsset} from '../src/reviewStages';

test('custom tab URLs preserve partition and other native parameters', () => {
  const query = new URLSearchParams(panelLink('?partition=CAT-3&view=overview&lineageDepth=2', 'review'));
  assert.deepEqual(Object.fromEntries(query), {
    partition: 'CAT-3', view: 'review', lineageDepth: '2',
  });
});

test('sorting cycles direction without changing other sort priorities', () => {
  const query = querySchema.parse({...initialQuery, offset: 12,
    sort: [{field: 'risk', direction: 'asc'}, {field: 'command', direction: 'asc'}]});
  const descending = toggleSort(query, 'risk');
  const removed = toggleSort(descending, 'risk');
  assert.deepEqual([descending.offset, descending.sort, removed.sort], [0,
    [{field: 'risk', direction: 'desc'}, {field: 'command', direction: 'asc'}],
    [{field: 'command', direction: 'asc'}],
  ]);
});

test('adding a fifth sort retains the existing first three priorities', () => {
  const query = querySchema.parse({...initialQuery, sort: [
    {field: 'a', direction: 'asc'}, {field: 'b', direction: 'desc'},
    {field: 'c', direction: 'asc'}, {field: 'd', direction: 'asc'},
  ]});
  assert.deepEqual(toggleSort(query, 'e').sort.map((sort) => sort.field), ['a', 'b', 'c', 'e']);
});

test('literal filters retain text and null filters omit a value', () => {
  const query = querySchema.parse({...initialQuery, limit: 2, filters: [
    {field: 'command', operator: 'equals', value: 'x\n  <b>$(inert)</b>'},
    {field: 'notes', operator: 'is_null'},
  ]});
  assert.deepEqual(query.filters, [
    {field: 'command', operator: 'equals', value: 'x\n  <b>$(inert)</b>'},
    {field: 'notes', operator: 'is_null'},
  ]);
});

test('full cell text preserves whitespace and uses sorted compact JSON', () => {
  assert.deepEqual([
    cellText('  a\t\nb\n'), cellText({z: [true, null, {b: 2, a: 1}], a: 'x'}),
  ], ['  a\t\nb\n', '{"a":"x","z":[true,null,{"a":1,"b":2}]}']);
});

test('registry matches complete asset keys and exposes multiple stages without an asset allowlist', () => {
  const descriptor = {
    title: 'Model selection', completion_label: 'Save selection', min_approvals: 1, max_approvals: 1,
  };
  const {stages} = reviewStagesSchema.parse({stages: [
    {...descriptor, stage_id: 'model-selection', asset_keys: [['models', 'available models']]},
    {...descriptor, stage_id: 'comparison', asset_keys: [['models', 'available models'], ['other', 'comparison']]},
    {...descriptor, stage_id: 'unrelated', asset_keys: [['datasets', 'available models']]},
  ]});
  assert.deepEqual(stagesForAsset(stages, assetKeyFromPath('/assets/models/available%20models'))
    .map((stage) => stage.stage_id), ['model-selection', 'comparison']);
  assert.deepEqual(stagesForAsset(stages, ['available models']), []);
});

test('stage URLs preserve partition and native parameters while replacing stage identity', () => {
  const query = new URLSearchParams(reviewStageLink(
    '?partition=CAT-3&view=data&review_stage=annotations&lineageDepth=2', 'model/selection'));
  assert.deepEqual(Object.fromEntries(query), {
    partition: 'CAT-3', view: 'review', review_stage: 'model/selection', lineageDepth: '2',
  });
  assert.notEqual(reviewScopeKey('a/b', 'c'), reviewScopeKey('a', 'b/c'));
  assert.notEqual(reviewScopeKey('dataset-1', 'annotations'), reviewScopeKey('dataset-1', 'model-selection'));
});
