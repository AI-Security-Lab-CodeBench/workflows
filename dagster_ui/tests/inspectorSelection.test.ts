import assert from 'node:assert/strict';
import {test} from 'node:test';
import {inspectorSelection} from '../src/inspectorSelection';

const first = {scope: 'snapshot-one', id: 'record-one'};
const pending = {scope: 'snapshot-one', id: 'record-two'};

test('saving advances an open inspector to the next pending record', () => {
  const opened = inspectorSelection({revision: 0}, {type: 'select', selection: first});
  const advanced = inspectorSelection(opened, {type: 'advance', revision: opened.revision, selection: pending});
  assert.deepEqual(advanced.selection, pending);
});

test('closing an inspector prevents a pending save from reopening it', async () => {
  const opened = inspectorSelection({revision: 0}, {type: 'select', selection: first});
  let state = opened;
  const completion = Promise.resolve(pending).then((selection) => {
    state = inspectorSelection(state, {type: 'advance', revision: opened.revision, selection});
  });

  state = inspectorSelection(state, {type: 'close'});
  await completion;

  assert.equal(state.selection, undefined);
});

test('reopening the same record rejects navigation from an earlier save', () => {
  const opened = inspectorSelection({revision: 0}, {type: 'select', selection: first});
  const closed = inspectorSelection(opened, {type: 'close'});
  const reopened = inspectorSelection(closed, {type: 'select', selection: first});
  const advanced = inspectorSelection(reopened, {type: 'advance', revision: opened.revision, selection: pending});
  assert.deepEqual(advanced.selection, first);
});

test('an exhausted pending queue dismisses only the original selection', () => {
  const opened = inspectorSelection({revision: 0}, {type: 'select', selection: first});
  const newer = inspectorSelection(opened, {type: 'select', selection: pending});
  assert.equal(inspectorSelection(opened, {type: 'advance', revision: opened.revision, selection: undefined}).selection, undefined);
  assert.deepEqual(inspectorSelection(newer, {type: 'advance', revision: opened.revision, selection: undefined}).selection, pending);
});
