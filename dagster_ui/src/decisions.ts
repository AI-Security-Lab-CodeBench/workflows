import type {Batch, Decision, DecisionValue, Edits, JsonValue, Review, ReviewStage} from './models';
import {parseEdit} from './labelCorrections';

export type Draft = {
  readonly decision: DecisionValue;
  readonly notes: string;
  readonly rowRevision: number;
  readonly edits: Edits;
  readonly invalidEdits: Readonly<Record<string, string>>;
};

export function decisionDraft(decision: Decision | undefined): Draft {
  return {
    decision: decision?.decision ?? 'pending',
    notes: decision?.notes ?? '',
    rowRevision: decision?.row_revision ?? 0,
    edits: {...(decision?.edits ?? decision?.label_corrections ?? {})},
    invalidEdits: {},
  };
}

export function changeEditValue(draft: Draft, field: string, value: JsonValue | undefined): Draft {
  const edits = {...draft.edits};
  const invalidEdits = {...draft.invalidEdits};
  delete invalidEdits[field];
  if (value === undefined) {delete edits[field];}
  else {edits[field] = value;}
  return {...draft, edits, invalidEdits};
}

export function changeEdit(draft: Draft, field: string, original: JsonValue, input: string): Draft {
  const parsed = parseEdit(original, input);
  return parsed.error === undefined ? changeEditValue(draft, field, parsed.value) :
    {...draft, invalidEdits: {...draft.invalidEdits, [field]: input}};
}

export function decisionBody(batch: Batch, candidateId: string, draft: Draft, reviewer: string, columns: readonly string[]) {
  return {
    dataset_id: batch.snapshot.dataset_id,
    artifact_id: batch.snapshot.artifact_id, scope_id: batch.snapshot.scope_id,
    snapshot_id: batch.snapshot.snapshot_id,
    change: {
      batch_id: batch.batch_id, generation: batch.generation, candidate_id: candidateId,
      expected_row_revision: draft.rowRevision, decision: draft.decision,
      notes: draft.notes, reviewer,
      edits: draft.decision === 'approve'
        ? Object.fromEntries(Object.entries(draft.edits).filter(([field]) => columns.includes(field))) : {},
    },
  };
}

export function batchBody(batch: Batch, reviewer: string) {
  return {
    dataset_id: batch.snapshot.dataset_id, snapshot_id: batch.snapshot.snapshot_id,
    artifact_id: batch.snapshot.artifact_id, scope_id: batch.snapshot.scope_id,
    expected: {batch_id: batch.batch_id, generation: batch.generation, batch_revision: batch.batch_revision},
    reviewer,
  };
}

export function retractionBody(batch: Batch, decision: Decision, reviewer: string) {
  return {...batchBody(batch, reviewer), candidate_id: decision.candidate_id,
    expected_row_revision: decision.row_revision};
}

export function mayRetract(review: Review, candidateId: string, state: {
  readonly reviewer: string; readonly dirty: boolean; readonly unavailable: boolean;
  readonly rowRevision: number;
}, selectedDecision?: Decision): boolean {
  const decision = selectedDecision ?? review.decisions.find((entry) => entry.candidate_id === candidateId);
  return (review.batch.state === 'open' || review.batch.state === 'completed') &&
    decision !== undefined && decision.batch_id === review.batch.batch_id &&
    decision.candidate_id === candidateId && decision.decision !== 'pending' &&
    decision.row_revision === state.rowRevision &&
    state.reviewer.trim().length > 0 && !state.dirty && !state.unavailable;
}

export function mayContinue(
  review: Review, stage: ReviewStage, state: {
    readonly reviewer: string; readonly dirty: boolean; readonly unavailable: boolean;
  },
): boolean {
  return review.batch.state === 'completed' && stage.can_continue &&
    review.batch.snapshot.review_stage === stage.stage_id &&
    state.reviewer.trim().length > 0 && !state.dirty && !state.unavailable;
}

export function draftScopeKey(batch: Batch): string {
  return `${JSON.stringify([batch.snapshot.dataset_id, batch.snapshot.review_stage,
    batch.batch_id, batch.snapshot.snapshot_id])}/`;
}

export function mayComplete(
  review: Review, stage: ReviewStage, reviewer: string, saving: boolean, dirty: boolean,
): boolean {
  return review.batch.state === 'open' && review.counts.pending === 0 &&
    review.counts.blocked === 0 &&
    review.batch.snapshot.review_stage === stage.stage_id &&
    review.counts.approved_occurrences >= stage.min_approvals &&
    (stage.max_approvals === null || review.counts.approved_occurrences <= stage.max_approvals) &&
    reviewer.trim().length > 0 && !saving && !dirty;
}
