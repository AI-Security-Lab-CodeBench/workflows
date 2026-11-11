import type {Batch, Decision, DecisionValue, Review, ReviewStage} from './models';

export type Draft = {
  readonly decision: DecisionValue;
  readonly notes: string;
  readonly rowRevision: number;
};

export function decisionDraft(decision: Decision | undefined): Draft {
  return {
    decision: decision?.decision ?? 'pending',
    notes: decision?.notes ?? '',
    rowRevision: decision?.row_revision ?? 0,
  };
}

export function decisionBody(batch: Batch, candidateId: string, draft: Draft, reviewer: string) {
  return {
    dataset_id: batch.snapshot.dataset_id,
    snapshot_id: batch.snapshot.snapshot_id,
    change: {
      batch_id: batch.batch_id, generation: batch.generation, candidate_id: candidateId,
      expected_row_revision: draft.rowRevision, decision: draft.decision,
      notes: draft.notes, reviewer,
    },
  };
}

export function batchBody(batch: Batch, reviewer: string) {
  return {
    dataset_id: batch.snapshot.dataset_id, snapshot_id: batch.snapshot.snapshot_id,
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
}): boolean {
  const decision = review.decisions.find((entry) => entry.candidate_id === candidateId);
  return (review.batch.state === 'open' || review.batch.state === 'completed') &&
    review.batch.snapshot.candidate_ids.includes(candidateId) &&
    decision !== undefined && decision.decision !== 'pending' &&
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

export function nextPending(review: Review, savedId: string, savedValue: DecisionValue): string | undefined {
  const pending = new Set(review.decisions.filter((decision) =>
    decision.decision === 'pending' && decision.candidate_id !== savedId).map((decision) => decision.candidate_id));
  const current = review.batch.snapshot.candidate_ids.indexOf(savedId);
  const ordered = [...review.batch.snapshot.candidate_ids.slice(current + 1),
    ...review.batch.snapshot.candidate_ids.slice(0, current)];
  const next = ordered.find((id) => pending.has(id) ||
    (id !== savedId && !review.decisions.some((decision) => decision.candidate_id === id)));
  return next ?? (savedValue === 'pending' ? savedId : undefined);
}

export function draftScopeKey(batch: Batch): string {
  return `${JSON.stringify([batch.snapshot.dataset_id, batch.snapshot.review_stage,
    batch.batch_id, batch.snapshot.snapshot_id])}/`;
}

export function mayComplete(
  review: Review, stage: ReviewStage, reviewer: string, saving: boolean, dirty: boolean,
): boolean {
  return review.batch.state === 'open' && review.counts.pending === 0 &&
    review.batch.snapshot.review_stage === stage.stage_id &&
    review.counts.approve >= stage.min_approvals &&
    (stage.max_approvals === null || review.counts.approve <= stage.max_approvals) &&
    reviewer.trim().length > 0 && !saving && !dirty;
}
