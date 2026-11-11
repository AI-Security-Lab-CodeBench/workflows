import {Button, Radio, RadioGroup, TextArea} from '@dagster-io/ui-components';
import type {Review} from './models';
import {decisionValue} from './models';
import type {Draft} from './decisions';
import {decisionDraft} from './decisions';
import {Evidence} from './Evidence';
import {Fields} from './Fields';
import styles from './ui.module.css';

export function DecisionInspector({review, candidateId, draft, dirty, disabled, canRetract, onDraft, onDiscard, onSave, onRebase, onRetract}: {
  readonly review: Review; readonly candidateId: string; readonly draft: Draft;
  readonly dirty: boolean; readonly disabled: boolean;
  readonly canRetract: boolean; readonly onRetract: () => void;
  readonly onDraft: (draft: Draft) => void; readonly onDiscard: () => void;
  readonly onSave: (next: boolean) => void; readonly onRebase: () => void;
}) {
  const saved = review.decisions.find((entry) => entry.candidate_id === candidateId);
  const original = decisionDraft(saved);
  const stale = original.rowRevision !== draft.rowRevision;
  const eligible = review.batch.snapshot.eligible_candidate_ids.includes(candidateId);
  const locked = review.batch.state !== 'open';
  const row = review.page.rows.find((entry) => entry.review_record_id === candidateId);
  return <aside className={`${styles.pane} ${styles.inspector}`} aria-label="Record inspector">
    <div className={styles.stack}>
      <strong>Record: {candidateId}</strong>
      <div>Saved human decision: {saved?.decision ?? 'pending'}; reviewer: {saved?.reviewer || '(none)'}</div>
      <pre className={styles.literal}>{saved?.notes ?? ''}</pre>
      {!eligible && <div role="status">Ineligible for approval. Explicitly reject this record to resolve it.</div>}
      {locked && <div role="status">{review.batch.state === 'completed'
        ? 'Retract a saved decision to return that record to pending before editing.'
        : 'This batch is superseded. Refresh to review the current snapshot.'}</div>}
      {saved && saved.decision !== 'pending' && <>
        <Button disabled={!canRetract} onClick={onRetract}>Retract decision</Button>
        <div role="status">Retraction returns this record to pending and invalidates dependent results until review is completed again.
          Other saved decisions are preserved. Save or discard all drafts before retracting.</div>
      </>}
      <RadioGroup aria-label="Human decision" value={draft.decision} disabled={disabled || locked}
        onValueChange={(value) => onDraft({...draft, decision: decisionValue.parse(value)})}>
        <Radio className={styles.radioOption} value="pending">Pending</Radio>
        <Radio className={styles.radioOption} value="approve" disabled={!eligible}>Approve</Radio>
        <Radio className={styles.radioOption} value="reject">Reject</Radio>
      </RadioGroup>
      <label className={styles.field}>Human notes
        <TextArea value={draft.notes} disabled={disabled || locked}
          onChange={(e) => onDraft({...draft, notes: e.target.value})} />
      </label>
      {stale && <div role="alert">
        This draft is based on an older decision revision. Inspect the saved decision before rebasing or discarding.
        <Button disabled={disabled || locked} onClick={onRebase}>Use draft with refreshed revision</Button>
      </div>}
      <div className={styles.toolbar}>
        <Button disabled={disabled || locked || stale || !dirty || (draft.decision === 'approve' && !eligible)}
          onClick={() => onSave(false)}>Save</Button>
        <Button intent="primary" disabled={disabled || locked || stale || (draft.decision === 'approve' && !eligible)}
          onClick={() => onSave(true)}>Save and next pending</Button>
        <Button disabled={disabled || !dirty} onClick={onDiscard}>Discard draft</Button>
      </div>
      {row && <details><summary>Complete reviewed row</summary><Fields fields={row.values} /></details>}
      <Evidence key={`${review.batch.snapshot.review_stage}/${review.batch.batch_id}/${candidateId}`}
        stageId={review.batch.snapshot.review_stage} batchId={review.batch.batch_id}
        candidateId={candidateId} datasetId={review.batch.snapshot.dataset_id} />
    </div>
  </aside>;
}
