import {Button, TextInput} from '@dagster-io/ui-components';
import type {AssetViewFeatureInput} from '@dagster-io/ui-core/assets/AssetFeatureContext';
import {useHistory, useLocation} from 'react-router-dom';
import {useEffect, useMemo, useRef, useState} from 'react';
import {ApiError, request} from './api';
import {batchBody, decisionBody, decisionDraft, draftScopeKey, mayComplete, mayContinue, mayRetract, nextPending, retractionBody} from './decisions';
import type {Draft} from './decisions';
import {
  initialQuery, stageResponses,
} from './models';
import type {Dataset, Review, ReviewRow, ReviewStage} from './models';
import {Scope} from './Scope';
import {QueryControls} from './QueryControls';
import {RowsTable} from './RowsTable';
import {Status} from './Status';
import {useSnapshotQuery} from './useResource';
import {useReviewer} from './useReviewer';
import {DecisionInspector} from './DecisionInspector';
import {PublicationLinks, RunLink} from './PublicationLinks';
import {useReviewStages} from './ReviewStageContext';
import {reviewPath, reviewScopeKey, reviewStageLink, stagesForAsset} from './reviewStages';
import styles from './ui.module.css';

export function ReviewView({assetKey}: AssetViewFeatureInput) {
  const location = useLocation();
  const history = useHistory();
  const registry = useReviewStages();
  const stages = stagesForAsset(registry.stages ?? [], assetKey.path);
  const requested = new URLSearchParams(location.search).get('review_stage');
  const stage = requested ? stages.find((entry) => entry.stage_id === requested) : stages[0];
  useEffect(() => {
    if (!requested && stage) {
      history.replace({...location, search: reviewStageLink(location.search, stage.stage_id)});
    }
  }, [requested, stage, history, location]);
  return <Scope>{(dataset) => <div className={styles.stack}>
    {registry.error !== undefined && <>
      <Status error={registry.error} />
      <Button onClick={registry.reload}>Refresh review stages</Button>
    </>}
    {!registry.stages && registry.error === undefined && <Status>Loading review stages...</Status>}
    {registry.stages && stages.length === 0 && <Status>No review stages are configured for this asset.</Status>}
    {stages.length > 0 && <label className={styles.field}>Review stage
      <select className={styles.select} value={stage?.stage_id ?? ''}
        onChange={(event) => history.push({...location, search: reviewStageLink(location.search, event.target.value)})}>
        {!stage && <option value="">Choose review stage</option>}
        {stages.map((entry) => <option key={entry.stage_id} value={entry.stage_id}>{entry.title}</option>)}
      </select>
    </label>}
    {requested && !stage && registry.stages && <Status>Unknown review stage for this asset. Choose a configured stage.</Status>}
    {stage && <ReviewTable key={reviewScopeKey(dataset.dataset_id, stage.stage_id)} dataset={dataset} stage={stage} />}
  </div>}</Scope>;
}

function ReviewTable({dataset, stage}: {readonly dataset: Dataset; readonly stage: ReviewStage}) {
  const [query, setQuery] = useState(initialQuery);
  const schemas = useMemo(
    () => stageResponses(stage.stage_id, stage.additional_publication_stages),
    [stage.stage_id, stage.additional_publication_stages],
  );
  const path = reviewPath(stage.stage_id);
  const result = useSnapshotQuery(path, schemas.review, dataset.dataset_id, query,
    (data) => data.batch.snapshot.snapshot_id);
  const [savedReview, setSavedReview] = useState<Review>();
  const review = result.data ?? savedReview;
  const {reviewer, change, storageError} = useReviewer();
  const [selected, setSelected] = useState<string>();
  const [drafts, setDrafts] = useState<Readonly<Record<string, Draft>>>({});
  const [saving, setSaving] = useState(false);
  const writeInFlight = useRef(false);
  const [writeError, setWriteError] = useState<unknown>();
  const [conflict, setConflict] = useState(false);
  const [notice, setNotice] = useState('');
  const prefix = review ? draftScopeKey(review.batch) : '';
  const draftKey = `${prefix}${selected ?? ''}`;
  const persisted = review?.decisions.find((entry) => entry.candidate_id === selected);
  const draft = drafts[draftKey] ?? decisionDraft(persisted);
  const dirty = Object.keys(drafts).some((key) => key.startsWith(prefix));
  const unavailable = saving || conflict || result.stale || result.loading || result.error !== undefined;
  const canRetract = !!review && !!selected && mayRetract(review, selected,
    {reviewer, dirty, unavailable, rowRevision: draft.rowRevision});

  function dropDraft() {
    setDrafts((current) => {
      const next = {...current};
      delete next[draftKey];
      return next;
    });
  }
  function refresh() {
    setConflict(false);
    setSavedReview(undefined);
    setWriteError(undefined);
    setNotice('Snapshot refreshed. Unsaved drafts are retained for their original batch and snapshot.');
    setQuery({...query, offset: 0});
    result.refresh();
  }
  function select(row: ReviewRow) {
    setSelected(row.review_record_id);
  }
  function handleFailure(error: unknown) {
    setWriteError(error);
    if (error instanceof ApiError && error.status === 409) {setConflict(true);}
  }
  async function save(next: boolean) {
    if (!review || !selected || unavailable || writeInFlight.current || !reviewer.trim() ||
      review.batch.state !== 'open' || draft.rowRevision !== (persisted?.row_revision ?? 0) ||
      (draft.decision === 'approve' && !review.batch.snapshot.eligible_candidate_ids.includes(selected))) {return;}
    writeInFlight.current = true;
    setSaving(true);
    setWriteError(undefined);
    setNotice('');
    try {
      const saved = await request(`${path}/decision`, schemas.saved, 'PUT',
        decisionBody(review.batch, selected, draft, reviewer));
      dropDraft();
      setNotice(`Decision saved. ${saved.counts.pending} pending; ${saved.counts.approve} approved; ${saved.counts.reject} rejected.`);
      if (next) {setSelected(nextPending(review, selected, saved.decision.decision));}
      result.reload();
    } catch (error) {handleFailure(error);}
    finally {writeInFlight.current = false; setSaving(false);}
  }
  async function retract() {
    if (!review || !persisted || !canRetract || writeInFlight.current) {return;}
    writeInFlight.current = true;
    setSaving(true);
    setWriteError(undefined);
    setNotice('');
    try {
      const saved = await request(`${path}/retract`, schemas.saved, 'POST',
        retractionBody(review.batch, persisted, reviewer));
      setSavedReview({...review, batch: saved.batch, counts: saved.counts, publication: null,
        decisions: review.decisions.map((entry) =>
          entry.candidate_id === saved.decision.candidate_id ? saved.decision : entry)});
      setNotice('Decision retracted. This record is pending. Dependent results are invalid until review is completed again; other saved decisions are preserved.');
      result.reload();
    } catch (error) {handleFailure(error);}
    finally {writeInFlight.current = false; setSaving(false);}
  }
  async function finish(action: 'complete' | 'reopen' | 'continue') {
    if (!review || unavailable || writeInFlight.current || !reviewer.trim()) {return;}
    if (action === 'complete' && !mayComplete(review, stage, reviewer, saving, dirty)) {return;}
    if (action === 'reopen' && (review.batch.state !== 'completed' || dirty)) {return;}
    if (action === 'continue' && !mayContinue(review, stage, {reviewer, dirty, unavailable})) {return;}
    writeInFlight.current = true;
    setSaving(true);
    setWriteError(undefined);
    setNotice('');
    try {
      const body = batchBody(review.batch, reviewer);
      switch (action) {
        case 'complete': {
          const completed = await request(`${path}/complete`, schemas.complete, 'POST', body);
          setSavedReview({...review, batch: {...completed.batch,
            continuation_run_id: completed.run_id ?? completed.batch.continuation_run_id},
          counts: completed.counts, publication: completed.publication});
          setNotice(completed.publication ? 'Review completed and publication registered.' : 'Review completed and saved.');
          break;
        }
        case 'reopen':
          await request(`${path}/reopen`, schemas.reopen, 'POST', body);
          setSavedReview(undefined);
          setNotice('Batch reopened. Dependent results are invalid until review is completed again. Persisted decisions are being restored.');
          break;
        case 'continue': {
          const continued = await request(`${path}/continue`, schemas.continue, 'POST', body);
          setSavedReview({...review, batch: {...continued.batch,
            continuation_run_id: continued.run_id ?? continued.batch.continuation_run_id},
          counts: continued.counts, publication: continued.publication});
          setNotice(continued.run_id ? 'Pipeline continuation started.' : 'Pipeline continuation is unchanged; no new run was started.');
          break;
        }
      }
      result.reload();
    } catch (error) {handleFailure(error);}
    finally {writeInFlight.current = false; setSaving(false);}
  }
  const runId = review?.batch.continuation_run_id ?? review?.publication?.dagster_run_id;
  return <div className={styles.stack}>
    <div className={styles.toolbar}>
      <label className={styles.field}>Reviewer (self-reported)
        <TextInput value={reviewer} disabled={saving} onChange={(e) => change(e.target.value)} />
      </label>
      {review && <span role="status">{review.counts.pending} pending / {review.counts.approve} approved / {review.counts.reject} rejected</span>}
    </div>
    {storageError && <Status>Browser storage is unavailable. Reviewer identity will not persist on refresh.</Status>}
    <QueryControls query={query} columns={review?.page.columns ?? []} onChange={setQuery}
      onRefresh={refresh} disabled={saving} />
    {result.loading && <Status>Loading persisted review decisions...</Status>}
    {result.error !== undefined && <Status error={result.error} />}
    {writeError !== undefined && <Status error={writeError} />}
    {notice && <Status>{notice}</Status>}
    {dirty && <Status>Unsaved drafts are retained while browsing. Save or discard them before completing review.</Status>}
    {review && <>
      <div className={styles.split}>
        <div className={styles.pane}><RowsTable page={review.page} query={query} onChange={setQuery}
          onSelect={select} disabled={saving} /></div>
        {selected && review.batch.snapshot.candidate_ids.includes(selected) && <DecisionInspector
          review={review} candidateId={selected} draft={draft} dirty={drafts[draftKey] !== undefined}
          disabled={unavailable || !reviewer.trim()} canRetract={canRetract} onRetract={() => void retract()}
          onDraft={(value) => setDrafts({...drafts, [draftKey]: value})}
          onDiscard={dropDraft} onSave={(next) => void save(next)}
          onRebase={() => setDrafts({...drafts, [draftKey]: {...draft, rowRevision: persisted?.row_revision ?? 0}})} />}
      </div>
      <Status>
        Completion requires no pending decisions or unsaved drafts and a reviewer.
        {' '}Approvals required: {stage.min_approvals} minimum; {stage.max_approvals === null ? 'no maximum' : `${stage.max_approvals} maximum`}.
        {' '}Currently approved: {review.counts.approve}.
      </Status>
      <div className={styles.toolbar}>
        {review.batch.state === 'completed' ? <>
          <span>Completed review is saved. Individual saved decisions can be retracted in the inspector.</span>
          {stage.can_continue && <Button intent="primary" loading={saving}
            disabled={!mayContinue(review, stage, {reviewer, dirty, unavailable})}
            onClick={() => void finish('continue')}>{stage.continuation_label}</Button>}
          <Button disabled={unavailable || dirty || !reviewer.trim()} onClick={() => void finish('reopen')}>Reopen</Button>
        </> : <Button intent="primary" loading={saving}
          disabled={unavailable || !mayComplete(review, stage, reviewer, saving, dirty)}
          onClick={() => void finish('complete')}>{stage.completion_label}</Button>}
      </div>
    </>}
    {runId && <RunLink runId={runId} />}
    {review?.publication && <PublicationLinks publication={review.publication}
      datasetId={dataset.dataset_id} historical={review.batch.state !== 'completed' ||
        review.publication.authorization.token !== review.batch.completion?.token} />}
  </div>;
}
