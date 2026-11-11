import {Button, Dialog, DialogBody, DialogFooter} from '@dagster-io/ui-components';
import type {AssetViewFeatureInput} from '@dagster-io/ui-core/assets/AssetFeatureContext';
import {useState} from 'react';
import {dataSchema, initialQuery} from './models';
import type {Dataset, Row} from './models';
import {Scope} from './Scope';
import {QueryControls} from './QueryControls';
import {RowsTable} from './RowsTable';
import {Fields} from './Fields';
import {Status} from './Status';
import {useSnapshotQuery} from './useResource';
import {cellText} from './api';
import styles from './ui.module.css';

export function DataView({assetKey}: AssetViewFeatureInput) {
  const stage = assetKey.path.slice(1).join('/');
  return <Scope>{(dataset) => <DataTable key={`${dataset.dataset_id}/${stage}`} dataset={dataset} stage={stage} />}</Scope>;
}

function DataTable({dataset, stage}: {readonly dataset: Dataset; readonly stage: string}) {
  const [query, setQuery] = useState(initialQuery);
  const [selected, setSelected] = useState<Row>();
  const result = useSnapshotQuery(`/data/${encodeURIComponent(stage)}`, dataSchema,
    dataset.dataset_id, query, (data) => data.snapshot_id);
  const data = result.data;
  function refresh() {setSelected(undefined); setQuery({...query, offset: 0}); result.refresh();}
  return <div className={styles.stack}>
    <QueryControls query={query} columns={data?.kind === 'table' ? data.page.columns : []}
      onChange={(next) => {setSelected(undefined); setQuery(next);}} onRefresh={refresh} />
    {result.loading && <Status>Loading persisted data...</Status>}
    {result.error !== undefined && <Status error={result.error} />}
    {data && <details><summary>Snapshot {data.snapshot_id.slice(0, 12)}</summary>
      <pre className={styles.literal}>{data.snapshot_id}</pre></details>}
    {data?.kind === 'table' && data.publication_state === 'legacy_unreviewed' &&
      <Status>Historical agent-reviewed output. This is not a registered human-reviewed publication.</Status>}
    {data?.kind === 'table' && data.publication_state === 'stale' &&
      <Status>Stale output: a supporting review was retracted or changed. These rows are historical, not currently approved.
        Complete the affected review and continue the pipeline before using dependent results.</Status>}
    {data?.kind === 'rules' && <pre className={styles.literal}>{cellText(data.rules)}</pre>}
    {data?.kind === 'table' && <RowsTable page={data.page} query={query} onChange={setQuery} onSelect={setSelected} />}
    <Dialog isOpen={!!selected} title="Complete row values" className={styles.dialog} onClose={() => setSelected(undefined)}>
      <DialogBody><div className={styles.dialogBody}>{selected && <Fields fields={selected.values} />}</div></DialogBody>
      <DialogFooter><Button onClick={() => setSelected(undefined)}>Close</Button></DialogFooter>
    </Dialog>
  </div>;
}
