import type {AssetViewFeatureInput} from '@dagster-io/ui-core/assets/AssetFeatureContext';
import {useMemo, useState} from 'react';
import {initialQuery} from './models';
import type {Row} from './models';
import {artifactResponse} from './artifacts';
import {FullscreenInspector} from './FullscreenInspector';
import {Scope} from './Scope';
import {QueryControls} from './QueryControls';
import {RowsTable} from './RowsTable';
import {Fields} from './Fields';
import {Status} from './Status';
import {useSnapshotQuery} from './useResource';
import {cellText} from './api';
import styles from './ui.module.css';

export function DataView({assetKey}: AssetViewFeatureInput) {
  return <Scope assetKey={assetKey.path}>{(partition) =>
    <DataTable key={JSON.stringify([assetKey.path, partition])} assetKey={assetKey.path} partition={partition} />}</Scope>;
}

function DataTable({assetKey, partition}: {readonly assetKey: readonly string[]; readonly partition: string | null}) {
  const [query, setQuery] = useState(initialQuery);
  const [selected, setSelected] = useState<Row>();
  const schema = useMemo(() => artifactResponse(assetKey, partition), [assetKey, partition]);
  const result = useSnapshotQuery('/artifacts/data', schema,
    {asset_key: assetKey, partition_key: partition}, query, (data) => data.snapshot_id);
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
    {data?.kind === 'json' && <pre className={styles.literal}>{cellText(data.value)}</pre>}
    {data?.kind === 'table' && <RowsTable page={data.page} query={query} onChange={setQuery} onSelect={setSelected} />}
    <FullscreenInspector isOpen={!!selected} title="Complete row values" onClose={() => setSelected(undefined)}>
      {selected && <Fields fields={selected.values} />}
    </FullscreenInspector>
  </div>;
}
