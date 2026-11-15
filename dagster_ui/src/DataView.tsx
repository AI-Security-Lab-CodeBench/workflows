import type {AssetViewFeatureInput} from '@dagster-io/ui-core/assets/AssetFeatureContext';
import {Button} from '@dagster-io/ui-components';
import {useMemo, useState} from 'react';
import {useHistory, useLocation} from 'react-router-dom';
import {initialQuery} from './models';
import type {Row} from './models';
import {artifactResponse} from './artifacts';
import {FullscreenInspector} from './FullscreenInspector';
import {Scope} from './Scope';
import {QueryControls} from './QueryControls';
import {RowsTable} from './RowsTable';
import {ArtifactContent} from './ArtifactContent';
import {Status} from './Status';
import {useSnapshotQuery} from './useResource';
import {ArtifactInputs} from './ArtifactInputs';
import {artifactSelectionLink} from './query';
import styles from './ui.module.css';

export function DataView({assetKey}: AssetViewFeatureInput) {
  const location = useLocation();
  const history = useHistory();
  const params = new URLSearchParams(location.search);
  const mode = params.get('data_view') ?? 'output';
  return <section className={styles.surface} aria-label="Artifact inspection"><div className={styles.stack}>
    <div className={styles.toolbar}>
      <label className={styles.field}>Artifact
        <select className={styles.select} value={mode} onChange={(event) => {
          const value = event.target.value;
          if (value === 'output' || value === 'inputs') {
            history.push({...location, search: artifactSelectionLink(location.search, value)});
          }
        }}>
          {mode !== 'output' && mode !== 'inputs' && <option value={mode}>Unknown selection</option>}
          <option value="output">Output</option><option value="inputs">Inputs</option>
        </select>
      </label>
    </div>
    {mode === 'output' && <Scope assetKey={assetKey.path} embedded>{(partition) =>
      <DataTable key={JSON.stringify([assetKey.path, partition])} assetKey={assetKey.path} partition={partition} />}</Scope>}
    {mode === 'inputs' && <ArtifactInputs key={JSON.stringify([assetKey.path, params.get('partition')])}
      assetKey={assetKey.path}>{(input, partition) =>
        <DataTable key={JSON.stringify([input, partition])} assetKey={input} partition={partition} />}</ArtifactInputs>}
    {mode !== 'output' && mode !== 'inputs' && <Status>Unknown artifact selection. Choose Output or Inputs.</Status>}
  </div></section>;
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
    {data?.kind === 'table' ? <QueryControls query={query} columns={data.page.columns}
      onChange={(next) => {setSelected(undefined); setQuery(next);}} onRefresh={refresh} />
      : <div className={styles.toolbar}><Button onClick={refresh}>Refresh snapshot</Button></div>}
    {result.loading && <Status>Loading persisted data...</Status>}
    {result.error !== undefined && <Status error={result.error} />}
    {data && <details><summary>Snapshot {data.snapshot_id.slice(0, 12)}</summary>
      <pre className={styles.literal}>{data.snapshot_id}</pre></details>}
    {data?.kind === 'table' && data.publication_state === 'legacy_unreviewed' &&
      <Status>Historical agent-reviewed output. This is not a registered human-reviewed publication.</Status>}
    {data?.kind === 'table' && data.publication_state === 'stale' &&
      <Status>Stale output: a supporting review was retracted or changed. These rows are historical, not currently approved.
        Complete the affected review and continue the pipeline before using dependent results.</Status>}
    {data?.kind === 'json' && <ArtifactContent key={data.snapshot_id} value={data.value} dataSchema={data.data_schema} />}
    {data?.kind === 'table' && <ArtifactContent key={data.snapshot_id} table
      value={data.page.rows.map((row) => row.values)} dataSchema={data.data_schema}>
      <RowsTable page={data.page} query={query} onChange={setQuery} onSelect={setSelected} />
    </ArtifactContent>}
    <FullscreenInspector isOpen={!!selected} title="Complete row values" onClose={() => setSelected(undefined)}>
      {selected && data && <ArtifactContent key={`${data.snapshot_id}:${selected.row_index}`}
        value={selected.values} dataSchema={data.data_schema} />}
    </FullscreenInspector>
  </div>;
}
