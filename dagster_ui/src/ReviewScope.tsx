import {Button} from '@dagster-io/ui-components';
import {useEffect, useMemo, useState} from 'react';
import type {ReactNode} from 'react';
import {useHistory, useLocation} from 'react-router-dom';
import {reviewPartitionsSchema} from './models';
import type {ReviewPartition} from './models';
import {reviewPath} from './reviewStages';
import {useResource} from './useResource';
import {Status} from './Status';
import styles from './ui.module.css';

export function ReviewScope({assetKey, stageId, children}: {
  readonly assetKey: readonly string[]; readonly stageId: string;
  readonly children: (partition: ReviewPartition) => ReactNode;
}) {
  const location = useLocation();
  const history = useHistory();
  const [refresh, setRefresh] = useState(0);
  const body = useMemo(() => ({asset_key: assetKey}), [assetKey]);
  const result = useResource(`${reviewPath(stageId)}/partitions`, reviewPartitionsSchema, body, refresh);
  const query = new URLSearchParams(location.search);
  const partitionKey = query.get('partition');
  const scopeId = query.get('review_scope') ?? '';
  const partitions = result?.data?.partitions;
  const selected = partitions?.find((entry) => entry.partition_key === partitionKey && entry.scope_id === scopeId);
  function choose(entry: ReviewPartition, replace = false) {
    const next = new URLSearchParams(location.search);
    if (entry.partition_key === null) {next.delete('partition');}
    else {next.set('partition', entry.partition_key);}
    if (entry.scope_id) {next.set('review_scope', entry.scope_id);}
    else {next.delete('review_scope');}
    const target = {...location, search: `?${next}`};
    if (replace) {history.replace(target);}
    else {history.push(target);}
  }
  useEffect(() => {
    const first = partitions?.[0];
    if (!selected && partitionKey === null && first) {choose(first, true);}
  }, [partitions, selected, partitionKey, location, history]);
  return <div className={styles.stack}>
    <label className={styles.field}>Partition
      <select className={styles.select} value={selected ? String(partitions?.indexOf(selected)) : ''}
        onChange={(event) => {
          const entry = partitions?.[Number(event.target.value)];
          if (entry) {choose(entry);}
        }}>
        {!selected && <option value="">{partitionKey ?? 'Choose partition'}</option>}
        {partitions?.map((entry, index) => <option key={index} value={index}>
          {entry.partition_key ?? 'Unpartitioned'}{entry.scope_id ? ` / ${entry.scope_id}` : ''}
        </option>)}
      </select>
    </label>
    {!result && <Status>Loading prepared review partitions...</Status>}
    {result?.error !== undefined && <>
      <Status error={result.error} /><Button onClick={() => setRefresh((value) => value + 1)}>Refresh partitions</Button>
    </>}
    {partitions?.length === 0 && <Status>No prepared review is available. Materialize this review stage first.</Status>}
    {partitions && partitions.length > 0 && !selected && <Status>Choose an available review partition.</Status>}
    {selected?.invalidated && <Status>Review inputs changed. Materialize this stage to prepare the current artifact.</Status>}
    {selected && !selected.invalidated && children(selected)}
  </div>;
}
