import {Button} from '@dagster-io/ui-components';
import {NavCollapseContext} from '@dagster-io/ui-core/app/navigation/NavCollapseProvider';
import {useContext, useEffect, useMemo, useState} from 'react';
import type {ReactNode} from 'react';
import {useHistory, useLocation} from 'react-router-dom';
import {partitionsResponse} from './artifacts';
import {useResource} from './useResource';
import {Status} from './Status';
import styles from './ui.module.css';

export function Scope({assetKey, children}: {
  readonly assetKey: readonly string[];
  readonly children: (partition: string | null) => ReactNode;
}) {
  const location = useLocation();
  const history = useHistory();
  const [refresh, setRefresh] = useState(0);
  const body = useMemo(() => ({asset_key: assetKey}), [assetKey]);
  const schema = useMemo(() => partitionsResponse(assetKey), [assetKey]);
  const result = useResource('/artifacts/partitions', schema, body, refresh);
  const partition = new URLSearchParams(location.search).get('partition');
  const partitions = result?.data?.partitions;
  const selected = partitions?.includes(partition) ?? false;
  const {isCollapsed, toggleCollapsed, canCollapse} = useContext(NavCollapseContext);
  useEffect(() => {
    const first = partitions?.[0];
    if (partition === null && !selected && first !== undefined && first !== null) {
      const query = new URLSearchParams(location.search);
      query.set('partition', first);
      history.replace({...location, search: `?${query}`});
    }
  }, [partitions, partition, selected, history, location]);
  function choose(partitionKey: string | null) {
    const query = new URLSearchParams(location.search);
    if (partitionKey === null) {query.delete('partition');}
    else {query.set('partition', partitionKey);}
    history.push({...location, search: `?${query}`});
  }
  return <section className={styles.surface} aria-label="Persisted artifact">
    <div className={styles.stack}>
      <div className={styles.toolbar}>
        <label className={styles.field}>Partition
          <select className={styles.select} value={selected ? String(partitions?.indexOf(partition)) : ''}
            onChange={(event) => {
              const value = partitions?.[Number(event.target.value)];
              if (value !== undefined) {choose(value);}
            }}>
            {!selected && <option value="">{partition ?? 'Choose partition'}</option>}
            {partitions?.map((value, index) => <option key={index} value={index}>{value ?? 'Unpartitioned'}</option>)}
          </select>
        </label>
        {canCollapse && <Button onClick={toggleCollapsed}>{isCollapsed ? 'Show navigation' : 'Hide navigation'}</Button>}
      </div>
      {!result && <Status>Loading partitions...</Status>}
      {result?.error !== undefined && <>
        <Status error={result.error} />
        <Button onClick={() => setRefresh((value) => value + 1)}>Refresh partitions</Button>
      </>}
      {partitions?.length === 0 && <Status>No persisted partitions are available for this artifact.</Status>}
      {partitions && partition !== null && !selected && <Status>Unknown partition. Choose an available partition.</Status>}
      {selected && children(partition)}
    </div>
  </section>;
}
