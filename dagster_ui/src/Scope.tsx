import {Button} from '@dagster-io/ui-components';
import {NavCollapseContext} from '@dagster-io/ui-core/app/navigation/NavCollapseProvider';
import {useContext, useEffect, useState} from 'react';
import type {ReactNode} from 'react';
import {useHistory, useLocation} from 'react-router-dom';
import {datasetsSchema} from './models';
import type {Dataset} from './models';
import {useResource} from './useResource';
import {Status} from './Status';
import styles from './ui.module.css';

export function Scope({children}: {readonly children: (dataset: Dataset) => ReactNode}) {
  const location = useLocation();
  const history = useHistory();
  const [refresh, setRefresh] = useState(0);
  const result = useResource('/datasets', datasetsSchema, undefined, refresh, 'GET');
  const partition = new URLSearchParams(location.search).get('partition');
  const datasets = result?.data?.datasets;
  const selected = datasets?.find((dataset) => dataset.dataset_id === partition);
  const {isCollapsed, toggleCollapsed, canCollapse} = useContext(NavCollapseContext);
  useEffect(() => {
    const first = datasets?.[0];
    if (!partition && first) {
      const query = new URLSearchParams(location.search);
      query.set('partition', first.dataset_id);
      history.replace({...location, search: `?${query}`});
    }
  }, [datasets, partition, history, location]);
  function choose(datasetId: string) {
    const query = new URLSearchParams(location.search);
    query.set('partition', datasetId);
    history.push({...location, search: `?${query}`});
  }
  return <section className={styles.surface} aria-label="Persisted dataset">
    <div className={styles.stack}>
      <div className={styles.toolbar}>
        <label className={styles.field}>Category
          <select className={styles.select} value={partition ?? ''} onChange={(e) => choose(e.target.value)}>
            {!selected && <option value={partition ?? ''}>{partition ?? 'Choose category'}</option>}
            {datasets?.map((dataset) => <option key={dataset.dataset_id} value={dataset.dataset_id}>{dataset.category}</option>)}
          </select>
        </label>
        {canCollapse && <Button onClick={toggleCollapsed}>{isCollapsed ? 'Show navigation' : 'Hide navigation'}</Button>}
      </div>
      {!result && <Status>Loading categories...</Status>}
      {result?.error !== undefined && <>
        <Status error={result.error} />
        <Button onClick={() => setRefresh((value) => value + 1)}>Refresh categories</Button>
      </>}
      {datasets?.length === 0 && <Status>No datasets are configured.</Status>}
      {datasets && partition && !selected && <Status>Unknown category. Choose a configured category.</Status>}
      {selected && children(selected)}
    </div>
  </section>;
}
