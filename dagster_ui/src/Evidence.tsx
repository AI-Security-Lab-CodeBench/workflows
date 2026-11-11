import {Button} from '@dagster-io/ui-components';
import {useState} from 'react';
import {evidenceSchema} from './models';
import {useResource} from './useResource';
import {Fields} from './Fields';
import {Status} from './Status';
import {reviewPath} from './reviewStages';
import styles from './ui.module.css';

export function Evidence({stageId, batchId, candidateId, datasetId}: {
  readonly stageId: string;
  readonly batchId: string; readonly candidateId: string; readonly datasetId: string;
}) {
  const [refresh, setRefresh] = useState(0);
  const path = `${reviewPath(stageId)}/${encodeURIComponent(batchId)}/evidence/${encodeURIComponent(candidateId)}?dataset_id=${encodeURIComponent(datasetId)}`;
  const result = useResource(path, evidenceSchema, undefined, refresh, 'GET');
  return <div className={styles.stack}>
    <strong>Record evidence</strong>
    {!result && <Status>Loading full immutable evidence...</Status>}
    {result?.error !== undefined && <>
      <Status error={result.error} />
      <Button onClick={() => setRefresh((value) => value + 1)}>Refresh evidence</Button>
    </>}
    {result?.data && Object.entries(result.data.fields).map(([name, value]) =>
      <details key={name}>
        <summary>{name.replaceAll('_', ' ')}</summary>
        <Fields fields={{[name]: value}} />
      </details>)}
  </div>;
}
