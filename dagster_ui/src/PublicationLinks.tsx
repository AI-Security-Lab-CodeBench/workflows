import type {Publication} from './models';
import styles from './ui.module.css';

export function RunLink({runId}: {readonly runId: string}) {
  return <div className={styles.toolbar}>
    <a href={`/runs/${encodeURIComponent(runId)}`}>Open Dagster run</a>
  </div>;
}

export function PublicationLinks({publication, datasetId, historical = false}: {
  readonly publication: Publication; readonly datasetId: string; readonly historical?: boolean;
}) {
  return <div className={styles.toolbar} role="status">
    <span>{historical ? 'Historical publication; not currently approved.' : 'Publication registered.'}</span>
    <a download href={`/codebench/api/publications/${encodeURIComponent(publication.publication_id)}/commands.csv?dataset_id=${encodeURIComponent(datasetId)}`}>
      {historical ? 'Download historical commands.csv' : 'Download commands.csv'}
    </a>
  </div>;
}
