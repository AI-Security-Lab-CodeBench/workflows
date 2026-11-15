import {Button} from '@dagster-io/ui-components';
import {gql, useQuery} from '@dagster-io/ui-core/apollo-client';
import type {ReactNode} from 'react';
import {useState} from 'react';
import {useHistory, useLocation} from 'react-router-dom';
import {artifactSelectionLink} from './query';
import {Scope} from './Scope';
import {Status} from './Status';
import styles from './ui.module.css';

type DependenciesQuery = {
  readonly assetNodeOrError:
    | {readonly __typename: 'AssetNode'; readonly id: string;
       readonly dependencyKeys: readonly {readonly path: readonly string[]}[]}
    | {readonly __typename: 'AssetNotFoundError'}
    | {readonly __typename: 'PythonError'};
};

const DEPENDENCIES_QUERY = gql`
  query CodeBenchArtifactInputs($assetKey: AssetKeyInput!) {
    assetNodeOrError(assetKey: $assetKey) {
      __typename
      ... on AssetNode {
        id
        dependencyKeys { path }
      }
    }
  }
`;

export function ArtifactInputs({assetKey, children}: {
  readonly assetKey: readonly string[];
  readonly children: (assetKey: readonly string[], partition: string | null) => ReactNode;
}) {
  const location = useLocation();
  const history = useHistory();
  const requested = new URLSearchParams(location.search).get('data_input');
  const [retryError, setRetryError] = useState<unknown>();
  const result = useQuery<DependenciesQuery, {assetKey: {path: readonly string[]}}>(DEPENDENCIES_QUERY, {
    variables: {assetKey: {path: assetKey}},
    fetchPolicy: 'network-only',
    notifyOnNetworkStatusChange: true,
  });
  const node = result.data?.assetNodeOrError;
  const error = result.error ?? retryError;
  const dependencies = !result.loading && !error && node?.__typename === 'AssetNode'
    ? node.dependencyKeys : undefined;
  const selected = dependencies?.find((key) => JSON.stringify(key.path) === requested);
  const failed = !result.loading && (error !== undefined || dependencies === undefined);
  return <div className={styles.stack}>
    <Status>Inputs show current upstream persisted artifacts, not the exact historical versions consumed by this output.
      Input partitions are selected independently; no owner-to-input partition mapping is applied.</Status>
    {result.loading && <Status>Loading direct upstream assets...</Status>}
    {failed && <>
      <Status>Could not discover direct upstream assets. Retry or choose Output.</Status>
      {error !== undefined && <Status error={error} />}
      <Button onClick={() => {
        setRetryError(undefined);
        void result.refetch().catch((error: unknown) => setRetryError(error));
      }}>Retry input discovery</Button>
    </>}
    {dependencies && <div className={styles.toolbar}>
      <label className={styles.field}>Input asset
        <select className={styles.select} value={selected ? JSON.stringify(selected.path) : ''}
          onChange={(event) => {
            const input = dependencies.find((key) => JSON.stringify(key.path) === event.target.value);
            if (input) {
              history.push({...location, search: artifactSelectionLink(location.search, 'inputs', input.path)});
            }
          }}>
          <option value="">{requested && !selected ? 'Unknown input asset' : 'Choose input asset'}</option>
          {dependencies.map((key) => <option key={JSON.stringify(key.path)} value={JSON.stringify(key.path)}>
            {key.path.join('/')}
          </option>)}
        </select>
      </label>
    </div>}
    {dependencies?.length === 0 && <Status>This asset has no direct upstream inputs. Choose Output to inspect its artifact.</Status>}
    {dependencies && requested !== null && !selected &&
      <Status>Unknown input asset for this owner. Choose an available input asset or Output.</Status>}
    {selected && <Scope key={JSON.stringify(selected.path)} assetKey={selected.path}
      partitionParameter="input_partition" selectFirst={false} embedded>
      {(partition) => children(selected.path, partition)}
    </Scope>}
  </div>;
}
