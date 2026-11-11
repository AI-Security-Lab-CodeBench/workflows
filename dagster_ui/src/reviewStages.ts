import type {ReviewStage} from './models';

export function stagesForAsset(stages: readonly ReviewStage[], assetKey: readonly string[]) {
  return stages.filter((stage) => stage.asset_keys.some((key) =>
    key.length === assetKey.length && key.every((part, index) => part === assetKey[index])));
}

export function assetKeyFromPath(pathname: string): readonly string[] {
  return pathname.startsWith('/assets/') ?
    pathname.slice('/assets/'.length).split('/').map(decodeURIComponent) : [];
}

export function reviewStageLink(search: string, stageId: string): string {
  const query = new URLSearchParams(search);
  query.set('view', 'review');
  query.set('review_stage', stageId);
  return `?${query}`;
}

export function reviewPath(stageId: string): string {
  return `/reviews/${encodeURIComponent(stageId)}`;
}

export function reviewScopeKey(datasetId: string, stageId: string): string {
  return JSON.stringify([datasetId, stageId]);
}
