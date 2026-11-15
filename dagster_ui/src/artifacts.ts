import {artifactSchema, partitionsSchema} from './models';

function sameAsset(left: readonly string[], right: readonly string[]) {
  return left.length === right.length && left.every((part, index) => part === right[index]);
}

export function partitionsResponse(assetKey: readonly string[]) {
  return partitionsSchema.refine((data) => sameAsset(data.asset_key, assetKey));
}

export function artifactResponse(assetKey: readonly string[], partition: string | null) {
  return artifactSchema.refine((data) => sameAsset(data.asset_key, assetKey) && data.partition_key === partition);
}
