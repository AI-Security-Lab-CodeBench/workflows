import * as React from 'react';
import {useLocation} from 'react-router-dom';
import {AssetFeatureContext} from '@dagster-io/ui-core/assets/AssetFeatureContext';
import type {AssetViewFeatureInput} from '@dagster-io/ui-core/assets/AssetFeatureContext';
import {useAssetTabs} from '@dagster-io/ui-core/assets/AssetTabs';
import type {AssetTabConfig, AssetTabConfigInput} from '@dagster-io/ui-core/assets/AssetTabs';
import {AssetsOverviewRoot as OriginalRoot} from '@dagster-io/ui-core/shared/assets/AssetsOverviewRoot';
import {DataView} from './DataView';
import {ReviewView} from './ReviewView';
import {panelLink} from './api';
import {useNarrowNavigation} from './useNarrowNavigation';
import {ReviewStagesProvider, useReviewStages} from './ReviewStageContext';
import {assetKeyFromPath, reviewStageLink, stagesForAsset} from './reviewStages';

export {ASSETS_OVERVIEW_ROOT_QUERY} from '@dagster-io/ui-core/shared/assets/AssetsOverviewRoot';
type Features = React.ContextType<typeof AssetFeatureContext>;
type Props = React.ComponentProps<typeof OriginalRoot>;
const views: Readonly<Record<string, React.ComponentType<AssetViewFeatureInput> | undefined>> = {
  data: DataView,
  review: ReviewView,
};
function isDataAsset(path: readonly string[]): boolean {
  return path[0] === 'datasets' &&
    (path.length === 2 || (path.length === 3 && path[1] === 'documentation'));
}

function useProjectTabs(input: AssetTabConfigInput): AssetTabConfig[] {
  const defaults = useAssetTabs(input);
  const location = useLocation();
  const registry = useReviewStages();
  const assetKey = assetKeyFromPath(location.pathname);
  const stages = stagesForAsset(registry.stages ?? [], assetKey);
  const dataAsset = isDataAsset(assetKey);
  if (!dataAsset && stages.length === 0) {
    return defaults;
  }
  const additions: AssetTabConfig[] = dataAsset ? [
    {id: 'data', title: 'Data', to: panelLink(location.search, 'data'), mobile: true},
  ] : [];
  const requested = new URLSearchParams(location.search).get('review_stage');
  const selected = stages.find((stage) => stage.stage_id === requested) ?? stages[0];
  if (selected) {
    additions.push({id: 'review', title: 'Review',
      to: reviewStageLink(location.search, selected.stage_id), mobile: true});
  }
  const partition = new URLSearchParams(location.search).get('partition');
  const native = defaults.map((tab) => {
    if (!partition && !requested) {return tab;}
    const query = new URLSearchParams(tab.to);
    if (partition) {query.set('partition', partition);}
    if (requested) {query.set('review_stage', requested);}
    return {...tab, to: `?${query}`};
  });
  const overview = native.findIndex((tab) => tab.id === 'overview');
  return [...native.slice(0, overview + 1), ...additions, ...native.slice(overview + 1)];
}

export function AssetsOverviewRoot(props: Props) {
  return <ReviewStagesProvider><ProjectAssetsOverviewRoot {...props} /></ReviewStagesProvider>;
}

function ProjectAssetsOverviewRoot(props: Props) {
  const inherited = React.useContext(AssetFeatureContext);
  const location = useLocation();
  useNarrowNavigation(isDataAsset(assetKeyFromPath(location.pathname)));
  const value = React.useMemo<Features>(() => ({
    ...inherited,
    useTabBuilder: useProjectTabs,
    renderFeatureView(input) {
      const View = views[input.selectedTab];
      return View && (isDataAsset(input.assetKey.path) || View === ReviewView)
        ? <View {...input} /> : inherited.renderFeatureView(input);
    },
  }), [inherited]);
  return <AssetFeatureContext.Provider value={value}><OriginalRoot {...props} /></AssetFeatureContext.Provider>;
}

export default AssetsOverviewRoot;
