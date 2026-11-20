from dagster import DagsterInstance
from dagster._core.asset_graph_view.asset_graph_view import AssetGraphView
from dagster._core.definitions.data_version import CachingStaleStatusResolver
from dagster._core.definitions.temporal_context import TemporalContext
from dagster._time import get_current_datetime


def provenance_resolver(instance: DagsterInstance) -> CachingStaleStatusResolver:
    """Read recorded Dagster lineage; this does not authorize use of the output."""
    from codebench_pipeline.definitions import defs

    graph = defs.resolve_asset_graph()
    view = AssetGraphView(
        temporal_context=TemporalContext(effective_dt=get_current_datetime(), last_event_id=None),
        instance=instance,
        asset_graph=graph,
    )
    return CachingStaleStatusResolver(instance, graph, view)
