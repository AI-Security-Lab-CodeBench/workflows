from pathlib import Path
from typing import Final

from dagster import AssetKey, AssetRecordsFilter, DagsterInstance

from codebench_pipeline.artifact_views import (
    ArtifactBinding,
    ArtifactKey,
    ArtifactPartitions,
    ArtifactTarget,
    RunConfigBinding,
)
from codebench_pipeline.config_assets import COLLECTION_SCOPES, GLOBAL_CONFIG, mapping_key
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.dataframe_io import AssetReference, asset_storage_key
from codebench_pipeline.dataset_branches import FINALIZATION_STAGES, asset_branch
from codebench_pipeline.dataset_plan import KNOWN_STAGES, graph_materialization_stale
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.object_store import ObjectStore

CONFIG_KEYS: Final = frozenset(
    tuple(key.path)
    for key in (
        GLOBAL_CONFIG,
        mapping_key("semantic_evidence"),
        *(mapping_key(scope) for scope in (*COLLECTION_SCOPES.values(), *FINALIZATION_STAGES)),
    )
)


class DatasetArtifactCatalog:
    """Bind dataset storage to the generic artifact viewer without resolving business inputs."""

    def __init__(self, home: Path, instance: DagsterInstance) -> None:
        self.home = home
        self.instance = instance
        self.registry = DatasetRegistry(home)

    def partitions(self, key: ArtifactKey) -> ArtifactPartitions:
        partitions: list[str] = []
        for partition in self.registry.keys():
            try:
                self.resolve(ArtifactTarget(asset_key=key.asset_key, partition_key=partition))
            except DatasetViewError as error:
                if error.code not in {"not_found", "not_materialized"}:
                    raise
            else:
                partitions.append(partition)
        return ArtifactPartitions(asset_key=key.asset_key, partitions=tuple(partitions))

    def resolve(self, target: ArtifactTarget) -> ArtifactBinding | RunConfigBinding:
        if target.asset_key[0] != "datasets" or target.partition_key is None:
            raise DatasetViewError("not_found", "No persisted artifact binding")
        if target.asset_key in CONFIG_KEYS:
            records = self.instance.fetch_materializations(
                AssetRecordsFilter(
                    asset_key=AssetKey(target.asset_key), asset_partitions=[target.partition_key]
                ),
                limit=1,
            ).records
            if records:
                path = self.home.joinpath("configuration-runs", records[0].run_id, *target.asset_key)
                if path.is_file():
                    retired = (
                        target.asset_key[-1]
                        in {
                            "map_labeling_documentation_config",
                            "map_semantic_evidence_config",
                        }
                        and self.registry.location(target.partition_key).has_declared_collection
                    )
                    return RunConfigBinding(path, "stale" if retired else None)
            raise DatasetViewError("not_materialized", "This config has no persisted output")
        try:
            location = self.registry.location(target.partition_key)
        except FileNotFoundError as error:
            raise DatasetViewError("not_found", "Partition is not registered") from error
        state = None
        storage = location.storage
        if target.asset_key[-1] == "dataset_release":
            branch = asset_branch(target.asset_key)
            if location.has_declared_collection:
                from codebench_pipeline.release_admission import read_publication

                release = read_publication(self.home, target.partition_key, self.instance, branch)
                if release is not None:
                    return ArtifactBinding(storage, release.reference, release.publication_state)
            else:
                with HumanReviewStore(self.home).reader() as tx:
                    published = tx.last_publication(target.partition_key, branch=branch)
                    if published is not None:
                        state = "stale"
                        if published.proposal_digest is None:
                            try:
                                batch = tx.get_current_batch(
                                    target.partition_key, published.authorization.review_stage, compact=True
                                )
                            except HumanReviewError as error:
                                if error.code not in {"not_found", "stale"}:
                                    raise
                            else:
                                if batch.state == "completed" and batch.completion == published.authorization:
                                    state = "published"
                        return ArtifactBinding(
                            storage,
                            AssetReference(format="parquet", digest=published.payload_hash),
                            state,
                        )
            state = "legacy_unreviewed"
        try:
            payload = ObjectStore(storage).read(asset_storage_key(target.asset_key, target.partition_key))
            reference = AssetReference.model_validate_json(payload)
        except FileNotFoundError as error:
            raise DatasetViewError("not_materialized", "This artifact has no persisted output") from error
        stage = "/".join(target.asset_key[1:])
        if location.has_declared_collection and (stage in KNOWN_STAGES or stage == "semantic_evidence"):
            spec = self.registry.get_snapshot(target.partition_key)
            if graph_materialization_stale(spec, self.instance, home=self.home, stage=stage):
                state = "stale"
        elif stage.startswith("keenable/"):
            state = "stale"
        return ArtifactBinding(storage, reference, state)
