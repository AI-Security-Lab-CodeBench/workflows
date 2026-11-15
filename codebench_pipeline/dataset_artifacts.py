from pathlib import Path

from codebench_pipeline.artifact_views import (
    ArtifactBinding,
    ArtifactKey,
    ArtifactPartitions,
    ArtifactTarget,
)
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.dataframe_io import AssetReference, asset_storage_key
from codebench_pipeline.dataset_branches import asset_branch
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.object_store import ObjectStore


class DatasetArtifactCatalog:
    """Bind dataset storage to the generic artifact viewer without resolving business inputs."""

    def __init__(self, home: Path) -> None:
        self.home = home
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

    def resolve(self, target: ArtifactTarget) -> ArtifactBinding:
        if target.asset_key[0] != "datasets" or target.partition_key is None:
            raise DatasetViewError("not_found", "No persisted artifact binding")
        try:
            storage = self.registry.storage(target.partition_key)
        except FileNotFoundError as error:
            raise DatasetViewError("not_found", "Partition is not registered") from error
        state = None
        if target.asset_key[-1] == "dataset_release":
            with HumanReviewStore(self.home).reader() as tx:
                published = tx.last_publication(
                    target.partition_key, branch=asset_branch(target.asset_key)
                )
                if published is not None:
                    state = "stale"
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
        return ArtifactBinding(storage, reference, state)
