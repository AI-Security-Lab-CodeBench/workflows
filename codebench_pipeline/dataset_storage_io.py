from pathlib import Path
from typing import cast

import pandas as pd
from dagster import Field, InitResourceContext, InputContext, IOManagerDefinition, OutputContext, String
from dagster._core.storage.tags import PARTITION_NAME_TAG
from pydantic import BaseModel

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dagster_table_metadata import MetadataSink
from codebench_pipeline.dataframe_io import AssetSnapshot, DataFrameIOManager
from codebench_pipeline.dataset_branches import DECLARED_SOURCE_STAGES, FINALIZATION_STAGES
from codebench_pipeline.dataset_dependencies import review_evidence_stages
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.finalization_publication import TABLE_REGISTRATION_METADATA, TableOutputAdmission
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.stage_implementation import capture_implementation
from codebench_pipeline.storage import IntegrityError


class StorageBinding(BaseModel):
    registered_revision: Digest | None = None
    review_token: str | None = None


class RegisteredDatasetTableIO(DataFrameIOManager):
    """Apply dataset policy at the registered Dagster I/O boundary."""

    def handle_output(
        self,
        context: OutputContext,
        obj: pd.DataFrame | CategoryRules,
        *,
        admission: TableOutputAdmission | None = None,
    ) -> None:
        key = context.asset_key.path
        if key[0] != "datasets":
            if admission is not None:
                raise IntegrityError("Table output admission requires a registered dataset writer")
            super().handle_output(context, obj)
            return

        stage = key[-1]
        if key == ["datasets", "human_review_queue"]:
            from codebench_pipeline.review_preparation import write_review_queue

            if not isinstance(obj, pd.DataFrame):
                raise IntegrityError("Review queue output must be a table")
            value, reference = write_review_queue(context, self, obj)
        elif stage in {"human_review_decisions", "dataset_release", "automatic_dataset_release"}:
            from codebench_pipeline.release_admission import write_review_output

            if not isinstance(obj, pd.DataFrame):
                raise IntegrityError("Review outputs must be tables")
            value, reference = obj, write_review_output(self, context, obj)
        else:
            value = obj
            reference = self.persist_value(obj) if admission is None else admission.output
            partition = context.asset_partition_key
            with self.review_store.transaction(timeout=10, dataset_id=partition) as tx:
                if admission is not None or stage in FINALIZATION_STAGES:
                    from codebench_pipeline.dataset_collection_binding import authorize_finalization_output

                    registered = self.run_configuration(context.run_id, partition)
                    if stage not in DECLARED_SOURCE_STAGES:
                        if not isinstance(obj, pd.DataFrame):
                            raise IntegrityError("Finalization outputs must be tables")
                        authorize_finalization_output(
                            self,
                            context.step_context.instance,
                            registered,
                            obj,
                            complete=stage != "merged_command_candidates",
                        )
                    if admission is not None:
                        self._check_admission(partition, registered.registration_digest, admission)
                if stage in DECLARED_SOURCE_STAGES:
                    from codebench_pipeline.source_review_guard import authorize_output

                    if not isinstance(obj, pd.DataFrame):
                        raise IntegrityError("Source-dependent outputs must be tables")
                    authorize_output(context, self, obj)
                try:
                    previous = self.current_reference(context)
                except FileNotFoundError:
                    previous = None
                if previous != reference and "/".join(key[1:]) in review_evidence_stages():
                    tx.invalidate(partition, "Upstream asset reference changed")
                self.publish_reference(context, reference)
        if admission is not None:
            cast(MetadataSink, context).add_output_metadata(
                {TABLE_REGISTRATION_METADATA: admission.registration_digest}
            )
        self.record_output(context, value, reference)

    def _check_admission(
        self, partition: str, registration_digest: Digest, admission: TableOutputAdmission
    ) -> None:
        if registration_digest != admission.registration_digest:
            raise HumanReviewError("stale", "Registration changed before table publication")
        for upstream in admission.upstreams:
            if upstream.partition != partition:
                raise IntegrityError("Table input and output must share a writer lock")
            try:
                current = self.reference_at(upstream.asset_key, upstream.partition)
            except FileNotFoundError:
                current = None
            if current != upstream.reference:
                name = "/".join(upstream.asset_key)
                raise HumanReviewError("stale", f"Immediate upstream {name} changed before table publication")

    def load_input(self, context: InputContext) -> pd.DataFrame | CategoryRules:
        if context.asset_key.path[-1] == "dataset_release":
            from codebench_pipeline.release_admission import load_authorized_release

            return load_authorized_release(context, self)
        return super().load_input(context)

    def load_versioned_input(self, context: InputContext) -> AssetSnapshot:
        from codebench_pipeline.review_output_state import registered_materialization_stale

        snapshot = super().load_versioned_input(context)
        if isinstance(snapshot.value, pd.DataFrame):
            self.bind_configuration_producer(context)
            registered = self.run_configuration(context.step_context.run_id, context.asset_partition_key)
            if registered_materialization_stale(self, context, snapshot.value, registered):
                raise HumanReviewError(
                    "stale", "This materialization is no longer authorized by current review"
                )
        return snapshot


def dataset_storage_io(home: Path, *, review: bool = True) -> IOManagerDefinition:
    """Bind a run's partition to its registered object store."""
    registry = DatasetRegistry(home)
    review_home = str(home.resolve()) if review else None
    fields = {"registered_revision": Field(String, is_required=False)}
    if review:
        fields["review_token"] = Field(String, is_required=False)

    def bind(context: InitResourceContext) -> DataFrameIOManager:
        run = context.dagster_run
        partition = run.tags.get(PARTITION_NAME_TAG) if run is not None else None
        if partition is None:
            raise IntegrityError("Dataset storage requires a dataset partition")
        settings = StorageBinding.model_validate(context.resource_config)
        revision = settings.registered_revision or registry.current_revision(partition)
        manager_type = RegisteredDatasetTableIO if review else DataFrameIOManager
        manager = manager_type(
            storage=registry.storage(partition, revision=revision),
            review_home=review_home,
            review_token=settings.review_token,
            registered_revision=revision if review else None,
        )
        if review:
            manager.bind_review_store(
                HumanReviewStore(home, implementation=capture_implementation(manager.implementation_source))
            )
        manager.bind_object_store(ObjectStore.from_environment(manager.storage))
        return manager

    return IOManagerDefinition(resource_fn=bind, config_schema=fields)
