from pathlib import Path
from tempfile import mkdtemp

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetsDefinition,
    DataVersion,
    MetadataValue,
    Output,
    ResourceParam,
    asset,
)
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.config_assets import mapped_input
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    LabelingAttemptsTable,
    LabelingFailuresTable,
    ProposedAnnotationsTable,
    SourceDocumentsTable,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES
from codebench_pipeline.finalization_table_asset import (
    FINALIZATION_TABLE_IO,
    LABELING_INPUT,
    PROJECTION_INPUT,
    LabelingInput,
    ProjectionInput,
    projection_output,
)
from codebench_pipeline.finalization_table_io import retain_failed_finalization
from codebench_pipeline.grounding_tables import GuardedLabeling, LabelingEvidenceInputs
from codebench_pipeline.labeling_checkpoint_io import LABELING_ATTEMPTS, labeling_generation_identity
from codebench_pipeline.labeling_checkpoint_store import LabelingCheckpointCache
from codebench_pipeline.labeling_runtime import LabelingRuntime
from codebench_pipeline.labeling_tables import (
    failures_from_attempts,
    proposed_from_attempts,
)
from codebench_pipeline.stage_configuration import GuardedLabelingConfig, PolicyArtifact
from codebench_pipeline.storage import IntegrityError


def annotation_assets() -> list[AssetsDefinition]:
    @asset(
        key=LABELING_ATTEMPTS,
        group_name="dataset_finalization",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key=FINALIZATION_TABLE_IO,
        hooks={retain_failed_finalization},
        dagster_type=TABLE_TYPES[LabelingAttemptsTable],
        ins={
            "filtered_commands": AssetIn(
                key=AssetKey(["datasets", "filtered_commands"]),
                dagster_type=LABELING_INPUT,
                input_manager_key=FINALIZATION_TABLE_IO,
            ),
            "documentation_capture": AssetIn(
                key=AssetKey(["datasets", "documentation", "capture_documentation"]),
                dagster_type=TABLE_TYPES[SourceDocumentsTable],
                input_manager_key=FINALIZATION_TABLE_IO,
            ),
            "mapped_config": mapped_input(
                "labeling_attempts", GuardedLabelingConfig, input_manager_key=FINALIZATION_TABLE_IO
            ),
        },
    )
    def labeling_attempts(
        context: AssetExecutionContext,
        filtered_commands: LabelingInput,
        documentation_capture: DataFrame[SourceDocumentsTable],
        mapped_config: GuardedLabelingConfig,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
        labeling_runtime: ResourceParam[LabelingRuntime],
        labeling_checkpoints: ResourceParam[LabelingCheckpointCache],
    ) -> Output[DataFrame[LabelingAttemptsTable]]:
        if filtered_commands.attempt is None:
            raise IntegrityError("Labeling input hooks did not prepare the attempt")
        signature, scratch_root = filtered_commands.attempt
        capacity_paths = labeling_runtime.require_current(context.partition_key, dataset_io_manager)
        settings = mapped_config.labeling_attempts
        evidence_inputs = LabelingEvidenceInputs(
            dataset_io_manager.repository_run(context.partition_key, context.run_id),
            mapped_config.collection,
            settings.batch.grounding,
            documentation_capture,
        )
        labeling = GuardedLabeling(
            mapped_config,
            evidence_inputs,
            settings.batch.bind_capacity(capacity_paths.state_directory, capacity_paths.expected_model_path),
            labeling_runtime.execution(settings.model.api_key_env, settings.batch.grounding),
        )
        identity = labeling_generation_identity(
            context.partition_key, signature, capacity_paths.expected_model_path
        )
        commands = filtered_commands.commands
        scratch = Path(mkdtemp(prefix="dataset-inference-", dir=scratch_root))
        filtered_commands.scratch = scratch
        table = LabelingAttemptsTable.validate(
            labeling.attempts_for_generation(commands, scratch, labeling_checkpoints, identity)
        )
        table.attrs = {**commands.attrs, **table.attrs}
        evidence_inputs.verify(table)
        metadata = TypeAdapter(dict[str, JsonValue])
        context.add_output_metadata(
            {
                key: MetadataValue.json(metadata.validate_python(table.attrs[key]))
                for key in ("labeling_coverage", "labeling_quota")
                if key in table.attrs
            }
        )
        return Output(table, data_version=DataVersion(signature))

    attempts_edge = AssetIn(
        key=AssetKey(["datasets", "labeling_attempts"]),
        input_manager_key=FINALIZATION_TABLE_IO,
        dagster_type=PROJECTION_INPUT,
    )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key=FINALIZATION_TABLE_IO,
        dagster_type=TABLE_TYPES[ProposedAnnotationsTable],
        hooks={retain_failed_finalization},
        ins={
            "labeling_attempts": attempts_edge,
            "mapped_config": mapped_input(
                "proposed_annotations", PolicyArtifact, input_manager_key=FINALIZATION_TABLE_IO
            ),
        },
    )
    def proposed_annotations(
        labeling_attempts: ProjectionInput,
        mapped_config: PolicyArtifact,
    ) -> Output[DataFrame[ProposedAnnotationsTable]]:
        return projection_output(
            labeling_attempts,
            ProposedAnnotationsTable,
            lambda attempts: proposed_from_attempts(attempts, mapped_config.category_rules),
        )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key=FINALIZATION_TABLE_IO,
        dagster_type=TABLE_TYPES[LabelingFailuresTable],
        hooks={retain_failed_finalization},
        ins={"labeling_attempts": attempts_edge},
    )
    def labeling_failures(
        labeling_attempts: ProjectionInput,
    ) -> Output[DataFrame[LabelingFailuresTable]]:
        return projection_output(labeling_attempts, LabelingFailuresTable, failures_from_attempts)

    return [
        labeling_attempts,
        proposed_annotations,
        labeling_failures,
    ]
