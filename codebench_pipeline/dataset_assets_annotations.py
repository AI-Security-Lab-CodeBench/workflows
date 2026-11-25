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

from codebench_pipeline.agent_trace import graph_log
from codebench_pipeline.config_assets import mapped_input
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    LabelingAttemptsTable,
    LabelingFailuresTable,
    ProposedAnnotationsTable,
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
from codebench_pipeline.grounding_tables import GuardedLabeling, verify_live_support
from codebench_pipeline.labeling_checkpoint_io import LABELING_ATTEMPTS, labeling_generation_identity
from codebench_pipeline.labeling_checkpoint_store import LabelingCheckpointCache
from codebench_pipeline.labeling_runtime import LabelingRuntime
from codebench_pipeline.labeling_stage_settings import ProposalRules
from codebench_pipeline.labeling_tables import (
    failures_from_attempts,
    proposed_from_attempts,
)
from codebench_pipeline.stage_configuration import GuardedLabelingConfig
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
            "mapped_config": mapped_input(
                "labeling_attempts", GuardedLabelingConfig, input_manager_key=FINALIZATION_TABLE_IO
            ),
        },
    )
    def labeling_attempts(
        context: AssetExecutionContext,
        filtered_commands: LabelingInput,
        mapped_config: GuardedLabelingConfig,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
        labeling_runtime: ResourceParam[LabelingRuntime],
        labeling_checkpoints: ResourceParam[LabelingCheckpointCache],
    ) -> Output[DataFrame[LabelingAttemptsTable]]:
        if filtered_commands.attempt is None:
            raise IntegrityError("Labeling input hooks did not prepare the attempt")
        signature, scratch_root = filtered_commands.attempt
        settings = mapped_config.labeling_attempts
        if settings.batch.inference_mode == "guarded":
            paths = labeling_runtime.require_current(context.partition_key, dataset_io_manager)
            batch = settings.batch.bind_capacity(paths.state_directory, paths.expected_model_path)
            expected_model_path = paths.expected_model_path
        else:
            batch = settings.batch.bind_capacity()
            expected_model_path = None
        labeling = GuardedLabeling(
            mapped_config,
            batch,
            labeling_runtime.execution(settings.model.api_key_env, settings.batch.grounding),
        )
        identity = labeling_generation_identity(context.partition_key, signature, expected_model_path)
        commands = filtered_commands.commands
        scratch = Path(mkdtemp(prefix="dataset-inference-", dir=scratch_root))
        filtered_commands.scratch = scratch
        with graph_log(context.log.info, labeling_runtime.credential):
            table = LabelingAttemptsTable.validate(
                labeling.attempts_for_generation(commands, scratch, labeling_checkpoints, identity)
            )
        table.attrs = {**commands.attrs, **table.attrs}
        verify_live_support(table, batch.grounding)
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
                "proposed_annotations", ProposalRules, input_manager_key=FINALIZATION_TABLE_IO
            ),
        },
    )
    def proposed_annotations(
        labeling_attempts: ProjectionInput,
        mapped_config: ProposalRules,
    ) -> Output[DataFrame[ProposedAnnotationsTable]]:
        return projection_output(
            labeling_attempts,
            ProposedAnnotationsTable,
            lambda attempts: proposed_from_attempts(attempts, mapped_config),
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
