from typing import Final
from uuid import UUID

import pandas as pd
import pandera.pandas as pa
from dagster import (
    AssetExecutionContext,
    AssetKey,
    DagsterEventType,
    DagsterRunStatus,
    InputContext,
    MetadataValue,
    TextMetadataValue,
    input_manager,
)
from dagster._core.execution.stats import StepEventStatus
from dagster._core.storage.tags import PARTITION_NAME_TAG
from pandera.typing import DataFrame, Series
from pydantic import BaseModel, ValidationError

from codebench_pipeline.config_assets import ConfigRunIOManager, current_configuration_record, mapping_key
from codebench_pipeline.dataframe_io import DataFrameIOManager, materialized_reference
from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID
from codebench_pipeline.finalization_cache_reuse import materialization_attempt_succeeded
from codebench_pipeline.finalization_publication import TABLE_REGISTRATION_METADATA
from codebench_pipeline.input_provenance import current_materialization, input_materialization
from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import sha256
from codebench_pipeline.rules_contracts import Bundle
from codebench_pipeline.rules_corpus import Corpus, build_corpus
from codebench_pipeline.rules_learning import CaptureResult, DedupResult
from codebench_pipeline.rules_metrics import HeldOutMetrics, TrainFingerprint
from codebench_pipeline.rules_settings import (
    RULES_CONFIG_IO,
    RULES_REGISTRATION,
    LearningPins,
    RegisteredRules,
    RulesCaptureSettings,
)
from codebench_pipeline.split_schema import SplitRowsTable
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.train_assets import TRAIN_TABLE_IO, Split
from codebench_pipeline.typed_config import ConfigArtifact

RULES_EVAL_KEY: Final = AssetKey(["codebench", "rules_eval"])
RULES_EVIDENCE_DIGEST: Final = "codebench/private_evidence_digest"


class RulesCaptureArtifact(ConfigArtifact):
    pins: LearningPins
    category: str
    inference_digest: Digest
    result: CaptureResult


class RulesDedupArtifact(ConfigArtifact):
    pins: LearningPins
    result: DedupResult


class FinalizedRules(ConfigArtifact):
    pins: LearningPins
    bundle: Bundle
    bundle_sha256: Digest
    training: TrainFingerprint


@input_manager(required_resource_keys={TRAIN_TABLE_IO, RULES_CONFIG_IO})
def rules_corpus_input(context: InputContext) -> Corpus:
    tables = context.resources.train_table_io
    if not isinstance(tables, DataFrameIOManager) or tables.registered_revision is None:
        raise IntegrityError("Rules inputs require registered split storage")
    split: Split
    match context.asset_key.path:
        case ["train", "merged_rows"]:
            split = "train"
        case ["test", "merged_rows"]:
            split = "test"
        case _:
            raise IntegrityError("Rules inputs must be a declared merged split")
    record = input_materialization(context)
    if (
        record is None
        or (materialization := record.asset_materialization) is None
        or materialization.metadata.get(TABLE_REGISTRATION_METADATA)
        != TextMetadataValue(tables.registered_revision)
    ):
        raise IntegrityError(f"The {split} merge does not match the current split registration")
    manager = context.resources.config_run_io_manager
    if not isinstance(manager, ConfigRunIOManager):
        raise IntegrityError("Rules split I/O requires the pinned rules registration")
    configuration = current_configuration_record(context.instance, RULES_REGISTRATION, CODEBENCH_DATASET_ID)
    registration = manager.materialized_scope(configuration, RegisteredRules).registration
    pin = registration.train if split == "train" else registration.test
    reference = materialized_reference(context.instance, record)
    if reference.digest != pin.digest:
        raise IntegrityError(f"Rules run requires the registered {split} snapshot")
    frame = tables.read_version(reference)
    if not isinstance(frame, pd.DataFrame) or len(frame) != pin.row_count:
        raise IntegrityError(f"The pinned {split} input is not a table with the registered row count")
    return build_corpus(SplitRowsTable.validate(frame), split=split, digest=pin.digest)


@input_manager(required_resource_keys={RULES_CONFIG_IO})
def rules_capture_inputs(context: InputContext) -> dict[str, RulesCaptureArtifact]:
    manager = context.resources.config_run_io_manager
    if not isinstance(manager, ConfigRunIOManager):
        raise IntegrityError("Capture fan-in requires private run storage")
    configuration = current_configuration_record(context.instance, RULES_REGISTRATION, CODEBENCH_DATASET_ID)
    selected = set(manager.materialized_scope(configuration, RegisteredRules).registration.categories)
    captures: dict[str, RulesCaptureArtifact] = {}
    for category in sorted(selected.intersection(context.asset_partition_keys)):
        record = current_materialization(context.instance, context.asset_key, category)
        if record is not None and materialization_attempt_succeeded(
            context.instance, record, output_name="result", retry_number="native"
        ):
            captures[category] = manager.materialized_scope(record, RulesCaptureArtifact)
    return captures


class RulesMetricsTable(pa.DataFrameModel):
    """Aggregate holdout measurements; undefined ratios have a null value."""

    cohort: Series[str]
    axis: Series[str]
    label: Series[str]
    metric: Series[str]
    value: Series[float] = pa.Field(nullable=True, ge=0)

    class Config(pa.DataFrameModel.Config):
        strict = True
        ordered = True


def aggregate_holdout(metrics: HeldOutMetrics) -> DataFrame[RulesMetricsTable]:
    measurements: list[tuple[str, str, str, str, int | float | None]] = [
        ("overall", "overlap", "", "rows", metrics.contaminated_rows),
        ("overall", "overlap", "", "unique_commands", metrics.contaminated_commands),
    ]
    for cohort, summary in (
        ("overall", metrics.overall),
        ("uncontaminated", metrics.uncontaminated),
        ("trained", metrics.trained),
        ("unseen", metrics.unseen),
    ):
        measurements.extend((
            (cohort, "outcome", "", "support", summary.support),
            (cohort, "outcome", "", "correct", summary.correct),
            (cohort, "outcome", "", "wrong", summary.wrong),
            (cohort, "outcome", "", "abstain", summary.abstain),
            (cohort, "outcome", "", "error", summary.error),
            (cohort, "outcome", "", "precision", summary.precision),
            (cohort, "outcome", "", "coverage", summary.coverage),
            (cohort, "outcome", "", "recall", summary.recall),
        ))
    for item in metrics.classes:
        measurements.extend((
            ("overall", item.axis, item.value, "support", item.support),
            ("overall", item.axis, item.value, "negative_support", item.negative_support),
            ("overall", item.axis, item.value, "predicted", item.predicted),
            ("overall", item.axis, item.value, "true_positive", item.true_positive),
            ("overall", item.axis, item.value, "false_positive", item.false_positive),
            ("overall", item.axis, item.value, "precision", item.precision),
            ("overall", item.axis, item.value, "coverage", item.coverage),
            ("overall", item.axis, item.value, "recall", item.recall),
            ("overall", item.axis, item.value, "fpr", item.fpr),
        ))
    frame = pd.DataFrame(measurements, columns=["cohort", "axis", "label", "metric", "value"])
    frame["value"] = frame["value"].astype("float64")
    return RulesMetricsTable.validate(frame)


def holdout_chart(metrics: HeldOutMetrics) -> str:
    lines = ["```text", "Outcomes by holdout cohort (bar length = cohort share)", ""]
    for name, summary in (
        ("Overall", metrics.overall),
        ("Uncontaminated", metrics.uncontaminated),
        ("Trained categories", metrics.trained),
        ("Unseen categories", metrics.unseen),
    ):
        lines.append(f"{name} (n={summary.support})")
        for label, count in (
            ("Correct", summary.correct), ("Wrong", summary.wrong),
            ("Abstain", summary.abstain), ("Error", summary.error),
        ):
            bar = "█" * max(1, round(count / summary.support * 30)) if count else ""
            lines.append(f"  {label:<8} {count:>6}  {bar}")
        lines.append("")
    return "\n".join([*lines, "```"])


def load_capture_attempt(
    context: AssetExecutionContext, settings: RulesCaptureSettings, source_run: str,
) -> CaptureResult:
    """Admit only the final failed attempt of the explicitly pinned capture run."""
    manager = context.resources.config_run_io_manager
    if not isinstance(manager, ConfigRunIOManager):
        raise IntegrityError("Capture resume requires private run storage")
    try:
        run_id = str(UUID(source_run))
    except ValueError:
        raise IntegrityError("Capture resume requires a Dagster run UUID") from None
    instance = context.instance
    partition = context.partition_key
    source = instance.get_run_by_id(run_id)
    if (
        source is None or source.status != DagsterRunStatus.FAILURE
        or source.job_name != "capture_rules" or source.tags.get(PARTITION_NAME_TAG) != partition
    ):
        raise IntegrityError("Capture resume source is not a failed capture_rules run for this partition")
    step = context.op_execution_context.op.name
    stats = instance.get_run_step_stats(run_id, step_keys=[step])
    if len(stats) != 1 or stats[0].status != StepEventStatus.FAILURE or not stats[0].attempts_list:
        raise IntegrityError("Capture resume source has no failed capture attempt")
    attempts = stats[0].attempts_list
    start, end = attempts[-1].start_time, attempts[-1].end_time
    if start is None or end is None:
        raise IntegrityError("Capture resume source has no bounded failed capture attempt")
    failures = [
        event.step_failure_data
        for record in instance.get_records_for_run(run_id, of_type=DagsterEventType.STEP_FAILURE).records
        if (entry := record.event_log_entry).step_key == step and start <= entry.timestamp <= end
        and (event := entry.dagster_event) is not None
    ]
    if len(failures) != 1:
        raise IntegrityError("Capture resume source has no unique failed capture event")
    mappings = [
        record
        for record in instance.get_records_for_run(run_id, of_type=DagsterEventType.ASSET_MATERIALIZATION).records
        if (asset := record.asset_materialization) is not None
        and asset.asset_key == mapping_key("rules_capture") and asset.partition == partition
        and record.event_log_entry.timestamp <= start
    ]
    if len(mappings) != 1 or not materialization_attempt_succeeded(
        instance, mappings[0], output_name="result", retry_number="native", manager_keys=frozenset({RULES_CONFIG_IO})
    ):
        raise IntegrityError("Capture resume source has no successful in-run capture settings")
    try:
        prior_settings = manager.materialized_scope(mappings[0], RulesCaptureSettings)
    except ValidationError:
        raise IntegrityError("Capture resume source settings are malformed") from None
    if prior_settings != settings or sha256(settings.agent.guide.encode()) != settings.pins.guide_sha256:
        raise IntegrityError("Capture resume source settings differ from the registered capture inputs")

    path = manager.run_output_path(run_id, context.asset_key, partition)
    name = f"{path.name}.attempt-{len(attempts) - 1}.evidence.json"
    try:
        payload = manager.read_private(path.with_name(name))
        if (failure := failures[0].user_failure_data) is not None:
            if failure.metadata.get(RULES_EVIDENCE_DIGEST) != TextMetadataValue(sha256(payload)):
                raise IntegrityError("Capture resume evidence differs from its failed attempt digest")
        else:
            error = failures[0].error
            origin = (error.cause or error) if error is not None else None
            if (
                origin is None or origin.cls_name != "IntegrityError"
                or not origin.message.startswith("codebench_pipeline.storage.IntegrityError: Category capture did not finish")
            ):
                raise IntegrityError("Capture resume source has no recognized legacy capture failure")
        result = CaptureResult.model_validate_json(payload, strict=True)
    except (OSError, ValidationError):
        raise IntegrityError("Capture resume evidence is missing, unreadable, or malformed") from None
    if (
        not result.batches or result.complete
        or result.corpus_digest != settings.pins.train.digest
        or result.runtime_version != settings.runtime.version
        or any(
            (batch.run.model, batch.run.backend_revision, batch.run.guide_sha256)
            != (settings.agent.model.model, settings.agent.backend_revision, settings.pins.guide_sha256)
            for batch in result.batches
        )
    ):
        raise IntegrityError("Capture resume evidence does not match the unfinished registered capture")
    return result


def record_rules_evidence(context: AssetExecutionContext, evidence: BaseModel) -> str:
    manager = context.resources.config_run_io_manager
    if not isinstance(manager, ConfigRunIOManager):
        raise IntegrityError("Rules evidence requires private run storage")
    path = manager.run_output_path(context.run.run_id, context.asset_key, context.partition_key)
    payload = evidence.model_dump_json().encode()
    manager.write_private(path.with_name(f"{path.name}.attempt-{context.retry_number}.evidence.json"), payload, exclusive=True)
    digest = sha256(payload)
    context.add_output_metadata({RULES_EVIDENCE_DIGEST: MetadataValue.text(digest)})
    return digest
