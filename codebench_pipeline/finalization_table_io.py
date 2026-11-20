"""Dagster hooks for annotation projection and finalization table caches."""

import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory

import pandas as pd
import pandera.pandas as pa
from dagster import (
    AssetKey,
    AssetObservation,
    DagsterEvent,
    DagsterEventType,
    HookContext,
    InitResourceContext,
    InputContext,
    IOManager,
    MetadataValue,
    OutputContext,
    failure_hook,
    io_manager,
)
from dagster._core.events import AssetObservationData
from pydantic import JsonValue

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.collection_config import CollectionMergeConfig
from codebench_pipeline.config_assets import ConfigRunIOManager, RegisteredConfig, require_registered_rules
from codebench_pipeline.dataframe_io import AssetSnapshot, UpstreamTableReference
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    FilteredCommandsTable,
    LabelingAttemptsTable,
    ProposedAnnotationsTable,
    SourceCatalogTable,
    SourceDocumentsTable,
)
from codebench_pipeline.dataset_branches import (
    BRANCH_BY_NAMESPACE,
    COLLECTION_BRANCHES,
    DECLARED_SOURCE_STAGES,
    asset_branch,
)
from codebench_pipeline.dataset_config import FilteringConfig
from codebench_pipeline.dataset_engine import (
    ReusedTable,
    TableCache,
    authorize_table_inputs,
    prepare_table_cache,
    record_table_output_metadata,
)
from codebench_pipeline.dataset_storage_io import RegisteredDatasetTableIO
from codebench_pipeline.documentation_authorization import SourceInventoryAuthorization, inventory_binding
from codebench_pipeline.finalization_cache_reuse import recover_admitted_finalization
from codebench_pipeline.finalization_publication import (
    FINALIZATION_PUBLICATION_METADATA,
    FinalizationPublicationV1,
    StepAttempt,
    TableOutputAdmission,
)
from codebench_pipeline.finalization_table_asset import (
    EVIDENCE_ATTRIBUTES,
    FINALIZATION_TABLE_IO,
    PROJECTION_SCHEMAS,
    CollectionCacheInput,
    CollectionInput,
    FilteringInput,
    FinalizationInput,
    LabelingInput,
    MergeInput,
    ProjectionInput,
)
from codebench_pipeline.object_store import safe_key
from codebench_pipeline.source_evidence import retain_source_receipt, validate_source_receipt
from codebench_pipeline.source_review_guard import authorize_inventory_input
from codebench_pipeline.stage_configuration import (
    CategoryEligibilityConfig,
    ConfigArtifact,
    DeclaredMergeConfig,
    DeduplicationConfig,
    FilterConfig,
    GuardedLabelingCacheConfig,
    GuardedLabelingConfig,
    PolicyArtifact,
)
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.typed_config import TypedConfig


@dataclass(slots=True)
class _FinalizationStep:
    key: AssetKey
    partition: str
    retry_number: int
    upstreams: list[UpstreamTableReference] = field(default_factory=list)
    loaded: (
        FinalizationInput
        | FilteringInput
        | LabelingInput
        | MergeInput
        | ProjectionInput
        | pd.DataFrame
        | None
    ) = None
    registered: RegisteredConfig | None = None
    mapped_config: ConfigArtifact | None = None
    documentation: pd.DataFrame | None = None
    cache: TableCache | None = None
    reused: ReusedTable[pa.DataFrameModel] | None = None
    attempt: str | None = None
    receipts: dict[str, JsonValue] | None = None
    collection_hit: bool = False
    inventory: SourceInventoryAuthorization | None = None
    scratch: TemporaryDirectory[str] | None = None

    @property
    def stage(self) -> str:
        return self.key.path[-1]

    @property
    def evidence_filename(self) -> str | None:
        attribute = EVIDENCE_ATTRIBUTES.get(self.stage)
        return None if attribute is None else f"{attribute.replace('_', '-')}.json"

    @property
    def output_schema(self) -> type[pa.DataFrameModel]:
        if self.stage == "labeling_attempts":
            return LabelingAttemptsTable
        if self.stage in DECLARED_SOURCE_STAGES:
            branch = BRANCH_BY_NAMESPACE[asset_branch(self.key.path)]
            if self.stage == branch.index:
                return SourceCatalogTable
            return SourceDocumentsTable if self.stage == branch.capture else CommandCandidatesTable
        return PROJECTION_SCHEMAS.get(self.stage, ProposedAnnotationsTable)


class FinalizationTableIO(IOManager):
    def __init__(self, tables: RegisteredDatasetTableIO, configuration: ConfigRunIOManager) -> None:
        self.tables = tables
        self.configuration = configuration
        self._scratch = TemporaryDirectory(prefix="dataset-steps-")
        self._scratch_root = Path(self._scratch.name)
        self.steps: dict[tuple[str, str], _FinalizationStep] = {}

    def load_input(
        self, context: InputContext
    ) -> (
        FinalizationInput
        | FilteringInput
        | LabelingInput
        | MergeInput
        | ProjectionInput
        | ConfigArtifact
        | CategoryRules
        | pd.DataFrame
    ):
        run = context.step_context
        keys = run.get_output_asset_keys()
        if len(keys) != 1 or (key := next(iter(keys))).path[
            -1
        ] not in EVIDENCE_ATTRIBUTES.keys() | PROJECTION_SCHEMAS.keys() | DECLARED_SOURCE_STAGES | {
            "labeling_attempts"
        }:
            return self.tables.load_input(context)

        self.tables.bind_configuration_producer(context)
        identity = (run.run_id, run.step.key)
        step = self.steps.get(identity)
        if step is None or step.retry_number != run.previous_attempt_count:
            step = _FinalizationStep(key, context.partition_key, run.previous_attempt_count)
            self.steps[identity] = step
        if step.stage in DECLARED_SOURCE_STAGES:
            return self._load_collection(context, step)
        if context.name == "mapped_config":
            result = self.configuration.load_scope(context)
            step.mapped_config = result
        elif step.stage == "merged_command_candidates":
            merge = self._load_merge(context, step)
            branch = asset_branch(context.asset_key.path)
            result = merge if branch == "dataset" else merge.frames[branch]
        else:
            snapshot = self.tables.load_versioned_input(context)
            step.upstreams.append(
                UpstreamTableReference(
                    asset_key=tuple(context.asset_key.path),
                    partition=context.asset_partition_key,
                    reference=snapshot.reference,
                )
            )
            value = snapshot.value
            if not isinstance(value, pd.DataFrame):
                raise IntegrityError("Finalization annotations input must be a table")
            if step.stage == "labeling_attempts":
                if context.name == "documentation_capture":
                    result = step.documentation = SourceDocumentsTable.validate(value)
                else:
                    result = step.loaded = LabelingInput(FilteredCommandsTable.validate(value))
            elif step.stage == "filtered_commands":
                result = step.loaded = FilteringInput(CommandCandidatesTable.validate(value))
            elif step.stage in PROJECTION_SCHEMAS:
                result = step.loaded = ProjectionInput(LabelingAttemptsTable.validate(value))
            else:
                result = step.loaded = FinalizationInput(ProposedAnnotationsTable.validate(value))
        self._prepare(context, step)
        return result

    def _load_collection(
        self, context: InputContext, step: _FinalizationStep
    ) -> CollectionInput | pd.DataFrame:
        if context.name == "mapped_config":
            result = CollectionInput.model_validate(
                self.configuration.load_scope(context), from_attributes=True
            )
            step.mapped_config = result
        else:
            snapshot = self.tables.load_versioned_input(context)
            if not isinstance(snapshot.value, pd.DataFrame):
                raise IntegrityError("Collection input must be a table")
            step.upstreams.append(
                UpstreamTableReference(
                    asset_key=tuple(context.asset_key.path),
                    partition=context.asset_partition_key,
                    reference=snapshot.reference,
                )
            )
            result = step.loaded = snapshot.value
        mapped = step.mapped_config
        if not isinstance(mapped, CollectionInput):
            return result
        branch = asset_branch(step.key.path)
        descriptor = BRANCH_BY_NAMESPACE[branch]
        first_stage = descriptor.declared_stages[0] == step.stage
        if not first_stage and not isinstance(step.loaded, pd.DataFrame):
            return result
        registered = self.tables.run_configuration(context.step_context.run_id, step.partition)
        step.registered = registered
        config = mapped.collection
        require_registered_rules(registered, mapped.category_rules)
        if config != registered.source_config(branch):
            raise IntegrityError("Mapped collection differs from the run configuration")
        inputs: dict[str, AssetSnapshot | CategoryRules]
        if isinstance(step.loaded, pd.DataFrame):
            name = "source_catalog" if step.stage == descriptor.capture else "source_documents"
            snapshot = AssetSnapshot(step.upstreams[0].reference, step.loaded)
            inputs = {name: snapshot}
            step.inventory = authorize_inventory_input(
                self.tables,
                context.instance,
                registered,
                branch,
                snapshot,
                inventory_input=name == "source_catalog" or branch == "documentation",
            )
        else:
            inputs = {"category_rules": mapped.category_rules}
        cache = step.cache = prepare_table_cache(
            self.tables, step.stage, step.partition, branch, config, inputs
        )
        cached = self.tables.read_cached_frame(cache.key)
        if cached is not None:
            validate_source_receipt(cached, config, self.tables)
            step.collection_hit = True
            mapped.bind_cache(CollectionCacheInput(cache.signature, cached, None))
        else:
            step.attempt = uuid.uuid4().hex
            step.scratch = TemporaryDirectory(prefix="dataset-inference-", dir=self._scratch_root)
            mapped.bind_cache(CollectionCacheInput(cache.signature, None, Path(step.scratch.name)))
        return result

    def _load_merge(self, context: InputContext, step: _FinalizationStep) -> MergeInput:
        loaded = step.loaded
        if not isinstance(loaded, MergeInput):
            loaded = step.loaded = MergeInput({}, {})
        branch = asset_branch(context.asset_key.path)
        snapshot = self.tables.load_versioned_input(context)
        if not isinstance(snapshot.value, pd.DataFrame):
            raise IntegrityError("Command candidates must be a table")
        loaded.frames[branch] = CommandCandidatesTable.validate(snapshot.value)
        loaded.binding[branch] = {"state": "configured", "digest": snapshot.reference.digest}
        step.upstreams.append(
            UpstreamTableReference(
                asset_key=tuple(context.asset_key.path),
                partition=context.asset_partition_key,
                reference=snapshot.reference,
            )
        )
        return loaded

    def _prepare(self, context: InputContext, step: _FinalizationStep) -> None:
        loaded, mapped = step.loaded, step.mapped_config
        if loaded is None or step.cache is not None:
            return
        if isinstance(loaded, MergeInput) and len(step.upstreams) != len(COLLECTION_BRANCHES):
            return
        run_id = context.step_context.run_id
        registered = self.tables.run_configuration(run_id, step.partition)
        config: TypedConfig | FilteringConfig | CollectionMergeConfig = TypedConfig()
        inputs: dict[str, pd.DataFrame | CategoryRules]
        match loaded:
            case FilteringInput():
                if not isinstance(mapped, FilterConfig):
                    return
                config = mapped.filtered_commands
                if config != registered.scopes.filtered_commands.filtered_commands:
                    raise IntegrityError("Mapped filter settings differ from the run configuration")
                inputs = {"command_candidates": loaded.candidates}
            case MergeInput():
                if not isinstance(mapped, DeclaredMergeConfig):
                    return
                if mapped != registered.scopes.merged_command_candidates:
                    raise IntegrityError("Mapped merge settings differ from the run configuration")
                config = mapped.merged_command_candidates
                inputs = {f"{branch}_candidates": frame for branch, frame in loaded.frames.items()}
            case LabelingInput():
                if not isinstance(mapped, GuardedLabelingConfig) or step.documentation is None:
                    return
                settings = registered.scopes.labeling_attempts
                if (
                    mapped.collection != settings.collection
                    or mapped.labeling_attempts != settings.labeling_attempts
                ):
                    raise IntegrityError("Mapped labeling settings differ from the run configuration")
                config = GuardedLabelingCacheConfig.from_settings(mapped)
                inputs = {"commands": loaded.commands, "documentation_capture": step.documentation}
            case ProjectionInput():
                if step.stage == "proposed_annotations" and not isinstance(mapped, PolicyArtifact):
                    return
                inputs = {"attempts": loaded.attempts}
            case FinalizationInput():
                if step.stage == "deduplicated_annotations":
                    if not isinstance(mapped, DeduplicationConfig):
                        return
                    if mapped != registered.scopes.deduplicated_annotations:
                        raise IntegrityError(
                            "Mapped deduplication settings differ from the run configuration"
                        )
                    config = mapped.deduplication
                elif step.stage == "category_eligible_annotations":
                    if not isinstance(mapped, CategoryEligibilityConfig):
                        return
                    if mapped.category_id != registered.rules.category_id:
                        raise IntegrityError("Mapped category differs from the run configuration")
                    config = mapped
                inputs = {"annotations": loaded.annotations}
            case pd.DataFrame():
                raise IntegrityError("Collection inputs are prepared by the collection input hook")
        if isinstance(mapped, PolicyArtifact):
            require_registered_rules(registered, mapped.category_rules)
            inputs["category_rules"] = mapped.category_rules

        authorize_table_inputs(
            self.tables,
            context.instance,
            step.stage,
            inputs,
            run_id=run_id,
            partition=step.partition,
            registered=registered,
        )
        cache = prepare_table_cache(self.tables, step.stage, step.partition, "dataset", config, inputs)
        step.registered = registered
        step.cache = cache
        if isinstance(loaded, LabelingInput):
            step.attempt = uuid.uuid4().hex
            loaded.attempt = (cache.signature, self._scratch_root)
            return
        loaded.signature = cache.signature
        step.reused = recover_admitted_finalization(
            context.instance,
            step.key,
            step.partition,
            self.tables,
            cache,
            upstreams=tuple(step.upstreams),
            registration_digest=registered.registration_digest,
            evidence_attribute=EVIDENCE_ATTRIBUTES.get(step.stage),
            output_schema=step.output_schema,
        )
        if step.reused is None:
            step.attempt = uuid.uuid4().hex
        elif isinstance(loaded, FinalizationInput):
            loaded.cached = ProposedAnnotationsTable.validate(step.reused.table)
        else:
            loaded.cached = step.reused.table

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        identity = (context.run_id, context.step_key)
        step = self.steps[identity]
        cache = step.cache
        registered = step.registered
        if cache is None or registered is None:
            raise IntegrityError("Finalization output has no cache plan")
        if isinstance(step.mapped_config, CollectionInput):
            self._record_collection(context, step, obj, cache, step.mapped_config)
            self.tables.handle_output(
                context,
                obj,
                admission=TableOutputAdmission(
                    self.tables.persist_value(obj), tuple(step.upstreams), registered.registration_digest
                ),
            )
            del self.steps[identity]
            return
        admission = TableOutputAdmission(
            self.tables.persist_value(obj) if step.reused is None else step.reused.reference,
            tuple(step.upstreams),
            registered.registration_digest,
        )
        self._record_result(context, step, obj, cache, admission)
        self.tables.handle_output(context, obj, admission=admission)
        del self.steps[identity]

    def _record_collection(
        self,
        context: OutputContext,
        step: _FinalizationStep,
        table: pd.DataFrame,
        cache: TableCache,
        mapped: CollectionInput,
    ) -> None:
        if not step.collection_hit:
            attempt = step.attempt
            if attempt is None:
                raise IntegrityError("Collection cache miss has no attempt")
            branch = asset_branch(step.key.path)
            try:
                if step.inventory is None:
                    table.attrs["codebench_source_inventory"] = {
                        "branch": branch,
                        "signature": cache.signature,
                    }
                else:
                    table.attrs[inventory_binding(branch)] = step.inventory.model_dump(mode="json")
                retain_source_receipt(table, mapped.collection, self.tables)
            finally:
                receipts = self._retain_scratch_attempt(step, cache, attempt)
                context.log_event(
                    AssetObservation(
                        asset_key=step.key,
                        partition=step.partition,
                        metadata={"inference_attempt": attempt, "receipts": MetadataValue.json(receipts)},
                    )
                )
            cache.retain_index(self.tables, {"attempt": attempt, "receipts": receipts})
            self.tables.write_cached_frame(cache.key, table)
        record_table_output_metadata(
            context, cache.metadata(self.tables, step.output_schema, hit=step.collection_hit)
        )

    def _retain_scratch_attempt(
        self, step: _FinalizationStep, cache: TableCache, attempt: str
    ) -> dict[str, JsonValue]:
        scratch = step.scratch
        labeling = step.loaded if isinstance(step.loaded, LabelingInput) else None
        address = (
            labeling.scratch if labeling is not None else Path(scratch.name) if scratch is not None else None
        )
        if address is None:
            step.receipts = cache.retain_attempt(self.tables, attempt, None)
            return step.receipts
        directory = self._scratch_root.joinpath(safe_key(address.name))
        if address != directory:
            raise IntegrityError("Receipt directory differs from its step root")
        try:
            step.receipts = cache.retain_attempt(self.tables, attempt, directory)
            return step.receipts
        finally:
            if scratch is not None:
                scratch.cleanup()
                step.scratch = None
            if labeling is not None:
                rmtree(directory)
                labeling.scratch = None

    def _record_result(
        self,
        context: OutputContext,
        step: _FinalizationStep,
        table: pd.DataFrame,
        cache: TableCache,
        admission: TableOutputAdmission,
    ) -> None:
        if step.reused is not None:
            record_table_output_metadata(
                context, cache.output_metadata(step.output_schema, hit=True, evidence=step.reused.evidence)
            )
            return
        attempt = step.attempt
        if attempt is None:
            raise IntegrityError("Finalization cache miss has no attempt")
        filename = step.evidence_filename
        if filename is None:
            step.receipts = self._retain_scratch_attempt(step, cache, attempt)
        else:
            with TemporaryDirectory(prefix="finalization-evidence-") as temporary:
                scratch = Path(temporary)
                evidence = table.attrs[EVIDENCE_ATTRIBUTES[step.stage]]
                (scratch / filename).write_text(json.dumps(evidence), encoding="utf-8")
                step.receipts = cache.retain_attempt(self.tables, attempt, scratch)
        context.log_event(
            AssetObservation(
                asset_key=step.key,
                partition=step.partition,
                metadata={"inference_attempt": attempt, "receipts": MetadataValue.json(step.receipts)},
            )
        )
        evidence, index_digest = cache.retain_index(
            self.tables, {"attempt": attempt, "receipts": step.receipts}, attempt=attempt
        )
        producer = FinalizationPublicationV1(
            input_signature=cache.signature,
            output=admission.output,
            artifact_attempt=attempt,
            receipt_index_digest=index_digest,
            step_attempt=StepAttempt(
                run_id=context.run_id, step_key=context.step_key, retry_number=step.retry_number
            ),
            upstreams=admission.upstreams,
            registration_digest=admission.registration_digest,
        )
        record_table_output_metadata(
            context,
            {
                **cache.output_metadata(step.output_schema, hit=False, evidence=evidence),
                FINALIZATION_PUBLICATION_METADATA: MetadataValue.json(producer.model_dump(mode="json")),
            },
        )

    def retain_failure(self, context: HookContext) -> None:
        step = self.steps.pop((context.run_id, context.step_key), None)
        if step is None or step.attempt is None or step.cache is None:
            return
        attempt = step.attempt
        receipts = step.receipts
        if receipts is None:
            receipts = self._retain_scratch_attempt(step, step.cache, attempt)
        diagnostic = self.tables.write_evidence(
            f"{step.cache.key}/{attempt}/failure.json",
            json.dumps({"kind": "failed_step", "attempt": attempt}).encode(),
        )
        observation = AssetObservation(
            asset_key=step.key,
            partition=step.partition,
            metadata={
                "inference_attempt": attempt,
                "receipts": MetadataValue.json(receipts),
                "failure_receipt": diagnostic,
            },
        )
        context.instance.report_dagster_event(
            DagsterEvent(
                DagsterEventType.ASSET_OBSERVATION.value,
                context.job_name,
                step_key=context.step_key,
                event_specific_data=AssetObservationData(observation),
            ),
            context.run_id,
        )


@io_manager(required_resource_keys={"dataset_io_manager", "config_run_io_manager"})
def finalization_table_io(context: InitResourceContext) -> IOManager:
    return FinalizationTableIO(context.resources.dataset_io_manager, context.resources.config_run_io_manager)


@failure_hook(required_resource_keys={FINALIZATION_TABLE_IO})
def retain_failed_finalization(context: HookContext) -> None:
    manager: FinalizationTableIO = context.resources.finalization_table_io_manager
    manager.retain_failure(context)
