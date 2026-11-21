from dataclasses import dataclass
from pathlib import Path
from typing import Final

from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.batch_labeling import (
    BatchExecution,
    GuardedSupport,
    LabelingGeneration,
    prepare_batch_rows,
    run_prepared_batches,
    validate_guarded_support,
)
from codebench_pipeline.batch_labeling_config import BatchLabelingConfig
from codebench_pipeline.batch_labeling_ranked import QUOTA_DEFERRED, RankedLabeling
from codebench_pipeline.batch_labeling_rows import BatchRow
from codebench_pipeline.dataframe_schemas import (
    FilteredCommandsTable,
    LabelingAttemptsTable,
    SourceDocumentsTable,
    model_records,
    validated_frame,
)
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding import GuardedEvidence, OperationReceiptBuffer
from codebench_pipeline.grounding_inventory import SharedOperations, documentation_captures
from codebench_pipeline.grounding_sources import CandidateEvidence
from codebench_pipeline.grounding_tools import GroundingConfig
from codebench_pipeline.keenable_policy import KeenableError
from codebench_pipeline.keenable_receipts import CaptureLock
from codebench_pipeline.labeling_checkpoint_store import LabelingCheckpointCache
from codebench_pipeline.labeling_outcomes import LabelingAttempt, failed_attempt, validate_attempts
from codebench_pipeline.labeling_stage_settings import LabelingSources
from codebench_pipeline.repository_config import SourceWeight
from codebench_pipeline.repository_receipts import RepositoryRun, canonical, publish_bytes
from codebench_pipeline.stage_configuration import GuardedLabelingConfig
from codebench_pipeline.storage import IntegrityError, fingerprint

SUPPORT_ATTR: Final = "labeling_support"
_HISTORICAL_SUPPORT_ATTR: Final = "v2_labeling_support"
_SUPPORT: Final = TypeAdapter(dict[str, str])


def labeling_supports(frame: DataFrame[LabelingAttemptsTable]) -> dict[str, str]:
    attr = SUPPORT_ATTR if SUPPORT_ATTR in frame.attrs else _HISTORICAL_SUPPORT_ATTR
    return _SUPPORT.validate_python(frame.attrs.get(attr))


@dataclass(frozen=True, slots=True)
class LabelingEvidenceInputs:
    run: RepositoryRun
    collection: LabelingSources
    grounding: GroundingConfig
    documentation_capture: DataFrame[SourceDocumentsTable]

    def admission(
        self,
        frame: DataFrame[FilteredCommandsTable] | DataFrame[LabelingAttemptsTable],
        *,
        operation_receipts: OperationReceiptBuffer | None = None,
    ) -> GuardedEvidence:
        run = self.run
        sources = CandidateEvidence.from_frame(frame, run, self.collection)
        return GuardedEvidence(
            sources,
            self.grounding,
            run.dataset_home / "label-grounding-v2" / run.dataset_id,
            SharedOperations(
                sources,
                sources.provider.read_bodies(sources.lock)
                if sources.provider is not None and isinstance(sources.lock, CaptureLock)
                else {},
                documentation_captures(
                    sources.repositories["documentation"], self.documentation_capture, run
                ),
            )
            if sources.source_scoped
            else None,
            operation_receipts,
        )

    def verify(
        self, frame: DataFrame[LabelingAttemptsTable], *, selected: frozenset[str] | None = None
    ) -> GuardedEvidence:
        guard = self.admission(frame)
        supports = labeling_supports(frame)
        successes = {
            item.candidate_id: item
            for item in model_records(frame, LabelingAttempt)
            if item.status == "success"
        }
        if supports.keys() != successes.keys() or (selected is not None and not selected <= successes.keys()):
            raise IntegrityError("Guarded attempts do not have exact success-to-support coverage")
        for identity in successes.keys() if selected is None else selected:
            attempt = successes[identity]
            validate_guarded_support(guard, attempt, attempt, supports[identity].encode())
        return guard


@dataclass(frozen=True, slots=True)
class LabelingSelection:
    candidates: list[CandidateRecord]
    grounded: list[CandidateRecord]
    failed: list[LabelingAttempt]
    weights: dict[str, SourceWeight]
    observations: dict[str, JsonValue]

    def write_scratch(self, evidence: Path) -> None:
        evidence.mkdir(parents=True, exist_ok=True)
        (evidence / "source-versions.json").write_bytes(canonical(self.observations))
        if self.failed:
            (evidence / "source-failures.json").write_bytes(
                TypeAdapter(list[LabelingAttempt]).dump_json(self.failed)
            )


@dataclass(frozen=True, slots=True)
class GuardedLabeling:
    mapped: GuardedLabelingConfig
    evidence_inputs: LabelingEvidenceInputs
    batch: BatchLabelingConfig
    execution: BatchExecution

    def selection(
        self, commands: DataFrame[FilteredCommandsTable], guard: GuardedEvidence
    ) -> LabelingSelection:
        eligible = commands.loc[commands["filter_decision"] == "eligible"]
        candidates = model_records(eligible, CandidateRecord)
        if candidates and "source_weight" not in eligible:
            raise IntegrityError("Guarded labeling requires source trust for eligible candidates")
        grounded: list[CandidateRecord] = []
        failed: list[LabelingAttempt] = []
        observations: dict[str, JsonValue] = {}
        for candidate in candidates:
            try:
                capture = guard.sources.resolve(candidate)
                observations[candidate.candidate_id] = {
                    "source_binding": capture.source_binding,
                    "source_versions": None if capture.versions is None else list(capture.versions),
                }
                if guard.source_limited:
                    grounded.append(candidate)
                else:
                    failed.append(
                        failed_attempt(
                            candidate,
                            "grounding_migration_required",
                            "This declaration can verify historical labels but needs source-level authorization for new labels.",
                            [],
                            fingerprint(candidate.model_dump(mode="json")),
                            "source-failures.json",
                        )
                    )
            except (IntegrityError, KeenableError, OSError, UnicodeError, KeyError):
                failed.append(
                    failed_attempt(
                        candidate,
                        "candidate_capture_unverified",
                        "The candidate's pinned capture, exact span, or current rights could not be verified.",
                        [],
                        fingerprint(candidate.model_dump(mode="json")),
                        "source-failures.json",
                    )
                )
        weights = (
            TypeAdapter(dict[str, SourceWeight]).validate_python(
                dict(zip(eligible["candidate_id"], eligible["source_weight"], strict=True))
            )
            if "source_weight" in eligible
            else {}
        )
        return LabelingSelection(candidates, grounded, failed, weights, observations)

    def ranked(self, evidence: Path, guard: GuardedEvidence) -> RankedLabeling:
        config = self.mapped.labeling_attempts
        run = self.evidence_inputs.run
        return RankedLabeling(
            self.mapped.rules,
            config.model,
            self.batch,
            evidence,
            run.dataset_home / "row-inference-v2" / run.dataset_id,
            config.max_request_bytes,
            guard,
            workers=config.workers,
            execution=self.execution,
        )

    def attempts(
        self, commands: DataFrame[FilteredCommandsTable], evidence: Path
    ) -> DataFrame[LabelingAttemptsTable]:
        receipts = OperationReceiptBuffer()
        guard = self.evidence_inputs.admission(commands, operation_receipts=receipts)
        selection = self.selection(commands, guard)
        selection.write_scratch(evidence)
        ranked = self.ranked(evidence, guard)
        outcomes, quota = ranked.run(selection.grounded, selection.weights, selection.failed)
        if receipts.receipts:
            for key, payload in receipts.receipts.items():
                publish_bytes(guard.root / key, payload)
        return self.attempt_table(commands, selection, ranked, outcomes, quota)

    def attempts_for_generation(
        self,
        commands: DataFrame[FilteredCommandsTable],
        evidence: Path,
        checkpoints: LabelingCheckpointCache,
        identity: str,
    ) -> DataFrame[LabelingAttemptsTable]:
        receipts = OperationReceiptBuffer()
        guard = self.evidence_inputs.admission(commands, operation_receipts=receipts)
        selection = self.selection(commands, guard)
        selection.write_scratch(evidence)
        ranked = self.ranked(evidence, guard)
        rules = self.mapped.rules
        config = self.mapped.labeling_attempts
        prepared = prepare_batch_rows(
            selection.grounded, [], rules, config.model, self.batch, implementation=identity, admission=guard
        )

        def referenced(rows: list[BatchRow]) -> dict[str, bytes]:
            return receipts.referenced(
                item
                for row in rows
                if row.attempt.status == "success"
                for item in GuardedSupport.model_validate_json(row.files["verified.json"]).evidence.values()
            )

        generation = LabelingGeneration(
            checkpoints, identity, referenced, self.evidence_inputs.admission(commands)
        )

        def run_packet(candidates: list[CandidateRecord], path: Path) -> list[LabelingAttempt]:
            return run_prepared_batches(
                candidates,
                prepared,
                rules,
                config.model,
                self.batch,
                path,
                self.execution,
                config.max_request_bytes,
                guard,
                generation,
            )

        outcomes, quota = ranked.run_packets(
            selection.grounded, selection.weights, selection.failed, run_packet
        )
        return self.attempt_table(commands, selection, ranked, outcomes, quota)

    def attempt_table(
        self,
        commands: DataFrame[FilteredCommandsTable],
        selection: LabelingSelection,
        ranked: RankedLabeling,
        outcomes: list[LabelingAttempt],
        quota: dict[str, JsonValue],
    ) -> DataFrame[LabelingAttemptsTable]:
        validate_attempts(selection.candidates, outcomes, self.mapped.rules)
        frame = validated_frame(
            [
                {
                    **attempt.model_dump(mode="json"),
                    "source_weight": selection.weights[attempt.candidate_id],
                }
                for attempt in outcomes
            ],
            LabelingAttemptsTable,
        )
        deferred = sum(attempt.failure_code == QUOTA_DEFERRED for attempt in outcomes)
        frame.attrs = {
            **commands.attrs,
            "labeling_quota": quota,
            SUPPORT_ATTR: {
                attempt.candidate_id: ranked.verified_support(attempt)
                for attempt in outcomes
                if attempt.status == "success"
            },
            "labeling_coverage": {
                "eligible": len(selection.candidates),
                "success": sum(attempt.status == "success" for attempt in outcomes),
                "failure": sum(attempt.status == "failure" for attempt in outcomes) - deferred,
                "deferred": deferred,
            },
        }
        return frame
