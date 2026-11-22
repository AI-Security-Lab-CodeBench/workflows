from dataclasses import dataclass
from pathlib import Path
from typing import Final

from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.batch_labeling import (
    BatchExecution,
    LabelingGeneration,
    prepare_batch_rows,
    run_prepared_batches,
)
from codebench_pipeline.batch_labeling_config import BatchLabelingConfig
from codebench_pipeline.batch_labeling_protocol import LiveSupport
from codebench_pipeline.batch_labeling_ranked import QUOTA_DEFERRED, RankedLabeling
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
from codebench_pipeline.grounding_tools import GroundingConfig, GroundingOrigins
from codebench_pipeline.keenable_receipts import CaptureLock
from codebench_pipeline.labeling_checkpoint_store import LabelingCheckpointCache
from codebench_pipeline.labeling_outcomes import LabelingAttempt, validate_attempts
from codebench_pipeline.labeling_stage_settings import LabelingSources
from codebench_pipeline.repository_config import SourceWeight
from codebench_pipeline.repository_receipts import RepositoryRun
from codebench_pipeline.stage_configuration import GuardedLabelingConfig
from codebench_pipeline.storage import IntegrityError

SUPPORT_ATTR: Final = "labeling_support"
_HISTORICAL_SUPPORT_ATTR: Final = "v2_labeling_support"
_SUPPORT: Final = TypeAdapter(dict[str, str])


def labeling_supports(frame: DataFrame[LabelingAttemptsTable]) -> dict[str, str]:
    attr = SUPPORT_ATTR if SUPPORT_ATTR in frame.attrs else _HISTORICAL_SUPPORT_ATTR
    return _SUPPORT.validate_python(frame.attrs.get(attr))


def verify_live_support(
    frame: DataFrame[LabelingAttemptsTable],
    grounding: GroundingOrigins,
    *,
    selected: frozenset[str] | None = None,
) -> None:
    supports = labeling_supports(frame)
    successes = {
        item.candidate_id: item for item in model_records(frame, LabelingAttempt) if item.status == "success"
    }
    if supports.keys() != successes.keys() or (selected is not None and not selected <= successes.keys()):
        raise IntegrityError("Labeling attempts do not have exact success-to-support coverage")
    for identity in successes.keys() if selected is None else selected:
        attempt = successes[identity]
        LiveSupport.model_validate_json(supports[identity]).verify(attempt, attempt, grounding)


@dataclass(frozen=True, slots=True)
class LabelingEvidenceInputs:
    """Read historical capture-grounding inputs; not part of live labeling."""

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


@dataclass(frozen=True, slots=True)
class GuardedLabeling:
    mapped: GuardedLabelingConfig
    batch: BatchLabelingConfig
    execution: BatchExecution

    def attempts(
        self,
        commands: DataFrame[FilteredCommandsTable],
        evidence: Path,
        generation: LabelingGeneration | None = None,
    ) -> DataFrame[LabelingAttemptsTable]:
        eligible = commands.loc[commands["filter_decision"] == "eligible"]
        candidates = model_records(eligible, CandidateRecord)
        if candidates and "source_weight" not in eligible:
            raise IntegrityError("Labeling requires source trust for eligible candidates")
        weights = (
            TypeAdapter(dict[str, SourceWeight]).validate_python(
                dict(zip(eligible["candidate_id"], eligible["source_weight"], strict=True))
            )
            if "source_weight" in eligible
            else {}
        )
        settings = self.mapped.labeling_attempts
        rules = self.mapped.rules
        ranked = RankedLabeling(
            rules,
            settings.model,
            self.batch,
            evidence,
            settings.max_request_bytes,
            execution=self.execution,
        )

        def run_packet(packet: list[CandidateRecord], path: Path) -> list[LabelingAttempt]:
            prepared = prepare_batch_rows(
                packet,
                rules,
                settings.model,
                self.batch,
                implementation=generation.identity if generation is not None else "live-grounding",
            )
            return run_prepared_batches(
                packet,
                prepared,
                rules,
                settings.model,
                self.batch,
                path,
                self.execution,
                settings.max_request_bytes,
                generation,
            )

        outcomes, quota = ranked.run_packets(candidates, weights, [], run_packet)
        validate_attempts(candidates, outcomes, rules)
        records: list[dict[str, JsonValue]] = [
            {**attempt.model_dump(mode="json"), "source_weight": weights[attempt.candidate_id]}
            for attempt in outcomes
        ]
        frame = validated_frame(records, LabelingAttemptsTable)
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
                "eligible": len(candidates),
                "success": sum(attempt.status == "success" for attempt in outcomes),
                "failure": sum(attempt.status == "failure" for attempt in outcomes) - deferred,
                "deferred": deferred,
            },
        }
        return frame

    def attempts_for_generation(
        self,
        commands: DataFrame[FilteredCommandsTable],
        evidence: Path,
        checkpoints: LabelingCheckpointCache,
        identity: str,
    ) -> DataFrame[LabelingAttemptsTable]:
        return self.attempts(commands, evidence, LabelingGeneration(checkpoints, identity))
