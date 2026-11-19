from dataclasses import dataclass
from pathlib import Path
from typing import Final

from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.batch_labeling import validate_guarded_support
from codebench_pipeline.batch_labeling_ranked import QUOTA_DEFERRED, RankedLabeling
from codebench_pipeline.dataframe_schemas import (
    FilteredCommandsTable,
    LabelingAttemptsTable,
    model_records,
    validated_frame,
)
from codebench_pipeline.dataset_config import DatasetBuildSpec, require_guarded_annotation
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding import GuardedEvidence
from codebench_pipeline.grounding_inventory import SharedOperations, documentation_captures
from codebench_pipeline.grounding_sources import CandidateEvidence
from codebench_pipeline.keenable_policy import KeenableError
from codebench_pipeline.labeling_outcomes import LabelingAttempt, failed_attempt, validate_attempts
from codebench_pipeline.repository_receipts import canonical
from codebench_pipeline.storage import IntegrityError, fingerprint

SUPPORT_ATTR: Final = "labeling_support"
_HISTORICAL_SUPPORT_ATTR: Final = "v2_labeling_support"
_SUPPORT: Final = TypeAdapter(dict[str, str])


def labeling_supports(frame: DataFrame[LabelingAttemptsTable]) -> dict[str, str]:
    attr = SUPPORT_ATTR if SUPPORT_ATTR in frame.attrs else _HISTORICAL_SUPPORT_ATTR
    return _SUPPORT.validate_python(frame.attrs.get(attr))


@dataclass(frozen=True, slots=True)
class GuardedLabeling:
    home: Path
    spec: DatasetBuildSpec

    def admission(
        self, frame: DataFrame[FilteredCommandsTable] | DataFrame[LabelingAttemptsTable]
    ) -> GuardedEvidence:
        batch = self.spec.annotation.batch
        if batch is None:
            raise IntegrityError("Guarded labeling requires batch settings")
        sources = CandidateEvidence.from_frame(frame, self.home, self.spec)
        source_limited = sources.provider.declaration.source_scoped
        return GuardedEvidence(
            sources,
            batch.grounding,
            self.home / "label-grounding-v2" / self.spec.dataset_id,
            SharedOperations(
                sources,
                sources.provider.read_bodies(sources.lock),
                documentation_captures(sources.repositories["documentation"]),
            )
            if source_limited
            else None,
        )

    def attempts(
        self, commands: DataFrame[FilteredCommandsTable], evidence: Path
    ) -> DataFrame[LabelingAttemptsTable]:
        config = self.spec.annotation
        batch = require_guarded_annotation(config)
        guard = self.admission(commands)
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
        evidence.mkdir(parents=True, exist_ok=True)
        (evidence / "source-versions.json").write_bytes(canonical(observations))
        if failed:
            (evidence / "source-failures.json").write_bytes(
                TypeAdapter(list[LabelingAttempt]).dump_json(failed)
            )
        cache = self.home / "row-inference-v2" / self.spec.dataset_id
        weights = (
            dict(zip(eligible["candidate_id"], eligible["source_weight"], strict=True))
            if "source_weight" in eligible
            else {}
        )
        outcomes, quota = RankedLabeling(
            self.spec.rules,
            config.model,
            batch,
            evidence,
            cache,
            config.max_request_bytes,
            guard,
            workers=config.workers,
        ).run(grounded, weights, failed)
        validate_attempts(candidates, outcomes, self.spec.rules)
        frame = validated_frame(
            [
                {
                    **attempt.model_dump(mode="json"),
                    "source_weight": weights[attempt.candidate_id],
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
                attempt.candidate_id: (evidence / attempt.attempt_evidence_reference)
                .with_name("verified.json")
                .read_text()
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

    def verify(
        self, frame: DataFrame[LabelingAttemptsTable], *, selected: frozenset[str] | None = None
    ) -> None:
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
