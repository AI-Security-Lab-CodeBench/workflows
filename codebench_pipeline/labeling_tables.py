from pathlib import Path

from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.annotation_cache import invocation_version
from codebench_pipeline.batch_labeling import annotate_batches
from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_schemas import (
    DocumentationTable,
    FilteredCommandsTable,
    LabelingAttemptsTable,
    LabelingFailuresTable,
    ProposedAnnotationsTable,
    frame_records,
    model_records,
    validated_frame,
)
from codebench_pipeline.dataset_annotation import (
    annotation_requests,
    invocation_path,
    label_schema,
    relevant_documentation,
)
from codebench_pipeline.dataset_config import AnnotationConfig
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision
from codebench_pipeline.dataset_tables import proposed_annotations
from codebench_pipeline.labeling_outcomes import LabelingAttempt, label_attempt, validate_attempts
from codebench_pipeline.models import SemanticSnippet


def labeling_attempts_table(
    commands: DataFrame[FilteredCommandsTable],
    documentation: DataFrame[DocumentationTable],
    rules: CategoryRules,
    config: AnnotationConfig,
    evidence: Path,
    *,
    cache: Path | None = None,
) -> DataFrame[LabelingAttemptsTable]:
    candidates = model_records(commands.loc[commands["filter_decision"] == "eligible"], CandidateRecord)
    docs = model_records(documentation, SemanticSnippet)
    if config.batch is not None:
        attempts = annotate_batches(
            candidates,
            docs,
            rules,
            config.model,
            config.batch,
            evidence,
            cache=cache,
            maximum_context_characters=config.maximum_context_characters,
        )
    else:
        labeled = proposed_annotations(commands, documentation, rules, config, evidence, cache=cache)
        labels = {label.candidate_id: label for label in model_records(labeled, LabelDecision)}
        requests = {
            request.candidate_id: request
            for request in annotation_requests(candidates, docs, rules, config.model)
        }
        attempts = [
            label_attempt(
                candidate,
                labels[candidate.candidate_id],
                relevant_documentation(candidate, docs),
                invocation_version(
                    label_schema(candidate, rules), requests[candidate.candidate_id], config.model
                ),
                str(invocation_path(Path("."), "label", candidate.candidate_id) / "receipt.json"),
            )
            for candidate in candidates
        ]
    validate_attempts(candidates, attempts, rules)
    frame = validated_frame([attempt.model_dump(mode="json") for attempt in attempts], LabelingAttemptsTable)
    frame.attrs = {
        **commands.attrs,
        "labeling_coverage": {
            "eligible": len(candidates),
            "success": sum(attempt.status == "success" for attempt in attempts),
            "failure": sum(attempt.status == "failure" for attempt in attempts),
        },
    }
    return frame


def proposed_from_attempts(
    attempts: DataFrame[LabelingAttemptsTable],
    rules: CategoryRules,
) -> DataFrame[ProposedAnnotationsTable]:
    outcomes = model_records(attempts, LabelingAttempt)
    candidates = model_records(attempts, CandidateRecord)
    validate_attempts(candidates, outcomes, rules)
    rows: list[dict[str, JsonValue]] = []
    documentation: dict[str, JsonValue] = {}
    for attempt in outcomes:
        if attempt.status != "success":
            continue
        label = LabelDecision.model_validate_json(attempt.label_json)
        original = attempt.model_dump(mode="json", include=set(CandidateRecord.model_fields))
        rows.append({**original, **label.model_dump(mode="json", by_alias=True)})
        documentation[attempt.candidate_id] = [
            doc.model_dump(mode="json")
            for doc in TypeAdapter(list[SemanticSnippet]).validate_json(attempt.documentation_json)
        ]
    frame = validated_frame(rows, ProposedAnnotationsTable)
    frame.attrs = {**attempts.attrs, "row_documentation": documentation}
    return frame


def failures_from_attempts(attempts: DataFrame[LabelingAttemptsTable]) -> DataFrame[LabelingFailuresTable]:
    outcomes = model_records(attempts, LabelingAttempt)
    failed_ids = {outcome.candidate_id for outcome in outcomes if outcome.status == "failure"}
    frame = validated_frame(
        frame_records(attempts.loc[attempts["candidate_id"].isin(failed_ids)]), LabelingFailuresTable
    )
    frame.attrs = dict(attempts.attrs)
    return frame
