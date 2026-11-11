from pathlib import Path

import pandera.pandas as pa
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_schemas import (
    CommandsTable,
    DocumentationTable,
    FilteredCommandsTable,
    ProposedAnnotationsTable,
    ReviewedAnnotationsTable,
    ReviewQueueTable,
    frame_records,
    model_records,
    validated_frame,
)
from codebench_pipeline.dataset_annotation import annotate, review
from codebench_pipeline.dataset_config import (
    AnnotationConfig,
    DocumentationConfig,
    PublicationConfig,
    ReviewConfig,
)
from codebench_pipeline.dataset_export import Publication, prepare_dataset
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision, ReviewDecision
from codebench_pipeline.models import SemanticSnippet


def documentation_table(config: DocumentationConfig) -> DataFrame[DocumentationTable]:
    rows = [snippet.to_semantic_snippet().model_dump(mode="json") for snippet in config.snippets]
    return validated_frame(rows, DocumentationTable)


@pa.check_types
def proposed_annotations(
    commands: DataFrame[FilteredCommandsTable],
    documentation: DataFrame[DocumentationTable],
    rules: CategoryRules,
    config: AnnotationConfig,
    evidence: Path,
    *,
    cache: Path | None = None,
) -> DataFrame[ProposedAnnotationsTable]:
    candidates = model_records(commands.loc[commands["filter_decision"] == "eligible"], CandidateRecord)
    docs = model_records(documentation, SemanticSnippet)
    labels = annotate(
        candidates,
        docs,
        rules,
        config.model,
        evidence,
        cache=cache,
        maximum_context_characters=config.maximum_context_characters,
    )
    originals = {candidate.candidate_id: candidate for candidate in candidates}
    rows = [
        {
            **originals[label.candidate_id].model_dump(mode="json"),
            **label.model_dump(mode="json", by_alias=True),
        }
        for label in labels
    ]
    return validated_frame(rows, ProposedAnnotationsTable)


@pa.check_types
def reviewed_annotations(
    annotations: DataFrame[ProposedAnnotationsTable],
    documentation: DataFrame[DocumentationTable],
    rules: CategoryRules,
    config: ReviewConfig,
    evidence: Path,
    *,
    cache: Path | None = None,
) -> DataFrame[ReviewedAnnotationsTable]:
    candidates = model_records(annotations, CandidateRecord)
    labels = model_records(annotations, LabelDecision)
    docs = model_records(documentation, SemanticSnippet)
    row_docs = (
        TypeAdapter(dict[str, list[SemanticSnippet]]).validate_python(annotations.attrs["row_documentation"])
        if "row_documentation" in annotations.attrs
        else None
    )
    decisions = review(
        candidates,
        docs,
        labels,
        rules,
        config.model,
        evidence,
        cache=cache,
        batch_size=config.batch_size,
        maximum_context_characters=config.maximum_context_characters,
        documentation_by_candidate=row_docs,
    )
    verdicts = {decision.candidate_id: decision for decision in decisions}
    rows: list[dict[str, JsonValue]] = []
    for row in frame_records(annotations):
        decision = verdicts[str(row["candidate_id"])]
        rows.append(
            {
                **row,
                "review_verdict": decision.verdict,
                "review_rationale": decision.rationale,
                "review_confidence": decision.confidence,
            }
        )
    result = validated_frame(rows, ReviewedAnnotationsTable)
    result.attrs = dict(annotations.attrs)
    return result


@pa.check_types
def publication(
    reviewed: DataFrame[ReviewedAnnotationsTable],
    rules: CategoryRules,
    config: PublicationConfig,
    *,
    approved_ids: frozenset[str] | None = None,
) -> Publication:
    candidates = model_records(reviewed, CandidateRecord)
    labels = model_records(reviewed, LabelDecision)
    reviews = [
        ReviewDecision.model_validate(
            {
                "candidate_id": row["candidate_id"],
                "verdict": row["review_verdict"],
                "rationale": row["review_rationale"],
                "confidence": row["review_confidence"],
            }
        )
        for row in frame_records(reviewed)
    ]
    return prepare_dataset(
        candidates, labels, reviews, rules, config.limit_per_risk, approved_ids=approved_ids
    )


@pa.check_types
def released_commands(
    reviewed: DataFrame[ReviewedAnnotationsTable],
    rules: CategoryRules,
    config: PublicationConfig,
) -> DataFrame[CommandsTable]:
    prepared = publication(reviewed, rules, config)
    return validated_frame(
        [{key: value for key, value in row.items()} for row in prepared.commands], CommandsTable
    )


@pa.check_types
def review_queue(
    reviewed: DataFrame[ReviewedAnnotationsTable],
    rules: CategoryRules,
    config: PublicationConfig,
) -> DataFrame[ReviewQueueTable]:
    prepared = publication(reviewed, rules, config)
    eligible = set(prepared.eligible_ids)
    reasons = {row["candidate_id"]: row["exclusion_reason"] for row in prepared.pending}
    rows = [
        {
            **row,
            "exclusion_reason": ""
            if str(row["candidate_id"]) in eligible
            else reasons[str(row["candidate_id"])],
        }
        for row in frame_records(reviewed)
        if str(row["candidate_id"]) in prepared.human_review_ids
    ]
    frame = validated_frame(rows, ReviewQueueTable)
    frame.attrs = dict(reviewed.attrs)
    return frame
