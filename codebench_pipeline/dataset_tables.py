from collections.abc import Sequence
from pathlib import Path

import pandera.pandas as pa
from pandera.typing import DataFrame

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_schemas import (
    CommandsTable,
    DocumentationTable,
    FilteredCommandsTable,
    ProposedAnnotationsTable,
    ReviewQueueTable,
    model_records,
    validated_frame,
)
from codebench_pipeline.dataset_annotation import annotate, relevant_documentation
from codebench_pipeline.dataset_config import (
    AnnotationConfig,
    DocumentationConfig,
    PublicationConfig,
)
from codebench_pipeline.dataset_export import Publication, prepare_dataset
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision, validate_label
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.models import SemanticSnippet
from codebench_pipeline.review_policy import requires_human_review


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
        maximum_context_characters=config.max_request_bytes,
        workers=config.workers,
    )
    originals = {candidate.candidate_id: candidate for candidate in candidates}
    rows = [
        {
            **originals[label.candidate_id].model_dump(mode="json"),
            **label.model_dump(mode="json", by_alias=True),
        }
        for label in labels
    ]
    frame = validated_frame(rows, ProposedAnnotationsTable)
    frame.attrs = {
        **commands.attrs,
        "row_documentation": {
            candidate.candidate_id: [
                doc.model_dump(mode="json") for doc in relevant_documentation(candidate, docs)
            ]
            for candidate in candidates
        },
    }
    return frame


@pa.check_types
def publication(
    annotations: DataFrame[ProposedAnnotationsTable],
    rules: CategoryRules,
    config: PublicationConfig,
    *,
    approved_ids: frozenset[str] | None = None,
) -> Publication:
    candidates = model_records(annotations, CandidateRecord)
    labels = model_records(annotations, LabelDecision)
    return prepare_dataset(candidates, labels, rules, config.limit_per_risk, approved_ids=approved_ids)


@pa.check_types
def released_commands(
    annotations: DataFrame[ProposedAnnotationsTable],
    rules: CategoryRules,
    config: PublicationConfig,
) -> DataFrame[CommandsTable]:
    prepared = publication(annotations, rules, config)
    return validated_frame(
        [{key: value for key, value in row.items()} for row in prepared.commands], CommandsTable
    )


@pa.check_types
def review_queue(
    annotations: DataFrame[ProposedAnnotationsTable],
    rules: CategoryRules,
    review_columns: Sequence[str],
) -> DataFrame[ReviewQueueTable]:
    if not set(review_columns).issubset(annotations.columns):
        raise HumanReviewError("ineligible", "A review column is absent from the input table")
    reasons: dict[str, str] = {}
    for candidate, label in zip(
        model_records(annotations, CandidateRecord), model_records(annotations, LabelDecision), strict=True
    ):
        validate_label(candidate, label, rules)
        if requires_human_review(label):
            reasons[candidate.candidate_id] = (
                f"label={label.decision}"
                if label.decision == "reject" or not label.documentation_urls
                else ""
            )
    queue = annotations.loc[annotations["candidate_id"].isin(reasons)].reset_index(drop=True)
    queue["exclusion_reason"] = queue["candidate_id"].map(reasons)
    return ReviewQueueTable.validate(queue)
