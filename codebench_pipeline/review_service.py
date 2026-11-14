from collections.abc import Callable
from pathlib import Path
from typing import assert_never

import pandas as pd
from pydantic import JsonValue

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_branches import Branch, annotation_stage_id, automatic_stage_id
from codebench_pipeline.human_review import (
    CompletionEffect,
    CompletionToken,
    HumanReviewError,
    PublicationReference,
    ReviewSnapshot,
)
from codebench_pipeline.label_corrections import validate_review_edits
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.review_api_models import EvidenceResponse, EvidenceTarget, PublicationTarget
from codebench_pipeline.review_evidence import review_evidence
from codebench_pipeline.review_snapshots import ReviewSnapshots
from codebench_pipeline.review_stages import ReviewInput, annotation_stage
from codebench_pipeline.review_workflow import ReviewWorkflow


class AnnotationReviewSource:
    def __init__(self, snapshots: ReviewSnapshots) -> None:
        self.snapshots = snapshots

    def current(self, dataset_id: str) -> ReviewInput:
        columns = tuple(self.snapshots.data.specification(dataset_id).publication.review_columns)
        resolved = self.snapshots.capture(dataset_id, columns)
        reviewed = pd.DataFrame(resolved.reviewed, copy=False)
        uncertain = reviewed.loc[reviewed["candidate_id"].isin(resolved.snapshot.candidate_ids)].copy()
        return ReviewInput(
            resolved.snapshot,
            uncertain,
            "candidate_id",
            validation={"rules": resolved.rules.model_dump(mode="json")},
            evidence={"manifest": resolved.manifest.model_dump(mode="json")},
        )

    def evidence(self, snapshot: ReviewSnapshot, candidate_id: str) -> EvidenceResponse:
        return review_evidence(self.snapshots, snapshot, candidate_id)


class ReviewService(ReviewWorkflow):
    """Dataset-inclusion adapter; lifecycle and decisions live in ReviewWorkflow."""

    def __init__(
        self,
        home: Path,
        data: DatasetDataService,
        publish: Callable[[CompletionToken], PublicationReference | CompletionEffect],
        branch: Branch = "dataset",
    ) -> None:
        self.branch: Branch = branch
        self.snapshots = ReviewSnapshots(home, data, branch)

        def on_complete(token: CompletionToken) -> CompletionEffect:
            result = publish(token)
            match result:
                case PublicationReference():
                    return CompletionEffect(publication=result, run_id=result.dagster_run_id)
                case CompletionEffect():
                    return result
                case _:
                    assert_never(result)

        super().__init__(
            home, annotation_stage(), AnnotationReviewSource(self.snapshots), on_complete=on_complete
        )

    def validate_edits(
        self,
        rows: tuple[dict[str, JsonValue], ...],
        edits: dict[str, JsonValue],
        validation: dict[str, JsonValue],
    ) -> None:
        rules = CategoryRules.model_validate(validation["rules"])
        for row in rows:
            validate_review_edits(row, edits, rules)

    def evidence(self, target: EvidenceTarget) -> EvidenceResponse:
        stored = super().evidence(target)
        with self.store.reader() as tx:
            snapshot = tx.get_batch(target.batch_id).snapshot
        original = self.source.evidence(snapshot, target.candidate_id)
        return EvidenceResponse(fields={**original.fields, **stored.fields})

    def download(self, target: PublicationTarget) -> bytes:
        with self.store.reader() as tx:
            reference = tx.get_publication(target.publication_id)
            if (
                reference.authorization.dataset_id != target.dataset_id
                or reference.authorization.review_stage
                not in {"annotations", "automatic", "documentation_annotations", "documentation_automatic"}
            ):
                raise HumanReviewError("not_found", "Publication does not belong to this dataset and stage")
        manifest = self.snapshots.manifest(reference.authorization.snapshot_id)
        if manifest.dataset_id != target.dataset_id:
            raise HumanReviewError("conflict", "Publication manifest has a different dataset binding")
        if reference.authorization.review_stage not in {
            annotation_stage_id(manifest.branch),
            automatic_stage_id(manifest.branch),
        }:
            raise HumanReviewError("conflict", "Publication manifest has a different branch binding")
        return ObjectStore(manifest.storage).read(reference.csv_reference)
