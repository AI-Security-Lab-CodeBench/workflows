from dataclasses import asdict, dataclass

import pandas as pd
from pandera.typing import DataFrame
from pydantic import JsonValue, RootModel, TypeAdapter

from codebench_pipeline.dataframe_io import frame_bytes
from codebench_pipeline.dataframe_schemas import (
    CommandsTable,
    LabelingAttemptsTable,
    SourceDocumentsTable,
    frame_records,
    model_records,
    validated_frame,
)
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.dataset_export import Publication, prepare_dataset
from codebench_pipeline.dataset_finalization import FINALIZATION_BINDING, normalized_command
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision
from codebench_pipeline.grounding_policy import command_tokens
from codebench_pipeline.grounding_tables import LabelingEvidenceInputs, labeling_supports
from codebench_pipeline.guarded_publication_sampling import BranchSample, sample_survivors
from codebench_pipeline.guarded_publication_selection import (
    REQUIRED_CONTRIBUTIONS,
    SELECTION_REVISION,
    Survivor,
    select_survivors,
)
from codebench_pipeline.human_review import CompletionToken, HumanReviewError, ReviewDecision
from codebench_pipeline.label_corrections import apply_label_corrections
from codebench_pipeline.labeling_outcomes import LabelingAttempt
from codebench_pipeline.repository_config import SourceWeight
from codebench_pipeline.repository_receipts import RepositoryRun, canonical, sha256
from codebench_pipeline.review_frames import REVIEW_BINDING
from codebench_pipeline.review_policy import requires_human_review
from codebench_pipeline.review_snapshots import ResolvedReview, ReviewSnapshots, release_configuration_digest
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig


@dataclass(frozen=True, slots=True)
class ReleaseInputs:
    snapshots: ReviewSnapshots
    resolved: ResolvedReview
    token: CompletionToken
    decisions: tuple[ReviewDecision, ...]

    def frame(self, stage: str) -> pd.DataFrame:
        return self.snapshots.frame(self.resolved.manifest, stage)


@dataclass(frozen=True, slots=True)
class ReleaseProposal:
    output: DataFrame[CommandsTable]
    publication: Publication
    manifest: dict[str, JsonValue]

    @property
    def body(self) -> bytes:
        return canonical(self.manifest)

    @property
    def digest(self) -> str:
        return sha256(self.body)

    def require_resolved_sampling(self) -> None:
        sampling = TypeAdapter(dict[str, BranchSample]).validate_python(self.manifest["sampling"])
        pending = [
            f"{branch}/{stratum}"
            for branch, sample in sorted(sampling.items())
            for stratum in ("semantics:inspection", "semantics:state_changing")
            if sample.strata[stratum].status in {"unclassified", "present_but_unsampled"}
        ]
        if pending:
            raise HumanReviewError(
                "pending", f"Resolve semantic coverage and resample the proposal: {', '.join(pending)}"
            )


def _identity_set(frame: pd.DataFrame) -> set[str]:
    identities = set(frame["candidate_id"])
    if len(identities) != len(frame):
        raise HumanReviewError("stale", "Release reconciliation found duplicate candidate identities")
    return identities


def reconciled_counts(inputs: ReleaseInputs) -> dict[str, JsonValue]:
    merged = inputs.frame("merged_command_candidates")
    filtered = inputs.frame("filtered_commands")
    attempts = inputs.frame("labeling_attempts")
    proposed = inputs.frame("proposed_annotations")
    failures = inputs.frame("labeling_failures")
    dedup = inputs.frame("deduplicated_annotations")
    eligible = inputs.resolved.reviewed
    duplicates = TypeAdapter(list[dict[str, JsonValue]]).validate_python(dedup.attrs.get("duplicates"))
    category_exclusions = TypeAdapter(list[dict[str, JsonValue]]).validate_python(
        eligible.attrs.get("category_exclusions")
    )
    success_ids, failure_ids = _identity_set(proposed), _identity_set(failures)
    duplicate_ids = {str(row["duplicate_id"]) for row in duplicates}
    excluded_ids = {str(row["candidate_id"]) for row in category_exclusions}
    attempts_ids = _identity_set(attempts)
    dedup_ids, eligible_ids = _identity_set(dedup), _identity_set(eligible)
    if (
        _identity_set(merged) != _identity_set(filtered)
        or _identity_set(filtered.loc[filtered["filter_decision"] == "eligible"]) != attempts_ids
        or attempts_ids != success_ids | failure_ids
        or success_ids & failure_ids
        or _identity_set(attempts.loc[attempts["status"] == "success"]) != success_ids
        or _identity_set(attempts.loc[attempts["status"] == "failure"]) != failure_ids
        or len(duplicate_ids) != len(duplicates)
        or not {str(row["survivor_id"]) for row in duplicates} <= dedup_ids
        or dedup_ids & duplicate_ids
        or success_ids != dedup_ids | duplicate_ids
        or len(excluded_ids) != len(category_exclusions)
        or eligible_ids & excluded_ids
        or dedup_ids != eligible_ids | excluded_ids
        or {row.candidate_id for row in inputs.decisions} != set(inputs.resolved.snapshot.candidate_ids)
    ):
        raise HumanReviewError(
            "stale", "Collection source, filter, labeling, deduplication or review counts do not reconcile"
        )
    deferred = int((failures["failure_code"] == "quota_deferred").sum())
    return {
        "candidates": len(merged),
        "eligible_to_label": len(attempts),
        "filter_decisions": {str(k): int(v) for k, v in filtered["filter_decision"].value_counts().items()},
        "supported": len(proposed),
        "unsupported": len(failures) - deferred,
        "quota_deferred": deferred,
        "exact_duplicates": len(duplicates),
        "category_excluded": len(category_exclusions),
        "category_eligible": len(eligible),
        "reviewed": len(inputs.decisions),
    }


def prepare_guarded_release(
    inputs: ReleaseInputs,
    run: RepositoryRun,
    settings: DeclaredGlobalConfig | DatasetBuildSpec,
    *,
    publication_versions: dict[str, str],
) -> ReleaseProposal:
    registered = (
        DeclaredGlobalConfig.from_spec(settings) if isinstance(settings, DatasetBuildSpec) else settings
    )
    if run.dataset_id != registered.dataset_id:
        raise HumanReviewError("stale", "Release evidence belongs to another dataset")
    resolved = inputs.resolved
    publication = resolved.manifest.publication
    if registered.scopes.human_review_queue.human_review_queue != publication:
        raise HumanReviewError("stale", "Registered release policy differs from the completed snapshot")
    rules = resolved.rules
    counts = reconciled_counts(inputs)
    corrected = apply_label_corrections(resolved.reviewed, inputs.decisions, rules)
    originals = {
        candidate.candidate_id: candidate for candidate in model_records(resolved.reviewed, CandidateRecord)
    }
    candidates = {
        candidate.candidate_id: candidate for candidate in model_records(corrected, CandidateRecord)
    }
    if any(candidate.command != originals[identity].command for identity, candidate in candidates.items()):
        raise HumanReviewError("pending", "Edited commands require newly verified literal provenance")
    labels = {label.candidate_id: label for label in model_records(corrected, LabelDecision)}
    approved = frozenset(
        decision.candidate_id for decision in inputs.decisions if decision.decision == "approve"
    )
    attempts = LabelingAttemptsTable.validate(inputs.frame("labeling_attempts"))
    labeling_config = registered.scopes.labeling_attempts
    labeling = LabelingEvidenceInputs(
        run,
        labeling_config.collection,
        labeling_config.labeling_attempts.batch.grounding,
        SourceDocumentsTable.validate(inputs.frame("documentation/capture_documentation")),
    )
    admission = labeling.admission(attempts)
    baseline: set[str] = set()
    collection = admission.sources
    if "source_weight" not in resolved.reviewed:
        for candidate in model_records(inputs.frame("merged_command_candidates"), CandidateRecord):
            branch, _, raw_id = candidate.candidate_id.partition(":")
            if (
                branch != "keenable"
                and collection.repositories[branch].rows[raw_id].origin_id not in REQUIRED_CONTRIBUTIONS
            ):
                baseline.add(normalized_command(candidate.command))
    survivors: list[Survivor] = []
    contributions: dict[str, str] = {}
    source_bindings: dict[str, dict[str, JsonValue]] = {}
    reviewed_weights = (
        resolved.reviewed.set_index("candidate_id")["source_weight"]
        if "source_weight" in resolved.reviewed
        else None
    )
    source_weights: dict[str, float] | None = {} if reviewed_weights is not None else None
    for identity, candidate in sorted(candidates.items()):
        label = labels[identity]
        uncertain = requires_human_review(label)
        if (
            label.decision == "reject"
            or not label.documentation_urls
            or (uncertain and identity not in approved)
        ):
            continue
        source = collection.resolve(originals[identity])
        source_bindings[identity] = {
            "origin_id": source.origin_id,
            "capture_sha256": source.capture_sha256,
            "rights_and_source_binding": source.source_binding,
        }
        if source_weights is not None and reviewed_weights is not None:
            weight = RootModel[SourceWeight].model_validate(reviewed_weights.at[identity]).root
            source_bindings[identity]["source_weight"] = weight
            source_weights[identity] = weight
        branch = identity.partition(":")[0]
        command = normalized_command(candidate.command)
        contribution = "keenable" if branch == "keenable" else source.origin_id
        if source_weights is None and contribution in REQUIRED_CONTRIBUTIONS and command not in baseline:
            contribution = contributions.setdefault(command, contribution)
            if contribution != ("keenable" if branch == "keenable" else source.origin_id):
                contribution = ""
        else:
            contribution = ""
        survivors.append(
            Survivor(
                identity,
                branch,
                source.origin_id,
                label.risk_id,
                " ".join(command_tokens(candidate)[:3]),
                label.class_,
                uncertain,
                contribution,
            )
        )
    cap = publication.limit_per_risk
    selected = select_survivors(survivors, {risk.id: cap for risk in rules.risks}, source_weights)
    ordered = selected.ordered
    selected_ids = frozenset(ordered)
    labeling.verify(attempts, selected=selected_ids)
    selected_attempts = {
        attempt.candidate_id: attempt
        for attempt in model_records(attempts.loc[attempts["candidate_id"].isin(ordered)], LabelingAttempt)
    }
    if any(
        (selected_attempts[identity].command, selected_attempts[identity].util)
        != (candidates[identity].command, candidates[identity].util)
        for identity in ordered
    ):
        raise HumanReviewError("stale", "Selected commands differ from their verified support")
    identities = (*ordered, *sorted(candidates.keys() - selected_ids))
    published = prepare_dataset(
        [candidates[identity] for identity in identities],
        [labels[identity] for identity in identities],
        rules,
        cap,
        approved_ids=approved,
        selected_ids=selected_ids,
    )
    output = validated_frame(
        [{key: value for key, value in row.items()} for row in published.commands], CommandsTable
    )
    output.attrs[REVIEW_BINDING] = inputs.token.model_dump(mode="json")
    if len(output) != len(ordered):
        raise HumanReviewError("stale", "Fair selection differs from final machine admission")
    sampling = sample_survivors([row for row in survivors if row.identity in selected_ids], selected_attempts)
    sampled_ids = {identity for sample in sampling.values() for identity in sample.sampled_row_ids}
    support = labeling_supports(attempts)
    coverage = {
        **counts,
        **selected.coverage,
        "review_approved": len(approved),
        "review_excluded": len(inputs.decisions) - len(approved),
    }
    csv = published.files["commands.csv"].encode()
    manifest = TypeAdapter(dict[str, JsonValue]).validate_python(
        {
            "partition": registered.dataset_id,
            "release_configuration": release_configuration_digest(registered),
            "review_snapshot": resolved.snapshot.snapshot_id,
            "review_completion": inputs.token.model_dump(mode="json"),
            "implementation": {
                **resolved.manifest.implementation,
                **publication_versions,
            },
            "selection_revision": "source-trust-cap-1" if source_weights is not None else SELECTION_REVISION,
            "branch_terminals": resolved.reviewed.attrs[FINALIZATION_BINDING],
            "branch_capture_locks": {
                **{
                    branch: binding.receipt.upstream[0] for branch, binding in collection.repositories.items()
                },
                "keenable": sha256(collection.lock.model_dump_json().encode()),
            },
            "labeling_attempts": resolved.manifest.references["labeling_attempts"].digest,
            "selected": [
                {
                    "id": identity,
                    "candidate": candidates[identity].model_dump(mode="json"),
                    "label": labels[identity].model_dump(mode="json", by_alias=True),
                    "source_binding": source_bindings[identity],
                    "support_sha256": sha256(support[identity].encode()),
                }
                for identity in ordered
            ],
            "schema": list(output.columns),
            "table_sha256": sha256(canonical(frame_records(output))),
            "parquet_sha256": sha256(frame_bytes(output)),
            "csv_sha256": sha256(csv),
            "coverage": coverage,
            "sampled_row_ids": [identity for identity in ordered if identity in sampled_ids],
            "sampling": {branch: asdict(sample) for branch, sample in sampling.items()},
        }
    )
    return ReleaseProposal(output, published, manifest)
