from dataclasses import asdict, dataclass

from pandera.typing import DataFrame
from pydantic import JsonValue, RootModel, TypeAdapter

from codebench_pipeline.dataframe_io import frame_bytes
from codebench_pipeline.dataframe_schemas import (
    CommandsTable,
    frame_records,
    model_records,
    validated_frame,
)
from codebench_pipeline.dataset_export import Publication, prepare_dataset
from codebench_pipeline.dataset_finalization import FINALIZATION_BINDING, normalized_command
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision
from codebench_pipeline.grounding_policy import command_tokens
from codebench_pipeline.grounding_tables import labeling_supports
from codebench_pipeline.guarded_publication_evidence import (
    ReleaseAuthorization,
    ReleaseExecution,
    ReleaseInputs,
)
from codebench_pipeline.guarded_publication_sampling import BranchSample, sample_survivors
from codebench_pipeline.guarded_publication_selection import (
    REQUIRED_CONTRIBUTIONS,
    SELECTION_REVISION,
    Survivor,
    select_survivors,
)
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.label_corrections import apply_label_corrections
from codebench_pipeline.labeling_outcomes import LabelingAttempt
from codebench_pipeline.release_stage_settings import DatasetReleaseConfig, PublicationTaxonomy
from codebench_pipeline.repository_config import SourceWeight
from codebench_pipeline.repository_receipts import RepositoryRun, canonical, sha256
from codebench_pipeline.review_frames import REVIEW_BINDING
from codebench_pipeline.review_policy import requires_human_review
from codebench_pipeline.review_snapshots import release_configuration_digest
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, dataset_release_settings


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


def prepare_guarded_release(
    inputs: ReleaseInputs,
    run: RepositoryRun,
    registered: DeclaredGlobalConfig,
    *,
    publication_versions: dict[str, str],
) -> ReleaseProposal:
    """Rebuild the exact proposal from registration for control-plane checks."""
    if registered.scopes.human_review_queue.human_review_queue != inputs.resolved.manifest.publication:
        raise HumanReviewError("stale", "Registered release policy differs from the completed snapshot")
    policy = dataset_release_settings(registered, registered.rules)
    return build_release_proposal(
        inputs,
        policy,
        ReleaseExecution.bind(
            run,
            inputs,
            policy,
            ReleaseAuthorization(
                policy.digest, release_configuration_digest(registered), publication_versions
            ),
        ),
    )


def build_release_proposal(
    inputs: ReleaseInputs, settings: DatasetReleaseConfig, execution: ReleaseExecution
) -> ReleaseProposal:
    resolved = inputs.resolved
    if (
        inputs.token.dataset_id != execution.dataset_id
        or inputs.token.dataset_id != resolved.snapshot.dataset_id
        or settings.digest != execution.mapping_digest
    ):
        raise HumanReviewError("stale", "Release evidence belongs to another dataset")
    taxonomy = settings.taxonomy
    cap = settings.limit_per_risk
    if taxonomy != PublicationTaxonomy.from_rules(resolved.rules) or (
        cap != resolved.manifest.publication.limit_per_risk
    ):
        raise HumanReviewError("stale", "Release settings differ from the completed snapshot")
    attempts = execution.attempts
    counts = execution.counts
    corrected = apply_label_corrections(resolved.reviewed, inputs.decisions, taxonomy.label_validation())
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
        source = execution.sources[identity]
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
        if (
            source_weights is None
            and contribution in REQUIRED_CONTRIBUTIONS
            and command not in execution.baseline
        ):
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
    selected = select_survivors(survivors, {risk.id: cap for risk in taxonomy.risks}, source_weights)
    ordered = selected.ordered
    selected_ids = frozenset(ordered)
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
        taxonomy,
        cap,
        approved_ids=approved,
        selected_ids=selected_ids,
        authored_rules_json=resolved.rules.model_dump_json(indent=2),
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
            "partition": execution.dataset_id,
            "release_configuration": execution.configuration_digest,
            "review_snapshot": resolved.snapshot.snapshot_id,
            "review_completion": inputs.token.model_dump(mode="json"),
            "implementation": {
                **resolved.manifest.implementation,
                **execution.publication_versions,
            },
            "selection_revision": "source-trust-cap-1" if source_weights is not None else SELECTION_REVISION,
            "branch_terminals": resolved.reviewed.attrs[FINALIZATION_BINDING],
            "branch_capture_locks": execution.capture_locks,
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
