import os
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from unittest import TestCase
from unittest.mock import patch

from keenable_mcp_fixture import keenable_mcp_fixture
from pandera.typing import DataFrame
from pydantic import TypeAdapter
from test_batch_labeling import (
    batch_config,
    live_mcp_fixture,
    remote_model,
    request_parts,
    submitted,
    tool_reply,
)
from test_dataset_rules import make_rules
from test_declared_dataset_graph import fixture_spec
from test_keenable import BODY, PEER, PLAN, SOURCE_BODY, permission, result
from test_keenable import source_declaration as source_scope_declaration
from test_repository import RepositoryFixture, repository_branch_fixture, repository_config_fixture

from codebench_pipeline.batch_labeling import BatchExecution
from codebench_pipeline.batch_labeling_rows import RowEvidence
from codebench_pipeline.collection_config import (
    CAT3_DATASET_ID,
    CollectionMergeConfig,
    DeclaredCollection,
    KeenableSnippetDeclaration,
    SourceDeclaration,
)
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    FilteredCommandsTable,
    SourceDocumentsTable,
)
from codebench_pipeline.dataset_config import AnnotationConfig, DatasetBuildSpec, require_declared_annotation
from codebench_pipeline.dataset_extract import filter_candidates
from codebench_pipeline.dataset_finalization import merge_candidates
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding import GuardedEvidence, OperationReceiptBuffer
from codebench_pipeline.grounding_policy import OperationGrant, SourceVersion, VersionIndependent
from codebench_pipeline.grounding_sources import persist_repository_origin_bindings
from codebench_pipeline.grounding_tables import GuardedLabeling, LabelingEvidenceInputs
from codebench_pipeline.grounding_tools import GroundingConfig, GroundingCredentials
from codebench_pipeline.inference_capacity import CapacityConfig
from codebench_pipeline.keenable import KeenableCollection
from codebench_pipeline.keenable_policy import CASES, PROVIDER, KeenableDeclaration, KeenableSnippetSettings
from codebench_pipeline.keenable_tables import (
    provider_adapters,
    provider_candidates,
    provider_catalog,
    provider_documents,
)
from codebench_pipeline.keenable_transport import DirectResponse, Request
from codebench_pipeline.labeling_stage_settings import LabelingSources, RiskRoute
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.repository import (
    RepositoryRun,
    repository_adapters,
    repository_candidates,
    repository_catalog,
    repository_documents,
)
from codebench_pipeline.repository_config import RepositoryBranch
from codebench_pipeline.repository_receipts import RECEIPT_ATTR, publish_bytes, sha256
from codebench_pipeline.repository_tree import raw_url, tree_url
from codebench_pipeline.stage_configuration import GuardedLabelingConfig

LOOKUP_BODY = b"Terraform plan previews proposed changes without applying them."
RIGHTS = b'{"synthetic":"Fixture authorization to retain and quote the operation documentation"}'
CORPUS = b"Enumerate the pods in the selected namespace:\n```bash\nkubectl get pods\n```\n"
COMPOSE = b"Display the current Compose service status:\n```bash\ndocker compose ps\n```\n"
DOCS = b"# Docker\n\n- List all existing containers:\n\n`docker ps --all`\n"
PLAN_REFERENCE = b"Previews Terraform changes using `terraform plan`.\n"
UNRELATED_SCOPE = b"This section uses a demo cluster.\n```bash\nkubectl get pods --all-namespaces\n```\n"
REVIEWED_SCOPE = (
    b"`--all-namespaces` lists pods across all namespaces.\n```bash\nkubectl get pods --all-namespaces\n```\n"
)


LIVE_URL = "https://developer.hashicorp.com/fixture-reference"
LIVE_QUOTES = {
    "kubectl get pods": "Enumerates the pods for `kubectl get pods`.",
    "kubectl get pods --all-namespaces": "`kubectl get pods --all-namespaces` lists pods across all namespaces.",
    "docker compose ps": "`docker compose ps` displays current Compose service status.",
    "docker ps --all": "`docker ps --all` lists all existing containers.",
    "terraform plan": "`terraform plan` previews changes without applying them.",
}


@dataclass(frozen=True, slots=True)
class FixtureLabeling(GuardedLabeling):
    evidence_inputs: LabelingEvidenceInputs


def historical_support(
    inputs: LabelingEvidenceInputs,
    commands: DataFrame[FilteredCommandsTable],
    candidate: CandidateRecord,
) -> tuple[GuardedEvidence, list[RowEvidence]]:
    receipts = OperationReceiptBuffer()
    guard = inputs.admission(commands, operation_receipts=receipts)
    _, evidence = guard.source(candidate)
    for key, body in receipts.receipts.items():
        publish_bytes(guard.root / key, body)
    return replace(guard, operation_receipts=None), evidence


def guarded_labeling(
    run: RepositoryRun,
    spec: DatasetBuildSpec,
    capture: DataFrame[SourceDocumentsTable],
    historical: GroundingConfig | None = None,
) -> FixtureLabeling:
    mapped = GuardedLabelingConfig.from_spec(spec)
    settings = mapped.labeling_attempts.batch
    paths = require_declared_annotation(spec.annotation).require_capacity()
    collection = spec.declared_collection
    assert collection is not None
    return FixtureLabeling(
        mapped,
        settings.bind_capacity(paths.state_directory, paths.expected_model_path),
        BatchExecution(None, GroundingCredentials.from_environment(settings.grounding)),
        LabelingEvidenceInputs(
            run,
            LabelingSources.from_collection(collection),
            historical or GroundingConfig(**settings.grounding.model_dump()),
            capture,
        ),
    )


class LabelingFixture:
    def __init__(
        self,
        case: TestCase,
        home: Path,
        *,
        native: bool = False,
        context7: bool = False,
        corpus: bytes = CORPUS,
        source_limited: bool = True,
        documentation_corpus: bytes = DOCS,
        shared_body: bytes = SOURCE_BODY,
        limit_per_risk: int = 3,
        category_confidence: Literal["high", "medium"] = "high",
    ) -> None:
        self.home = home
        self.category_confidence = category_confidence
        self.remote_rows: list[str] = []
        self.library = "/fixture/docs" if context7 else None
        config = repository_config_fixture()
        kube, compose, tldr = (
            RepositoryFixture(origin)
            for origin in (config.dataset[1], config.dataset[2], config.documentation[0])
        )
        kube.file("sample/README.md", corpus)
        compose.file("sample/README.md", COMPOSE)
        compose.file("sample/compose.yml", b"services: {}")
        tldr.file("pages/common/docker.md", documentation_corpus)
        if native:
            tldr.file("pages/common/terraform.md", PLAN_REFERENCE)
        self.repositories = (kube, compose, tldr)
        dataset = repository_branch_fixture("dataset", (kube, compose))
        documentation = repository_branch_fixture("documentation", (tldr,))
        self.repository_configs = {
            "dataset": dataset.runtime("dataset"),
            "documentation": documentation.runtime("documentation"),
        }
        self.declaration = (
            source_scope_declaration()
            if source_limited
            else KeenableDeclaration(
                dataset=CAT3_DATASET_ID,
                generation="legacy-grounding-fixture",
                generation_review="fixture-review",
                cases=CASES,
                permissions=(permission(),),
            )
        )
        plan_start = BODY.index(b"terraform plan")
        grant = OperationGrant(
            url=PLAN,
            capture_sha256=sha256(LOOKUP_BODY),
            rights_sha256=sha256(RIGHTS),
            attribution="Synthetic operation docs",
            valid_until=datetime(2099, 1, 1, tzinfo=UTC),
            media_type="text/plain",
            operation="terraform plan",
            applicability="Fixture 1.0, default scope"
            if context7
            else "Version independent at default scope",
            version_scope=SourceVersion(
                version="1.0",
                source_url=PLAN,
                source_capture_sha256=sha256(BODY),
                source_position=f"bytes:{plan_start}:{plan_start + len(b'terraform plan')}",
                source_revision=self.declaration.generation,
            )
            if context7
            else VersionIndependent(),
            byte_start=0,
            byte_end=len(LOOKUP_BODY),
            library_id=self.library,
            requested_version="1.0" if self.library else None,
            permitted_use="fetch_retain_quote",
            retain_origin_bytes=True,
        )
        seed_grants = []
        for source, path, body, operation, flags, operands in (
            (
                kube,
                "sample/README.md",
                corpus,
                "kubectl get pods",
                ("--all-namespaces",) if corpus == REVIEWED_SCOPE else (),
                ("--all-namespaces",) if corpus == REVIEWED_SCOPE else (),
            ),
            (compose, "sample/README.md", COMPOSE, "docker compose ps", (), ()),
            (tldr, "pages/common/docker.md", DOCS, "docker ps", ("--all",), ("--all",)),
        ):
            quote = body.splitlines()[2] if body == DOCS else body.splitlines()[0]
            start = body.index(quote)
            seed_grants.append(
                OperationGrant(
                    url=raw_url(source.origin, path),
                    capture_sha256=sha256(body),
                    rights_sha256=sha256(RIGHTS),
                    attribution="Synthetic repository operation",
                    valid_until=datetime(2099, 1, 1, tzinfo=UTC),
                    media_type="text/plain",
                    operation=operation,
                    applicability="Version independent for the reviewed invocation",
                    version_scope=VersionIndependent(),
                    flags=flags,
                    argument_patterns=operands,
                    byte_start=start,
                    byte_end=start + len(quote),
                    source_revision=source.origin.commit,
                    permitted_use="fetch_retain_quote",
                    retain_origin_bytes=True,
                )
            )
        batch = batch_config(home)
        live = batch.grounding.model_copy(
            update={"allowed_domains": ["developer.hashicorp.com", "raw.githubusercontent.com"]}
        )
        self.grounding = GroundingConfig(
            allowed_domains=live.allowed_domains,
            pinned_library_ids=[self.library] if self.library else [],
            maximum_provider_calls=10,
            operation_grants_json=""
            if source_limited
            else TypeAdapter(tuple[OperationGrant, ...]).dump_json((grant, *seed_grants)).decode(),
        )
        base_rules = make_rules("CAT-3")
        risk = base_rules.risks[0].model_copy(
            update={
                "command_patterns": [
                    r"^(?:kubectl get pods(?: --all-namespaces)?|docker (?:compose ps|ps --all)|terraform plan)$"
                ]
            }
        )
        self.rules = base_rules.model_copy(update={"risks": [risk]})
        rights_path = home / "label-grounding-v2" / CAT3_DATASET_ID / "rights" / f"{sha256(RIGHTS)}.json"
        rights_path.parent.mkdir(parents=True)
        rights_path.write_bytes(RIGHTS)
        spec = fixture_spec(home)
        self.spec = spec.model_copy(
            update={
                "revision": "synthetic-guarded",
                "rules": self.rules,
                "publication": spec.publication.model_copy(update={"limit_per_risk": limit_per_risk}),
                "annotation": AnnotationConfig(
                    model=remote_model(),
                    batch=batch.model_copy(
                        update={"grounding": live, "batch_size": 3, "grounding_calls_per_command": 2}
                    ),
                ),
                "declared_collection": DeclaredCollection(
                    existing_datasets=SourceDeclaration.from_declaration(dataset),
                    documentation_crawl=SourceDeclaration.from_declaration(documentation),
                    keenable_use_cases=SourceDeclaration.from_declaration(
                        KeenableSnippetDeclaration(
                            schema_version="4",
                            dataset_id=spec.dataset_id,
                            search=KeenableSnippetSettings(seeds=("terraform_preview",)),
                        )
                        if native
                        else self.declaration
                    ),
                    merge=CollectionMergeConfig(
                        repository_origin_weights={
                            kube.origin.origin_id: 0.8,
                            compose.origin.origin_id: 0.62,
                            tldr.origin.origin_id: 0.9,
                        }
                    ),
                ),
            }
        )
        case.enterContext(
            patch(
                "codebench_pipeline.repository_acquisition.fetch_repository_bytes",
                side_effect=self.repository_wire,
            )
        )

        def current_wire(request: Request) -> DirectResponse:
            if request.url == PROVIDER and request.payload is not None:
                return result(PLAN)
            if request.url == PLAN and request.payload is None:
                return DirectResponse(shared_body, PEER)
            raise AssertionError(f"Unexpected synthetic Keenable request: {request.url}")

        if native:
            case.enterContext(keenable_mcp_fixture())
        else:
            case.enterContext(patch.dict(os.environ, {"KEENABLE_API_KEY": "fixture-key-long"}))
            case.enterContext(
                patch(
                    "codebench_pipeline.keenable.request_direct",
                    side_effect=(
                        current_wire
                        if source_limited
                        else [
                            result(PLAN),
                            result(),
                            result(),
                            result(),
                            result(),
                            DirectResponse(BODY, PEER),
                        ]
                    ),
                )
            )
        self.inference = case.enterContext(
            patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=self.infer)
        )
        self.live_calls = case.enterContext(live_mcp_fixture({LIVE_URL: "\n\n".join(LIVE_QUOTES.values())}))

    def repository_wire(self, url: str, limit: int) -> bytes:
        for fixture in self.repositories:
            if url == tree_url(fixture.origin):
                return fixture.response()
            prefix = f"https://raw.githubusercontent.com/{fixture.origin.repository}/{fixture.origin.commit}/"
            if url.startswith(prefix):
                body = fixture.bodies[url.removeprefix(prefix)]
                if len(body) <= limit:
                    return body
        raise AssertionError(f"Unexpected synthetic capture: {url}")

    def filtered(self) -> DataFrame[FilteredCommandsTable]:
        filtered, _ = self.collect()
        return filtered

    def labeling(self) -> tuple[DataFrame[FilteredCommandsTable], FixtureLabeling]:
        filtered, capture = self.collect()
        run = RepositoryRun(self.home, self.spec.dataset_id, "synthetic-filter")
        return filtered, guarded_labeling(run, self.spec, capture, self.grounding)

    def collect(self) -> tuple[DataFrame[FilteredCommandsTable], DataFrame[SourceDocumentsTable]]:
        run = RepositoryRun(self.home, self.spec.dataset_id, "synthetic-filter")
        collection = self.spec.declared_collection
        assert collection is not None
        persist_repository_origin_bindings(collection, run)

        def repository(
            branch: RepositoryBranch,
        ) -> tuple[DataFrame[CommandCandidatesTable], DataFrame[SourceDocumentsTable]]:
            config = self.repository_configs[branch]
            catalog = repository_catalog(config, branch, run)
            documents = repository_documents(catalog, config, run)
            if branch == "documentation":
                documents.attrs["documentation_scope_receipt_json"] = catalog.attrs[RECEIPT_ATTR]
            adapters = repository_adapters(documents, config, run)
            return repository_candidates(documents, adapters, config, run), documents

        provider = KeenableCollection(self.home, self.declaration)
        api_key = os.environ["KEENABLE_API_KEY"]
        documents = provider_documents(provider, provider_catalog(provider, api_key), api_key)
        search_rows = provider_candidates(provider, documents, provider_adapters(provider, documents))
        dataset, _ = repository("dataset")
        documentation, capture = repository("documentation")
        merged = merge_candidates(dataset, documentation, search_rows, config=collection.merge_config)
        routes = tuple(RiskRoute.from_rule(risk) for risk in self.rules.risks)
        filtered = filter_candidates(merged, routes, self.spec.filtering)
        filtered.attrs = merged.attrs
        return filtered, capture

    def infer(
        self,
        _url: str,
        body: bytes,
        _key: str | None,
        _timeout: float,
        _capacity: CapacityConfig | None = None,
        _path: Path | None = None,
    ) -> HTTPResponse:
        rows, results = request_parts(body)
        ids = [row["candidate"]["candidate_id"] for row in rows]
        if not results:
            self.remote_rows.extend(ids)
            return tool_reply("query", {"query": "Official fixture CLI reference", "candidate_ids": ids})
        if len(results) == 1:
            (hit,) = results[0]["discovery"]
            return tool_reply("page_fetch", {"url": hit["url"], "candidate_ids": ids})
        page = results[1].get("page", {})
        outcomes = []
        for row in rows:
            candidate = CandidateRecord.model_validate(row["candidate"])
            evidence = page.get("evidence_ids", {}).get(candidate.candidate_id)
            quote = LIVE_QUOTES.get(candidate.command)
            if evidence and quote:
                outcome = submitted(row, [evidence], page["url"], quote=quote)
                outcome["label"]["category_id"] = candidate.risk_candidates[0].split(".", 1)[0]
                outcome["label"]["category_confidence"] = self.category_confidence
            else:
                outcome = {
                    "candidate_id": candidate.candidate_id,
                    "row_input_fingerprint": row["row_input_fingerprint"],
                    "label": None,
                    "evidence_ids": [],
                    "grounding_quotes": {},
                    "grounding_rationale": "",
                    "failure_code": "operation_unsupported",
                    "failure_detail": "Origin evidence is unavailable.",
                }
            outcomes.append(outcome)
        return tool_reply("finalize", {"outcomes": outcomes})
