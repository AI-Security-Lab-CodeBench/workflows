import json
import os
from datetime import UTC, datetime
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from pandera.typing import DataFrame
from pydantic import TypeAdapter
from test_batch_labeling import batch_config, remote_model, submitted, tool_reply
from test_dataset_rules import make_rules
from test_keenable import BODY, DECLARATION, PEER, PLAN, SOURCE_BODY, permission, result
from test_keenable import source_declaration as source_scope_declaration
from test_repository import CONFIG, RepositoryFixture

from codebench_pipeline.collection_config import CAT3_DATASET_ID, DeclaredCollection
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, FilteredCommandsTable
from codebench_pipeline.dataset_config import (
    AdapterConfig,
    AnnotationConfig,
    CatalogConfig,
    DatasetBuildSpec,
    ExtractionConfig,
    FilteringConfig,
    PublicationConfig,
)
from codebench_pipeline.dataset_extract import filter_candidates
from codebench_pipeline.dataset_finalization import merge_candidates
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.declared_dataset_bundle import source_declaration
from codebench_pipeline.grounding_policy import OperationGrant, SourceVersion, VersionIndependent
from codebench_pipeline.inference_capacity import CapacityConfig
from codebench_pipeline.keenable import KeenableCollection
from codebench_pipeline.keenable_policy import CASES, KeenableDeclaration
from codebench_pipeline.keenable_tables import (
    provider_adapters,
    provider_candidates,
    provider_catalog,
    provider_documents,
)
from codebench_pipeline.keenable_transport import DirectResponse
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.repository import (
    RepositoryRun,
    load_repository_config,
    repository_adapters,
    repository_candidates,
    repository_catalog,
    repository_documents,
)
from codebench_pipeline.repository_config import RepositoryBranch
from codebench_pipeline.repository_receipts import sha256
from codebench_pipeline.repository_tree import raw_url, tree_url

LOOKUP_BODY = b"Terraform plan previews proposed changes without applying them."
RIGHTS = b'{"synthetic":"Fixture authorization to retain and quote the operation documentation"}'
CORPUS = b"Enumerate the pods in the selected namespace:\n```bash\nkubectl get pods\n```\n"
COMPOSE = b"Display the current Compose service status:\n```bash\ndocker compose ps\n```\n"
DOCS = b"# Docker\n\n- List all existing containers:\n\n`docker ps --all`\n"
UNRELATED_SCOPE = b"This section uses a demo cluster.\n```bash\nkubectl get pods --all-namespaces\n```\n"
REVIEWED_SCOPE = (
    b"`--all-namespaces` lists pods across all namespaces.\n```bash\nkubectl get pods --all-namespaces\n```\n"
)


class LabelingFixture:
    def __init__(
        self,
        case: TestCase,
        home: Path,
        *,
        context7: bool = False,
        corpus: bytes = CORPUS,
        source_limited: bool = True,
        documentation_corpus: bytes = DOCS,
        shared_body: bytes = SOURCE_BODY,
        limit_per_risk: int = 3,
    ) -> None:
        self.home = home
        self.remote_rows: list[str] = []
        self.library = "/fixture/docs" if context7 else None
        config = load_repository_config(CONFIG)
        kube, compose, tldr = (
            RepositoryFixture(origin)
            for origin in (config.dataset[1], config.dataset[2], config.documentation[0])
        )
        kube.file("sample/README.md", corpus)
        compose.file("sample/README.md", COMPOSE)
        compose.file("sample/compose.yml", b"services: {}")
        tldr.file("pages/common/docker.md", documentation_corpus)
        self.repositories = (kube, compose, tldr)
        self.repository_config = config.model_copy(
            update={
                "dataset": (kube.declaration(), compose.declaration()),
                "documentation": (tldr.declaration(),),
                "source_weights": {
                    source.origin.origin_id: config.source_weights[source.origin.origin_id]
                    for source in self.repositories
                },
            }
        )
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
        self.grounding = batch.grounding.model_copy(
            update={
                "allowed_domains": ["developer.hashicorp.com", "raw.githubusercontent.com"],
                "pinned_library_ids": [self.library] if self.library else [],
                "maximum_provider_calls": 10,
                "operation_grants_json": ""
                if source_limited
                else TypeAdapter(tuple[OperationGrant, ...]).dump_json((grant, *seed_grants)).decode(),
            }
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
        repository = source_declaration(CONFIG, self.repository_config.model_dump_json())
        self.spec = DatasetBuildSpec(
            dataset_id=CAT3_DATASET_ID,
            revision="synthetic-guarded",
            rules=self.rules,
            catalog=CatalogConfig(),
            adapters=AdapterConfig(),
            extraction=ExtractionConfig(),
            filtering=FilteringConfig(),
            publication=PublicationConfig(limit_per_risk=limit_per_risk),
            annotation=AnnotationConfig(
                model=remote_model(),
                batch=batch.model_copy(update={"grounding": self.grounding, "batch_size": 3}),
            ),
            storage=StorageConfig(local_root=str(home / "tables")),
            declared_collection=DeclaredCollection(
                existing_datasets=repository,
                documentation_crawl=repository,
                keenable_use_cases=source_declaration(DECLARATION, self.declaration.model_dump_json()),
            ),
        )
        case.enterContext(patch.dict(os.environ, {"KEENABLE_API_KEY": "fixture-key-long"}))
        case.enterContext(
            patch(
                "codebench_pipeline.repository_acquisition.fetch_repository_bytes",
                side_effect=self.repository_wire,
            )
        )
        case.enterContext(
            patch(
                "codebench_pipeline.keenable.request_direct",
                side_effect=(
                    [result(PLAN), DirectResponse(shared_body, PEER)]
                    if source_limited
                    else [result(PLAN), result(), result(), result(), result(), DirectResponse(BODY, PEER)]
                ),
            )
        )
        self.inference = case.enterContext(
            patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=self.infer)
        )

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
        run = RepositoryRun(self.home, self.spec.dataset_id, "synthetic-filter")

        def repository(branch: RepositoryBranch) -> DataFrame[CommandCandidatesTable]:
            catalog = repository_catalog(self.repository_config, branch, run)
            documents = repository_documents(catalog, self.repository_config, run)
            adapters = repository_adapters(documents, self.repository_config, run)
            return repository_candidates(documents, adapters, self.repository_config, run)

        provider = KeenableCollection(self.home, self.declaration)
        documents = provider_documents(provider, provider_catalog(provider))
        search_rows = provider_candidates(provider, documents, provider_adapters(provider, documents))
        merged = merge_candidates(repository("dataset"), repository("documentation"), search_rows)
        filtered = filter_candidates(merged, self.rules, self.spec.filtering)
        filtered.attrs = merged.attrs
        return filtered

    def infer(
        self,
        _url: str,
        body: bytes,
        _key: str | None,
        _timeout: float,
        _capacity: CapacityConfig,
        _path: Path,
    ) -> HTTPResponse:
        messages = json.loads(body)["messages"]
        packet = json.loads(messages[1]["content"])
        rows = packet["records"]
        if len(messages) == 2:
            self.remote_rows.extend(row["candidate"]["candidate_id"] for row in rows)
            missing = [row for row in rows if not row["evidence_ids"]]
            if missing:
                query = {
                    "action": "context7_context" if self.library else "keenable_search",
                    "query": "a model-supplied question that must never be sent",
                    "candidate_ids": [row["candidate"]["candidate_id"] for row in missing],
                }
                if self.library:
                    query["library_id"] = self.library
                return tool_reply("request_documentation", {"queries": [query]})
        outcomes = []
        for row in rows:
            candidate = CandidateRecord.model_validate(row["candidate"])
            evidence = {name: packet["documentation"][name] for name in row["evidence_ids"]}
            for message in messages[2:]:
                if message["role"] == "tool":
                    result = json.loads(message["content"])
                    for entry in result.get("results", []):
                        evidence.update(
                            {
                                item["evidence_id"]: item
                                for item in entry.get("verified_evidence", {}).get(candidate.candidate_id, [])
                            }
                        )
            if evidence:
                name, document = next(iter(evidence.items()))
                snippet = document["snippet"]
                outcome = submitted(row, [name], snippet["url"])
                outcome["label"]["category_id"] = self.rules.category_id
                outcome["grounding_quotes"] = {name: snippet["snippet"]}
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
        return tool_reply("submit_labels", {"outcomes": outcomes})
