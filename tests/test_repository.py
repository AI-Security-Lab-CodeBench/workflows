from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.repository import (
    RepositoryRun,
    repository_adapters,
    repository_candidates,
    repository_catalog,
    repository_documents,
)
from codebench_pipeline.repository_branch_config import (
    DeclaredRepositoryOrigin,
    RepositoryBranchConfig,
    load_repository_branch,
)
from codebench_pipeline.repository_config import RepositoryBranch, RepositoryConfig, RepositoryOrigin
from codebench_pipeline.repository_receipts import read_receipt
from codebench_pipeline.storage import IntegrityError

COLLECTION = Path("business-configs/categories/cat3/collection")
LICENSE = b"Synthetic fixture license."
KUBE = (
    "Overview: caf\u00e9\r\n"
    "Outside: `kubectl get nodes`\r\n"
    "```console\r\n"
    "  $ kubectl get pods -l 'name=two words'\r\n"
    "kubectl describe ignored-output\r\n"
    "```\r\n"
    "````yaml\r\n"
    "```bash\r\n"
    "kubectl get fake\r\n"
    "```\r\n"
    "````\r\n"
    "```shell\r\n"
    "kubectl get unclosed\r\n"
).encode()
DOCKER = b"```\n$ docker compose ps --all\n```\n"
TLDR = b"# docker\n\n- List containers:\n\n`docker ps --all`\n"


class RepositoryFixture:
    def __init__(self, origin: RepositoryOrigin) -> None:
        self.origin = origin
        self.bodies: dict[str, bytes] = {"LICENSE": LICENSE}
        self.entries: dict[str, tuple[str, str, str, str]] = {}
        self.file("LICENSE", LICENSE)

    def file(self, path: str, body: bytes, *, mode: str = "100644") -> None:
        self.bodies[path] = body
        elements = path.split("/")
        for offset in range(1, len(elements)):
            self.entries["/".join(elements[:offset])] = ("040000", "tree", "2" * 40, "")
        blob = hashlib.sha1(f"blob {len(body)}\0".encode() + body, usedforsecurity=False)
        self.entries[path] = (mode, "blob", blob.hexdigest(), str(len(body)))

    def declaration(self) -> RepositoryOrigin:
        canonical = "".join("\t".join((name, *entry)) + "\n" for name, entry in sorted(self.entries.items()))
        return self.origin.model_copy(
            update={
                "tree_digest": hashlib.sha256(canonical.encode()).hexdigest(),
                "license": self.origin.license.model_copy(
                    update={
                        "path": "LICENSE",
                        "sha256": hashlib.sha256(LICENSE).hexdigest(),
                    }
                ),
                "notices": (),
                "excluded_paths": (),
            }
        )

    def response(self, *, truncated: bool = False) -> bytes:
        return json.dumps(
            {
                "sha": self.origin.commit,
                "truncated": truncated,
                "tree": [
                    {
                        "path": name,
                        "mode": mode,
                        "type": kind,
                        "sha": blob,
                        **({"size": int(size)} if size else {}),
                    }
                    for name, (mode, kind, blob, size) in self.entries.items()
                ],
            }
        ).encode()


def repository_config_fixture() -> RepositoryConfig:
    dataset = load_repository_branch(COLLECTION / "existing-datasets.json").runtime("dataset")
    docs = load_repository_branch(COLLECTION / "documentation-crawl.json").runtime("documentation")
    return RepositoryConfig(
        version=1,
        category_id="CAT-3",
        revision="synthetic",
        refresh="reviewed_declaration_and_pin_only",
        failure="block_branch_on_incomplete_scope_or_integrity_error",
        dataset=dataset.origins("dataset"),
        documentation=docs.origins("documentation"),
        source_weights=dataset.source_weights | docs.source_weights,
    )


def repository_branch_fixture(
    branch: RepositoryBranch, fixtures: tuple[RepositoryFixture, ...]
) -> RepositoryBranchConfig:
    filename = {"dataset": "existing-datasets.json", "documentation": "documentation-crawl.json"}[branch]
    templates = load_repository_branch(COLLECTION / filename)
    origins = {origin.origin_id: origin for origin in templates.origins}
    source_ids = [fixture.origin.origin_id for fixture in fixtures]
    return RepositoryBranchConfig(
        revision="synthetic",
        origins=tuple(
            DeclaredRepositoryOrigin.model_validate(
                {
                    **fixture.declaration().model_dump(exclude={"transform"}),
                    "transform": origins[fixture.origin.origin_id].transform,
                }
            )
            for fixture in fixtures
        ),
        source_weights={source_id: templates.source_weight(branch, source_id) for source_id in source_ids},
    )


class RepositoryStagesTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.run_context = RepositoryRun(Path(temporary.name), "cat3-fixture", "run-1")
        initial = repository_config_fixture()
        self.kube = RepositoryFixture(initial.dataset[1])
        self.kube.file("sample/README.md", KUBE)
        self.kube.file("sample/nested/tests/README.md", b"out of scope")
        self.kube.file("links/README.md", b"../sample/README.md", mode="120000")
        self.kube.file("index/README.md", b"An index with no examples.\n")
        self.kube.file("external/README.md", b"Not admitted")
        self.kube.file("external/NOTICE", b"Separate upstream")
        self.docker = RepositoryFixture(initial.dataset[2])
        self.docker.file("sample/nested/README.md", DOCKER)
        self.docker.file("sample/nested/compose.yaml", b"services: {}")
        self.docker.file("sample/README.md", b"Manifest symlink should not count")
        self.docker.file("sample/compose.yaml", b"nested/compose.yaml", mode="120000")
        self.tldr = RepositoryFixture(initial.documentation[0])
        self.tldr.file("pages/common/docker.md", TLDR)
        self.tldr.file("pages.fr/common/docker.md", b"Do not include translation")
        self.fixtures = (self.kube, self.docker, self.tldr)
        self.config = initial.model_copy(
            update={
                "dataset": (self.kube.declaration(), self.docker.declaration()),
                "documentation": (self.tldr.declaration(),),
            }
        )
        self.fetch = self.enterContext(
            patch(
                "codebench_pipeline.repository_acquisition.fetch_repository_bytes",
                side_effect=self.wire,
            )
        )

    def wire(self, url: str, limit: int) -> bytes:
        for fixture in self.fixtures:
            origin = fixture.origin
            if (
                url
                == f"https://api.github.com/repos/{origin.repository}/git/trees/{origin.commit}?recursive=1"
            ):
                return fixture.response()
            prefix = f"https://raw.githubusercontent.com/{origin.repository}/{origin.commit}/"
            if url.startswith(prefix):
                path = url.removeprefix(prefix)
                body = fixture.bodies[path]
                if (
                    path
                    not in {
                        "LICENSE",
                        "sample/README.md",
                        "sample/nested/README.md",
                        "index/README.md",
                        "pages/common/docker.md",
                    }
                    or len(body) > limit
                ):
                    raise AssertionError(f"Unexpected capture: {path}")
                return body
        raise AssertionError(f"Unexpected repository URL: {url}")

    def test_catalog_withholds_notices_and_symlinks_and_requires_regular_compose_sibling(self) -> None:
        catalog = repository_catalog(self.config, "dataset", self.run_context)

        rows = frame_records(catalog)
        self.assertEqual(
            {(row["source_locator"], row["decision"], row["rationale"]) for row in rows},
            {
                ("sample/README.md", "selected", "retain_and_quote_attributed_examples"),
                ("sample/nested/README.md", "selected", "retain_and_quote_attributed_examples"),
                ("index/README.md", "selected", "retain_and_quote_attributed_examples"),
                ("external/README.md", "exclude", "unresolved_local_notice"),
                ("links/README.md", "exclude", "not_regular_file"),
            },
        )

    def test_branches_extract_literal_candidates_with_unicode_byte_spans_and_replay_offline(self) -> None:
        cases: tuple[tuple[RepositoryBranch, list[tuple[str, bytes]]], ...] = (
            (
                "dataset",
                [("kubectl get pods -l 'name=two words'", KUBE), ("docker compose ps --all", DOCKER)],
            ),
            ("documentation", [("docker ps --all", TLDR)]),
        )
        for branch, expected in cases:
            with self.subTest(branch=branch):
                catalog = repository_catalog(self.config, branch, self.run_context)
                repository_documents(catalog, self.config, self.run_context)

                replay = RepositoryRun(self.run_context.dataset_home, self.run_context.dataset_id, "run-2")
                with patch(
                    "codebench_pipeline.repository_acquisition.fetch_repository_bytes",
                    side_effect=AssertionError("replay attempted network"),
                ):
                    catalog = repository_catalog(self.config, branch, replay)
                    documents = repository_documents(catalog, self.config, replay)
                    adapters = repository_adapters(documents, self.config, replay)
                    candidates = repository_candidates(documents, adapters, self.config, replay)

                rows = frame_records(candidates)
                self.assertEqual([row["command"] for row in rows], [command for command, _ in expected])
                receipt = read_receipt(candidates, "command_candidates", replay)
                self.assertEqual(receipt.run_id, "run-2")
                evidence = TypeAdapter(dict[str, dict[str, JsonValue]]).validate_python(
                    receipt.observations["candidates"]
                )
                for row, (command, original) in zip(rows, expected, strict=True):
                    source = evidence[str(row["candidate_id"])]
                    span = TypeAdapter(dict[str, int]).validate_python(source["source_span"])
                    self.assertEqual(original[span["byte_start"] : span["byte_end"]], command.encode())
                    self.assertEqual(source["capture_sha256"], hashlib.sha256(original).hexdigest())

    def test_capture_hash_failure_does_not_publish_a_documents_receipt(self) -> None:
        catalog = repository_catalog(self.config, "dataset", self.run_context)
        self.kube.bodies["sample/README.md"] = b"x" * len(KUBE)

        with self.assertRaisesRegex(IntegrityError, "capture Git blob differs"):
            repository_documents(catalog, self.config, self.run_context)

        failures = list((self.run_context.root / "failures" / self.run_context.run_id).glob("*.json"))
        self.assertEqual(len(failures), 1)
        self.assertFalse(json.loads(failures[0].read_bytes())["complete"])
        self.assertEqual(len(list((self.run_context.root / "receipts").glob("*.json"))), 1)

    def test_catalog_rejects_truncation_duplicate_paths_and_rights_digest_mismatch(self) -> None:
        original = self.kube.response()
        duplicate = json.loads(original)
        duplicate["tree"].append(duplicate["tree"][0])
        for result, reason in (
            (self.kube.response(truncated=True), "Truncated"),
            (json.dumps(duplicate).encode(), "Duplicate paths"),
            (original, "Rights evidence SHA-256 differs"),
        ):
            with self.subTest(reason=reason):
                with TemporaryDirectory() as isolated:
                    config = self.config.model_copy(
                        update={
                            "dataset": (
                                self.config.dataset[0].model_copy(
                                    update={
                                        "license": self.config.dataset[0].license.model_copy(
                                            update={"sha256": "f" * 64}
                                        ),
                                    }
                                ),
                            )
                        }
                    )

                    def changed_wire(url: str, limit: int) -> bytes:
                        return result if "?recursive=" in url else self.wire(url, limit)

                    self.fetch.side_effect = changed_wire
                    with self.assertRaisesRegex(IntegrityError, reason):
                        repository_catalog(config, "dataset", RepositoryRun(Path(isolated), "fixture", "run"))

    def test_correct_hash_on_unapproved_transform_cannot_authorize_import(self) -> None:
        body = b"raise RuntimeError('Unapproved code must never reach import')\n"
        module = self.run_context.dataset_home / "unapproved.py"
        module.write_bytes(body)
        original = self.config.documentation[0]
        transform = original.transform
        assert transform is not None
        custom = original.model_copy(
            update={
                "transform": transform.model_copy(
                    update={
                        "module_path": str(module),
                        "sha256": hashlib.sha256(body).hexdigest(),
                    }
                ),
            }
        )
        config = self.config.model_copy(update={"documentation": (custom,)})
        catalog = repository_catalog(config, "documentation", self.run_context)
        documents = repository_documents(catalog, config, self.run_context)

        with self.assertRaisesRegex(IntegrityError, "absent from the deployment allowlist"):
            repository_adapters(documents, config, self.run_context)
