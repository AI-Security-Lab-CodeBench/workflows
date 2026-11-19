from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from pydantic import ValidationError
from test_repository import TLDR, RepositoryFixture, repository_branch_fixture, repository_config_fixture

from codebench_pipeline.repository import (
    RepositoryRun,
    repository_adapters,
    repository_candidates,
    repository_catalog,
    repository_documents,
)
from codebench_pipeline.repository_branch_config import RepositoryBranchDeclaration
from codebench_pipeline.repository_config import RepositoryConfig
from codebench_pipeline.storage import IntegrityError, digest


class RepositoryCurrentnessTests(TestCase):
    def setUp(self) -> None:
        home = self.enterContext(TemporaryDirectory())
        self.run_context = RepositoryRun(Path(home), "cat3-isolation", "run")
        shipped = repository_config_fixture()
        cheat = RepositoryFixture(shipped.dataset[0])
        cheat.file("docker", b"docker ps --all\n")
        docs = RepositoryFixture(shipped.documentation[0])
        docs.file("pages/common/docker.md", TLDR)
        self.docs = docs
        self.config = shipped.model_copy(
            update={
                "dataset": (cheat.declaration(),),
                "documentation": (docs.declaration(),),
                "source_weights": {
                    source.origin.origin_id: shipped.source_weights[source.origin.origin_id]
                    for source in (cheat, docs)
                },
            }
        )
        mocked: dict[str, bytes] = {}
        for fixture in (cheat, docs):
            repo, pin = fixture.origin.repository, fixture.origin.commit
            mocked[f"https://api.github.com/repos/{repo}/git/trees/{pin}?recursive=1"] = fixture.response()
            mocked.update(
                {
                    f"https://raw.githubusercontent.com/{repo}/{pin}/{path}": body
                    for path, body in fixture.bodies.items()
                }
            )

        def wire(url: str, _limit: int) -> bytes:
            return mocked[url]

        self.enterContext(
            patch(
                "codebench_pipeline.repository_acquisition.fetch_repository_bytes",
                side_effect=wire,
            )
        )

    def test_refresh_revision_invalidates_receipts_without_changing_the_selected_files(self) -> None:
        declaration = repository_branch_fixture("documentation", (self.docs,))
        config = declaration.runtime("documentation")
        catalog = repository_catalog(config, "documentation", self.run_context)
        refreshed = declaration.model_copy(update={"refresh_revision": "force-refresh"}).runtime(
            "documentation"
        )

        with self.assertRaisesRegex(IntegrityError, "catalog is stale"):
            repository_documents(catalog, refreshed, self.run_context)

    def test_capture_obeys_the_authored_byte_limit(self) -> None:
        declaration = repository_branch_fixture("documentation", (self.docs,))
        payload = {**declaration.model_dump(mode="json"), "maximum_file_bytes": len(TLDR) - 1}
        config = RepositoryBranchDeclaration.model_validate(payload).runtime("documentation")
        catalog = repository_catalog(config, "documentation", self.run_context)

        with self.assertRaisesRegex(IntegrityError, "exceeds the configured size: pages/common/docker.md"):
            repository_documents(catalog, config, self.run_context)

    def test_catalog_blocks_an_unverified_tree_or_an_exclusion_outside_the_selector(self) -> None:
        declaration = repository_branch_fixture("documentation", (self.docs,))
        origin = declaration.origins[0]
        for change, reason in (
            ({"tree_digest": "0" * 64}, "Tree contents differ from approved inventory"),
            ({"excluded_paths": ("pages/common/missing.md",)}, r"exclusions.*pages/common/missing.md"),
        ):
            with self.subTest(change=change):
                revised = declaration.model_copy(update={"origins": (origin.model_copy(update=change),)})
                with self.assertRaisesRegex(IntegrityError, reason):
                    repository_catalog(revised.runtime("documentation"), "documentation", self.run_context)

    def test_docs_rights_change_invalidates_only_documentation_receipts(self) -> None:
        run = self.run_context
        dataset_catalog = repository_catalog(self.config, "dataset", run)
        docs_catalog = repository_catalog(self.config, "documentation", run)
        docs = repository_documents(docs_catalog, self.config, run)
        docs_adapters = repository_adapters(docs, self.config, run)
        changed = self.config.model_copy(
            update={
                "revision": "docs-attribution-updated",
                "documentation": (
                    self.config.documentation[0].model_copy(
                        update={
                            "attribution": "Updated documentation attribution",
                        }
                    ),
                ),
            }
        )

        dataset = repository_documents(dataset_catalog, changed, run)
        adapters = repository_adapters(dataset, changed, run)
        rows = repository_candidates(dataset, adapters, changed, run)

        self.assertEqual(list(rows["command"]), ["docker ps --all"])
        with self.assertRaisesRegex(IntegrityError, "catalog is stale"):
            repository_documents(docs_catalog, changed, run)
        with self.assertRaisesRegex(IntegrityError, "documents are stale"):
            repository_adapters(docs, changed, run)
        with self.assertRaisesRegex(IntegrityError, "candidate inputs are stale"):
            repository_candidates(docs, docs_adapters, changed, run)

    def test_shared_transform_engine_change_invalidates_each_branch_using_it(self) -> None:
        run = self.run_context
        catalogs = (
            repository_catalog(self.config, "dataset", run),
            repository_catalog(self.config, "documentation", run),
        )

        def changed_file(path: Path) -> str:
            return "f" * 64 if path.name == "source_transforms.py" else digest(path)

        with patch("codebench_pipeline.repository_etl.digest", side_effect=changed_file):
            for catalog in catalogs:
                with self.assertRaisesRegex(IntegrityError, "catalog is stale"):
                    repository_documents(catalog, self.config, run)

    def test_declared_parser_can_change_without_changing_scope_or_capture(self) -> None:
        home = self.run_context
        shipped = repository_config_fixture()
        kube = RepositoryFixture(shipped.dataset[1])
        kube.file("sample/README.md", b"```bash\nkubectl get pods\n```\n\n`kubectl get services`\n")
        old = self.config.model_copy(update={"dataset": (kube.declaration(),)})
        swapped = old.model_copy(
            update={
                "dataset": (
                    kube.declaration().model_copy(update={"transform": shipped.documentation[0].transform}),
                ),
            }
        )
        prefix = f"https://raw.githubusercontent.com/{kube.origin.repository}/{kube.origin.commit}/"
        responses = {
            f"https://api.github.com/repos/{kube.origin.repository}/git/trees/{kube.origin.commit}?recursive=1": kube.response(),
            **{prefix + path: body for path, body in kube.bodies.items()},
        }

        def wire(url: str, _limit: int) -> bytes:
            return responses[url]

        self.assertEqual(old.branch_digest("documentation"), swapped.branch_digest("documentation"))
        with patch("codebench_pipeline.repository_acquisition.fetch_repository_bytes", side_effect=wire):
            original_catalog = repository_catalog(old, "dataset", home)
            original = repository_documents(original_catalog, old, home)
            original_adapters = repository_adapters(original, old, home)
        with patch(
            "codebench_pipeline.repository_acquisition.fetch_repository_bytes",
            side_effect=AssertionError("a parser change must reuse capture bytes"),
        ):
            with self.assertRaisesRegex(IntegrityError, "catalog is stale"):
                repository_documents(original_catalog, swapped, home)
            revised = repository_documents(repository_catalog(swapped, "dataset", home), swapped, home)
        revised_adapters = repository_adapters(revised, swapped, home)

        original_rows = repository_candidates(original, original_adapters, old, home)
        revised_rows = repository_candidates(revised, revised_adapters, swapped, home)

        self.assertEqual(list(original["sha256"]), list(revised["sha256"]))
        self.assertEqual(list(original_rows["command"]), ["kubectl get pods"])
        self.assertEqual(list(revised_rows["command"]), ["kubectl get services"])

    def test_missing_and_untrusted_transforms_are_rejected_by_declaration_parser(self) -> None:
        for transform in (None, {**self.config.dataset[0].transform.model_dump(), "trusted": False}):
            with self.subTest(transform=transform):
                declaration = self.config.model_dump()
                declaration["dataset"][0]["transform"] = transform
                with self.assertRaises(ValidationError):
                    RepositoryConfig.model_validate(declaration)
