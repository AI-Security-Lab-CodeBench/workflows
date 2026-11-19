import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from pydantic import ValidationError
from test_batch_labeling import batch_config, remote_model
from test_repository import COLLECTION, repository_config_fixture

from codebench_pipeline.collection_config import DeclaredCollection, DeclaredSourceStageConfig
from codebench_pipeline.dataset_config import AnnotationConfig
from codebench_pipeline.dataset_registry import read_registered_spec
from codebench_pipeline.declared_dataset_bundle import DeclaredDatasetBundle, source_declaration
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.repository_branch_config import RepositoryParser, load_repository_branch
from codebench_pipeline.repository_config import RepositoryBranch
from codebench_pipeline.storage import IntegrityError

BUNDLE = COLLECTION.parents[2] / "datasets" / "cat3.json"


class CollectionDeclarationTests(TestCase):
    def test_registration_resolves_independent_path_free_repository_declarations(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        payload = json.loads(BUNDLE.read_bytes())
        payload["finalization"]["annotation"] = AnnotationConfig(
            model=remote_model(), batch=batch_config(home)
        ).model_dump(mode="json")

        bundle = DeclaredDatasetBundle.model_validate(payload)
        spec = bundle.resolve(BUNDLE.parent, StorageConfig(local_root=str(home)))
        collection = spec.declared_collection
        assert collection is not None

        cases: tuple[tuple[RepositoryBranch, str, str], ...] = (
            ("dataset", "cheat", "tldr"),
            ("documentation", "tldr", "cheat"),
        )
        for branch, origin, sibling in cases:
            with self.subTest(branch=branch):
                source = collection.for_branch(branch)
                declaration = source.model_dump_json()
                ids = {item.origin_id for item in collection.repository_config(branch).origins(branch)}
                self.assertIn(origin, ids)
                self.assertNotIn(sibling, ids)
                self.assertNotIn("source_list", declaration)
                self.assertNotIn("module_path", declaration)
        self.assertEqual(collection.keenable_declaration.dataset, spec.dataset_id)

    def test_parser_config_rejects_unreviewed_pins_and_local_paths(self) -> None:
        parser = load_repository_branch(COLLECTION / "existing-datasets.json").origins[0].transform
        payload = parser.model_dump()

        for change, error_type in (
            ({"parser_id": "unknown-parser"}, "literal_error"),
            ({"sha256": "0" * 64}, "parser_pin"),
            ({"module_path": "private/parser.py"}, "extra_forbidden"),
        ):
            with self.subTest(change=change):
                with self.assertRaises(ValidationError) as raised:
                    RepositoryParser.model_validate({**payload, **change})
                self.assertEqual(raised.exception.errors()[0]["type"], error_type)

    def test_collection_rejects_weight_overrides_that_invert_branch_trust(self) -> None:
        dataset = load_repository_branch(COLLECTION / "existing-datasets.json")
        docs = load_repository_branch(COLLECTION / "documentation-crawl.json")
        keenable = source_declaration((COLLECTION.parent / "keenable" / "declaration.json").read_text())
        for revisions in (
            {
                "existing_datasets": source_declaration(dataset.model_dump_json()),
                "merge_keenable_weight": 0.55,
            },
            {
                "existing_datasets": source_declaration(
                    dataset.model_copy(update={"source_weights": {"cheat": 0.90}}).model_dump_json()
                )
            },
        ):
            with (
                self.subTest(fields=tuple(revisions)),
                self.assertRaisesRegex(IntegrityError, "above Keenable"),
            ):
                DeclaredCollection.model_validate(
                    {
                        "existing_datasets": source_declaration(dataset.model_dump_json()),
                        "documentation_crawl": source_declaration(docs.model_dump_json()),
                        "keenable_use_cases": keenable,
                        **revisions,
                    }
                )

    def test_path_bearing_registered_snapshot_is_readable_but_requires_reregistration(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        payload = json.loads(BUNDLE.read_bytes())
        payload["finalization"]["annotation"] = AnnotationConfig(
            model=remote_model(), batch=batch_config(home)
        ).model_dump(mode="json")
        spec = DeclaredDatasetBundle.model_validate(payload).resolve(
            BUNDLE.parent, StorageConfig(local_root=str(home))
        )
        snapshot = spec.model_dump(mode="json")
        repository = source_declaration(repository_config_fixture().model_dump_json()).model_dump(mode="json")
        legacy_path = "/prior/deployment/repositories.json"
        snapshot["declared_collection"]["existing_datasets"] = {"source_list": legacy_path, **repository}
        snapshot["declared_collection"]["documentation_crawl"] = {"source_list": legacy_path, **repository}

        restored = read_registered_spec(json.dumps(snapshot).encode()).declared_collection
        assert restored is not None
        self.assertEqual(restored.existing_datasets.source_list, legacy_path)
        with self.assertRaisesRegex(IntegrityError, "re-register the path-free"):
            DeclaredSourceStageConfig(branch="dataset", source=restored.existing_datasets)
