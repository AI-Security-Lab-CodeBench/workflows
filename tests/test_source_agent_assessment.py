import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from configuration_fixture import example_config, source_bytes
from pydantic import TypeAdapter, ValidationError

from codebench_pipeline.dataframe_schemas import SourceCatalogTable, frame_records, validated_frame
from codebench_pipeline.dataset_catalog import build_catalog, catalog_row, provided_documents
from codebench_pipeline.dataset_config import CatalogConfig
from codebench_pipeline.dataset_seed_config import SourceSeed
from codebench_pipeline.dataset_source_models import SourceChoice
from codebench_pipeline.discovery_models import SourceProposal
from codebench_pipeline.human_review import DecisionChange, HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.review_stages import ReviewStage
from codebench_pipeline.review_tables import ReviewTable, TableReviewSource
from codebench_pipeline.object_store import ObjectStore, StorageConfig
from codebench_pipeline.source_manifests import resolve_source_paths


def source_proposal() -> SourceProposal:
    return SourceProposal(
        source_id="inspected-source",
        risk="CAT-99.1",
        url="https://source.invalid/commands",
        source_type="documentation",
        origin="human_authored",
        origin_evidence="Maintainer examples",
        commands_evidence="git status",
        license="MIT",
        license_url="https://source.invalid/license",
        license_evidence="MIT License",
        reuse_decision="permitted",
        revision="inspected-revision",
        decision="proposed",
        analysis="The inspected maintainer examples contain relevant Git commands and reuse terms.",
        confidence="medium",
        rationale="Relevant inspected source.",
    )


class SourceAgentAssessmentTests(unittest.TestCase):
    def test_source_models_require_analysis_and_enum_confidence(self) -> None:
        choice = SourceChoice(
            source_id="source",
            decision="selected",
            analysis="The sample contains relevant commands.",
            confidence="high",
            rationale="Relevant sample.",
        )
        for model, record in (
            (SourceChoice, choice.model_dump()),
            (SourceProposal, source_proposal().model_dump()),
        ):
            for field in ("analysis", "confidence"):
                with self.subTest(model=model.__name__, missing=field):
                    incomplete = {key: value for key, value in record.items() if key != field}
                    with self.assertRaises(ValidationError):
                        model.model_validate(incomplete)
            for confidence in (0.9, 1, True, "0.9", "certain", None):
                with self.subTest(model=model.__name__, confidence=confidence):
                    with self.assertRaises(ValidationError):
                        model.model_validate({**record, "confidence": confidence})
            with self.subTest(model=model.__name__, analysis=""):
                with self.assertRaises(ValidationError):
                    model.model_validate({**record, "analysis": ""})
            for confidence in ("low", "medium", "high"):
                with self.subTest(model=model.__name__, confidence=confidence):
                    self.assertEqual(
                        model.model_validate({**record, "confidence": confidence}).confidence,
                        confidence,
                    )

    def test_catalog_retains_manifest_provenance_without_source_model_selection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = example_config()
            supplied = fixture.documentation_sources
            assert supplied is not None
            (root / "source.md").write_bytes(source_bytes())
            source = resolve_source_paths(supplied.documents[0], root).model_copy(
                update={
                    "provenance_evidence_json": '{"readme_url":"https://example.org/readme","assessment":"local agent"}'
                }
            )
            config = CatalogConfig(documents=[source])
            with patch(
                "codebench_pipeline.inference.subprocess.run", side_effect=AssertionError("No source model")
            ):
                catalog = build_catalog(config, fixture.rules, root, branch="documentation")
            self.assertEqual(frame_records(catalog)[0]["source_id"], source.source_id)
            self.assertEqual(frame_records(catalog)[0]["confidence"], "not_applicable")
            retained = TypeAdapter(list[SourceSeed]).validate_json(catalog.attrs["source_inputs_json"])
            self.assertEqual(retained[0].provenance_evidence_json, source.provenance_evidence_json)

    def test_search_results_are_not_an_active_pipeline_catalog_configuration(self) -> None:
        with self.assertRaises(ValidationError):
            CatalogConfig.model_validate(
                {
                    "mode": "keenable",
                    "search_request": {"searches": [{"risk": "CAT-99.1", "queries": ["command sources"]}]},
                }
            )

    def test_legacy_source_authorization_preserves_historical_decision_and_completion_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = example_config()
            supplied = fixture.documentation_sources
            assert supplied is not None
            (root / "source.md").write_bytes(source_bytes())
            document = provided_documents([resolve_source_paths(supplied.documents[0], root)])[0]
            rows = [
                catalog_row(
                    document.model_copy(update={"source_id": decision}),
                    SourceChoice(
                        source_id=decision,
                        decision=decision,
                        analysis="Inspected command and provenance evidence.",
                        confidence="high",
                        rationale="Source assessment.",
                    ),
                    "git status",
                )
                for decision in ("selected", "needs_review", "exclude")
            ]
            table = ReviewTable(
                frame=validated_frame(rows, SourceCatalogTable),
                id_column="source_id",
                revision="assessment-review",
                evidence={},
                eligible_ids=("selected",),
            )
            stage = ReviewStage(
                stage_id="sources",
                title="Archived source decisions",
                asset_keys=(("datasets", "source_catalog"),),
                review_columns=("source_id",),
            )
            source = TableReviewSource(
                root, stage, lambda _: table, ObjectStore(StorageConfig(local_root=str(root / "objects")))
            )
            resolved = source.current(fixture.dataset_id)
            self.assertEqual(resolved.snapshot.candidate_ids, ("selected", "needs_review", "exclude"))
            store = HumanReviewStore(root)
            with store.transaction() as tx:
                batch = tx.ensure_current_batch(resolved.snapshot)
                self.assertIsNone(batch.completion)
                for identity in resolved.snapshot.candidate_ids:
                    with self.assertRaises(HumanReviewError) as incomplete:
                        current = tx.get_current_batch(fixture.dataset_id, "sources")
                        tx.complete(resolved.snapshot, current, "synthetic reviewer")
                    self.assertEqual(incomplete.exception.code, "pending")
                    tx.save_decision(
                        resolved.snapshot,
                        DecisionChange(
                            batch_id=batch.batch_id,
                            generation=batch.generation,
                            candidate_id=identity,
                            expected_row_revision=0,
                            decision="approve" if identity == "selected" else "reject",
                            reviewer="synthetic reviewer",
                        ),
                    )
                current = tx.get_current_batch(fixture.dataset_id, "sources")
                self.assertIsNone(current.completion)
                completed = tx.complete(resolved.snapshot, current, "synthetic reviewer")
            with HumanReviewStore(root).reader() as tx:
                archived = tx.get_batch(batch.batch_id)
                self.assertEqual(archived.completion, completed)
                self.assertEqual(
                    {row.candidate_id: row.decision for row in tx.list_decisions(batch.batch_id)},
                    {"selected": "approve", "needs_review": "reject", "exclude": "reject"},
                )
