import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from configuration_fixture import write_fixture
from pydantic import TypeAdapter, ValidationError

from codebench_pipeline.dataframe_schemas import SourceCatalogTable, frame_records, validated_frame
from codebench_pipeline.dataset_catalog import build_catalog, catalog_row, provided_documents
from codebench_pipeline.dataset_config import (
    CatalogConfig,
    catalog_for_branch,
    load_build_config,
)
from codebench_pipeline.dataset_seed_config import SourceSeed
from codebench_pipeline.dataset_source_models import SourceChoice
from codebench_pipeline.discovery_models import SourceProposal
from codebench_pipeline.human_review import DecisionChange, HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.review_tables import ReviewTable
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.source_review_inputs import SourceReviewSource, source_authorization


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
            spec = load_build_config(write_fixture(Path(temporary)), Path(temporary))
            supplied = catalog_for_branch(spec, "documentation")
            source = supplied.documents[0].model_copy(update={
                "provenance_evidence_json": '{"readme_url":"https://example.org/readme","assessment":"local agent"}'
            })
            config = supplied.model_copy(update={"documents": [source]})
            with patch("codebench_pipeline.inference.subprocess.run", side_effect=AssertionError("No source model")):
                catalog = build_catalog(config, spec.rules, Path(temporary))
            self.assertEqual(frame_records(catalog)[0]["source_id"], source.source_id)
            self.assertEqual(frame_records(catalog)[0]["confidence"], "not_applicable")
            retained = TypeAdapter(list[SourceSeed]).validate_json(catalog.attrs["source_inputs_json"])
            self.assertEqual(retained[0].provenance_evidence_json, source.provenance_evidence_json)

    def test_search_results_are_not_an_active_pipeline_catalog_configuration(self) -> None:
        with self.assertRaises(ValidationError):
            CatalogConfig.model_validate({
                "mode": "keenable",
                "search_request": {"searches": [{"risk": "CAT-99.1", "queries": ["command sources"]}]},
            })

    def test_legacy_source_authorization_preserves_historical_decision_and_completion_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, dataset_instance(Path(temporary)) as instance:
            root = Path(temporary)
            spec = load_build_config(write_fixture(root), root)
            document = provided_documents(catalog_for_branch(spec, "documentation").documents)[0]
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
            source = SourceReviewSource(root, instance)
            with patch.object(source.tables, "resolve", return_value=table):
                resolved = source.current(spec.dataset_id)
                self.assertEqual(resolved.snapshot.candidate_ids, ("selected", "needs_review", "exclude"))
                with HumanReviewStore(root).transaction() as tx:
                    batch = tx.ensure_current_batch(resolved.snapshot)
                    with self.assertRaises(HumanReviewError) as pending:
                        source_authorization(source, spec.dataset_id, tx)
                    self.assertEqual(pending.exception.code, "pending")
                    for identity in resolved.snapshot.candidate_ids:
                        with self.assertRaises(HumanReviewError) as incomplete:
                            current = tx.get_current_batch(spec.dataset_id, "sources")
                            tx.complete(resolved.snapshot, current, "human reviewer")
                        self.assertEqual(incomplete.exception.code, "pending")
                        tx.save_decision(
                            resolved.snapshot,
                            DecisionChange(
                                batch_id=batch.batch_id,
                                generation=batch.generation,
                                candidate_id=identity,
                                expected_row_revision=0,
                                decision="approve" if identity == "selected" else "reject",
                                reviewer="human reviewer",
                            ),
                        )
                    with self.assertRaises(HumanReviewError) as unfinished:
                        source_authorization(source, spec.dataset_id, tx)
                    self.assertEqual(unfinished.exception.code, "pending")
                    current = tx.get_current_batch(spec.dataset_id, "sources")
                    completed = tx.complete(resolved.snapshot, current, "human reviewer")
                    _, authorization = source_authorization(source, spec.dataset_id, tx)
                    self.assertEqual(authorization, completed)
