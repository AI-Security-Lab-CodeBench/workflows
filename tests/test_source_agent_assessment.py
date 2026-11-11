import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from configuration_fixture import write_fixture
from pydantic import ValidationError

from codebench_pipeline.dataframe_schemas import SourceCatalogTable, frame_records, validated_frame
from codebench_pipeline.dataset_catalog import build_catalog, catalog_row, provided_documents
from codebench_pipeline.dataset_config import (
    CatalogConfig,
    SnapshotConfig,
    catalog_for_branch,
    load_build_config,
)
from codebench_pipeline.dataset_documents import snapshot_documents
from codebench_pipeline.dataset_seed_config import RiskSearchSeeds, SearchSeeds
from codebench_pipeline.dataset_source_models import SourceChoice, SourceSelection
from codebench_pipeline.discovery_models import DiscoveryOutput, InspectedPage, SourceProposal
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

    def test_provided_catalog_retains_agent_assessment_for_every_decision(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            spec = load_build_config(write_fixture(Path(temporary)), Path(temporary))
            source_config = catalog_for_branch(spec, "documentation")
            identifier = source_config.documents[0].source_id
            for decision, confidence in (
                ("selected", "high"),
                ("needs_review", "medium"),
                ("exclude", "low"),
            ):
                with self.subTest(decision=decision):
                    selection = SourceSelection.model_validate(
                        {
                            "sources": [
                                {
                                    "source_id": identifier,
                                    "decision": decision,
                                    "analysis": f"Agent inspected the sample for {decision}.",
                                    "confidence": confidence,
                                    "rationale": f"Agent recommendation: {decision}.",
                                }
                            ]
                        }
                    )
                    with patch("codebench_pipeline.dataset_catalog.infer_typed", return_value=selection):
                        catalog = build_catalog(source_config, spec.rules, Path(temporary))
                    self.assertEqual(
                        [
                            frame_records(catalog)[0][field]
                            for field in ("analysis", "confidence", "rationale")
                        ],
                        [
                            selection.sources[0].analysis,
                            confidence,
                            selection.sources[0].rationale,
                        ],
                    )

    def test_discovered_assessment_survives_catalog_but_not_document_conversion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = load_build_config(write_fixture(root), root)
            proposal = source_proposal()
            output = DiscoveryOutput(
                sources=[proposal],
                adapter_requests=[],
                inspected=[
                    InspectedPage(
                        url=proposal.url,
                        risk=proposal.risk,
                        retrieved_at="2026-11-11T00:00:00Z",
                        evidence_path=root / "inspected.json",
                        text="Maintainer examples\ngit status\n",
                    )
                ],
                failures=[],
            )
            config = CatalogConfig(
                mode="keenable",
                search_request=SearchSeeds(
                    searches=[RiskSearchSeeds(risk="CAT-99.1", queries=["Git command examples"])]
                ),
                model=spec.catalog.model,
            )
            with patch("codebench_pipeline.dataset_catalog.discover", return_value=output):
                catalog = build_catalog(config, spec.rules, root)
            self.assertEqual(
                [frame_records(catalog)[0][field] for field in ("analysis", "confidence", "rationale")],
                [proposal.analysis, proposal.confidence, proposal.rationale],
            )
            documents = snapshot_documents(catalog, SnapshotConfig())
            self.assertEqual(documents.loc[0, "content"], output.inspected[0].text)
            self.assertTrue({"analysis", "confidence", "rationale"}.isdisjoint(documents.columns))

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
