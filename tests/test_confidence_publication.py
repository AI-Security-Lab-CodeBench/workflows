import csv
import io
import json
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from configuration_fixture import write_fixture
from pydantic import BaseModel
from review_test_support import approve_fixture_sources
from test_dataset_incremental import provider_response
from test_dataset_rules import make_candidate, make_label, make_review, make_rules

from codebench_pipeline.data_view_models import DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_cli import execute_build, execute_registered_build
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_export import prepare_dataset
from codebench_pipeline.dataset_records import LabelDecision, ReviewDecision
from codebench_pipeline.human_review import DecisionChange
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.publication_authorization import authorize_publication
from codebench_pipeline.release_runner import ReleaseRunner
from codebench_pipeline.review_api_models import (
    CompletionRequest,
    DecisionRequest,
    PublicationTarget,
    RetractionRequest,
)
from codebench_pipeline.review_service import ReviewService
from codebench_pipeline.runtime_instance import dataset_instance


def mixed_confidence_response(
    schema: type[BaseModel], system: str, prompt: str, model: ModelConfig, out: Path
) -> BaseModel:
    result = provider_response(schema, system, prompt, model, out)
    payload = json.loads(prompt[prompt.index("{") :])
    if isinstance(result, LabelDecision):
        return result.model_copy(
            update={
                "category_confidence": "medium"
                if "-auto-approve" in payload["candidate"]["command"]
                else "high"
            }
        )
    if isinstance(result, ReviewDecision):
        return result.model_copy(
            update={"confidence": "low" if "-compact-warnings" in payload["candidate"]["command"] else "high"}
        )
    return result


class ConfidencePublicationTests(unittest.TestCase):
    def test_each_uncertain_judgment_requires_human_approval_and_marks_the_result(self) -> None:
        rules = make_rules()
        candidate = make_candidate()
        for field in (
            "category_confidence",
            "risk_confidence",
            "severity_confidence",
            "class_confidence",
            "review",
        ):
            for confidence in ("low", "medium", "high"):
                with self.subTest(field=field, confidence=confidence):
                    label = make_label(candidate, rules)
                    review = make_review(candidate)
                    if field == "review":
                        review = review.model_copy(update={"confidence": confidence})
                    else:
                        label = label.model_copy(update={field: confidence})
                    pending = prepare_dataset([candidate], [label], [review], rules, 250)
                    self.assertEqual(pending.commands, [])
                    self.assertEqual(pending.human_review_ids, (candidate.candidate_id,))
                    accepted = prepare_dataset(
                        [candidate],
                        [label],
                        [review],
                        rules,
                        250,
                        approved_ids=frozenset({candidate.candidate_id}),
                    )
                    self.assertEqual([row["human_reviewed"] for row in accepted.commands], ["true"])

    def test_confident_rejection_is_neither_automatic_inclusion_nor_uncertain_review(self) -> None:
        rules = make_rules()
        candidate = make_candidate()
        publication = prepare_dataset(
            [candidate],
            [make_label(candidate, rules)],
            [make_review(candidate).model_copy(update={"verdict": "reject"})],
            rules,
            250,
        )
        self.assertEqual(publication.commands, [])
        self.assertEqual(publication.human_review_ids, (candidate.candidate_id,))
        self.assertEqual(publication.eligible_ids, ())

    def test_all_confidences_wait_for_human_review_and_retraction_stales_release(self) -> None:
        with ExitStack() as stack:
            home = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            original = write_fixture(home, "CAT-98")
            config = json.loads(original.read_text())
            config["seeds"] = config.pop("documentation_sources")
            for seed in config["seeds"]["documents"]:
                seed["local_path"] = str((original.parent / seed["local_path"]).resolve())
            config["storage"] = {"local_root": str(home / "tables")}
            path = home / "config.json"
            path.write_text(json.dumps(config))
            for module in ("dataset_catalog", "dataset_documents", "dataset_annotation"):
                stack.enter_context(
                    patch(f"codebench_pipeline.{module}.infer_typed", side_effect=mixed_confidence_response)
                )
            with redirect_stdout(io.StringIO()):
                first = execute_build(path, home)
            self.assertNotIn("source_documents", first)
            self.assertNotIn("automatic_dataset_release", first)
            spec = load_build_config(path, home)
            approve_fixture_sources(home, spec.dataset_id)
            with redirect_stdout(io.StringIO()):
                prepared = execute_registered_build(spec.dataset_id, home)
            self.assertIn("human_review_queue", prepared)
            self.assertNotIn("automatic_dataset_release", prepared)
            instance = stack.enter_context(dataset_instance(home))
            data = DatasetDataService(home, instance)
            service = ReviewService(home, data, ReleaseRunner(home, instance))
            query = DatasetQueryRequest(dataset_id=spec.dataset_id)
            view = service.view(query)
            self.assertEqual(view.counts.pending, 3)
            self.assertEqual(
                {str(row.values["command"]) for row in view.page.rows},
                {
                    "terraform destroy",
                    "terraform destroy -auto-approve",
                    "terraform destroy -compact-warnings",
                },
            )
            self.assertIsNone(view.publication)
            selected = next(
                row.review_record_id
                for row in view.page.rows
                if row.values["command"] == "terraform destroy -compact-warnings"
            )
            for row in view.decisions:
                service.decide(
                    DecisionRequest(
                        dataset_id=spec.dataset_id,
                        snapshot_id=view.batch.snapshot.snapshot_id,
                        change=DecisionChange(
                            batch_id=view.batch.batch_id,
                            generation=view.batch.generation,
                            candidate_id=row.candidate_id,
                            expected_row_revision=row.row_revision,
                            decision="approve" if row.candidate_id == selected else "reject",
                            reviewer="fixture human",
                        ),
                    )
                )
            saved = service.view(query)
            completed = service.complete(
                CompletionRequest(
                    dataset_id=spec.dataset_id,
                    snapshot_id=saved.batch.snapshot.snapshot_id,
                    expected=saved.batch,
                    reviewer="fixture human",
                )
            )
            assert completed.publication is not None
            union_csv = service.download(
                PublicationTarget(
                    dataset_id=spec.dataset_id, publication_id=completed.publication.publication_id
                )
            )
            self.assertEqual(
                {
                    (row["command"], row["human_reviewed"])
                    for row in csv.DictReader(io.StringIO(union_csv.decode()))
                },
                {("terraform destroy -compact-warnings", "true")},
            )
            with HumanReviewStore(home).transaction() as transaction:
                resolved = service.snapshots.current(spec.dataset_id)
                authorize_publication(resolved, completed.publication.authorization, transaction)
            current = data.query("dataset_release", query)
            assert isinstance(current, TableResponse)
            self.assertEqual(current.page.total_rows, 1)
            self.assertEqual(current.publication_state, "published")
            final_view = service.view(query)
            service.retract(
                RetractionRequest(
                    dataset_id=spec.dataset_id,
                    snapshot_id=final_view.batch.snapshot.snapshot_id,
                    expected=final_view.batch,
                    candidate_id=selected,
                    expected_row_revision=1,
                    reviewer="fixture human",
                )
            )
            stale_release = data.query("dataset_release", query)
            assert isinstance(stale_release, TableResponse)
            self.assertEqual(stale_release.publication_state, "stale")
            self.assertEqual(stale_release.page.total_rows, 1)
            self.assertIsNone(service.view(query).publication)
            self.assertEqual(
                service.download(
                    PublicationTarget(
                        dataset_id=spec.dataset_id,
                        publication_id=completed.publication.publication_id,
                    )
                ),
                union_csv,
            )
