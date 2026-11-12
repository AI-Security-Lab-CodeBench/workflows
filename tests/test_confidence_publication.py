import csv
import io
import json
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

from configuration_fixture import write_fixture
from pydantic import BaseModel, JsonValue, TypeAdapter
from test_dataset_incremental import provider_response
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.data_view_models import DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_cli import execute_build, execute_registered_build
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_export import prepare_dataset
from codebench_pipeline.dataset_records import LabelDecision
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
from codebench_pipeline.review_snapshots import ReviewSnapshots
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
                if any(
                    flag in payload["candidate"]["command"]
                    for flag in ("-auto-approve", "-compact-warnings")
                )
                else "high"
            }
        )
    return result


def high_confidence_response(
    schema: type[BaseModel], system: str, prompt: str, model: ModelConfig, out: Path
) -> BaseModel:
    result = provider_response(schema, system, prompt, model, out)
    if isinstance(result, LabelDecision):
        return result.model_copy(update={"category_confidence": "high"})
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
        ):
            for confidence in ("low", "medium", "high"):
                with self.subTest(field=field, confidence=confidence):
                    label = make_label(candidate, rules)
                    label = label.model_copy(update={field: confidence})
                    pending = prepare_dataset([candidate], [label], rules, 250)
                    if confidence == "high":
                        self.assertEqual(
                            [(row["command"], row["human_reviewed"]) for row in pending.commands],
                            [(candidate.command, "false")],
                        )
                        self.assertEqual(pending.human_review_ids, ())
                    else:
                        self.assertEqual(pending.commands, [])
                        self.assertEqual(pending.human_review_ids, (candidate.candidate_id,))
                    accepted = prepare_dataset(
                        [candidate],
                        [label],
                        rules,
                        250,
                        approved_ids=frozenset({candidate.candidate_id}),
                    )
                    self.assertEqual(
                        [row["human_reviewed"] for row in accepted.commands],
                        ["false" if confidence == "high" else "true"],
                    )

    def test_confident_rejection_is_neither_automatic_inclusion_nor_uncertain_review(self) -> None:
        rules = make_rules()
        candidate = make_candidate()
        publication = prepare_dataset(
            [candidate],
            [make_label(candidate, rules).model_copy(update={"decision": "reject"})],
            rules,
            250,
        )
        self.assertEqual(publication.commands, [])
        self.assertEqual(publication.human_review_ids, ())
        self.assertEqual(publication.eligible_ids, ())

    def test_human_approval_cannot_admit_machine_ineligible_uncertain_labels(self) -> None:
        rules = make_rules()
        candidate = make_candidate()
        for decision in ("reject", "needs_review"):
            with self.subTest(decision=decision):
                label = make_label(candidate, rules).model_copy(
                    update={"decision": decision, "class_confidence": "medium"}
                )
                publication = prepare_dataset(
                    [candidate],
                    [label],
                    rules,
                    250,
                    approved_ids=frozenset({candidate.candidate_id}),
                )
                self.assertEqual(publication.human_review_ids, (candidate.candidate_id,))
                self.assertEqual(publication.eligible_ids, ())
                self.assertEqual(publication.commands, [])
                self.assertEqual(publication.pending[0]["human_reviewed"], "false")

    def test_only_uncertain_rows_wait_for_human_review_and_retraction_stales_release(self) -> None:
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
            for module in ("dataset_annotation",):
                stack.enter_context(
                    patch(f"codebench_pipeline.{module}.infer_typed", side_effect=mixed_confidence_response)
                )
            with redirect_stdout(io.StringIO()):
                first = execute_build(path, home)
            self.assertIn("source_documents", first)
            self.assertNotIn("human_source_decisions", first)
            self.assertNotIn("automatic_dataset_release", first)
            spec = load_build_config(path, home)
            with redirect_stdout(io.StringIO()):
                prepared = execute_registered_build(spec.dataset_id, home)
            self.assertIn("human_review_queue", first)
            self.assertEqual(prepared, {})
            self.assertNotIn("automatic_dataset_release", prepared)
            instance = stack.enter_context(dataset_instance(home))
            data = DatasetDataService(home, instance)
            service = ReviewService(home, data, ReleaseRunner(home, instance))
            query = DatasetQueryRequest(dataset_id=spec.dataset_id)
            view = service.view(query)
            self.assertEqual(view.counts.pending, 2)
            self.assertEqual(
                {str(row.values["command"]) for row in view.page.rows},
                {
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
                {
                    ("terraform destroy", "false"),
                    ("terraform destroy -compact-warnings", "true"),
                },
            )
            with HumanReviewStore(home).transaction() as transaction:
                resolved = service.snapshots.current(spec.dataset_id)
                authorize_publication(resolved, completed.publication.authorization, transaction)
            current = data.query("dataset_release", query)
            assert isinstance(current, TableResponse)
            self.assertEqual(current.page.total_rows, 2)
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
            self.assertEqual(stale_release.page.total_rows, 2)
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

    def test_all_high_confidence_build_publishes_without_human_decisions_and_reuses_release(self) -> None:
        with ExitStack() as stack:
            home = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            config = write_fixture(home)
            for module in ("dataset_annotation",):
                stack.enter_context(
                    patch(f"codebench_pipeline.{module}.infer_typed", side_effect=high_confidence_response)
                )
            with redirect_stdout(io.StringIO()):
                built = execute_build(config, home)
            self.assertIn("dataset_release", built)
            spec = load_build_config(config, home)
            instance = stack.enter_context(dataset_instance(home))
            data = DatasetDataService(home, instance)
            query = DatasetQueryRequest(dataset_id=spec.dataset_id)
            released = data.query("dataset_release", query)
            assert isinstance(released, TableResponse)
            self.assertEqual(released.publication_state, "published")
            self.assertEqual(
                {(row.values["command"], row.values["human_reviewed"]) for row in released.page.rows},
                {("git status", "false"), ("git status --show-stash", "false")},
            )
            resolved = ReviewSnapshots(home, data).current(spec.dataset_id)
            self.assertNotIn("reviewed_annotations", resolved.manifest.references)
            label_stage = "documentation/proposed_annotations"
            metadata = TypeAdapter(dict[str, JsonValue]).validate_python(
                resolved.manifest.evidence[label_stage]
            )
            receipts = TypeAdapter(dict[str, str]).validate_python(metadata["receipts"])
            labeled = data.query(label_stage, query)
            assert isinstance(labeled, TableResponse)
            identities = [
                TypeAdapter(str).validate_python(row.values["candidate_id"]) for row in labeled.page.rows
            ]
            self.assertEqual(
                set(receipts),
                {
                    "label-packets.json",
                    *(
                        f"label/{sha256(identity.encode()).hexdigest()}/reuse.json"
                        for identity in identities
                    ),
                },
            )
            exported = [
                json.loads(line)
                for line in resolved.prepared.files["review/reviewed.jsonl"].splitlines()
            ]
            self.assertEqual(
                [set(row) & {"review", "review_confidence", "review_verdict"} for row in exported],
                [set(), set()],
            )
            with HumanReviewStore(home).reader() as transaction:
                batch = transaction.get_current_batch(spec.dataset_id, "annotations")
                self.assertEqual(batch.snapshot.candidate_ids, ())
                self.assertEqual(transaction.list_decisions(batch.batch_id), ())
            before = len(instance.get_runs())
            with redirect_stdout(io.StringIO()):
                self.assertEqual(execute_registered_build(spec.dataset_id, home), {})
            self.assertEqual(len(instance.get_runs()), before)
