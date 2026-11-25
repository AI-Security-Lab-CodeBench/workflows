import csv
import io
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from pydantic import JsonValue
from test_batch_labeling import tool_reply
from test_dataset_rules import make_rules
from test_labeling_tables import unsupported_plan_reply
from test_review_release import attest_fixture_release

from codebench_pipeline.batch_labeling_protocol import SubmittedLabel
from codebench_pipeline.data_view_models import DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import refresh_registered_configuration
from codebench_pipeline.human_review import DecisionChange, HumanReviewError
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.release_runner import ReleaseRunner
from codebench_pipeline.review_api_models import CompletionRequest, DecisionRequest, PublicationTarget
from codebench_pipeline.review_service import ReviewService
from codebench_pipeline.runtime_instance import dataset_instance


def fixture_reply(name: str, arguments: dict[str, JsonValue]) -> HTTPResponse:
    if name != "finalize":
        return tool_reply(name, arguments)
    submission = SubmittedLabel.model_validate(arguments)
    row = submission.outcome
    label = row.label
    assert label is not None
    if label.util == "terraform":
        return unsupported_plan_reply(name, arguments)
    if label.util == "kubectl":
        label = label.model_copy(update={"risk_id": "CAT-3.2"})
    elif any("docker compose ps" in quote for quote in row.grounding_quotes.values()):
        label = label.model_copy(update={"risk_confidence": "medium"})
    else:
        label = label.model_copy(update={"decision": "reject", "risk_id": "other"})
    return tool_reply(
        "finalize",
        {"outcome": row.model_copy(update={"label": label}).model_dump(mode="json", by_alias=True)},
    )


class AdvisoryRiskReleaseTests(TestCase):
    def test_release_preserves_resolved_risk_and_excludes_grounded_other(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, native=True)
        second = make_rules("CAT-3").risks[1]
        rules = fixture.rules.model_copy(update={"risks": [*fixture.rules.risks, second]})
        spec = fixture.spec.model_copy(update={"rules": rules})
        DatasetRegistry(home).register(spec)
        refresh_registered_configuration(spec.dataset_id, home)
        instance = self.enterContext(dataset_instance(home))
        with patch("guarded_labeling_fixture.tool_reply", side_effect=fixture_reply):
            NativeDatasetCycle(home).drain(instance, partition=spec.dataset_id)
        data = DatasetDataService(home, instance)
        query = DatasetQueryRequest(dataset_id=spec.dataset_id)
        proposals = data.query("proposed_annotations", query)
        failures = data.query("labeling_failures", query)
        assert isinstance(proposals, TableResponse) and isinstance(failures, TableResponse)
        self.assertEqual(
            {(r.values["command"], r.values["risk_id"], r.values["decision"]) for r in proposals.page.rows},
            {
                ("kubectl get pods", second.id, "accept"),
                ("docker ps --all", "other", "reject"),
                ("docker compose ps", "CAT-3.1", "accept"),
            },
        )
        for row in proposals.page.rows:
            self.assertEqual(row.values["risk_candidates"], ["CAT-3.1"])
        self.assertEqual([r.values["failure_code"] for r in failures.page.rows], ["grounding_missing"])
        service = ReviewService(home, data, ReleaseRunner(home, instance))
        view = service.view(query)
        (review,) = view.page.rows
        self.assertEqual(review.values["command"], "docker compose ps")
        service.decide(
            DecisionRequest(
                dataset_id=spec.dataset_id,
                snapshot_id=view.batch.snapshot.snapshot_id,
                change=DecisionChange(
                    batch_id=view.batch.batch_id,
                    generation=view.batch.generation,
                    expected_row_revision=0,
                    candidate_id=review.review_record_id,
                    decision="approve",
                    reviewer="fixture reviewer",
                    edits={"risk_id": second.id},
                ),
            )
        )
        batch = service.view(query).batch
        with self.assertRaises(HumanReviewError) as pending:
            service.complete(
                CompletionRequest(
                    dataset_id=spec.dataset_id,
                    snapshot_id=batch.snapshot.snapshot_id,
                    expected=batch,
                    reviewer="fixture reviewer",
                )
            )
        self.assertEqual(pending.exception.code, "pending")
        attest_fixture_release(home, instance, spec.dataset_id)
        batch = service.view(query).batch
        release = service.continue_review(
            CompletionRequest(
                dataset_id=spec.dataset_id,
                snapshot_id=batch.snapshot.snapshot_id,
                expected=batch,
                reviewer="fixture reviewer",
            )
        )
        assert release.publication is not None
        payload = service.download(
            PublicationTarget(
                dataset_id=spec.dataset_id,
                publication_id=release.publication.publication_id,
            )
        )
        self.assertEqual(
            {
                (r["command"], r["risk"], r["human_reviewed"])
                for r in csv.DictReader(io.StringIO(payload.decode()))
            },
            {
                ("kubectl get pods", second.canonical, "false"),
                ("docker compose ps", second.canonical, "true"),
            },
        )
