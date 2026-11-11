import tempfile
import unittest
from pathlib import Path

from configuration_fixture import write_fixture
from dagster import AssetKey, build_output_context
from httpx import Response
from pydantic import TypeAdapter
from starlette.testclient import TestClient

from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.data_view_models import DatasetList, DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import DecisionChange, ReviewSnapshot
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.review_api_models import (
    CompletionRequest,
    DecisionRequest,
    DecisionResponse,
    EvidenceResponse,
    EvidenceTarget,
    PublicationResponse,
    PublicationTarget,
    RetractionRequest,
    ReviewStatus,
    ReviewViewResponse,
)
from codebench_pipeline.runtime_instance import dataset_instance

ORIGIN = "http://127.0.0.1:3000"


class UnreachableReview:
    def view(self, request: DatasetQueryRequest) -> ReviewViewResponse:
        raise AssertionError("Review backend must not be reached")

    def decide(self, request: DecisionRequest) -> DecisionResponse:
        raise AssertionError("Review backend must not be reached")

    def complete(self, request: CompletionRequest) -> PublicationResponse:
        raise AssertionError("Review backend must not be reached")

    def reopen(self, request: CompletionRequest) -> ReviewStatus:
        raise AssertionError("Review backend must not be reached")

    def retract(self, request: RetractionRequest) -> DecisionResponse:
        raise AssertionError("Review backend must not be reached")

    def continue_review(self, request: CompletionRequest) -> PublicationResponse:
        raise AssertionError("Review backend must not be reached")

    def evidence(self, target: EvidenceTarget) -> EvidenceResponse:
        raise AssertionError("Review backend must not be reached")

    def download(self, target: PublicationTarget) -> bytes:
        raise AssertionError("Review backend must not be reached")


class SQLiteReview(UnreachableReview):
    def __init__(self, home: Path, snapshot: ReviewSnapshot) -> None:
        self.store = HumanReviewStore(home)
        self.snapshot = snapshot
        with self.store.transaction() as tx:
            self.batch = tx.ensure_current_batch(snapshot)

    def decide(self, request: DecisionRequest) -> DecisionResponse:
        with self.store.transaction() as tx:
            decision = tx.save_decision(self.snapshot, request.change)
            return DecisionResponse(
                batch=tx.get_current_batch(request.dataset_id),
                decision=decision,
                counts=tx.counts(request.change.batch_id),
            )

    def complete(self, request: CompletionRequest) -> PublicationResponse:
        with self.store.transaction() as tx:
            tx.complete(self.snapshot, request.expected, request.reviewer)
        raise AssertionError("Publication success is outside this transport test")


class DatasetAPITests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.spec = load_build_config(write_fixture(self.home), self.home)
        DatasetRegistry(self.home).register(self.spec)
        self.instance = self.enterContext(dataset_instance(self.home))
        self.instance.add_dynamic_partitions("dataset_builds", [self.spec.dataset_id])
        self.service = DatasetDataService(self.home, self.instance)
        self.context = "<script>literal evidence</script>" + "full context " * 1000
        frame = validated_frame(
            [
                {
                    "candidate_id": candidate_id,
                    "command": command,
                    "util": "probe",
                    "context": self.context,
                    "source_id": "source",
                    "source_url": "https://example.org/source",
                    "source_position": candidate_id,
                    "transformations": [],
                    "risk_candidates": ["CAT-99.1"],
                }
                for candidate_id, command in (("first", "probe inspect"), ("second", "probe inspect --all"))
            ],
            CommandCandidatesTable,
        )
        self.frame = frame
        with build_output_context(
            asset_key=AssetKey(["datasets", "command_candidates"]),
            partition_key=self.spec.dataset_id,
            asset_partitions_def=DATASET_PARTITIONS,
            instance=self.instance,
        ) as output:
            DataFrameIOManager(storage=self.spec.storage).handle_output(output, frame)
        self.client = self.enterContext(
            TestClient(make_dataset_api(self.service, UnreachableReview(), 3000), base_url=ORIGIN)
        )

    def assert_error(self, response: Response, status: int, code: str) -> None:
        self.assertEqual(response.status_code, status)
        error = TypeAdapter(dict[str, dict[str, str]]).validate_json(response.content)
        self.assertEqual(error["error"]["code"], code)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertNotIn("access-control-allow-origin", response.headers)

    def test_datasets_lists_real_registered_category_without_origin(self) -> None:
        response = self.client.get("/datasets")
        self.assertEqual(response.status_code, 200)
        datasets = DatasetList.model_validate_json(response.content)
        self.assertEqual(tuple(item.dataset_id for item in datasets.datasets), (self.spec.dataset_id,))

    def test_persisted_table_read_returns_full_cells_and_original_positions(self) -> None:
        response = self.client.post(
            "/data/command_candidates",
            headers={"Origin": ORIGIN},
            json={
                "dataset_id": self.spec.dataset_id,
                "query": {"filters": [{"field": "command", "operator": "contains", "value": "--ALL"}]},
            },
        )
        self.assertEqual(response.status_code, 200)
        table = TableResponse.model_validate_json(response.content)
        self.assertEqual((table.page.total_rows, table.page.matched_rows), (2, 1))
        self.assertEqual(table.page.rows[0].row_index, 1)
        self.assertEqual(table.page.rows[0].values["context"], self.context)
        self.assertEqual(self.instance.get_runs(), [])
        self.assertEqual(response.headers["cache-control"], "no-store")

    def test_unknown_fields_filters_and_expressions_are_invalid_requests(self) -> None:
        for extra in (
            {"sql": "SELECT * FROM annotations"},
            {"query": {"filters": [{"field": "missing", "operator": "equals", "value": "x"}]}},
            {"query": {"filters": [{"field": "command", "operator": "regex", "value": ".*"}]}},
            {"query": {"sort": [{"field": "missing"}]}},
            {"query": {"limit": "25"}},
            {"dataset_id": "../other"},
        ):
            with self.subTest(extra=extra):
                response = self.client.post(
                    "/data/command_candidates",
                    headers={"Origin": ORIGIN},
                    json={"dataset_id": self.spec.dataset_id} | extra,
                )
                self.assert_error(response, 422, "invalid_request")

    def test_documentation_paths_read_the_qualified_asset_without_dataset_fallback(self) -> None:
        stage = "documentation/command_candidates"
        frame = self.frame.copy()
        frame["context"] = "documentation " + self.context
        with build_output_context(
            asset_key=AssetKey(["datasets", "documentation", "command_candidates"]),
            partition_key=self.spec.dataset_id,
            asset_partitions_def=DATASET_PARTITIONS,
            instance=self.instance,
        ) as output:
            DataFrameIOManager(storage=self.spec.storage).handle_output(output, frame)
        for path in (stage, stage.replace("/", "%2F")):
            with self.subTest(path=path):
                response = self.client.post(
                    f"/data/{path}",
                    headers={"Origin": ORIGIN},
                    json={"dataset_id": self.spec.dataset_id},
                )
                self.assertEqual(response.status_code, 200)
                table = TableResponse.model_validate_json(response.content)
                self.assertEqual(table.stage, stage)
                self.assertEqual(table.page.total_rows, 2)
                self.assertEqual(table.page.rows[0].values["context"], "documentation " + self.context)
        dataset = TableResponse.model_validate(
            self.service.query("command_candidates", DatasetQueryRequest(dataset_id=self.spec.dataset_id))
        )
        self.assertEqual(dataset.page.rows[0].values["context"], self.context)
        self.assertEqual(self.instance.get_runs(), [])

    def test_unknown_qualified_stages_do_not_resolve_by_leaf(self) -> None:
        for stage in ("documentation/unknown", "other/command_candidates", "documentation/category_rules"):
            with self.subTest(stage=stage):
                response = self.client.post(
                    f"/data/{stage}",
                    headers={"Origin": ORIGIN},
                    json={"dataset_id": self.spec.dataset_id},
                )
                self.assert_error(response, 404, "not_found")

    def test_json_content_type_and_streamed_body_limit_precede_parsing(self) -> None:
        payload = DatasetQueryRequest(dataset_id=self.spec.dataset_id).model_dump_json().encode()
        exact = payload + b" " * (65536 - len(payload))
        response = self.client.post(
            "/data/command_candidates",
            headers={"Origin": ORIGIN, "Content-Type": "application/json; charset=utf-8"},
            content=exact,
        )
        self.assertEqual(response.status_code, 200)
        for content, content_type, status in (
            (iter((exact, b"x")), "application/json", 413),
            (b"{", "application/json", 422),
            (payload, "text/plain", 422),
            (payload, "", 422),
        ):
            with self.subTest(content_type=content_type, status=status):
                response = self.client.post(
                    "/data/command_candidates",
                    headers={"Origin": ORIGIN, "Content-Type": content_type},
                    content=content,
                )
                self.assert_error(response, status, "invalid_request")

    def test_host_and_exact_origin_guards_prevent_review_backend_access(self) -> None:
        for headers in (
            {},
            {"Origin": "http://foreign.example:3000"},
            {"Origin": "null"},
            {"Origin": ORIGIN + "/"},
            {"Origin": "http://localhost:3000"},
            {"Origin": ORIGIN + "@foreign.example"},
            {"Origin": ORIGIN, "Host": "foreign.example:3000"},
            {"Origin": "http://foreign.example:3000", "Host": "foreign.example:3000"},
            {"Origin": ORIGIN, "Host": "127.0.0.1:3001"},
            {"Origin": ORIGIN, "Host": "localhost.evil:3000"},
        ):
            with self.subTest(headers=headers):
                response = self.client.post(
                    "/review", json={"dataset_id": self.spec.dataset_id}, headers=headers
                )
                self.assert_error(response, 403, "forbidden")
        self.assert_error(
            self.client.get("/datasets", headers={"Host": "evil.example:3000"}), 403, "forbidden"
        )

    def test_missing_outputs_and_unexpected_failures_have_safe_error_headers(self) -> None:
        missing = self.client.post(
            "/data/reviewed_annotations",
            headers={"Origin": ORIGIN},
            json={"dataset_id": self.spec.dataset_id},
        )
        self.assert_error(missing, 404, "not_materialized")
        failure = self.client.post(
            "/review", headers={"Origin": ORIGIN}, json={"dataset_id": self.spec.dataset_id}
        )
        self.assert_error(failure, 500, "internal_error")
        self.assertNotIn("AssertionError", failure.text)
        self.assert_error(self.client.get("/unknown"), 404, "not_found")

    def test_sqlite_stale_decision_and_pending_completion_return_conflicts(self) -> None:
        backend = SQLiteReview(
            self.home,
            ReviewSnapshot(
                dataset_id=self.spec.dataset_id,
                snapshot_id="a" * 64,
                candidate_ids=("first", "second"),
                eligible_candidate_ids=("first", "second"),
            ),
        )
        with TestClient(make_dataset_api(self.service, backend, 3000), base_url=ORIGIN) as client:
            change = DecisionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id="a" * 64,
                change=DecisionChange(
                    batch_id=backend.batch.batch_id,
                    generation=1,
                    candidate_id="first",
                    expected_row_revision=0,
                    decision="approve",
                    reviewer="self-reported reviewer",
                ),
            )
            headers = {"Origin": ORIGIN, "Content-Type": "application/json"}
            saved = client.put("/review/decision", headers=headers, content=change.model_dump_json())
            self.assertEqual(saved.status_code, 200)
            decision = DecisionResponse.model_validate_json(saved.content)
            self.assertEqual((decision.decision.row_revision, decision.counts.pending), (1, 1))
            stale = client.put("/review/decision", headers=headers, content=change.model_dump_json())
            self.assert_error(stale, 409, "stale")
            completion = CompletionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id="a" * 64,
                expected=decision.batch,
                reviewer="self-reported reviewer",
            )
            pending = client.post("/review/complete", headers=headers, content=completion.model_dump_json())
            self.assert_error(pending, 409, "pending")
        with backend.store.transaction() as tx:
            self.assertEqual(tx.get_current_batch(self.spec.dataset_id).state, "open")
            self.assertEqual(tx.list_decisions(backend.batch.batch_id)[0].row_revision, 1)
