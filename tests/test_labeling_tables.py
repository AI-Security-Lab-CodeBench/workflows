import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pyarrow
import pyarrow.parquet as parquet
from configuration_fixture import write_fixture
from pandera.errors import SchemaError, SchemaErrors
from pydantic import JsonValue, TypeAdapter
from test_batch_labeling import batch_config, remote_model, tool_reply
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.batch_labeling import RowEvidence, SubmittedLabels, SubmittedOutcome
from codebench_pipeline.data_view_models import DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager, StorageConfig, frame_bytes, read_frame
from codebench_pipeline.dataframe_schemas import (
    LabelingAttemptsTable,
    frame_records,
    model_records,
    validated_frame,
)
from codebench_pipeline.dataset_cli import execute_build, execute_registered_build
from codebench_pipeline.dataset_config import DatasetFileConfig, PublicationConfig, load_build_config
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.dataset_tables import publication
from codebench_pipeline.inference_capacity import CapacityConfig
from codebench_pipeline.labeling_outcomes import LabelingAttempt, failed_attempt, label_attempt
from codebench_pipeline.labeling_tables import failures_from_attempts, proposed_from_attempts
from codebench_pipeline.models import SemanticSnippet
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.runtime_instance import dataset_instance


class LabelingTableTests(unittest.TestCase):
    def test_materialized_failure_is_persisted_but_excluded_from_proposals_and_release(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            path = write_fixture(home, "CAT-99")
            config = DatasetFileConfig.model_validate_json(path.read_bytes())
            config = config.model_copy(
                update={
                    "models": config.models.model_copy(update={"labeler": remote_model()}),
                    "labeling_batch": batch_config(home),
                }
            )
            path.write_text(config.model_dump_json(), encoding="utf-8")
            spec = load_build_config(path, home)

            def provider(
                base_url: str,
                body: bytes,
                key: str | None,
                timeout: float,
                config: CapacityConfig,
                out: Path,
            ) -> HTTPResponse:
                payload = json.loads(json.loads(body)["messages"][1]["content"])
                outcomes: list[SubmittedOutcome] = []
                for row in payload["records"]:
                    candidate = CandidateRecord.model_validate(row["candidate"])
                    failed = candidate.command == "git status --show-stash"
                    evidence = (
                        [
                            RowEvidence.model_validate(payload["documentation"][identifier])
                            for identifier in row["evidence_ids"]
                        ]
                        if not failed
                        else []
                    )
                    label = (
                        None
                        if failed
                        else make_label(candidate, spec.rules).model_copy(
                            update={"documentation_urls": [item.snippet.url for item in evidence]}
                        )
                    )
                    outcomes.append(
                        SubmittedOutcome(
                            candidate_id=candidate.candidate_id,
                            row_input_fingerprint=TypeAdapter(str).validate_python(
                                row["row_input_fingerprint"]
                            ),
                            label=label,
                            evidence_ids=[item.evidence_id for item in evidence],
                            grounding_quotes={item.evidence_id: item.snippet.snippet for item in evidence},
                            grounding_rationale=""
                            if failed
                            else "The supplied snippet documents inspection.",
                            failure_code="grounding_missing" if failed else "",
                            failure_detail="Stash count semantics remain unsupported." if failed else "",
                        )
                    )
                return tool_reply("submit_labels", SubmittedLabels(outcomes=outcomes).model_dump(mode="json"))

            with patch(
                "codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider
            ) as inference:
                with redirect_stdout(io.StringIO()):
                    built = execute_build(path, home)
                for stage in ("labeling_failures", "proposed_annotations", "dataset_release"):
                    self.assertIn(stage, built)
                self.assertEqual(inference.call_count, 1)
                with dataset_instance(home) as instance:
                    data = DatasetDataService(home, instance)
                    query = DatasetQueryRequest(dataset_id=spec.dataset_id)
                    failures = data.query("labeling_failures", query)
                    proposals = data.query("proposed_annotations", query)
                    released = data.query("dataset_release", query)
                    assert isinstance(failures, TableResponse)
                    assert isinstance(proposals, TableResponse)
                    assert isinstance(released, TableResponse)
                    self.assertEqual(failures.page.total_rows, 1)
                    self.assertEqual(
                        [
                            (row.values["command"], row.values["failure_code"], row.values["label"])
                            for row in failures.page.rows
                        ],
                        [("git status --show-stash", "grounding_missing", None)],
                    )
                    self.assertEqual(proposals.page.total_rows, 1)
                    self.assertEqual([row.values["command"] for row in proposals.page.rows], ["git status"])
                    self.assertEqual(released.publication_state, "published")
                    self.assertEqual(released.page.total_rows, 1)
                    self.assertEqual(
                        [(row.values["command"], row.values["human_reviewed"]) for row in released.page.rows],
                        [("git status", "false")],
                    )
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(execute_registered_build(spec.dataset_id, home), {})
                self.assertEqual(inference.call_count, 1)

    def test_failed_row_is_inspectable_but_cannot_become_a_released_command(self) -> None:
        rules = make_rules()
        valid, unsupported = make_candidate("valid"), make_candidate("unsupported")
        attempts = [
            label_attempt(valid, make_label(valid, rules), documentation(), "a" * 64, "label/valid"),
            failed_attempt(
                unsupported, "grounding_missing", "No matching evidence.", [], "b" * 64, "label/failed"
            ),
        ]
        table = validated_frame(
            [
                {**attempt.model_dump(mode="json"), "source_weight": weight}
                for attempt, weight in zip(attempts, [0.7, 0.2], strict=True)
            ],
            LabelingAttemptsTable,
        )
        proposals = proposed_from_attempts(table, rules)
        failures = failures_from_attempts(table)
        released = publication(proposals, rules, PublicationConfig())
        self.assertEqual(proposals["candidate_id"].tolist(), ["valid"])
        self.assertEqual(failures["candidate_id"].tolist(), ["unsupported"])
        self.assertEqual(frame_records(failures)[0]["command"], unsupported.command)
        self.assertEqual(proposals["source_weight"].tolist(), [0.7])
        self.assertEqual(failures["source_weight"].tolist(), [0.2])
        self.assertEqual(len(released.commands), 1)
        self.assertEqual(proposals.attrs["row_documentation"]["valid"][0]["url"], documentation()[0].url)

    def test_all_failed_attempts_preserve_empty_success_schema_and_failure_reasons(self) -> None:
        candidate = make_candidate("unsupported")
        attempt = failed_attempt(
            candidate, "tool_budget_exhausted", "Still unsupported.", [], "a" * 64, "label/1"
        )
        table = validated_frame([attempt.model_dump(mode="json")], LabelingAttemptsTable)
        proposals = proposed_from_attempts(table, make_rules())
        failures = failures_from_attempts(table)
        self.assertEqual(proposals["candidate_id"].tolist(), [])
        self.assertIn("class", proposals.columns)
        self.assertEqual(failures["failure_code"].tolist(), ["tool_budget_exhausted"])
        self.assertEqual(failures["label"].tolist(), [None])

    def test_nested_attempt_types_survive_parquet_with_no_successes_or_rows(self) -> None:
        candidate = make_candidate("one")
        docs = documentation()
        success = label_attempt(candidate, make_label(candidate, make_rules()), docs, "a" * 64, "label/1")
        failure = failed_attempt(candidate, "missing", "No docs.", [], "b" * 64, "label/2")
        for attempts in ([success], [failure], []):
            with self.subTest(attempts=attempts):
                frame = validated_frame(
                    [attempt.model_dump(mode="json") for attempt in attempts], LabelingAttemptsTable
                )
                frame.attrs["provenance"] = {"fixture": "labeling"}
                payload = frame_bytes(frame)
                physical = parquet.read_schema(BytesIO(payload))
                self.assertIsInstance(physical.field("label").type, pyarrow.StructType)
                excerpts = physical.field("documentation").type
                assert isinstance(excerpts, pyarrow.ListType)
                self.assertIsInstance(excerpts.value_type, pyarrow.StructType)
                restored = read_frame(payload)
                self.assertEqual(
                    model_records(LabelingAttemptsTable.validate(restored), LabelingAttempt), attempts
                )
                self.assertEqual(restored.attrs, frame.attrs)
                self.assertEqual(frame_bytes(restored), payload)

    def test_historical_attempt_is_typed_on_read_without_rewriting_stored_evidence(self) -> None:
        candidate = make_candidate("one")
        docs = documentation()
        label = make_label(candidate, make_rules())
        expected = label_attempt(candidate, label, docs, "a" * 64, "label/1")
        historical = pd.DataFrame(
            [
                {
                    **expected.model_dump(mode="json", exclude={"label", "documentation"}),
                    "label_json": label.model_dump_json(by_alias=True),
                    "documentation_json": TypeAdapter(list[SemanticSnippet]).dump_json(docs).decode(),
                    "source_weight": 0.123456789012345,
                }
            ]
        )
        historical.attrs["provenance"] = {"receipt": "original"}
        with tempfile.TemporaryDirectory() as root:
            manager = DataFrameIOManager(storage=StorageConfig(local_root=root))
            reference = manager.persist_value(historical)
            store = ObjectStore(manager.storage)
            key = f"objects/{reference.digest}.parquet"
            original = store.read(key)
            restored = manager.read_version(reference)
            assert isinstance(restored, pd.DataFrame)
            self.assertEqual(model_records(restored, LabelingAttempt), [expected])
            self.assertEqual(restored["source_weight"].tolist(), [0.123456789012345])
            self.assertEqual(restored.attrs, historical.attrs)
            self.assertEqual(store.read(key), original)
            self.assertIn("documentation_json", read_frame(original))

    def test_attempt_table_rejects_unknown_fields_before_storage_conversion(self) -> None:
        candidate = make_candidate("one")
        label, docs = make_label(candidate, make_rules()), documentation()
        attempt = label_attempt(candidate, label, docs, "a" * 64, "label/1")
        row = attempt.model_dump(mode="json")
        invalid_rows: tuple[dict[str, JsonValue], ...] = (
            {**row, "unexpected": True},
            {**row, "label": {**label.model_dump(mode="json"), "unexpected": True}},
            {**row, "documentation": [{**docs[0].model_dump(mode="json"), "unexpected": True}]},
        )
        for invalid in invalid_rows:
            with self.subTest(invalid=invalid), self.assertRaises((SchemaError, SchemaErrors)):
                validated_frame([invalid], LabelingAttemptsTable)
