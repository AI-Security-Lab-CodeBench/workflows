import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from configuration_fixture import write_fixture
from pydantic import TypeAdapter
from test_batch_labeling import batch_config, remote_model, tool_reply
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.batch_labeling import RowEvidence, SubmittedLabels, SubmittedOutcome
from codebench_pipeline.data_view_models import DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_schemas import LabelingAttemptsTable, frame_records, validated_frame
from codebench_pipeline.dataset_cli import execute_build, execute_registered_build
from codebench_pipeline.dataset_config import DatasetFileConfig, PublicationConfig, load_build_config
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.dataset_tables import publication
from codebench_pipeline.inference_capacity import CapacityConfig
from codebench_pipeline.labeling_outcomes import failed_attempt, label_attempt
from codebench_pipeline.labeling_tables import failures_from_attempts, proposed_from_attempts
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
                            (row.values["command"], row.values["failure_code"], row.values["label_json"])
                            for row in failures.page.rows
                        ],
                        [("git status --show-stash", "grounding_missing", "")],
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
            [attempt.model_dump(mode="json") for attempt in attempts], LabelingAttemptsTable
        )
        proposals = proposed_from_attempts(table, rules)
        failures = failures_from_attempts(table)
        released = publication(proposals, rules, PublicationConfig())
        self.assertEqual(proposals["candidate_id"].tolist(), ["valid"])
        self.assertEqual(failures["candidate_id"].tolist(), ["unsupported"])
        self.assertEqual(frame_records(failures)[0]["command"], unsupported.command)
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
        self.assertEqual(failures["label_json"].tolist(), [""])
