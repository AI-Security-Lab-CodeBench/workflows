from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from guarded_labeling_fixture import (
    CORPUS,
    DOCS,
    REVIEWED_SCOPE,
    UNRELATED_SCOPE,
    LabelingFixture,
    historical_support,
)

from codebench_pipeline.dataframe_schemas import model_records
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding import OperationReceiptBuffer
from codebench_pipeline.grounding_inventory import DocumentationCapture
from codebench_pipeline.inference_capacity import CapacityUnavailable, guarded_complete
from codebench_pipeline.labeling_outcomes import LabelingAttempt
from codebench_pipeline.storage import IntegrityError

SCOPE_COMMAND = "kubectl get pods --all-namespaces"


class EvidenceAdmissionTests(TestCase):
    def test_shared_index_reuses_verified_template_across_operands_but_not_flags_or_operations(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        permitted = (
            "kubectl get pods invoice-a --namespace=team-a",
            "kubectl get pods invoice-b --namespace=team-b",
        )
        denied = (
            "kubectl get pods invoice-a --all-namespaces",
            "kubectl get pods invoice-a --namespace=team-a stray-word",
            "kubectl get deployments invoice-a --namespace=team-a",
            "kubectl delete pods invoice-a --namespace=team-a",
        )
        template = b"`kubectl get pods <pod-name> --namespace=<namespace>` lists a named pod in the selected namespace."
        fixture = LabelingFixture(
            self,
            home,
            corpus=b"\n\n".join(b"```bash\n" + cmd.encode() + b"\n```" for cmd in (*permitted, *denied)),
            documentation_corpus=DOCS + b"\n" + template + b"\n",
        )
        filtered, labeling = fixture.labeling()
        rows = {row.command: row for row in model_records(filtered, CandidateRecord)}
        guard = labeling.evidence_inputs.admission(filtered, operation_receipts=OperationReceiptBuffer())
        read = DocumentationCapture.read
        with patch.object(DocumentationCapture, "read", autospec=True, side_effect=read) as reads:
            for command in permitted:
                with self.subTest(allowed=command):
                    _, items = guard.source(rows[command])
                    (item,) = items
                    self.assertEqual(item.snippet.snippet, template.decode())
                    guard.verify(rows[command], item, template.decode())
            for command in denied:
                with self.subTest(denied=command):
                    self.assertEqual(guard.source(rows[command])[1], [])
            reads.assert_called_once()

    def test_eligible_same_source_passage_precedes_external_inventory_reads(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, corpus=CORPUS)
        filtered, labeling = fixture.labeling()
        candidate = next(
            row for row in model_records(filtered, CandidateRecord) if row.command == "kubectl get pods"
        )
        guard = labeling.evidence_inputs.admission(filtered, operation_receipts=OperationReceiptBuffer())
        with patch.object(
            DocumentationCapture, "read", side_effect=AssertionError("external inventory read")
        ):
            _, items = guard.source(candidate)
            (item,) = items
            self.assertEqual(item.snippet.url, candidate.source_url)
            guard.verify(candidate, item, item.snippet.snippet)

    def test_historical_shared_capture_support_revalidates_without_network(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        documentation = DOCS + b"\n`kubectl get pods` lists pods in the selected namespace.\n"
        fixture = LabelingFixture(
            self,
            home,
            corpus=b"```bash\nkubectl get pods\n```\n\n```bash\nkubectl get pods --all-namespaces\n```\n",
            documentation_corpus=documentation,
        )
        filtered, labeling = fixture.labeling()
        candidates = {row.command: row for row in model_records(filtered, CandidateRecord)}
        with (
            patch("codebench_pipeline.keenable.request_direct", side_effect=AssertionError("recrawl")),
            patch(
                "codebench_pipeline.grounding_tools.GroundingBroker.execute",
                side_effect=AssertionError("provider call"),
            ),
        ):
            guard, items = historical_support(
                labeling.evidence_inputs, filtered, candidates["kubectl get pods"]
            )
            (item,) = items
            self.assertIn("lists pods in the selected namespace", item.snippet.snippet)
            guard.verify(candidates["kubectl get pods"], item, item.snippet.snippet)
            self.assertEqual(guard.source(candidates[SCOPE_COMMAND])[1], [])
        fixture.inference.assert_not_called()
        (orig,) = [
            path for path in home.glob("repository-v2/*/blobs/*") if path.read_bytes() == documentation
        ]
        orig.write_bytes(b"changed")
        with self.assertRaisesRegex(IntegrityError, "capture digest mismatch"):
            labeling.evidence_inputs.admission(filtered).verify(
                candidates["kubectl get pods"], item, item.snippet.snippet
            )

    def test_unrelated_adjacent_prose_does_not_admit_scope_changing_flag(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, corpus=UNRELATED_SCOPE)
        filtered, labeling = fixture.labeling()
        rows = {row.command: row for row in model_records(filtered, CandidateRecord)}
        row = rows[SCOPE_COMMAND]
        self.assertIn("This section uses a demo cluster.", row.context)
        self.assertEqual(labeling.evidence_inputs.admission(filtered).source(row)[1], [])
        self.assertNotIn(row.candidate_id, fixture.remote_rows)

    def test_exact_scope_passage_must_still_have_its_persisted_admission_receipt(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, corpus=REVIEWED_SCOPE)
        filtered, labeling = fixture.labeling()
        row = next(row for row in model_records(filtered, CandidateRecord) if row.command == SCOPE_COMMAND)
        guard, items = historical_support(labeling.evidence_inputs, filtered, row)
        (item,) = items
        self.assertIn("lists pods across all namespaces", item.snippet.snippet)
        guard.verify(row, item, item.snippet.snippet)

        for receipt in home.glob("label-grounding-v2/*/shared-operations/*.json"):
            receipt.unlink()
        with self.assertRaises(FileNotFoundError):
            guard.verify(row, item, item.snippet.snippet)

    def test_legacy_source_permissions_do_not_block_live_grounding(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, source_limited=False)
        filtered, labeling = fixture.labeling()
        with patch(
            "codebench_pipeline.grounding_tools.GroundingBroker.execute",
            side_effect=AssertionError("legacy provider call"),
        ):
            result = labeling.attempts(filtered, home / "attempts")
        rows = model_records(result, LabelingAttempt)
        self.assertEqual(len(rows), 4)
        self.assertEqual({row.status for row in rows}, {"success"})
        self.assertCountEqual(fixture.remote_rows, [row.candidate_id for row in rows])

    def test_actual_capacity_guard_never_posts_without_telemetry(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, corpus=REVIEWED_SCOPE)
        filtered, labeling = fixture.labeling()
        fixture.inference.side_effect = guarded_complete
        with (
            patch("codebench_pipeline.inference_capacity._get", return_value=b"{}") as telemetry,
            patch("codebench_pipeline.inference_capacity._tokenize") as tokenize,
            patch("codebench_pipeline.inference_capacity.complete") as submit,
            self.assertRaises(CapacityUnavailable),
        ):
            labeling.attempts(filtered, home / "attempts")
        self.assertTrue(telemetry.call_args.args[0].endswith("/get_server_info"))
        tokenize.assert_not_called()
        submit.assert_not_called()
