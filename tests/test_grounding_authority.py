from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from guarded_labeling_fixture import LabelingFixture
from test_keenable import PLAN

from codebench_pipeline.dataframe_schemas import model_records
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding import OperationReceiptBuffer
from codebench_pipeline.keenable_policy import KeenableSourceAccess

REFERENCE = b"Previews Terraform changes using `terraform plan`.\n"


class GroundingAuthorityTests(TestCase):
    def test_retained_candidate_uses_only_passages_from_approved_origins(self) -> None:
        for reference in (b"# No matching documentation\n", REFERENCE):
            with self.subTest(reference=reference), TemporaryDirectory() as directory:
                fixture = LabelingFixture(self, Path(directory), documentation_corpus=reference)
                filtered, runtime = fixture.labeling()
                candidate = next(
                    row for row in model_records(filtered, CandidateRecord) if row.source_url == PLAN
                )
                evidence = runtime.evidence_inputs
                guard = evidence.admission(filtered)
                _, (formerly_approved,) = guard.source(candidate)
                self.assertEqual(formerly_approved.snippet.url, PLAN)
                retained = guard.sources.resolve(candidate)
                inventory = guard.shared_operations
                assert inventory is not None
                self.assertIn(PLAN, {item.url for item in inventory.operation(("terraform", "plan"))})

                alternate = evidence.admission(filtered, operation_receipts=OperationReceiptBuffer())
                with (
                    patch.object(KeenableSourceAccess, "authority", return_value=None),
                    patch.object(alternate.sources.provider, "read_body", return_value=retained.body),
                ):
                    _, admitted = alternate.source(candidate)
                    inventory = alternate.shared_operations
                    assert inventory is not None
                    self.assertEqual(
                        [item.quote for item in inventory.operation(("terraform", "plan"))],
                        [REFERENCE.decode().strip()] if reference == REFERENCE else [],
                    )
                    self.assertEqual(
                        [item.snippet.snippet for item in admitted],
                        [REFERENCE.decode().strip()] if reference == REFERENCE else [],
                    )
                    for item in admitted:
                        alternate.verify(candidate, item, item.snippet.snippet)
                    with self.assertRaisesRegex(ValueError, "current source-level operation passage"):
                        alternate.verify(candidate, formerly_approved, formerly_approved.snippet.snippet)
