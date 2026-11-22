from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from guarded_labeling_fixture import LabelingFixture
from httpx import AsyncClient, MockTransport
from test_keenable import PLAN
from test_keenable_snippets import SNIPPET_KEY, SNIPPET_URL, KeenableWire, snippet_transport

from codebench_pipeline.dataframe_schemas import model_records
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding import OperationReceiptBuffer
from codebench_pipeline.keenable_policy import KeenableSnippetSettings, KeenableSourceAccess
from codebench_pipeline.keenable_snippets import DiscoveryRun, provider_snippet_candidates
from codebench_pipeline.labeling_stage_settings import KeenableSnippetEvidence
from codebench_pipeline.storage import IntegrityError

REFERENCE = b"Previews Terraform changes using `terraform plan`.\n"


class GroundingAuthorityTests(TestCase):
    def test_snippet_is_candidate_evidence_but_cannot_be_its_own_documentation(self) -> None:
        text = "Checks Terraform configuration using `terraform validate`.\n```bash\nterraform validate\n```"
        settings = KeenableSnippetSettings(seeds=("terraform_validation",))
        scratch = Path(self.enterContext(TemporaryDirectory()))
        wire = KeenableWire([{"url": SNIPPET_URL, "snippet": text}])
        client = AsyncClient(headers={"X-API-Key": SNIPPET_KEY}, transport=MockTransport(wire.respond))
        with patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", return_value=client):
            snippets = provider_snippet_candidates(settings, DiscoveryRun(scratch, snippet_transport()))
        (original,) = model_records(snippets, CandidateRecord)
        row = original.model_copy(
            update={
                "candidate_id": f"keenable:{original.candidate_id}",
                "source_id": f"keenable:{original.source_id}",
            }
        )
        with TemporaryDirectory() as directory:
            fixture = LabelingFixture(self, Path(directory), documentation_corpus=b"# No operation docs\n")
            filtered, runtime = fixture.labeling()
            inputs = runtime.evidence_inputs
            filtered.attrs["branch_metadata"]["keenable"] = snippets.attrs
            evidence = replace(
                inputs,
                collection=inputs.collection.model_copy(
                    update={
                        "keenable_use_cases": KeenableSnippetEvidence(
                            settings_sha256=sha256(settings.model_dump_json().encode()).hexdigest()
                        )
                    }
                ),
            )
            guard = evidence.admission(filtered)
            self.assertEqual(guard.sources.resolve(row).body, text.encode())
            self.assertEqual(guard.source(row)[1], [])
            with self.assertRaises(IntegrityError):
                guard.sources.resolve(row.model_copy(update={"context": "Unreported consequence"}))

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
