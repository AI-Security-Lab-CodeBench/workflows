import json
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from configuration_fixture import seed_prepared_snapshot
from guarded_labeling_fixture import CORPUS, LOOKUP_BODY, LabelingFixture
from pydantic import TypeAdapter

from codebench_pipeline.batch_labeling_rows import RowEvidence
from codebench_pipeline.dataframe_schemas import model_records
from codebench_pipeline.dataset_config import require_guarded_annotation
from codebench_pipeline.dataset_documentation_config import DocumentationConfig
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.grounding import GuardedEvidence, OperationReceipt, admitted_excerpt
from codebench_pipeline.grounding_passages import Applicability, operation_passages
from codebench_pipeline.grounding_policy import OperationGrant
from codebench_pipeline.grounding_sources import CandidateCapture, CandidateEvidence
from codebench_pipeline.labeling_outcomes import LabelingAttempt
from codebench_pipeline.storage import IntegrityError, fingerprint

RESTRICTED_REFERENCE = (
    b"<h2>Listing pods</h2>"
    b"<p>All examples in this section require Kubernetes 1.20 or earlier.</p>"
    b"<p>The examples below operate on the selected namespace.</p>"
    b"<p>Lists pods using <code>kubectl get pods</code>.</p>"
)
PARENT = (
    b"<section><h2>Listing pods</h2>\n"
    b"<p>All examples in this section require Kubernetes 1.20 or earlier.</p>\n"
    b"<p>The examples below operate on the selected namespace.</p>\n"
)
DETAILS = b"<section><h2>Detailed examples</h2>\n"
EFFECT = b"<p>Lists pods using <code>kubectl get pods</code>.</p>"
NESTED = PARENT + DETAILS + EFFECT + b"\n</section></section>"
SIBLING = PARENT + b"</section>" + DETAILS + EFFECT + b"\n</section>"


class GroundingVersionTests(TestCase):
    def test_html_container_bounds_take_precedence_over_heading_rank(self) -> None:
        for body in (
            NESTED,
            SIBLING.removesuffix(b"</section>"),
            SIBLING.replace(b"</section><section>", b"</article><section>", 1),
            SIBLING.replace(b"<section><h2>Detailed", b"<section/><h2>Detailed", 1),
        ):
            with self.subTest(body=body):
                self.assertEqual(tuple(operation_passages(body)), ())
        (passage,) = operation_passages(SIBLING)
        start = SIBLING.index(EFFECT)
        self.assertEqual((passage.start, passage.end), (start, start + len(EFFECT)))
        self.assertEqual(SIBLING[passage.start : passage.end], EFFECT)

    def test_genuine_html_sibling_support_survives_inference_cache_and_release(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(
            self,
            home,
            corpus=b"```bash\nkubectl get pods\n```\n",
            documentation_corpus=SIBLING,
        )
        filtered, runtime = fixture.labeling()
        pod = filtered.loc[filtered["command"] == "kubectl get pods"].copy()
        fresh = runtime.attempts(pod, home / "fresh")
        (accepted,) = model_records(fresh, LabelingAttempt)
        self.assertEqual(accepted.status, "success")
        self.assertTrue(any(EFFECT.decode() in doc.snippet for doc in accepted.documentation))
        fixture.inference.assert_called_once()
        fixture.inference.reset_mock()
        replay = runtime.attempts(pod, home / "replay")
        verified = runtime.evidence_inputs.verify(replay, selected=frozenset({accepted.candidate_id}))
        branch, _, original_id = accepted.candidate_id.partition(":")
        self.assertEqual(branch, "dataset")
        receipt_row = verified.sources.repositories[branch].rows[original_id]
        collection = fixture.spec.declared_collection
        assert collection is not None
        origin = next(
            item
            for item in collection.repository_config("dataset").origins("dataset")
            if item.origin_id == receipt_row.origin_id
        )
        self.assertEqual(
            verified.sources.resolve(accepted).source_binding,
            fingerprint(
                {"source": receipt_row.model_dump(mode="json"), "origin": origin.model_dump(mode="json")}
            ),
        )
        fixture.inference.assert_not_called()

    def test_documentation_origin_binding_rejects_bytes_at_a_valid_address(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home)
        filtered, runtime = fixture.labeling()
        inputs = runtime.evidence_inputs
        origin = inputs.collection.documentation_crawl.origins[0]
        binding = inputs.run.root / "origin-bindings" / f"{origin.origin_sha256}.json"
        binding.write_bytes(b'{"repository":"unapproved/replacement"}')

        with self.assertRaisesRegex(IntegrityError, "origin binding bytes differ from the authorized digest"):
            inputs.admission(filtered)
        fixture.inference.assert_not_called()

    def test_shared_operation_verification_rejects_linked_receipt_directory(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(
            self, home, corpus=b"```bash\nkubectl get pods\n```\n", documentation_corpus=SIBLING
        )
        filtered, runtime = fixture.labeling()
        pod = filtered.loc[filtered["command"] == "kubectl get pods"].copy()
        (candidate,) = model_records(pod, CandidateRecord)
        (attempt,) = model_records(runtime.attempts(pod, home / "labeling"), LabelingAttempt)
        self.assertEqual(attempt.status, "success")
        guard = runtime.evidence_inputs.admission(pod)
        _, (item,) = guard.source(candidate)
        guard.verify(candidate, item, item.snippet.snippet)

        receipts = guard.root / "shared-operations"
        external = receipts.rename(home / "external-receipts")
        receipts.symlink_to(external, target_is_directory=True)

        with self.assertRaisesRegex(
            IntegrityError, "Linked or non-directory local evidence component: shared-operations"
        ):
            guard.verify(candidate, item, item.snippet.snippet)

    def test_section_and_document_restrictions_survive_prose_markup_and_false_scope_resets(self) -> None:
        effect = b"<p>Lists pods using <code>kubectl get pods</code>.</p>"
        restriction = b"<p>Examples require Kubernetes <strong>1.20</strong> or earlier.</p>"
        intro = b"<p>The examples operate on the selected namespace.</p>"
        for body in (
            RESTRICTED_REFERENCE,
            effect + b"<aside>Only for Kubernetes 1.20 or earlier.</aside>",
            b"<h2>Legacy client</h2><h3>Listing pods</h3>" + effect,
            b"<h2>Parent</h2>" + restriction + intro + b"<h3>Nested subsection</h3>" + intro + effect,
            b"<h2>Parent</h2>" + restriction + intro + b"<aside><h2>Not a sibling</h2></aside>" + effect,
            b"<h2>Parent</h2>" + restriction + intro + b"<!-- <h2>Not a sibling</h2> -->" + effect,
            b"<h2>Parent</h2><p>Use Kubernetes 1.20 or earlier throughout this document.</p>"
            + intro
            + b"<h2>Sibling</h2>"
            + effect,
            b"<p>Kubernetes 1.20 or earlier is required.</p><h2>Listing pods</h2>" + intro + effect,
            restriction + intro + intro + effect,
            b"> This example requires Kubernetes 1.20 or earlier.\n\nOrdinary prose.\n\n"
            b"Lists pods using `kubectl get pods`.",
            b"## Old examples\n\nThese require Kubernetes **1.20** or earlier.\n\n"
            b"~~~~markdown\n## Not a sibling\n~~~~\n\n"
            b"Lists pods using `kubectl get pods`.",
            b"Examples require Kubernetes 1.20 or earlier.\n\n"
            b"`<h2>Not a sibling</h2>`\n\n"
            b"Lists pods using `kubectl get pods`.",
            b"Examples require Kubernetes 1.20 or earlier.\n\n"
            b"    <h2>Not a sibling</h2>\n\n"
            b"Lists pods using `kubectl get pods`.",
            b"<para>Examples require Kubernetes 1.20 or earlier.</para><title>Not a proven boundary</title>"
            b"<para>These commands use the current namespace.</para>"
            b"<para>Lists pods using <command>kubectl get pods</command>.</para>",
        ):
            with self.subTest(body=body):
                self.assertEqual(tuple(operation_passages(body)), ())
        body = RESTRICTED_REFERENCE + "<h2>Listing services in Québec</h2>".encode()
        service = b"<p>Lists services using <code>kubectl get services</code>.</p>"
        (passage,) = operation_passages(body + service)
        self.assertEqual((passage.start, passage.end), (len(body), len(body + service)))
        self.assertEqual((body + service)[passage.start : passage.end], service)
        markdown = (
            b"## Older clients\n\nExamples require Kubernetes 1.20 or earlier.\n\n"
            b"Lists pods using `kubectl get pods`.\n\n## Listing services\n\n"
        )
        markdown_effect = b"Lists services using `kubectl get services`."
        (passage,) = operation_passages(markdown + markdown_effect)
        self.assertEqual((passage.start, passage.end), (len(markdown), len(markdown + markdown_effect)))

    def test_restricted_reference_is_rejected_in_inference_cache_and_release(self) -> None:
        for qualifier in (b"", b"Kubernetes client version `1.28` only.\n\n"):
            with self.subTest(qualifier=qualifier), TemporaryDirectory() as directory:
                home = Path(directory)
                fixture = LabelingFixture(
                    self,
                    home,
                    corpus=qualifier + b"```bash\nkubectl get pods\n```\n",
                    documentation_corpus=NESTED,
                )
                filtered, runtime = fixture.labeling()
                evidence = runtime.evidence_inputs
                pod = next(
                    row
                    for row in model_records(filtered, CandidateRecord)
                    if row.command == "kubectl get pods"
                )
                only_pod = filtered.loc[filtered["candidate_id"] == pod.candidate_id].copy()
                self.assertEqual(
                    evidence.admission(only_pod).sources.resolve(pod).versions,
                    ("1.28",) if qualifier else (),
                )
                resolve = CandidateEvidence.resolve

                def old_resolution(
                    sources: CandidateEvidence, candidate: CandidateRecord
                ) -> CandidateCapture:
                    return replace(resolve(sources, candidate), versions=())

                with (
                    patch.object(Applicability, "overlaps", return_value=False),
                    patch.object(CandidateEvidence, "resolve", old_resolution),
                ):
                    previous = runtime.attempts(only_pod, home / "old")
                    (accepted,) = model_records(previous, LabelingAttempt)
                    self.assertEqual(accepted.status, "success")
                    old_binding, _ = evidence.admission(only_pod).source(pod)

                with self.assertRaisesRegex(ValueError, "current source-level operation passage"):
                    evidence.verify(previous, selected=frozenset({pod.candidate_id}))

                source = GuardedEvidence.source
                verify = GuardedEvidence.verify

                def cached_binding(
                    guard: GuardedEvidence, candidate: CandidateRecord
                ) -> tuple[str, list[RowEvidence]]:
                    return old_binding, source(guard, candidate)[1]

                fixture.inference.reset_mock()
                with (
                    patch.object(GuardedEvidence, "source", cached_binding),
                    patch.object(GuardedEvidence, "verify", autospec=True, side_effect=verify) as checked,
                ):
                    replay = runtime.attempts(only_pod, home / "replay")
                checked.assert_called_once()
                (rejected,) = model_records(replay, LabelingAttempt)
                self.assertEqual(rejected.failure_code, "operation_unsupported")
                normal = runtime.attempts(only_pod, home / "normal")
                (rejected,) = model_records(normal, LabelingAttempt)
                self.assertEqual(rejected.failure_code, "operation_unsupported")
                fixture.inference.assert_not_called()

    def test_version_restrictions_do_not_authorize_a_current_shared_passage(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        body = b"Kubernetes client version `1.0` only.\n\n" + CORPUS
        fixture = LabelingFixture(self, home, corpus=body)
        filtered, runtime = fixture.labeling()
        result = runtime.attempts(filtered, home / "attempts")
        pod = next(row for row in model_records(result, LabelingAttempt) if row.command == "kubectl get pods")
        self.assertEqual(pod.failure_code, "operation_unsupported")
        versions = json.loads((home / "attempts/source-versions.json").read_bytes())
        self.assertEqual(versions[pod.candidate_id]["source_versions"], ["1.0"])
        self.assertNotIn(pod.candidate_id, fixture.remote_rows)

    def test_historical_persisted_receipt_rechecks_version_and_rights_without_transport(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, context7=True, source_limited=False)
        archived, revision = seed_prepared_snapshot(home, fixture.spec, DocumentationConfig())
        registry = DatasetRegistry(home)
        snapshot = registry.get_snapshot(archived.dataset_id, revision=revision)
        self.assertEqual(snapshot, archived)
        with self.assertRaisesRegex(IntegrityError, "read-only"):
            registry.get(archived.dataset_id)
        filtered, runtime = fixture.labeling()
        evidence = replace(
            runtime.evidence_inputs, grounding=require_guarded_annotation(snapshot.annotation).grounding
        )
        guard = evidence.admission(filtered)
        candidate = next(
            row for row in model_records(filtered, CandidateRecord) if row.command == "terraform plan"
        )
        capture = guard.sources.resolve(candidate)
        grant = guard.config.operation_grants[0]
        old = OperationReceipt(
            grant=grant,
            source_binding=capture.source_binding,
            source_versions=capture.versions,
            returned_version=grant.requested_version,
            lead_sha256="1" * 64,
        )
        item = admitted_excerpt(candidate, grant.url, LOOKUP_BODY.decode(), old.digest, grant.applicability)
        raw = guard.root / "origin-bytes" / grant.capture_sha256
        raw.parent.mkdir()
        raw.write_bytes(LOOKUP_BODY)
        receipt = guard.root / "verified-operations" / f"{item.evidence_id}.json"
        receipt.parent.mkdir()
        receipt.write_bytes(old.body)
        with patch(
            "codebench_pipeline.grounding_tools.GroundingBroker.execute",
            side_effect=AssertionError("historical replay attempted transport"),
        ):
            guard.verify(candidate, item, item.snippet.snippet)
            for update in (
                {"version_scope": None},
                {"requested_version": "2.0"},
                {"rights_sha256": "0" * 64},
                {"source_revision": "unreviewed"},
                {"operation": "terraform validate"},
                {"flags": ("-destroy",)},
                {"retain_origin_bytes": False},
            ):
                changed_grants = (grant.model_copy(update=update),)
                config = guard.config.model_copy(
                    update={
                        "operation_grants_json": TypeAdapter(tuple[OperationGrant, ...])
                        .dump_json(changed_grants)
                        .decode()
                    }
                )
                with self.subTest(update=update):
                    with self.assertRaises(ValueError):
                        replace(guard, config=config).verify(candidate, item, item.snippet.snippet)
            receipt.write_bytes(old.model_copy(update={"returned_version": "2.0"}).body)
            with self.assertRaisesRegex(IntegrityError, "admission receipt"):
                guard.verify(candidate, item, item.snippet.snippet)

            external = home / "external-operation-receipt.json"
            external.write_bytes(old.body)
            receipt.unlink()
            receipt.symlink_to(external)
            with self.assertRaises(IntegrityError):
                guard.verify(candidate, item, item.snippet.snippet)
