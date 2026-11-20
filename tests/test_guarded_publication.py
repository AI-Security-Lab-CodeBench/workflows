import csv
import json
import os
import re
from contextlib import ExitStack
from dataclasses import replace
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
from guarded_labeling_fixture import CORPUS, LabelingFixture, fixture_vectors

from codebench_pipeline import guarded_publication_sampling
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import read_frame
from codebench_pipeline.dataframe_schemas import model_records
from codebench_pipeline.dataset_config import PublicationConfig
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import run_registered
from codebench_pipeline.grounding_tables import SUPPORT_ATTR
from codebench_pipeline.guarded_publication import ReleaseInputs, ReleaseProposal, prepare_guarded_release
from codebench_pipeline.guarded_publication_controls import record_control
from codebench_pipeline.guarded_publication_runtime import (
    current_proposal,
    preview_proposal,
    update_publication_control,
)
from codebench_pipeline.guarded_publication_sampling import CoverageStatus, StratumCoverage, sample_survivors
from codebench_pipeline.guarded_publication_selection import Survivor, select_survivors
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.labeling_outcomes import LabelingAttempt, label_attempt
from codebench_pipeline.models import SemanticSnippet
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.release_admission import read_publication
from codebench_pipeline.release_runner import ReleaseContinuation, ReleaseRunner
from codebench_pipeline.repository_receipts import RepositoryRun
from codebench_pipeline.review_snapshots import ReviewManifest, ReviewSnapshots
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig
from codebench_pipeline.storage import IntegrityError, fingerprint


class GuardedPublicationTests(TestCase):
    def test_exact_release_requires_attestation_and_hold_clearance_and_rechecks_cached_output(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, limit_per_risk=4)
        dataset_id = DatasetRegistry(home).register(fixture.spec)
        store = HumanReviewStore(home)
        with store.transaction(dataset_id=dataset_id) as tx:
            record_control(tx, dataset_id, "hold", actor=100)
        with (
            patch("codebench_pipeline.labeling_runtime.encode_from_directory", side_effect=fixture_vectors),
            patch(
                "codebench_pipeline.grounding_tools.GroundingBroker.execute",
                side_effect=AssertionError("labeler attempted provider lookup"),
            ),
        ):
            outcome = run_registered(dataset_id, home)
        self.assertEqual(outcome.status, "prepared")
        with store.reader() as tx:
            batch = tx.get_current_batch(dataset_id)
            self.assertEqual(batch.snapshot.candidate_ids, ())
            self.assertEqual(tx.list_decisions(batch.batch_id), ())
            self.assertIsNone(tx.last_publication(dataset_id))
            token = batch.completion
        if token is None:
            self.fail("Empty synthetic review did not complete by policy")
        with dataset_instance(home) as instance:
            snapshots = ReviewSnapshots.from_environment(home, DatasetDataService(home, instance))
            approved = snapshots.authoritative(dataset_id)
            stale_settings = PublicationConfig(
                limit_per_risk=1, review_columns=approved.manifest.publication.review_columns
            )
            different_cap = fixture.spec.model_copy(update={"publication": stale_settings})
            with self.assertRaisesRegex(HumanReviewError, "Registered release policy differs"):
                prepare_guarded_release(
                    ReleaseInputs(snapshots, approved, token, ()),
                    RepositoryRun(home, dataset_id, "release-verification"),
                    different_cap,
                    publication_versions={},
                )
            proposal, path = preview_proposal(home, instance, dataset_id)
            csv_rows = list(csv.DictReader(StringIO((path / "commands.csv").read_text())))
            self.assertEqual(
                [row["command"] for row in csv_rows],
                ["docker ps --all", "kubectl get pods", "docker compose ps", "terraform plan"],
            )
            self.assertEqual(
                read_frame((path / "commands.parquet").read_bytes())["command"].tolist(),
                [row["command"] for row in csv_rows],
            )
            self.assertEqual({row["human_reviewed"] for row in csv_rows}, {"false"})
            expected = json.loads((path / "manifest.json").read_bytes())
            self.assertEqual(
                [row["candidate"]["command"] for row in expected["selected"]],
                [row["command"] for row in csv_rows],
            )
            selected = {row["id"]: row for row in expected["selected"]}
            sampling = expected["sampling"]
            self.assertEqual(set(expected["sampled_row_ids"]), set(selected))
            for branch, count in {"dataset": 2, "documentation": 1, "keenable": 1}.items():
                with self.subTest(branch=branch):
                    sample = sampling[branch]
                    ids = sample["sampled_row_ids"]
                    self.assertEqual(len(ids), count)
                    self.assertEqual(set(ids), {key for key in selected if key.startswith(f"{branch}:")})
                    strata = sample["strata"]
                    self.assertEqual(strata["route:automatic"]["sampled_row_ids"], ids)
                    self.assertEqual(strata["semantics:inspection"]["sampled_row_ids"], ids)
                    self.assertEqual(strata["route:human_reviewed"]["status"], "absent")
                    self.assertEqual(strata["semantics:state_changing"]["status"], "absent")
            runner = ReleaseRunner(home, instance)
            with store.transaction(dataset_id=dataset_id) as tx:
                record_control(tx, dataset_id, "clear", actor=100)
                record_control(tx, dataset_id, "attest", actor=200, proposal="f" * 64)
            with self.assertRaisesRegex(HumanReviewError, "exact proposal"):
                runner(token)
            (home / "publication-authorities.json").write_text(
                json.dumps({"operators": [], "stewards": [os.getuid()]})
            )
            without_plan = re.compile(guarded_publication_sampling.INSPECTION.pattern.replace("plan|", "", 1))
            with patch.object(guarded_publication_sampling, "INSPECTION", without_plan):
                unresolved, unresolved_path = preview_proposal(home, instance, dataset_id)
                self.assertNotEqual(unresolved, proposal)
                self.assertEqual(
                    (unresolved_path / "commands.csv").read_bytes(), (path / "commands.csv").read_bytes()
                )
                message = "Resolve semantic coverage and resample.*keenable/semantics:state_changing"
                with self.assertRaisesRegex(HumanReviewError, message):
                    update_publication_control(home, instance, dataset_id, "attest", unresolved)
                with store.transaction(dataset_id=dataset_id) as tx:
                    record_control(tx, dataset_id, "attest", actor=200, proposal=unresolved)
                with self.assertRaisesRegex(HumanReviewError, message):
                    runner(token)
                with patch(
                    "codebench_pipeline.release_runner.launch_reviewed_run",
                    side_effect=AssertionError("Unresolved coverage cannot launch a release"),
                ):
                    self.assertIsNone(ReleaseContinuation(home, instance, 0)(token).run_id)
                with store.reader() as tx:
                    self.assertIsNone(tx.last_publication(dataset_id))

            self.assertEqual(preview_proposal(home, instance, dataset_id), (proposal, path))
            with self.assertRaisesRegex(HumanReviewError, "changed before attestation"):
                update_publication_control(home, instance, dataset_id, "attest", unresolved)
            with self.assertRaisesRegex(HumanReviewError, "exact proposal"):
                runner(token)
            update_publication_control(home, instance, dataset_id, "attest", proposal)
            with store.transaction(dataset_id=dataset_id) as tx:
                record_control(tx, dataset_id, "hold", actor=100)
            with self.assertRaisesRegex(HumanReviewError, "operator hold"):
                runner(token)
            with store.transaction(dataset_id=dataset_id) as tx:
                record_control(tx, dataset_id, "clear", actor=100)
            sampled = sampling["dataset"]["sampled_row_ids"][0]
            read_review_frame = ReviewSnapshots.frame

            def changed_support(
                snapshots: ReviewSnapshots, manifest: ReviewManifest, stage: str
            ) -> pd.DataFrame:
                frame = read_review_frame(snapshots, manifest, stage)
                if stage == "labeling_attempts":
                    supports = frame.attrs[SUPPORT_ATTR]
                    frame.attrs[SUPPORT_ATTR] = {**supports, sampled: supports[sampled] + "\n"}
                return frame

            with ExitStack() as race:

                def change_registration_after_load(
                    inputs: ReleaseInputs,
                    run: RepositoryRun,
                    registered: DeclaredGlobalConfig,
                    *,
                    publication_versions: dict[str, str],
                ) -> ReleaseProposal:
                    prepared = prepare_guarded_release(
                        inputs, run, registered, publication_versions=publication_versions
                    )
                    race.enter_context(
                        patch.object(
                            DatasetRegistry,
                            "current_revision",
                            return_value=fingerprint("registration changed after release input resolution"),
                        )
                    )
                    return prepared

                with (
                    patch(
                        "codebench_pipeline.review_inputs.prepare_guarded_release",
                        side_effect=change_registration_after_load,
                    ) as intercepted,
                    self.assertRaisesRegex(
                        HumanReviewError, "Registered dataset configuration changed during the run"
                    ) as stale,
                ):
                    runner(token)
                intercepted.assert_called_once()
                self.assertEqual(stale.exception.code, "stale")
                with store.reader() as tx:
                    self.assertFalse(tx.list_publications(dataset_id))

            publication = runner(token)
            self.assertEqual(publication.proposal_digest, proposal)
            self.assertEqual(runner(token), publication)
            published_manifest = ObjectStore(fixture.spec.storage).read(
                f"{publication.bundle_reference}/proposal-manifest.json"
            )
            self.assertEqual(published_manifest, (path / "manifest.json").read_bytes())
            with patch.object(ReviewSnapshots, "frame", changed_support):
                with store.transaction(dataset_id=dataset_id) as tx:
                    changed = current_proposal(
                        ReviewSnapshots.from_environment(home, DatasetDataService(home, instance)),
                        token,
                        tx,
                        fixture.spec,
                    )
                self.assertEqual(changed.manifest["sampling"], sampling)
                self.assertEqual(changed.manifest["csv_sha256"], expected["csv_sha256"])
                self.assertNotEqual(changed.digest, proposal)
                with self.assertRaisesRegex(HumanReviewError, "changed before attestation"):
                    update_publication_control(home, instance, dataset_id, "attest", proposal)
                with self.assertRaisesRegex(HumanReviewError, "exact proposal"):
                    runner(token)
            observed = read_publication(home, dataset_id, instance)
            if observed is None or not isinstance(observed.value, pd.DataFrame):
                self.fail("Published commands must be readable as a table")
            self.assertEqual(observed.publication_state, "published")
            self.assertEqual(observed.value["command"].tolist(), [row["command"] for row in csv_rows])
            with store.transaction(dataset_id=dataset_id) as tx:
                record_control(tx, dataset_id, "revoke", actor=200, proposal=proposal)
            with self.assertRaisesRegex(HumanReviewError, "exact proposal"):
                runner(token)
            revoked = read_publication(home, dataset_id, instance)
            if revoked is None:
                self.fail("The revoked publication must remain inspectable")
            self.assertEqual(revoked.publication_state, "stale")

    def test_shared_command_capture_without_effect_evidence_cannot_produce_a_label(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(
            self, home, shared_body=b"<pre>terraform plan</pre><p>Unrelated origin response.</p>"
        )
        filtered, labeling = fixture.labeling()
        attempts = labeling.attempts(filtered, home / "attempts")
        rows = {row.command: row for row in model_records(attempts, LabelingAttempt)}
        self.assertEqual(rows["terraform plan"].failure_code, "operation_unsupported")
        self.assertEqual(rows["kubectl get pods"].status, "success")

    def test_mutating_one_repository_capture_cannot_restore_its_success(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, context7=True)
        filtered, labeling = fixture.labeling()
        attempts = labeling.attempts(filtered, home / "attempts")
        self.assertEqual({row.status for row in model_records(attempts, LabelingAttempt)}, {"success"})
        (capture,) = [path for path in home.rglob("blobs/*") if path.read_bytes() == CORPUS]
        capture.write_bytes(CORPUS.replace(b"Enumerate", b"Overwrite"))
        with self.assertRaisesRegex(IntegrityError, "capture digest mismatch"):
            labeling.evidence_inputs.verify(attempts)
        during = len(fixture.remote_rows)
        evidence = home / "relabel"
        evidence.mkdir()
        replay = labeling.attempts(filtered, evidence)
        rows = {row.command: row for row in model_records(replay, LabelingAttempt)}
        self.assertEqual(rows["kubectl get pods"].failure_code, "candidate_capture_unverified")
        self.assertEqual(rows["docker compose ps"].status, "success")
        self.assertEqual(fixture.remote_rows[during:], [])


class ReleaseSelectionTests(TestCase):
    def test_steward_sample_includes_the_only_documentation_deployment_update(self) -> None:
        rows = sorted(
            [
                Survivor(f"documentation:{i}", "documentation", "docs", "A", "kubectl", "allow", False)
                for i in range(12)
            ],
            key=lambda row: row.stable_key,
        )
        for update in ("kubectl edit deployment/nginx", "kubectl label deployment/nginx owner=qa"):
            with self.subTest(update=update):
                commands = [
                    "kubectl version --client",
                    *(f"kubectl get deployment/example-{i}" for i in range(10)),
                    update,
                ]
                attempts = {
                    row.identity: supported_attempt(row, command)
                    for row, command in zip(rows, commands, strict=True)
                }
                sample = sample_survivors(rows, attempts)["documentation"]
                self.assertEqual(sample, sample_survivors(list(reversed(rows)), attempts)["documentation"])
                self.assertEqual(sample.population, 12)
                self.assertEqual(
                    sample.sampled_row_ids, [*(row.identity for row in rows[:9]), rows[-1].identity]
                )
                self.assertEqual(
                    sample.strata["semantics:state_changing"],
                    StratumCoverage(1, [rows[-1].identity], "sampled"),
                )
                self.assertEqual(sample.unclassified_row_ids, [rows[0].identity])

    def test_unclassified_operations_do_not_establish_absence(self) -> None:
        row = Survivor("documentation:0", "documentation", "docs", "A", "kubectl", "allow", False)
        cases: tuple[tuple[str, CoverageStatus], ...] = (
            ('kubectl "edit deployment/nginx"', "unclassified"),
            ("kubectl label deployment/nginx owner=qa --dry-run=client", "unclassified"),
            ("kubectl label deployment/nginx --list", "absent"),
        )
        for command, expected in cases:
            with self.subTest(command=command):
                attempt = supported_attempt(row, command)
                sample = sample_survivors([row], {row.identity: attempt})["documentation"]
                self.assertEqual(sample.strata["semantics:state_changing"], StratumCoverage(0, [], expected))
                self.assertEqual(sample.strata["route:human_reviewed"], StratumCoverage(0, [], "absent"))
        unsampled = StratumCoverage.from_rows([row.identity], set(), unclassified=False)
        self.assertEqual(unsampled, StratumCoverage(1, [], "present_but_unsampled"))

    def test_steward_sample_reserves_rare_strata_before_filling_each_branch(self) -> None:
        dataset = sorted(
            [
                Survivor(f"dataset:{i}", "dataset", "kubernetes-examples", "A", "get", "allow", False)
                for i in range(12)
            ],
            key=lambda row: row.stable_key,
        )
        docs = sorted(
            [
                Survivor(f"documentation:{i}", "documentation", "docs", "A", "get", "allow", False)
                for i in range(12)
            ],
            key=lambda row: row.stable_key,
        )
        change = replace(dataset[-2], operation="apply", uncertain=True)
        compose = replace(dataset[-1], origin="docker-awesome-compose")
        rows = [*dataset[:-2], change, compose, *docs]
        attempts = {
            row.identity: supported_attempt(
                row,
                "kubectl apply -f example.yaml"
                if row == change
                else "docker compose ps"
                if row == compose
                else "kubectl get pods",
            )
            for row in rows
        }

        sampling = sample_survivors(rows, attempts)

        self.assertEqual(sampling, sample_survivors(list(reversed(rows)), attempts))
        self.assertEqual(
            sampling["dataset"].sampled_row_ids,
            [row.identity for row in (*dataset[:8], change, compose)],
        )
        strata = sampling["dataset"].strata
        self.assertEqual(strata["origin:docker-awesome-compose"].sampled_row_ids, [compose.identity])
        self.assertEqual(strata["semantics:state_changing"].sampled_row_ids, [change.identity])
        self.assertEqual(strata["route:human_reviewed"].sampled_row_ids, [change.identity])
        self.assertEqual(sampling["documentation"].sampled_row_ids, [row.identity for row in docs[:10]])

    def test_scarce_risk_is_reserved_and_reordering_does_not_change_selection(self) -> None:
        rows = [
            Survivor(
                "compose-shared",
                "dataset",
                "compose",
                "A",
                "inspect",
                "allow",
                False,
                "docker-awesome-compose",
            ),
            Survivor(
                "compose-spare",
                "dataset",
                "compose",
                "B",
                "inspect",
                "allow",
                False,
                "docker-awesome-compose",
            ),
            Survivor("kubernetes", "dataset", "kube", "A", "inspect", "allow", False, "kubernetes-examples"),
            Survivor("keenable", "keenable", "vendor", "B", "inspect", "allow", False, "keenable"),
        ]
        caps = {"A": 1, "B": 2}
        first = select_survivors(rows, caps)
        second = select_survivors(list(reversed(rows)), caps)
        self.assertEqual(first, second)
        self.assertEqual(first.ordered, ("compose-spare", "keenable", "kubernetes"))
        with self.assertRaisesRegex(HumanReviewError, "cannot fit"):
            select_survivors(rows, {"A": 1, "B": 1})

    def test_weighted_caps_prefer_trust_and_do_not_require_low_trust_contributions(self) -> None:
        rows = [
            Survivor("low", "keenable", "vendor", "A", "inspect", "allow", False),
            Survivor("mid", "dataset", "corpus", "A", "inspect", "allow", False),
            Survivor("high", "documentation", "upstream", "A", "inspect", "allow", False),
            Survivor("other", "dataset", "corpus", "B", "inspect", "allow", False),
        ]
        weights = {"low": 0.2, "mid": 0.6, "high": 0.95, "other": 0.6}
        caps = {"A": 1, "B": 2}
        selected = select_survivors(rows, caps, weights)
        self.assertEqual(selected, select_survivors(list(reversed(rows)), caps, weights))
        self.assertEqual(selected.ordered, ("high", "other"))
        self.assertEqual(selected.coverage["risk_deficits"], {"A": 0, "B": 1})
        with self.assertRaisesRegex(HumanReviewError, "source weight for every survivor"):
            select_survivors(rows, caps, {"low": 0.2})


def supported_attempt(row: Survivor, command: str) -> LabelingAttempt:
    url = "https://example.com/docs"
    utility = command.split()[0]
    candidate = CandidateRecord(
        candidate_id=row.identity,
        command=command,
        util=utility,
        context="",
        source_id=row.origin,
        source_url=url,
        source_position="line:1",
        transformations=[],
        risk_candidates=[row.risk],
    )
    evidence = SemanticSnippet(
        utility=utility,
        subcommands=[command],
        url=url,
        retrieved_at="fixture",
        snippet=f"Documented example: {command}",
        version_limits="Synthetic",
    )
    label = LabelDecision.model_validate(
        {
            "candidate_id": row.identity,
            "decision": "accept",
            "category_id": "CAT-3",
            "risk_id": row.risk,
            "severity": "low",
            "class": "allow",
            "description": "Fixture annotation",
            "util": utility,
            "category_confidence": "high",
            "risk_confidence": "high",
            "severity_confidence": "high",
            "class_confidence": "high",
            "rationale": "Synthetic annotation",
            "documentation_urls": [url],
        }
    )
    return label_attempt(candidate, label, [evidence], "a" * 64, "synthetic.json")
