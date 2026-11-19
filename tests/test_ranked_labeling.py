import json
from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock
from unittest import TestCase
from unittest.mock import Mock, patch

import numpy as np
from numpy.typing import NDArray
from pydantic import BaseModel, JsonValue, ValidationError
from test_batch_labeling import batch_config, remote_model, submitted, tool_reply
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules
from test_declared_dataset_graph import fixture_spec
from test_repository import repository_config_fixture

from codebench_pipeline.batch_labeling import EvidenceAdmission, RowEvidence, annotate_batches
from codebench_pipeline.batch_labeling_config import BatchLabelingConfig
from codebench_pipeline.batch_labeling_ranked import QUOTA_DEFERRED, QUOTA_RECEIPT, RankedLabeling
from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_schemas import FilteredCommandsTable, validated_frame
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding import GuardedEvidence
from codebench_pipeline.grounding_sources import CandidateCapture
from codebench_pipeline.grounding_tables import GuardedLabeling
from codebench_pipeline.labeling_outcomes import LabelingAttempt, failed_attempt, label_attempt
from codebench_pipeline.labeling_tables import proposed_from_attempts
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import SemanticSnippet
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.repository_config import RepositoryConfig
from codebench_pipeline.storage import fingerprint


class RankedLabelingTests(TestCase):
    def test_guarded_packets_overlap_but_credit_and_refill_in_rank_order(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        rules = make_rules()
        first_risk, second_risk = (risk.id for risk in rules.risks)
        spec = fixture_spec(home)
        annotation = spec.annotation.model_copy(
            update={"workers": 2, "batch": batch_config(home).model_copy(update={"batch_size": 1})}
        )
        labeling = GuardedLabeling(home, spec.model_copy(update={"rules": rules, "annotation": annotation}))
        rows = [
            (
                identity,
                {
                    **make_candidate(identity, risk).model_dump(mode="json"),
                    "command": command,
                    "filter_decision": "eligible",
                    "filter_reason": "",
                    "source_weight": weight,
                },
            )
            for identity, risk, command, weight in (
                ("lower", second_risk, "probe publish example", 0.7),
                ("d-defer", first_risk, "probe inspect unnecessary", 0.9),
                ("c-refill", first_risk, "probe inspect unique", 0.9),
                ("b-duplicate", first_risk, "probe   inspect example", 0.9),
                ("a-first", first_risk, "probe inspect example", 0.9),
            )
        ]
        filtered = validated_frame([row for _, row in rows], FilteredCommandsTable)
        doc = documentation()[0]
        guard = Mock(spec=GuardedEvidence)
        guard.sources.resolve.return_value = CandidateCapture(
            "fixture", "fixed-binding", make_candidate().source_url, b"", "0" * 64, "fixture", None
        )
        guard.source.return_value = (
            "fixed-binding",
            [RowEvidence(evidence_id=fingerprint(doc.url), snippet=doc)],
        )
        guard.preflight.return_value = []
        first_started = Event()
        finished = {identity: Event() for identity, _ in rows}
        calls: list[str] = []
        calls_lock = Lock()

        def provider(_base: str, body: bytes, *_args: str | int | Path | BaseModel) -> HTTPResponse:
            (row,) = json.loads(json.loads(body)["messages"][1]["content"])["records"]
            identity = CandidateRecord.model_validate(row["candidate"]).candidate_id
            with calls_lock:
                calls.append(identity)
            match identity:
                case "a-first":
                    first_started.set()
                    self.assertTrue(
                        finished["b-duplicate"].wait(5), "Second packet did not finish concurrently"
                    )
                case "b-duplicate":
                    self.assertTrue(first_started.wait(5), "First packet did not start concurrently")
                case "c-refill":
                    self.assertTrue(finished["a-first"].is_set(), "Refill ran before quota was available")
                case "lower":
                    self.assertTrue(
                        finished["c-refill"].is_set(), "Lower trust ran before the higher tier settled"
                    )
                case _:
                    self.fail(f"Unexpected inference for {identity}")
            return tool_reply("submit_labels", {"outcomes": [submitted(row, row["evidence_ids"])]})

        def observe_packet(
            packet: list[CandidateRecord],
            docs: list[SemanticSnippet],
            rules: CategoryRules,
            model: ModelConfig,
            config: BatchLabelingConfig,
            out: Path,
            *,
            cache: Path,
            maximum_context_characters: int,
            admission: EvidenceAdmission,
        ) -> list[LabelingAttempt]:
            attempts = annotate_batches(
                packet,
                docs,
                rules,
                model,
                config,
                out,
                cache=cache,
                maximum_context_characters=maximum_context_characters,
                admission=admission,
            )
            (candidate,) = packet
            finished[candidate.candidate_id].set()
            return attempts

        with (
            patch.object(GuardedLabeling, "admission", return_value=guard),
            patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider),
            patch("codebench_pipeline.batch_labeling_ranked.annotate_batches", side_effect=observe_packet),
            patch("codebench_pipeline.batch_labeling_ranked.SUBRISK_QUOTA", 2),
        ):
            attempts = labeling.attempts(filtered, home / "stage")

        self.assertCountEqual(calls, ["a-first", "b-duplicate", "c-refill", "lower"])
        self.assertEqual(list(attempts["candidate_id"]), [identity for identity, _ in rows])
        observed = attempts.set_index("candidate_id")
        self.assertEqual(observed.loc["d-defer", "failure_code"], QUOTA_DEFERRED)
        for identity in calls:
            self.assertEqual(observed.loc[identity, "status"], "success")
            row_dir = home / "stage" / "label" / sha256(identity.encode()).hexdigest()
            self.assertTrue((row_dir / "verified.json").is_file())
        quotas = attempts.attrs["labeling_quota"]["subrisks"]
        self.assertEqual(quotas[first_risk]["credited_candidates"], ["a-first", "c-refill"])
        self.assertEqual(quotas[second_risk]["credited_candidates"], ["lower"])

    def test_guarded_rank_change_reuses_judgment_and_keeps_inspectable_trust_and_evidence(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        rules = make_rules()
        labeling = GuardedLabeling(home, fixture_spec(home).model_copy(update={"rules": rules}))
        candidate = make_candidate()
        identity = candidate.candidate_id
        filtered = validated_frame(
            [
                {
                    **candidate.model_dump(mode="json"),
                    "filter_decision": "eligible",
                    "filter_reason": "",
                    "source_weight": 0.8,
                }
            ],
            FilteredCommandsTable,
        )
        doc = documentation()[0]
        guard = Mock(spec=GuardedEvidence)
        guard.sources.resolve.return_value = CandidateCapture(
            "fixture", "fixed-binding", candidate.source_url, b"", "0" * 64, "fixture", None
        )
        guard.source.return_value = (
            "fixed-binding",
            [RowEvidence(evidence_id=fingerprint(doc.url), snippet=doc)],
        )
        guard.preflight.return_value = []

        def provider(_base: str, body: bytes, *_args: str | int | Path | BaseModel) -> HTTPResponse:
            (row,) = json.loads(json.loads(body)["messages"][1]["content"])["records"]
            return tool_reply("submit_labels", {"outcomes": [submitted(row, row["evidence_ids"])]})

        self.enterContext(patch.object(GuardedLabeling, "admission", return_value=guard))
        inference = self.enterContext(
            patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider)
        )
        first = labeling.attempts(filtered, home / "first")
        revised = filtered.copy()
        revised.loc[revised["candidate_id"] == identity, "source_weight"] = 0.79
        replay = labeling.attempts(revised, home / "replay")
        labeling.verify(replay)

        inference.assert_called_once()
        observed = first.set_index("candidate_id")
        self.assertEqual(observed.loc[identity, "source_weight"], 0.8)
        self.assertEqual(observed.loc[identity, "status"], "success")
        replayed = replay.set_index("candidate_id")
        self.assertEqual(
            replayed.loc[identity, "row_input_fingerprint"], observed.loc[identity, "row_input_fingerprint"]
        )
        proposal = proposed_from_attempts(replay, rules).set_index("candidate_id")
        self.assertEqual(proposal.loc[identity, "source_weight"], 0.79)
        row_dir = home / "replay" / "label" / sha256(identity.encode()).hexdigest()
        self.assertTrue((row_dir / "reuse.json").is_file())
        self.assertTrue((row_dir / "verified.json").is_file())
        self.assertNotEqual(
            first.attrs["labeling_quota"]["eligible_scope_fingerprint"],
            replay.attrs["labeling_quota"]["eligible_scope_fingerprint"],
        )

    def test_only_distinct_all_high_verified_risk_judgments_can_fill_a_quota(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        rules = make_rules()
        first_risk, second_risk = (item.id for item in rules.risks)
        rules = rules.model_copy(
            update={
                "risks": [
                    rules.risks[0],
                    rules.risks[1].model_copy(update={"command_patterns": [r"\binspect both\b"]}),
                ]
            }
        )
        candidates: list[CandidateRecord] = []
        weights: dict[str, float] = {}
        for identity, command, weight, risks in (
            *((f"high-{i:03}", f"probe inspect row-{i}", 0.9, [first_risk]) for i in range(249)),
            ("duplicate", "probe   inspect row-0", 0.8, [first_risk]),
            ("uncertain", "probe inspect uncertain", 0.8, [first_risk]),
            ("unaccepted", "probe inspect unaccepted", 0.8, [first_risk]),
            ("filler", "probe inspect filler", 0.6, [first_risk]),
            ("deferred", "probe inspect deferred", 0.3, [first_risk]),
            ("ambiguous", "probe inspect both", 0.3, [first_risk, second_risk]),
        ):
            candidates.append(
                make_candidate(identity).model_copy(update={"command": command, "risk_candidates": risks})
            )
            weights[identity] = weight
        scheduler = RankedLabeling(
            rules,
            remote_model(),
            batch_config(home),
            home / "stage",
            home / "cache",
            100_000,
            Mock(spec=GuardedEvidence),
        )
        submitted: list[str] = []

        def judge(
            packet: list[CandidateRecord],
            *_args: BaseModel | Path | list[BaseModel],
            **_kwargs: Path | int | GuardedEvidence,
        ) -> list[LabelingAttempt]:
            outcomes: list[LabelingAttempt] = []
            amendments: dict[str, dict[str, JsonValue]] = {
                "uncertain": {"class_confidence": "medium"},
                "unaccepted": {"decision": "needs_review"},
                "ambiguous": {"risk_id": second_risk},
            }
            for candidate in packet:
                identity = candidate.candidate_id
                submitted.append(identity)
                label = make_label(candidate, rules).model_copy(update=amendments.get(identity))
                outcomes.append(
                    label_attempt(
                        candidate,
                        label,
                        documentation(),
                        fingerprint(identity),
                        f"label/{identity}/receipt.json",
                    )
                )
            return outcomes

        def vectors(commands: Sequence[str]) -> NDArray[np.float32]:
            return np.ones((len(commands), 1), dtype=np.float32)

        blocked = make_candidate("unverified", first_risk)
        weights[blocked.candidate_id] = 0.95
        source_failure = failed_attempt(
            blocked,
            "candidate_capture_unverified",
            "Capture could not be verified.",
            [],
            fingerprint(blocked.candidate_id),
            "source-failures.json",
        )
        with (
            patch("codebench_pipeline.command_batches.encode_commands", side_effect=vectors),
            patch("codebench_pipeline.batch_labeling_ranked.annotate_batches", side_effect=judge),
        ):
            attempts, _ = scheduler.run(list(reversed(candidates)), weights, [source_failure])

        observed_weights = [weights[identity] for identity in submitted]
        self.assertEqual(observed_weights, sorted(observed_weights, reverse=True))
        self.assertNotIn("deferred", submitted)
        self.assertIn("ambiguous", submitted)
        self.assertIn("filler", submitted)
        by_id = {attempt.candidate_id: attempt for attempt in attempts}
        self.assertEqual(by_id["deferred"].failure_code, QUOTA_DEFERRED)
        receipt = json.loads((scheduler.out / QUOTA_RECEIPT).read_bytes())
        full, short = receipt["subrisks"][first_risk], receipt["subrisks"][second_risk]
        self.assertEqual(full["state"], "target_met")
        self.assertEqual(
            (full["shortage"], full["remaining_to_target"], full["blocked_candidates"]), (0, 0, 1)
        )
        self.assertEqual(full["credited_candidates"], [f"high-{i:03}" for i in range(249)] + ["filler"])
        self.assertEqual(full["deferred_candidates"], 1)
        self.assertEqual((short["state"], short["shortage"]), ("scope_exhausted", 249))
        self.assertEqual(short["credited_candidates"], ["ambiguous"])
        self.assertEqual(short["verified_candidates"], 1)
        self.assertEqual(short["blocked_candidates"], 0)

    def test_source_capture_failure_blocks_only_compatible_subrisk(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        rules = make_rules()
        candidate = make_candidate().model_copy(update={"command": "probe inspect row"})
        failure = failed_attempt(
            candidate,
            "candidate_capture_unverified",
            "Capture could not be verified.",
            [],
            fingerprint(candidate.candidate_id),
            "source-failures.json",
        )
        scheduler = RankedLabeling(
            rules,
            remote_model(),
            batch_config(home),
            home / "stage",
            home / "cache",
            100_000,
            Mock(spec=GuardedEvidence),
        )

        with patch("codebench_pipeline.batch_labeling_ranked.annotate_batches") as infer:
            attempts, _ = scheduler.run([], {candidate.candidate_id: 0.9}, [failure])

        infer.assert_not_called()
        self.assertEqual(attempts, [failure])
        receipt = json.loads((scheduler.out / QUOTA_RECEIPT).read_bytes())
        blocked, empty = (receipt["subrisks"][risk.id] for risk in rules.risks)
        self.assertEqual(blocked["state"], "scope_blocked")
        self.assertIsNone(blocked["shortage"])
        self.assertEqual(blocked["remaining_to_target"], 250)
        self.assertEqual(blocked["source_unverified_candidates"], 1)
        self.assertEqual(blocked["scheduled_candidates"], 0)
        self.assertEqual(blocked["blocked_candidates"], 1)
        self.assertEqual(blocked["failure_counts"], {"candidate_capture_unverified": 1})
        self.assertEqual((empty["state"], empty["shortage"]), ("scope_exhausted", 250))

    def test_unresolved_routing_and_label_failures_remain_visible_by_compatible_subrisk(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        rules = make_rules()
        first_risk, second_risk = (item.id for item in rules.risks)
        unknown = make_candidate("unknown").model_copy(update={"risk_candidates": []})
        reasons = (
            "operation_unsupported",
            "missing_operation_documentation",
            "protocol_exhausted",
            QUOTA_DEFERRED,
        )
        candidates = [make_candidate(reason, first_risk) for reason in reasons]
        failures = [
            failed_attempt(
                candidate,
                candidate.candidate_id,
                "Unresolved attempt.",
                [],
                fingerprint(candidate.candidate_id),
                f"label/{candidate.candidate_id}/receipt.json",
            )
            for candidate in candidates
        ]
        scheduler = RankedLabeling(
            rules,
            remote_model(),
            batch_config(home).model_copy(update={"batch_size": 1}),
            home / "stage",
            home / "cache",
            100_000,
            Mock(spec=GuardedEvidence),
        )
        weights = {candidate.candidate_id: 0.8 for candidate in [*candidates, unknown]}
        with patch(
            "codebench_pipeline.batch_labeling_ranked.annotate_batches",
            side_effect=[[attempt] for attempt in sorted(failures, key=lambda row: row.candidate_id)],
        ):
            attempts, _ = scheduler.run([*candidates, unknown], weights, [])

        by_id = {attempt.candidate_id: attempt for attempt in attempts}
        self.assertEqual(by_id["unknown"].failure_code, "routing_unverified")
        self.assertEqual(
            by_id["protocol_exhausted"].attempt_evidence_reference, "label/protocol_exhausted/receipt.json"
        )
        receipt = json.loads((scheduler.out / QUOTA_RECEIPT).read_bytes())
        for risk, expected in (
            (first_risk, dict.fromkeys((*reasons, "routing_unverified"), 1)),
            (second_risk, {"routing_unverified": 1}),
        ):
            with self.subTest(risk=risk):
                row = receipt["subrisks"][risk]
                self.assertEqual(row["state"], "scope_blocked")
                self.assertIsNone(row["shortage"])
                self.assertEqual(row["blocked_candidates"], len(expected))
                self.assertEqual(row["failure_counts"], expected)
                self.assertEqual(row["deferred_candidates"], 0)

    def test_repository_weights_reject_invalid_numbers_unknown_origins_and_tier_inversions(self) -> None:
        config = repository_config_fixture()
        dataset_id = config.dataset[0].origin_id
        for origin, value in (
            (dataset_id, float("nan")),
            (dataset_id, float("inf")),
            (dataset_id, -0.01),
            (dataset_id, 1.01),
            (dataset_id, True),
            (dataset_id, "0.55"),
            (dataset_id, 0.99),
            ("unknown-origin", 0.55),
        ):
            with self.subTest(origin=origin, value=value), self.assertRaises(ValidationError):
                RepositoryConfig.model_validate(
                    {**config.model_dump(), "source_weights": {**config.source_weights, origin: value}}
                )

    def test_source_weight_changes_only_its_branch_declaration_digest(self) -> None:
        config = repository_config_fixture()
        origin = config.dataset[0].origin_id
        revised = RepositoryConfig.model_validate(
            {**config.model_dump(), "source_weights": {**config.source_weights, origin: 0.51}}
        )
        self.assertEqual(revised.source_weight("dataset", origin), 0.51)
        self.assertNotEqual(revised.branch_digest("dataset"), config.branch_digest("dataset"))
        self.assertEqual(revised.branch_digest("documentation"), config.branch_digest("documentation"))

    def test_keenable_weight_is_optional_for_older_declarations_and_cannot_tie_a_dataset(self) -> None:
        config = repository_config_fixture()
        old = RepositoryConfig.model_validate(config.model_dump(exclude={"keenable_source_weight"}))
        self.assertEqual(old.keenable_source_weight, 0.3)
        changed = RepositoryConfig.model_validate({**config.model_dump(), "keenable_source_weight": 0.24})
        self.assertEqual(changed.branch_digest("dataset"), old.branch_digest("dataset"))
        self.assertEqual(changed.branch_digest("documentation"), old.branch_digest("documentation"))
        lowest = min(config.source_weight("dataset", origin.origin_id) for origin in config.dataset)
        for invalid in (lowest, 1, float("inf"), True, "0.24"):
            with self.subTest(invalid=invalid), self.assertRaises(ValidationError):
                RepositoryConfig.model_validate({**config.model_dump(), "keenable_source_weight": invalid})
