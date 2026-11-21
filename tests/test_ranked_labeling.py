import json
from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock
from unittest import TestCase
from unittest.mock import patch

from pandera.typing import DataFrame
from test_batch_labeling import (
    QUOTE,
    URL,
    batch_config,
    live_mcp_fixture,
    remote_model,
    request_parts,
    shared_retrieval,
    submitted,
    tool_reply,
)
from test_dataset_rules import make_candidate, make_rules
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.batch_labeling import (
    BatchExecution,
    LabelingGeneration,
    run_prepared_batches,
)
from codebench_pipeline.batch_labeling_config import BatchLabelingConfig
from codebench_pipeline.batch_labeling_protocol import RowInput
from codebench_pipeline.batch_labeling_ranked import QUOTA_DEFERRED, QUOTA_RECEIPT, RankedLabeling
from codebench_pipeline.dataframe_schemas import FilteredCommandsTable, validated_frame
from codebench_pipeline.dataset_config import DatasetBuildSpec, require_declared_annotation
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding_tables import GuardedLabeling, verify_live_support
from codebench_pipeline.grounding_tools import GroundingCredentials
from codebench_pipeline.inference_capacity import CapacityConfig, CapacityUnavailable
from codebench_pipeline.labeling_checkpoint_io import labeling_generation_identity
from codebench_pipeline.labeling_checkpoint_store import LabelingCheckpointCache
from codebench_pipeline.labeling_outcomes import LabelingAttempt, failed_attempt
from codebench_pipeline.labeling_stage_settings import LabelingRules, ProposalRules
from codebench_pipeline.labeling_tables import proposed_from_attempts
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.stage_configuration import GuardedLabelingConfig
from codebench_pipeline.storage import fingerprint


def bound_labeling(spec: DatasetBuildSpec) -> GuardedLabeling:
    mapped = GuardedLabelingConfig.from_spec(spec)
    batch = require_declared_annotation(spec.annotation)
    return GuardedLabeling(
        mapped, batch, BatchExecution(None, grounding=GroundingCredentials.from_environment(batch.grounding))
    )


class RankedLabelingTests(TestCase):
    def test_guarded_packets_overlap_but_credit_and_refill_in_rank_order(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        rules = make_rules()
        first_risk, second_risk = (risk.id for risk in rules.risks)
        spec = fixture_spec(home)
        annotation = spec.annotation.model_copy(
            update={"workers": 2, "batch": batch_config(home).model_copy(update={"batch_size": 1})}
        )
        spec = spec.model_copy(update={"rules": rules, "annotation": annotation})
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
        publish_quote = "`probe publish` publishes the named item."
        first_started = Event()
        finished = {identity: Event() for identity, _ in rows}
        calls: list[str] = []
        calls_lock = Lock()

        def provider(
            _base: str, body: bytes, *_args: str | float | Path | CapacityConfig | None
        ) -> HTTPResponse:
            (row,), results = request_parts(body)
            identity = str(row["candidate"]["candidate_id"])
            if results:
                if identity == "lower" and len(results) == 2:
                    evidence = results[-1]["page"]["evidence_ids"][identity]
                    return tool_reply(
                        "finalize", {"outcomes": [submitted(row, [evidence], quote=publish_quote)]}
                    )
                return shared_retrieval(body)
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
            return shared_retrieval(body)

        def observe_packet(
            packet: list[CandidateRecord],
            prepared: Mapping[str, RowInput],
            rules: LabelingRules,
            model: ModelConfig,
            config: BatchLabelingConfig,
            out: Path,
            execution: BatchExecution,
            maximum_context_characters: int,
            generation: LabelingGeneration | None,
        ) -> list[LabelingAttempt]:
            attempts = run_prepared_batches(
                packet,
                prepared,
                rules,
                model,
                config,
                out,
                execution,
                maximum_context_characters,
                generation,
            )
            (candidate,) = packet
            finished[candidate.candidate_id].set()
            return attempts

        with (
            live_mcp_fixture({URL: f"{QUOTE}\n\n{publish_quote}"}) as mcp,
            patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider),
            patch("codebench_pipeline.grounding_tables.run_prepared_batches", side_effect=observe_packet),
            patch("codebench_pipeline.batch_labeling_ranked.SUBRISK_QUOTA", 2),
        ):
            labeling = bound_labeling(spec)
            attempts = labeling.attempts(filtered, home / "stage")

        self.assertCountEqual(calls, ["a-first", "b-duplicate", "c-refill", "lower"])
        self.assertEqual([call[0] for call in mcp].count("fetch_page_content"), 4)
        verify_live_support(attempts, labeling.batch.grounding)
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

    def test_checkpoints_resume_same_generation_but_reinfer_after_input_or_io_change(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        rules = make_rules()
        spec = fixture_spec(home)
        annotation = spec.annotation.model_copy(
            update={"workers": 1, "batch": batch_config(home).model_copy(update={"batch_size": 1})}
        )
        spec = spec.model_copy(update={"rules": rules, "annotation": annotation})
        candidates = [make_candidate("first"), make_candidate("later")]
        filtered = validated_frame(
            [
                {
                    **candidate.model_dump(mode="json"),
                    "filter_decision": "eligible",
                    "filter_reason": "",
                    "source_weight": 0.8,
                }
                for candidate in candidates
            ],
            FilteredCommandsTable,
        )
        calls: list[str] = []

        def provider(
            _base: str, body: bytes, *_args: str | float | Path | CapacityConfig | None
        ) -> HTTPResponse:
            (row,), results = request_parts(body)
            if not results:
                calls.append(str(row["candidate"]["candidate_id"]))
            if len(calls) == 2 and len(results) == 1:
                raise CapacityUnavailable("fixture side load")
            return shared_retrieval(body)

        mcp = self.enterContext(live_mcp_fixture({URL: QUOTE}))
        self.enterContext(patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider))
        labeling = bound_labeling(spec)
        checkpoint = LabelingCheckpointCache(home / "cache", home / "operations")
        settings = labeling.mapped

        def generation(
            commands: DataFrame[FilteredCommandsTable],
            storage: str,
        ) -> str:
            signature = stage_signature(
                "labeling_attempts",
                settings,
                {"commands": commands},
                storage_identity=storage,
            )
            return labeling_generation_identity(spec.dataset_id, signature, "/fixture/model")

        io = spec.storage.model_dump_json()
        identity = generation(filtered, io)
        with self.assertRaises(CapacityUnavailable):
            labeling.attempts_for_generation(filtered, home / "failed", checkpoint, identity)
        retry = bound_labeling(spec)
        resumed = retry.attempts_for_generation(filtered, home / "resumed", checkpoint, identity)
        self.assertEqual(calls, ["first", "later", "later"])
        row_dir = Path("label") / sha256(b"first").hexdigest()
        original = home / "failed" / row_dir / "receipt.json"
        restored = home / "resumed" / row_dir
        self.assertEqual((restored / "receipt.json").read_bytes(), original.read_bytes())
        self.assertTrue((restored / "reuse.json").is_file())
        verify_live_support(resumed, retry.batch.grounding)
        self.assertEqual(
            [name for name, _ in mcp],
            [
                "search_web_pages",
                "fetch_page_content",
                "search_web_pages",
                "search_web_pages",
                "fetch_page_content",
            ],
        )

        revised = filtered.copy()
        revised.loc[revised["candidate_id"] == "first", "source_weight"] = 0.79
        replay = retry.attempts_for_generation(
            revised, home / "new-input", checkpoint, generation(revised, io)
        )
        self.assertEqual(calls[-2:], ["later", "first"])
        self.assertEqual(len(calls), 5)
        proposal = proposed_from_attempts(replay, ProposalRules.from_rules(rules)).set_index("candidate_id")
        self.assertEqual(proposal.loc["first", "source_weight"], 0.79)

        changed_io = spec.storage.model_copy(update={"sample_rows": spec.storage.sample_rows + 1})
        retry.attempts_for_generation(
            revised, home / "new-io", checkpoint, generation(revised, changed_io.model_dump_json())
        )
        self.assertEqual(calls[-2:], ["later", "first"])
        self.assertEqual(len(calls), 7)

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
            ("ungrounded", "probe inspect undocumented", 0.8, [first_risk]),
            ("filler", "probe inspect filler", 0.6, [first_risk]),
            ("deferred", "probe inspect deferred", 0.3, [first_risk]),
            ("ambiguous", "probe inspect both", 0.3, [first_risk, second_risk]),
        ):
            candidates.append(
                make_candidate(identity).model_copy(update={"command": command, "risk_candidates": risks})
            )
            weights[identity] = weight
        config = batch_config(home)
        inferred: list[str] = []
        amendments = {
            "uncertain": {"class_confidence": "medium"},
            "unaccepted": {"decision": "needs_review"},
            "ambiguous": {"risk_id": second_risk},
        }

        def judge(
            _base: str, body: bytes, *_args: str | float | Path | CapacityConfig | None
        ) -> HTTPResponse:
            rows, results = request_parts(body)
            identities = [str(row["candidate"]["candidate_id"]) for row in rows]
            if not results:
                inferred.extend(identities)
            if len(results) < 2:
                return shared_retrieval(body)
            evidence = results[1]["page"]["evidence_ids"]
            outcomes = []
            for row, identity in zip(rows, identities, strict=True):
                quote = (
                    "Forged citation that is not present on the fetched page."
                    if identity == "ungrounded"
                    else QUOTE
                )
                outcome = submitted(row, [evidence[identity]], quote=quote)
                outcome["label"].update(amendments.get(identity, {}))
                outcomes.append(outcome)
            return tool_reply("finalize", {"outcomes": outcomes})

        with (
            live_mcp_fixture({URL: QUOTE}),
            patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=judge),
        ):
            scheduler = RankedLabeling(
                LabelingRules.from_rules(rules),
                remote_model(),
                config,
                home / "stage",
                100_000,
                execution=BatchExecution(
                    None, grounding=GroundingCredentials.from_environment(config.grounding)
                ),
            )
            attempts, _ = scheduler.run(list(reversed(candidates)), weights, [])

        observed_weights = [weights[identity] for identity in inferred]
        self.assertEqual(observed_weights, sorted(observed_weights, reverse=True))
        self.assertCountEqual(inferred, weights.keys() - {"deferred"})
        by_id = {attempt.candidate_id: attempt for attempt in attempts}
        self.assertEqual(by_id["deferred"].failure_code, QUOTA_DEFERRED)
        self.assertEqual(by_id["ungrounded"].status, "failure")
        self.assertEqual(by_id["ungrounded"].documentation, [])
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

    def test_live_documentation_failure_blocks_only_compatible_subrisk(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        rules = make_rules()
        candidate = make_candidate().model_copy(update={"command": "probe inspect row"})
        config = batch_config(home)

        def provider(
            _base: str, body: bytes, *_args: str | float | Path | CapacityConfig | None
        ) -> HTTPResponse:
            (row,), results = request_parts(body)
            if not results:
                return shared_retrieval(body)
            outcome = {
                **submitted(row, []),
                "label": None,
                "grounding_rationale": "",
                "failure_code": "missing_operation_documentation",
                "failure_detail": "The documentation search yielded no results.",
            }
            return tool_reply("finalize", {"outcomes": [outcome]})

        with (
            live_mcp_fixture({}) as calls,
            patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider),
        ):
            scheduler = RankedLabeling(
                LabelingRules.from_rules(rules),
                remote_model(),
                config,
                home / "stage",
                100_000,
                execution=BatchExecution(
                    None, grounding=GroundingCredentials.from_environment(config.grounding)
                ),
            )
            (attempt,), _ = scheduler.run([candidate], {candidate.candidate_id: 0.9}, [])

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "search_web_pages")
        self.assertEqual((attempt.status, attempt.documentation), ("failure", []))
        self.assertEqual(attempt.failure_code, "missing_operation_documentation")
        receipt = json.loads((scheduler.out / QUOTA_RECEIPT).read_bytes())
        blocked, empty = (receipt["subrisks"][risk.id] for risk in rules.risks)
        self.assertEqual(blocked["state"], "scope_blocked")
        self.assertIsNone(blocked["shortage"])
        self.assertEqual(blocked["remaining_to_target"], 250)
        self.assertEqual(blocked["source_unverified_candidates"], 0)
        self.assertEqual(blocked["scheduled_candidates"], 1)
        self.assertEqual(blocked["blocked_candidates"], 1)
        self.assertEqual(blocked["failure_counts"], {"missing_operation_documentation": 1})
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
            LabelingRules.from_rules(rules),
            remote_model(),
            batch_config(home).model_copy(update={"batch_size": 1}),
            home / "stage",
            100_000,
        )
        weights = {candidate.candidate_id: 0.8 for candidate in [*candidates, unknown]}
        with patch(
            "codebench_pipeline.batch_labeling_ranked.annotate_batches_bound",
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
