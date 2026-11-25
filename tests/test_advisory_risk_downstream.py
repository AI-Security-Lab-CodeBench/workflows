import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from test_batch_labeling import batch_config, remote_model
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.batch_labeling_ranked import QUOTA_DEFERRED, QUOTA_RECEIPT, RankedLabeling
from codebench_pipeline.category_eligibility import category_eligible_annotations
from codebench_pipeline.dataframe_schemas import LabelingAttemptsTable, validated_frame
from codebench_pipeline.dataset_export import prepare_dataset
from codebench_pipeline.dataset_records import OTHER_RISK_ID, CandidateRecord
from codebench_pipeline.dataset_tables import review_queue
from codebench_pipeline.labeling_outcomes import LabelingAttempt, failed_attempt, label_attempt
from codebench_pipeline.labeling_stage_settings import LabelingRules, ProposalRules
from codebench_pipeline.labeling_tables import failures_from_attempts, proposed_from_attempts
from codebench_pipeline.storage import fingerprint


class AdvisoryRiskDownstreamTests(TestCase):
    def test_quotas_follow_resolved_families_and_only_defer_after_all_are_full(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        rules = make_rules()
        first, second = (risk.id for risk in rules.risks)
        candidates = [
            make_candidate(identity).model_copy(
                update={"command": f"probe inspect {identity}", "risk_candidates": hints}
            )
            for identity, hints in (
                ("a", [first]),
                ("b-overflow", [first]),
                ("c-no-fit", []),
                ("d-off-hint", [first]),
                ("e-defer", [second]),
            )
        ]
        docs = documentation()
        resolved = {"a": first, "b-overflow": first, "c-no-fit": OTHER_RISK_ID, "d-off-hint": second}

        def judge(packet: list[CandidateRecord], _path: Path) -> list[LabelingAttempt]:
            attempts: list[LabelingAttempt] = []
            for candidate in packet:
                identity = candidate.candidate_id
                risk = resolved[identity]
                original = make_label(make_candidate(identity), rules)
                label = original.model_copy(
                    update={
                        "risk_id": risk,
                        "decision": "reject" if risk == OTHER_RISK_ID else "accept",
                        "documentation_urls": [docs[0].url],
                    }
                )
                attempts.append(
                    label_attempt(candidate, label, docs, fingerprint(identity), f"label/{identity}/receipt.json")
                )
            return attempts

        source = make_candidate("unavailable")
        source_failure = failed_attempt(
            source, "source_unverified", "Synthetic source failure.", [], fingerprint("source"),
            "label/unavailable/receipt.json",
        )
        scheduler = RankedLabeling(
            LabelingRules.from_rules(rules), remote_model(), batch_config(home), home / "quota", 100_000
        )
        weights = dict.fromkeys([row.candidate_id for row in candidates] + [source.candidate_id], 0.9)
        with patch("codebench_pipeline.batch_labeling_ranked.SUBRISK_QUOTA", 1):
            outcomes, _ = scheduler.run_packets(candidates, weights, [source_failure], judge)

        self.assertEqual([attempt.status for attempt in outcomes], ["success"] * 4 + ["failure"] * 2)
        self.assertEqual(outcomes[2].risk_candidates, [])
        self.assertEqual(outcomes[3].risk_candidates, [first])
        self.assertEqual(outcomes[4].failure_code, QUOTA_DEFERRED)
        self.assertEqual(outcomes[5], source_failure)
        quotas = json.loads((scheduler.out / QUOTA_RECEIPT).read_bytes())["subrisks"]
        self.assertEqual(set(quotas), {first, second})
        for risk, credited in ((first, "a"), (second, "d-off-hint")):
            self.assertEqual(
                quotas[risk],
                {
                    "eligible_candidates": 6,
                    "scheduled_candidates": 4,
                    "deferred_candidates": 1,
                    "source_unverified_candidates": 1,
                    "verified_candidates": 4,
                    "blocked_candidates": 1,
                    "failure_counts": {"source_unverified": 1},
                    "credited_candidates": [credited],
                    "remaining_to_target": 0,
                    "shortage": 0,
                    "state": "target_met",
                },
            )

    def test_proposals_keep_no_fit_evidence_but_only_publish_approved_configured_risks(self) -> None:
        rules = make_rules()
        first, second = (risk.id for risk in rules.risks)
        policy = ProposalRules.from_rules(rules)
        docs = documentation()
        candidates = [make_candidate(identity, first) for identity in ("off-hint", "no-fit", "failure")]
        labels = [
            make_label(candidate, rules).model_copy(
                update={
                    "risk_id": risk,
                    "decision": decision,
                    "risk_confidence": confidence,
                    "documentation_urls": [docs[0].url],
                }
            )
            for candidate, risk, decision, confidence in (
                (candidates[0], second, "needs_review", "medium"),
                (candidates[1], OTHER_RISK_ID, "reject", "high"),
            )
        ]
        attempts = [
            label_attempt(candidate, label, docs, fingerprint(candidate.candidate_id), "label/receipt.json")
            for candidate, label in zip(candidates[:2], labels, strict=True)
        ]
        attempts.append(
            failed_attempt(candidates[2], "unsupported", "Synthetic evidence failure.", [], "a" * 64, "fail")
        )
        table = validated_frame([attempt.model_dump(mode="json") for attempt in attempts], LabelingAttemptsTable)
        proposed = proposed_from_attempts(table, policy)
        eligible = category_eligible_annotations(proposed, rules.category_id)

        self.assertEqual(proposed["risk_id"].tolist(), [second, OTHER_RISK_ID])
        self.assertEqual(attempts[1].risk_candidates, [first])
        self.assertEqual(attempts[1].documentation, docs)
        self.assertEqual(failures_from_attempts(table)["failure_code"].tolist(), ["unsupported"])
        self.assertEqual(
            eligible.attrs["category_exclusions"],
            [{"candidate_id": "no-fit", "reasons": ["no_configured_risk"]}],
        )
        self.assertEqual(review_queue(eligible, policy, ("risk_id",))["candidate_id"].tolist(), ["off-hint"])
        release = prepare_dataset(candidates[:1], labels[:1], rules, 1, approved_ids=frozenset({"off-hint"}))
        self.assertEqual([row["risk"] for row in release.commands], [rules.risks[1].canonical])

    def test_direct_export_refuses_no_fit_without_trusted_row_documentation(self) -> None:
        rules = make_rules()
        candidate = make_candidate("no-fit")
        label = make_label(candidate, rules).model_copy(
            update={
                "risk_id": OTHER_RISK_ID,
                "decision": "reject",
                "documentation_urls": [documentation()[0].url],
            }
        )
        with self.assertRaisesRegex(ValueError, "supporting row documentation"):
            prepare_dataset([candidate], [label], rules, 1)
