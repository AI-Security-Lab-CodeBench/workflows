from collections import Counter
from dataclasses import dataclass, field
from typing import Final

from codebench_pipeline.models import Digest
from codebench_pipeline.rules_agents import (
    SAFETY_PROMPT,
    AgentTool,
    Conversation,
    Feedback,
    RuleAgent,
    TrainEvidence,
)
from codebench_pipeline.rules_contracts import (
    Category,
    Diagnostic,
    EvaluationError,
    FinalizeError,
    FinalizeReceipt,
    FinalizeSubmission,
    Package,
)
from codebench_pipeline.rules_corpus import Corpus
from codebench_pipeline.rules_engine import BashguardClient, evaluate_package, finalize_batch
from codebench_pipeline.rules_similarity import (
    FINISH_JUSTIFICATION_LIMIT,
    CaptureBatch,
    CaptureClosure,
    CaptureDecision,
    CapturedRule,
    DedupResult,
    DiffInput,
    FinishInput,
    JudgeCandidate,
    JudgeDecision,
    JudgeInput,
    JudgePair,
    Nomination,
    PageInput,
    capture_batches,
    judge_group,
    nominate_rules,
    review_examples,
)
from codebench_pipeline.typed_config import ConfigArtifact

_CAPTURE_TURNS: Final = 8
_JUDGE_TURNS: Final = 7
_MAX_JUDGE_BATCHES: Final = 32


class CaptureResult(ConfigArtifact):
    corpus_digest: Digest
    runtime_version: str
    batches: tuple[CaptureDecision, ...]

    @property
    def rules(self) -> tuple[CapturedRule, ...]:
        return tuple(rule for batch in self.batches for rule in batch.rules)

    @property
    def packages(self) -> tuple[Package, ...]:
        return tuple(entry.package for entry in self.rules)

    @property
    def complete(self) -> bool:
        return bool(self.batches) and self.finished_batches == len(self.batches)

    @property
    def finished_batches(self) -> int:
        return sum(item.run.status == "finished" for item in self.batches)


@dataclass(frozen=True, slots=True)
class _Train:
    corpus: Corpus
    client: BashguardClient

    def authorize(self, package: Package) -> None:
        rule = package.rule
        if not set(rule.sources).issubset(self.corpus.sources_for(rule.labels.category)):
            raise EvaluationError(Diagnostic(
                code="unauthorized_source", phase="validate", rule_id=rule.id, field="sources",
                message="Each source must exactly match a pinned manifest URL for the rule category.",
            ))
        if not any(test.expected == rule.labels for test in package.tests) or not any(
            test.expected.category == "other" for test in package.tests
        ):
            raise EvaluationError(Diagnostic(
                code="fixture_support", phase="validate", rule_id=rule.id, field="tests",
                message="Provide a positive fixture for this label tuple and at least one other/abstain near miss.",
            ))


def _capture_batch(agent: RuleAgent, train: _Train, batch: CaptureBatch) -> CaptureDecision:
    evidence = TrainEvidence()
    accepted: list[CapturedRule] = []
    justification: str | None = None

    def add_rule(arguments: str) -> Feedback:
        package = Package.model_validate_json(arguments)
        rule = package.rule
        expected_id = f"{batch.id}-{len(accepted) + 1:02d}"
        if rule.id != expected_id or rule.labels.category != batch.category:
            return Feedback(next_rule_id=expected_id, diagnostic=Diagnostic(
                code="capture_identity", phase="validate", rule_id=rule.id,
                message="Use next_rule_id and the assigned category. Failed drafts do not consume IDs.",
            ))
        train.authorize(package)
        receipt = evaluate_package(train.client, package, train.corpus)
        view = evidence.record(receipt)
        if view.fixture_failures or view.train_wrong_matches or view.train_errors or not view.train_matches:
            code = (
                "fixture_mismatch" if view.fixture_failures else
                "train_error" if view.train_errors else "wrong_match" if view.train_wrong_matches else "no_train_hits"
            )
            return Feedback(
                diagnostic=Diagnostic(
                    code=code, phase="gate", rule_id=rule.id,
                    message="Candidate must pass its fixtures and make at least one correct train match with no errors.",
                ),
                receipts=(view,), examples=review_examples((receipt,)), next_rule_id=expected_id,
            )
        accepted.append(CapturedRule(package=package, singleton=receipt))
        return Feedback(recorded=(rule.id,), receipts=(view,), next_rule_id=f"{batch.id}-{len(accepted) + 1:02d}")

    def finish(arguments: str) -> Feedback:
        nonlocal justification
        justification = FinishInput.model_validate_json(arguments).justification
        return Feedback(recorded=tuple(entry.package.rule.id for entry in accepted))

    def closure(codes: tuple[str, ...]) -> CaptureClosure:
        covered = {row_id for entry in accepted for hit in entry.singleton.hit_sets for row_id in hit.row_ids}
        return CaptureClosure(
            batch_id=batch.id, accepted_rule_ids=tuple(entry.package.rule.id for entry in accepted),
            uncovered_row_ids=tuple(row.id for row in batch.rows if row.id not in covered), diagnostic_codes=codes,
        )

    run = Conversation(
        prompt=SAFETY_PROMPT + " Capture narrowly supported rules for this category and token neighborhood. "
        "Use add_rule with next_rule_id; acceptance evaluates all train categories, not just this batch. "
        f"Finish with a brief justification (at most {FINISH_JUSTIFICATION_LIMIT} characters) "
        "that says what remains uncovered.",
        payload=batch, terminal="finish", turns=_CAPTURE_TURNS,
        closing_task="Acknowledge only the supplied batch outcome. Give one short, nonblank justification "
        f"(at most {FINISH_JUSTIFICATION_LIMIT} characters) stating whether any rows remain uncovered. "
        "It is valid to report that no rule was recorded and the batch is uncovered. "
        "Do not invent rule IDs, evidence, or reasons.",
        acknowledgement=closure,
        tools=(
            AgentTool("add_rule", "Evaluate a package and record it only if it passes.", Package, add_rule),
            AgentTool("page_evaluation", "Read a train-only receipt page.", PageInput, evidence.page),
            AgentTool("finish", "Record why you are done with this neighborhood.", FinishInput, finish),
        ),
    ).run(agent)
    return CaptureDecision(
        batch_id=batch.id, row_ids=tuple(row.id for row in batch.rows), justification=justification,
        rules=tuple(accepted), run=run,
    )


def capture_rules(
    agent: RuleAgent, train: Corpus, client: BashguardClient, *, category: Category | None = None,
) -> CaptureResult:
    execution = _Train(train, client)
    batches = tuple(_capture_batch(agent, execution, batch) for batch in capture_batches(train, category))
    return CaptureResult(corpus_digest=train.digest, runtime_version=client.runtime_version, batches=batches)


@dataclass(frozen=True, slots=True)
class _JudgeBatch:
    id: str
    nominees: tuple[CapturedRule, ...]
    pairs: tuple[Nomination, ...]
    base: tuple[Package, ...]
    evidence: TrainEvidence = field(default_factory=TrainEvidence)

    def run(self, agent: RuleAgent, train: _Train) -> JudgeDecision:
        finalized: FinalizeReceipt | None = None
        ids = tuple(entry.package.rule.id for entry in self.nominees)
        categories = {entry.package.rule.labels.category for entry in self.nominees}
        prefixes = {category: f"{category.lower()}.{self.id}-" for category in sorted(categories)}

        def evaluate(arguments: str) -> Feedback:
            package = Package.model_validate_json(arguments)
            train.authorize(package)
            receipt = evaluate_package(train.client, package, train.corpus)
            return Feedback(receipts=(self.evidence.record(receipt),), examples=review_examples((receipt,)))

        def finalize(arguments: str) -> Feedback:
            nonlocal finalized
            diff = DiffInput.model_validate_json(arguments)
            if not set(diff.drop_ids).issubset(ids):
                raise EvaluationError(Diagnostic(code="drop_scope", phase="validate", field="drop_ids",
                                                 message="Only the explicitly nominated IDs can be dropped."))
            counts: Counter[str] = Counter()
            for package in diff.add:
                train.authorize(package)
                rule = package.rule
                counts[rule.labels.category] += 1
                prefix = prefixes.get(rule.labels.category)
                if prefix is None or rule.id != f"{prefix}{counts[rule.labels.category]:02d}":
                    raise EvaluationError(Diagnostic(
                        code="fresh_id", phase="validate", rule_id=rule.id, field="add.rule.id",
                        message="Use the batch's category prefix and successive two-digit IDs starting at 01.",
                    ))
            try:
                receipt = finalize_batch(
                    train.client, self.base,
                    FinalizeSubmission(drop=diff.drop_ids, add=diff.add, justification=diff.justification),
                    train.corpus,
                )
            except FinalizeError as exc:
                return Feedback(
                    diagnostic=exc.diagnostic, receipts=tuple(self.evidence.record(item) for item in exc.evidence),
                    examples=review_examples(exc.evidence),
                )
            finalized = receipt
            return Feedback(
                recorded=tuple(entry.rule.id for entry in diff.add),
                receipts=tuple(self.evidence.record(item) for item in (*receipt.additions, receipt.combined)),
            )

        payload = JudgeInput(
            candidates=tuple(JudgeCandidate(
                package=entry.package, singleton=self.evidence.record(entry.singleton)
            ) for entry in self.nominees),
            nominations=tuple(JudgePair(
                left_id=pair.left_id, right_id=pair.right_id, same_tuple=pair.same_tuple,
                semantic_cosine=pair.semantic_cosine,
                shared_hit_count=len(pair.shared_hits), left_only_hit_count=len(pair.left_only_hits),
                right_only_hit_count=len(pair.right_only_hits),
                shared_examples=pair.shared_hits[:3], left_only_examples=pair.left_only_hits[:3],
                right_only_examples=pair.right_only_hits[:3],
            ) for pair in self.pairs),
            new_id_prefixes=tuple(prefixes.values()),
            sources=tuple(source for source in train.corpus.sources if source.category in categories),
        )
        run = Conversation(
            prompt=SAFETY_PROMPT + " Judge nominated rules, treating similarity as a hint, not proof. Compare complete "
            "label tuples and inspect shared and unique train hits; nominations only show up to three IDs per "
            "hit set, and singleton receipt pages hold the full evidence. Read-only evaluate_rule cannot modify "
            "anything. Submit one atomic finalize with drop_ids, fresh add packages, and a justification. Explain "
            "each drop, any old tests left behind, and why any unique coverage is intentionally relinquished. "
            "There is no obligation to preserve old tests or coverage. If keeping all, submit empty add/drop lists "
            "with your reason. New packages need fresh IDs using new_id_prefixes and a per-category 01, 02, ... suffix.",
            payload=payload, terminal="finalize", turns=_JUDGE_TURNS,
            closing_task="Submit the actual add/drop diff and your evidence-based justification. "
            "Use explicit drop_ids and add lists; submit two empty lists only if you justify keeping every rule.",
            tools=(
                AgentTool("evaluate_rule", "Evaluate a package read-only on its fixtures and full train.", Package, evaluate),
                AgentTool("page_evaluation", "Read a train-only receipt page.", PageInput, self.evidence.page),
                AgentTool("finalize", "Atomically submit a justified add/drop diff.", DiffInput, finalize),
            ),
        ).run(agent)
        return JudgeDecision(batch_id=self.id, candidate_ids=ids, nominations=self.pairs, run=run, finalized=finalized)


def deduplicate_rules(
    agent: RuleAgent, train: Corpus, client: BashguardClient, capture: CaptureResult,
) -> DedupResult:
    if train.split != "train" or capture.corpus_digest != train.digest or capture.runtime_version != client.runtime_version:
        raise EvaluationError(Diagnostic(code="capture_lineage", message="Dedup requires the same train and evaluator"))
    if not capture.complete:
        raise EvaluationError(Diagnostic(code="capture_incomplete", message="Cannot deduplicate unfinished capture"))
    nominations = nominate_rules(capture.rules)
    originals = {entry.package.rule.id: entry for entry in capture.rules}
    base = capture.packages
    pending: tuple[Nomination, ...] = nominations
    decisions: list[JudgeDecision] = []
    while pending and len(decisions) < _MAX_JUDGE_BATCHES:
        active = {package.rule.id for package in base}
        pending = tuple(pair for pair in pending if pair.in_scope(active))
        if not pending:
            break
        ids, pairs = judge_group(pending)
        decision = _JudgeBatch(
            f"judge-{len(decisions) + 1:04d}", tuple(originals[key] for key in sorted(ids)), pairs, base,
        ).run(agent, _Train(train, client))
        decisions.append(decision)
        if decision.finalized is not None:
            base = decision.finalized.packages
        pending = tuple(pair for pair in pending if not pair.in_scope(ids))
    return DedupResult(
        corpus_digest=train.digest, runtime_version=client.runtime_version, nominations=nominations,
        decisions=tuple(decisions),
        remaining_nominations=tuple(pair for pair in pending if pair.in_scope({item.rule.id for item in base})),
        packages=base,
    )
