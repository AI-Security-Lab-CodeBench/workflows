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
    TerminalRecovery,
    TrainEvidence,
)
from codebench_pipeline.rules_contracts import (
    Category,
    Diagnostic,
    EvaluationError,
    EvaluationReceipt,
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
    CaptureRecoveryReason,
    DedupResult,
    DiffInput,
    FinishInput,
    JudgeCandidate,
    JudgeClosure,
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
        return sum(item.run.status == "finished" and bool((item.justification or "").strip()) for item in self.batches)


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


def _capture_failure(receipt: EvaluationReceipt) -> str | None:
    if not receipt.fixtures_passed:
        return "fixture_mismatch"
    if any(row.error is not None for row in receipt.rows):
        return "train_error"
    if any(row.rule_ids and not row.passed for row in receipt.rows):
        return "wrong_match"
    return None if any(row.rule_ids for row in receipt.rows) else "no_train_hits"


def _closure(batch: CaptureBatch, accepted: tuple[CapturedRule, ...], codes: tuple[str, ...]) -> CaptureClosure:
    covered = {row_id for entry in accepted for hit in entry.singleton.hit_sets for row_id in hit.row_ids}
    abstained = tuple(row.id for row in batch.rows if row.id not in covered)
    return CaptureClosure(
        batch_id=batch.id, accepted_rule_ids=tuple(entry.package.rule.id for entry in accepted),
        uncovered_row_ids=abstained, diagnostic_codes=codes, validated_rule_count=len(accepted),
        covered_row_count=len(batch.rows) - len(abstained), abstained_row_count=len(abstained),
    )


@dataclass(frozen=True, slots=True)
class _CaptureWork:
    batch: CaptureBatch
    previous: CaptureDecision | None = None

    def replay(self, train: _Train) -> None:
        previous = self.previous
        if previous is None:
            return
        batch = self.batch
        if (
            previous.batch_id != batch.id or previous.row_ids != tuple(row.id for row in batch.rows)
            or (previous.descriptor_sha256 is not None and previous.descriptor_sha256 != batch.descriptor_sha256)
        ):
            raise EvaluationError(Diagnostic(code="capture_resume_batch", message="Pinned batch descriptor changed"))
        for index, entry in enumerate(previous.rules, 1):
            rule = entry.package.rule
            if rule.id != f"{batch.id}-{index:02d}" or rule.labels.category != batch.category:
                raise EvaluationError(Diagnostic(code="capture_resume_rule", message="Pinned rule identity changed"))
            train.authorize(entry.package)
            receipt = evaluate_package(train.client, entry.package, train.corpus)
            if _capture_failure(receipt) or receipt != entry.singleton:
                raise EvaluationError(Diagnostic(
                    code="capture_resume_receipt", rule_id=rule.id, message="Pinned rule gate or receipt changed",
                ))
        if previous.run.status == "finished":
            slate = _closure(batch, previous.rules, ())
            if (
                not previous.run.request_sha256 or not (previous.justification or "").strip()
                or (previous.recovery_reason is not None
                    and (previous.recovery_reason != slate.reason or previous.justification != slate.narrative))
            ):
                raise EvaluationError(Diagnostic(
                    code="capture_resume_closure", message="Pinned finished batch lacks a valid terminal decision",
                ))


class _NoArguments(ConfigArtifact):
    pass


def _capture_batch(agent: RuleAgent, train: _Train, work: _CaptureWork) -> CaptureDecision:
    batch, previous = work.batch, work.previous
    if previous is not None and previous.run.status == "finished":
        return previous.model_copy(update={"descriptor_sha256": batch.descriptor_sha256})
    evidence = TrainEvidence()
    accepted = list(previous.rules) if previous is not None else []
    prior_runs = () if previous is None else (*previous.prior_runs, previous.run)
    prior_codes = tuple(failure.diagnostic.code for attempt in prior_runs for failure in attempt.failures)
    justification: str | None = None
    reason: CaptureRecoveryReason | None = None

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
        if code := _capture_failure(receipt):
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
        return _closure(batch, tuple(accepted), tuple(dict.fromkeys((*prior_codes, *codes))))

    def recover(codes: tuple[str, ...]) -> TerminalRecovery:
        slate = closure(codes)

        def select(arguments: str) -> Feedback:
            nonlocal justification, reason
            _NoArguments.model_validate_json(arguments)
            reason = slate.reason
            justification = slate.narrative
            return Feedback(recorded=slate.accepted_rule_ids)

        return TerminalRecovery(slate, AgentTool(
            slate.reason.value, f"Close the batch with this evidenced finding: {slate.narrative}", _NoArguments, select,
        ))

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
        recovery=recover,
        tools=(
            AgentTool("add_rule", "Evaluate a package and record it only if it passes.", Package, add_rule),
            AgentTool("page_evaluation", "Read a train-only receipt page.", PageInput, evidence.page),
            AgentTool("finish", "Record why you are done with this neighborhood.", FinishInput, finish),
        ),
    ).run(agent, recover_only=bool(accepted))
    return CaptureDecision(
        batch_id=batch.id, row_ids=tuple(row.id for row in batch.rows), justification=justification,
        rules=tuple(accepted), run=run, descriptor_sha256=batch.descriptor_sha256,
        recovery_reason=reason, prior_runs=prior_runs,
    )


def capture_rules(
    agent: RuleAgent, train: Corpus, client: BashguardClient, *, category: Category | None = None,
    resume: CaptureResult | None = None,
) -> CaptureResult:
    execution = _Train(train, client)
    plan = capture_batches(train, category)
    if resume is None:
        work = tuple(_CaptureWork(batch) for batch in plan)
    else:
        if (
            resume.corpus_digest != train.digest or resume.runtime_version != client.runtime_version
            or len(resume.batches) != len(plan)
        ):
            raise EvaluationError(Diagnostic(code="capture_resume_lineage", message="Pinned capture scope or lineage changed"))
        work = tuple(_CaptureWork(batch, previous) for batch, previous in zip(plan, resume.batches, strict=True))
        for item in work:
            item.replay(execution)
    batches = tuple(_capture_batch(agent, execution, item) for item in work)
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

        def apply_diff(diff: DiffInput) -> Feedback:
            nonlocal finalized
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

        def finalize(arguments: str) -> Feedback:
            return apply_diff(DiffInput.model_validate_json(arguments))

        def keep_rules(arguments: str) -> Feedback:
            choice = FinishInput.model_validate_json(arguments)
            return apply_diff(DiffInput(drop_ids=(), add=(), justification=choice.justification))

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

        def recover(codes: tuple[str, ...]) -> TerminalRecovery:
            return TerminalRecovery(
                JudgeClosure(nominations=payload.nominations, diagnostic_codes=codes),
                AgentTool(
                    "keep_rules", "Explicitly propose keeping every nominated rule with a short evidence-based "
                    "justification. If a change is warranted or the evidence is insufficient, do not call this tool. "
                    "No unresolved edit, fixture or drop is implicitly accepted.", FinishInput, keep_rules,
                ),
            )

        run = Conversation(
            prompt=SAFETY_PROMPT + " Judge nominated rules, treating similarity as a hint, not proof. Compare complete "
            "label tuples and inspect shared and unique train hits; nominations only show up to three IDs per "
            "hit set, and singleton receipt pages (kind=matches) hold all matching rows. Read-only evaluate_rule cannot modify "
            "anything. Submit one atomic finalize with drop_ids, fresh add packages, and a justification. Explain "
            "each drop, any old tests left behind, and why any unique coverage is intentionally relinquished. "
            "There is no obligation to preserve old tests or coverage. If keeping all, submit empty add/drop lists "
            "with your reason. New packages need fresh IDs using new_id_prefixes and a per-category 01, 02, ... suffix.",
            payload=payload, terminal="finalize", turns=_JUDGE_TURNS,
            closing_task="Submit the actual add/drop diff and your evidence-based justification. "
            "Use explicit drop_ids and add lists; submit two empty lists only if you justify keeping every rule.",
            recovery=recover,
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
