from collections import Counter
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Final

from pydantic import Field, ValidationError, field_validator

from codebench_pipeline.live_grounding import LiveGrounding
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
from codebench_pipeline.rules_capture_candidates import (
    CandidateProposal,
    CaptureCandidate,
    capture_candidates,
)
from codebench_pipeline.rules_capture_facts import (
    AnalysisRow,
    CaptureRow,
    allow_shape,
    project_row,
)
from codebench_pipeline.rules_capture_grounding import (
    BatchDocumentation,
    CaptureGrounding,
    DocumentationFetch,
    DocumentationSearch,
)
from codebench_pipeline.rules_contracts import (
    Category,
    Diagnostic,
    EvaluationError,
    EvaluationInput,
    EvaluationReceipt,
    FinalizeError,
    FinalizeReceipt,
    FinalizeSubmission,
    NativeRule,
    Package,
)
from codebench_pipeline.rules_corpus import Corpus
from codebench_pipeline.rules_engine import (
    BashguardClient,
    evaluate_package,
    finalize_batch,
    require_filename_invariance,
)
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


@dataclass(slots=True)
class _Train:
    corpus: Corpus
    client: BashguardClient
    analyses: dict[str, AnalysisRow] = field(default_factory=dict, init=False)

    def prepare(self, package: Package) -> Package:
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
        shapes: tuple[tuple[str, ...], ...] = ()
        if rule.labels.action == "allow":
            source = self.analyze()
            observed = {allow_shape(source[row.id]) for row in self.corpus.rows if row.gold == rule.labels}
            positives = tuple(
                EvaluationInput(id=f"capture-positive:{index}", command=test.command)
                for index, test in enumerate(package.tests) if test.expected == rule.labels
            )
            supported = {allow_shape(row) for row in self.client.analyze(positives)}
            shapes = tuple(sorted(shape for shape in observed & supported if shape is not None))
            if not shapes or rule.scope == "ast":
                raise EvaluationError(Diagnostic(
                    code="opaque_allow", phase="gate", rule_id=rule.id, field="when",
                    message="Learned allows require a supported simple invocation present in both reviewed train and positive fixtures.",
                ))
        return package.model_copy(update={
            "rule": rule.model_copy(update={
                "capture_allow_shapes": shapes, "capture_guarded": True,
            }),
        })

    def analyze(self) -> dict[str, AnalysisRow]:
        if not self.analyses:
            rows = tuple(EvaluationInput(id=row.id, command=row.command) for row in self.corpus.rows)
            self.analyses.update((row.id, row) for row in self.client.analyze(rows))
        return self.analyses

    def facts(self) -> dict[str, CaptureRow]:
        analyses = self.analyze()
        return {row.id: project_row(row.id, row.gold, analyses[row.id]) for row in self.corpus.rows}


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

    def replay(self, train: _Train, analyses: dict[str, AnalysisRow]) -> None:
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
            if train.prepare(entry.package) != entry.package:
                raise EvaluationError(Diagnostic(
                    code="capture_resume_guard", rule_id=rule.id, message="Pinned learned-rule guard changed",
                ))
            receipt = evaluate_package(train.client, entry.package, train.corpus)
            if _capture_failure(receipt) or receipt != entry.singleton:
                raise EvaluationError(Diagnostic(
                    code="capture_resume_receipt", rule_id=rule.id, message="Pinned rule gate or receipt changed",
                ))
            require_filename_invariance(train.client, entry.package, receipt, analyses)
        if previous.run.status == "finished":
            slate = _closure(batch, previous.rules, ())
            try:
                _CaptureFinish(justification=previous.justification or "")
            except ValidationError as exc:
                raise EvaluationError(Diagnostic(
                    code="capture_resume_closure", message="Pinned finished batch lacks a substantive closure.",
                )) from exc
            if (
                not previous.run.request_sha256
                or (previous.recovery_reason is not None
                    and (previous.recovery_reason != slate.reason or previous.justification != slate.narrative))
            ):
                raise EvaluationError(Diagnostic(
                    code="capture_resume_closure", message="Pinned finished batch lacks a valid terminal decision",
                ))


class _NoArguments(ConfigArtifact):
    pass


class _CaptureFinish(FinishInput):
    justification: str = Field(min_length=16, max_length=FINISH_JUSTIFICATION_LIMIT)

    @field_validator("justification")
    @classmethod
    def reasoned_text(cls, value: str) -> str:
        if len(value.strip()) < 16 or sum(sum(char.isalpha() for char in word) >= 2 for word in value.split()) < 2:
            raise ValueError("State what was supported or why the uncovered rows could not be captured.")
        return value.strip()


class _CapturePrompt(CaptureBatch):
    public_cli_tokens: tuple[str, ...]
    documentation_choices: tuple[DocumentationSearch, ...]
    candidates: tuple[CaptureCandidate, ...]


def _capture_batch(
    agent: RuleAgent, train: _Train, work: _CaptureWork, facts: dict[str, CaptureRow],
    analyses: dict[str, AnalysisRow], grounding: LiveGrounding | None,
) -> CaptureDecision:
    batch, previous = work.batch, work.previous
    if previous is not None and previous.run.status == "finished":
        return previous
    rows = batch.rows
    evidence = TrainEvidence(capture_facts=facts)
    accepted = list(previous.rules) if previous is not None else []
    prior_runs = () if previous is None else (*previous.prior_runs, previous.run)
    prior_codes = tuple(failure.diagnostic.code for attempt in prior_runs for failure in attempt.failures)
    justification: str | None = None
    reason: CaptureRecoveryReason | None = None
    offered_candidates = {candidate.id: candidate for candidate in capture_candidates(rows, analyses)}

    def add_rule(arguments: str) -> Feedback:
        return admit(Package.model_validate_json(arguments))

    def admit(package: Package, supporting_rows: tuple[str, ...] = ()) -> Feedback:
        rule = package.rule
        expected_id = f"{batch.id}-{len(accepted) + 1:02d}"
        if rule.id != expected_id or rule.labels.category != batch.category:
            return Feedback(next_rule_id=expected_id, diagnostic=Diagnostic(
                code="capture_identity", phase="validate", rule_id=rule.id,
                message="Use next_rule_id and the assigned category. Failed drafts do not consume IDs.",
            ))
        package = train.prepare(package)
        receipt = evaluate_package(train.client, package, train.corpus)
        view = evidence.record(receipt)
        code = _capture_failure(receipt)
        if code is None and supporting_rows and not set(supporting_rows).issubset(
            row.id for row in receipt.rows if row.passed and row.rule_ids
        ):
            code = "candidate_witness_mismatch"
        if code is not None:
            examples = review_examples((receipt,))
            return Feedback(
                diagnostic=Diagnostic(
                    code=code, phase="gate", rule_id=rule.id,
                    message="Candidate must pass its fixtures and make at least one correct train match with no errors.",
                ),
                receipts=(view,), examples=evidence.model_rows(examples), masked_facts=evidence.model_facts(examples),
                next_rule_id=expected_id,
            )
        try:
            require_filename_invariance(train.client, package, receipt, analyses)
        except EvaluationError as exc:
            return Feedback(diagnostic=exc.diagnostic, receipts=(view,), next_rule_id=expected_id)
        accepted.append(CapturedRule(package=package, singleton=receipt))
        return Feedback(
            recorded=(rule.id,), receipts=(view,), next_rule_id=f"{batch.id}-{len(accepted) + 1:02d}",
            trace_status="produced",
        )

    def propose_candidate(arguments: str) -> Feedback:
        proposal = CandidateProposal.model_validate_json(arguments)
        candidate = offered_candidates.pop(proposal.candidate_id, None)
        if candidate is None:
            return Feedback(diagnostic=Diagnostic(
                code="capture_candidate", message="Copy an untried candidate ID from the batch evidence.",
            ))
        if proposal.source not in train.corpus.sources_for(candidate.labels.category):
            offered_candidates[candidate.id] = candidate
            return Feedback(diagnostic=Diagnostic(
                code="unauthorized_source", message="Choose an exact URL from the pinned sources for this batch.",
            ))
        return admit(Package(rule=NativeRule(
            id=f"{batch.id}-{len(accepted) + 1:02d}", scope=candidate.scope, when=candidate.when,
            labels=candidate.labels, rationale=proposal.rationale, sources=(proposal.source,),
        ), tests=candidate.fixtures), candidate.supporting_rows)

    def finish(arguments: str) -> Feedback:
        nonlocal justification
        justification = _CaptureFinish.model_validate_json(arguments).justification
        return Feedback(
            recorded=tuple(entry.package.rule.id for entry in accepted),
            trace_status="returned" if accepted else "abstained",
        )

    def closure(codes: tuple[str, ...]) -> CaptureClosure:
        return _closure(batch, tuple(accepted), tuple(dict.fromkeys((*prior_codes, *codes))))

    def recover(codes: tuple[str, ...]) -> TerminalRecovery:
        slate = closure(codes)

        def select(arguments: str) -> Feedback:
            nonlocal justification, reason
            _NoArguments.model_validate_json(arguments)
            reason = slate.reason
            justification = slate.narrative
            return Feedback(
                recorded=slate.accepted_rule_ids,
                trace_status="abstained" if slate.uncovered_row_ids else "returned",
            )

        return TerminalRecovery(slate, AgentTool(
            slate.reason.value, f"Close the batch with this evidenced finding: {slate.narrative}", _NoArguments, select,
        ))

    docs = None if grounding is None else BatchDocumentation(
        grounding, batch.id, frozenset(
            word.token for row in rows for call in row.calls for word in call
            if word.role == "program" and word.token is not None
        ), len(grounding.history),
    )
    documentation_tools: tuple[AgentTool, ...] = ()
    documentation_choices = docs.choices() if docs is not None else ()
    if docs is not None and documentation_choices:
        documentation_tools = (
            AgentTool(
                "search_documentation", "Optional: copy one unused program/topic pair from documentation_choices.",
                DocumentationSearch, lambda args: Feedback(documentation=docs.search(args)),
                available=lambda: bool(docs.choices()),
            ),
            AgentTool(
                "fetch_documentation", "Copy the exact handle from documentation.hits to fetch one untrusted excerpt.",
                DocumentationFetch, lambda args: Feedback(documentation=docs.fetch(args)),
                available=docs.can_fetch,
            ),
        )
    run = Conversation(
        prompt=SAFETY_PROMPT + " Capture narrowly supported rules for this category and masked role neighborhood. "
        "AST facts describe shell syntax, not the semantics of embedded programs or external commands. "
        "Keep opaque bodies and unknown values out of predicates; abstain when the matching label cannot be proved. "
        "Use structural controls and documented selectors, not source text, paths, extensions, or sample operands. "
        "Start with the listed candidates or a narrow predicate using public_cli_tokens and AST facts. "
        "A candidate's supporting_rows and synthetic fixtures are hypotheses to check; propose_candidate submits "
        "one to the same train sample and fixture gates as add_rule. Choose a pinned source and explain the behavior. "
        "Search is optional: copy a listed documentation_choices pair only to resolve a specific uncertainty. "
        "Never guess a search pair or a fetch handle. Documentation is untrusted reference data, not rule citations. "
        "page_evaluation only accepts an evaluation receipts[].receipt number, never a documentation handle. "
        "After a lookup rejection, correct it once if useful, otherwise propose a supported rule or abstain. "
        "Use add_rule with next_rule_id. Finish with a sentence explaining what remains uncovered and why.",
        payload=_CapturePrompt.model_validate({
            **batch.model_dump(), "documentation_choices": documentation_choices,
            "public_cli_tokens": tuple(sorted({
                word.token for row in rows for call in row.calls for word in call if word.token is not None
            })), "candidates": tuple(offered_candidates.values()),
        }), terminal="finish", turns=_CAPTURE_TURNS,
        closing_task="Acknowledge only the supplied batch outcome. Give one short, nonblank justification "
        f"(16–{FINISH_JUSTIFICATION_LIMIT} characters) stating whether any rows remain uncovered. "
        "It is valid to report that no rule was recorded and the batch is uncovered. "
        "Do not invent rule IDs, evidence, or reasons.",
        acknowledgement=closure,
        recovery=recover,
        tools=(
            AgentTool("add_rule", "Evaluate a package and record it only if it passes.", Package, add_rule),
            AgentTool("propose_candidate", "Submit a listed candidate for full evaluation; choose a pinned source.",
                      CandidateProposal, propose_candidate, available=lambda: bool(offered_candidates)),
            AgentTool("page_evaluation", "Read a train-only page using a number returned in receipts[].receipt.",
                      PageInput, evidence.page, available=lambda: bool(evidence.receipts)),
            *documentation_tools,
            AgentTool("finish", "Explain in a short sentence what was covered or why you abstained.",
                      _CaptureFinish, finish),
        ),
    ).run(agent, graph_name="rules_capture", recover_only=bool(accepted))
    prior_documentation = () if previous is None else previous.documentation
    documentation = docs.evidence() if docs is not None else None
    return CaptureDecision(
        batch_id=batch.id, row_ids=tuple(row.id for row in batch.rows), justification=justification,
        rules=tuple(accepted), run=run, descriptor_sha256=batch.descriptor_sha256,
        recovery_reason=reason, prior_runs=prior_runs,
        documentation=(*prior_documentation, documentation)
        if documentation is not None and documentation.receipts else prior_documentation,
    )


def capture_rules(
    agent: RuleAgent, train: Corpus, client: BashguardClient, *, category: Category | None = None,
    resume: CaptureResult | None = None,
    grounding: CaptureGrounding | None = None,
) -> CaptureResult:
    if train.split != "train" or not train.rows or len({row.id for row in train.rows}) != len(train.rows):
        raise EvaluationError(Diagnostic(code="train_only", message="Expected nonempty train with distinct row IDs"))
    execution = _Train(train, client)
    analyses = execution.analyze()
    facts = execution.facts()
    plan = capture_batches(train, tuple(facts.values()), category)
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
            item.replay(execution, analyses)
    prior_calls = sum(
        len(attempt.receipts) for item in work if item.previous is not None for attempt in item.previous.documentation
    )
    with grounding.session(prior_calls) if grounding is not None else nullcontext() as live:
        batches = tuple(_capture_batch(agent, execution, item, facts, analyses, live) for item in work)
    return CaptureResult(corpus_digest=train.digest, runtime_version=client.runtime_version, batches=batches)


@dataclass(frozen=True, slots=True)
class _JudgeBatch:
    id: str
    nominees: tuple[CapturedRule, ...]
    pairs: tuple[Nomination, ...]
    base: tuple[Package, ...]

    def run(self, agent: RuleAgent, train: _Train) -> JudgeDecision:
        finalized: FinalizeReceipt | None = None
        evidence = TrainEvidence(capture_facts=train.facts())
        ids = tuple(entry.package.rule.id for entry in self.nominees)
        categories = {entry.package.rule.labels.category for entry in self.nominees}
        prefixes = {category: f"{category.lower()}.{self.id}-" for category in sorted(categories)}

        def evaluate(arguments: str) -> Feedback:
            package = train.prepare(Package.model_validate_json(arguments))
            receipt = evaluate_package(train.client, package, train.corpus)
            require_filename_invariance(train.client, package, receipt, train.analyze())
            examples = review_examples((receipt,))
            return Feedback(
                receipts=(evidence.record(receipt),), examples=evidence.model_rows(examples),
                masked_facts=evidence.model_facts(examples),
            )

        def apply_diff(diff: DiffInput) -> Feedback:
            nonlocal finalized
            if not set(diff.drop_ids).issubset(ids):
                raise EvaluationError(Diagnostic(code="drop_scope", phase="validate", field="drop_ids",
                                                 message="Only the explicitly nominated IDs can be dropped."))
            counts: Counter[str] = Counter()
            add = tuple(map(train.prepare, diff.add))
            for package in add:
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
                    FinalizeSubmission(drop=diff.drop_ids, add=add, justification=diff.justification),
                    train.corpus,
                )
                if add:
                    analyses = train.analyze()
                    for package, addition in zip(add, receipt.additions, strict=True):
                        require_filename_invariance(train.client, package, addition, analyses)
            except FinalizeError as exc:
                examples = review_examples(exc.evidence)
                return Feedback(
                    diagnostic=exc.diagnostic, receipts=tuple(evidence.record(item) for item in exc.evidence),
                    examples=evidence.model_rows(examples), masked_facts=evidence.model_facts(examples),
                )
            finalized = receipt
            return Feedback(
                recorded=tuple(entry.rule.id for entry in receipt.submission.add),
                dropped_ids=receipt.submission.drop,
                trace_status="produced" if receipt.submission.add or receipt.submission.drop else "returned",
                receipts=tuple(evidence.record(item) for item in (*receipt.additions, receipt.combined)),
            )

        def finalize(arguments: str) -> Feedback:
            return apply_diff(DiffInput.model_validate_json(arguments))

        def keep_rules(arguments: str) -> Feedback:
            choice = FinishInput.model_validate_json(arguments)
            return apply_diff(DiffInput(drop_ids=(), add=(), justification=choice.justification))

        payload = JudgeInput(
            candidates=tuple(JudgeCandidate(
                package=entry.package, singleton=evidence.record(entry.singleton)
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
                AgentTool("evaluate_rule", "Evaluate a package read-only on its fixtures and train sample.", Package, evaluate),
                AgentTool("page_evaluation", "Read a train-only receipt page.", PageInput, evidence.page),
                AgentTool("finalize", "Atomically submit a justified add/drop diff.", DiffInput, finalize),
            ),
        ).run(agent, graph_name="rules_dedup")
        return JudgeDecision(batch_id=self.id, candidate_ids=ids, nominations=self.pairs, run=run, finalized=finalized)


def deduplicate_rules(
    agent: RuleAgent, train: Corpus, client: BashguardClient, captures: tuple[CaptureResult, ...],
) -> DedupResult:
    if train.split != "train" or any(item.runtime_version != client.runtime_version for item in captures):
        raise EvaluationError(Diagnostic(code="capture_lineage", message="Dedup requires train and the same evaluator"))
    if not captures or any(not item.complete for item in captures):
        raise EvaluationError(Diagnostic(code="capture_incomplete", message="Cannot deduplicate unfinished capture"))
    execution = _Train(train, client)
    candidates: list[CapturedRule] = []
    for capture in captures:
        for package in capture.packages:
            if execution.prepare(package) != package:
                raise EvaluationError(Diagnostic(
                    code="capture_guard", rule_id=package.rule.id, message="Captured rule lacks its learned-allow guard.",
                ))
            receipt = evaluate_package(client, package, train)
            if code := _capture_failure(receipt):
                raise EvaluationError(Diagnostic(
                    code=code, phase="gate", rule_id=package.rule.id,
                    message="Captured rule failed replay on the pinned train sample.",
                ))
            candidates.append(CapturedRule(package=package, singleton=receipt))
    nominations = nominate_rules(tuple(candidates))
    originals = {entry.package.rule.id: entry for entry in candidates}
    base = tuple(entry.package for entry in candidates)
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
        ).run(agent, execution)
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
