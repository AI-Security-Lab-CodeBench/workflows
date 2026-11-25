import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Literal, TypedDict

from langgraph.constants import END, START
from langgraph.graph import StateGraph
from langgraph.types import Command
from pydantic import ValidationError

from .agent_events import InferenceError, parse_json
from .agent_trace import CallOutcome, llm_call, record_graph
from .inference_capacity import CapacityConfig, CapacityQuarantined, guarded_complete
from .keenable_ledger import Ledger, SlotIdentity
from .keenable_policy import (
    MAX_SEARCH,
    QUERIES,
    Case,
    CurrentAdministrativeSearch,
    Declaration,
    KeenableError,
    UnrestrictedSearch,
    digest,
)
from .keenable_receipts import CallReceipt, Lead, SearchDecision, SearchFeedback, SearchPlanReceipt
from .keenable_transport import search_payload
from .openai_transport import complete, parse_completion

_PROMPT: Final = (
    "Choose one available option ID to find additional distinct pages matching the available topics, "
    'using the previous search counts. Return only {"option":"ID"} or {"option":null} to stop. '
    "Try each unsearched case before repeating one. Use the available focus that best distinguishes "
    "a real operator symptom from generic reference material. After repeated or ineligible results, "
    "switch the symptom. Counts describe URLs only; never treat them as proof of useful commands."
)
_CASE_TOPICS: Final[dict[Case, str]] = {
    "plan": "Terraform plan or validate showing an unexpected difference between configuration and state",
    "workloads": "kubectl workload inventory with missing, stale or misleading status",
    "containers": "Docker container inventory and discrepancies in reported disk usage",
    "storage_sql": "S3 listing with misleading prefixes or PostgreSQL metadata missing because of search_path",
    "rollout": "Kubernetes deployment whose rollout status disagreed with observed readiness",
}
_CASE_FOCUSES: Final = (
    (
        "unexpected-output",
        "An operator explains a surprising result and the diagnostic command that clarified it.",
    ),
    ("quoting", "A practitioner works around shell quoting, unusual names or JSONPath escaping."),
    ("version-difference", "A field report traces inconsistent output across versions or environments."),
    (
        "partial-results",
        "A troubleshooting report explains an empty or incomplete result and shows how it was checked.",
    ),
)
_FOCUSES: Final = (
    ("reference", ""),
    ("inventory", " Focus on read-only inventory examples."),
    ("fields", " Focus on output fields and status."),
    ("maintenance", " Focus on documented maintenance scope and operational limits."),
    ("lifecycle", " Focus on lifecycle states and documented restart behavior."),
    ("retention", " Focus on documented data retention limits and defaults."),
)


@dataclass(frozen=True, slots=True)
class SearchPlanner:
    endpoint: str
    model: str
    capacity: CapacityConfig | None
    receipt_directory: Path
    key: str | None = field(default=None, repr=False)
    backend_revision: str = ""
    max_output_tokens: int = 1024
    timeout_seconds: float = 30

    @property
    def identity(self) -> str:
        return digest(
            json.dumps(
                [
                    self.endpoint,
                    self.model,
                    self.backend_revision,
                    self.capacity.model_dump_json() if self.capacity is not None else "plain",
                    _PROMPT,
                    self.max_output_tokens,
                    self.timeout_seconds,
                ]
            ).encode()
        )

    def decide(self, ledger: Ledger, turn: "_PlanningTurn") -> tuple[CallReceipt, SearchDecision]:
        prompt = {
            "available": [
                {"id": option.id, "case": option.case, "focus": option.focus} for option in turn.options
            ],
            "previous": [feedback.model_dump(mode="json") for feedback in turn.feedback],
            "remaining_searches": turn.remaining,
        }
        body = json.dumps(
            {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": _PROMPT},
                    {"role": "user", "content": json.dumps(prompt, separators=(",", ":"))},
                ],
                "temperature": 0,
                "max_completion_tokens": self.max_output_tokens,
                "response_format": {"type": "json_object"},
                "stream": False,
            },
            separators=(",", ":"),
        ).encode()
        identity = SlotIdentity(
            slot=f"plan:{len(turn.feedback) + 1}", target=self.identity, request_sha256=digest(body)
        )
        if (prior := ledger.call(identity.slot)) is not None:
            receipt, payload = prior
            if (
                payload is None
                or receipt.kind != "plan"
                or receipt.state != "succeeded"
                or receipt.target != identity.target
                or receipt.request_sha256 != identity.request_sha256
            ):
                raise KeenableError("planner_receipt_integrity")
            return receipt, SearchDecision.model_validate_json(payload)
        if len(body) > 16 * 1024:
            raise KeenableError("planner_request_budget")
        receipt = ledger.reserve("plan", identity)
        raw: bytes | None = None
        reason = "planner_call_failed"
        measured = CallOutcome()
        try:
            if self.capacity is not None:
                raw = guarded_complete(
                    self.endpoint, body, self.key, self.timeout_seconds, self.capacity, self.receipt_directory
                ).body
            else:
                with llm_call() as measured:
                    raw = complete(
                        self.endpoint.rstrip("/") + "/chat/completions", body, self.key, self.timeout_seconds
                    ).body
                measured.record({"status": "rejected"})
            if len(raw) > receipt.reserved_bytes:
                raise KeenableError("planner_response_budget")
            reason = "planner_response_invalid"
            completion = parse_completion(raw.decode())
            if completion.model != self.model:
                raise KeenableError("planner_model_mismatch")
            decision = SearchDecision.model_validate(parse_json(completion.text()))
            if decision.option is not None and decision.option not in {option.id for option in turn.options}:
                raise KeenableError("planner_option_not_available")
        except (InferenceError, KeenableError, OSError, UnicodeError, ValidationError, ValueError) as error:
            failure = (
                "planner_guard_quarantined"
                if isinstance(error, CapacityQuarantined)
                else error.code
                if isinstance(error, KeenableError)
                else reason
            )
            ledger.finish(receipt, state="failed", reason=failure, response=raw)
            raise KeenableError(failure) from None
        saved = ledger.finish(
            receipt, state="succeeded", response=raw, payload=decision.model_dump_json().encode()
        )
        measured.record(
            {"status": "abstained", "count": 0}
            if decision.option is None
            else {"status": "produced", "count": 1}
        )
        return saved, decision


@dataclass(frozen=True, slots=True)
class SearchOption:
    id: str
    case: Case
    focus: str
    payload: bytes
    urls: frozenset[str] | None


@dataclass(frozen=True, slots=True)
class _PlanningTurn:
    options: tuple[SearchOption, ...]
    feedback: tuple[SearchFeedback, ...]
    remaining: int


type _Stop = Literal["budget", "options_exhausted", "planner_stop"]


class _SearchState(TypedDict):
    leads: tuple[Lead, ...]
    searches: tuple[CallReceipt, ...]
    feedback: tuple[SearchFeedback, ...]
    decisions: tuple[CallReceipt, ...]
    seen: frozenset[str]
    selected: SearchOption | None
    stop: _Stop


@dataclass(frozen=True, slots=True)
class SearchSession:
    ledger: Ledger
    declaration: Declaration
    planner: SearchPlanner | None

    def run(
        self, search: Callable[[SearchOption], tuple[CallReceipt, tuple[Lead, ...]]]
    ) -> tuple[tuple[Lead, ...], tuple[CallReceipt, ...], SearchPlanReceipt]:
        plan = self.declaration.search_plan
        assert plan is not None
        planner = self.planner if plan.mode == "adaptive" else None
        if plan.mode == "adaptive" and planner is None:
            raise KeenableError("adaptive_planner_required")
        options: list[SearchOption] = []
        case_urls: tuple[tuple[Case, frozenset[str] | None], ...]
        if isinstance(plan, UnrestrictedSearch):
            case_urls = tuple((case, None) for case in plan.unrestricted_cases)
        else:
            case_urls = tuple((scope.case, frozenset(scope.reviewed_urls)) for scope in plan.scopes)
        community = isinstance(plan, UnrestrictedSearch) and plan.community_use_cases
        for focus, suffix in _CASE_FOCUSES if community else _FOCUSES:
            for case, urls in case_urls:
                query = (
                    f"Firsthand operator use case: {_CASE_TOPICS[case]}. {suffix} "
                    "Find community troubleshooting writeups with the observed symptom, literal read-only "
                    "command and explanation. Prefer explicitly CC BY 4.0 licensed posts. "
                    "Exclude official documentation, CLI references, cheatsheets and exploit walkthroughs."
                    if community
                    else QUERIES[case] + suffix
                )
                options.append(
                    SearchOption(
                        f"{case}-{focus}",
                        case,
                        focus,
                        search_payload(query, tuple(sorted(urls)) if urls else ()),
                        urls,
                    )
                )
        limit = self.declaration.budget.search_calls if self.declaration.budget is not None else MAX_SEARCH
        if (
            isinstance(plan, (CurrentAdministrativeSearch, UnrestrictedSearch))
            and plan.max_queries is not None
        ):
            limit = plan.max_queries
        policy = digest(
            json.dumps(
                [self.declaration.declaration_sha256, [digest(option.payload) for option in options]]
            ).encode()
        )
        planner_id = planner.identity if planner is not None else None
        self.ledger.publish("search_policy", json.dumps([policy, planner_id]).encode())

        def choose(state: _SearchState) -> Command[str]:
            remaining = limit - len(state["searches"])
            used = {feedback.option for feedback in state["feedback"]}
            available = tuple(option for option in options if option.id not in used)
            if remaining == 0:
                return Command(update={"stop": "budget"}, goto=END)
            if not available:
                return Command(update={"stop": "options_exhausted"}, goto=END)
            decisions = state["decisions"]
            selected = available[0]
            if planner is not None:
                self.declaration.check_current(datetime.now(UTC))
                receipt, decision = planner.decide(
                    self.ledger, _PlanningTurn(available, state["feedback"], remaining)
                )
                decisions += (receipt,)
                if decision.option is None:
                    return Command(update={"decisions": decisions, "stop": "planner_stop"}, goto=END)
                selected = next(option for option in available if option.id == decision.option)
            return Command(update={"selected": selected, "decisions": decisions}, goto="search")

        def dispatch(state: _SearchState) -> Command[str]:
            self.declaration.check_current(datetime.now(UTC))
            selected = state["selected"]
            assert selected is not None
            receipt, leads = search(selected)
            eligible = frozenset(lead.url for lead in leads if lead.url is not None)
            feedback = SearchFeedback(
                option=selected.id,
                returned=len(leads),
                eligible=len(eligible),
                new_urls=len(eligible - state["seen"]),
            )
            return Command(
                update={
                    "leads": state["leads"] + leads,
                    "searches": state["searches"] + (receipt,),
                    "feedback": state["feedback"] + (feedback,),
                    "seen": state["seen"] | eligible,
                },
                goto="choose",
            )

        graph = StateGraph[_SearchState, None, _SearchState, _SearchState](
            _SearchState, input_schema=_SearchState, output_schema=_SearchState
        )
        with record_graph("keenable_planner") as trace:
            graph.add_node("choose", trace.wrap("choose", choose), destinations=("search", END))
            graph.add_node("search", trace.wrap("search", dispatch), destinations=("choose",))
            graph.add_edge(START, "choose")
            result = graph.compile().invoke(
                {
                    "leads": (),
                    "searches": (),
                    "feedback": (),
                    "decisions": (),
                    "seen": frozenset(
                        url for source in self.declaration.sources for url in source.exact_urls
                    ),
                    "selected": None,
                    "stop": "budget",
                },
            )
        return (
            result["leads"],
            result["searches"],
            SearchPlanReceipt(
                mode=plan.mode,
                policy_sha256=policy,
                planner_sha256=planner_id,
                feedback=result["feedback"],
                decisions=result["decisions"],
                stop=result["stop"],
            ),
        )
