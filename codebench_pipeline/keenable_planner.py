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
from .inference_capacity import CapacityConfig, CapacityQuarantined, guarded_complete
from .keenable_ledger import PLAN_RESPONSE_BYTES, Ledger, SlotIdentity
from .keenable_policy import MAX_SEARCH, QUERIES, Case, KeenableDeclaration, KeenableError, digest
from .keenable_receipts import CallReceipt, Lead, SearchDecision, SearchFeedback, SearchPlanReceipt
from .keenable_transport import search_payload
from .openai_transport import parse_completion

_PROMPT: Final = (
    "Choose one available option ID to find additional ordinary administrative reference pages, "
    'using the previous search counts. Return only {"option":"ID"} or {"option":null} to stop. '
    "Prefer uncovered reference topics or a different focus after low yield."
)
_FOCUSES: Final = (
    ("reference", ""),
    ("inventory", " Focus on read-only inventory examples."),
    ("fields", " Focus on output fields and status."),
    ("maintenance", " Focus on documented maintenance scope and operational limits."),
    ("lifecycle", " Focus on lifecycle states and documented restart behavior."),
    ("retention", " Focus on documented data retention limits and defaults."),
)
_OUTPUT_TOKENS: Final = 96


@dataclass(frozen=True, slots=True)
class GuardedSearchPlanner:
    endpoint: str
    model: str
    capacity: CapacityConfig
    receipt_directory: Path
    key: str | None = field(default=None, repr=False)
    backend_revision: str = ""

    @property
    def identity(self) -> str:
        return digest(
            json.dumps(
                [
                    self.endpoint,
                    self.model,
                    self.backend_revision,
                    self.capacity.model_dump_json(),
                    _PROMPT,
                    _OUTPUT_TOKENS,
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
                "max_completion_tokens": _OUTPUT_TOKENS,
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
        if len(body) > PLAN_RESPONSE_BYTES:
            raise KeenableError("planner_request_budget")
        receipt = ledger.reserve("plan", identity)
        raw: bytes | None = None
        reason = "planner_call_failed"
        try:
            raw = guarded_complete(
                self.endpoint, body, self.key, 30, self.capacity, self.receipt_directory
            ).body
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
        return ledger.finish(
            receipt, state="succeeded", response=raw, payload=decision.model_dump_json().encode()
        ), decision


@dataclass(frozen=True, slots=True)
class SearchOption:
    id: str
    case: Case
    focus: str
    payload: bytes
    urls: frozenset[str]


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
    declaration: KeenableDeclaration
    planner: GuardedSearchPlanner | None

    def run(
        self, search: Callable[[SearchOption], tuple[CallReceipt, tuple[Lead, ...]]]
    ) -> tuple[tuple[Lead, ...], tuple[CallReceipt, ...], SearchPlanReceipt]:
        plan = self.declaration.search_plan
        assert plan is not None
        planner = self.planner if plan.mode == "adaptive" else None
        if plan.mode == "adaptive" and planner is None:
            raise KeenableError("guarded_planner_required")
        options: list[SearchOption] = []
        for focus, suffix in _FOCUSES:
            for scope in plan.scopes:
                options.append(
                    SearchOption(
                        f"{scope.case}-{focus}",
                        scope.case,
                        focus,
                        search_payload(QUERIES[scope.case] + suffix, tuple(sorted(scope.reviewed_urls))),
                        frozenset(scope.reviewed_urls),
                    )
                )
        limit = self.declaration.budget.search_calls if self.declaration.budget is not None else MAX_SEARCH
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
        graph.add_node("choose", choose, destinations=("search", END))
        graph.add_node("search", dispatch, destinations=("choose",))
        graph.add_edge(START, "choose")
        result = graph.compile().invoke(
            {
                "leads": (),
                "searches": (),
                "feedback": (),
                "decisions": (),
                "seen": frozenset(url for source in self.declaration.sources for url in source.exact_urls),
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
