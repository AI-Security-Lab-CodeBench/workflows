from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Final

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.rules_agents import RuleAgent
from codebench_pipeline.rules_learning import capture_rules, deduplicate_rules
from tests.rules_learning_fixture import (
    COMPILE,
    Step,
    rule_package,
    scripted_agent,
    synthetic_evaluator,
    train_corpus,
)

FIRST_RULE_ID: Final = "cat-6.capture-0001-01"
SECOND_RULE_ID: Final = "cat-6.capture-0001-02"
_CAPTURE_JUSTIFICATION: Final = "Both synthetic variants are covered."
_DEDUP_JUSTIFICATION: Final = (
    "The second rule covers both reviewed variants, including the first rule's fixtures."
)
RULE_PAYLOAD_SENTINELS: Final = (
    "pwd -P",
    "pwd --physical",
    "pwd -L",
    COMPILE.message.splitlines()[0],
    _CAPTURE_JUSTIFICATION,
    _DEDUP_JUSTIFICATION,
)
MODEL_WITNESS: Final = "rules-model-exchanges.json"
type Exchange = tuple[dict[str, JsonValue], dict[str, JsonValue]]
MODEL_EXCHANGES: Final = TypeAdapter(dict[str, list[Exchange]])
_JSON_OBJECT: Final = TypeAdapter(dict[str, JsonValue])


def _observed_agent(replies: Iterator[Step], exchanges: list[Exchange]) -> RuleAgent:
    agent = scripted_agent(replies)

    def complete(url: str, body: bytes, key: str | None, timeout: float) -> HTTPResponse:
        response = agent.request(url, body, key, timeout)
        exchanges.append((_JSON_OBJECT.validate_json(body), _JSON_OBJECT.validate_json(response.body)))
        return response

    return replace(agent, request=complete)


def exercise_rules_agents(scratch: Path) -> None:
    train = train_corpus("pwd -P", "pwd --physical")
    first = rule_package("pwd -P", FIRST_RULE_ID)
    second = rule_package("pwd -P|pwd --physical", SECOND_RULE_ID, positive="pwd --physical")
    capture_replies: tuple[Step, ...] = (
        ("add_rule", rule_package("syntax error", FIRST_RULE_ID)),
        ("add_rule", first),
        ("add_rule", second),
        ("finish", {"justification": _CAPTURE_JUSTIFICATION}),
    )
    capture_exchanges: list[Exchange] = []
    client = synthetic_evaluator([])
    capture = capture_rules(_observed_agent(iter(capture_replies), capture_exchanges), train, client)
    assert capture.complete
    (batch,) = capture.batches
    assert [failure.diagnostic.code for failure in batch.run.failures] == ["cel_compile"]
    assert tuple(package.rule.id for package in capture.packages) == (first.rule.id, second.rule.id)

    preview = rule_package("pwd -P", "cat-6.judge-0001-01")
    dedup_replies: tuple[Step, ...] = (
        ("evaluate_rule", preview),
        (
            "finalize",
            {
                "drop_ids": [first.rule.id],
                "add": [],
                "justification": _DEDUP_JUSTIFICATION,
            },
        ),
    )
    dedup_exchanges: list[Exchange] = []
    dedup = deduplicate_rules(
        _observed_agent(iter(dedup_replies), dedup_exchanges), train, client, (capture,)
    )
    assert dedup.complete
    assert tuple(package.rule.id for package in dedup.packages) == (second.rule.id,)
    (scratch / MODEL_WITNESS).write_bytes(
        MODEL_EXCHANGES.dump_json(
            {
                "rules_capture": capture_exchanges,
                "rules_dedup": dedup_exchanges,
            }
        )
    )
