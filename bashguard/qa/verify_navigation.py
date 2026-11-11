from __future__ import annotations

import json

from verify_ast import ROOT, invoke

CASES = [
    ("redirect", "redis-cli FLUSHALL >audit.log", True),
    ("spacing", "redis-cli FLUSHALL > audit.log", True),
    ("comments", "# café\nredis-cli FLUSHALL >audit.log # note\n", True),
    ("trailing-newline", "redis-cli FLUSHALL >audit.log\n", True),
    ("negated", "! redis-cli FLUSHALL >audit.log", False),
    ("background", "redis-cli FLUSHALL >audit.log &", False),
    ("function", "f() { redis-cli FLUSHALL >audit.log; }", False),
    ("conditional", "false && redis-cli FLUSHALL >audit.log", False),
    ("literal", "printf 'redis-cli FLUSHALL >audit.log'", False),
    ("wrong-utility", "printf FLUSHALL >audit.log", False),
    ("wrong-action", "redis-cli INFO >audit.log", False),
    ("input-redirect", "redis-cli FLUSHALL <audit.log", False),
    ("append-redirect", "redis-cli FLUSHALL >>audit.log", False),
    ("other-file", "redis-cli FLUSHALL >other.log", False),
    ("dynamic", "redis-cli $ACTION >audit.log", False),
    ("two-statements", "printf x; redis-cli FLUSHALL >audit.log", False),
    ("explicit-fd", "redis-cli FLUSHALL 2>audit.log", False),
    ("assignment", "X=1 redis-cli FLUSHALL >audit.log", False),
]


def main() -> None:
    payload = "".join(json.dumps({"id": name, "command": command}) + "\n" for name, command, _ in CASES)
    result = invoke(payload)
    assert result.returncode == 0, result.stderr
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    assert len(rows) == len(CASES)
    for row, (name, command, matched) in zip(rows, CASES, strict=True):
        assert row["id"] == name and row["command"] == command, row
        decision = row["result"]
        if matched:
            assert decision["status"] == "matched", row
            assert decision["basis"] == "bash_ast", row
            assert decision["rule_ids"] == ["ast.redis-redirect"], row
            assert decision["labels"]["action"] == "ask", row
        else:
            assert decision["status"] == "abstain" and "labels" not in decision, row
    evidence = ROOT / "evidence/navigation-v3"
    evidence.mkdir(exist_ok=True, parents=True)
    (evidence / "classification.jsonl").write_text(result.stdout, encoding="utf-8")
    print(json.dumps({
        "status": "PASS",
        "requests": len(rows),
        "matched": sum(matched for _, _, matched in CASES),
        "negatives": sum(not matched for _, _, matched in CASES),
        "local_bindings": "verified by actual compiled redirect rule",
        "parent_and_children": "verified by actual compiled redirect rule",
    }, indent=2))


if __name__ == "__main__":
    main()
