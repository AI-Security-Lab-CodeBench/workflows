from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "evidence/ast-rules-v2"


def invoke(payload: str, *, mode: str = "classify", rules: str = "ast-examples.json") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(ROOT / "bin/bashguard"), "-mode", mode, "-rules", str(ROOT / "rules" / rules)],
        input=payload,
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
        env=os.environ.copy(),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", type=Path, default=EVIDENCE)
    evidence = parser.parse_args().evidence_dir
    evidence.mkdir(exist_ok=True, parents=True)
    payload = (ROOT / "testdata/ast-requests.jsonl").read_text(encoding="utf-8")
    classified = invoke(payload)
    assert classified.returncode == 0, classified.stderr
    rows = [json.loads(line) for line in classified.stdout.splitlines()]
    expected = {
        "redirect": "ast.redis-redirect",
        "pipeline": "ast.redis-pipeline",
        "substitution": "ast.redis-substitution",
        "quoted-pipeline": "ast.redis-pipeline",
    }
    assert len(rows) == 14
    for row in rows:
        result = row["result"]
        if row["id"] in expected:
            assert result["status"] == "matched", row
            assert result["basis"] == "bash_ast", row
            assert result["rule_ids"] == [expected[row["id"]]], row
            assert result["labels"]["action"] == "ask", row
        else:
            assert result["status"] == "abstain" and "labels" not in result, row
    (evidence / "classification.jsonl").write_text(classified.stdout, encoding="utf-8")

    analyzed = invoke(payload, mode="analyze")
    assert analyzed.returncode == 0, analyzed.stderr
    documents = {row["id"]: row["analysis"] for row in map(json.loads, analyzed.stdout.splitlines())}
    assert all(document["ast"]["Type"] == "File" for document in documents.values())
    redirect = documents["redirect"]["ast"]["Stmts"][0]["Redirs"][0]
    assert redirect["Op"] == ">"
    assert redirect["Word"]["Parts"][0]["Value"] == "audit.log"
    pipeline = documents["pipeline"]["ast"]["Stmts"][0]["Cmd"]
    assert pipeline["Type"] == "BinaryCmd" and pipeline["Op"] == "|"
    assert pipeline["X"]["Cmd"]["Args"][0]["Parts"][0]["Value"] == "redis-cli"
    assert pipeline["Y"]["Cmd"]["Args"][0]["Parts"][0]["Value"] == "cat"
    parts = documents["substitution"]["ast"]["Stmts"][0]["Cmd"]["Args"][1]["Parts"]
    assert parts[0]["Type"] == "DblQuoted"
    assert parts[0]["Parts"][0]["Type"] == "CmdSubst"
    assert not documents["pipeline"]["eligible"]
    (evidence / "analysis.jsonl").write_text(analyzed.stdout, encoding="utf-8")

    original = invoke(payload, rules="cat3.json")
    assert original.returncode == 0, original.stderr
    assert all(row["result"]["status"] == "abstain" for row in map(json.loads, original.stdout.splitlines()))

    with tempfile.TemporaryDirectory() as directory:
        sentinel = Path(directory) / "executed"
        request = json.dumps({"command": f'printf "$(touch {sentinel})"'}) + "\n"
        inert = invoke(request)
        assert inert.returncode == 0, inert.stderr
        assert json.loads(inert.stdout)["result"]["status"] == "abstain"
        assert not sentinel.exists()

    malformed = invoke('{"command":"printf \\""}\n')
    assert malformed.returncode == 1
    error_row = json.loads(malformed.stdout)
    assert error_row["result"]["status"] == "abstain"
    assert "labels" not in error_row["result"] and error_row["error"]
    print(json.dumps({
        "status": "PASS",
        "native_ast_requests": len(rows),
        "matched": len(expected),
        "structural_negatives": len(rows) - len(expected),
        "typed_ast_export": "PASS",
        "default_seed_scope": "unchanged",
        "malformed_source": "rejected",
        "inert_sentinel": "absent",
    }, indent=2))


if __name__ == "__main__":
    main()
