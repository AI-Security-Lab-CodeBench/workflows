from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BINARY = ROOT / "bin/bashguard"
EVIDENCE = ROOT / "evidence"


def invoke(mode: str, payload: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(BINARY), "-mode", mode, "-rules", str(ROOT / "rules/cat3.json")],
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
    payload = (ROOT / "testdata/requests.jsonl").read_text(encoding="utf-8")
    classified = invoke("classify", payload)
    assert classified.returncode == 0, classified.stderr
    rows = [json.loads(line) for line in classified.stdout.splitlines()]
    expected = [
        "matched", "matched", "matched", "matched", "matched", "matched", "matched",
        "abstain", "abstain", "abstain", "abstain", "abstain", "abstain",
        "abstain", "abstain", "abstain", "matched", "abstain",
    ]
    assert [row["result"]["status"] for row in rows] == expected
    assert rows[1]["result"]["labels"]["action"] == "deny"
    assert rows[2]["result"]["labels"]["severity"] == "high"
    assert rows[3]["result"]["labels"]["severity"] == "critical"
    assert all("labels" not in row["result"] for row in rows if row["result"]["status"] == "abstain")
    (evidence / "cli.jsonl").write_text(classified.stdout, encoding="utf-8")

    analyzed = invoke("analyze", payload)
    assert analyzed.returncode == 0, analyzed.stderr
    originals = [json.loads(line)["command"] for line in payload.splitlines()]
    documents = [json.loads(line)["analysis"] for line in analyzed.stdout.splitlines()]
    assert [doc["source"] for doc in documents] == originals
    for doc in documents:
        encoded = doc["source"].encode("utf-8")
        for call in doc["calls"]:
            for word in call["words"]:
                assert encoded[word["start"]:word["end"]].decode("utf-8") == word["raw"]
    (evidence / "analysis.jsonl").write_text(analyzed.stdout, encoding="utf-8")

    masked = invoke("mask", (ROOT / "testdata/masking.jsonl").read_text(encoding="utf-8"))
    assert masked.returncode == 0, masked.stderr
    assert [json.loads(line)["masked"] for line in masked.stdout.splitlines()] == [
        "printf MASKED café", "printf 'same' 'MASKED'",
    ]
    (evidence / "masking.jsonl").write_text(masked.stdout, encoding="utf-8")

    with tempfile.TemporaryDirectory() as directory:
        sentinel = Path(directory) / "executed"
        request = {"id": "never-execute", "command": f"redis-cli FLUSHALL; touch {sentinel}"}
        result = invoke("classify", json.dumps(request) + "\n")
        output = json.loads(result.stdout)
        assert result.returncode == 0, result.stderr
        assert output["id"] == "never-execute"
        assert output["command"] == request["command"]
        assert output["result"]["status"] == "abstain"
        assert not sentinel.exists()

    malformed = invoke("analyze", '{"command":"redis-cli \\""}\n{"command":"printf x"}\n')
    bad_rows = [json.loads(line) for line in malformed.stdout.splitlines()]
    assert malformed.returncode == 1
    assert "error" in bad_rows[0] and "analysis" in bad_rows[1]
    invalid_edit = invoke("mask", json.dumps({
        "command": "printf café",
        "edits": [{"start": 7, "end": 11, "replacement": "x"}],
    }) + "\n")
    assert invalid_edit.returncode == 1 and "error" in json.loads(invalid_edit.stdout)
    print(json.dumps({
        "status": "PASS",
        "classification_requests": len(rows),
        "matched": expected.count("matched"),
        "abstained": expected.count("abstain"),
        "masking_cases": 2,
        "invalid_edit": "rejected",
        "malformed_record": "reported; following record retained",
        "inert_command_sentinel": "absent",
        "source_roundtrip": "PASS",
    }, indent=2))


if __name__ == "__main__":
    main()
