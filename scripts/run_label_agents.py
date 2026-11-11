from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from label_contract import DatasetError, read_json, require, validate_packet, validate_response
from label_pipeline import json_text
from validate_dataset import unique_object


@dataclass(frozen=True, slots=True)
class AgentRun:
    executable: str
    model: str
    output: Path
    timeout: int
    thinking: str = "low"


def assistant_result(events: str, requested_model: str) -> tuple[dict, str]:
    messages = []
    for line in events.splitlines():
        if not line.strip():
            continue
        event = json.loads(line)
        if event.get("type") == "message_end" and event.get("message", {}).get("role") == "assistant":
            messages.append(event["message"])
    require(bool(messages), "CLI returned no assistant message")
    message = messages[-1]
    require(message.get("stopReason") not in {"error", "aborted", "length"}, "Incomplete assistant response")
    require(all(part.get("type") in {"text", "thinking"} for item in messages for part in item["content"]),
            "Unexpected tool invocation in isolated inference")
    actual_model = message.get("provider", "") + "/" + message.get("model", "")
    require(actual_model == requested_model, f"Unexpected inference model: {actual_model}")
    text = "".join(part["text"] for part in message["content"] if part.get("type") == "text").strip()
    if text.startswith("```json\n") and text.endswith("\n```"):
        text = text[8:-4]
    return json.loads(text, object_pairs_hook=unique_object), actual_model


def run_packet(path: Path, settings: AgentRun) -> dict:
    packet = read_json(path)
    validate_packet(packet, packet["stage"])
    destination = settings.output / path.name
    require(not destination.exists(), f"Response already exists: {destination}")
    invocation = str(uuid.uuid4())
    evidence = settings.output / "evidence" / path.stem / invocation
    evidence.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="label-agent-") as temporary:
        environment = dict(os.environ, TMPDIR=temporary,
                           NODE_COMPILE_CACHE=str(Path(temporary) / "node-compile-cache"),
                           BUN_RUNTIME_TRANSPILER_CACHE_PATH=str(Path(temporary) / "bun-transpiler"))
        command = [
            settings.executable, "--print", "--mode", "json", "--model", settings.model,
            "--thinking", settings.thinking, "--no-tools", "--no-extensions", "--no-skills",
            "--no-prompt-templates", "--no-context-files", "--no-session", "--offline",
            "--system-prompt", packet["system_prompt"], "-",
        ]
        started = datetime.now(UTC).isoformat()
        process = subprocess.run(command, input=packet["prompt"], text=True, capture_output=True,
                                 timeout=settings.timeout, cwd=temporary, env=environment, check=False)
        (evidence / (path.stem + ".events.jsonl")).write_text(process.stdout, encoding="utf-8")
        (evidence / (path.stem + ".stderr.txt")).write_text(process.stderr, encoding="utf-8")
        require(process.returncode == 0, f"Agent process failed for {path.name}; inspect retained stderr")
        result, actual_model = assistant_result(process.stdout, settings.model)
        validate_response(packet, result)
        envelope = {
            "packet_sha256": packet["packet_sha256"], "model": actual_model,
            "responded_at": datetime.now(UTC).isoformat(), "invocation_id": invocation, "result": result,
        }
        receipt = {
            "invocation_id": invocation, "started_at": started, "ended_at": envelope["responded_at"],
            "model": actual_model, "packet": str(path), "packet_sha256": packet["packet_sha256"],
            "system_prompt_sha256": hashlib.sha256(packet["system_prompt"].encode()).hexdigest(),
            "tools_enabled": False, "context_files_enabled": False, "session_saved": False,
            "exit_code": process.returncode, "response_sha256": hashlib.sha256(process.stdout.encode()).hexdigest(),
            "command_options": command[1:-3], "temporary_directory_cleanup": "context_manager",
        }
    (evidence / (path.stem + ".receipt.json")).write_text(json_text(receipt), encoding="utf-8")
    with destination.open("x", encoding="utf-8") as stream:
        stream.write(json_text(envelope))
    return {"packet": path.name, "stage": packet["stage"], "records": len(packet["candidate_ids"]),
            "model": actual_model, "invocation_id": invocation}


def main() -> None:
    parser = argparse.ArgumentParser(description="Fresh no-tools native CLI inference per packet; no credential extraction.")
    parser.add_argument("--packets", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--executable", default="omo")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--thinking", choices=("off", "minimal", "low", "medium", "high"), default="low")
    args = parser.parse_args()
    require(args.workers > 0 and args.timeout > 0, "Workers and timeout must be positive")
    paths = sorted(args.packets.glob("*.json"))
    require(bool(paths), "No inference packets")
    args.out.mkdir(parents=True, exist_ok=True)
    settings = AgentRun(args.executable, args.model, args.out, args.timeout, args.thinking)
    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_packet, path, settings): path for path in paths
                   if not (args.out / path.name).exists()}
        for future in as_completed(futures):
            try:
                print("PACKET " + json.dumps(future.result()), flush=True)
            except (DatasetError, OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as error:
                failures.append({"packet": futures[future].name, "error": str(error)})
                print("FAILED " + json.dumps(failures[-1]), flush=True)
    require(not failures, f"{len(failures)} inference packets failed; responses and evidence retained")


if __name__ == "__main__":
    main()
