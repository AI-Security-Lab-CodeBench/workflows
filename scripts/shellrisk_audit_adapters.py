"""Inert reconstruction of the inspected, pinned ShellRisk adapter specifications."""

from __future__ import annotations

import hashlib
import io
import json
import re
import subprocess
import tarfile
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Final

CODE_REVISION: Final = "616e89cb07e10df12b24d596b3034d8b19edb8cd"
PINS: Final = {
    "swesmith": ("Kwai-Klear/SWE-smith-mini_swe_agent_plus-trajectories-66k", "750b2c11239fd5e32f97e6cfb9bf80fb9a9a2983"),
    "terminalbench": ("yoonholee/terminalbench-trajectories", "04e8940f5b6736a7ce8d22224fe2f2af74163ed2"),
    "nl2bash": ("TellinaTool/nl2bash", "d6b9f5bdff45621d190134e31ab63b7bf7002190"),
    "atomicredteam": ("redcanaryco/atomic-red-team", "5cdeb06642dbdfb3c595d4096cb9eea5f6434d8b"),
    "gtfobins": ("GTFOBins/GTFOBins.github.io", "acd524623f9c406acedd2754ebd9c2431f3675ad"),
    "payloads": ("swisskyrepo/InternalAllTheThings", "203bb0c0b290bf7c9158c32d43523b8d66f292c1"),
}
PAYLOAD_FILES: Final = (
    "docs/cheatsheets/shell-reverse-cheatsheet.md",
    "docs/cheatsheets/shell-bind-cheatsheet.md",
    "docs/redteam/escalation/linux-privilege-escalation.md",
    "docs/redteam/evasion/linux-evasion.md",
    "docs/redteam/persistence/linux-persistence.md",
)
URL: Final = re.compile(r"https?://[^\s'\"`;|)]+", re.IGNORECASE)
IP: Final = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
B64: Final = re.compile(r"(?<![A-Za-z0-9+/])[A-Za-z0-9+/]{40,}={0,2}(?![A-Za-z0-9+/])")
BASH_FENCE: Final = re.compile(r"```bash\s*\n(.*?)```", re.DOTALL)
FENCE: Final = re.compile(r"```([a-zA-Z0-9+]*)\n(.*?)```", re.DOTALL)
WINDOWS: Final = re.compile(r"(?i)(IEX|Invoke-\w|New-Object|System\.|\.exe\b|cmd\.exe|C:\\\\|-nop\b|FromBase64String|powershell)")
PROMPT: Final = re.compile(r"^\s*(?:[\w.\-]+@[\w.\-]+[:~][^\$#]*[#$]|[#$])\s+")
PROSE: Final = re.compile(r"^\s*(Victim|Attacker|Target|Listener|On |In |Note|Example|Output|Server|Client|Step|Then|First|Now|Run|Start|Use)\b", re.IGNORECASE)
LANGUAGES: Final = {"", "bash", "sh", "shell", "console", "python", "python3", "py", "perl", "ruby", "php", "awk"}


@dataclass(frozen=True, slots=True)
class Extracted:
    command: str
    locator: dict
    notice_flags: tuple[str, ...] = ()


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def normalize(text: str) -> str:
    return B64.sub("BASE64", IP.sub("1.1.1.1", URL.sub("http://example.com", text)))


def shape(text: str) -> str:
    text = re.sub(r"'[^']*'", "Q", text)
    text = re.sub(r'"[^"]*"', "Q", text)
    text = re.sub(r"[\w./-]+\.[A-Za-z0-9]{1,5}", "F", text)
    return " ".join(re.sub(r"\d+", "N", text).split())[:120]


def notices(text: str) -> tuple[str, ...]:
    return tuple(name for name, pattern in (
        ("copyright_marker", r"(?i)copyright|\(c\)"),
        ("license_marker", r"(?i)SPDX|licensed under|license"),
        ("external_reference", r"https?://"),
        ("embedded_program", r"<<|python[23]?\s+-c|perl\s+-e|ruby\s+-e"),
    ) if re.search(pattern, text))


def clean_payload(body: str) -> str | None:
    if WINDOWS.search(body):
        return None
    lines = []
    for line in body.splitlines():
        line = PROMPT.sub("", line).rstrip()
        short = line.strip()
        if not short or len(short) < 4 or short.endswith(":") or PROSE.search(short):
            continue
        if any(char in short for char in " /|=><&"):
            lines.append(line)
    return "\n".join(lines) or None


def yaml_documents(texts: list[str], bun: str) -> list[dict]:
    """Parse YAML data through a fixed local program, never source-provided code."""
    program = "const a=await Bun.stdin.text();console.log(JSON.stringify(JSON.parse(a).map(s=>Bun.YAML.parse(s))))"
    result = subprocess.run(
        [bun, "-e", program], input=json.dumps(texts), text=True,
        capture_output=True, check=True,
    )
    return json.loads(result.stdout)


def repository_commands(source: str, fetch: Callable[[str], bytes], bun: str) -> Iterator[Extracted]:
    repo, pin = PINS[source]
    root = f"https://raw.githubusercontent.com/{repo}/{pin}"
    if source == "nl2bash":
        url = f"{root}/data/bash/all.cm"
        for line_number, line in enumerate(fetch(url).decode("utf-8", "replace").splitlines(), 1):
            if line.strip():
                yield Extracted(line.strip(), {"url": url, "line": line_number}, notices(line))
        return
    if source == "payloads":
        for path in PAYLOAD_FILES:
            url = f"{root}/{path}"
            text = fetch(url).decode("utf-8", "replace")
            for fence_number, match in enumerate(FENCE.finditer(text)):
                lang, body = match.groups()
                if lang.lower() not in LANGUAGES:
                    continue
                command = clean_payload(body)
                if command:
                    yield Extracted(command, {
                        "url": url, "fence_index": fence_number,
                        "line": text.count("\n", 0, match.start()) + 1,
                    }, notices(body))
        return
    archive = fetch(f"https://codeload.github.com/{repo}/tar.gz/{pin}")
    documents = []
    members = []
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        for member in tar:
            path = member.name.partition("/")[2]
            art = re.search(r"^atomics/(T[0-9A-Za-z.\-]+)/(T[0-9A-Za-z.\-]+)\.yaml$", path)
            keep = bool(art and art[1] == art[2]) if source == "atomicredteam" else bool(re.match(r"^_gtfobins/[^/]+$", path))
            if not member.isfile() or not keep:
                continue
            handle = tar.extractfile(member)
            if handle is not None:
                with handle:
                    text = handle.read().decode("utf-8", "replace")
                documents.append(text)
                members.append((path, notices(text)))
    parsed = yaml_documents(documents, bun)
    for (path, flags), document in zip(members, parsed, strict=True):
        url = f"{root}/{path}"
        if source == "atomicredteam":
            for test_index, test in enumerate((document or {}).get("atomic_tests") or []):
                executor = test.get("executor") or {}
                command = (executor.get("command") or "").strip()
                if executor.get("name") not in {"sh", "bash"} or not command:
                    continue
                defaults = {key: str((value or {}).get("default", "")) for key, value in (test.get("input_arguments") or {}).items()}
                command = re.sub(r"#\{([^}]+)\}", lambda m: defaults.get(m[1], m[0]), command).strip()
                command = re.sub(r"\bT\d{4}(?:\.\d{3})?\b", "sample", command)
                command = re.sub(r"(?i)atomic[\s_-]?red[\s_-]?team", "test", command)
                command = re.sub(r"(?i)atomic", "test", command)
                yield Extracted(command, {
                    "url": url, "atomic_test_index": test_index,
                    "guid": test.get("auto_generated_guid"), "field": "executor.command",
                }, tuple(sorted(set(flags + notices(command)))))
        else:
            for function, entries in (document.get("functions") or {}).items():
                for entry_index, entry in enumerate(entries or []):
                    variants = []
                    if isinstance(entry.get("code"), str) and entry["code"].strip():
                        variants.append(("default", entry["code"].strip()))
                    for context, value in (entry.get("contexts") or {}).items():
                        if isinstance(value, dict) and isinstance(value.get("code"), str):
                            variants.append((context, value["code"].strip()))
                    for context, command in variants:
                        yield Extracted(command, {
                            "url": url, "function": function,
                            "entry_index": entry_index, "context": context,
                        }, tuple(sorted(set(flags + notices(command)))))


def trajectory_commands(source: str, rows: Iterator[tuple[str, int, dict]]) -> Iterator[Extracted]:
    for url, row_number, row in rows:
        if source == "swesmith":
            for message_index, message in enumerate(row.get("messages") or []):
                if message.get("role") != "assistant":
                    continue
                content = message.get("content") or ""
                for fence_index, match in enumerate(BASH_FENCE.finditer(content)):
                    command = match[1].strip()
                    if command:
                        yield Extracted(command, {
                            "url": url, "row": row_number,
                            "instance_id_sha256": digest(str(row["instance_id"])) if row.get("instance_id") else None,
                            "assistant_message_index": message_index, "bash_fence_index": fence_index,
                        }, notices(command))
        else:
            raw = row.get("steps")
            if not raw or raw == "null":
                continue
            try:
                steps = json.loads(raw)
            except (ValueError, TypeError):
                continue
            for step_index, step in enumerate(steps):
                for tool_index, tool in enumerate(step.get("tools") or []):
                    if not isinstance(tool, dict) or tool.get("fn") != "execute_bash":
                        continue
                    command = tool.get("cmd")
                    if not isinstance(command, str) or not command.strip() or re.match(r"^\$\{?\d+\}?$", command.strip()):
                        continue
                    yield Extracted(command.strip(), {
                        "url": url, "row": row_number,
                        "trial_id_sha256": digest(str(row["trial_id"])) if row.get("trial_id") else None,
                        "step_index": step_index, "tool_index": tool_index,
                    }, notices(command))


def reconstruct(source: str, commands: Iterator[Extracted]) -> Iterator[tuple[int, str, Extracted]]:
    """Count all post-filter records including sessions before selecting command layer."""
    seen = set()
    shapes = Counter()
    heredocs = 0
    ordinal = 0
    for item in commands:
        command = item.command
        key = digest(command)
        if key in seen:
            continue
        if source == "swesmith":
            if "<<" in command:
                if heredocs >= 2000:
                    continue
                heredocs += 1
            else:
                key_shape = shape(command)
                if shapes[key_shape] >= 5:
                    continue
                shapes[key_shape] += 1
        seen.add(key)
        if source == "gtfobins":
            command = re.sub(r"/path/to/\S+", "/tmp/file", command)
            command = re.sub(r"\bLFILE\b|\bLPATH\b", "/tmp/file", command)
            command = re.sub(r"\bLDIR\b", "/tmp/dir", command)
            command = re.sub(r"\bLSO\b", "/tmp/lib.so", command)
            command = re.sub(r"\bDATA\b", "data", command)
        lines = [line.strip() for line in command.splitlines() if line.strip()]
        if len(lines) <= 1:
            yield ordinal, normalize(command.strip()).strip(), item
        ordinal += 1
