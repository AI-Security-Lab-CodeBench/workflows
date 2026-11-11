from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from collections.abc import Callable
from dataclasses import asdict
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from time import perf_counter_ns

import bashlex
from bashlex.errors import ParsingError
from oracles import (
    FIXTURES,
    LANGUAGE,
    LEXICAL_SCAN,
    TREE_PARSER,
    InputT,
    Observation,
    ProtocolError,
    Response,
    ResultT,
    Span,
    Timing,
    mismatch,
    observe_bashlex,
    observe_regex,
    observe_tree,
)


def hot_timing(
    parse: Callable[[InputT], ResultT], sources: tuple[InputT, ...], iterations: int
) -> Timing:
    failures = 0
    for source in sources:
        try:
            parse(source)
        except (ParsingError, NotImplementedError):
            pass
    start = perf_counter_ns()
    for _ in range(iterations):
        for source in sources:
            try:
                parse(source)
            except (ParsingError, NotImplementedError):
                failures += 1
    elapsed = perf_counter_ns() - start
    count = iterations * len(sources)
    return {
        "metric": "hot_parse_attempts_including_rejections",
        "total_ns": elapsed,
        "count": count,
        "ns_per_attempt": elapsed / count,
        "raised_attempts": failures,
    }


def invoke(binary: Path, mode: str, sources: tuple[str, ...]) -> tuple[list[Response], int]:
    payload = "".join(json.dumps({"command": source}) + "\n" for source in sources)
    start = perf_counter_ns()
    process = subprocess.run(
        [str(binary), "-mode", mode],
        input=payload,
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    elapsed = perf_counter_ns() - start
    if process.returncode != 0 and not (mode == "analyze" and process.returncode == 1):
        raise subprocess.CalledProcessError(
            process.returncode, process.args, process.stdout, process.stderr
        )
    rows: list[Response] = [
        json.loads(line) for line in process.stdout.splitlines() if line.strip()
    ]
    if not all(isinstance(row, dict) for row in rows):
        raise ProtocolError(f"{mode} returned a non-object JSONL row")
    if process.returncode == 1 and not any(row.get("error") for row in rows):
        raise ProtocolError(f"{mode} failed without per-record error evidence: {process.stderr}")
    return rows, elapsed


def observe_mvdan(row: Response) -> Observation:
    error = row.get("error")
    analysis = row.get("analysis") or {}
    if error:
        return Observation(False, "syntax_error", (), diagnostics=(str(error),))
    heads = []
    for call in analysis.get("calls") or []:
        words = call.get("words") or []
        if words:
            heads.append(words[0].get("value") if words[0].get("static") else None)
    spans = tuple(
        Span(node["kind"], node["start"], node["end"], "utf8_bytes")
        for node in analysis.get("nodes") or []
    )
    return Observation(
        True,
        "accepted",
        tuple(heads),
        spans,
        tuple(analysis.get("reasons") or []),
        analysis.get("eligible"),
    )


def mvdan_timing(binary: Path, iterations: int) -> Timing:
    sources = tuple(fixture.source for _ in range(iterations) for fixture in FIXTURES)
    try:
        rows, transport_ns = invoke(binary, "benchmark", sources)
    except (subprocess.CalledProcessError, ProtocolError, json.JSONDecodeError) as error:
        return {"metric": "unavailable", "error": str(error)}
    totals: list[tuple[int, int]] = []
    for row in rows:
        parse_ns = row.get("parse_ns")
        count = row.get("count")
        if not isinstance(parse_ns, int) or not isinstance(count, int):
            return {
                "metric": "unavailable",
                "error": "benchmark response lacks integer parse_ns/count",
                "response": rows,
                "subprocess_transport_ns": transport_ns,
            }
        totals.append((parse_ns, count))
    if not totals:
        return {
            "metric": "unavailable",
            "error": "benchmark returned no JSONL rows",
            "subprocess_transport_ns": transport_ns,
        }
    elapsed = sum(total[0] for total in totals)
    count = sum(total[1] for total in totals)
    if count != len(sources):
        raise ProtocolError(f"benchmark counted {count} attempts; expected {len(sources)}")
    return {
        "metric": "go_reported_parse_attempts",
        "total_ns": elapsed,
        "count": count,
        "ns_per_attempt": elapsed / count,
        "subprocess_transport_ns": transport_ns,
        "go_errors": sum(row.get("errors", 0) for row in rows),
        "go_analyze_ns": sum(row.get("analyze_ns", 0) for row in rows),
    }


def main() -> None:
    cli = argparse.ArgumentParser(description="Compare inert Bash parsing; never execute fixture commands.")
    cli.add_argument("--binary", required=True, type=Path)
    cli.add_argument("--output", required=True, type=Path)
    cli.add_argument("--iterations", type=int, default=100)
    args = cli.parse_args()
    if args.iterations <= 0:
        cli.error("--iterations must be positive")
    binary = args.binary.resolve(strict=True)
    sources = tuple(fixture.source for fixture in FIXTURES)
    mvdan_rows, transport_ns = invoke(binary, "analyze", sources)
    if len(mvdan_rows) != len(FIXTURES):
        raise ProtocolError(f"analyze returned {len(mvdan_rows)} rows; expected {len(FIXTURES)}")
    observations = []
    for fixture, mvdan_row in zip(FIXTURES, mvdan_rows, strict=True):
        tools = {
            "mvdan": observe_mvdan(mvdan_row),
            "tree_sitter": observe_tree(fixture.source),
            "bashlex": observe_bashlex(fixture.source),
            "regex": observe_regex(fixture.source),
        }
        observations.append({
            "fixture": asdict(fixture),
            "utf8_bytes": len(fixture.source.encode("utf-8")),
            "unicode_characters": len(fixture.source),
            "tools": {
                name: {**asdict(result), "mismatches": mismatch(fixture, result)}
                for name, result in tools.items()
            },
        })
    timing = {
        "tree_sitter": hot_timing(
            TREE_PARSER.parse, tuple(source.encode("utf-8") for source in sources), args.iterations
        ),
        "bashlex": hot_timing(bashlex.parse, sources, args.iterations),
        "regex": {
            **hot_timing(lambda source: tuple(LEXICAL_SCAN.finditer(source)), sources, args.iterations),
            "metric": "hot_compiled_regex_lexical_scan_not_parser",
        },
        "mvdan": mvdan_timing(binary, args.iterations),
        "mvdan_analyze_transport": {
            "metric": "batch_subprocess_wall_time_includes_startup_json_and_analysis",
            "total_ns": transport_ns,
            "count": len(FIXTURES),
            "ns_per_record": transport_ns / len(FIXTURES),
        },
    }
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "versions": {name: version(name) for name in ("tree-sitter", "tree-sitter-bash", "bashlex")},
            "tree_sitter_abi": LANGUAGE.abi_version,
            "binary": str(binary),
        },
        "method": {
            "fixture_count": len(FIXTURES),
            "iterations": args.iterations,
            "warmup": "one corpus pass for each in-process tool; Go warmup controlled by binary",
            "timing_scope": "Python hot parse excludes imports, encoding, extraction and oracle checks",
            "oracle_scope": "ordered command heads, including nested syntactic calls; not runtime execution or arguments",
            "head_normalization": "tree-sitter static head only: shlex quote removal after backslash-newline removal",
            "limits": "fixed tiny corpus; unsupported syntax and malformed-input costs included; no speed extrapolation",
            "positions": "half-open; tree-sitter/mvdan UTF-8 bytes, bashlex/regex Unicode characters",
            "regex": "compiled lexical heuristic, not a Bash parser; acceptance deliberately null",
            "execution": "only the supplied analysis binary runs; fixture strings travel as inert JSONL stdin",
        },
        "fixtures": observations,
        "timing": timing,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.write("\n")


if __name__ == "__main__":
    main()
