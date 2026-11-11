from __future__ import annotations

import csv
import hashlib
import json
import re
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

if __package__:
    from .validate_dataset import contains_secret, load_jsonl, web_url
else:
    from validate_dataset import contains_secret, load_jsonl, web_url

EXCLUDED_ORIGIN = re.compile(
    r"agent.*(?:trace|trajector)|generated|synthetic|(?:^|[\s_-])sft(?:$|[\s_-])", re.I
)


def write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n" for row in records),
        encoding="utf-8",
    )


def write_csv(path: Path, records: list[dict], fields: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in records:
            writer.writerow(
                {
                    key: json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value
                    for key, value in row.items()
                }
            )


def source_eligible(source: dict) -> bool:
    source_type = source.get("source_type", "")
    dataset = re.search(r"dataset|corpus|benchmark|trajector", source_type, re.I)
    origin_verified = not dataset or source.get("provenance_type") in {"human_authored", "human_collected"}
    return (
        source.get("accessibility") == "accessible"
        and source.get("reuse_decision") == "permitted"
        and source.get("commands_present") is True
        and bool(source.get("license"))
        and web_url(source.get("url"))
        and web_url(source.get("license_url"))
        and not EXCLUDED_ORIGIN.search(source_type)
        and source.get("eligible") is not False
        and origin_verified
    )


def capture_sources(inputs: Path, out: Path) -> dict[str, int]:
    candidates = load_jsonl(inputs / "candidates.jsonl")
    wanted = {key for candidate in candidates for key in candidate["source_ids"]}
    captures, failures = [], []
    for source in load_jsonl(inputs / "sources.jsonl"):
        if source["source_id"] not in wanted or not source_eligible(source):
            continue
        url = source["url"]
        if url.startswith("https://github.com/") and "/blob/" in url:
            url = url.replace("https://github.com/", "https://raw.githubusercontent.com/", 1).replace(
                "/blob/", "/", 1
            )
        request = urllib.request.Request(url, headers={"User-Agent": "CodeBench-inert-source-pipeline/1"})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read(8_000_001)
            if len(body) > 8_000_000:
                raise ValueError("Source exceeds 8 MB capture limit")
            text = body.decode("utf-8")
            if contains_secret(text):
                raise ValueError("Possible sensitive value; source body not persisted")
            filename = hashlib.sha256(source["source_id"].encode()).hexdigest()[:20] + ".txt"
            out.mkdir(parents=True, exist_ok=True)
            (out / filename).write_text(text, encoding="utf-8")
            captures.append(
                dict(
                    source,
                    local_path=filename,
                    fetched_url=url,
                    captured_at=datetime.now(UTC).isoformat(),
                    sha256=hashlib.sha256(body).hexdigest(),
                )
            )
        except (urllib.error.URLError, TimeoutError, UnicodeError, ValueError) as error:
            failures.append(
                {"source_id": source["source_id"], "url": url, "error": str(error), "status": "needs_review"}
            )
    write_jsonl(out / "manifest.jsonl", captures)
    write_jsonl(out / "failures.jsonl", failures)
    return {"captured": len(captures), "failed": len(failures)}


def inventory_tables(research: Path, out: Path) -> dict[str, int]:
    datasets = load_jsonl(research / "datasets.jsonl")
    documents = load_jsonl(research / "documentation.jsonl")
    write_csv(
        out / "datasets.csv",
        datasets,
        (
            "id",
            "source",
            "license",
            "full_link",
            "opened_at",
            "source_type",
            "risks",
            "provenance_type",
            "eligible",
            "decision",
            "license_url",
            "local_path",
            "observed_matches",
            "evidence",
        ),
    )
    write_csv(
        out / "sources.csv",
        documents,
        (
            "source_id",
            "source_full_url",
            "description",
            "trust",
            "license",
            "license_url",
            "opened_at",
            "risks",
            "accessibility",
            "reuse_decision",
            "evidence",
        ),
    )
    return {"datasets": len(datasets), "documentation_sources": len(documents)}
