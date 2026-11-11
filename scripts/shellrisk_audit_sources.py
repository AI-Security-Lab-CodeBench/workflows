"""Reconstruct provenance in memory; emit an apply_patch payload, not raw corpora."""

from __future__ import annotations

import hashlib
import io
import json
import shutil
import sys
import urllib.error
import urllib.request
from collections import Counter
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Final

import pyarrow.parquet as pq

from shellrisk_audit_adapters import CODE_REVISION, PINS, digest, reconstruct, repository_commands, trajectory_commands

ROOT: Final = Path(__file__).resolve().parent.parent
DATASET: Final = "kontext-security/ShellRisk-Bench"
REVISION: Final = "437467862139b4e9cdd5322024ef3434a67c7ec8"
BUN: Final = shutil.which("bun") or "/Users/belyak/.omo/runtime/bun/bin/bun"
LICENSE_PATHS: Final = {
    "swesmith": "README.md", "terminalbench": "README.md",
    "nl2bash": "data/bash/LICENSE", "atomicredteam": "LICENSE.txt",
    "gtfobins": "LICENSE", "payloads": "LICENSE",
}


class Fetcher:
    """Mutable evidence accumulator; bodies are returned only in memory."""

    def __init__(self) -> None:
        self.evidence: list[dict] = []

    def fetch(self, url: str) -> bytes:
        evidence = {"url": url, "retrieved_at": datetime.now(UTC).isoformat()}
        self.evidence.append(evidence)
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "CodeBench-inert-provenance-audit"})
            with urllib.request.urlopen(request, timeout=180) as response:
                body = response.read()
                evidence.update(status=response.status, bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
                return body
        except urllib.error.HTTPError as error:
            evidence.update(status=error.code, failure="http_error")
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            evidence.update(status=None, failure=type(error).__name__)
            raise


def parquet_rows(fetcher: Fetcher, url: str, columns: list[str] | None = None):
    body = fetcher.fetch(url)
    with io.BytesIO(body) as buffer:
        parquet = pq.ParquetFile(buffer)
        row_number = 0
        for batch in parquet.iter_batches(batch_size=32, columns=columns):
            for row in batch.to_pylist():
                yield url, row_number, row
                row_number += 1


def trajectory_rows(fetcher: Fetcher, source: str):
    repo, pin = PINS[source]
    listing = json.loads(fetcher.fetch(f"https://huggingface.co/api/datasets/{repo}/tree/{pin}/data?limit=100"))
    paths = sorted(entry["path"] for entry in listing if entry["path"].endswith(".parquet"))
    expected = 47 if source == "swesmith" else 2
    assert len(paths) == expected, (source, len(paths))
    columns = ["messages", "instance_id"] if source == "swesmith" else ["steps", "trial_id"]
    for index, path in enumerate(paths):
        print(f"PROGRESS {source} shard {index + 1}/{len(paths)}", file=sys.stderr, flush=True)
        yield from parquet_rows(fetcher, f"https://huggingface.co/datasets/{repo}/resolve/{pin}/{path}", columns)


def license_evidence(fetcher: Fetcher, source: str) -> dict:
    repo, pin = PINS[source]
    trajectory = source in {"swesmith", "terminalbench"}
    root = f"https://huggingface.co/datasets/{repo}/resolve/{pin}" if trajectory else f"https://raw.githubusercontent.com/{repo}/{pin}"
    url = f"{root}/{LICENSE_PATHS[source]}"
    try:
        text = fetcher.fetch(url).decode("utf-8", "replace")
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return {"url": url, "scope": "no_grant_established", "document_accessible": False}
    expected = {"swesmith": "mit", "terminalbench": "apache-2.0", "nl2bash": "mit", "atomicredteam": "mit", "gtfobins": "gnu"} .get(source)
    return {
        "url": url, "document_accessible": True,
        "expected_license_marker_present": bool(expected and expected in text.lower()),
        "scope": "uploader_dataset_card_only" if trajectory else "dataset_directory" if source == "nl2bash" else "repository",
        "sha256": digest(text), "notice_inspection": "document_body_read; individual source text scanned for notice markers, not legal clearance",
    }


def run_audit() -> dict[str, str]:
    fetcher = Fetcher()
    corpus = []
    for split in ("train", "test"):
        url = f"https://huggingface.co/datasets/{DATASET}/resolve/{REVISION}/data/{split}-00000-of-00001.parquet"
        for _, row_number, row in parquet_rows(fetcher, url):
            assert row["id"] == "sha256:" + digest(row["command"])
            corpus.append({**row, "split": split, "row_number": row_number + 1})
    assert len(corpus) == len({row["id"] for row in corpus}) == 20966
    manifest = {
        "retrieved_at": datetime.now(UTC).isoformat(), "dataset": DATASET, "revision": REVISION,
        "adapter_revision": CODE_REVISION, "raw_retention": "memory_only",
        "commands_executed": False, "downloaded_code_executed": False,
        "corpus_rows": len(corpus), "sources": {}, "access": fetcher.evidence,
        "indexing": "corpus row_number one-based per split; upstream row, fence, message, entry and tool indices zero-based; nl2bash line one-based",
        "normalization": {"url_regex": r"https?://[^\s'\"`;|)]+", "url_flags": "IGNORECASE", "url_replacement": "http://example.com", "ip_regex": r"\b(?:\d{1,3}\.){3}\d{1,3}\b", "ip_replacement": "1.1.1.1", "base64_regex": r"(?<![A-Za-z0-9+/])[A-Za-z0-9+/]{40,}={0,2}(?![A-Za-z0-9+/])", "base64_replacement": "BASE64"},
    }
    for name in ("_common", *PINS):
        url = f"https://raw.githubusercontent.com/kontext-security/shellrisk-bench/{CODE_REVISION}/adapters/{name}.py"
        fetcher.fetch(url)
    records = []
    for source, (repo, pin) in PINS.items():
        selected = [row for row in corpus if row["source"] == source]
        by_hash = {row["id"][7:]: [] for row in selected}
        expected_ordinals = {int(row["upstream_id"].rsplit("-", 1)[1]) for row in selected}
        at_ordinal = {}
        license_info = license_evidence(fetcher, source)
        source_report = {"repository": repo, "revision": pin, "license": license_info, "failure": None}
        manifest["sources"][source] = source_report
        emitted = 0
        try:
            commands = trajectory_commands(source, trajectory_rows(fetcher, source)) if source in {"swesmith", "terminalbench"} else repository_commands(source, fetcher.fetch, BUN)
            for ordinal, normalized, extracted in reconstruct(source, commands):
                emitted += 1
                key = digest(normalized)
                if ordinal in expected_ordinals:
                    at_ordinal[ordinal] = key
                if key in by_hash:
                    by_hash[key].append({
                        "locator": extracted.locator, "adapter_ordinal": ordinal,
                        "notice_flags": list(extracted.notice_flags),
                    })
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            source_report["failure"] = {"type": type(error).__name__, "last_access": fetcher.evidence[-1]}
        source_report["command_layer_reconstructed"] = emitted
        source_report["counts"] = Counter()
        for row in selected:
            ordinal = int(row["upstream_id"].rsplit("-", 1)[1])
            matches = by_hash[row["id"][7:]]
            exact = [item for item in matches if item["adapter_ordinal"] == ordinal]
            status = "normalized_and_ordinal_match" if len(exact) == 1 else "ambiguous" if len(exact) > 1 else "normalized_match_ordinal_mismatch" if matches else "not_recovered"
            caveats = []
            if not exact:
                caveats.append("pinned_adapter_to_corpus_linkage_unconfirmed")
            if source_report["failure"]:
                caveats.append("source_scan_incomplete")
            if source in {"swesmith", "terminalbench"}:
                decision = "needs_review"
                caveats.append("uploader_license_does_not_establish_rights_to_embedded_or_copied_material")
            elif source == "payloads" or not license_info["document_accessible"]:
                decision = "needs_review"
                caveats.append("no_redistribution_grant_established")
            else:
                decision = "terms_review" if exact else "needs_review"
                caveats.append("repository_or_directory_terms_are_evidence_not_per_row_legal_clearance")
            if source == "gtfobins":
                caveats.append("GPL_3_0_terms_review_required_not_automatic_rejection")
            record = {key: row[key] for key in ("id", "split", "row_number", "source", "upstream_id")}
            record.update(
                match_status=status, locators=exact or matches,
                ordinal_observed_normalized_sha256=at_ordinal.get(ordinal),
                access="original_locator_fetched" if matches else "original_not_located",
                accessibility={"status": "accessible" if matches else "unresolved", "evidence_ref": f"upstream-manifest.json#/sources/{source}", "scan_complete": source_report["failure"] is None},
                license_url=license_info["url"], license_scope=license_info["scope"],
                license_decision=decision, reuse_decision=decision,
                license_evidence={"ref": f"upstream-manifest.json#/sources/{source}/license", "kind": "uploader_assertion" if source in {"swesmith", "terminalbench"} else "no_grant_established" if source == "payloads" else "source_license_document", "sha256": license_info.get("sha256")},
                caveats=caveats,
                transformation_basis=f"inspected adapters@{CODE_REVISION}; source filters/dedup and shared normalization; sha256 text comparison plus ordinal",
            )
            records.append(record)
            source_report["counts"][status] += 1
        print(f"RESULT {source} {dict(source_report['counts'])}", file=sys.stderr, flush=True)
    records.sort(key=lambda row: (row["split"], row["row_number"]))
    assert len(records) == 20966
    assert len({(row["split"], row["row_number"]) for row in records}) == 20966
    manifest["counts"] = dict(Counter(row["match_status"] for row in records))
    manifest["license_decisions"] = dict(Counter(row["license_decision"] for row in records))
    ledger = "".join(json.dumps(row, ensure_ascii=True, separators=(",", ":")) + "\n" for row in records)
    manifest["metadata_sha256"] = digest(ledger)
    report = "\n".join([
        "# Independent upstream provenance reconstruction", "",
        f"Date: {datetime.now(UTC).date()}. Corpus: {DATASET}@{REVISION}.",
        f"Inspected adapter specification: {CODE_REVISION}. Exact build linkage is not assumed.",
        "", "## Results", "",
        f"Attempted and reconciled metadata records: {len(records):,}. Unique corpus keys: 20,966.",
        f"Match counts: `{json.dumps(manifest['counts'], sort_keys=True)}`.",
        f"License decisions: `{json.dumps(manifest['license_decisions'], sort_keys=True)}`.",
        "", "| Source | Corpus rows | Reconstruction outcomes |", "|---|---:|---|",
        *[f"| {source} | {sum(info['counts'].values())} | {json.dumps(info['counts'], sort_keys=True)} |" for source, info in manifest["sources"].items()],
        "", "## Evidence and limits", "",
        "The JSONL contains only metadata, hashes, locators and caveats. Corpora and third-party code remain in memory; no dataset command or downloaded adapter is executed. Parquet is processed one shard at a time in batches of 32 rows. Tar members preserve archive order. YAML uses Bun.YAML.parse through fixed locally authored code. No extraction to disk occurs.",
        "Hash matching alone does not establish ordinal agreement. Both are recorded separately; mismatches are not promoted to verified reconstruction. The source ordinal includes sessions before command-layer selection. The SHA-256 id is checked against each corpus command.",
        "Available trajectory instance/trial identifiers are SHA-256 hashed to avoid retaining personal-looking identifiers; missing or empty identifiers are null. File, row and nested indices provide the exact locator independently.",
        "License document bodies are fetched and hashed. Matched source snippets are scanned individually for notice/copyright/external-reference/embedded-program markers. This is not manual per-row notice interpretation or a proof of ownership. Uploader assertions never clear embedded/copied material. Trajectories remain needs_review. MIT directory/repository evidence and GPL repository evidence require terms review; GPL is not automatically rejected. Unmatched records remain needs_review. Payloads has no grant established.",
        "Access evidence, full regexes, pins and per-source failures are in upstream-manifest.json. Repository terms apply only as potential evidence after matching an individual source record. Lead owns final suitability and semantic/class decisions.",
        "", "## Reproduction", "",
        "Run the authored parser with PyArrow available; it emits an apply_patch payload on stdout and progress on stderr. Apply that payload using the apply_patch tool. It does not write raw corpora or generated metadata itself.",
        "```sh", "PYTHONPATH=/Users/belyak/.cache/omo-data-scientist/py-cp314 /opt/homebrew/opt/python@3.14/bin/python3.14 -B scripts/shellrisk_audit_sources.py", "```", "",
        "Programmatic use: import shellrisk_audit_sources, call run_audit(), then apply each returned UTF-8 artifact through apply_patch. Network failures are represented explicitly, not treated as complete source scans.",
        "",
    ])
    return {
        "research/shellrisk-audit/upstream-manifest.json": json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        "data/review/shellrisk-audit/upstream.jsonl": ledger,
        "research/shellrisk-audit/upstream-report.md": report,
    }


def patch_for(path: str, content: str) -> str:
    existing = ROOT / path
    if existing.exists():
        old = existing.read_text(encoding="utf-8")
        body = "*** Update File: " + str(existing) + "\n@@\n"
        body += "".join("-" + line + "\n" for line in old.splitlines())
    else:
        body = "*** Add File: " + str(existing) + "\n"
    body += "".join("+" + line + "\n" for line in content.splitlines())
    return "*** Begin Patch\n" + body + "*** End Patch\n"


if __name__ == "__main__":
    artifacts = run_audit()
    if "--serve" in sys.argv:
        payload = json.dumps(artifacts).encode()

        class ArtifactHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        with HTTPServer(("127.0.0.1", 0), ArtifactHandler) as server:
            print(f"ARTIFACTS_READY http://127.0.0.1:{server.server_port}", flush=True)
            server.handle_request()
    else:
        for artifact_path, artifact_text in artifacts.items():
            print(patch_for(artifact_path, artifact_text))
