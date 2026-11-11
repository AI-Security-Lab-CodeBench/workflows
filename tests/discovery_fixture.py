from __future__ import annotations

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from typing import Iterator

from codebench_pipeline.discovery import (
    InspectedPage,
    SourceProposal,
)

DOC_TEXT = "Human-authored examples\nterraform destroy -auto-approve\n"
LICENSE_TEXT = "MIT License\nPermission is hereby granted to use, copy, modify and distribute."
DOC_URL = "https://fixture.invalid/docs"
LICENSE_URL = "https://fixture.invalid/license"
SERVER = """
from mcp.server.fastmcp import FastMCP

server = FastMCP("fixture-keenable")

@server.tool()
def keenable_search_web_pages(query: str, max_results: int, output_format: str) -> dict[str, list[dict[str, str]]]:
    return {"results": [
        {"url": "https://fixture.invalid/docs", "snippet": "SEARCH ONLY"},
        {"url": "https://fixture.invalid/license", "snippet": "SEARCH LICENSE ONLY"},
        {"url": "https://fixture.invalid/fail"},
        {"url": "https://fixture.invalid/not-fetched"},
    ][:max_results]}

@server.tool()
def keenable_fetch_page_content(url: str, max_chars: int, live: bool = False) -> str:
    if url.endswith("/fail"):
        raise ValueError("fixture fetch denied")
    if url.endswith("/license"):
        return "MIT License\\nPermission is hereby granted to use, copy, modify and distribute."
    return "Human-authored examples\\nterraform destroy -auto-approve\\n"

server.run(transport="stdio")
"""


def proposal(url: str = DOC_URL, license_url: str = LICENSE_URL) -> SourceProposal:
    return SourceProposal(
        source_id="fixture-doc",
        risk="CAT-3.1",
        url=url,
        source_type="documentation",
        origin="human_authored",
        origin_evidence="Human-authored examples",
        commands_evidence="terraform destroy -auto-approve",
        license="MIT",
        license_url=license_url,
        license_evidence="MIT License",
        reuse_decision="permitted",
        revision="fixture-revision",
        decision="proposed",
        rationale="Inspected fixture",
        analysis="The inspected page contains a human-authored in-scope command.",
        confidence="high",
        adapter_requested=True,
    )


def pages(root: Path, url: str = DOC_URL, license_url: str = LICENSE_URL) -> list[InspectedPage]:
    return [
        InspectedPage(
            url=url,
            risk="CAT-3.1",
            retrieved_at="2026-11-09T00:00:00Z",
            evidence_path=root / "doc.json",
            text=DOC_TEXT,
        ),
        InspectedPage(
            url=license_url,
            risk="CAT-3.1",
            retrieved_at="2026-11-09T00:00:00Z",
            evidence_path=root / "license.json",
            text=LICENSE_TEXT,
        ),
    ]


class SnapshotHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = DOC_TEXT.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: str | int) -> None:
        pass


@contextmanager
def snapshot_server() -> Iterator[str]:
    with HTTPServer(("127.0.0.1", 0), SnapshotHandler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/source"
        finally:
            server.shutdown()
            thread.join(timeout=5)
            if thread.is_alive():
                raise RuntimeError("Fixture HTTP server did not terminate")
