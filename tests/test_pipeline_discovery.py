from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import anyio
from discovery_fixture import DOC_TEXT, DOC_URL, LICENSE_URL, SERVER, pages, proposal, snapshot_server
from pydantic import ValidationError

from codebench_pipeline.discovery import (
    DiscoveryError,
    DiscoveryOutput,
    InspectedSelection,
    RiskSearch,
    SearchRequest,
    SourceProposal,
    acquire,
    discover,
    gate_selection,
)
from codebench_pipeline.mcp_transport import connect_keenable
from codebench_pipeline.models import AdapterSpec, DatasetSpec, SourceDocument


class PipelineDiscoveryTests(unittest.TestCase):
    def test_stdio_mcp_discovers_names_fetches_bounded_pages_and_retains_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            server = root / "server.py"
            server.write_text(SERVER)
            request = SearchRequest(
                searches=[RiskSearch(risk="CAT-3.1", queries=["commands and license"])],
                model="fixture/small",
                results_per_query=4,
                pages_per_risk=3,
            )
            config = root / "request.json"
            config.write_text(request.model_dump_json())
            with (
                patch.dict(
                    os.environ,
                    {
                        "CODEBENCH_KEENABLE_COMMAND": sys.executable,
                        "CODEBENCH_KEENABLE_ARGS": json.dumps([str(server)]),
                        "CODEBENCH_KEENABLE_ENV": "{}",
                        "CODEBENCH_KEENABLE_URL": "",
                    },
                ),
                patch(
                    "codebench_pipeline.discovery.infer",
                    return_value=InspectedSelection(sources=[proposal()]),
                ),
            ):
                result = discover(config, root / "discovery")
            self.assertEqual([page.url for page in result.inspected], [DOC_URL, LICENSE_URL])
            self.assertEqual([failure.operation for failure in result.failures], ["fetch"])
            self.assertEqual(result.sources[0].decision, "proposed")
            self.assertFalse(result.downloaded_source_ready)
            self.assertEqual(result.adapter_requests[0].source_url, DOC_URL)
            self.assertIn("SEARCH ONLY", (root / "discovery/search/CAT-3.1-0.json").read_text())
            self.assertNotIn("SEARCH ONLY", result.inspected[0].text)
            self.assertTrue((root / "discovery/fetched/CAT-3.1-2.json").exists())
            self.assertFalse((root / "discovery/fetched/CAT-3.1-3.json").exists())
            persisted = DiscoveryOutput.model_validate_json((root / "discovery/discovery.json").read_text())
            self.assertEqual(persisted, result)

    def test_selection_excludes_origins_and_never_approves_unknown_license(self) -> None:
        cases = (
            ({"origin": "agent"}, "exclude"),
            ({"origin": "sft"}, "exclude"),
            ({"origin": "synthetic"}, "exclude"),
            ({"source_type": "agent_results"}, "exclude"),
            ({"source_type": "SFT dataset"}, "exclude"),
            ({"license": "unknown"}, "needs_review"),
            ({"reuse_decision": "unknown"}, "needs_review"),
            ({"reuse_decision": "prohibited"}, "exclude"),
            ({"origin": "unknown", "origin_evidence": ""}, "needs_review"),
        )
        for changes, expected in cases:
            with self.subTest(changes=changes):
                value = SourceProposal.model_validate({**proposal().model_dump(), **changes})
                result = gate_selection(InspectedSelection(sources=[value]), pages(Path("/unused")))
                self.assertEqual(result.sources[0].decision, expected)
                self.assertFalse(result.sources[0].adapter_requested)

    def test_documentation_discovery_keeps_semantics_without_approving_dataset_reuse(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            server = root / "server.py"
            server.write_text(SERVER)
            request = SearchRequest(
                searches=[RiskSearch(risk="CAT-3.1", queries=["command behavior"])],
                model="fixture/small",
                pages_per_risk=2,
            )
            source = proposal().model_copy(
                update={
                    "license": "unknown",
                    "license_evidence": "",
                    "license_url": "",
                    "reuse_decision": "unknown",
                }
            )
            with (
                patch.dict(
                    os.environ,
                    {
                        "CODEBENCH_KEENABLE_COMMAND": sys.executable,
                        "CODEBENCH_KEENABLE_ARGS": json.dumps([str(server)]),
                        "CODEBENCH_KEENABLE_ENV": "{}",
                        "CODEBENCH_KEENABLE_URL": "",
                    },
                ),
                patch(
                    "codebench_pipeline.discovery.infer",
                    return_value=InspectedSelection(sources=[source]),
                ),
            ):
                result = discover(request, root / "discovery", purpose="documentation")
            self.assertEqual(result.sources[0].decision, "proposed")
            self.assertEqual(result.sources[0].reuse_decision, "unknown")
            self.assertEqual(result.adapter_requests, [])
            self.assertEqual(result.purpose, "documentation")
            with patch("codebench_pipeline.acquisition.urlopen") as download:
                with self.assertRaises(DiscoveryError):
                    acquire(
                        result, root / "acquired", "fixture", "revision", root / "absent", "fixture/small"
                    )
            download.assert_not_called()

    def test_selection_rejects_unknown_sources_and_withholds_ungrounded_claims(self) -> None:
        cases = (
            {"url": "https://invented.invalid/source"},
            {"license_url": "https://invented.invalid/license"},
            {"risk": "CAT-3.2"},
            {"commands_evidence": "invented command"},
            {"origin_evidence": "invented origin"},
            {"license_evidence": "invented license"},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                value = SourceProposal.model_validate({**proposal().model_dump(), **changes})
                if "url" in changes or "risk" in changes:
                    with self.assertRaises(DiscoveryError):
                        gate_selection(InspectedSelection(sources=[value]), pages(Path("/unused")))
                else:
                    result = gate_selection(InspectedSelection(sources=[value]), pages(Path("/unused")))
                    self.assertEqual(result.sources[0].decision, "needs_review")
                    self.assertFalse(result.sources[0].adapter_requested)
                    for field, original in changes.items():
                        self.assertEqual(result.sources[0].model_dump()[field], original)

    def test_withheld_proposal_does_not_abort_valid_independent_sources(self) -> None:
        withheld = proposal().model_copy(
            update={
                "source_id": "withheld",
                "decision": "exclude",
                "commands_evidence": "Explanation rather than an excerpt",
                "license_url": "https://example.org/uninspected-license",
            }
        )
        result = gate_selection(InspectedSelection(sources=[withheld, proposal()]), pages(Path("/unused")))
        self.assertEqual([row.decision for row in result.sources], ["exclude", "proposed"])
        self.assertFalse(result.sources[0].adapter_requested)
        self.assertEqual(result.sources[0].commands_evidence, withheld.commands_evidence)

    def test_request_enforces_per_risk_bounds_before_creating_output(self) -> None:
        for count in (0, 5):
            with self.subTest(count=count), self.assertRaises(ValidationError):
                RiskSearch(risk="CAT-3.1", queries=["query"] * count)
        with tempfile.TemporaryDirectory() as temporary:
            request = SearchRequest(
                searches=[
                    RiskSearch(risk="CAT-3.1", queries=["one"]),
                    RiskSearch(risk="CAT-3.1", queries=["two"]),
                ],
                model="fixture/small",
            )
            out = Path(temporary) / "discovery"
            with self.assertRaises(DiscoveryError):
                discover(request, out)
            self.assertFalse(out.exists())

    def test_ambiguous_transport_is_preserved_as_failure_without_inference(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            request = SearchRequest(
                searches=[RiskSearch(risk="CAT-3.1", queries=["query"])],
                model="fixture/small",
            )
            with patch.dict(
                os.environ,
                {
                    "CODEBENCH_KEENABLE_COMMAND": "unused",
                    "CODEBENCH_KEENABLE_URL": "https://example.org/mcp",
                },
            ):
                result = discover(request, Path(temporary) / "discovery")
            self.assertEqual([failure.operation for failure in result.failures], ["mcp"])
            self.assertEqual(result.sources, [])

    def test_public_http_transport_is_available_without_worker_environment(self) -> None:
        async def connect() -> None:
            async with connect_keenable():
                raise AssertionError("The transport probe should have stopped before initialization")

        with (
            patch.dict(
                os.environ,
                {
                    "CODEBENCH_KEENABLE_COMMAND": "",
                    "CODEBENCH_KEENABLE_URL": "",
                    "CODEBENCH_KEENABLE_HEADERS": "{}",
                },
            ),
            patch(
                "codebench_pipeline.mcp_transport.streamable_http_client", side_effect=RuntimeError
            ) as client,
        ):
            with self.assertRaises(RuntimeError):
                anyio.run(connect)
        self.assertEqual(client.call_args.args[0], "https://api.keenable.ai/mcp")

    def test_acquisition_downloads_full_snapshot_and_registers_validated_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, snapshot_server() as url:
            root = Path(temporary)
            selection = DiscoveryOutput(
                sources=[proposal(url)],
                inspected=pages(root, url),
                adapter_requests=[],
                failures=[],
            )
            semantics = root / "supplied-semantics.jsonl"
            semantics.write_text(
                json.dumps(
                    {
                        "utility": "terraform",
                        "subcommands": ["destroy"],
                        "url": DOC_URL,
                        "retrieved_at": "2026-11-09T00:00:00Z",
                        "snippet": "Inspected destroy semantics.",
                        "version_limits": "fixture",
                    }
                )
                + "\n"
            )
            adapter = AdapterSpec(format="text", rationale="Full plain text")
            with patch("codebench_pipeline.acquisition.propose_adapter", return_value=adapter):
                dataset = acquire(
                    selection, root / "acquired", "fixture", "revision", semantics, "fixture/small"
                )
            self.assertEqual(
                DatasetSpec.model_validate_json((root / "acquired/dataset.json").read_text()), dataset
            )
            document = SourceDocument.model_validate_json(dataset.documents.read_text().strip())
            self.assertEqual(document.local_path.read_bytes(), DOC_TEXT.encode())
            self.assertEqual(document.provenance_type, "human_authored")
            self.assertEqual(dataset.semantics.read_bytes(), semantics.read_bytes())
            self.assertTrue((root / "acquired/source-0000/receipt.json").exists())

    def test_acquisition_refuses_unknown_license_without_network_or_registration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            value = SourceProposal.model_validate({**proposal().model_dump(), "license": "unknown"})
            selection = DiscoveryOutput(
                sources=[value], inspected=pages(root), adapter_requests=[], failures=[]
            )
            with patch("codebench_pipeline.acquisition.urlopen") as download:
                with self.assertRaises(DiscoveryError):
                    acquire(
                        selection, root / "acquired", "fixture", "revision", root / "absent", "fixture/small"
                    )
            download.assert_not_called()
            self.assertFalse((root / "acquired/dataset.json").exists())
