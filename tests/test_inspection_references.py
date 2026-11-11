import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from discovery_fixture import LICENSE_URL, SERVER
from pydantic import ValidationError

from codebench_pipeline.discovery import InspectedSelection, RiskSearch, SearchRequest, discover
from codebench_pipeline.dataset_seed_config import SearchSeeds


class InspectionReferenceTests(unittest.TestCase):
    def test_explicit_license_reference_is_inspected_first_within_the_existing_page_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            server = root / "server.py"
            server.write_text(SERVER)
            request = SearchRequest(
                searches=[
                    RiskSearch(
                        risk="CAT-3.1",
                        queries=["commands"],
                        inspection_urls=[LICENSE_URL],
                    )
                ],
                model="fixture/small",
                pages_per_risk=1,
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
                patch("codebench_pipeline.discovery.infer", return_value=InspectedSelection(sources=[])),
            ):
                result = discover(request, root / "discovery")
            self.assertEqual(result.failures, [])
            self.assertEqual([page.url for page in result.inspected], [LICENSE_URL])

    def test_seed_conversion_retains_references_and_rejects_non_http_or_credential_urls(self) -> None:
        source = SearchSeeds.model_validate(
            {"searches": [{"risk": "CAT-91.7", "queries": ["license"], "inspection_urls": [LICENSE_URL]}]}
        )
        self.assertEqual(source.to_search_request("fixture/small").searches[0].inspection_urls, [LICENSE_URL])
        for url in ("file:///etc/passwd", "https://user:password@example.org/license"):
            with self.subTest(url=url), self.assertRaises(ValidationError):
                RiskSearch(risk="CAT-91.7", queries=["license"], inspection_urls=[url])
