import unittest

from pydantic import ValidationError

from codebench_pipeline.dataset_seed_config import SearchSeeds
from codebench_pipeline.discovery_models import RiskSearch

LICENSE_URL = "https://example.org/license"


class InspectionReferenceTests(unittest.TestCase):
    def test_seed_conversion_retains_references_and_rejects_non_http_or_credential_urls(self) -> None:
        source = SearchSeeds.model_validate(
            {"searches": [{"risk": "CAT-91.7", "queries": ["license"], "inspection_urls": [LICENSE_URL]}]}
        )
        self.assertEqual(source.to_search_request("fixture/small").searches[0].inspection_urls, [LICENSE_URL])
        for url in ("file:///etc/passwd", "https://user:password@example.org/license"):
            with self.subTest(url=url), self.assertRaises(ValidationError):
                RiskSearch(risk="CAT-91.7", queries=["license"], inspection_urls=[url])
