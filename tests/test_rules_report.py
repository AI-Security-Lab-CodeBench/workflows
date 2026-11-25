import base64
import hashlib
import json
import unittest
from html.parser import HTMLParser

import pandas as pd
from pandera.typing import DataFrame
from rules_report_http_fixture import synthetic_report
from starlette.testclient import TestClient

from codebench_pipeline.rules_artifacts import RulesMetricsTable
from codebench_pipeline.rules_report import render_rules_report

type Measurement = tuple[str, str, str, str, float | None]


def metrics_table(*rows: Measurement) -> DataFrame[RulesMetricsTable]:
    frame = pd.DataFrame(rows, columns=["cohort", "axis", "label", "metric", "value"])
    frame["value"] = frame["value"].astype("float64")
    return RulesMetricsTable.validate(frame)


class ReportDocument(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.policy = ""
        self.inline: dict[str, str] = {}
        self.external: list[str] = []
        self.ids: set[str] = set()
        self._element: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if identity := attributes.get("id"):
            self.ids.add(identity)
        self.external.extend(value for key, value in attrs if key in {"href", "src"} and value)
        if tag == "meta" and attributes.get("http-equiv") == "Content-Security-Policy":
            self.policy = attributes.get("content") or ""
        if tag in {"script", "style"}:
            self._element = attributes.get("id") or tag

    def handle_endtag(self, tag: str) -> None:
        self._element = None

    def handle_data(self, data: str) -> None:
        if self._element is not None:
            self.inline[self._element] = self.inline.get(self._element, "") + data


class RulesReportTest(unittest.TestCase):
    def test_report_preserves_distinct_cohorts_zero_and_undefined_as_inert_data(self) -> None:
        label = '</script><p id="unexpected-report-node">測定</p>'
        document = ReportDocument()
        document.feed(render_rules_report(metrics_table(
            ("overall", "category", label, "recall", 0.625),
            ("unseen", "category", label, "recall", 0),
            ("trained", "category", label, "recall", None),
        )))

        self.assertNotIn("unexpected-report-node", document.ids)
        self.assertEqual(json.loads(document.inline["measurements"]), [
            {"cohort": "overall", "axis": "category", "label": label, "metric": "recall", "value": 0.625},
            {"cohort": "unseen", "axis": "category", "label": label, "metric": "recall", "value": 0},
            {"cohort": "trained", "axis": "category", "label": label, "metric": "recall", "value": None},
        ])

    def test_standalone_document_authorizes_only_its_inline_script_and_styles(self) -> None:
        document = ReportDocument()
        document.feed(render_rules_report(metrics_table()))

        directives = dict(directive.strip().split(" ", 1) for directive in document.policy.split(";"))
        self.assertEqual(directives["default-src"], "'none'")
        for tag in ("script", "style"):
            digest = hashlib.sha256(document.inline[tag].encode("utf-8")).digest()
            self.assertEqual(directives[f"{tag}-src"], f"'sha256-{base64.b64encode(digest).decode()}'")
        self.assertEqual(document.external, [])

    def test_served_report_preserves_content_address_and_offline_sandbox(self) -> None:
        payload = render_rules_report(metrics_table(
            ("overall", "outcome", "", "support", 120),
        )).encode("utf-8")
        with (
            synthetic_report(payload, 18753) as (app, path),
            TestClient(app, base_url="http://127.0.0.1:18753") as client,
        ):
            response = client.get(path)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.content, payload)
            self.assertTrue(response.headers["content-type"].startswith("text/html"))
            self.assertEqual(response.headers["referrer-policy"], "no-referrer")
            directives = dict(part.strip().split(" ", 1) for part in response.headers[
                "content-security-policy"
            ].split(";"))
            self.assertEqual(directives["sandbox"], "allow-scripts")
            for directive in ("default-src", "base-uri", "form-action", "frame-ancestors"):
                self.assertEqual(directives[directive], "'none'")
            self.assertEqual(client.get(f"{path}?metric=support").status_code, 422)
            digest = hashlib.sha256(payload).hexdigest()
            self.assertEqual(client.get(path.replace(digest, "0" * 64)).status_code, 404)
