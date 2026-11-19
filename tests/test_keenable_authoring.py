import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from pydantic import JsonValue, ValidationError
from test_keenable import BODY, DECLARATION, DOCKER, PEER, PLAN, VALIDATE, result

from codebench_pipeline.keenable import KeenableCollection, KeenableError, load_declaration
from codebench_pipeline.keenable_policy import (
    RESPONSE_BYTES,
    CallBudget,
    Case,
    CurrentKeenableDeclaration,
    CurrentSourcePermission,
    PageNotice,
    PinnedLicenseFile,
    StewardGrant,
    digest,
    parse_declaration,
)
from codebench_pipeline.keenable_transport import DirectResponse

EXPIRY = datetime(2099, 1, 1, tzinfo=UTC)
PREFIX = PLAN.rsplit("/", 1)[0] + "/"


def grant(
    *, exact: tuple[str, ...] = (), discovery: bool = False, cases: tuple[Case, ...] = ("plan",)
) -> CurrentSourcePermission:
    return CurrentSourcePermission(
        origin_id="fixture",
        url_prefix=PREFIX,
        exact_urls=exact,
        cases=cases,
        media_type="text/html",
        valid_until=EXPIRY,
        attribution="Synthetic source fixture",
        permitted_use="fetch_retain_quote",
        allow_discovery=discovery,
        rights=StewardGrant(
            kind="steward_decision", steward_decision="fixture-rights", declared_evidence_sha256="1" * 64
        ),
    )


class CurrentKeenableAuthoringTests(unittest.TestCase):
    def test_shipped_authority_and_budget_match_the_migrated_generation(self) -> None:
        current = load_declaration(DECLARATION)
        assert isinstance(current, CurrentKeenableDeclaration)
        self.assertEqual(current.generation, "cat3-keenable-administrative-fixed-v2")
        self.assertEqual(current.search_cases, ("workloads", "containers"))
        self.assertEqual(current.budget.total_bytes, 25 * RESPONSE_BYTES)
        self.assertEqual(len(current.raw_licenses), 3)
        self.assertEqual(sum(len(source.exact_urls) for source in current.sources), 14)
        self.assertEqual(
            [source.origin_id for source in current.sources if source.allow_discovery],
            ["kubernetes-website-kubectl", "docker-engine-resource-docs"],
        )

    def test_current_authoring_rejects_historical_fields_and_ambiguous_rights(self) -> None:
        current = load_declaration(DECLARATION)
        misplaced_fields: tuple[dict[str, JsonValue], ...] = (
            {"generation_review": "review-id"},
            {"cases": ["plan"]},
            {"permissions": []},
            {"budget": {"search_calls": 2, "origin_calls": 23, "total_bytes": 25 * RESPONSE_BYTES}},
        )
        for misplaced in misplaced_fields:
            with self.subTest(misplaced=misplaced), self.assertRaisesRegex(ValidationError, "Extra inputs"):
                parse_declaration(json.dumps({**current.model_dump(mode="json"), **misplaced}))
        values = grant(exact=(PLAN,)).model_dump(mode="json")
        values["rights"]["evidence_sha256"] = "2" * 64
        with self.assertRaisesRegex(ValidationError, "evidence_sha256"):
            CurrentSourcePermission.model_validate_json(json.dumps(values))

    def test_exact_seed_cannot_inherit_its_case_from_list_order(self) -> None:
        with self.assertRaisesRegex(ValidationError, "single case"):
            grant(exact=(PLAN,), cases=("plan", "workloads"))

    def test_discovery_opt_out_keeps_seed_capture_but_rejects_search_hits(self) -> None:
        discovered_source = grant(discovery=True).model_copy(
            update={"origin_id": "other", "url_prefix": DOCKER, "cases": ("plan",)}
        )
        exact = grant(exact=(PLAN,))
        declaration = CurrentKeenableDeclaration(
            schema_version="3",
            dataset_id="dataset",
            generation="fixture",
            budget=CallBudget(origin_calls=2, search_calls=1),
            sources=(exact, discovered_source),
        )
        with (
            tempfile.TemporaryDirectory() as root,
            patch(
                "codebench_pipeline.keenable.request_direct",
                side_effect=(
                    result(PLAN, VALIDATE, DOCKER),
                    DirectResponse(BODY, PEER),
                    DirectResponse(BODY, PEER),
                ),
            ) as wire,
        ):
            branch = KeenableCollection(Path(root), declaration)
            catalog = branch.discover()
            lock = branch.capture(catalog)
            self.assertEqual(
                [(item.lead.url, item.lead.origin) for item in catalog.selected],
                [(PLAN, "declared"), (DOCKER, "search")],
            )
            self.assertEqual(sum(lead.excluded == "outside_case" for lead in catalog.leads), 2)
            replay = KeenableCollection(Path(root), parse_declaration(declaration.model_dump_json()))
            self.assertEqual(replay.discover().model_dump_json(), catalog.model_dump_json())
            self.assertEqual(replay.capture(replay.discover()).model_dump_json(), lock.model_dump_json())
            self.assertEqual(replay.read_bodies(lock), {PLAN: BODY, DOCKER: BODY})
            self.assertEqual(wire.call_count, 3)

    def test_disabled_prefix_without_seeds_schedules_no_search(self) -> None:
        declaration = CurrentKeenableDeclaration(
            schema_version="3",
            dataset_id="dataset",
            generation="fixture",
            budget=CallBudget(origin_calls=1, search_calls=0),
            sources=(grant(),),
        )
        with tempfile.TemporaryDirectory() as root:
            branch = KeenableCollection(Path(root), declaration)
            with patch("codebench_pipeline.keenable.request_direct") as wire:
                catalog = branch.discover()
            self.assertEqual((catalog.searches, catalog.selected), ((), ()))
            wire.assert_not_called()

    def test_rights_reservations_verify_pinned_bytes_without_fetching_declared_metadata(self) -> None:
        repository = "https://raw.githubusercontent.com/owner/repo/" + "6" * 40 + "/"
        document = repository + "docs/reference.md"
        evidence = b"Known test license text."
        pinned = CurrentSourcePermission(
            origin_id="raw",
            url_prefix=repository + "docs/",
            exact_urls=(document,),
            cases=("plan",),
            media_type="text/plain",
            valid_until=EXPIRY,
            attribution="Synthetic raw source",
            permitted_use="fetch_retain_quote",
            allow_discovery=False,
            rights=PinnedLicenseFile(
                kind="pinned_license_file",
                identifier="Apache-2.0",
                evidence_url=repository + "LICENSE",
                license_url="https://www.apache.org/licenses/LICENSE-2.0",
                evidence_sha256=digest(evidence),
            ),
        )
        notice = PageNotice(
            kind="page_notice",
            identifier="CC-BY-4.0",
            evidence_url="https://example.org/evidence",
            license_url="https://creativecommons.org/licenses/by/4.0/",
            declared_evidence_sha256="3" * 64,
            required_page_notice="creative commons",
        )
        values = grant(exact=(PLAN,)).model_dump(mode="json")
        values["rights"] = notice.model_dump(mode="json")
        html = CurrentSourcePermission.model_validate_json(json.dumps(values))
        declaration = CurrentKeenableDeclaration(
            schema_version="3",
            dataset_id="dataset",
            generation="fixture",
            budget=CallBudget(origin_calls=3, search_calls=0),
            sources=(pinned, html),
        )
        html_body = b"<pre>Creative Commons</pre>"
        with (
            tempfile.TemporaryDirectory() as root,
            patch(
                "codebench_pipeline.keenable.request_direct",
                side_effect=(
                    DirectResponse(evidence, PEER),
                    DirectResponse(html_body, PEER),
                    DirectResponse(BODY, PEER),
                ),
            ) as wire,
        ):
            branch = KeenableCollection(Path(root), declaration)
            lock = branch.capture(branch.discover())
            self.assertEqual(branch.read_bodies(lock), {PLAN: html_body, document: BODY})
            self.assertEqual([call.target for call in lock.rights_calls], [repository + "LICENSE"])
            self.assertEqual(
                {page.url: page.rights_evidence_sha256 for page in lock.pages},
                {
                    PLAN: "3" * 64,
                    document: digest(evidence),
                },
            )
            self.assertEqual(wire.call_count, 3)

    def test_steward_grant_expires_at_the_declared_instant(self) -> None:
        declaration = CurrentKeenableDeclaration(
            schema_version="3",
            dataset_id="dataset",
            generation="fixture",
            sources=(grant(exact=(PLAN,)),),
        )
        with self.assertRaisesRegex(KeenableError, "authorization_expired"):
            declaration.check_current(EXPIRY)
