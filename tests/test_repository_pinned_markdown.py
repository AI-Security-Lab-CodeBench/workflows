from __future__ import annotations

import hashlib
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from test_repository import RepositoryFixture, repository_config_fixture

from codebench_pipeline.repository_acquisition import catalog_origin
from codebench_pipeline.repository_etl import authorize_adapter, repository_rows, source_span
from codebench_pipeline.repository_receipts import RepositoryRun
from codebench_pipeline.source_transform_contract import TRANSFORM_OPTIONS, TransformContext
from codebench_pipeline.storage import IntegrityError


class PinnedMarkdownTests(unittest.TestCase):
    def test_catalog_admits_only_declared_markdown_with_verified_rights(self) -> None:
        initial = repository_config_fixture()
        origin = next(item for item in initial.documentation if item.origin_id == "podman-docs")
        fixture = RepositoryFixture(origin)
        known = "manual/ps.md"
        fixture.file(known, b"```sh\npodman ps\n```\n")
        fixture.file("manual/unreviewed.md", b"```sh\npodman ps --all\n```\n")
        fixture.file("other/NOTICE", b"Unrelated licensed material")
        fixture.file("external/NOTICE", b"Unresolved terms for this subtree")
        fixture.file("external/reference.md", b"Reference")
        declaration = fixture.declaration().model_copy(
            update={"included_paths": (known, "external/reference.md")}
        )

        with (
            TemporaryDirectory() as temporary,
            patch(
                "codebench_pipeline.repository_acquisition.fetch_repository_bytes",
                side_effect=[fixture.response(), fixture.bodies["LICENSE"]],
            ),
        ):
            run = RepositoryRun(Path(temporary), "fixture", "run")
            selected, evidence = catalog_origin(declaration, run, initial.maximum_file_bytes)

        self.assertEqual(
            {source.path: source.exclusion for source in selected},
            {known: "", "external/reference.md": "unresolved_local_notice"},
        )
        self.assertEqual(
            evidence["rights"],
            [
                {
                    "path": "LICENSE",
                    "url": f"https://raw.githubusercontent.com/{origin.repository}/{origin.commit}/LICENSE",
                    "sha256": hashlib.sha256(fixture.bodies["LICENSE"]).hexdigest(),
                    "git_blob_sha1": fixture.entries["LICENSE"][2],
                    "license_id": "Apache-2.0",
                    "attribution": origin.attribution,
                    "use": "retain_and_quote_attributed_examples",
                }
            ],
        )

    def test_catalog_blocks_an_absent_or_nonregular_declared_file(self) -> None:
        initial = repository_config_fixture()
        origin = next(item for item in initial.documentation if item.origin_id == "podman-docs")
        for mode in (None, "120000"):
            with self.subTest(mode=mode), TemporaryDirectory() as temporary:
                fixture = RepositoryFixture(origin)
                fixture.file("manual/ps.md", b"Selected")
                if mode:
                    fixture.file("manual/missing.md", b"ps.md", mode=mode)
                declaration = fixture.declaration().model_copy(
                    update={"included_paths": ("manual/ps.md", "manual/missing.md")}
                )
                run = RepositoryRun(Path(temporary), "fixture", "run")
                with (
                    patch(
                        "codebench_pipeline.repository_acquisition.fetch_repository_bytes",
                        return_value=fixture.response(),
                    ),
                    self.assertRaisesRegex(IntegrityError, "absent or nonregular.*manual/missing.md"),
                ):
                    catalog_origin(declaration, run, initial.maximum_file_bytes)

    def test_literal_usage_keeps_exact_spans_and_rejects_templates(self) -> None:
        initial = repository_config_fixture()
        origin = next(item for item in initial.documentation if item.origin_id == "pulumi-docs")
        text = "\r\n".join(
            [
                "---",
                "title: Use `pulumi stack select bogus`",
                "---",
                "Café: use `pulumi stack ls`.",
                "An identifier: `pulumi stack history`.",
                "Use `pulumi stack select <stack>`.",
                "Use `pulumi stack [flags]`.",
                "<!-- Use `pulumi stack select ignored`. -->",
                "```yaml",
                "run: use `pulumi stack select ignored`",
                "```",
                "```bash",
                "$ pulumi preview \\",
                "    --stack 'dev team' \\",
                "    --diff",
                "pulumi stack select \\",
                "    ${STACK}",
                "```",
                "```bash",
                "pulumi stack select \\",
                "$ pulumi stack select swallowed",
                "pulumi-cli stack ls",
                "pulumi stack output $(echo value)",
                "```",
                "```bash",
                "printf '%s\\n' $(echo",
                "pulumi stack select nested",
                ")",
                "```",
                "```bash",
                "printf '%s\\n' 'first line",
                "pulumi stack ls",
                "last line'",
                "pulumi stack ls",
                'printf "%s\\n" "first line \\',
                "$ pulumi stack select swallowed",
                'last line"',
                "printf '%s\\n' \\",
                "pulumi stack select continued-argument",
                "pulumi stack history",
                "printf '%s\\n' \\\\",
                "pulumi stack --show-name",
                '# It is safe to leave an unmatched " in a shell comment.',
                "pulumi stack output",
                "```",
                "```console",
                "$ printf '%s\\n' 'first line",
                "$ pulumi stack select swallowed",
                "last line'",
                "$ pulumi stack select dev",
                "```",
                "```",
                "pulumi preview [flags]",
                "pulumi stack history",
                "$ pulumi stack output --json",
                "```",
                "```sh",
                "pulumi stack select unclosed",
            ]
        )
        options = TRANSFORM_OPTIONS.validate_json(origin.transform.options_json)
        context = TransformContext("synthetic", "https://example.test/doc.md", "0" * 64, options)
        adapter = authorize_adapter(origin.transform)

        actual = tuple(repository_rows(text, adapter, context))

        expected = (
            ("pulumi stack ls", "pulumi stack ls"),
            (
                "pulumi preview --stack 'dev team' --diff",
                "pulumi preview \\\r\n    --stack 'dev team' \\\r\n    --diff",
            ),
            ("pulumi stack ls", "pulumi stack ls"),
            ("pulumi stack history", "pulumi stack history"),
            ("pulumi stack --show-name", "pulumi stack --show-name"),
            ("pulumi stack output", "pulumi stack output"),
            ("pulumi stack select dev", "pulumi stack select dev"),
            ("pulumi stack output --json", "pulumi stack output --json"),
        )
        self.assertEqual(tuple(row.command for row in actual), tuple(command for command, _ in expected))
        source = text.encode()
        for row, (_, original) in zip(actual, expected, strict=True):
            span = source_span(text, row.source_position)
            self.assertEqual(source[span.byte_start : span.byte_end].decode(), original)
        closing_line = b"last line'\r\n"
        self.assertEqual(
            source_span(text, actual[2].source_position).byte_start,
            source.index(closing_line) + len(closing_line),
        )
        self.assertEqual(tuple(repository_rows("", adapter, context)), ())
