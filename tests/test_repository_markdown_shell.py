import hashlib
import shutil
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from codebench_pipeline import repository_etl
from codebench_pipeline.repository_config import load_repository_config
from codebench_pipeline.repository_etl import authorize_adapter, repository_rows, source_span
from codebench_pipeline.source_transform_contract import (
    TRANSFORM_OPTIONS,
    CanonicalSourceRow,
    SourceTransformConfig,
    TransformContext,
)
from codebench_pipeline.storage import IntegrityError

ROOT = Path(__file__).resolve().parents[1]
DECLARATIONS = ROOT / "business-configs/categories/cat3/collection/repositories.json"


def declared_transform(origin_id: str) -> SourceTransformConfig:
    config = load_repository_config(DECLARATIONS)
    return next(
        origin.transform
        for origin in (*config.dataset, *config.documentation)
        if origin.origin_id == origin_id
    )


def extract(text: str, transform: SourceTransformConfig) -> tuple[CanonicalSourceRow, ...]:
    adapter = authorize_adapter(transform)
    options = TRANSFORM_OPTIONS.validate_json(transform.options_json)
    context = TransformContext(
        "synthetic", "https://example.test/README.md", hashlib.sha256(text.encode()).hexdigest(), options
    )
    return tuple(repository_rows(text, adapter, context))


class RepositoryMarkdownShellTests(unittest.TestCase):
    def test_single_line_origins_keep_their_utility_lists_and_exact_byte_spans(self) -> None:
        text = (
            "Café\r\n"
            "```console\r\n"
            "  $ helm status demo\r\n"
            "terraform plan --output-example\r\n"
            "```\r\n"
            "```bash\r\n"
            "terraform plan -var 'name=two words'\r\n"
            "gcloud storage ls gs://example-bucket\r\n"
            "aws s3 ls\r\n"
            "helmfile list\r\n"
            "terraform plan $(printf example)\r\n"
            "gcloud storage ls \\\r\n"
            "  gs://example-bucket\r\n"
            "```\r\n"
            "```sh\r\n"
            "helm list\r\n"
        )
        cases = (
            ("kubeflow-manifests", ["helm status demo"]),
            (
                "online-boutique",
                ["terraform plan -var 'name=two words'", "gcloud storage ls gs://example-bucket"],
            ),
            ("terraform-eks-examples", ["terraform plan -var 'name=two words'", "aws s3 ls"]),
        )
        for origin_id, expected in cases:
            with self.subTest(origin=origin_id):
                rows = extract(text, declared_transform(origin_id))

                self.assertEqual([row.command for row in rows], expected)
                for row in rows:
                    span = source_span(text, row.source_position)
                    self.assertEqual(text.encode()[span.byte_start : span.byte_end], row.command.encode())

    def test_kubernetes_origin_keeps_fence_only_selection_and_transformations(self) -> None:
        text = "Run `kubectl get nodes`.\n```bash\nhelm list\n  $ kubectl get pods\n```\n"
        transform = declared_transform("kubernetes-examples")

        (row,) = extract(text, transform)

        self.assertEqual(row.command, "kubectl get pods")
        self.assertEqual(row.context, text)
        self.assertEqual(
            row.transformations,
            [
                f"trusted_python:{transform.sha256}:transform",
                "selected shell example from a Markdown fence",
                "removed source indentation",
                "removed shell prompt",
            ],
        )

    def test_pulumi_origins_keep_inline_opt_in_and_original_continuation_bytes(self) -> None:
        physical = "pulumi stack \\\r\n    ls"
        text = (
            "---\r\nRun `pulumi preview`.\r\n---\r\n"
            "Café. Run `pulumi version`. `pulumi about` is a command name.\r\n"
            "<!--\r\nRun `pulumi login`.\r\n-->\r\n"
            f"```console\r\npulumi stack\r\n  $ {physical}\r\n```\r\n"
            "Run `pulumi stack select <name>`.\r\n"
        )
        cases = (
            ("pulumi-docs", ["pulumi version", "pulumi stack ls"], ["pulumi version", physical]),
            ("pulumi-examples", ["pulumi stack ls"], [physical]),
        )
        for origin_id, commands, original_spans in cases:
            with self.subTest(origin=origin_id):
                rows = extract(text, declared_transform(origin_id))

                self.assertEqual([row.command for row in rows], commands)
                for row, original in zip(rows, original_spans, strict=True):
                    span = source_span(text, row.source_position)
                    self.assertEqual(text.encode()[span.byte_start : span.byte_end], original.encode())
                self.assertEqual(rows[-1].transformations[-1], "joined literal shell continuation lines")

    def test_podman_origin_ignores_quoted_lines_and_stops_at_ambiguous_expansion(self) -> None:
        text = (
            "```bash\n"
            "printf 'sample\n"
            "podman info\n"
            "'\n"
            "podman ps\n"
            'printf "${example}"\n'
            "podman images\n"
            "```\n"
            "```bash\npodman version\n```\n"
        )

        rows = extract(text, declared_transform("podman-docs"))

        self.assertEqual([row.command for row in rows], ["podman ps", "podman version"])

    def test_repository_allowlist_rejects_a_tampered_markdown_declaration_hash(self) -> None:
        transform = declared_transform("kubernetes-examples")
        tampered = transform.model_copy(update={"sha256": "0" * 64})

        with self.assertRaisesRegex(IntegrityError, "absent from the deployment allowlist"):
            authorize_adapter(tampered)

    def test_repository_adapter_rejects_tampered_markdown_bytes_at_the_approved_path(self) -> None:
        transform = declared_transform("kubernetes-examples")
        with TemporaryDirectory() as directory:
            deployment = Path(directory).resolve()
            parser = deployment / transform.module_path
            parser.parent.mkdir(parents=True)
            parser.write_bytes((ROOT / transform.module_path).read_bytes() + b"\n")
            shutil.copyfile(ROOT / "uv.lock", deployment / "uv.lock")

            with (
                patch.object(repository_etl, "_DEPLOYMENT_ROOT", deployment),
                self.assertRaisesRegex(IntegrityError, "code differs from its manifest hash"),
            ):
                authorize_adapter(transform)
