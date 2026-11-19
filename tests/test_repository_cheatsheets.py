import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from pydantic import JsonValue, TypeAdapter
from test_repository import RepositoryFixture, repository_config_fixture

from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.repository import (
    RepositoryRun,
    repository_adapters,
    repository_candidates,
    repository_catalog,
    repository_documents,
)
from codebench_pipeline.repository_etl import authorize_adapter
from codebench_pipeline.repository_receipts import read_receipt
from codebench_pipeline.source_transform_contract import SourceTransformConfig, TransformContext
from codebench_pipeline.source_transforms import source_rows, transformed_rows
from codebench_pipeline.storage import IntegrityError


class CheatsheetFrontMatterTests(TestCase):
    def test_reviewed_parser_rejects_a_stale_runtime_lock(self) -> None:
        transform = repository_config_fixture().dataset[0].transform
        home = Path(self.enterContext(TemporaryDirectory()))
        parser = home / transform.module_path
        parser.parent.mkdir(parents=True)
        parser.write_bytes(Path(transform.module_path).read_bytes())
        dependency = Path("transforms/cheatsheets.py")
        (home / dependency.parent).mkdir()
        (home / dependency).write_bytes(dependency.read_bytes())
        (home / "uv.lock").write_bytes(Path("uv.lock").read_bytes() + b"\n")

        with (
            patch("codebench_pipeline.repository_etl._DEPLOYMENT_ROOT", home),
            self.assertRaisesRegex(IntegrityError, "dependency lock differs from the reviewed deployment"),
        ):
            authorize_adapter(transform)

    def test_parser_skips_only_valid_initial_metadata_and_preserves_original_command_spans(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        run = RepositoryRun(home, "cat3-fixture", "front-matter")
        shipped = repository_config_fixture()
        fixture = RepositoryFixture(shipped.dataset[0])
        sheet = (
            "---\r\n"
            "title: Caf\u00e9 examples\r\n"
            "tags: [ compression ]\r\n"
            "---\r\n"
            "# Archives\r\n"
            "7z l 'sample archive.7z'\r\n"
            "---\r\n"
            "7z t 'sample archive.7z'\r\n"
        ).encode()
        plain = b"---\nprintf '%s\\n' literal\n---\n"
        fixture.file("7z", sheet)
        fixture.file("plain", plain)
        config = shipped.model_copy(update={"dataset": (fixture.declaration(),)})
        prefix = f"https://raw.githubusercontent.com/{fixture.origin.repository}/{fixture.origin.commit}/"
        responses = {
            f"https://api.github.com/repos/{fixture.origin.repository}/git/trees/{fixture.origin.commit}?recursive=1": fixture.response(),
            **{prefix + path: body for path, body in fixture.bodies.items()},
        }

        def wire(url: str, _limit: int) -> bytes:
            return responses[url]

        with patch("codebench_pipeline.repository_acquisition.fetch_repository_bytes", side_effect=wire):
            catalog = repository_catalog(config, "dataset", run)
            documents = repository_documents(catalog, config, run)
        adapters = repository_adapters(documents, config, run)

        rows = repository_candidates(documents, adapters, config, run)

        receipt = read_receipt(rows, "command_candidates", run)
        evidence = TypeAdapter(dict[str, dict[str, JsonValue]]).validate_python(
            receipt.observations["candidates"]
        )
        rows_by_file: dict[str, list[tuple[JsonValue, JsonValue]]] = {"7z": [], "plain": []}
        for row in frame_records(rows):
            candidate = evidence[str(row["candidate_id"])]
            filename = str(candidate["source_url"]).rsplit("/", 1)[1]
            span = TypeAdapter(dict[str, int]).validate_python(candidate["source_span"])
            original = sheet if filename == "7z" else plain
            self.assertEqual(
                original[span["byte_start"] : span["byte_end"]].rstrip(b"\r\n"), str(row["command"]).encode()
            )
            if filename == "7z":
                self.assertNotIn("tags:", str(row["context"]))
                self.assertNotIn("title:", str(row["context"]))
            rows_by_file[filename].append((row["command"], row["source_position"]))
        self.assertEqual(
            rows_by_file["7z"],
            [
                ("7z l 'sample archive.7z'", "document; lines 6:6"),
                ("---", "document; lines 7:7"),
                ("7z t 'sample archive.7z'", "document; lines 8:8"),
            ],
        )
        self.assertEqual(
            [command for command, _ in rows_by_file["plain"]], ["---", "printf '%s\\n' literal", "---"]
        )

    def test_prepared_manifest_still_executes_the_original_parser_pin(self) -> None:
        manifest = Path("business-configs/manifests/cat3/datasets.json")
        declared = SourceTransformConfig.model_validate(
            json.loads(manifest.read_bytes())["sources"][0]["transform"]
        )
        pinned = declared.model_copy(
            update={"module_path": str((manifest.parent / declared.module_path).resolve())}
        )
        context = TransformContext("fixture", "https://example.org/source", "a" * 64, {})

        rows = transformed_rows(
            pinned, source_rows("---\ntags: [ compression ]\n---\n7z l file.7z\n", "text"), context
        )

        self.assertEqual(
            [row.command for row in rows], ["---", "tags: [ compression ]", "---", "7z l file.7z"]
        )
