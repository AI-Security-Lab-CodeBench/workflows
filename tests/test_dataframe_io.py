from __future__ import annotations

import tempfile
import unittest
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from unittest.mock import patch
from urllib.parse import unquote, urlsplit

import pandas as pd
import pandera.pandas as pa
from botocore.exceptions import ClientError
from dagster import (
    DagsterInvalidInvocationError,
    StaticPartitionsDefinition,
    TableMetadataValue,
    build_input_context,
    build_output_context,
)
from pandas.testing import assert_frame_equal
from pydantic import JsonValue, ValidationError
from test_dataset_rules import make_rules

from codebench_pipeline.dataframe_io import DataFrameIOManager, StorageConfig
from codebench_pipeline.object_store import sha256
from codebench_pipeline.storage import IntegrityError


@dataclass
class S3State:
    objects: dict[str, tuple[bytes, str]] = field(default_factory=dict)
    reject_writes: bool = False


@contextmanager
def s3_fixture() -> Generator[tuple[StorageConfig, S3State]]:
    state = S3State()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: str | int) -> None:
            pass

        def reply(self, status: int, data: bytes, digest: str = "") -> None:
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("ETag", f'"{sha256(data)}"')
            self.send_header("x-amz-meta-sha256", digest)
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:
            result = state.objects.get(self.path)
            if result is None:
                self.reply(404, b"<Error><Code>NoSuchKey</Code></Error>")
            else:
                self.reply(200, *result)

        def do_PUT(self) -> None:
            data = self.rfile.read(int(self.headers["Content-Length"]))
            previous = state.objects.get(self.path)
            if previous is None:
                permitted = self.headers.get("If-None-Match") == "*"
            else:
                permitted = self.headers.get("If-Match") == f'"{sha256(previous[0])}"'
            if not permitted or state.reject_writes:
                self.reply(412, b"<Error><Code>PreconditionFailed</Code></Error>")
                return
            state.objects[self.path] = data, self.headers.get("x-amz-meta-sha256", "")
            self.reply(200, b"")

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        config = StorageConfig(
            backend="s3",
            bucket="test-bucket",
            endpoint_url=f"http://127.0.0.1:{server.server_port}",
            access_key_env="IO_TEST_ACCESS",
            secret_key_env="IO_TEST_SECRET",
        )
        try:
            with patch.dict(
                "os.environ", {"IO_TEST_ACCESS": "fixture-access", "IO_TEST_SECRET": "fixture-secret"}
            ):
                yield config, state
        finally:
            server.shutdown()
            thread.join(timeout=5)
            if thread.is_alive():
                raise RuntimeError("S3 fixture did not shut down")


class DataFrameIOTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = StorageConfig(local_root=temporary.name, sample_rows=2)
        self.manager = DataFrameIOManager(storage=self.config)

    def test_parquet_preserves_nullable_dtypes_indexes_and_empty_frames(self) -> None:
        frame = pd.DataFrame(
            {
                "number": pd.Series([1, None], dtype="Int64"),
                "flag": pd.Series([True, None], dtype="boolean"),
                "text": pd.Series(["example", None], dtype="string"),
                "kind": pd.Categorical(["one", "two"]),
                "time": pd.to_datetime(["2026-01-01", None], utc=True),
            }
        )
        frame.index = pd.Index(["a", "b"], name="identity")
        for number, expected in enumerate((frame, frame.head(0), pd.DataFrame())):
            with self.subTest(number=number):
                context = build_output_context(asset_key=["table", str(number)])
                self.manager.handle_output(context, expected)
                loaded = DataFrameIOManager(storage=self.config).load_input(
                    build_input_context(asset_key=["table", str(number)])
                )
                assert isinstance(loaded, pd.DataFrame)
                assert_frame_equal(loaded, expected)

    def test_native_metadata_limits_rows_and_cells_without_truncating_storage(self) -> None:
        frame = pa.DataFrameSchema({"text": pa.Column(str)}).validate(
            pd.DataFrame({"text": ["x" * 1200, "second", "third"]})
        )
        context = build_output_context(asset_key="preview")
        self.manager.handle_output(context, frame)
        metadata = context.get_logged_metadata()
        sample = metadata["sample"]
        assert isinstance(sample, TableMetadataValue)
        self.assertEqual([row.data["text"] for row in sample.records], ["x" * 500, "second"])
        self.assertEqual(metadata["dagster/row_count"].value, 3)
        self.assertEqual(
            [(column.name, column.type) for column in sample.schema.columns],
            [("text", str(frame.dtypes["text"]))],
        )
        loaded = self.manager.load_input(build_input_context(asset_key="preview"))
        assert isinstance(loaded, pd.DataFrame)
        assert_frame_equal(loaded, frame)

    def test_asset_paths_and_partitions_have_distinct_restart_stable_identity(self) -> None:
        partitions = StaticPartitionsDefinition(["a/b", "b"])
        for asset, partition, number in ((["a", "b"], "a/b", 1), (["a/b"], "a/b", 2), (["a", "b"], "b", 3)):
            context = build_output_context(
                asset_key=asset,
                partition_key=partition,
                asset_partitions_def=partitions,
            )
            self.manager.handle_output(context, pd.DataFrame({"number": [number]}))
        loaded = DataFrameIOManager(storage=self.config).load_input(
            build_input_context(
                asset_key=["a", "b"],
                partition_key="a/b",
                asset_partitions_def=partitions,
            )
        )
        assert isinstance(loaded, pd.DataFrame)
        assert_frame_equal(loaded, pd.DataFrame({"number": [1]}))

        updated = build_output_context(
            asset_key=["a", "b"], partition_key="b", asset_partitions_def=partitions
        )
        self.manager.handle_output(updated, pd.DataFrame({"number": [4]}))
        reloaded = self.manager.load_input(
            build_input_context(
                asset_key=["a", "b"],
                partition_key="b",
                asset_partitions_def=partitions,
            )
        )
        assert isinstance(reloaded, pd.DataFrame)
        assert_frame_equal(reloaded, pd.DataFrame({"number": [4]}))
        self.assertEqual(len(list((self.root / "datasets/objects").glob("*/current"))), 4)

    def test_category_rules_roundtrip_returns_typed_taxonomy(self) -> None:
        rules = make_rules()
        self.manager.handle_output(build_output_context(asset_key="rules"), rules)
        self.assertEqual(self.manager.load_input(build_input_context(asset_key="rules")), rules)

    def test_cache_hit_does_not_compute_and_corruption_does_not_repair(self) -> None:
        expected = pd.DataFrame({"id": [7]})
        first, hit = self.manager.cached_frame("input-v1/schema-v2", lambda: expected)
        self.assertFalse(hit)
        assert_frame_equal(first, expected)

        def unexpected_compute() -> pd.DataFrame:
            self.fail("Cache hit or corruption must not compute")

        loaded, hit = DataFrameIOManager(storage=self.config).cached_frame(
            "input-v1/schema-v2", unexpected_compute
        )
        self.assertTrue(hit)
        assert_frame_equal(loaded, expected)
        next((self.root / "datasets/cache").glob("*/*/data")).write_bytes(b"tampered")
        with self.assertRaises(IntegrityError):
            self.manager.cached_frame("input-v1/schema-v2", unexpected_compute)

    def test_evidence_is_immutable_and_missing_hash_metadata_fails_closed(self) -> None:
        self.assertFalse(self.manager.exists("receipt"))
        uri = self.manager.write_evidence("receipt", b"original")
        self.assertEqual(self.manager.read_evidence("receipt"), b"original")
        self.assertTrue(self.manager.exists("receipt"))
        with self.assertRaises(IntegrityError):
            self.manager.write_evidence("receipt", b"replacement")
        (Path(unquote(urlsplit(uri).path)) / "current").unlink()
        with self.assertRaises(IntegrityError):
            self.manager.write_evidence("receipt", b"original")

    def test_configuration_is_strict_frozen_and_does_not_print_secrets(self) -> None:
        cases: list[dict[str, JsonValue]] = [
            {"backend": "other"},
            {"backend": "s3"},
            {"extra": True},
            {"sample_rows": -1},
            {"sample_rows": 101},
            {"sample_rows": "2"},
            {"prefix": "../escape"},
            {"local_root": "s3://bucket"},
            {"bucket": "not-local"},
            {"backend": "s3", "bucket": "s3://bucket"},
            {"backend": "s3", "bucket": "bucket", "access_key_env": "NAME"},
            {"backend": "s3", "bucket": "bucket", "access_key_env": "name=value", "secret_key_env": "SECRET"},
            {"backend": "s3", "bucket": "bucket", "endpoint_url": "http://name:private-secret@localhost"},
            {"backend": "s3", "bucket": "bucket", "endpoint_url": "http://localhost/?token=private-secret"},
        ]
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValidationError) as caught:
                StorageConfig.model_validate(case)
            self.assertNotIn("private-secret", str(caught.exception))
        with self.assertRaises((ValidationError, DagsterInvalidInvocationError)):
            self.config.sample_rows = 9
        config = StorageConfig(
            backend="s3",
            bucket="bucket",
            access_key_env="IO_MISSING_ACCESS",
            secret_key_env="IO_MISSING_SECRET",
        )
        with patch.dict("os.environ", {}, clear=True), self.assertRaises(ValueError):
            DataFrameIOManager(storage=config).read_evidence("missing")

    def test_s3_http_roundtrips_conditional_replacement_and_tamper_detection(self) -> None:
        with s3_fixture() as (config, state):
            manager = DataFrameIOManager(storage=config)
            for number in (1, 2):
                manager.handle_output(
                    build_output_context(asset_key="remote"), pd.DataFrame({"id": [number]})
                )
            loaded = manager.load_input(build_input_context(asset_key="remote"))
            assert isinstance(loaded, pd.DataFrame)
            assert_frame_equal(loaded, pd.DataFrame({"id": [2]}))
            expected = pd.DataFrame({"flag": pd.Series([], dtype="boolean")})
            manager.cached_frame("empty-v1", lambda: expected)

            def unexpected_compute() -> pd.DataFrame:
                self.fail("Remote cache hit must not compute")

            cached, hit = DataFrameIOManager(storage=config).cached_frame("empty-v1", unexpected_compute)
            self.assertTrue(hit)
            assert_frame_equal(cached, expected)
            uri = manager.write_evidence("receipt", b"original")
            self.assertTrue(uri.startswith("s3://test-bucket/datasets/"))
            self.assertEqual(manager.read_evidence("receipt"), b"original")
            with self.assertRaises(IntegrityError):
                manager.write_evidence("receipt", b"changed")
            key = "/test-bucket/" + uri.split("test-bucket/", 1)[1]
            state.objects[key] = b"tampered", sha256(b"original")
            with self.assertRaises(IntegrityError):
                manager.read_evidence("receipt")
            state.reject_writes = True
            with self.assertRaises(ClientError):
                manager.write_evidence("new-receipt", b"racing writer")
            self.assertFalse(manager.exists("new-receipt"))
            self.assertEqual(state.objects[key][0], b"tampered")


if __name__ == "__main__":
    unittest.main()
