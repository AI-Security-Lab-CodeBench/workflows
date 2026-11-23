from io import BytesIO
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
import pyarrow
from pandas.testing import assert_frame_equal
from pyarrow.parquet import ParquetFile

from codebench_pipeline.artifact_schemas import table_artifact_schema
from codebench_pipeline.dataframe_io import (
    AssetReference,
    DataFrameIOManager,
    StorageConfig,
    frame_bytes,
    read_frame,
)
from codebench_pipeline.object_store import sha256


def attempts() -> pd.DataFrame:
    frame = pd.DataFrame(
        {"status": pd.Categorical([1] * 1597, categories=[2, 1], ordered=True)},
        index=pd.Index(range(1597), name="candidate_id"),
    )
    frame.attrs = {
        "origins": {"source": "synthetic"},
        "branch_metadata": {"ref": "fixture"},
        "labeling_support": {"evidence": "é" * 8000},
    }
    return frame


def scaled_parquet_open(source: pyarrow.NativeFile, *, thrift_string_size_limit: int = 65_536) -> ParquetFile:
    return ParquetFile(source, thrift_string_size_limit=thrift_string_size_limit)


class ParquetMetadataTests(TestCase):
    def test_read_frame_respects_array_manager_option(self) -> None:
        frame = pd.DataFrame(
            {"quantity": pd.array([3, None], dtype="Int64"), "note": ["ready", None]},
            index=pd.Index(["a", "b"], name="candidate_id"),
        )
        frame.columns.name = "fields"
        frame.attrs = {"origins": {"source": "synthetic", "missing": None}}
        payload = frame_bytes(frame)

        with (
            self.assertWarnsRegex(FutureWarning, "data_manager option is deprecated"),
            pd.option_context("mode.data_manager", "array"),
        ):
            expected = pd.read_parquet(BytesIO(payload), engine="pyarrow")
            restored = read_frame(payload)
            assert_frame_equal(restored, expected)
            self.assertEqual(restored.attrs, frame.attrs)
            self.assertIs(type(restored._mgr), type(expected._mgr))

    def test_large_attrs_remain_readable_with_an_ordinary_thrift_limit(self) -> None:
        expected = attempts()

        with patch("codebench_pipeline.dataframe_io._ATTRS_CHUNK_BYTES", 16_384):
            payload = frame_bytes(expected)

        with ParquetFile(BytesIO(payload), thrift_string_size_limit=65_536) as source:
            self.assertEqual(source.metadata.num_rows, len(expected))
        with patch("pyarrow.parquet.ParquetFile", side_effect=scaled_parquet_open):
            restored = read_frame(payload)
        assert_frame_equal(restored, expected)
        self.assertEqual(restored.attrs, expected.attrs)

    def test_oversize_legacy_footer_recovers_from_the_original_verified_bytes(self) -> None:
        expected = attempts()
        payload = frame_bytes(expected)
        with self.assertRaisesRegex(OSError, "TProtocolException: Exceeded size limit"):
            pd.read_parquet(BytesIO(payload), engine="pyarrow", thrift_string_size_limit=65_536)

        with TemporaryDirectory() as root:
            manager = DataFrameIOManager(storage=StorageConfig(local_root=root))
            reference = AssetReference(format="parquet", digest=sha256(payload))
            manager.object_store().write(f"objects/{reference.digest}.parquet", payload)
            with patch("pyarrow.parquet.ParquetFile", side_effect=scaled_parquet_open):
                restored = manager.read_version(reference)
                schema = table_artifact_schema(payload, ("status",), None)
                with (
                    patch("codebench_pipeline.dataframe_io._MAX_LEGACY_FOOTER_BYTES", 65_536),
                    self.assertRaisesRegex(OSError, "TProtocolException: Exceeded size limit"),
                ):
                    manager.read_version(reference)

        assert isinstance(restored, pd.DataFrame)
        assert_frame_equal(restored, expected)
        self.assertEqual(restored.attrs, expected.attrs)
        self.assertEqual(schema.source, "storage")
