"""Exercise the production IO manager against an owned, ephemeral MinIO server."""

import argparse
import json
import os
import secrets
import selectors
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol

import boto3
import dagster
import pandas as pd
from dagster import AssetKey, OutputContext, build_input_context
from pandas.testing import assert_frame_equal

from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.object_store import StorageConfig, configured_client


class ContextFactory(Protocol):
    def build_output_context(self, *, asset_key: AssetKey) -> OutputContext: ...


def output_context(factory: ContextFactory, key: AssetKey) -> OutputContext:
    return factory.build_output_context(asset_key=key)


def verify(binary: Path, report: Path) -> None:
    with TemporaryDirectory(prefix="minio-qa-") as temporary:
        credentials = {
            "MINIO_ROOT_USER": secrets.token_hex(12),
            "MINIO_ROOT_PASSWORD": secrets.token_hex(24),
        }
        environment = {**os.environ, **credentials}
        process = subprocess.Popen(
            [
                str(binary),
                "server",
                temporary,
                "--address",
                "127.0.0.1:19000",
                "--console-address",
                "127.0.0.1:19001",
            ],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            if process.stdout is None:
                raise RuntimeError("Missing MinIO startup stream")
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                startup = b""
                while b"API: http://127.0.0.1:19000" not in startup:
                    if not selector.select(timeout=30):
                        raise TimeoutError("MinIO did not signal API readiness")
                    chunk = os.read(process.stdout.fileno(), 65536)
                    if not chunk:
                        raise RuntimeError("MinIO exited before API readiness")
                    startup += chunk
            os.environ.update(credentials)
            settings = StorageConfig(
                backend="s3",
                bucket="dataset-qa",
                endpoint_url="http://127.0.0.1:19000",
                access_key_env="MINIO_ROOT_USER",
                secret_key_env="MINIO_ROOT_PASSWORD",
                sample_rows=2,
            )
            client = configured_client(boto3, settings)
            try:
                client.create_bucket(Bucket="dataset-qa")
                manager = DataFrameIOManager(storage=settings)
                frame = pd.DataFrame(
                    {"command": ["git status", "terraform destroy"], "accepted": [True, False]}
                )
                key = AssetKey(["datasets", "qa"])
                with output_context(dagster, key) as output:
                    manager.handle_output(output, frame)
                    with build_input_context(asset_key=key) as incoming:
                        restored = manager.load_input(incoming)
                    if not isinstance(restored, pd.DataFrame):
                        raise TypeError("IO manager returned a non-DataFrame")
                    assert_frame_equal(frame, restored)
                    if output.get_logged_metadata()["dagster/row_count"].value != 2:
                        raise AssertionError("Missing row-count preview")
                _, first_hit = manager.cached_frame("qa", lambda: frame)
                _, second_hit = manager.cached_frame("qa", lambda: frame.iloc[:0])
                if first_hit or not second_hit:
                    raise AssertionError("Cache did not preserve the original table")
                objects = client.list_objects_v2(Bucket="dataset-qa").get("Contents", [])
                parquet_count = 0
                for item in objects:
                    object_key = item.get("Key")
                    if object_key is None:
                        raise AssertionError("Object listing omitted its key")
                    response = client.get_object(Bucket="dataset-qa", Key=object_key)
                    try:
                        payload = response["Body"].read()
                    finally:
                        response["Body"].close()
                    if any(value.encode() in payload for value in credentials.values()):
                        raise AssertionError("Credentials leaked into object storage")
                    if object_key.endswith(".parquet"):
                        if not (payload.startswith(b"PAR1") and payload.endswith(b"PAR1")):
                            raise AssertionError("Stored table is not Parquet")
                        parquet_count += 1
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text(
                    json.dumps(
                        {
                            "passed": True,
                            "rows": len(frame),
                            "parquet_objects": parquet_count,
                            "cache_reused": second_hit,
                            "credentials_in_objects": False,
                        },
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                print(report.read_text(encoding="utf-8"))
            finally:
                client.close()
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
            if process.stdout is not None:
                process.stdout.close()
            for name in credentials:
                os.environ.pop(name, None)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    verify(Path(args.binary), Path(args.report))


if __name__ == "__main__":
    main()
