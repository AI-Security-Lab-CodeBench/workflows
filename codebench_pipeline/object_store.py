from __future__ import annotations

import hashlib
import os
import re
from contextlib import ExitStack, closing, suppress
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal, Protocol, assert_never
from urllib.parse import urlsplit
from uuid import uuid4

import boto3
from botocore.config import Config as ClientConfig
from botocore.exceptions import ClientError
from pydantic import ConfigDict, Field, model_validator

from codebench_pipeline.storage import (
    IntegrityError,
    open_child_directory,
    open_directory,
    open_regular_file,
    write_new_file,
)
from codebench_pipeline.typed_config import TypedConfig

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class S3Factory(Protocol):
    def client(
        self,
        service_name: Literal["s3"],
        *,
        endpoint_url: str | None,
        region_name: str,
        aws_access_key_id: str | None,
        aws_secret_access_key: str | None,
        config: ClientConfig,
    ) -> S3Client: ...


def configured_client(factory: S3Factory, settings: "StorageConfig") -> S3Client:
    access = secret = None
    if settings.access_key_env is not None and settings.secret_key_env is not None:
        access = os.environ.get(settings.access_key_env)
        secret = os.environ.get(settings.secret_key_env)
        if not access or not secret:
            raise ValueError("Configured credential environment variables must be populated")
    return factory.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        region_name=settings.region,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        config=ClientConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


class StorageConfig(TypedConfig):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, hide_input_in_errors=True)

    backend: Literal["local", "s3"] = "local"
    local_root: str = ".codebench/dataframes"
    bucket: str | None = None
    endpoint_url: str | None = Field(default=None, repr=False)
    region: str = "us-east-1"
    access_key_env: str | None = None
    secret_key_env: str | None = None
    prefix: str = "datasets"
    sample_rows: int = Field(default=10, ge=0, le=100)

    @model_validator(mode="after")
    def valid_storage(self) -> StorageConfig:
        if not self.local_root.strip() or "://" in self.local_root:
            raise ValueError("local_root must be a filesystem directory")
        safe_key(self.prefix)
        if not re.fullmatch(r"[a-z0-9-]+", self.region):
            raise ValueError("region must be a region identifier")
        for name in (self.access_key_env, self.secret_key_env):
            if name is not None and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
                raise ValueError("Credentials must reference environment variable names")
        if (self.access_key_env is None) != (self.secret_key_env is None):
            raise ValueError("Both credential environment variable names are required together")
        if self.endpoint_url is not None:
            url = urlsplit(self.endpoint_url)
            if (
                url.scheme not in {"http", "https"}
                or not url.hostname
                or url.username is not None
                or url.password is not None
                or url.query
                or url.fragment
                or url.path not in {"", "/"}
            ):
                raise ValueError("endpoint_url must be an HTTP origin without credentials")
            url.port
        match self.backend:
            case "s3":
                if self.bucket is None or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", self.bucket):
                    raise ValueError("S3 requires a bucket name, not a URL")
            case "local":
                if any(
                    value is not None
                    for value in (
                        self.bucket,
                        self.endpoint_url,
                        self.access_key_env,
                        self.secret_key_env,
                    )
                ):
                    raise ValueError("S3 settings require backend=s3")
            case _:
                assert_never(self.backend)
        return self


def safe_key(key: str) -> str:
    if (
        not key
        or "\\" in key
        or ":" in key
        or "\x00" in key
        or any(part in {"", ".", ".."} for part in key.split("/"))
        or PurePosixPath(key).is_absolute()
    ):
        raise ValueError("Storage keys must be nonempty relative POSIX paths")
    return key


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_local(directory: int, key: str) -> bytes:
    try:
        with open_regular_file(directory, "current") as pointer:
            digest = pointer.read(65)
    except FileNotFoundError:
        if any(name != ".lock" for name in os.listdir(directory)):
            raise IntegrityError(f"Missing completion metadata: {key}") from None
        raise FileNotFoundError(key) from None
    if not re.fullmatch(rb"[a-f0-9]{64}", digest):
        raise IntegrityError(f"Invalid completion metadata: {key}")
    try:
        with open_child_directory(directory, digest.decode("ascii")) as version:
            with open_regular_file(version, "data") as stream:
                data = stream.read()
    except FileNotFoundError as error:
        raise IntegrityError(f"Incomplete object: {key}") from error
    if sha256(data).encode("ascii") != digest:
        raise IntegrityError(f"Object content mismatch: {key}")
    return data


def _retain_local_version(directory: int, key: str, data: bytes, digest: str) -> None:
    with ExitStack() as stack:
        try:
            version = stack.enter_context(open_child_directory(directory, digest))
        except FileNotFoundError:
            staging = f".write-{uuid4().hex}"
            os.mkdir(staging, mode=0o700, dir_fd=directory)
            with open_child_directory(directory, staging) as target:
                installed = False
                try:
                    write_new_file(target, "data", data, mode=0o666)
                    os.rename(staging, digest, src_dir_fd=directory, dst_dir_fd=directory)
                    installed = True
                finally:
                    if not installed:
                        with suppress(FileNotFoundError):
                            os.unlink("data", dir_fd=target)
                        os.rmdir(staging, dir_fd=directory)
        else:
            try:
                with open_regular_file(version, "data") as stream:
                    if stream.read(len(data) + 1) != data:
                        raise IntegrityError(f"Object content mismatch: {key}")
            except FileNotFoundError as error:
                raise IntegrityError(f"Incomplete object: {key}") from error


class ObjectStore:
    """Hash-checked objects; replacement is reserved for asset references."""

    def __init__(self, config: StorageConfig, *, client: S3Client | None = None) -> None:
        self.config = config
        self._s3_client = client

    @staticmethod
    def from_environment(config: StorageConfig) -> ObjectStore:
        client = configured_client(boto3, config) if config.backend == "s3" else None
        return ObjectStore(config, client=client)

    def _client(self) -> S3Client:
        if self._s3_client is None:
            raise IntegrityError("S3 client is not bound to the object store")
        return self._s3_client

    def _bucket(self) -> str:
        if self.config.bucket is None:
            raise ValueError("S3 requires a bucket")
        return self.config.bucket

    def _key(self, key: str) -> str:
        return safe_key(f"{self.config.prefix}/{key}")

    def _directory(self, key: str) -> Path:
        return Path(self.config.local_root).absolute().joinpath(self._key(key))

    def _read_s3(self, client: S3Client, key: str) -> tuple[bytes, str] | None:
        try:
            response = client.get_object(Bucket=self._bucket(), Key=self._key(key))
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
                return None
            raise
        with closing(response["Body"]) as body:
            data = body.read()
        if response.get("Metadata", {}).get("sha256") != sha256(data):
            raise IntegrityError(f"Object content mismatch: {key}")
        return data, response["ETag"]

    def read(self, key: str) -> bytes:
        match self.config.backend:
            case "local":
                try:
                    with open_directory(self._directory(key)) as directory:
                        return _read_local(directory, key)
                except FileNotFoundError:
                    raise FileNotFoundError(key) from None
            case "s3":
                with closing(self._client()) as client:
                    result = self._read_s3(client, key)
                if result is None:
                    raise FileNotFoundError(key)
                return result[0]
            case _:
                assert_never(self.config.backend)

    def write(self, key: str, data: bytes, *, replace: bool = False) -> str:
        digest = sha256(data)
        match self.config.backend:
            case "local":
                path = self._directory(key)
                with open_directory(path, create=True) as directory:
                    try:
                        write_new_file(directory, ".lock", str(os.getpid()).encode("ascii"))
                    except FileExistsError as error:
                        raise IntegrityError(
                            f"Another run owns {path / '.lock'}; inspect it before retrying"
                        ) from error
                    try:
                        try:
                            previous = _read_local(directory, key)
                        except FileNotFoundError:
                            previous = None
                        if previous is not None:
                            if previous == data:
                                return self.uri(key)
                            if not replace:
                                raise IntegrityError(f"Immutable object already exists: {key}")
                        _retain_local_version(directory, key, data, digest)
                        pointer = f".commit-{uuid4().hex}"
                        write_new_file(directory, pointer, digest.encode("ascii"), mode=0o666)
                        try:
                            os.replace(pointer, "current", src_dir_fd=directory, dst_dir_fd=directory)
                        finally:
                            with suppress(FileNotFoundError):
                                os.unlink(pointer, dir_fd=directory)
                    finally:
                        os.unlink(".lock", dir_fd=directory)
            case "s3":
                with closing(self._client()) as client:
                    previous = self._read_s3(client, key)
                    if previous is not None:
                        if previous[0] == data:
                            return self.uri(key)
                        if not replace:
                            raise IntegrityError(f"Immutable object already exists: {key}")
                        client.put_object(
                            Bucket=self._bucket(),
                            Key=self._key(key),
                            Body=data,
                            Metadata={"sha256": digest},
                            IfMatch=previous[1],
                        )
                    else:
                        client.put_object(
                            Bucket=self._bucket(),
                            Key=self._key(key),
                            Body=data,
                            Metadata={"sha256": digest},
                            IfNoneMatch="*",
                        )
            case _:
                assert_never(self.config.backend)
        return self.uri(key)

    def uri(self, key: str) -> str:
        match self.config.backend:
            case "local":
                return self._directory(key).as_uri()
            case "s3":
                return f"s3://{self._bucket()}/{self._key(key)}"
            case _:
                assert_never(self.config.backend)
