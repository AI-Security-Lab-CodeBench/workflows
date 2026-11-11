"""Build the pinned Dagster UI extension without modifying upstream source."""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import shutil
import subprocess
import tarfile
import urllib.request
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Final, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

COMMIT: Final = "7dca97fabebf771d4ac0515d674e50e2a742446f"
VERSION: Final = "1.13.29"
YARN_VERSION: Final = "4.12.0"
REGISTRY: Final = "https://registry.npmjs.org"
CONFIGS: Final = (
    "package.json",
    "next.config.js",
    "postcss.config.js",
    "tsconfig.json",
    "tooling/package.json",
    "tooling/yarn.lock",
    "tooling/.yarnrc.yml",
)


class BuildError(RuntimeError):
    """A build input or ownership contract was violated."""


class Distribution(BaseModel):
    model_config = ConfigDict(frozen=True)
    tarball: Literal["https://registry.npmjs.org/@yarnpkg/cli-dist/-/cli-dist-4.12.0.tgz"]
    integrity: str = Field(pattern=r"^sha512-[A-Za-z0-9+/]+={0,2}$")


class YarnPackage(BaseModel):
    model_config = ConfigDict(frozen=True)
    version: Literal["4.12.0"]
    dist: Distribution


class Workspace(BaseModel):
    model_config = ConfigDict(frozen=True)
    packageManager: Literal["yarn@4.12.0"]
    workspaces: dict[str, list[str]]


def run(argv: Sequence[str], cwd: Path, env: Mapping[str, str]) -> str:
    return subprocess.run(
        list(argv),
        cwd=cwd,
        env=env,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()


def execute(argv: Sequence[str], cwd: Path, env: Mapping[str, str]) -> None:
    subprocess.run(list(argv), cwd=cwd, env=env, check=True)


def verify_source(source: Path, env: Mapping[str, str]) -> None:
    if run(["git", "rev-parse", "HEAD"], source, env) != COMMIT:
        raise BuildError(f"Expected Dagster {VERSION} at {COMMIT}: {source}")
    if run(["git", "status", "--porcelain", "--untracked-files=no"], source, env):
        raise BuildError(f"Tracked upstream source is not pristine: {source}")
    package = Workspace.model_validate_json((source / "js_modules/package.json").read_bytes())
    if not package.workspaces.get("packages"):
        raise BuildError("Pinned upstream must declare the Yarn 4.12.0 workspace")


def links(source: Path) -> dict[str, Path]:
    return {
        ".upstream": source,
        "node_modules": source / "js_modules/node_modules",
        "public": source / "js_modules/app-oss/public",
    }


def check_links(ui: Path, source: Path) -> None:
    for name, target in links(source).items():
        link = ui / name
        if link.is_symlink():
            if link.resolve() != target.resolve():
                raise BuildError(f"Unexpected link, left untouched: {link}")
        elif link.exists():
            raise BuildError(f"Expected absent path or matching symlink: {link}")


def bootstrap_yarn(ui: Path, root: Path) -> Path:
    existing = ui / "tooling/node_modules/@yarnpkg/cli-dist/bin/yarn.js"
    if existing.is_file():
        return existing.resolve()
    with urllib.request.urlopen(f"{REGISTRY}/@yarnpkg%2fcli-dist/{YARN_VERSION}", timeout=60) as response:
        package = YarnPackage.model_validate_json(response.read())
    with urllib.request.urlopen(package.dist.tarball, timeout=60) as response:
        archive = response.read()
    integrity = "sha512-" + base64.b64encode(hashlib.sha512(archive).digest()).decode("ascii")
    if integrity != package.dist.integrity:
        raise BuildError("Official Yarn distribution integrity mismatch")
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as bundle:
        members = [member for member in bundle.getmembers() if member.name == "package/bin/yarn.js"]
        if len(members) != 1 or not members[0].isfile():
            raise BuildError("Yarn archive must contain exactly one regular package/bin/yarn.js")
        payload = bundle.extractfile(members[0])
        if payload is None:
            raise BuildError("Yarn archive entry is unreadable")
        with payload:
            target = root / "yarn.js"
            target.write_bytes(payload.read())
    return target


def input_files(ui: Path) -> list[Path]:
    files = [ui / name for name in CONFIGS]
    for name in ("src", "tests"):
        files.extend(path for path in (ui / name).rglob("*") if path.is_file())
    if any(path.is_symlink() for path in files):
        raise BuildError("Project source/config inputs must be regular files, not links")
    return sorted(files)


def hashes(paths: Sequence[Path], base: Path) -> dict[str, str]:
    return {
        path.relative_to(base).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(paths)
    }


def tree_hashes(root: Path) -> dict[str, str]:
    paths = sorted(root.rglob("*"))
    if any(path.is_symlink() for path in paths):
        raise BuildError(f"Generated bundle must not contain symlinks: {root}")
    return hashes([path for path in paths if path.is_file()], root)


def publish(build: Path, ui: Path) -> None:
    destination = ui / "build"
    if destination.is_symlink() or (destination.exists() and not destination.is_dir()):
        raise BuildError(f"Refusing unrelated build path: {destination}")
    with TemporaryDirectory(prefix=".codebench-package-", dir=ui) as temporary:
        ready = Path(temporary) / "build"
        previous = Path(temporary) / "previous"
        shutil.copytree(build, ready)
        if tree_hashes(ready) != tree_hashes(build):
            raise BuildError("Bundle copy failed SHA256 verification")
        if destination.exists():
            if not (destination / "index.html").is_file() or not (destination / "_next").is_dir():
                raise BuildError("Existing build is not a static Next export; move it aside explicitly")
            inventory = tree_hashes(destination)
            backup = ui / f"build.backup-{uuid4().hex}"
            shutil.copytree(destination, backup)
            if tree_hashes(backup) != inventory or tree_hashes(destination) != inventory:
                raise BuildError("Build backup changed during publication")
            destination.rename(previous)
        try:
            ready.rename(destination)
        finally:
            if previous.exists() and not destination.exists():
                previous.rename(destination)


@contextmanager
def build_lock(ui: Path) -> Generator[None]:
    lock = ui / ".codebench-build.lock"
    lock.mkdir()
    try:
        yield
    finally:
        lock.rmdir()


def build(source_path: Path | None = None) -> Path:
    ui = Path(__file__).resolve().parents[1] / "dagster_ui"
    env = {key: value for key, value in os.environ.items() if not key.startswith(("YARN_", "GIT_"))}
    for key in ("NEXT_PUBLIC_BASE_PATH", "NEXT_PUBLIC_URL"):
        env.pop(key, None)
    env.update(
        NEXT_TELEMETRY_DISABLED="1",
        YARN_ENABLE_TELEMETRY="0",
        YARN_IGNORE_PATH="1",
        YARN_ENABLE_GLOBAL_CACHE="false",
        YARN_ENABLE_IMMUTABLE_INSTALLS="true",
        YARN_NM_MODE="hardlinks-local",
        YARN_CHECKSUM_BEHAVIOR="throw",
        YARN_NPM_REGISTRY_SERVER=REGISTRY,
        NODE_ENV="production",
    )
    with build_lock(ui), TemporaryDirectory(prefix="codebench-ui-") as temporary:
        root = Path(temporary)
        if int(run(["node", "--version"], root, env).lstrip("v").split(".")[0]) < 20:
            raise BuildError("Node >=20 is required")
        source = source_path.expanduser().resolve() if source_path is not None else root / "dagster"
        if source_path is None:
            run(
                [
                    "git",
                    "clone",
                    "--depth=1",
                    "--filter=blob:none",
                    "--sparse",
                    f"--branch={VERSION}",
                    "https://github.com/dagster-io/dagster.git",
                    str(source),
                ],
                root,
                env,
            )
            run(
                ["git", "sparse-checkout", "set", "js_modules", "python_modules/dagster-webserver"],
                source,
                env,
            )
        verify_source(source, env)
        try:
            check_links(ui, source)
            paths = input_files(ui)
            inputs = hashes([*paths, Path(__file__).resolve()], ui.parent)
            stage = root / "extension"
            for path in paths:
                target = stage / path.relative_to(ui)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, target)
            yarn = bootstrap_yarn(ui, root)
            if run(["node", str(yarn), "--version"], stage, env) != YARN_VERSION:
                raise BuildError("Yarn CLI must be exactly 4.12.0")
            execute(["node", str(yarn), "install", "--immutable"], stage / "tooling", env)
            execute(["node", str(yarn), "install", "--immutable"], source / "js_modules", env)
            for name, target in links(source).items():
                (stage / name).symlink_to(target, target_is_directory=True)
            execute(["node", str(source / "js_modules/node_modules/next/dist/bin/next"), "build"], stage, env)
            output = stage / "build"
            shutil.copyfile(source / "js_modules/app-oss/csp-header.txt", output / "csp-header.txt")
            vendor = source / "python_modules/dagster-webserver/dagster_webserver/graphiql"
            shutil.copytree(vendor, output / "vendor/graphiql")
            (output / "licenses").mkdir(exist_ok=True)
            shutil.copyfile(source / "LICENSE", output / "licenses/dagster-LICENSE")
            shutil.copyfile(stage / "tooling/node_modules/zod/LICENSE", output / "licenses/zod-LICENSE")
            if hashes([*input_files(ui), Path(__file__).resolve()], ui.parent) != inputs:
                raise BuildError("Project source/config changed during build; refusing publication")
            verify_source(source, env)
            manifest = {
                "dagster_version": VERSION,
                "upstream_commit": COMMIT,
                "source_files": inputs,
                "source_digest": hashlib.sha256(
                    json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest(),
            }
            (output / "codebench-build.json").write_text(
                json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
            )
            publish(output, ui)
        finally:
            verify_source(source, env)
    return ui / "build"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, help="Existing pristine checkout at the pinned Dagster commit")
    args = parser.parse_args()
    print(build(args.source))


if __name__ == "__main__":
    main()
