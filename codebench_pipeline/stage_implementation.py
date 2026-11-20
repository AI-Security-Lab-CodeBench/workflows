import ast
import hashlib
import json
import os
import re
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from fnmatch import fnmatchcase
from functools import lru_cache
from pathlib import Path
from typing import Final, assert_never

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.storage import IntegrityError, open_directory, open_regular_file

STAGE_ROOTS: Final = {
    "enumerate_corpus_files": ("declared_source_assets", "index_sources"),
    "capture_corpus_files": ("declared_source_assets", "capture_indexed_sources"),
    "command_candidates": ("declared_source_assets", "command_candidates"),
    "documentation/capture_documentation": ("declared_source_assets", "capture_documentation"),
    "documentation/command_candidates": ("declared_source_assets", "command_candidates"),
    "keenable/discover_use_cases": ("declared_source_assets", "discover_use_cases"),
    "keenable/capture_origin_pages": ("declared_source_assets", "capture_use_case_pages"),
    "keenable/command_candidates": ("declared_source_assets", "keenable_command_candidates"),
    "filtered_commands": ("dataset_assets_sources", "filtered_commands"),
    "labeling_attempts": ("dataset_assets_annotations", "labeling_attempts"),
    "proposed_annotations": ("dataset_assets_annotations", "proposed_annotations"),
    "labeling_failures": ("dataset_assets_annotations", "labeling_failures"),
    "merged_command_candidates": ("collection_merge", "merged_command_candidates"),
    "deduplicated_annotations": ("dataset_finalization", "deduplicated_annotations"),
    "category_eligible_annotations": ("dataset_finalization", "category_eligible_annotations"),
    "human_review_queue": ("dataset_finalization", "human_review_queue"),
}
BOUNDARIES: Final = {
    ("dataset_engine", "cached_table"),
    ("dataset_finalization", "finalization_binding"),
    ("review_preparation", "prepare_review_queue"),
    ("data_views", "DatasetDataService"),
    ("human_review", "HumanReviewError"),
    ("dataframe_io", "DataFrameIOManager"),
}
WHOLE_MODULES: Final = frozenset({"object_store", "storage"})
RUNTIME_FILES: Final = ("pyproject.toml", "uv.lock", "scripts/dataset_schema.py")
ENGINE_FILE: Final = "codebench_pipeline/dataset_engine.py"
REVIEW_DEPENDENCIES: Final = frozenset({"psycopg", "psycopg-binary"})
REVIEW_DECLARATIONS: Final = frozenset({"jsonschema"})


def _installed_relative(name: str) -> str:
    if any(part in {"", ".", ".."} for part in name.split("/")) or any(mark in name for mark in "\\:\0"):
        raise IntegrityError(f"Invalid installed source name: {name!r}")
    return name


@dataclass(frozen=True, slots=True)
class ImplementationIdentity:
    publication: dict[str, str]
    stages: dict[str, dict[str, str]]
    review_policy: dict[str, str]


@dataclass(frozen=True, slots=True)
class ImplementationSource:
    """Address the files in one installed processing checkout."""

    root: Path = Path(__file__).parent.parent

    def _address(self, name: str) -> Path:
        return self.root.joinpath(_installed_relative(name))

    def read(self, name: str) -> bytes:
        path = self._address(name)
        with open_directory(path.parent) as directory, open_regular_file(directory, path.name) as stream:
            return stream.read()

    def digest(self, name: str) -> str:
        return hashlib.sha256(self.read(name)).hexdigest()

    def publication_versions(self) -> dict[str, str]:
        return {
            name.rsplit("/", 1)[-1]: self.digest(name)
            for name in self.package_matches("guarded_publication*.py")
        }

    def package_matches(self, pattern: str) -> tuple[str, ...]:
        if "/" in pattern:
            raise IntegrityError(f"Installed package patterns must match a single component: {pattern!r}")
        self._address(pattern)
        package = "codebench_pipeline"
        with open_directory(self.root / package) as directory:
            return tuple(f"{package}/{name}" for name in os.listdir(directory) if fnmatchcase(name, pattern))

    def is_file(self, name: str) -> bool:
        path = self._address(name)
        try:
            with open_directory(path.parent) as directory, open_regular_file(directory, path.name):
                return True
        except FileNotFoundError:
            return False


def processing_runtime(value: JsonValue) -> JsonValue:
    match value:
        case dict():
            return {key: processing_runtime(item) for key, item in value.items()}
        case list():
            return [
                processing_runtime(item)
                for item in value
                if not (
                    isinstance(item, dict)
                    and item.get("name") in REVIEW_DEPENDENCIES
                    or isinstance(item, dict)
                    and item.get("name") in REVIEW_DECLARATIONS
                    and "version" not in item
                    or isinstance(item, str)
                    and re.split(r"[\[<>=!~; ]", item, maxsplit=1)[0]
                    in REVIEW_DEPENDENCIES | REVIEW_DECLARATIONS
                )
            ]
        case str() | bool() | int() | float() | None:
            return value
        case _:
            assert_never(value)


class ProcessingChanged(IntegrityError):
    pass


class ProcessingReceiptIncomplete(IntegrityError):
    pass


@dataclass(frozen=True, slots=True)
class _ModuleIndex:
    definitions: Mapping[str, ast.AST]
    imports: Mapping[str, tuple[ast.AST, str | None, str]]
    initialization: str
    nested_root: ast.FunctionDef | None


@lru_cache(maxsize=128)
def _module_index(filename: str, payload: bytes, root_symbol: str | None) -> _ModuleIndex:
    """Index exact source bytes without importing or executing the source."""
    tree = ast.parse(payload, filename=filename)
    definitions: dict[str, ast.AST] = {}
    imports: dict[str, tuple[ast.AST, str | None, str]] = {}
    initialization: list[ast.stmt] = []
    for node in tree.body:
        match node:
            case ast.FunctionDef() | ast.AsyncFunctionDef() | ast.ClassDef():
                definitions[node.name] = node
            case ast.TypeAlias(name=ast.Name(id=name)):
                definitions[name] = node
            case ast.Assign() | ast.AnnAssign():
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    for name in ast.walk(target):
                        if isinstance(name, ast.Name):
                            definitions[name.id] = node
            case ast.ImportFrom():
                if node.level > 1:
                    raise IntegrityError(f"Relative import requires an explicit scope: {filename}")
                imported = f"codebench_pipeline.{node.module}" if node.level == 1 else node.module
                for alias in node.names:
                    imports[alias.asname or alias.name] = (node, imported, alias.name)
            case ast.Import():
                for alias in node.names:
                    imports[alias.asname or alias.name.split(".")[0]] = (node, None, alias.name)
            case ast.stmt():
                initialization.append(node)
    nested_root = None
    if root_symbol is not None and root_symbol not in definitions and root_symbol not in imports:
        nested_root = next(
            (
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef) and node.name == root_symbol
            ),
            None,
        )
        if nested_root is not None:
            nested_root.decorator_list = []
    digest = _node_digest(ast.Module(body=initialization, type_ignores=[]))
    return _ModuleIndex(definitions, imports, digest, nested_root)


def _node_digest(node: ast.AST) -> str:
    return hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()


@lru_cache(maxsize=4096)
def _symbol_version(node: ast.AST) -> tuple[str, tuple[str, ...]]:
    return _node_digest(node), tuple(
        child.id
        for child in ast.walk(node)
        if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load)
    )


@lru_cache(maxsize=16)
def _runtime_version(name: str, payload: bytes) -> str:
    if name == ENGINE_FILE:
        build = next(
            node
            for node in ast.walk(ast.parse(payload))
            if isinstance(node, ast.FunctionDef) and node.name == "build"
        )
        processing = next(node for node in build.body if isinstance(node, ast.With))
        invocation = next(node for node in processing.body if isinstance(node, ast.Try))
        return _node_digest(ast.Module(body=invocation.body, type_ignores=[]))
    if name in {"pyproject.toml", "uv.lock"}:
        parsed = TypeAdapter(dict[str, JsonValue]).validate_python(tomllib.loads(payload.decode()))
        if name == "uv.lock":
            parsed.pop("revision", None)
        payload = json.dumps(processing_runtime(parsed), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def processing_versions(stage: str, read_source: Callable[[str], bytes]) -> dict[str, str]:
    """Read current or retained bytes on every call, reusing only their static analysis."""
    root = STAGE_ROOTS[stage]
    pending = [root]
    visited: set[tuple[str, str]] = set()
    versions: dict[str, str] = {}
    modules: dict[str, _ModuleIndex] = {}
    while pending:
        module, symbol = pending.pop()
        if (module, symbol) in visited or (module, symbol) in BOUNDARIES:
            continue
        visited.add((module, symbol))
        filename = f"codebench_pipeline/{module}.py"
        if module in WHOLE_MODULES:
            versions[filename] = hashlib.sha256(read_source(filename)).hexdigest()
            continue
        if module not in modules:
            modules[module] = _module_index(
                filename, read_source(filename), root[1] if module == root[0] else None
            )
        index = modules[module]
        if symbol in index.imports:
            node, imported, original = index.imports[symbol]
            versions[f"{filename}:{symbol}"] = hashlib.sha256(
                repr((type(node).__name__, imported, original, symbol)).encode()
            ).hexdigest()
            if imported is not None and imported.startswith("codebench_pipeline."):
                pending.append((imported.removeprefix("codebench_pipeline."), original))
            elif original.startswith("codebench_pipeline"):
                raise IntegrityError(f"Module import requires an explicit scope: {filename}:{symbol}")
            continue
        selected = index.definitions.get(symbol)
        if selected is None and (module, symbol) == root:
            selected = index.nested_root
        if selected is None:
            raise IntegrityError(f"Missing implementation symbol: {filename}:{symbol}")
        version, names = _symbol_version(selected)
        versions[f"{filename}:{symbol}"] = version
        pending.extend((module, name) for name in names if name in index.definitions or name in index.imports)
        versions[f"{filename}:initialization"] = index.initialization
    for name in RUNTIME_FILES:
        versions[name] = _runtime_version(name, read_source(name))
    versions[f"{ENGINE_FILE}:frame_processing"] = _runtime_version(ENGINE_FILE, read_source(ENGINE_FILE))
    return versions


def capture_implementation(source: ImplementationSource = ImplementationSource()) -> ImplementationIdentity:
    from codebench_pipeline.dataset_branches import DECLARED_SOURCE_STAGES, stage_branch
    from codebench_pipeline.dataset_dependencies import preparation_stages
    from codebench_pipeline.dataset_engine import review_policy_versions, scoped_code_versions

    return ImplementationIdentity(
        publication=source.publication_versions(),
        stages={
            stage: scoped_code_versions(
                stage,
                source,
                stage_branch(stage) if stage.rsplit("/", 1)[-1] in DECLARED_SOURCE_STAGES else None,
            )
            for stage in preparation_stages()
            if stage != "category_rules"
        },
        review_policy=review_policy_versions(source),
    )


def stage_versions(stage: str, source: ImplementationSource = ImplementationSource()) -> dict[str, str]:
    versions = processing_versions(stage, source.read)
    for name in ("dataset_engine.py", "stage_implementation.py"):
        filename = f"codebench_pipeline/{name}"
        versions[filename] = source.digest(filename)
    return versions


def verify_processing(
    stage: str, implementation: Mapping[str, str], read_evidence: Callable[[str], bytes]
) -> None:
    def retained(name: str) -> bytes:
        if name not in implementation:
            raise ProcessingReceiptIncomplete(f"Legacy implementation receipt is missing: {name}")
        expected = implementation[name]
        payload = read_evidence(f"implementation/{expected}/{name}")
        if hashlib.sha256(payload).hexdigest() != expected:
            raise IntegrityError(f"Legacy implementation receipt changed: {name}")
        return payload

    if processing_versions(stage, retained) != processing_versions(stage, ImplementationSource().read):
        raise ProcessingChanged("Legacy processing implementation differs from current processing")
