import ast
import hashlib
import json
import re
import tomllib
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Final, assert_never

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.storage import IntegrityError

STAGE_ROOTS: Final = {
    "source_catalog": ("dataset_assets_sources", "source_catalog"),
    "source_documents": ("dataset_assets_sources", "source_documents"),
    "document_adapters": ("dataset_assets_sources", "document_adapters"),
    "command_candidates": ("dataset_assets_sources", "command_candidates"),
    "filtered_commands": ("dataset_assets_sources", "filtered_commands"),
    "labeling_documentation": ("dataset_assets_sources", "labeling_documentation"),
    "proposed_annotations": ("dataset_assets_annotations", "proposed_annotations"),
    "merged_command_candidates": ("dataset_finalization", "merged_command_candidates"),
    "deduplicated_annotations": ("dataset_finalization", "deduplicated_annotations"),
    "category_eligible_annotations": ("dataset_finalization", "category_eligible_annotations"),
}
BOUNDARIES: Final = {
    ("dataset_engine", "cached_table"),
    ("dataset_finalization", "finalization_binding"),
    ("data_views", "DatasetDataService"),
    ("human_review", "HumanReviewError"),
    ("dataframe_io", "DataFrameIOManager"),
}
WHOLE_MODULES: Final = frozenset({"object_store", "storage"})
RUNTIME_FILES: Final = ("pyproject.toml", "uv.lock", "scripts/dataset_schema.py")
REVIEW_DEPENDENCIES: Final = frozenset({"psycopg", "psycopg-binary"})


def processing_runtime(value: JsonValue) -> JsonValue:
    match value:
        case dict():
            return {key: processing_runtime(item) for key, item in value.items()}
        case list():
            return [
                processing_runtime(item)
                for item in value
                if not (
                    isinstance(item, dict) and item.get("name") in REVIEW_DEPENDENCIES
                    or isinstance(item, str) and re.split(r"[\[<>=!~; ]", item, maxsplit=1)[0]
                    in REVIEW_DEPENDENCIES
                )
            ]
        case str() | bool() | int() | float() | None:
            return value
        case _:
            assert_never(value)


class ProcessingChanged(IntegrityError):
    pass


def processing_versions(stage: str, read_source: Callable[[str], bytes]) -> dict[str, str]:
    """Resolve stage code statically; retained sources are parsed, never imported."""
    pending = [STAGE_ROOTS[stage]]
    visited: set[tuple[str, str]] = set()
    versions: dict[str, str] = {}
    modules: dict[str, ast.Module] = {}
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
            modules[module] = ast.parse(read_source(filename), filename=filename)
        tree = modules[module]
        definitions: dict[str, ast.AST] = {}
        imports: dict[str, tuple[ast.AST, str | None, str]] = {}
        for node in tree.body:
            match node:
                case ast.FunctionDef() | ast.AsyncFunctionDef() | ast.ClassDef():
                    definitions[node.name] = node
                case ast.Assign() | ast.AnnAssign():
                    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    for target in targets:
                        for name in ast.walk(target):
                            if isinstance(name, ast.Name):
                                definitions[name.id] = node
                case ast.ImportFrom():
                    if node.level > 1:
                        raise IntegrityError(f"Relative import requires an explicit scope: {filename}")
                    imported = (
                        f"codebench_pipeline.{node.module}" if node.level == 1 else node.module
                    )
                    for alias in node.names:
                        imports[alias.asname or alias.name] = (node, imported, alias.name)
                case ast.Import():
                    for alias in node.names:
                        imports[alias.asname or alias.name.split(".")[0]] = (node, None, alias.name)
                case ast.stmt():
                    continue
        if symbol in imports:
            node, imported, original = imports[symbol]
            versions[f"{filename}:{symbol}"] = hashlib.sha256(
                repr((type(node).__name__, imported, original, symbol)).encode()
            ).hexdigest()
            if imported is not None and imported.startswith("codebench_pipeline."):
                pending.append((imported.removeprefix("codebench_pipeline."), original))
            elif original.startswith("codebench_pipeline"):
                raise IntegrityError(f"Module import requires an explicit scope: {filename}:{symbol}")
            continue
        selected = definitions.get(symbol)
        if selected is None and (module, symbol) == STAGE_ROOTS[stage]:
            selected = next(
                (node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == symbol),
                None,
            )
            if isinstance(selected, ast.FunctionDef):
                selected.decorator_list = []
        if selected is None:
            raise IntegrityError(f"Missing implementation symbol: {filename}:{symbol}")
        versions[f"{filename}:{symbol}"] = hashlib.sha256(
            ast.dump(selected, include_attributes=False).encode()
        ).hexdigest()
        for node in ast.walk(selected):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                if node.id in definitions or node.id in imports:
                    pending.append((module, node.id))
        initialization = [
            node for node in tree.body if not isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Import,
                       ast.ImportFrom, ast.Assign, ast.AnnAssign)
            )
        ]
        versions[f"{filename}:initialization"] = hashlib.sha256(
            ast.dump(ast.Module(body=initialization, type_ignores=[]), include_attributes=False).encode()
        ).hexdigest()
    for name in RUNTIME_FILES:
        payload = read_source(name)
        if name in {"pyproject.toml", "uv.lock"}:
            parsed = TypeAdapter(dict[str, JsonValue]).validate_python(tomllib.loads(payload.decode()))
            if name == "uv.lock":
                parsed.pop("revision", None)
            payload = json.dumps(processing_runtime(parsed), sort_keys=True, separators=(",", ":")).encode()
        versions[name] = hashlib.sha256(payload).hexdigest()
    engine = "codebench_pipeline/dataset_engine.py"
    build = next(
        node for node in ast.walk(ast.parse(read_source(engine)))
        if isinstance(node, ast.FunctionDef) and node.name == "build"
    )
    processing = next(node for node in build.body if isinstance(node, ast.With))
    invocation = next(node for node in processing.body if isinstance(node, ast.Try))
    versions[f"{engine}:frame_processing"] = hashlib.sha256(
        ast.dump(ast.Module(body=invocation.body, type_ignores=[]), include_attributes=False).encode()
    ).hexdigest()
    return versions


def stage_versions(stage: str) -> dict[str, str]:
    root = Path(__file__).parent.parent
    versions = processing_versions(stage, lambda name: (root / name).read_bytes())
    for name in ("dataset_engine.py", "stage_implementation.py"):
        filename = f"codebench_pipeline/{name}"
        versions[filename] = hashlib.sha256((root / filename).read_bytes()).hexdigest()
    return versions


def verify_processing(
    stage: str, implementation: Mapping[str, str], read_evidence: Callable[[str], bytes]
) -> None:
    def retained(name: str) -> bytes:
        if name not in implementation:
            raise IntegrityError(f"Legacy implementation receipt is missing: {name}")
        expected = implementation[name]
        payload = read_evidence(f"implementation/{expected}/{name}")
        if hashlib.sha256(payload).hexdigest() != expected:
            raise IntegrityError(f"Legacy implementation receipt changed: {name}")
        return payload

    root = Path(__file__).parent.parent
    if processing_versions(stage, retained) != processing_versions(
        stage, lambda name: (root / name).read_bytes()
    ):
        raise ProcessingChanged("Legacy processing implementation differs from current processing")
