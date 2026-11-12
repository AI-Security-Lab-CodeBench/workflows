import json
from pathlib import Path

import pandera.pandas as pa
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_schemas import DocumentationTable, FilteredCommandsTable, validated_frame
from codebench_pipeline.dataset_documentation_config import DocumentationConfig
from codebench_pipeline.dataset_documentation_models import coverage, eligible_operations
from codebench_pipeline.models import SemanticSnippet
from codebench_pipeline.storage import IntegrityError


def _save(path: Path, value: JsonValue) -> None:
    with path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2) + "\n")


@pa.check_types
def collect_documentation(
    commands: DataFrame[FilteredCommandsTable],
    rules: CategoryRules,
    config: DocumentationConfig,
    evidence: Path,
) -> DataFrame[DocumentationTable]:
    """Validate supplied semantic evidence and report operation coverage without discovery."""
    evidence.mkdir(parents=True, exist_ok=True)
    if (evidence / "documentation-receipt.json").exists():
        raise IntegrityError("Documentation evidence already exists")
    operations = eligible_operations(commands)
    receipt: dict[str, JsonValue] = {
        "mode": config.mode,
        "revision": config.revision,
        "maximum_bytes": config.maximum_bytes,
        "eligible_operations": len(operations),
        "use_scope": "semantic_reference_only",
        "status": "failed",
    }
    snippets: list[SemanticSnippet] = []
    try:
        if config.manifest is not None:
            (evidence / "documentation-manifest.json").write_bytes(Path(config.manifest).read_bytes())
        snippets = [seed.to_semantic_snippet() for seed in config.snippets]
        if sum(len(snippet.snippet.encode("utf-8")) for snippet in snippets) > config.maximum_bytes:
            raise IntegrityError("Documentation snippets exceed configured acquisition size")
        if config.mode == "provided_snippets":
            receipt["provided"] = [snippet.model_dump(mode="json") for snippet in snippets]
        table = validated_frame([snippet.model_dump(mode="json") for snippet in snippets], DocumentationTable)
        receipt["status"] = "complete"
        return table
    except (ValueError, RuntimeError, OSError) as error:
        receipt["error_type"] = type(error).__name__
        receipt["error"] = str(error)
        raise
    finally:
        coverage_rows = coverage(operations, snippets)
        if receipt["status"] == "complete" and any(row["status"] == "gap" for row in coverage_rows):
            receipt["status"] = "partial"
        receipt["coverage"] = TypeAdapter(JsonValue).validate_python(coverage_rows)
        receipt["rows"] = len(snippets)
        _save(evidence / "documentation-receipt.json", receipt)
