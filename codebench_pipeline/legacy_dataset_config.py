from pathlib import Path

from codebench_pipeline.business_config import (
    DEFAULT_BASE_PROMPT,
    LEGACY_BASE_PROMPT,
    load_business_rules,
    read_prompt,
)
from codebench_pipeline.dataset_config import (
    AdapterConfig,
    AnnotationConfig,
    CatalogConfig,
    DatasetBuildSpec,
    DatasetFileConfig,
    DatasetSeeds,
    SnapshotConfig,
)
from codebench_pipeline.dataset_documentation_config import DocumentationConfig
from codebench_pipeline.dataset_source_models import is_documentation_source
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.source_manifests import (
    manifest_documentation,
    manifest_sources,
    resolve_source_paths,
    validate_transformations,
)


def _legacy_catalog(seeds: DatasetSeeds, parent: Path, model: ModelConfig | None) -> CatalogConfig:
    return CatalogConfig(
        collection_revision=seeds.revision,
        documents=[resolve_source_paths(document, parent) for document in seeds.documents],
        manifest=str((parent / seeds.manifest).resolve()) if seeds.manifest is not None else None,
        model=model,
    )


def resolve_legacy_file(
    supplied: DatasetFileConfig, parent: Path, storage: StorageConfig
) -> DatasetBuildSpec:
    business_config = (
        str((parent / supplied.business_config).resolve()) if supplied.business_config is not None else None
    )
    rules = load_business_rules(Path(business_config)) if business_config is not None else supplied.rules
    assert rules is not None
    return resolve_business_inputs(
        DatasetBuildSpec(
            dataset_id=supplied.dataset_id,
            revision=supplied.revision,
            refresh_external_inputs=True,
            rules=rules,
            business_config=business_config,
            catalog=_legacy_catalog(supplied.seeds, parent, supplied.models.selector),
            snapshots=SnapshotConfig(maximum_bytes=supplied.snapshot_maximum_bytes),
            documentation_catalog=(
                _legacy_catalog(supplied.documentation_sources, parent, supplied.models.selector)
                if supplied.documentation_sources is not None
                else None
            ),
            documentation_snapshots=SnapshotConfig(maximum_bytes=supplied.snapshot_maximum_bytes),
            adapters=AdapterConfig(
                model=supplied.models.adapter,
                mode=supplied.adapters,
                sample_characters=supplied.adapter_sample_characters,
            ),
            extraction=supplied.extraction,
            filtering=supplied.filtering,
            documentation=supplied.documentation.model_copy(
                update={"manifest": str((parent / supplied.documentation.manifest).resolve())}
                if supplied.documentation.manifest is not None
                else {}
            ),
            annotation=AnnotationConfig(
                model=supplied.models.labeler,
                workers=supplied.labeling_workers,
                batch=supplied.labeling_batch,
            ),
            publication=supplied.publication,
            storage=storage,
        )
    )


def resolve_business_inputs(spec: DatasetBuildSpec) -> DatasetBuildSpec:
    spec = resolve_labeling_inputs(spec)
    updates: dict[str, CatalogConfig | SnapshotConfig | DocumentationConfig] = {}
    for catalog_name, snapshot_name in (
        ("catalog", "snapshots"),
        ("documentation_catalog", "documentation_snapshots"),
    ):
        catalog = spec.catalog if catalog_name == "catalog" else spec.documentation_catalog
        if catalog is None:
            continue
        documents = (
            manifest_sources(Path(catalog.manifest), spec.rules.category_id)
            if catalog.manifest is not None
            else catalog.documents
        )
        validate_transformations(documents)
        if catalog_name == "documentation_catalog" and any(
            not is_documentation_source(document.source_type, document.provenance_type)
            for document in documents
        ):
            raise ValueError("Documentation manifest cannot contain dataset corpora")
        updates[catalog_name] = catalog.model_copy(update={"documents": documents})
        snapshots = spec.snapshots if snapshot_name == "snapshots" else spec.documentation_snapshots
        updates[snapshot_name] = snapshots.model_copy(update={"documents": documents})
    documentation = spec.documentation
    if documentation is not None and documentation.manifest is not None:
        updates["documentation"] = documentation.model_copy(
            update={"snippets": manifest_documentation(Path(documentation.manifest), spec.rules.category_id)}
        )
    return spec.model_copy(update=updates)


def resolve_labeling_inputs(spec: DatasetBuildSpec) -> DatasetBuildSpec:
    """Refresh the policy for a registration with live inputs."""
    rules = (
        load_business_rules(Path(spec.business_config), spec.rules.category_id)
        if spec.business_config is not None
        else spec.rules
    )
    if spec.business_config is None and not rules.base_prompt:
        prompt = LEGACY_BASE_PROMPT if LEGACY_BASE_PROMPT.is_file() else DEFAULT_BASE_PROMPT
        rules = rules.model_copy(update={"base_prompt": read_prompt(prompt)})
    return spec.model_copy(update={"rules": rules})
