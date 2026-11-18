import os
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, TypeAdapter

from codebench_pipeline.business_config import load_business_rules
from codebench_pipeline.dataset_config import (
    CatalogConfig,
    DatasetBuildSpec,
    DocumentationConfig,
    SnapshotConfig,
)
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.legacy_dataset_config import resolve_business_inputs, resolve_labeling_inputs
from codebench_pipeline.models import Identifier
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.storage import IntegrityError, exclusive, fingerprint


@lru_cache(maxsize=4)
def _validated_spec(payload: bytes) -> DatasetBuildSpec:
    return DatasetBuildSpec.model_validate_json(payload)


class StoredLocation(BaseModel):
    dataset_id: Identifier
    storage: StorageConfig


class DatasetRegistry:
    def __init__(self, home: Path) -> None:
        self.root = home.resolve() / "dataset-specifications"

    def register(self, spec: DatasetBuildSpec, *, previous: DatasetBuildSpec | None = None) -> str:
        directory = self.root / spec.dataset_id
        version = fingerprint(spec.model_dump_json())
        directory.mkdir(parents=True, exist_ok=True)
        with (
            HumanReviewStore(self.root.parent).transaction(dataset_id=spec.dataset_id) as tx,
            exclusive(directory / ".lock"),
        ):
            if previous is not None:
                current = self.get_snapshot(spec.dataset_id)
                if current != previous:
                    raise IntegrityError("Dataset changed concurrently while binding business configuration")
            target = directory / f"{version}.json"
            if not target.exists():
                target.write_text(spec.model_dump_json(indent=2), encoding="utf-8")
            temporary = directory / ".current"
            pointer = directory / "current"
            if not pointer.exists() or pointer.read_text(encoding="ascii") != version:
                tx.invalidate(spec.dataset_id, "Registered dataset configuration changed")
            temporary.write_text(version, encoding="ascii")
            os.replace(temporary, pointer)
        return spec.dataset_id

    def _stored_spec(self, dataset_id: str) -> DatasetBuildSpec:
        key = TypeAdapter(Identifier).validate_python(dataset_id)
        directory = self.root / key
        version = (directory / "current").read_text(encoding="ascii")
        spec = _validated_spec((directory / f"{version}.json").read_bytes())
        if spec.dataset_id != key:
            raise IntegrityError("Dataset registry identity mismatch")
        return spec

    def get_snapshot(self, dataset_id: str) -> DatasetBuildSpec:
        """Read persisted configuration without resolving any live business files."""
        return self._stored_spec(dataset_id).model_copy(deep=True)

    def storage(self, dataset_id: str) -> StorageConfig:
        """Read the persisted location without loading category policy or source inventories."""
        key = TypeAdapter(Identifier).validate_python(dataset_id)
        directory = self.root / key
        version = (directory / "current").read_text(encoding="ascii")
        location = StoredLocation.model_validate_json((directory / f"{version}.json").read_bytes())
        if location.dataset_id != key:
            raise IntegrityError("Dataset registry identity mismatch")
        return location.storage

    def get(self, dataset_id: str) -> DatasetBuildSpec:
        spec = self._stored_spec(dataset_id)
        if spec.bundle_version is not None:
            return spec.model_copy(deep=True)
        updates: dict[str, CatalogConfig | SnapshotConfig | DocumentationConfig] = {}
        for catalog_name, snapshot_name in (
            ("catalog", "snapshots"),
            ("documentation_catalog", "documentation_snapshots"),
        ):
            catalog = spec.catalog if catalog_name == "catalog" else spec.documentation_catalog
            if catalog is not None and catalog.manifest is not None:
                updates[catalog_name] = catalog.model_copy(update={"documents": []})
                snapshots = spec.snapshots if snapshot_name == "snapshots" else spec.documentation_snapshots
                updates[snapshot_name] = snapshots.model_copy(update={"documents": []})
        if spec.documentation.manifest is not None:
            updates["documentation"] = spec.documentation.model_copy(update={"snippets": []})
        return resolve_business_inputs(spec.model_copy(update=updates).model_copy(deep=True))

    def bind_business(self, dataset_id: str, path: Path) -> DatasetBuildSpec:
        """Attach a category policy without replacing existing runtime settings."""
        spec = self.get_snapshot(dataset_id)
        if spec.bundle_version is None:
            updated = resolve_labeling_inputs(
                spec.model_copy(update={"business_config": str(path.resolve())})
            )
        else:
            updated = spec.model_copy(update={"rules": load_business_rules(path, spec.rules.category_id)})
        self.register(updated, previous=spec)
        return updated

    def keys(self) -> list[str]:
        return sorted(path.parent.name for path in self.root.glob("*/current"))
