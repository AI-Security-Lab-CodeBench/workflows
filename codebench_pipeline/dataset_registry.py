import os
from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, BaseModel, Field, JsonValue, TypeAdapter

from codebench_pipeline.business_config import load_business_rules
from codebench_pipeline.dataset_config import (
    CatalogConfig,
    DatasetBuildSpec,
    DocumentationConfig,
    SnapshotConfig,
)
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.legacy_dataset_config import resolve_business_inputs, resolve_labeling_inputs
from codebench_pipeline.models import Digest, Identifier
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.storage import IntegrityError, exclusive, fingerprint


@lru_cache(maxsize=4)
def _validated_spec(payload: bytes) -> tuple[DatasetBuildSpec, str | None]:
    json_object = TypeAdapter(dict[str, JsonValue])
    snapshot = json_object.validate_json(payload)
    if "refresh_external_inputs" in snapshot:
        spec = DatasetBuildSpec.model_validate_json(payload)
        return spec, fingerprint(spec.model_dump_json())

    historical_collection = "collection_v2" in snapshot
    checksum = fingerprint(json_object.dump_json(snapshot).decode()) if historical_collection else None
    marker = snapshot.pop("bundle_version", None)
    if marker is not None and (type(marker) is not int or marker not in (1, 2)):
        raise IntegrityError("Invalid historical dataset specification format")
    if (marker == 2) != historical_collection:
        raise IntegrityError("Historical dataset specification does not match its source inputs")
    snapshot["refresh_external_inputs"] = marker is None
    if historical_collection:
        snapshot["declared_collection"] = snapshot.pop("collection_v2")
    return DatasetBuildSpec.model_validate_json(json_object.dump_json(snapshot)), checksum


def read_registered_spec(payload: bytes) -> DatasetBuildSpec:
    """Decode a current or historical persisted specification without resolving inputs."""
    return _validated_spec(payload)[0].model_copy(deep=True)


class StoredLocation(BaseModel):
    dataset_id: Identifier
    storage: StorageConfig
    declared_collection: dict[str, JsonValue] | None = Field(
        default=None, validation_alias=AliasChoices("declared_collection", "collection_v2")
    )

    @property
    def has_declared_collection(self) -> bool:
        return self.declared_collection is not None


class DatasetRegistry:
    def __init__(self, home: Path) -> None:
        self.root = home.resolve() / "dataset-specifications"

    def register(self, spec: DatasetBuildSpec, *, previous: DatasetBuildSpec | None = None) -> str:
        spec = DatasetBuildSpec.model_validate_json(spec.model_dump_json())
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

    def _stored_spec(self, dataset_id: str, revision: str | None = None) -> DatasetBuildSpec:
        key = TypeAdapter(Identifier).validate_python(dataset_id)
        directory = self.root / key
        version = TypeAdapter(Digest).validate_python(
            revision if revision is not None else (directory / "current").read_text(encoding="ascii")
        )
        spec, checksum = _validated_spec((directory / f"{version}.json").read_bytes())
        if spec.dataset_id != key:
            raise IntegrityError("Dataset registry identity mismatch")
        if checksum is not None and checksum != version:
            raise IntegrityError("Dataset registry revision differs from the registered specification")
        return spec

    def get_snapshot(self, dataset_id: str, *, revision: str | None = None) -> DatasetBuildSpec:
        """Read persisted configuration without resolving any live business files."""
        return self._stored_spec(dataset_id, revision).model_copy(deep=True)

    def storage(self, dataset_id: str) -> StorageConfig:
        """Read the persisted location without loading category policy or source inventories."""
        return self.location(dataset_id).storage

    def location(self, dataset_id: str) -> StoredLocation:
        key = TypeAdapter(Identifier).validate_python(dataset_id)
        directory = self.root / key
        version = TypeAdapter(Digest).validate_python((directory / "current").read_text(encoding="ascii"))
        location = StoredLocation.model_validate_json((directory / f"{version}.json").read_bytes())
        if location.dataset_id != key:
            raise IntegrityError("Dataset registry identity mismatch")
        return location

    def get(self, dataset_id: str) -> DatasetBuildSpec:
        spec = self._stored_spec(dataset_id)
        if not spec.refresh_external_inputs:
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
        documentation = spec.documentation
        if documentation is not None and documentation.manifest is not None:
            updates["documentation"] = documentation.model_copy(update={"snippets": []})
        return resolve_business_inputs(spec.model_copy(update=updates).model_copy(deep=True))

    def bind_business(self, dataset_id: str, path: Path) -> DatasetBuildSpec:
        """Attach a category policy without replacing existing runtime settings."""
        spec = self.get_snapshot(dataset_id)
        if spec.refresh_external_inputs:
            updated = resolve_labeling_inputs(
                spec.model_copy(update={"business_config": str(path.resolve())})
            )
        else:
            updated = spec.model_copy(update={"rules": load_business_rules(path, spec.rules.category_id)})
        self.register(updated, previous=spec)
        return updated

    def keys(self) -> list[str]:
        return sorted(path.parent.name for path in self.root.glob("*/current"))
