import os
from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, AliasPath, BaseModel, Field, JsonValue, TypeAdapter

from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.inference_capacity import CapacityPaths
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


class StoredObjectLocation(BaseModel):
    dataset_id: Identifier
    storage: StorageConfig


class StoredLocation(StoredObjectLocation):
    declared_collection: dict[str, JsonValue] | None = Field(
        default=None, validation_alias=AliasChoices("declared_collection", "collection_v2")
    )

    @property
    def has_declared_collection(self) -> bool:
        return self.declared_collection is not None


class StoredLabelingCapacity(BaseModel):
    dataset_id: Identifier
    paths: CapacityPaths = Field(validation_alias=AliasPath("annotation", "batch", "capacity"))


class DatasetRegistry:
    def __init__(self, home: Path) -> None:
        self.root = home.resolve() / "dataset-specifications"

    def register(self, spec: DatasetBuildSpec) -> str:
        spec = DatasetBuildSpec.model_validate_json(spec.model_dump_json())
        if not spec.has_declared_collection:
            raise IntegrityError("Only declared collection specifications can be registered")
        directory = self.root / spec.dataset_id
        version = fingerprint(spec.model_dump_json())
        directory.mkdir(parents=True, exist_ok=True)
        with (
            HumanReviewStore(self.root.parent).transaction(dataset_id=spec.dataset_id) as tx,
            exclusive(directory / ".lock"),
        ):
            target = directory / f"{version}.json"
            if not target.exists():
                target.write_text(spec.model_dump_json(indent=2), encoding="utf-8")
            temporary = directory / ".current"
            pointer = directory / "current"
            previous = self.current_revision(spec.dataset_id) if pointer.exists() else None
            if previous != version and (
                previous is None
                or self._stored_spec(spec.dataset_id, previous).model_dump_json(exclude={"revision"})
                != spec.model_dump_json(exclude={"revision"})
            ):
                tx.invalidate(spec.dataset_id, "Registered dataset configuration changed")
            temporary.write_text(version, encoding="ascii")
            os.replace(temporary, pointer)
        return spec.dataset_id

    def current_revision(self, dataset_id: str) -> Digest:
        key = TypeAdapter(Identifier).validate_python(dataset_id)
        return TypeAdapter(Digest).validate_python((self.root / key / "current").read_text(encoding="ascii"))

    def _stored_payload(self, dataset_id: str, revision: str | None = None) -> tuple[bytes, str]:
        key = TypeAdapter(Identifier).validate_python(dataset_id)
        directory = self.root / key
        version = (
            TypeAdapter(Digest).validate_python(revision)
            if revision is not None
            else self.current_revision(dataset_id)
        )
        return (directory / f"{version}.json").read_bytes(), version

    def _stored_spec(self, dataset_id: str, revision: str | None = None) -> DatasetBuildSpec:
        payload, version = self._stored_payload(dataset_id, revision)
        spec, checksum = _validated_spec(payload)
        if spec.dataset_id != dataset_id:
            raise IntegrityError("Dataset registry identity mismatch")
        if checksum is not None and checksum != version:
            raise IntegrityError("Dataset registry revision differs from the registered specification")
        return spec

    def get_snapshot(self, dataset_id: str, *, revision: str | None = None) -> DatasetBuildSpec:
        """Read persisted configuration without resolving any live business files."""
        return self._stored_spec(dataset_id, revision).model_copy(deep=True)

    def storage(self, dataset_id: str, *, revision: str | None = None) -> StorageConfig:
        """Read the persisted location without loading category policy or source inventories."""
        payload, _ = self._stored_payload(dataset_id, revision)
        location = StoredObjectLocation.model_validate_json(payload)
        if location.dataset_id != dataset_id:
            raise IntegrityError("Dataset registry identity mismatch")
        return location.storage

    def labeling_capacity_paths(self, dataset_id: str, *, revision: Digest | None = None) -> CapacityPaths:
        """Project only the operational capacity location from a registration."""
        payload, _ = self._stored_payload(dataset_id, revision)
        location = StoredLabelingCapacity.model_validate_json(payload)
        if location.dataset_id != dataset_id:
            raise IntegrityError("Dataset registry identity mismatch")
        return location.paths

    def location(self, dataset_id: str) -> StoredLocation:
        payload, _ = self._stored_payload(dataset_id)
        location = StoredLocation.model_validate_json(payload)
        if location.dataset_id != dataset_id:
            raise IntegrityError("Dataset registry identity mismatch")
        return location

    def get(self, dataset_id: str) -> DatasetBuildSpec:
        """Load the current declared specification for execution."""
        spec = self._stored_spec(dataset_id)
        if not spec.has_declared_collection:
            raise IntegrityError(
                "Historical prepared specifications are read-only; register a declared collection to build"
            )
        return spec.model_copy(deep=True)

    def keys(self) -> list[str]:
        return sorted(path.parent.name for path in self.root.glob("*/current"))
