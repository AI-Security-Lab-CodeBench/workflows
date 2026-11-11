import os
from pathlib import Path

from pydantic import TypeAdapter

from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.models import Identifier
from codebench_pipeline.storage import IntegrityError, exclusive, fingerprint


class DatasetRegistry:
    def __init__(self, home: Path) -> None:
        self.root = home.resolve() / "dataset-specifications"

    def register(self, spec: DatasetBuildSpec) -> str:
        directory = self.root / spec.dataset_id
        version = fingerprint(spec.model_dump_json())
        directory.mkdir(parents=True, exist_ok=True)
        with HumanReviewStore(self.root.parent).transaction(), exclusive(directory / ".lock"):
            target = directory / f"{version}.json"
            if not target.exists():
                target.write_text(spec.model_dump_json(indent=2), encoding="utf-8")
            temporary = directory / ".current"
            temporary.write_text(version, encoding="ascii")
            os.replace(temporary, directory / "current")
        return spec.dataset_id

    def get(self, dataset_id: str) -> DatasetBuildSpec:
        key = TypeAdapter(Identifier).validate_python(dataset_id)
        directory = self.root / key
        version = (directory / "current").read_text(encoding="ascii")
        spec = DatasetBuildSpec.model_validate_json((directory / f"{version}.json").read_bytes())
        if spec.dataset_id != key:
            raise IntegrityError("Dataset registry identity mismatch")
        return spec

    def keys(self) -> list[str]:
        return sorted(path.parent.name for path in self.root.glob("*/current"))
