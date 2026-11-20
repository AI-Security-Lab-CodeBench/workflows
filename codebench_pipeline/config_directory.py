import os
from pathlib import Path
from typing import Final, Self

from pydantic import BaseModel, ConfigDict, JsonValue, model_validator

from codebench_pipeline.models import Digest, Identifier
from codebench_pipeline.storage import IntegrityError, fingerprint, open_directory, open_regular_file

_MAX_FILE_BYTES: Final = 1024 * 1024
_SHARED_FILES: Final = ("labeling/instructions.md", "labeling/policy.json")
_CATEGORY_FILES: Final = (
    "category.json",
    "collection/documentation-crawl.json",
    "collection/existing-datasets.json",
    "examples.json",
    "instructions.md",
    "keenable/declaration.json",
)
_EXCLUDED_CATEGORY_ENTRIES: Final = {
    ".": frozenset({"reference-examples"}),
    "collection": frozenset({"README.md", "cheatsheets_front_matter.py", "markdown_shell.py"}),
    "keenable": frozenset({"README.md"}),
}


def _category_directory(category_id: Identifier) -> Path:
    return Path("categories", category_id.replace("-", "").lower())


class ConfigDirectorySnapshot(BaseModel):
    """The authored data files authorized for a single category."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    category_id: Identifier
    files: dict[str, str]

    @property
    def scope(self) -> str:
        return f"dataset-authoring/{self.category_id}"

    @property
    def category_directory(self) -> Path:
        return _category_directory(self.category_id)

    @property
    def digest(self) -> Digest:
        return fingerprint({"scope": self.scope, "files": dict[str, JsonValue](self.files)})

    @model_validator(mode="after")
    def require_authorized_files(self) -> Self:
        if not self.files.keys() <= set(self.names(self.category_id)):
            raise IntegrityError(f"Incorrect file set for configuration scope {self.scope}")
        return self

    @staticmethod
    def names(category_id: Identifier) -> tuple[str, ...]:
        category = _category_directory(category_id)
        return tuple(sorted((*_SHARED_FILES, *((category / name).as_posix() for name in _CATEGORY_FILES))))

    @classmethod
    def observe(cls, root: Path, category_id: Identifier) -> Self:
        category = _category_directory(category_id)
        expected = {Path(name) for name in cls.names(category_id)}
        directories = {
            Path("labeling"): frozenset[str](),
            **{category / name: entries for name, entries in _EXCLUDED_CATEGORY_ENTRIES.items()},
        }
        files: dict[str, str] = {}
        for relative, excluded in sorted(directories.items()):
            authorized = {path.name for path in expected if path.parent == relative}
            children = {path.name for path in directories if path.parent == relative}
            try:
                with open_directory(root / relative) as directory:
                    entries = set(os.listdir(directory))
                    unsupported = entries - authorized - children - excluded
                    if unsupported:
                        raise IntegrityError(
                            f"Unsupported entries in configuration scope {relative}: {sorted(unsupported)!r}"
                        )
                    for name in sorted(entries & authorized):
                        with open_regular_file(directory, name) as stream:
                            body = stream.read(_MAX_FILE_BYTES + 1)
                        path = (relative / name).as_posix()
                        if len(body) > _MAX_FILE_BYTES:
                            raise IntegrityError(f"Configuration file exceeds the read limit: {path}")
                        files[path] = body.decode("utf-8")
            except FileNotFoundError as error:
                raise FileNotFoundError(
                    f"Configuration scope changed or is unavailable: {relative}"
                ) from error
        return cls(category_id=category_id, files=files)

    @classmethod
    def capture(cls, root: Path, category_id: Identifier) -> Self:
        return cls.observe(root, category_id).require_complete(category_id)

    def require_complete(self, category_id: Identifier) -> Self:
        if self.category_id != category_id:
            raise IntegrityError("Observed configuration scope belongs to a different category")
        missing = set(self.names(category_id)) - self.files.keys()
        if missing:
            raise FileNotFoundError(f"Required configuration file is missing: {sorted(missing)!r}")
        return self

    def read(self, path: Path) -> bytes:
        name = path.as_posix()
        if name not in self.files:
            raise IntegrityError(f"File is not authorized in configuration scope {self.scope}: {name}")
        return self.files[name].encode("utf-8")
