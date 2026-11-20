from typing import Final, Self, assert_never

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    ModelWrapValidatorHandler,
    PrivateAttr,
    TypeAdapter,
    field_serializer,
    model_validator,
)
from pydantic_core import PydanticCustomError

from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.inference_capacity import ScopedCapacityConfig
from codebench_pipeline.keenable_policy import (
    CurrentKeenableDeclaration,
    Declaration,
    KeenableDeclaration,
    parse_declaration,
)
from codebench_pipeline.models import Digest
from codebench_pipeline.repository_branch_config import (
    RepositoryAcquisitionConfig,
    RepositoryBranchConfig,
    RepositoryBranchDeclaration,
)
from codebench_pipeline.repository_config import (
    DEFAULT_SOURCE_WEIGHTS,
    RepositoryBranch,
    RepositoryConfig,
    SourceWeight,
)
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.typed_config import TypedConfig

type DeclaredSource = RepositoryBranchDeclaration | RepositoryBranchConfig | RepositoryConfig | Declaration
_JSON_OBJECT: Final = TypeAdapter(dict[str, JsonValue])


class SourceDeclaration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source_list: str | None = Field(default=None, exclude_if=lambda path: path is None)
    declaration: DeclaredSource
    digest: Digest
    _original_wire: bytes | None = PrivateAttr(default=None)

    @classmethod
    def from_declaration(cls, declaration: DeclaredSource) -> Self:
        return cls.model_construct(
            declaration=declaration,
            digest=fingerprint(declaration.model_dump(mode="json")),
        )

    @model_validator(mode="wrap")
    @classmethod
    def read_wire(
        cls, data: dict[str, JsonValue | DeclaredSource] | Self, handler: ModelWrapValidatorHandler[Self]
    ) -> Self:
        if not isinstance(data, dict):
            return handler(data)
        payload = data.get("declaration")
        if not isinstance(payload, dict):
            source = handler(data)
            if source.digest != fingerprint(source.declaration.model_dump(mode="json")):
                raise PydanticCustomError(
                    "source_digest", "Source declaration digest does not match its contents"
                )
            return source
        raw = _JSON_OBJECT.validate_python(payload, strict=True)
        if data.get("digest") != fingerprint(raw):
            raise PydanticCustomError(
                "source_digest", "Source declaration digest does not match its contents"
            )
        wire = _JSON_OBJECT.dump_json(raw)
        declaration: DeclaredSource
        if "origins" in raw:
            declaration = (
                RepositoryBranchDeclaration.model_validate_json(wire)
                if "refresh_revision" in raw
                else RepositoryBranchConfig.model_validate_json(wire)
            )
        elif "dataset" in raw and "documentation" in raw:
            declaration = RepositoryConfig.model_validate_json(wire)
        else:
            declaration = parse_declaration(wire)
        source = handler({**data, "declaration": declaration})
        if wire != declaration.model_dump_json().encode():
            source._original_wire = wire
        return source

    @field_serializer("declaration")
    def declaration_wire(self, declaration: DeclaredSource) -> dict[str, JsonValue]:
        if self._original_wire is not None:
            return _JSON_OBJECT.validate_json(self._original_wire)
        return declaration.model_dump(mode="json")

    def repository_config(self, branch: RepositoryBranch) -> RepositoryAcquisitionConfig:
        declaration = self.declaration
        match declaration:
            case RepositoryBranchDeclaration() | RepositoryBranchConfig():
                return declaration.runtime(branch)
            case RepositoryConfig():
                return declaration
            case CurrentKeenableDeclaration() | KeenableDeclaration():
                raise IntegrityError("Keenable does not have repository acquisition configuration")
        assert_never(declaration)

    def keenable_config(self) -> Declaration:
        declaration = self.declaration
        match declaration:
            case CurrentKeenableDeclaration() | KeenableDeclaration():
                return declaration
            case RepositoryBranchDeclaration() | RepositoryBranchConfig() | RepositoryConfig():
                raise IntegrityError("Repository sources do not have Keenable acquisition configuration")
        assert_never(declaration)

    def require_current(self, branch: Branch) -> None:
        if self.source_list is not None or isinstance(self.declaration, RepositoryConfig):
            raise IntegrityError(
                "Historical declared collection contains source-list paths; re-register the path-free dataset configuration"
            )
        if branch == "keenable":
            self.keenable_config()
        else:
            self.repository_config(branch)


class DeclaredCollection(TypedConfig):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    existing_datasets: SourceDeclaration
    documentation_crawl: SourceDeclaration
    keenable_use_cases: SourceDeclaration
    merge_keenable_weight: SourceWeight | None = Field(default=None, exclude_if=lambda weight: weight is None)

    @property
    def keenable_source_weight(self) -> float:
        if self.merge_keenable_weight is not None:
            return self.merge_keenable_weight
        dataset = self.existing_datasets.declaration
        docs = self.documentation_crawl.declaration
        dataset_weight = (
            dataset.keenable_source_weight
            if isinstance(dataset, RepositoryConfig)
            else DEFAULT_SOURCE_WEIGHTS["keenable"]
        )
        if isinstance(docs, RepositoryConfig) and docs.keenable_source_weight != dataset_weight:
            raise IntegrityError("Collection declarations disagree on the Keenable source weight")
        return dataset_weight

    @model_validator(mode="after")
    def verify_collection(self) -> Self:
        self.keenable_use_cases.keenable_config()
        dataset = self.repository_config("dataset")
        docs = self.repository_config("documentation")
        dataset_origins = dataset.origins("dataset")
        docs_origins = docs.origins("documentation")
        origins = (*dataset_origins, *docs_origins)
        if len({origin.origin_id for origin in origins}) != len(origins):
            raise IntegrityError("Repository origin IDs must be unique across branches")
        if len({origin.repository.casefold() for origin in origins}) != len(origins):
            raise IntegrityError("Repository origins must be independent across branches")
        dataset_weights = [dataset.source_weight("dataset", origin.origin_id) for origin in dataset_origins]
        docs_weights = [docs.source_weight("documentation", origin.origin_id) for origin in docs_origins]
        if not self.keenable_source_weight < min(dataset_weights) <= max(dataset_weights) < min(docs_weights):
            raise IntegrityError("Source weights must keep documentation above datasets above Keenable")
        return self

    def for_branch(self, branch: Branch) -> SourceDeclaration:
        return {
            "dataset": self.existing_datasets,
            "documentation": self.documentation_crawl,
            "keenable": self.keenable_use_cases,
        }[branch]

    def require_current_declarations(self) -> None:
        self.existing_datasets.require_current("dataset")
        self.documentation_crawl.require_current("documentation")
        self.keenable_use_cases.require_current("keenable")

    def repository_config(self, branch: RepositoryBranch) -> RepositoryAcquisitionConfig:
        return self.for_branch(branch).repository_config(branch)

    @property
    def keenable_declaration(self) -> Declaration:
        return self.keenable_use_cases.keenable_config()


class AdaptivePlannerConfig(TypedConfig):
    base_url: str
    model: str
    api_key_env: str | None
    capacity: ScopedCapacityConfig
    backend_revision: str


class DeclaredSourceStageConfig(TypedConfig):
    branch: Branch
    source: SourceDeclaration
    planner: AdaptivePlannerConfig | None = None

    @model_validator(mode="after")
    def require_current_source(self) -> Self:
        self.source.require_current(self.branch)
        return self

    @property
    def repository_config(self) -> RepositoryAcquisitionConfig:
        if self.branch == "keenable":
            raise IntegrityError("Keenable does not have repository acquisition configuration")
        return self.source.repository_config(self.branch)

    @property
    def declaration_digest(self) -> str:
        if self.branch == "keenable":
            if self.planner is not None:
                return fingerprint([self.source.digest, self.planner.model_dump(mode="json")])
            return self.source.digest
        return self.repository_config.branch_digest(self.branch)


class CollectionMergeConfig(TypedConfig):
    keenable_source_weight: SourceWeight = DEFAULT_SOURCE_WEIGHTS["keenable"]


CAT3_DATASET_ID: Final = "cloud_infrastructure_commands_grounded_v1"
