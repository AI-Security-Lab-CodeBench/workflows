import os
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path

from codebench_pipeline.batch_labeling import BatchExecution
from codebench_pipeline.command_embeddings import ENCODER_DIRECTORY_ENV, encode_from_directory
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.grounding_tools import GroundingConfig, GroundingCredentials
from codebench_pipeline.inference_capacity import CapacityPaths
from codebench_pipeline.storage import IntegrityError


@dataclass(frozen=True, slots=True)
class LabelingRuntime:
    """Operational paths and secret resolution for guarded labeling."""

    dataset_home: Path
    encoder_directory: Path | None
    credential: Callable[[str], str | None] = field(repr=False)

    @staticmethod
    def for_home(home: Path) -> "LabelingRuntime":
        encoder_directory = os.environ.get(ENCODER_DIRECTORY_ENV)
        return LabelingRuntime(
            home.resolve(), Path(encoder_directory) if encoder_directory else None, os.environ.get
        )

    def require_current(
        self,
        dataset_id: str,
        storage: DataFrameIOManager,
    ) -> CapacityPaths:
        if (
            storage.review_home is None
            or Path(storage.review_home).resolve() != self.dataset_home
            or storage.registered_revision is None
        ):
            raise IntegrityError("Guarded labeling requires the registered dataset home")
        return DatasetRegistry(self.dataset_home).labeling_capacity_paths(
            dataset_id, revision=storage.registered_revision
        )

    def capacity_paths(self, dataset_id: str) -> CapacityPaths:
        return DatasetRegistry(self.dataset_home).labeling_capacity_paths(dataset_id)

    def execution(self, model_api_key_env: str | None, grounding: GroundingConfig) -> BatchExecution:
        return BatchExecution(
            model_api_key=self.credential(model_api_key_env) if model_api_key_env is not None else None,
            encoder=partial(encode_from_directory, directory=self.encoder_directory),
            grounding=GroundingCredentials.bind(grounding, self.credential),
        )
