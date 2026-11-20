from collections.abc import Callable, Sequence
from typing import Final

import numpy as np
import numpy.typing as npt

from codebench_pipeline.command_embeddings import ENCODER_REVISION, encode_commands
from codebench_pipeline.dataset_records import CandidateRecord, index_records

GROUPING_VERSION: Final = f"command-minilm-l6-v2-int8-cosine-v1@{ENCODER_REVISION}"
type CommandEncoder = Callable[[Sequence[str]], npt.NDArray[np.float32]]


def similarity_batches(
    candidates: Sequence[CandidateRecord], batch_size: int, *, encoder: CommandEncoder | None = None
) -> list[list[CandidateRecord]]:
    """Group inert commands deterministically; never change or deduplicate a record."""
    if not 1 <= batch_size <= 50:
        raise ValueError("Labeling batches must contain between 1 and 50 records")
    indexed = index_records(candidates)
    ordered = [indexed[identity] for identity in sorted(indexed)]
    if len(ordered) <= 1 or batch_size == 1:
        return [[candidate] for candidate in ordered]
    vectors = (encoder or encode_commands)([candidate.command for candidate in ordered])
    pending = np.ones(len(ordered), dtype=np.bool_)
    batches: list[list[CandidateRecord]] = []
    while (indices := np.flatnonzero(pending)).size:
        seed, neighbors = indices[0], indices[1:]
        similarities = np.einsum("ij,j->i", vectors[neighbors], vectors[seed])
        nearest = neighbors[np.argsort(-similarities, stable=True)[: batch_size - 1]]
        batches.append([ordered[seed], *(ordered[index] for index in nearest)])
        pending[seed] = False
        pending[nearest] = False
    return batches
