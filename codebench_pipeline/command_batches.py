from collections.abc import Sequence
from typing import Final

from codebench_pipeline.dataset_documentation_models import command_operations
from codebench_pipeline.dataset_records import CandidateRecord, index_records

GROUPING_VERSION: Final = "parsed-command-prefix-v1"


def command_family_batches(
    candidates: Sequence[CandidateRecord], batch_size: int
) -> list[list[CandidateRecord]]:
    """Group inert commands deterministically; never change or deduplicate a record."""
    if not 1 <= batch_size <= 50:
        raise ValueError("Labeling batches must contain between 1 and 50 records")
    indexed = index_records(candidates)

    def family(candidate: CandidateRecord) -> tuple[str, tuple[tuple[str, ...], ...], str]:
        utility = candidate.util.rsplit("/", 1)[-1]
        operations = command_operations(candidate.candidate_id, utility, candidate.command)
        prefixes = tuple(operation.words[:1] for operation in operations if operation.recognized)
        return utility, prefixes, candidate.candidate_id

    ordered = sorted(indexed.values(), key=family)
    return [ordered[start : start + batch_size] for start in range(0, len(ordered), batch_size)]
