import math
import re
from collections import Counter
from collections.abc import Sequence
from typing import Final

from codebench_pipeline.dataset_records import CandidateRecord, index_records

GROUPING_VERSION: Final = "command-tfidf-cosine-v1"


def similarity_batches(candidates: Sequence[CandidateRecord], batch_size: int) -> list[list[CandidateRecord]]:
    """Group inert commands deterministically; never change or deduplicate a record."""
    if not 1 <= batch_size <= 50:
        raise ValueError("Labeling batches must contain between 1 and 50 records")
    indexed = index_records(candidates)
    counts: dict[str, Counter[str]] = {}
    document_frequency: Counter[str] = Counter()
    for identity, candidate in indexed.items():
        tokens = re.findall(r"--?[\w-]+|[\w]+|[^\w\s]", candidate.command)
        features = Counter("token:" + token for token in tokens)
        text = re.sub(r"\s+", " ", candidate.command)
        features.update("char:" + text[offset : offset + 3] for offset in range(max(0, len(text) - 2)))
        counts[identity] = features
        document_frequency.update(features.keys())
    vectors: dict[str, dict[str, float]] = {}
    for identity, features in counts.items():
        vector = {
            feature: (
                (1 + math.log(count))
                * (1 + math.log((1 + len(candidates)) / (1 + document_frequency[feature])))
                * (2 if feature.startswith("token:") else 1)
            )
            for feature, count in features.items()
        }
        magnitude = math.sqrt(sum(value * value for value in vector.values()))
        vectors[identity] = (
            {feature: value / magnitude for feature, value in vector.items()} if magnitude else {}
        )
    pending = set(indexed)
    batches: list[list[CandidateRecord]] = []
    while pending:
        seed = min(pending)
        reference = vectors[seed]
        neighbors = sorted(
            pending - {seed},
            key=lambda identity: (
                -sum(weight * vectors[identity].get(feature, 0.0) for feature, weight in reference.items()),
                identity,
            ),
        )
        identities = [seed, *neighbors[: batch_size - 1]]
        batches.append([indexed[identity] for identity in identities])
        pending.difference_update(identities)
    return batches
