from typing import Final

from dagster import (
    AssetIn,
    AssetKey,
    AssetsDefinition,
    DagsterInstance,
    DataVersion,
    IntMetadataValue,
    MetadataValue,
    Output,
    SpecificPartitionsPartitionMapping,
    TextMetadataValue,
    asset,
)

from codebench_pipeline.config_assets import current_configuration_record, mapped_input, mapping_type
from codebench_pipeline.dataframe_io import materialized_reference
from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID
from codebench_pipeline.input_provenance import current_materialization
from codebench_pipeline.rules_contracts import Category, Split
from codebench_pipeline.rules_corpus import Corpus
from codebench_pipeline.rules_registration_config import SampleSelection, SplitPin
from codebench_pipeline.rules_sampling import SAMPLING_VERSION, RulesSample, sample_corpus
from codebench_pipeline.rules_settings import (
    RULES_CONFIG_IO,
    RULES_GROUP,
    RULES_PARTITIONS,
    RulesSampleSettings,
)
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.train_config import MergeSettings

_MERGE_DIGEST: Final = "Source merge digest"
_SOURCES_DIGEST: Final = "Source manifests digest"
_SAMPLE_DIGEST: Final = "Sample digest"
_SAMPLE_ROWS: Final = "Sample rows"
_SAMPLE_VERSION: Final = "Sampling version"
_SAMPLE_CATEGORIES: Final = "Capture categories digest"


def sample_scope(split: Split) -> str:
    return f"rules_{split}_sample"


def sample_key(split: Split) -> AssetKey:
    return AssetKey(["codebench", sample_scope(split)])


def sample_input(split: Split, *, for_capture: bool = False) -> AssetIn:
    return AssetIn(
        key=sample_key(split), dagster_type=mapping_type(RulesSample),
        partition_mapping=SpecificPartitionsPartitionMapping([CODEBENCH_DATASET_ID]) if for_capture else None,
    )


def _sources_digest(sources: MergeSettings) -> str:
    return fingerprint([[key, digest] for key, digest in sorted(sources.category_digests.items())])


def _categories_digest(categories: tuple[Category, ...]) -> str:
    return fingerprint([str(category) for category in sorted(categories)])


def _current_merge_digest(instance: DagsterInstance, split: Split) -> str:
    record = current_materialization(instance, AssetKey([split, "merged_rows"]), CODEBENCH_DATASET_ID)
    if record is None:
        raise IntegrityError(f"No current {split} merge; materialize it before sampling")
    return materialized_reference(instance, record).digest


def require_sample(
    instance: DagsterInstance, sample: RulesSample, split: Split, sources: MergeSettings, selection: SampleSelection,
    *, required_categories: tuple[Category, ...] = (),
) -> SplitPin:
    if (
        sample.corpus.split != split or sample.version != SAMPLING_VERSION or sample.selection != selection
        or sample.required_categories != tuple(sorted(required_categories))
        or sample.corpus.category_sources_digests != sources.category_digests
        or sample.source_digest != _current_merge_digest(instance, split)
    ):
        raise IntegrityError(f"The {split} sample is stale or belongs to another split; rematerialize it")
    return sample.pin


def require_frozen_train_source(
    instance: DagsterInstance, pin: SplitPin, sources: MergeSettings, categories: tuple[Category, ...],
) -> str:
    """Check holdout lineage using only train materialization metadata, never train rows."""
    record = current_configuration_record(instance, sample_key("train"), CODEBENCH_DATASET_ID)
    materialization = record.asset_materialization
    if materialization is None:
        raise IntegrityError("Train sample has no materialization evidence")
    metadata = materialization.metadata
    source = _current_merge_digest(instance, "train")
    if (
        metadata.get(_SAMPLE_DIGEST) != TextMetadataValue(pin.digest)
        or metadata.get(_SAMPLE_ROWS) != IntMetadataValue(pin.row_count)
        or metadata.get(_MERGE_DIGEST) != TextMetadataValue(source)
        or metadata.get(_SOURCES_DIGEST) != TextMetadataValue(_sources_digest(sources))
        or metadata.get(_SAMPLE_VERSION) != TextMetadataValue(SAMPLING_VERSION)
        or metadata.get(_SAMPLE_CATEGORIES) != TextMetadataValue(_categories_digest(categories))
    ):
        raise IntegrityError("Frozen bundle does not match the current train sample; rerun learning")
    return source


def _sample_asset(split: Split) -> AssetsDefinition:
    scope = sample_scope(split)

    @asset(
        key=sample_key(split),
        group_name=RULES_GROUP,
        tags={"stage": scope},
        partitions_def=RULES_PARTITIONS,
        io_manager_key=RULES_CONFIG_IO,
        dagster_type=mapping_type(RulesSample),
        ins={
            "settings": mapped_input(scope, RulesSampleSettings),
            "merged": AssetIn(
                key=AssetKey([split, "merged_rows"]), dagster_type=mapping_type(Corpus),
                input_manager_key="rules_corpus_input",
            ),
        },
    )
    def split_sample(settings: RulesSampleSettings, merged: Corpus) -> Output[RulesSample]:
        """Persist the deterministic private rules sample of one pinned split."""
        if merged.split != split or merged.category_sources_digests != settings.sources.category_digests:
            raise IntegrityError(f"The {split} merge does not match configured sources; rematerialize it")
        sample = sample_corpus(merged, settings.selection, required_categories=settings.required_categories)
        pin = sample.pin
        return Output(sample, data_version=DataVersion(pin.digest), metadata={
            _MERGE_DIGEST: MetadataValue.text(sample.source_digest),
            _SOURCES_DIGEST: MetadataValue.text(_sources_digest(settings.sources)),
            _SAMPLE_DIGEST: MetadataValue.text(pin.digest),
            _SAMPLE_ROWS: MetadataValue.int(pin.row_count),
            _SAMPLE_VERSION: MetadataValue.text(sample.version),
            _SAMPLE_CATEGORIES: MetadataValue.text(_categories_digest(sample.required_categories)),
        })

    return split_sample


rules_train_sample = _sample_asset("train")
rules_test_sample = _sample_asset("test")
