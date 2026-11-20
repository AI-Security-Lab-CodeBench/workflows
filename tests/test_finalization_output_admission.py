import json
import unittest
from collections.abc import Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd
import psycopg
from dagster import (
    AssetKey,
    AssetSelection,
    DagsterInstance,
    IOManagerDefinition,
    OutputContext,
    StaticPartitionsDefinition,
    asset,
    build_input_context,
    materialize,
)
from psycopg import sql
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.config_assets import (
    GLOBAL_CONFIG,
    ConfigRunIOManager,
    RegisteredConfig,
    configuration_assets,
    mapping_key,
)
from codebench_pipeline.dataframe_io import AssetReference, DataFrameIOManager
from codebench_pipeline.dataframe_schemas import LabelingAttemptsTable, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_assets_annotations import annotation_assets
from codebench_pipeline.dataset_assets_sources import category_rules
from codebench_pipeline.dataset_collection_binding import FINALIZATION_BINDING
from codebench_pipeline.dataset_finalization import category_eligible_annotations, deduplicated_annotations
from codebench_pipeline.dataset_storage_io import RegisteredDatasetTableIO
from codebench_pipeline.finalization_publication import (
    FINALIZATION_PUBLICATION_METADATA,
    FinalizationPublicationV1,
)
from codebench_pipeline.finalization_table_asset import FINALIZATION_TABLE_IO, finalization_table_assets
from codebench_pipeline.finalization_table_io import FinalizationTableIO, finalization_table_io
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.labeling_outcomes import failed_attempt, label_attempt
from codebench_pipeline.labeling_tables import proposed_from_attempts


class FinalizationOutputAdmissionTests(unittest.TestCase):
    def test_changed_immediate_input_cannot_replace_current_output(self) -> None:
        for target, upstream in (
            ("proposed_annotations", "labeling_attempts"),
            ("labeling_failures", "labeling_attempts"),
            ("deduplicated_annotations", "proposed_annotations"),
            ("category_eligible_annotations", "deduplicated_annotations"),
        ):
            with self.subTest(target=target):
                self.exercise_race(target, upstream)

    def exercise_race(self, target: str, upstream: str) -> None:
        with TemporaryDirectory() as temporary:
            home = Path(temporary)
            store = HumanReviewStore(home)
            try:
                self.run_race(home, store, target, upstream)
            finally:
                with psycopg.connect(store.dsn, autocommit=True) as connection:
                    connection.execute(
                        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(store.schema))
                    )

    def run_race(self, home: Path, store: HumanReviewStore, target: str, upstream: str) -> None:
        spec = fixture_spec(home).model_copy(update={"rules": make_rules()})
        registered = RegisteredConfig.from_spec(spec)
        binding = {"dataset": {"state": "configured", "digest": "0" * 64}}
        target_key = AssetKey(["datasets", target])
        upstream_key = AssetKey(["datasets", upstream])
        rules = registered.rules

        def input_frame(version: str) -> pd.DataFrame:
            supported = make_candidate(f"supported-{version}", rules.risks[0].id)
            unsupported = make_candidate(f"unsupported-{version}", rules.risks[0].id)
            attempts = validated_frame(
                [
                    label_attempt(
                        supported, make_label(supported, rules), documentation(), "a" * 64, "label/supported"
                    ).model_dump(mode="json"),
                    failed_attempt(
                        unsupported, "grounding_missing", "No proof.", [], "b" * 64, "label/unsupported"
                    ).model_dump(mode="json"),
                ],
                LabelingAttemptsTable,
            )
            attempts.attrs[FINALIZATION_BINDING] = binding
            if upstream == "labeling_attempts":
                return attempts
            return proposed_from_attempts(attempts, rules)

        current_input = input_frame("previous")
        io = RegisteredDatasetTableIO(storage=spec.storage, review_home=str(home))
        writer = RegisteredDatasetTableIO(storage=spec.storage, review_home=str(home))
        io.bind_review_store(store)
        writer.bind_review_store(HumanReviewStore(home))

        @asset(key=GLOBAL_CONFIG, partitions_def=DATASET_PARTITIONS, io_manager_key="config_run_io_manager")
        def global_config() -> RegisteredConfig:
            return registered

        @asset(key=upstream_key, partitions_def=DATASET_PARTITIONS, io_manager_key="dataset_io_manager")
        def upstream_table() -> pd.DataFrame:
            return current_input

        migrated = [
            *annotation_assets(),
            *finalization_table_assets(deduplicated_annotations, category_eligible_annotations),
        ]
        mappings = [item for item in configuration_assets(home) if item.key == mapping_key(target)]
        assets = [global_config, *mappings, upstream_table, *(a for a in migrated if a.key == target_key)]
        if target == "proposed_annotations":
            assets.append(category_rules)
        handle_output = FinalizationTableIO.handle_output
        lookup_partitions = StaticPartitionsDefinition([spec.dataset_id])
        with (
            DagsterInstance.local_temp(str(home)) as instance,
            patch("codebench_pipeline.dataset_collection_binding.finalization_binding", return_value=binding),
            patch.object(DataFrameIOManager, "run_configuration", return_value=registered),
            patch(
                "codebench_pipeline.review_output_state.registered_materialization_stale", return_value=False
            ),
            build_input_context(
                asset_key=target_key,
                partition_key=spec.dataset_id,
                asset_partitions_def=lookup_partitions,
                instance=instance,
            ) as target_context,
            build_input_context(
                asset_key=upstream_key,
                partition_key=spec.dataset_id,
                asset_partitions_def=lookup_partitions,
                instance=instance,
            ) as upstream_context,
        ):
            instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
            resources = {
                "config_run_io_manager": ConfigRunIOManager(home=str(home)),
                "dataset_io_manager": IOManagerDefinition(resource_fn=lambda _: io),
                FINALIZATION_TABLE_IO: finalization_table_io,
            }
            previous = materialize(
                assets, resources=resources, instance=instance, partition_key=spec.dataset_id
            )
            self.assertTrue(previous.success)
            previous_output = io.current_reference(target_context)
            materialization = previous.asset_materializations_for_node(target_key.to_python_identifier())[0]
            publication = FinalizationPublicationV1.model_validate_json(
                json.dumps(materialization.metadata[FINALIZATION_PUBLICATION_METADATA].value)
            )
            self.assertEqual(publication.output, previous_output)
            self.assertEqual(
                {item.asset_key: item.reference for item in publication.upstreams},
                {tuple(upstream_key.path): io.reference_at(upstream_key.path, spec.dataset_id)},
            )
            current_input = input_frame("in-flight")

            replacement_published = False
            reference_at = DataFrameIOManager.reference_at

            def assert_serialized_recheck(
                manager: DataFrameIOManager, asset_key: Sequence[str], partition: str | None
            ) -> AssetReference:
                if manager is io and replacement_published and list(asset_key) == upstream_key.path:
                    with (
                        self.assertRaises(HumanReviewError) as held,
                        writer.review_store.transaction(dataset_id=spec.dataset_id),
                    ):
                        self.fail("Upstream writer lock is free during admission")
                    self.assertEqual(held.exception.code, "conflict")
                return reference_at(manager, asset_key, partition)

            def publish_after_upstream_changed(
                manager: FinalizationTableIO, context: OutputContext, obj: pd.DataFrame
            ) -> None:
                nonlocal current_input, replacement_published
                self.assertTrue(
                    all(str(candidate).endswith("-in-flight") for candidate in obj["candidate_id"])
                )
                self.assertGreater(len(obj), 0)
                consumed = writer.current_reference(upstream_context)
                current_input = input_frame("replacement")
                replacement = materialize(
                    [upstream_table],
                    resources={"dataset_io_manager": IOManagerDefinition(resource_fn=lambda _: writer)},
                    instance=instance,
                    partition_key=spec.dataset_id,
                )
                self.assertTrue(replacement.success)
                self.assertNotEqual(writer.current_reference(upstream_context), consumed)
                replacement_published = True
                handle_output(manager, context, obj)

            with (
                patch.object(FinalizationTableIO, "handle_output", publish_after_upstream_changed),
                patch.object(DataFrameIOManager, "reference_at", assert_serialized_recheck),
            ):
                raced = materialize(
                    assets,
                    resources=resources,
                    instance=instance,
                    partition_key=spec.dataset_id,
                    selection=AssetSelection.assets(GLOBAL_CONFIG, *mappings, upstream_key, target_key),
                    raise_on_error=False,
                )
            self.assertTrue(replacement_published)
            self.assertEqual(io.current_reference(target_context), previous_output)
            self.assertFalse(raced.success)
            self.assertFalse(raced.asset_materializations_for_node(target_key.to_python_identifier()))
            self.assertIn(
                f"Immediate upstream datasets/{upstream} changed",
                str(raced.failure_data_for_node(target_key.to_python_identifier())),
            )
