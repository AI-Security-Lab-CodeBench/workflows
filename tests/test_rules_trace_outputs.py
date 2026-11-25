import json
import logging
import unittest
from collections.abc import Iterator, Mapping

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetsDefinition,
    DagsterEvent,
    DagsterEventType,
    DagsterInstance,
    EventLogEntry,
    MetadataValue,
    Output,
    RetryRequested,
    StaticPartitionsDefinition,
    asset,
    materialize,
)
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.trace_projection import Span, project_run

_PRIVATE = "s3://private/rules/secret.html"
_DIGEST = "e" * 64
_JSON = TypeAdapter(JsonValue)


def _example(
    name: str, metadata: Mapping[str, MetadataValue | str | int], partition: str = "codebench"
) -> AssetsDefinition:
    @asset(key=["codebench", name], partitions_def=StaticPartitionsDefinition([partition]))
    def synthetic_output() -> Output[int]:
        return Output(0, metadata=metadata)

    return synthetic_output


def _attributes(span: Span) -> dict[str, str]:
    return {item["key"]: item["value"]["stringValue"] for item in span["attributes"]}


def _output(span: Span) -> JsonValue:
    return _JSON.validate_json(_attributes(span)["mlflow.spanOutputs"])


def _entries(instance: DagsterInstance, run_id: str) -> list[EventLogEntry]:
    return [record.event_log_entry for record in instance.get_records_for_run(run_id).records]


class RulesTraceOutputTests(unittest.TestCase):
    def test_stored_rules_products_export_only_typed_allowlisted_fields(self) -> None:
        fixtures: dict[str, tuple[Mapping[str, MetadataValue | str | int], dict[str, str | int]]] = {
            "rules_capture": (
                {
                    "rules": 0,
                    "batches": 4,
                    "category digest": _DIGEST,
                    "codebench/config_digest": _DIGEST,
                    "Train digest": _DIGEST,
                    "wrong field with a valid digest": _DIGEST,
                    "resumed from run": _PRIVATE,
                },
                {"rule_count": 0, "batch_count": 4, "category_digest": _DIGEST, "config_digest": _DIGEST},
            ),
            "rules_dedup": (
                {"rules": 3, "judge batches": 2, "Train rows": -1, "Train digest": _PRIVATE},
                {"rule_count": 3, "judge_batch_count": 2},
            ),
            "rules_finalize": (
                {"rules": "3", "bundle digest": _DIGEST, "Train rows": 7},
                {"bundle_digest": _DIGEST, "train_rows": 7},
            ),
            "rules_eval": (
                {
                    "Held-out support": 7,
                    "Exact matches": 2,
                    "Wrong matches": 1,
                    "Abstentions": 3,
                    "Evaluator errors": 1,
                    "Test digest": MetadataValue.url(_DIGEST),
                    "codebench/registration_digest": _PRIVATE,
                    "dagster/row_count": 100,
                    "Outcome distribution": MetadataValue.md(_PRIVATE),
                },
                {
                    "heldout_support": 7,
                    "heldout_exact_matches": 2,
                    "heldout_wrong_matches": 1,
                    "heldout_abstentions": 3,
                    "heldout_evaluator_errors": 1,
                    "table_rows": 100,
                },
            ),
            "rules_eval_report": (
                {
                    "codebench/content_digest": _DIGEST,
                    "codebench/registration_digest": _DIGEST,
                    "dagster/row_count": 1,
                    "HTML object": _PRIVATE,
                },
                {"content_digest": _DIGEST, "registration_digest": _DIGEST, "table_rows": 1},
            ),
        }
        captures = {"rules_capture": fixtures["rules_capture"]}
        remaining = {name: sample for name, sample in fixtures.items() if name != "rules_capture"}
        with DagsterInstance.local_temp() as instance:
            for partition, stage_fixtures in (("CAT-6", captures), ("codebench", remaining)):
                with self.subTest(partition=partition):
                    assets = [
                        _example(name, metadata, partition) for name, (metadata, _) in stage_fixtures.items()
                    ]
                    result = materialize(assets, instance=instance, partition_key=partition)
                    entries = _entries(instance, result.run_id)

                    root, *spans = project_run(entries[-1], entries, enabled_at=0).spans

                    expected = {
                        f"codebench__{name}": {
                            "asset_key": f"codebench/{name}",
                            "execution_status": "ok",
                            "status": "materialized",
                            "partition": partition,
                            **fields,
                        }
                        for name, (_, fields) in stage_fixtures.items()
                    }
                    self.assertEqual({span["name"]: _output(span) for span in spans}, expected)
                    self.assertEqual(
                        _output(root),
                        {"execution_status": "ok", "products": [expected[span["name"]] for span in spans]},
                    )
                    for span in spans:
                        self.assertEqual(_attributes(span)["codebench.output.partition"], partition)
                    self.assertNotIn(_PRIVATE, json.dumps([root, *spans]))

    def test_retry_does_not_reuse_materialization_and_failed_root_keeps_partial_product(self) -> None:
        @asset(key=["codebench", "rules_capture"])
        def capture(context: AssetExecutionContext) -> Iterator[AssetMaterialization]:
            if context.retry_number == 0:
                yield AssetMaterialization(
                    ["codebench", "rules_capture"], partition="CAT-6", metadata={"rules": 2}
                )
                raise RetryRequested(max_retries=1)
            yield AssetMaterialization(["codebench", "rules_dedup"], metadata={"rules": 99})
            raise RuntimeError(_PRIVATE)

        with DagsterInstance.local_temp() as instance:
            result = materialize([capture], instance=instance, raise_on_error=False)
            entries = _entries(instance, result.run_id)

        root, first, retry = project_run(entries[-1], entries, enabled_at=0).spans

        product = {
            "asset_key": "codebench/rules_capture",
            "execution_status": "retry",
            "status": "materialized",
            "partition": "CAT-6",
            "rule_count": 2,
        }
        self.assertEqual(_output(first), product)
        self.assertEqual(
            _output(retry),
            {"asset_key": "codebench/rules_capture", "execution_status": "error", "status": "missing"},
        )
        self.assertEqual(_output(root), {"execution_status": "error", "products": [product]})
        self.assertNotIn(_PRIVATE, json.dumps([root, first, retry]))

    def test_canceled_root_keeps_completed_product_but_not_unfinished_step(self) -> None:
        report_key = "codebench__rules_eval_report"
        eval_key = AssetKey(["codebench", "rules_eval"])
        scratch_key = AssetKey(["codebench", report_key.removeprefix("codebench__")])

        @asset(key=scratch_key, deps=[eval_key], partitions_def=StaticPartitionsDefinition(["codebench"]))
        def report() -> None:
            return None

        with DagsterInstance.local_temp() as instance:
            result = materialize(
                [_example("rules_eval", {"Exact matches": 1}), report],
                instance=instance,
                partition_key="codebench",
            )
            entries = _entries(instance, result.run_id)
        report_start = next(
            i
            for i, entry in enumerate(entries)
            if entry.step_key == report_key and entry.dagster_event_type == DagsterEventType.STEP_START
        )
        terminal = EventLogEntry(
            None,
            logging.INFO,
            "",
            result.run_id,
            entries[-1].timestamp,
            job_name="evaluate_rules",
            dagster_event=DagsterEvent(DagsterEventType.RUN_CANCELED.value, "evaluate_rules"),
        )

        root, evaluation, unfinished = project_run(
            terminal, [*entries[: report_start + 1], terminal], 0
        ).spans

        self.assertEqual(
            _output(root),
            {"execution_status": "canceled", "products": [_output(evaluation)]},
        )
        self.assertEqual(
            _output(unfinished),
            {"asset_key": "codebench/rules_eval_report", "execution_status": "canceled", "status": "missing"},
        )

    def test_root_keeps_legacy_status_when_step_has_no_materialization(self) -> None:
        @asset(key=["codebench", "rules_finalize"], output_required=False)
        def omit_output() -> Iterator[Output[int]]:
            yield from ()

        with DagsterInstance.local_temp() as instance:
            result = materialize([omit_output], instance=instance)
            entries = _entries(instance, result.run_id)

        root, step = project_run(entries[-1], entries, enabled_at=0).spans

        self.assertEqual(_output(root), "ok")
        self.assertEqual(
            _output(step),
            {"asset_key": "codebench/rules_finalize", "execution_status": "ok", "status": "missing"},
        )

    def test_untrusted_and_ambiguous_partitions_are_not_projected(self) -> None:
        key = ["codebench", "rules_capture"]

        @asset(key=key, output_required=False)
        def unsafe() -> Iterator[AssetMaterialization]:
            yield AssetMaterialization(key, partition=_PRIVATE)

        @asset(key=key, output_required=False)
        def ambiguous() -> Iterator[AssetMaterialization]:
            yield AssetMaterialization(key, partition="CAT-6")
            yield AssetMaterialization(key, partition="CAT-7")

        with DagsterInstance.local_temp() as instance:
            for fixture, status in ((unsafe, "materialized"), (ambiguous, "ambiguous")):
                with self.subTest(status=status):
                    result = materialize([fixture], instance=instance)
                    entries = _entries(instance, result.run_id)

                    root, span = project_run(entries[-1], entries, enabled_at=0).spans

                    self.assertEqual(
                        _output(span),
                        {"asset_key": "codebench/rules_capture", "execution_status": "ok", "status": status},
                    )
                    self.assertNotIn("codebench.output.partition", _attributes(span))
                    self.assertNotIn(_PRIVATE, json.dumps([root, span]))
