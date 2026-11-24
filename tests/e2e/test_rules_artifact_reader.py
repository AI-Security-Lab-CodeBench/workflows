from pathlib import Path
from typing import NamedTuple

import pandas as pd
from dagster import AssetKey, AssetsDefinition, DagsterInstance, asset, materialize
from jsonschema import Draft202012Validator
from jsonschema.protocols import Validator
from pydantic import BaseModel, JsonValue
from starlette.testclient import TestClient

from codebench_pipeline.artifact_views import ArtifactDataService, ArtifactPartitions, ArtifactTable
from codebench_pipeline.config_assets import PRIVATE_RULES_RUN_ARTIFACTS, ConfigRunIOManager
from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.dataset_artifacts import DatasetArtifactCatalog
from codebench_pipeline.dataset_registry import StoredObjectLocation
from codebench_pipeline.dataset_storage_io import dataset_storage_io
from codebench_pipeline.object_store import StorageConfig, sha256
from codebench_pipeline.rules_artifacts import FinalizedRules, RulesCaptureArtifact, RulesDedupArtifact
from codebench_pipeline.rules_contracts import Bundle, EvaluationReceipt, Fixture, Labels, NativeRule, Package
from codebench_pipeline.rules_learning import CaptureResult
from codebench_pipeline.rules_metrics import TrainFingerprint
from codebench_pipeline.rules_settings import (
    CAPTURE_PARTITIONS,
    RULES_PARTITIONS,
    AgentSettings,
    GuidePin,
    LearningPins,
    RulesCaptureSettings,
    RulesDedupSettings,
    RulesEvalSettings,
    RulesFinalizeSettings,
    RulesRegistration,
    RulesRuntime,
    SplitPin,
    rules_registration,
)
from codebench_pipeline.rules_similarity import AgentRun, CaptureDecision, CapturedRule, DedupResult
from codebench_pipeline.typed_config import ConfigArtifact

_ORIGIN = "http://127.0.0.1:3000"
_HEADERS = {"Origin": _ORIGIN}
_PRIVATE = "SYNTHETIC_PRIVATE"
_RULE = "cat-3.capture-0001-01"
_PREDICATE = "argv == ['synthetic-inspect']"
_BUNDLE_SHA = "d" * 64
_VALIDATOR: type[Validator] = Draft202012Validator


class _Example(NamedTuple):
    key: tuple[str, str]
    partition: str
    output: BaseModel


class _LegacyCaptureSettings(ConfigArtifact):
    pins: LearningPins
    category: str
    runtime: RulesRuntime
    agent: AgentSettings


def _private_asset(item: _Example) -> AssetsDefinition:
    @asset(
        key=AssetKey(item.key),
        partitions_def=RULES_PARTITIONS if item.partition == "codebench" else CAPTURE_PARTITIONS,
        io_manager_key="config_run_io_manager",
    )
    def synthetic_private_output() -> BaseModel:
        return item.output

    return synthetic_private_output


def _examples(settings: RulesRegistration) -> tuple[_Example, ...]:
    pins = LearningPins(
        train=settings.require_pin("train"), categories=settings.categories, runtime_version=settings.runtime.version,
        guide_sha256=settings.prompt_guide.sha256,
    )
    sources = {category: sha256(category.encode()) for category in settings.categories}
    agent = AgentSettings.model_validate({
        "model": {
            "provider": "openai_compatible", "model": _PRIVATE + "_MODEL",
            "base_url": "http://synthetic-private.invalid/v1", "api_key_env": _PRIVATE + "_KEY",
        },
        "backend_revision": _PRIVATE + "_BACKEND", "max_request_bytes": 1200, "guide": _PRIVATE + "_GUIDE",
    })
    runtime = settings.runtime
    labels = Labels(category="CAT-3", risk="CAT-3.1", severity="low", action="allow")
    rule = NativeRule(
        id=_RULE, when=_PREDICATE, labels=labels,
        rationale=_PRIVATE + "_RATIONALE", sources=("https://example.invalid/" + _PRIVATE,),
    )
    package = Package(rule=rule, tests=(Fixture(command=_PRIVATE + "_COMMAND", expected=labels),))
    receipt = EvaluationReceipt(
        runtime_version=runtime.version, bundle_sha256=_BUNDLE_SHA, split="train",
        corpus_digest=pins.train.digest, rows=(), hit_sets=(),
    )
    run = AgentRun(
        model=agent.model.model, backend_revision=agent.backend_revision, guide_sha256=pins.guide_sha256,
        request_sha256=("e" * 64,), status="finished", failures=(),
    )
    captured = CapturedRule(package=package, singleton=receipt)
    captures = [
        _Example(("codebench", "rules_capture"), category, RulesCaptureArtifact(
            category=category, sources_digest=sources[category], guide_sha256=pins.guide_sha256,
            inference_digest=agent.inference_digest,
            result=CaptureResult(
                corpus_digest=pins.train.digest, runtime_version=runtime.version,
                batches=(CaptureDecision(
                    batch_id=f"{category.lower()}.capture-0001", row_ids=(_PRIVATE + "_ROW",),
                    justification=_PRIVATE + "_JUSTIFICATION", rules=(captured,) if category == "CAT-3" else (),
                    run=run,
                ),),
            ),
        ))
        for category in settings.categories
    ]
    return (
        *captures,
        _Example(("datasets", "map_rules_capture_config"), "CAT-3", RulesCaptureSettings(
            category="CAT-3", runtime=runtime, agent=agent, sources_digest=sources["CAT-3"],
            guide_sha256=pins.guide_sha256,
        )),
        _Example(("datasets", "map_rules_capture_config"), "CAT-4", _LegacyCaptureSettings(
            pins=pins, category="CAT-4", runtime=runtime, agent=agent,
        )),
        _Example(("datasets", "map_rules_dedup_config"), "codebench", RulesDedupSettings(
            pins=pins, runtime=runtime, agent=agent, category_sources_digests=sources,
        )),
        _Example(("datasets", "map_rules_finalize_config"), "codebench", RulesFinalizeSettings(
            pins=pins, runtime=runtime,
        )),
        _Example(("datasets", "map_rules_eval_config"), "codebench", RulesEvalSettings(
            pins=pins, runtime=runtime, test=settings.require_pin("test"),
        )),
        _Example(("codebench", "rules_dedup"), "codebench", RulesDedupArtifact(
            pins=pins, result=DedupResult(
                corpus_digest=pins.train.digest, runtime_version=runtime.version, nominations=(),
                decisions=(), remaining_nominations=(), packages=(package,),
            ),
        )),
        _Example(("codebench", "rules_finalize"), "codebench", FinalizedRules(
            pins=pins, bundle=Bundle(rules=(rule,)), bundle_sha256=_BUNDLE_SHA,
            training=TrainFingerprint(digest=pins.train.digest, categories=("CAT-3",), command_hashes=()),
        )),
    )


def _materialize_parquet(home: Path, instance: DagsterInstance) -> None:
    registration = home / "dataset-specifications" / "codebench"
    registration.mkdir(parents=True)
    payload = StoredObjectLocation(
        dataset_id="codebench", storage=StorageConfig(local_root=str(home / "objects")),
    ).model_dump_json().encode()
    revision = sha256(payload)
    (registration / f"{revision}.json").write_bytes(payload)
    (registration / "current").write_text(revision, encoding="ascii")

    @asset(key=["codebench", "rules_eval"], partitions_def=RULES_PARTITIONS, io_manager_key="train_table_io")
    def metrics() -> pd.DataFrame:
        return pd.DataFrame({"metric": ["support"], "value": [5.0]})

    @asset(key=["codebench", "rules_eval_report"], partitions_def=RULES_PARTITIONS, io_manager_key="train_table_io")
    def report() -> pd.DataFrame:
        return pd.DataFrame({"report_digest": [_BUNDLE_SHA]})

    assert materialize(
        [metrics, report], instance=instance, partition_key="codebench",
        resources={"train_table_io": dataset_storage_io(home, review=False, dataset_id="codebench")},
    ).success


def _partitions(client: TestClient, key: tuple[str, str]) -> tuple[str | None, ...]:
    response = client.post("/artifacts/partitions", headers=_HEADERS, json={"asset_key": key})
    assert response.status_code == 200
    return ArtifactPartitions.model_validate_json(response.content).partitions


def _query(client: TestClient, target: tuple[tuple[str, str], str], **query: JsonValue) -> ArtifactTable:
    key, partition = target
    assert partition in _partitions(client, key)
    response = client.post("/artifacts/data", headers=_HEADERS, json={
        "asset_key": key, "partition_key": partition, "query": query,
    })
    assert response.status_code == 200
    assert _PRIVATE not in response.text
    assert "synthetic-private.invalid" not in response.text
    result = ArtifactTable.model_validate_json(response.content)
    if key in PRIVATE_RULES_RUN_ARTIFACTS:
        assert result.data_schema.source == "declared"
        schema = result.data_schema.schema_
        Draft202012Validator.check_schema(schema)
        properties = schema["properties"]
        assert isinstance(properties, dict)
        assert set(result.page.columns) == set(properties)
        validator = _VALIDATOR(schema)
        for row in result.page.rows:
            assert validator.is_valid(row.values), (key, row.values)
        assert not validator.is_valid("{}")
    return result


def test_rules_artifact_reader(tmp_path: Path) -> None:
    guide = b'{"instructions":"SYNTHETIC_PRIVATE_GUIDE","examples":[{}]}'
    settings = RulesRegistration(
        runtime=RulesRuntime(base_url="http://synthetic-private.invalid", version="sha256:" + "a" * 64),
        prompt_guide=GuidePin(path="guide.json", sha256=sha256(guide)), categories=("CAT-3", "CAT-4"),
        train=SplitPin(digest="b" * 64, row_count=17), test=SplitPin(digest="c" * 64, row_count=5),
    )
    registration = settings.model_copy(update={"test": None}).model_dump_json()
    dagster_home = tmp_path / "dagster"
    dagster_home.mkdir()
    with DagsterInstance.local_temp(str(dagster_home)) as instance:
        instance.add_dynamic_partitions("codebench_train_categories", ["CAT-3", "CAT-4", "CAT-5"])
        resources = {"config_run_io_manager": ConfigRunIOManager(home=str(tmp_path))}
        assert materialize(
            [rules_registration], instance=instance, resources=resources, partition_key="codebench",
            run_config={"ops": {"codebench__rules_registration": {"config": {
                "registration_json": registration, "expected_sha256": sha256(registration.encode()),
                "guide_json": guide.decode(),
            }}}},
        ).success
        examples = _examples(settings)
        for partition in ("codebench", "CAT-3", "CAT-4"):
            assert materialize(
                [_private_asset(item) for item in examples if item.partition == partition],
                instance=instance, resources=resources, partition_key=partition,
            ).success
        _materialize_parquet(tmp_path, instance)

        service = ArtifactDataService(DatasetArtifactCatalog(tmp_path, instance))
        with TestClient(make_dataset_api(None, None, 3000, artifacts=service), base_url=_ORIGIN) as client:
            registered = _query(client, (("codebench", "rules_registration"), "codebench"), search="CAT-4")
            assert registered.page.total_rows == 2
            assert [(row.values["category"], row.values["train_rows"]) for row in registered.page.rows] == [
                ("CAT-4", 17),
            ]
            assert registered.page.rows[0].values["test_rows"] is None
            cells = (
                (("datasets", "map_rules_capture_config"), "CAT-3", "category", "CAT-3"),
                (("datasets", "map_rules_capture_config"), "CAT-4", "train_rows", 17),
                (("datasets", "map_rules_dedup_config"), "codebench", "train_rows", 17),
                (("datasets", "map_rules_finalize_config"), "codebench", "guide_sha256", sha256(guide)),
                (("datasets", "map_rules_eval_config"), "codebench", "test_rows", 5),
                (("codebench", "rules_capture"), "CAT-3", "batch_id", "cat-3.capture-0001"),
                (("codebench", "rules_dedup"), "codebench", "rule_id", _RULE),
                (("codebench", "rules_finalize"), "codebench", "bundle_sha256", _BUNDLE_SHA),
                (("codebench", "rules_finalize"), "codebench", "when", _PREDICATE),
                (("codebench", "rules_eval"), "codebench", "value", 5.0),
                (("codebench", "rules_eval_report"), "codebench", "report_digest", _BUNDLE_SHA),
            )
            assert {key for key, *_ in cells if key in PRIVATE_RULES_RUN_ARTIFACTS} | {
                registered.asset_key
            } == PRIVATE_RULES_RUN_ARTIFACTS
            for key, partition, column, expected in cells:
                result = _query(client, (key, partition))
                assert result.page.rows[0].values[column] == expected
            capture = ("codebench", "rules_capture")
            assert _partitions(client, capture) == ("CAT-3", "CAT-4")
            empty = _query(client, (capture, "CAT-4"))
            assert empty.page.total_rows == 0 and not empty.page.rows
            assert "rule_id" in empty.page.columns
            instance.delete_dynamic_partition("codebench_train_categories", "CAT-4")
            assert _partitions(client, capture) == ("CAT-3",)
            assert client.post("/artifacts/data", headers=_HEADERS, json={
                "asset_key": capture, "partition_key": "CAT-4",
            }).status_code == 404
