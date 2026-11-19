from json import dumps
from pathlib import Path
from tempfile import TemporaryDirectory
from time import time
from unittest import TestCase
from uuid import uuid4

from configuration_fixture import write_fixture
from dagster import AssetMaterialization, DagsterEvent, DagsterEventType, DagsterInstance
from dagster._core.events import StepMaterializationData
from dagster._core.events.log import EventLogEntry
from jsonschema import Draft202012Validator
from starlette.testclient import TestClient

from codebench_pipeline.config_assets import GLOBAL_CONFIG, mapping_key
from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.object_store import sha256

ORIGIN = "http://127.0.0.1:3000"


class ConfigArtifactAPITests(TestCase):
    def test_inspector_reads_run_config_for_the_selected_partition(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        check_config_inspector(home)

    def test_inspector_reads_persisted_config_for_a_removed_map(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        check_config_inspector(home, scope="deduplicated_annotations")


def check_config_inspector(tmp_path: Path, *, scope: str = "labeling_documentation") -> None:
    spec = load_build_config(write_fixture(tmp_path), tmp_path)
    registry = DatasetRegistry(tmp_path)
    registry.register(spec)
    registry.register(spec.model_copy(update={"dataset_id": "unmaterialized"}))
    key = mapping_key(scope)
    expected: dict[str, dict[str, str]] = (
        {scope: {"revision": "from the materialization"}}
        if scope == "labeling_documentation"
        else {scope: {}}
    )
    selected = dumps(expected).encode()

    with DagsterInstance.ephemeral() as instance:
        for partition, payload in (
            (spec.dataset_id, b'{"older":true}'),
            (spec.dataset_id, selected),
            ("different_partition", b'{"wrong_partition":true}'),
        ):
            run_id = str(uuid4())
            config_dir = tmp_path / "configuration-runs" / run_id / "datasets"
            config_dir.mkdir(parents=True)
            (config_dir / key.path[-1]).write_bytes(payload)
            (config_dir / GLOBAL_CONFIG.path[-1]).write_bytes(
                b'{"large":"' + b"x" * (4 * 1024 * 1024) + b'","revision":"persisted input"}'
            )
            for asset_key in (key, GLOBAL_CONFIG):
                instance.store_event(
                    EventLogEntry(
                        error_info=None,
                        level="INFO",
                        user_message="",
                        run_id=run_id,
                        timestamp=time(),
                        dagster_event=DagsterEvent(
                            event_type_value=DagsterEventType.ASSET_MATERIALIZATION.value,
                            job_name="build_dataset",
                            event_specific_data=StepMaterializationData(
                                AssetMaterialization(asset_key=asset_key, partition=partition)
                            ),
                        ),
                    )
                )

        service = DatasetDataService(tmp_path, instance)
        with TestClient(make_dataset_api(service, None, 3000), base_url=ORIGIN) as client:
            headers = {"Origin": ORIGIN}
            partitions = client.post("/artifacts/partitions", headers=headers, json={"asset_key": key.path})
            assert partitions.status_code == 200, partitions.text
            assert partitions.json()["partitions"] == [spec.dataset_id]

            target = {"asset_key": key.path, "partition_key": spec.dataset_id}
            output = client.post("/artifacts/data", headers=headers, json=target)
            assert output.status_code == 200, output.text
            body = output.json()
            assert body["kind"] == "json"
            assert body["snapshot_id"] == sha256(selected)
            assert body["value"] == expected
            assert body["data_schema"]["source"] == "storage"
            schema = body["data_schema"]["schema"]
            Draft202012Validator.check_schema(schema)
            validator = Draft202012Validator(schema)
            assert validator.is_valid(body["value"])
            assert not validator.is_valid(selected.decode())

            upstream = client.post(
                "/artifacts/data", headers=headers, json={**target, "asset_key": GLOBAL_CONFIG.path}
            )
            assert upstream.status_code == 200
            assert len(upstream.content) < 4 * 1024 * 1024
            assert upstream.json()["value"] == {"revision": "persisted input"}
            assert upstream.json()["data_schema"]["schema"]["x-codebench-truncated"] is True

            missing = client.post(
                "/artifacts/data", headers=headers, json={**target, "partition_key": "unmaterialized"}
            )
            assert missing.status_code == 404
            assert missing.json()["error"]["code"] == "not_materialized"
