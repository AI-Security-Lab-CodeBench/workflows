from collections.abc import Generator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from dagster import DagsterInstance
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.testclient import TestClient

from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID
from codebench_pipeline.dataset_registry import StoredObjectLocation
from codebench_pipeline.object_store import ObjectStore, StorageConfig
from codebench_pipeline.rules_report_document import report_key, report_url
from codebench_pipeline.runtime_instance import dataset_instance


@contextmanager
def synthetic_report(payload: bytes, port: int) -> Generator[tuple[Starlette, str]]:
    digest = sha256(payload).hexdigest()
    with TemporaryDirectory(prefix="rules-report-") as temporary:
        home = Path(temporary)
        location = StoredObjectLocation(
            dataset_id=CODEBENCH_DATASET_ID, storage=StorageConfig(local_root=str(home / "dataframes")),
        )
        snapshot = location.model_dump_json().encode("utf-8")
        revision = sha256(snapshot).hexdigest()
        registration = home / "dataset-specifications" / CODEBENCH_DATASET_ID
        registration.mkdir(parents=True)
        (registration / f"{revision}.json").write_bytes(snapshot)
        (registration / "current").write_text(revision, encoding="ascii")
        ObjectStore.from_environment(location.storage).write(report_key(digest), payload)
        with dataset_instance(home) as instance:
            api = make_dataset_api(DatasetDataService(home, instance), None, port)
            yield Starlette(routes=[Mount("/codebench/api", app=api)]), report_url(digest)


def assert_report_document(home: Path, instance: DagsterInstance, manager: DataFrameIOManager) -> None:
    manifest = manager.read_version(manager.reference_at(["codebench", "rules_eval_report"], "codebench"))
    metrics = manager.read_version(manager.reference_at(["codebench", "rules_eval"], "codebench"))
    assert isinstance(manifest, pd.DataFrame) and isinstance(metrics, pd.DataFrame)
    support = metrics[(metrics.axis == "category") & (metrics.metric == "support")]
    assert dict(zip(support[support.cohort == "unseen"].label, support[support.cohort == "unseen"].value)) == {
        "CAT-5": 1,
    }
    row, = manifest.to_dict(orient="records")
    assert row["metric_rows"] == len(metrics) > 0
    url = row["report_url"].removeprefix("/codebench/api")
    with TestClient(make_dataset_api(DatasetDataService(home, instance), None, 3000),
                    base_url="http://localhost:3000") as client:
        response = client.get(url)
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html")
        assert "sandbox allow-scripts;" in response.headers["content-security-policy"]
        assert sha256(response.content).hexdigest() == row["sha256"]
        assert len(response.content) == row["size_bytes"]
        assert client.get(url.replace(row["sha256"], "not-a-digest")).status_code == 422
        assert client.get(url.replace(row["sha256"], "0" * 64)).status_code == 404
