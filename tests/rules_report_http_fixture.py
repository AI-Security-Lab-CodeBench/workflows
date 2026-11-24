from hashlib import sha256
from pathlib import Path

import pandas as pd
from dagster import DagsterInstance
from starlette.testclient import TestClient

from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager


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
