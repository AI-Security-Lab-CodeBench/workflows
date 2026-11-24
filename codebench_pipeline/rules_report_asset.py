import pandas as pd
import pandera.pandas as pa
from dagster import AssetExecutionContext, AssetIn, AssetKey, MetadataValue, Output, asset
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.typing import DataFrame, Series

from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.object_store import sha256
from codebench_pipeline.rules_artifacts import RULES_EVAL_KEY, RULES_METRICS_TYPE, RulesMetricsTable
from codebench_pipeline.rules_report import render_rules_report
from codebench_pipeline.rules_report_document import report_key, report_url
from codebench_pipeline.rules_settings import RULES_GROUP, RULES_PARTITIONS
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.train_assets import TRAIN_TABLE_IO


class RulesReportTable(pa.DataFrameModel):
    """Immutable HTML document and its browser-accessible address."""

    report_url: Series[str]
    object_uri: Series[str]
    sha256: Series[str]
    metric_rows: Series[int]
    size_bytes: Series[int]

    class Config(pa.DataFrameModel.Config):
        strict = True
        ordered = True


@asset(
    key=AssetKey(["codebench", "rules_eval_report"]),
    group_name=RULES_GROUP,
    tags={"stage": "rules_eval"},
    partitions_def=RULES_PARTITIONS,
    ins={"metrics": AssetIn(key=RULES_EVAL_KEY, dagster_type=RULES_METRICS_TYPE)},
    io_manager_key=TRAIN_TABLE_IO,
    required_resource_keys={TRAIN_TABLE_IO},
    dagster_type=pandera_schema_to_dagster_type(RulesReportTable),
)
def rules_eval_report(
    context: AssetExecutionContext, metrics: DataFrame[RulesMetricsTable],
) -> Output[DataFrame[RulesReportTable]]:
    """Publish a standalone, interactive report from held-out aggregate metrics only."""
    manager = context.resources.train_table_io
    if not isinstance(manager, DataFrameIOManager):
        raise IntegrityError("Evaluation reports require registered dataset storage")
    document = render_rules_report(metrics).encode("utf-8")
    digest = sha256(document)
    uri = manager.object_store().write(report_key(digest), document)
    url = report_url(digest)
    manifest = RulesReportTable.validate(pd.DataFrame([{
        "report_url": url, "object_uri": uri, "sha256": digest,
        "metric_rows": len(metrics), "size_bytes": len(document),
    }]))
    return Output(manifest, metadata={"Open interactive report": MetadataValue.url(url), "HTML object": uri})
