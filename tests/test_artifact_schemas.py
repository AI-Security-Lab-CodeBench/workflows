import unittest
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pandera.pandas as pa
import pyarrow as arrow
import pyarrow.parquet as parquet
from dagster import Definitions, Output, asset
from dagster_pandera import pandera_schema_to_dagster_type
from jsonschema import Draft202012Validator
from pandera.typing import DataFrame, Series
from pydantic import ValidationError
from starlette.testclient import TestClient
from test_artifact_views import FixtureCatalog
from test_dataset_rules import make_rules

from codebench_pipeline.artifact_schemas import declared_table_schema
from codebench_pipeline.artifact_views import ArtifactBinding, ArtifactDataService, ArtifactQuery
from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.dataframe_io import AssetReference, DataFrameIOManager, frame_bytes
from codebench_pipeline.dataframe_schemas import (
    HumanReviewDecisionsTable,
    ProposedAnnotationsTable,
)
from codebench_pipeline.dataset_asset_types import RULES_TYPE, TABLE_TYPES
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.object_store import ObjectStore, StorageConfig, sha256
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.table_query import TableQuery, TableQueryError


class NullableExample(pa.DataFrameModel):
    labels: Series[list[str]] = pa.Field(nullable=True)

    class Config:
        strict = True


class ArtifactSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.storage = StorageConfig(local_root=str(self.home))
        self.manager = DataFrameIOManager(storage=self.storage)
        self.definitions = build_definitions(self.home)

    def service(self, key: tuple[str, ...], reference: AssetReference) -> ArtifactDataService:
        return ArtifactDataService(FixtureCatalog({
            (key, None): ArtifactBinding(self.storage, reference),
        }), self.definitions)

    def test_new_typed_assets_are_discovered_without_viewer_registration_or_execution(self) -> None:
        @asset(key=["novel", "configuration"], dagster_type=RULES_TYPE)
        def configuration() -> Output[CategoryRules]:
            raise AssertionError("Schema inspection must not execute an asset")

        @asset(key=["novel", "rows"], dagster_type=pandera_schema_to_dagster_type(NullableExample))
        def rows() -> Output[DataFrame[NullableExample]]:
            raise AssertionError("Schema inspection must not execute an asset")

        rules = self.manager.persist_value(make_rules())
        empty = self.manager.persist_value(pd.DataFrame(columns=["labels"]))
        catalog = FixtureCatalog({
            (("novel", "configuration"), None): ArtifactBinding(self.storage, rules),
            (("novel", "rows"), None): ArtifactBinding(self.storage, empty),
        })
        service = ArtifactDataService(catalog, Definitions(assets=[configuration, rows]))

        config_response = service.query(ArtifactQuery(asset_key=("novel", "configuration")))
        table_response = service.query(ArtifactQuery(asset_key=("novel", "rows")))

        self.assertEqual(config_response.data_schema.source, "declared")
        self.assertEqual(config_response.data_schema.schema_["properties"]["risks"]["minItems"], 1)
        self.assertEqual(table_response.data_schema.source, "declared")
        self.assertEqual(table_response.data_schema.schema_["properties"]["labels"], {
            "anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "null"}],
        })

    def test_category_rules_http_preserves_complete_values_and_nested_constraints(self) -> None:
        rules = make_rules().model_copy(update={"base_prompt": "Full original " * 1000})
        reference = self.manager.persist_value(rules)
        service = self.service(("datasets", "category_rules"), reference)
        origin = "http://127.0.0.1:3000"

        with TestClient(make_dataset_api(None, None, 3000, artifacts=service), base_url=origin) as client:
            response = client.post(
                "/artifacts/data",
                headers={"Origin": origin},
                json={"asset_key": ["datasets", "category_rules"]},
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["snapshot_id"], reference.digest)
        self.assertEqual(body["value"], rules.model_dump(mode="json"))
        self.assertEqual(body["data_schema"]["source"], "declared")
        schema = body["data_schema"]["schema"]
        Draft202012Validator.check_schema(schema)
        self.assertFalse(schema["additionalProperties"])
        self.assertIn("category_id", schema["required"])
        self.assertEqual(schema["properties"]["risks"]["minItems"], 1)
        self.assertEqual(schema["properties"]["risks"]["items"]["$ref"], "#/$defs/RiskRule")
        self.assertEqual(schema["$defs"]["ClassRule"]["properties"]["name"]["enum"], ["allow", "deny", "ask"])
        altered = {**body["value"], "risks": []}
        self.assertFalse(Draft202012Validator(schema).is_valid(altered))
        self.assertTrue(Draft202012Validator(schema).is_valid(body["value"]))

    def test_all_registered_asset_outputs_have_declared_schemas_when_empty(self) -> None:
        definitions = build_definitions(self.home)
        reverse_types = {value.key: model for model, value in TABLE_TYPES.items()}
        for definition in definitions.assets:
            for key in definition.keys:
                with self.subTest(asset=key.path):
                    output = definition.get_output_name_for_asset_key(key)
                    dtype = definition.node_def.output_def_named(output).dagster_type
                    if dtype.key == RULES_TYPE.key:
                        value = make_rules()
                        expected = set(type(value).model_fields)
                    else:
                        model = reverse_types[dtype.key]
                        expected = set(model.to_schema().columns)
                        value = pd.DataFrame(columns=list(model.to_schema().columns))
                    reference = self.manager.persist_value(value)

                    response = self.service(tuple(key.path), reference).query(
                        ArtifactQuery(asset_key=tuple(key.path))
                    )

                    self.assertEqual(response.data_schema.source, "declared")
                    schema = response.data_schema.schema_
                    self.assertEqual(set(schema["properties"]), expected)
                    Draft202012Validator.check_schema(schema)
                    self.assertEqual(response.snapshot_id, reference.digest)

    def test_empty_declared_table_keeps_lists_aliases_and_constraints(self) -> None:
        frame = pd.DataFrame(columns=list(ProposedAnnotationsTable.to_schema().columns))
        reference = self.manager.persist_value(frame)

        response = self.service(("datasets", "category_eligible_annotations"), reference).query(
            ArtifactQuery(asset_key=("datasets", "category_eligible_annotations"))
        )

        schema = response.data_schema.schema_
        self.assertEqual(response.page.total_rows, 0)
        self.assertEqual(schema["properties"]["transformations"], {"type": "array", "items": {"type": "string"}})
        self.assertEqual(schema["properties"]["class"]["enum"], ["allow", "deny", "ask"])
        self.assertNotIn("class_", schema["properties"])
        self.assertEqual(set(schema["required"]), set(frame.columns))
        self.assertFalse(schema["additionalProperties"])

    def test_declared_numeric_pattern_and_nullable_constraints_survive(self) -> None:
        decisions = declared_table_schema(HumanReviewDecisionsTable)
        nullable = declared_table_schema(NullableExample)

        self.assertTrue(decisions["properties"]["candidate_id"]["x-column-unique"])
        self.assertEqual(decisions["properties"]["generation"], {"type": "integer", "minimum": 1})
        self.assertEqual(decisions["properties"]["reviewer"]["pattern"], r"[\s\S]*\S")
        self.assertEqual(nullable["properties"]["labels"], {
            "anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "null"}],
        })

    def test_generic_empty_parquet_preserves_nested_storage_types_and_nullability(self) -> None:
        stored = arrow.schema([
            arrow.field("count", arrow.int64(), nullable=False),
            arrow.field("detail", arrow.struct([
                arrow.field("tags", arrow.list_(arrow.field("item", arrow.string(), nullable=False)), nullable=False),
            ]), nullable=True),
        ])
        with BytesIO() as output:
            parquet.write_table(arrow.Table.from_batches([], schema=stored), output)
            payload = output.getvalue()
        reference = AssetReference(format="parquet", digest=sha256(payload))
        ObjectStore(self.storage).write(f"objects/{reference.digest}.parquet", payload)

        response = self.service(("experiments", "nested"), reference).query(
            ArtifactQuery(asset_key=("experiments", "nested"))
        )

        self.assertEqual(response.data_schema.source, "storage")
        schema = response.data_schema.schema_
        self.assertEqual(response.page.rows, ())
        self.assertEqual(schema["properties"]["count"], {"type": "integer"})
        detail = schema["properties"]["detail"]["anyOf"][0]
        self.assertEqual(detail["properties"]["tags"], {"type": "array", "items": {"type": "string"}})
        self.assertFalse(detail["additionalProperties"])
        self.assertEqual(schema["required"], ["count", "detail"])

    def test_incompatible_historical_columns_remain_inspectable_as_storage(self) -> None:
        original = pd.DataFrame({"legacy": ["complete historical value"], "count": [4]})
        reference = self.manager.persist_value(original)
        key = ("datasets", "command_candidates")

        response = self.service(key, reference).query(ArtifactQuery(asset_key=key))

        self.assertEqual(response.data_schema.source, "storage")
        self.assertEqual(set(response.data_schema.schema_["properties"]), {"legacy", "count"})
        self.assertEqual(response.page.rows[0].values, {"legacy": "complete historical value", "count": 4})
        self.assertEqual(response.snapshot_id, reference.digest)

    def test_json_with_historical_fields_does_not_claim_current_rules_schema(self) -> None:
        payload = b'{"historical": {"complete": ["original"]}}'
        reference = AssetReference(format="rules", digest=sha256(payload))
        ObjectStore(self.storage).write(f"objects/{reference.digest}.json", payload)
        key = ("datasets", "category_rules")

        response = self.service(key, reference).query(ArtifactQuery(asset_key=key))

        self.assertEqual(response.data_schema.source, "storage")
        self.assertEqual(response.value, {"historical": {"complete": ["original"]}})

    def test_rules_table_query_and_corrupt_payload_still_fail(self) -> None:
        key = ("datasets", "category_rules")
        reference = self.manager.persist_value(make_rules())
        service = self.service(key, reference)
        with self.assertRaises(TableQueryError):
            service.query(ArtifactQuery(asset_key=key, query=TableQuery(search="policy")))

        malformed = AssetReference(format="parquet", digest="0" * 64)
        ObjectStore(self.storage).write(f"objects/{malformed.digest}.parquet", frame_bytes(pd.DataFrame({"x": [1]})))
        with self.assertRaises(IntegrityError):
            self.service(("arbitrary",), malformed).query(ArtifactQuery(asset_key=("arbitrary",)))

    def test_malformed_json_with_valid_digest_still_fails_parsing(self) -> None:
        payload = b'{"category_id":'
        reference = AssetReference(format="rules", digest=sha256(payload))
        ObjectStore(self.storage).write(f"objects/{reference.digest}.json", payload)
        key = ("datasets", "category_rules")

        with self.assertRaises(ValidationError):
            self.service(key, reference).query(ArtifactQuery(asset_key=key))
