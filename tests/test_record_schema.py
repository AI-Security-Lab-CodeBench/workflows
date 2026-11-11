import unittest

from pydantic import BaseModel, ValidationError

from codebench_pipeline.dataset_annotation import candidate_schema
from codebench_pipeline.dataset_records import LabelDecision, ReviewDecision


class RecordJudgment(BaseModel):
    candidate_id: str
    accepted: bool


class RecordSchemaTests(unittest.TestCase):
    def test_bound_judgment_schemas_require_enum_confidences(self) -> None:
        for model, fields in (
            (
                LabelDecision,
                ("category_confidence", "risk_confidence", "severity_confidence", "class_confidence"),
            ),
            (ReviewDecision, ("confidence",)),
        ):
            schema = candidate_schema(model, "record-a").model_json_schema()
            for field in fields:
                with self.subTest(model=model.__name__, field=field):
                    self.assertIn(field, schema["required"])
                    self.assertEqual(schema["properties"][field]["type"], "string")
                    self.assertEqual(schema["properties"][field]["enum"], ["low", "medium", "high"])
                    self.assertNotIn("default", schema["properties"][field])

    def test_inference_schema_binds_the_exact_record_identity(self) -> None:
        identity = "94007c991a0fa39f1272e8efcd76396330b2094ded9443bef6339e6bc7c261e1"
        bound = candidate_schema(RecordJudgment, identity)
        result = bound.model_validate_json(f'{{"candidate_id":"{identity}","accepted":true}}')
        self.assertEqual(result.model_dump(mode="json"), {"candidate_id": identity, "accepted": True})
        for wrong in (identity[:-1], identity + "1", "another-record"):
            with self.subTest(identity=wrong), self.assertRaises(ValidationError):
                bound.model_validate_json(f'{{"candidate_id":"{wrong}","accepted":true}}')
        schema = bound.model_json_schema()
        self.assertEqual(schema["$defs"]["CandidateIdentity"]["enum"], [identity])

    def test_non_hash_identifiers_remain_supported_without_cross_record_reuse(self) -> None:
        first = candidate_schema(RecordJudgment, "record-a")
        second = candidate_schema(RecordJudgment, "record-b")
        payload = '{"candidate_id":"record-a","accepted":false}'
        self.assertEqual(first.model_validate_json(payload).candidate_id, "record-a")
        with self.assertRaises(ValidationError):
            second.model_validate_json(payload)
