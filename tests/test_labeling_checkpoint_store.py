import json
from base64 import urlsafe_b64encode
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.batch_labeling_rows import BatchRow, RowEvidence
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.labeling_checkpoint_io import labeling_generation_identity
from codebench_pipeline.labeling_checkpoint_store import LabelingCheckpointCache
from codebench_pipeline.labeling_outcomes import label_attempt
from codebench_pipeline.object_store import ObjectStore, StorageConfig
from codebench_pipeline.stage_implementation import ImplementationSource, capture_implementation
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.typed_config import TypedConfig


class LabelingCheckpointStoreTests(TestCase):
    def test_v1_checkpoint_preserves_batch_and_receipt_byte_addresses(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        cache = LabelingCheckpointCache(home / "cache", home / "operations")
        store = ObjectStore(StorageConfig(local_root=str(cache.root), prefix="labeling-checkpoints-v1"))
        generation, batch, other_batch = "a" * 64, "b" * 64, "c" * 64
        candidate = make_candidate()
        docs = documentation()
        attempt = label_attempt(
            candidate, make_label(candidate, make_rules()), docs, "d" * 64, "fixture/receipt.json"
        )
        evidence = RowEvidence(evidence_id="fixture", snippet=docs[0])
        binary = b"\xff\x00"
        row = BatchRow(attempt, [evidence], [], {"fixture.bin": binary})
        operation = b'{"receipt":"fixture"}'
        operation_address = f"shared-operations/{sha256(operation).hexdigest()}.json"
        old_payload = json.dumps(
            {
                "generation": generation,
                "batch": batch,
                "rows": [
                    {
                        "attempt": attempt.model_dump(mode="json"),
                        "evidence": [evidence.model_dump(mode="json")],
                        "queries": [],
                        "files": {"fixture.bin": urlsafe_b64encode(binary).decode("ascii")},
                    }
                ],
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()

        with cache.claim(generation, batch):
            cache.complete(generation, batch, [row], {operation_address: operation})

        self.assertEqual(store.read(f"generations/{generation}/{batch}.json"), old_payload)
        self.assertEqual((cache.operation_root / operation_address).read_bytes(), operation)
        self.assertEqual(cache.resume(generation, batch), [row])
        self.assertIsNone(cache.resume(generation, other_batch))
        store.write(f"generations/{generation}/{other_batch}.json", old_payload)
        with self.assertRaises(IntegrityError):
            cache.resume(generation, other_batch)

    def test_checkpoint_serialization_changes_bound_labeling_generation(self) -> None:
        class HexCheckpointSource(ImplementationSource):
            def read(self, name: str) -> bytes:
                payload = super().read(name)
                if name == "codebench_pipeline/labeling_checkpoint_store.py":
                    return payload.replace(b'ser_json_bytes="base64"', b'ser_json_bytes="hex"')
                return payload

        partition = "checkpoint-fixture"
        stage = "labeling_attempts"
        config = TypedConfig()
        original, changed = ImplementationSource(), HexCheckpointSource()
        before = stage_signature(stage, config, {}, source=original)
        after = stage_signature(stage, config, {}, source=changed)

        self.assertNotEqual(
            labeling_generation_identity(partition, before, "/fixture/model"),
            labeling_generation_identity(partition, after, "/fixture/model"),
        )
        self.assertNotEqual(
            capture_implementation(original).stages[stage], capture_implementation(changed).stages[stage]
        )
