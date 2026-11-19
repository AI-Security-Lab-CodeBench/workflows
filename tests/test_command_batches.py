import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_batch_labeling import batch_config, remote_model, submitted, tool_reply
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_rules

from codebench_pipeline.annotation_cache import reviewer_cache_lock
from codebench_pipeline.batch_labeling import annotate_batches
from codebench_pipeline.command_batches import similarity_batches
from codebench_pipeline.command_embeddings import ENCODER_DIRECTORY_ENV, CommandEncoderError


class CommandBatchTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get(ENCODER_DIRECTORY_ENV), "requires the pinned offline command encoder")
    def test_semantics_outweigh_shared_utility_and_source_context(self) -> None:
        anchor = make_candidate("a").model_copy(update={"command": "getent passwd", "context": "hostnames"})
        decoy = make_candidate("b").model_copy(update={"command": "getent hosts", "context": "hostnames"})
        paraphrase = make_candidate("c").model_copy(
            update={"command": "cat /etc/passwd", "context": "fixture"}
        )
        self.assertEqual(similarity_batches([decoy, paraphrase, anchor], 2), [[anchor, paraphrase], [decoy]])
        self.assertEqual(similarity_batches([anchor, decoy, paraphrase], 2), [[anchor, paraphrase], [decoy]])

    @unittest.skipUnless(os.environ.get(ENCODER_DIRECTORY_ENV), "requires the pinned offline command encoder")
    def test_similarity_groups_related_operations_without_merging_different_flags(self) -> None:
        candidates = [
            make_candidate(identity).model_copy(update={"command": command})
            for identity, command in (
                ("a", "docker container prune"),
                ("b", "psql database -c 'DROP TABLE invoices'"),
                ("c", "docker container prune --force"),
                ("d", "psql database -c 'SELECT * FROM invoices'"),
            )
        ]
        grouped = similarity_batches(candidates, 2)
        self.assertEqual([[row.candidate_id for row in batch] for batch in grouped], [["a", "c"], ["b", "d"]])
        self.assertEqual(
            {row.candidate_id: row.command for batch in grouped for row in batch},
            {row.candidate_id: row.command for row in candidates},
        )

    @unittest.skipUnless(os.environ.get(ENCODER_DIRECTORY_ENV), "requires the pinned offline command encoder")
    def test_reordering_preserves_ties_bound_and_every_record(self) -> None:
        candidates = [make_candidate(f"{index:02d}") for index in range(51)]
        result = similarity_batches(candidates, 50)
        self.assertEqual(result, similarity_batches(list(reversed(candidates)), 50))
        self.assertEqual(result, [candidates[:50], candidates[50:]])
        for observed, original in zip((row for batch in result for row in batch), candidates, strict=True):
            self.assertIs(observed, original)

    @unittest.skipUnless(os.environ.get(ENCODER_DIRECTORY_ENV), "requires the pinned offline command encoder")
    def test_only_cache_misses_are_grouped_and_full_replay_needs_no_encoder(self) -> None:
        def provider(base_url, body, key, timeout, config, out):
            records = json.loads(json.loads(body)["messages"][1]["content"])["records"]
            return tool_reply(
                "submit_labels",
                {"outcomes": [submitted(row, row["evidence_ids"]) for row in records]},
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            a, b, c, d = [make_candidate(identity) for identity in "abcd"]
            config = batch_config(root).model_copy(update={"batch_size": 2})
            args = documentation(), make_rules(), remote_model(), config
            cache = root / "cache"
            with patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider):
                original = annotate_batches([a, c], *args, root / "original", cache=cache)
                expanded = annotate_batches([d, c, b, a], *args, root / "expanded", cache=cache)
            packets = json.loads((root / "expanded" / "label-packets.json").read_text(encoding="utf-8"))
            self.assertEqual(packets["cache_hits"], 2)
            self.assertEqual(packets["packets"], [["b", "d"]])
            self.assertEqual([row.status for row in expanded], ["success"] * 4)
            self.assertEqual([expanded[3], expanded[1]], original)
            with (
                patch.dict(os.environ, {ENCODER_DIRECTORY_ENV: ""}),
                patch(
                    "codebench_pipeline.batch_labeling.guarded_complete",
                    side_effect=AssertionError("Full cache replay must not invoke inference"),
                ),
            ):
                replayed = annotate_batches([a, b, c, d], *args, root / "replayed", cache=cache)
            self.assertEqual(replayed, list(reversed(expanded)))

    def test_missing_or_tampered_artifact_cannot_fall_back_to_lexical_grouping(self) -> None:
        candidates = [make_candidate("a"), make_candidate("b")]
        with patch.dict(os.environ, {ENCODER_DIRECTORY_ENV: ""}):
            with (
                self.assertRaisesRegex(CommandEncoderError, "offline command encoder directory"),
                reviewer_cache_lock(None),
            ):
                similarity_batches(candidates, 2)
            self.assertEqual(
                similarity_batches(list(reversed(candidates)), 1), [[candidates[0]], [candidates[1]]]
            )
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch.dict(os.environ, {ENCODER_DIRECTORY_ENV: temporary}),
        ):
            with self.assertRaisesRegex(CommandEncoderError, "Cannot read encoder artifact"):
                similarity_batches(candidates, 2)
            (Path(temporary) / "tokenizer.json").write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(CommandEncoderError, "SHA-256 mismatch"):
                similarity_batches(candidates, 2)

    def test_empty_singleton_invalid_bound_and_duplicate_identity_are_explicit(self) -> None:
        self.assertEqual(similarity_batches([], 25), [])
        candidate = make_candidate("single").model_copy(update={"command": ""})
        self.assertEqual(similarity_batches([candidate], 25), [[candidate]])
        for batch_size in (0, 51):
            with self.subTest(batch_size=batch_size), self.assertRaises(ValueError):
                similarity_batches([candidate], batch_size)
        with self.assertRaises(ValueError):
            similarity_batches([candidate, candidate], 25)
