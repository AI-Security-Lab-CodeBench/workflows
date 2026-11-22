import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_batch_labeling import QUOTE, URL, batch_config, live_mcp_fixture, remote_model, row_retrieval
from test_dataset_rules import make_candidate, make_rules

from codebench_pipeline.batch_labeling import (
    BatchExecution,
    LabelingGeneration,
    prepare_batch_rows,
    run_prepared_batches,
)
from codebench_pipeline.command_batches import command_family_batches
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding_tools import GroundingCredentials
from codebench_pipeline.labeling_checkpoint_store import LabelingCheckpointCache
from codebench_pipeline.labeling_outcomes import LabelingAttempt
from codebench_pipeline.storage import fingerprint


class CommandBatchTests(unittest.TestCase):
    def test_wrappers_and_global_options_keep_related_command_prefixes_together(self) -> None:
        candidates = [
            make_candidate(identity).model_copy(update={"command": command, "util": "docker"})
            for identity, command in (
                ("a", "docker --context staging container ls"),
                ("b", "docker image ls"),
                ("c", "env REGION=test sudo /usr/bin/docker container prune --force"),
                ("d", "docker --context=staging image prune"),
            )
        ]
        result = command_family_batches(candidates, 2)
        self.assertEqual([[row.candidate_id for row in batch] for batch in result], [["a", "c"], ["b", "d"]])
        self.assertEqual(result, command_family_batches(list(reversed(candidates)), 2))

    def test_grouping_preserves_quoted_operands_flags_and_every_record(self) -> None:
        candidates = [
            make_candidate(identity).model_copy(update={"command": command, "util": utility})
            for identity, utility, command in (
                ("a", "docker", "docker container prune"),
                ("b", "psql", "psql database -c 'DROP TABLE invoices; SELECT 1'"),
                ("c", "docker", "docker container prune --force"),
                ("d", "psql", "psql database -c 'SELECT * FROM invoices'"),
            )
        ]
        grouped = command_family_batches(candidates, 2)
        self.assertEqual([[row.candidate_id for row in batch] for batch in grouped], [["a", "c"], ["b", "d"]])
        self.assertEqual(
            {row.candidate_id: row.command for batch in grouped for row in batch},
            {row.candidate_id: row.command for row in candidates},
        )

    def test_reordering_preserves_ties_bound_and_every_record(self) -> None:
        candidates = [make_candidate(f"{index:02d}") for index in range(51)]
        result = command_family_batches(candidates, 50)
        self.assertEqual(result, command_family_batches(list(reversed(candidates)), 50))
        self.assertEqual(result, [candidates[:50], candidates[50:]])
        for observed, original in zip((row for batch in result for row in batch), candidates, strict=True):
            self.assertIs(observed, original)

    def test_changed_generation_relabels_all_rows_and_full_replay_needs_no_provider(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            a, b, c, d = [make_candidate(identity) for identity in "abcd"]
            config = batch_config(root).model_copy(update={"row_concurrency": 2})
            rules, model = make_rules(), remote_model()
            cache = LabelingCheckpointCache(root / "cache", root / "operations")

            def run(rows: list[CandidateRecord], stage: str, scope: str) -> list[LabelingAttempt]:
                identity = fingerprint(scope)
                prepared = prepare_batch_rows(rows, rules, model, config, implementation=identity)
                return run_prepared_batches(
                    rows,
                    prepared,
                    rules,
                    model,
                    config,
                    root / stage,
                    BatchExecution(None, grounding=GroundingCredentials.from_environment(config.grounding)),
                    100_000,
                    LabelingGeneration(cache, identity),
                )

            with (
                live_mcp_fixture({URL: QUOTE}) as calls,
                patch(
                    "codebench_pipeline.batch_labeling.guarded_complete",
                    side_effect=lambda _url, body, *_: row_retrieval(body),
                ),
            ):
                original = run([a, c], "original", "original inputs")
                expanded = run([d, c, b, a], "expanded", "expanded inputs")
                self.assertCountEqual(
                    [name for name, _ in calls], ["search_web_pages", "fetch_page_content"] * 6
                )
            membership = [
                json.loads(path.read_bytes())["candidate_ids"]
                for path in (root / "expanded").glob("batches/*/transcript.json")
            ]
            self.assertCountEqual(membership, [["a"], ["b"], ["c"], ["d"]])
            self.assertEqual([row.status for row in expanded], ["success"] * 4)
            self.assertNotEqual(expanded[3].row_input_fingerprint, original[0].row_input_fingerprint)
            with (
                patch(
                    "codebench_pipeline.batch_labeling.guarded_complete",
                    side_effect=AssertionError("Full checkpoint replay must not invoke inference"),
                ) as inference,
                live_mcp_fixture({}) as replay_calls,
            ):
                replayed = run([a, b, c, d], "replayed", "expanded inputs")
            self.assertEqual(replayed, list(reversed(expanded)))
            inference.assert_not_called()
            self.assertEqual(replay_calls, [])

    def test_unparseable_commands_remain_inert_and_are_not_dropped(self) -> None:
        candidates = [
            make_candidate("broken").model_copy(update={"command": "docker 'unterminated", "util": "docker"}),
            make_candidate("dynamic").model_copy(
                update={"command": "docker inspect $(touch /never-execute-this)", "util": "docker"}
            ),
        ]
        with patch.dict(os.environ, {"CODEBENCH_COMMAND_ENCODER_DIR": "/missing"}):
            self.assertEqual(command_family_batches(candidates, 2), [candidates])

    def test_empty_singleton_invalid_bound_and_duplicate_identity_are_explicit(self) -> None:
        self.assertEqual(command_family_batches([], 25), [])
        candidate = make_candidate("single").model_copy(update={"command": ""})
        self.assertEqual(command_family_batches([candidate], 25), [[candidate]])
        for batch_size in (0, 51):
            with self.subTest(batch_size=batch_size), self.assertRaises(ValueError):
                command_family_batches([candidate], batch_size)
        with self.assertRaises(ValueError):
            command_family_batches([candidate, candidate], 25)
