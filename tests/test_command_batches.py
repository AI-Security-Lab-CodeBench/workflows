import unittest

from test_dataset_rules import make_candidate

from codebench_pipeline.command_batches import similarity_batches


class CommandBatchTests(unittest.TestCase):
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

    def test_reordering_preserves_groups_and_short_tail_retains_every_record(self) -> None:
        candidates = [make_candidate(str(index)) for index in range(7)]
        result = similarity_batches(candidates, 3)
        self.assertEqual(result, similarity_batches(list(reversed(candidates)), 3))
        self.assertEqual([len(batch) for batch in result], [3, 3, 1])
        self.assertEqual(
            sorted(row.candidate_id for batch in result for row in batch),
            [str(index) for index in range(7)],
        )

    def test_empty_zero_vector_and_invalid_identity_inputs_are_explicit(self) -> None:
        self.assertEqual(similarity_batches([], 25), [])
        candidate = make_candidate("single").model_copy(update={"command": ""})
        self.assertEqual(similarity_batches([candidate], 25), [[candidate]])
        for batch_size in (0, 51):
            with self.subTest(batch_size=batch_size), self.assertRaises(ValueError):
                similarity_batches([candidate], batch_size)
        with self.assertRaises(ValueError):
            similarity_batches([candidate, candidate], 25)
