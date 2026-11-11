import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from shellrisk_audit_adapters import Extracted, normalize, reconstruct, trajectory_commands


class ShellRiskAuditTests(unittest.TestCase):
    def test_sessions_consume_ordinals_before_command_selection(self):
        commands = iter([
            Extracted("printf first", {"line": 1}),
            Extracted("printf first", {"line": 2}),
            Extracted("printf session\nprintf second", {"line": 3}),
            Extracted("printf last", {"line": 4}),
        ])

        result = list(reconstruct("terminalbench", commands))

        self.assertEqual([(ordinal, text) for ordinal, text, _ in result],
                         [(0, "printf first"), (2, "printf last")])

    def test_distinct_originals_keep_ordinals_when_normalization_collides(self):
        commands = iter([
            Extracted("curl https://first.example/path", {"line": 1}),
            Extracted("curl https://second.example/path", {"line": 2}),
        ])

        result = list(reconstruct("terminalbench", commands))

        self.assertEqual([(ordinal, text) for ordinal, text, _ in result],
                         [(0, "curl http://example.com"), (1, "curl http://example.com")])

    def test_shape_cap_does_not_consume_a_source_ordinal(self):
        commands = iter(
            [Extracted(f"cat file{number}.txt", {"line": number}) for number in range(7)]
            + [Extracted("printf complete", {"line": 8})]
        )

        result = list(reconstruct("swesmith", commands))

        self.assertEqual([(ordinal, text) for ordinal, text, _ in result],
                         [(number, f"cat file{number}.txt") for number in range(5)]
                         + [(5, "printf complete")])

    def test_normalization_preserves_shell_structure(self):
        command = "curl 'https://host.example/path'; echo 192.0.2.1 " + "A" * 40

        result = normalize(command)

        self.assertEqual(result, "curl 'http://example.com'; echo 1.1.1.1 BASE64")

    def test_trajectory_extraction_keeps_only_literal_bash_calls(self):
        rows = iter([
            ("https://example.invalid/source", 4, {
                "trial_id": "",
                "steps": '[{"tools":[{"fn":"execute_ipython_cell","cmd":"print(1)"},'
                         '{"fn":"execute_bash","cmd":"$12"},'
                         '{"fn":"execute_bash","cmd":"  printf literal  "}]}]',
            }),
        ])

        result = list(trajectory_commands("terminalbench", rows))

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].command, "printf literal")
        self.assertEqual(result[0].locator["row"], 4)
        self.assertEqual(result[0].locator["tool_index"], 2)
        self.assertIsNone(result[0].locator["trial_id_sha256"])


if __name__ == "__main__":
    unittest.main()
