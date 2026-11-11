import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from command_filter_cases import COMMAND_CASES

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from command_filter import assess_command, classify_command, extract_commands


ROOT = Path(__file__).resolve().parents[1]


class CommandFilterTests(unittest.TestCase):
    def test_actual_strategy_examples_extract_with_their_risk(self) -> None:
        examples = 0
        for risk, command in COMMAND_CASES:
            with self.subTest(command=command):
                records = list(extract_commands(command))
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0].command, command)
                self.assertIn(risk, records[0].risk_candidates)
                self.assertEqual(records[0].status, "eligible")
            examples += 1
        self.assertGreater(examples, 50)

    def test_quoted_sql_semicolons_and_hashes_stay_inside_command(self) -> None:
        command = """psql -c "SELECT ';', '#tag'; DROP TABLE old;" """

        records = list(extract_commands(command))

        self.assertEqual(records[0].command, command.strip())
        self.assertEqual(records[0].risk_candidates, ("CAT-3.4",))
        self.assertEqual(records[0].status, "eligible")

    def test_wrapped_flags_route_only_real_subcommand_positions(self) -> None:
        cases = [
            ("sudo -u postgres mysql -e 'DROP DATABASE old;'", ("CAT-3.4",)),
            ("env CLOUD=dev aws --profile prod cloudformation delete-stack --stack-name old", ("CAT-3.1",)),
            ("kubectl --context prod -n app apply -f deployment.yaml", ("CAT-3.5",)),
            ("kubectl -n app get deployments", ("CAT-3.2", "CAT-3.5")),
            ("gcloud --project sample container clusters list", ("CAT-3.2",)),
            ("gcloud --project sample storage rm gs://bucket --recursive", ("CAT-3.4",)),
            ("aws --region x s3 ls s3://bucket", ("CAT-3.4",)),
            ("docker --host unix:///run/docker.sock volume prune -f", ("CAT-3.3",)),
            ("helm --namespace sample diff upgrade app ./chart", ("CAT-3.5",)),
            ("echo 'terraform destroy; kubectl delete ns prod'", ()),
            ("aws configure set region cloudformation", ()),
            ("gcloud config set project storage", ()),
            ("kubectl explain delete", ()),
            ("terraform destroyer", ()),
            ("aws s3 cp help", ()),
            ("aws s3 rm help", ()),
            ("docker compose start", ()),
            ("docker compose logs", ()),
        ]
        for command, expected in cases:
            with self.subTest(command=command):
                self.assertEqual(classify_command(command), expected)

    def test_whole_compound_keeps_every_family_and_original_span(self) -> None:
        command = "terraform validate && kubectl delete ns old | xargs docker rm; aws s3 ls"
        text = "intro\n" + command + "\noutro"

        records = list(extract_commands(text))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].command, command)
        self.assertEqual(records[0].risk_candidates, ("CAT-3.1", "CAT-3.2", "CAT-3.3", "CAT-3.4"))
        self.assertEqual(text[records[0].start:records[0].end], command)

    def test_context_uses_original_character_offsets_after_stripping(self) -> None:
        raw = "- $ terraform validate"
        text = "я" * 130 + "\n```bash\n" + raw + "\n```\n" + "z" * 130

        record, = extract_commands(text)

        self.assertEqual(record.command, "terraform validate")
        self.assertEqual(text[record.start:record.end], raw)
        self.assertEqual(record.context, text[record.start - 100:record.end + 100])
        self.assertEqual(record.transformations, ("markdown_fence", "markdown_prefix", "shell_prompt"))

    def test_multiline_quotes_continuations_and_connectors_form_one_command(self) -> None:
        cases = [
            ("aws s3 rm \\\n  s3://bucket --recursive", "aws s3 rm s3://bucket --recursive"),
            ("psql -c 'SELECT 1;\nDROP TABLE old;'", "psql -c 'SELECT 1;\nDROP TABLE old;'"),
            ("terraform validate &&\nterraform plan", "terraform validate &&\nterraform plan"),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                record, = extract_commands(raw)
                self.assertEqual(record.command, expected)
                self.assertEqual(record.status, "eligible")
                self.assertEqual((record.start, record.end), (0, len(raw)))

    def test_prose_comments_and_foreign_fences_are_rejected(self) -> None:
        text = (
            "Use terraform destroy to delete the stack.\n"
            "terraform is an infrastructure tool.\n"
            "# terraform destroy\n"
            "```python\nterraform destroy\n```\n"
            "~~~sql\nDROP TABLE old;\n~~~\n"
            "`terraform validate`\n"
            "terraform plan # terraform destroy\n"
        )

        records = list(extract_commands(text))

        self.assertEqual([record.command for record in records], ["terraform validate", "terraform plan"])
        self.assertIn("comments_or_outer_whitespace", records[1].transformations)

    def test_opaque_and_malformed_shell_is_retained_for_review(self) -> None:
        cases = [
            "docker rm $(docker ps -aq)",
            "docker rm `docker ps -aq`",
            "bash -c 'terraform destroy'",
            "terraform destroy > logfile",
            "terraform destroy &&",
            "psql -c 'DROP TABLE old;",
            "psql <<'SQL'\nDROP TABLE old;\nSQL",
            "if true; then\nterraform destroy\nfi",
            "echo ready; terraform destroy",
        ]
        for command in cases:
            with self.subTest(command=command):
                record, = extract_commands(command)
                self.assertEqual(record.status, "needs_review")
                self.assertNotEqual(record.reason, "recognized_family")
                self.assertEqual((record.start, record.end), (0, len(command)))

    def test_literal_substitution_text_is_not_executed_or_expanded(self) -> None:
        command = "psql -c 'SELECT \"$(not_a_command)\";'"

        record, = extract_commands(command)

        self.assertEqual(record.command, command)
        self.assertEqual(record.status, "eligible")

    def test_observed_candidate_extensions_are_recognized(self) -> None:
        cases = [
            ("sam delete --stack-name app", "CAT-3.1"),
            ("serverless remove --stage dev", "CAT-3.1"),
            ("sst remove --stage production", "CAT-3.1"),
            ("incus list --format csv", "CAT-3.3"),
            ("coi kill --all --force", "CAT-3.3"),
            ("podman-compose down -v", "CAT-3.3"),
            ("rclone --min-size 100M delete remote:path", "CAT-3.4"),
            ("aws redshift describe-clusters", "CAT-3.4"),
            ("az postgres flexible-server db list --resource-group example", "CAT-3.4"),
            ("k3d cluster delete --all", "CAT-3.2"),
            ("crictl rm --all --force", "CAT-3.3"),
            ("kamal deploy -d production", "CAT-3.5"),
            ("vercel --prod", "CAT-3.5"),
            ("aws eks delete-cluster --name example", "CAT-3.2"),
        ]
        for command, risk in cases:
            with self.subTest(command=command):
                self.assertEqual(classify_command(command), (risk,))

    def test_assessment_preserves_structured_commands_exactly(self) -> None:
        cases = [
            (" \nterraform validate\n", "eligible", ("CAT-3.1",)),
            ("if true; then\n  kubectl delete ns old\nfi", "needs_review", ("CAT-3.2",)),
            ("docker rm $(docker ps -aq)", "needs_review", ("CAT-3.3",)),
            ("unrecognized program", "needs_review", ()),
        ]
        for command, status, risks in cases:
            with self.subTest(command=command):
                record = assess_command(command)
                self.assertEqual((record.command, record.context), (command, command))
                self.assertEqual((record.start, record.end), (0, len(command)))
                self.assertEqual((record.status, record.risk_candidates), (status, risks))

    def test_cli_writes_utf8_jsonl_without_executing_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, target, sentinel = root / "input.txt", root / "output.jsonl", root / "executed"
            source.write_text(f"terraform validate; touch {sentinel}\n", encoding="utf-8")

            result = subprocess.run(
                [sys.executable, "-B", str(ROOT / "scripts/command_filter.py"),
                 str(source), "--output", str(target)],
                check=False, capture_output=True, text=True, env=os.environ.copy(),
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            rows = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(rows[0]["status"], "needs_review")
            self.assertEqual(rows[0]["risk_candidates"], ["CAT-3.1"])
            self.assertFalse(sentinel.exists())


if __name__ == "__main__":
    unittest.main()
