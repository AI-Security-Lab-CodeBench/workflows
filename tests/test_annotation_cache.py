import json
import subprocess
import tempfile
import unittest
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from threading import Event, Lock
from unittest.mock import patch

from business_fixture import write_business
from pydantic import JsonValue, TypeAdapter
from test_dataset_annotation import config, documentation, native_response
from test_dataset_rules import make_candidate, make_label, make_review, make_rules

from codebench_pipeline.annotation_cache import AnnotationContextError, reviewer_cache_lock
from codebench_pipeline.business_config import load_business_rules
from codebench_pipeline.dataset_annotation import annotate, invocation_path, review
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.models import SemanticSnippet


class AnnotationCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.calls: list[tuple[str, tuple[str, ...]]] = []
        self.bad_identity = False
        self.inference_started: Event | None = None
        self.inference_release: Event | None = None
        self.enterContext(patch("codebench_pipeline.inference.subprocess.run", side_effect=self.provider))

    def provider(
        self,
        command: list[str],
        *,
        input: str,
        text: bool,
        capture_output: bool,
        cwd: str,
        env: dict[str, str],
        timeout: float,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        prompt = input.split("\nReturn only one complete JSON value conforming to this schema:\n", 1)[0]
        payload = TypeAdapter(dict[str, JsonValue]).validate_json(prompt[prompt.index("{") :])
        if "records" in payload:
            if self.inference_started is not None:
                self.inference_started.set()
                assert self.inference_release is not None
                self.assertTrue(self.inference_release.wait(timeout=10))
            records = TypeAdapter(list[dict[str, JsonValue]]).validate_python(payload["records"])
            candidates = [CandidateRecord.model_validate(row["candidate"]) for row in records]
            self.calls.append(("review", tuple(row.candidate_id for row in candidates)))
            reviews = [make_review(row).model_dump(mode="json") for row in candidates]
            if self.bad_identity:
                reviews[-1]["candidate_id"] = "unexpected"
            return native_response(
                TypeAdapter(dict[str, JsonValue]).validate_python({"reviews": reviews}),
                command[command.index("--model") + 1].split("/", 1)[1],
            )
        candidate = CandidateRecord.model_validate(payload["candidate"])
        self.calls.append(("label", (candidate.candidate_id,)))
        return native_response(
            make_label(candidate, make_rules()).model_dump(mode="json", by_alias=True),
            command[command.index("--model") + 1].split("/", 1)[1],
        )

    def test_adding_reordering_and_repacketizing_rows_reuses_original_labels_reviews_and_receipts(
        self,
    ) -> None:
        first, second = make_candidate("one"), make_candidate("two")
        cache = self.root / "cache"
        labels = annotate([first], documentation(), make_rules(), config(), self.root / "first", cache=cache)
        review([first], documentation(), labels, make_rules(), config(), self.root / "first", cache=cache)
        receipt = (invocation_path(self.root / "first", "review", "one") / "receipt.json").read_bytes()
        expanded = [first, second]
        labels = annotate(
            expanded, documentation(), make_rules(), config(), self.root / "expanded", cache=cache
        )
        review(expanded, documentation(), labels, make_rules(), config(), self.root / "expanded", cache=cache)
        unrelated = SemanticSnippet(
            utility="other",
            subcommands=["inspect"],
            url="https://example.org/other",
            retrieved_at="2026-11-11",
            snippet="An unrelated utility.",
            version_limits="fixture",
        )
        labels = annotate(
            list(reversed(expanded)),
            [*documentation(), unrelated],
            make_rules(),
            config(),
            self.root / "reordered",
            cache=cache,
        )
        review(
            list(reversed(expanded)),
            [*documentation(), unrelated],
            labels,
            make_rules(),
            config(),
            self.root / "reordered",
            cache=cache,
            batch_size=1,
        )
        self.assertEqual(
            self.calls,
            [
                ("label", ("one",)),
                ("review", ("one",)),
                ("label", ("two",)),
                ("review", ("two",)),
            ],
        )
        restored = invocation_path(self.root / "reordered", "review", "one")
        self.assertEqual((restored / "receipt.json").read_bytes(), receipt)
        self.assertTrue(json.loads((restored / "reuse.json").read_text())["cache_hit"])

    def test_changed_row_context_invalidates_only_that_rows_label_and_review(self) -> None:
        candidates = [make_candidate("one"), make_candidate("two")]
        cache = self.root / "cache"
        labels = annotate(
            candidates, documentation(), make_rules(), config(), self.root / "first", cache=cache
        )
        review(candidates, documentation(), labels, make_rules(), config(), self.root / "first", cache=cache)
        candidates[0] = candidates[0].model_copy(update={"context": "Changed adjacent source context."})
        self.calls.clear()
        labels = annotate(
            candidates, documentation(), make_rules(), config(), self.root / "changed", cache=cache
        )
        review(
            candidates, documentation(), labels, make_rules(), config(), self.root / "changed", cache=cache
        )
        self.assertEqual(self.calls, [("label", ("one",)), ("review", ("one",))])

    def test_business_edits_invalidate_labels_but_unselected_examples_and_neighbors_reuse_receipts(self) -> None:
        business = write_business(self.root / "business")
        candidate = make_candidate("one")
        cache = self.root / "cache"
        rules = load_business_rules(business)
        annotate([candidate], documentation(), rules, config(), self.root / "first", cache=cache)
        receipt = (invocation_path(self.root / "first", "label", "one") / "receipt.json").read_bytes()
        examples_file = business.parent / "examples.json"
        examples = json.loads(examples_file.read_text())
        examples[1]["examples"][0]["expected"]["rationale"] = "Changed unrelated example judgment."
        examples_file.write_text(json.dumps(examples))
        annotate(
            [candidate, make_candidate("neighbor")], documentation(), load_business_rules(business),
            config(), self.root / "neighbor", cache=cache,
        )
        self.assertEqual(self.calls, [("label", ("one",)), ("label", ("neighbor",))])
        reused = invocation_path(self.root / "neighbor", "label", "one")
        self.assertEqual((reused / "receipt.json").read_bytes(), receipt)
        self.assertTrue(json.loads((reused / "reuse.json").read_text())["cache_hit"])
        for index, file in enumerate((business.parent / "base.md", business.parent / "instructions.md")):
            file.write_text(file.read_text() + "\nEdited business instructions.")
            annotate(
                [candidate], documentation(), load_business_rules(business), config(),
                self.root / f"policy-{index}", cache=cache,
            )
        examples[0]["examples"][0]["expected"]["rationale"] = "Changed selected example judgment."
        examples_file.write_text(json.dumps(examples))
        annotate(
            [candidate], documentation(), load_business_rules(business), config(),
            self.root / "selected", cache=cache,
        )
        self.assertEqual(
            self.calls,
            [("label", ("one",)), ("label", ("neighbor",))] + [("label", ("one",))] * 3,
        )

    def test_reviewer_packets_limit_actual_request_count_and_serialized_context(self) -> None:
        candidates = [make_candidate(str(index)) for index in range(5)]
        labels = [make_label(candidate, make_rules()) for candidate in candidates]
        review(candidates, documentation(), labels, make_rules(), config(), self.root, batch_size=2)
        self.assertEqual(
            self.calls,
            [
                ("review", ("0", "1")),
                ("review", ("2", "3")),
                ("review", ("4",)),
            ],
        )
        for request in self.root.glob("review-packets/*/request.json"):
            self.assertLessEqual(len(request.read_bytes().rstrip(b"\n")), 131072)

    def test_label_documentation_rules_and_model_changes_invalidate_only_affected_review_inputs(self) -> None:
        candidates = [make_candidate("one"), make_candidate("two")]
        rules = make_rules()
        labels = [make_label(candidate, rules) for candidate in candidates]
        cache = self.root / "cache"
        review(candidates, documentation(), labels, rules, config(), self.root / "baseline", cache=cache)
        for variant, expected in (
            ("label", ("one",)),
            ("documentation", ("one",)),
            ("rules", ("one", "two")),
            ("model", ("one", "two")),
        ):
            with self.subTest(variant=variant):
                self.calls.clear()
                changed_labels = list(labels)
                changed_rules = rules
                changed_model = config()
                row_docs = {"one": documentation(), "two": documentation()}
                if variant == "label":
                    changed_labels[0] = labels[0].model_copy(
                        update={"description": "Another valid description."}
                    )
                elif variant == "documentation":
                    row_docs["one"] = [
                        documentation()[0].model_copy(update={"snippet": "Changed reference text."})
                    ]
                elif variant == "rules":
                    changed_rules = rules.model_copy(update={"policy": "Changed evidence policy."})
                else:
                    changed_model = config().model_copy(update={"model": "fixture/another-model"})
                review(
                    candidates,
                    documentation(),
                    changed_labels,
                    changed_rules,
                    changed_model,
                    self.root / variant,
                    cache=cache,
                    documentation_by_candidate=row_docs,
                )
                self.assertEqual(self.calls, [("review", expected)])

    def test_context_limit_splits_packets_and_rejects_oversized_record_without_truncating(self) -> None:
        rules = make_rules().model_copy(update={"base_prompt": "Fixture labeling instructions."})
        candidates = [
            make_candidate(str(index)).model_copy(update={"context": "x" * 8000}) for index in range(3)
        ]
        labels = [make_label(candidate, rules) for candidate in candidates]
        review(
            candidates,
            documentation(),
            labels,
            rules,
            config(),
            self.root / "bounded",
            batch_size=64,
            maximum_context_characters=15000,
        )
        self.assertEqual(self.calls, [("review", ("0",)), ("review", ("1",)), ("review", ("2",))])
        for request in (self.root / "bounded").glob("review-packets/*/request.json"):
            self.assertLessEqual(len(request.read_bytes().rstrip(b"\n")), 15000)
        oversized = candidates[0].model_copy(update={"context": "y" * 30000})
        before = list(self.calls)
        with self.assertRaises(AnnotationContextError):
            review(
                [oversized],
                documentation(),
                [make_label(oversized, rules)],
                rules,
                config(),
                self.root / "oversized",
                maximum_context_characters=15000,
            )
        self.assertEqual(self.calls, before)

    def test_invalid_packet_identity_never_caches_any_partial_row(self) -> None:
        candidates = [make_candidate("one"), make_candidate("two")]
        labels = [make_label(candidate, make_rules()) for candidate in candidates]
        cache = self.root / "cache"
        self.bad_identity = True
        with self.assertRaisesRegex(ValueError, "identities"):
            review(
                candidates,
                documentation(),
                labels,
                make_rules(),
                config(),
                self.root / "invalid",
                cache=cache,
            )
        self.bad_identity = False
        review(candidates, documentation(), labels, make_rules(), config(), self.root / "valid", cache=cache)
        self.assertEqual(self.calls, [("review", ("one", "two")), ("review", ("one", "two"))])

    def test_concurrent_review_runs_recheck_cache_after_partition_lock(self) -> None:
        self.inference_started = Event()
        self.inference_release = Event()
        second_attempt = Event()
        attempts_lock = Lock()
        attempts = 0
        cache = self.root / "cache"
        candidates = [make_candidate("one")]
        labels = [make_label(candidates[0], make_rules())]

        @contextmanager
        def observed_lock(path: Path | None) -> Generator[None]:
            nonlocal attempts
            with attempts_lock:
                attempts += 1
                if attempts == 2:
                    second_attempt.set()
            with reviewer_cache_lock(path):
                yield

        with (
            patch("codebench_pipeline.dataset_annotation.reviewer_cache_lock", observed_lock),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            first = executor.submit(
                review,
                candidates,
                documentation(),
                labels,
                make_rules(),
                config(),
                self.root / "first",
                cache=cache,
            )
            self.assertTrue(self.inference_started.wait(timeout=10))
            second = executor.submit(
                review,
                candidates,
                documentation(),
                labels,
                make_rules(),
                config(),
                self.root / "second",
                cache=cache,
            )
            try:
                self.assertTrue(second_attempt.wait(timeout=10))
            finally:
                self.inference_release.set()
            self.assertEqual(first.result(timeout=10), second.result(timeout=10))
        self.assertEqual(self.calls, [("review", ("one",))])
