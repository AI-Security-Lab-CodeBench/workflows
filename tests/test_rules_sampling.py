from collections import Counter
from pathlib import Path

import pytest
from dagster import AssetKey
from pydantic import TypeAdapter, ValidationError

from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.rules_contracts import Labels, ModifyGold
from codebench_pipeline.rules_corpus import Corpus, CorpusRow
from codebench_pipeline.rules_registration_config import (
    DEFAULT_SAMPLE,
    SampleCount,
    SamplePercent,
    SampleSelection,
)
from codebench_pipeline.rules_sample_assets import sample_key
from codebench_pipeline.rules_sampling import sample_corpus
from codebench_pipeline.storage import IntegrityError


def _train() -> Corpus:
    labels = (
        *(Labels(category="CAT-6", risk="CAT-6.1", severity="low", action="allow") for _ in range(6)),
        *(Labels(category="CAT-6", risk="CAT-6.2", severity="high", action="deny") for _ in range(2)),
        *(Labels(category="CAT-7", risk="CAT-7.1", severity="low", action="ask") for _ in range(2)),
    )
    return Corpus(split="train", digest="a" * 64, rows=tuple(
        CorpusRow(id=f"train:{index}", command="synthetic", gold=label) for index, label in enumerate(labels)
    ))


def test_global_count_stratifies_and_selects_stable_row_ids() -> None:
    source = _train()
    reversed_source = source.model_copy(update={"rows": source.rows[::-1]})
    sample = sample_corpus(source, SampleCount(count=5), required_categories=("CAT-6",))
    assert sample == sample_corpus(reversed_source, SampleCount(count=5), required_categories=("CAT-6",))
    assert sample.pin.row_count == 5
    assert len({row.id for row in sample.corpus.rows}) == 5
    assert Counter(row.gold.risk for row in sample.corpus.rows) == {"CAT-6.1": 3, "CAT-6.2": 1, "CAT-7.1": 1}
    projection = sample.for_capture("CAT-6")
    assert projection.digest == sample.pin.digest
    assert projection.rows == tuple(row for row in sample.corpus.rows if row.gold.category == "CAT-6")
    assert sample.pin != sample_corpus(source, SampleCount(count=5), required_categories=("CAT-7",)).pin
    assert sample_corpus(source, DEFAULT_SAMPLE).pin.row_count == 10


def test_insufficient_train_budget_does_not_silently_omit_a_capture_category() -> None:
    source = _train()
    with pytest.raises(IntegrityError, match="cannot cover all registered capture categories"):
        sample_corpus(source, SampleCount(count=1), required_categories=("CAT-6", "CAT-7"))
    sample = sample_corpus(source, SampleCount(count=1), required_categories=("CAT-7",))
    (row,) = sample.corpus.rows
    assert row.gold.category == "CAT-7"
    assert sample.corpus.rows == sample_corpus(
        source, SamplePercent(percent=100), required_categories=("CAT-7",),
    ).corpus.rows[:1]
    with pytest.raises(IntegrityError, match="cannot cover all registered capture categories"):
        sample_corpus(source, SamplePercent(percent=100), required_categories=("CAT-9",))


def test_default_caps_each_split_once_at_200() -> None:
    train = _train()
    sources = (
        train,
        train.model_copy(update={"split": "test", "digest": "b" * 64}),
    )
    for source in sources:
        rows = tuple(
            row.model_copy(update={"id": f"{source.split}:{batch}:{index}"})
            for batch in range(24) for index, row in enumerate(source.rows)
        )
        sample = sample_corpus(source.model_copy(update={"rows": rows}), DEFAULT_SAMPLE)
        assert sample.pin.row_count == 200
        assert Counter(row.gold.category for row in sample.corpus.rows) == {"CAT-6": 160, "CAT-7": 40}


def test_holdout_percent_keeps_unseen_categories_and_modify_gold() -> None:
    categories = ("CAT-1", "CAT-2", "CAT-3", "CAT-4", "CAT-5", "CAT-6")
    rows = tuple(
        CorpusRow(
            id=f"test:{category}:{index}", command="synthetic",
            gold=ModifyGold(category=category, risk=f"{category}.1", severity="low", action="modify"),
        )
        for category in categories for index in range(2)
    )
    source = Corpus(split="test", digest="b" * 64, rows=rows)
    half = sample_corpus(source, SamplePercent(percent=50))
    assert Counter(row.gold.category for row in half.corpus.rows) == dict.fromkeys(categories, 1)
    assert all(row.gold.action == "modify" for row in half.corpus.rows)
    assert sample_corpus(source, SamplePercent(percent=50.1)).pin.row_count == 7
    whole = sample_corpus(source, SamplePercent(percent=100))
    assert set(row.id for row in whole.corpus.rows) == set(row.id for row in rows)
    assert whole.corpus.rows[:6] == half.corpus.rows
    assert whole.pin.digest != source.digest and whole.pin != half.pin
    assert whole.pin.digest != sample_corpus(source, DEFAULT_SAMPLE).pin.digest


def test_imbalanced_split_uses_one_stratified_permutation_of_ordinal_ids() -> None:
    strata = (
        (Labels(category="CAT-6", risk="CAT-6.1", severity="low", action="allow"), 62),
        (Labels(category="CAT-6", risk="CAT-6.1", severity="high", action="deny"), 17),
        (Labels(category="CAT-6", risk="CAT-6.1", severity="low", action="ask"), 1),
        (Labels(category="CAT-7", risk="CAT-7.1", severity="high", action="deny"), 19),
        (Labels(category="CAT-8", risk="CAT-8.1", severity="low", action="ask"), 1),
    )
    rows = tuple(
        CorpusRow(id=f"train:{index:03d}", command="synthetic", gold=label)
        for index, label in enumerate(label for label, size in strata for _ in range(size))
    )
    source = Corpus(split="train", digest="a" * 64, rows=rows)
    reversed_source = source.model_copy(update={"rows": rows[::-1]})
    selections = (SampleCount(count=10), SampleCount(count=25), SamplePercent(percent=100))
    results = tuple(sample_corpus(source, selection, required_categories=("CAT-6",)) for selection in selections)
    assert results == tuple(
        sample_corpus(reversed_source, selection, required_categories=("CAT-6",)) for selection in selections
    )
    small, medium, whole = (sample.corpus.rows for sample in results)
    assert small == medium[:10] == whole[:10]
    assert medium == whole[:25]
    assert sample_corpus(source, SamplePercent(percent=25), required_categories=("CAT-6",)).corpus.rows == medium
    assert {row.id for row in whole} == {row.id for row in rows}
    assert len(whole) == len(rows)
    assert whole != rows
    assert tuple(row.id for row in small) != tuple(sorted(row.id for row in small))
    for prefix, cats, actions in (
        (small, {"CAT-6": 8, "CAT-7": 2}, {"allow": 6, "deny": 4}),
        (medium, {"CAT-6": 20, "CAT-7": 5}, {"allow": 16, "deny": 9}),
    ):
        assert Counter(row.gold.category for row in prefix) == cats
        assert Counter(row.gold.action for row in prefix) == actions


@pytest.mark.parametrize("payload", [b'{"count":0}', b'{"percent":0}', b'{"percent":100.1}',
                                      b'{"count":200,"percent":100}', b'{}'])
def test_rules_budget_requires_one_valid_mode(payload: bytes) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(SampleSelection).validate_json(payload, strict=True)


def test_rules_graph_has_no_holdout_ancestor_of_learning(tmp_path: Path) -> None:
    graph = build_definitions(tmp_path).resolve_asset_graph()
    train, test = sample_key("train"), sample_key("test")
    capture, dedup, finalize, evaluate = (
        AssetKey(["codebench", f"rules_{stage}"]) for stage in ("capture", "dedup", "finalize", "eval")
    )
    for split, key in (("train", train), ("test", test)):
        assert AssetKey([split, "merged_rows"]) in graph.get(key).parent_keys
        assert AssetKey(["test" if split == "train" else "train", "merged_rows"]) not in (
            graph.get_ancestor_asset_keys(key)
        )
    for key in (capture, dedup, finalize):
        assert train in graph.get(key).parent_keys
        assert AssetKey(["train", "merged_rows"]) not in graph.get(key).parent_keys
        assert AssetKey(["train", "category_rows"]) not in graph.get(key).parent_keys
        assert test not in graph.get_ancestor_asset_keys(key)
        assert AssetKey(["test", "merged_rows"]) not in graph.get_ancestor_asset_keys(key)
    assert {test, finalize} <= graph.get(evaluate).parent_keys
    assert AssetKey(["test", "merged_rows"]) not in graph.get(evaluate).parent_keys
