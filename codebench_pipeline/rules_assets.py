from collections.abc import Callable
from typing import Final

from dagster import (
    AllPartitionMapping,
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetSelection,
    Config,
    Dict,
    Failure,
    MetadataValue,
    Output,
    PythonObjectDagsterType,
    asset,
)
from pandera.typing import DataFrame

from codebench_pipeline.config_assets import mapped_input, mapping_key
from codebench_pipeline.rules_agents import RuleAgent
from codebench_pipeline.rules_artifacts import (
    RULES_EVAL_KEY,
    RULES_EVIDENCE_DIGEST,
    RULES_METRICS_TYPE,
    FinalizedRules,
    RulesCaptureArtifact,
    RulesDedupArtifact,
    RulesMetricsTable,
    aggregate_holdout,
    holdout_chart,
    load_capture_attempt,
    record_rules_evidence,
)
from codebench_pipeline.rules_contracts import Bundle
from codebench_pipeline.rules_corpus import Corpus
from codebench_pipeline.rules_engine import BashguardClient, evaluate_corpus
from codebench_pipeline.rules_learning import capture_rules, deduplicate_rules
from codebench_pipeline.rules_metrics import TrainFingerprint, score_holdout
from codebench_pipeline.rules_report_asset import rules_eval_report
from codebench_pipeline.rules_settings import (
    CAPTURE_PARTITIONS,
    RULES_CONFIG_IO,
    RULES_GROUP,
    RULES_PARTITIONS,
    RULES_REGISTRATION,
    AgentSettings,
    RulesCaptureSettings,
    RulesDedupSettings,
    RulesEvalSettings,
    RulesFinalizeSettings,
    SplitPin,
)
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.train_assets import TRAIN_TABLE_IO, Split

_CAPTURE_KEY: Final = AssetKey(["codebench", "rules_capture"])
_DEDUP_KEY: Final = AssetKey(["codebench", "rules_dedup"])
_FINAL_KEY: Final = AssetKey(["codebench", "rules_finalize"])
_CORPUS_TYPE: Final = PythonObjectDagsterType(Corpus, name="RulesCorpus")


_CAPTURE_TYPE: Final = PythonObjectDagsterType(RulesCaptureArtifact, name="RulesCaptureArtifact")
_DEDUP_TYPE: Final = PythonObjectDagsterType(RulesDedupArtifact, name="RulesDedupArtifact")
_FINAL_TYPE: Final = PythonObjectDagsterType(FinalizedRules, name="FinalizedRules")


class RulesCaptureConfig(Config):
    resume_run: str | None = None


def _corpus_input(split: Split) -> AssetIn:
    return AssetIn(
        key=AssetKey([split, "merged_rows"]),
        dagster_type=_CORPUS_TYPE,
        input_manager_key="rules_corpus_input",
    )


def _require_corpus(corpus: Corpus, pin: SplitPin, split: Split) -> None:
    if corpus.split != split or corpus.digest != pin.digest or len(corpus.rows) != pin.row_count:
        raise IntegrityError(f"Rules run requires the registered {split} snapshot and row count")


def _agent(context: AssetExecutionContext, settings: AgentSettings, guide_sha256: str) -> RuleAgent:
    credential: Callable[[str], str] = context.resources.rules_model_environment
    key = settings.model.api_key_env
    return RuleAgent(
        model=settings.model,
        backend_revision=settings.backend_revision,
        guide=settings.guide,
        guide_sha256=guide_sha256,
        max_request_bytes=settings.max_request_bytes,
        api_key=credential(key) if key is not None else None,
    )


@asset(
    key=_CAPTURE_KEY,
    group_name=RULES_GROUP,
    tags={"stage": "rules_capture"},
    partitions_def=CAPTURE_PARTITIONS,
    io_manager_key=RULES_CONFIG_IO,
    required_resource_keys={"rules_model_environment", RULES_CONFIG_IO},
    dagster_type=_CAPTURE_TYPE,
    ins={
        "settings": mapped_input("rules_capture", RulesCaptureSettings),
        "train": AssetIn(
            key=AssetKey(["train", "category_rows"]), dagster_type=_CORPUS_TYPE,
            input_manager_key="rules_corpus_input",
        ),
    },
)
def rules_capture(
    context: AssetExecutionContext, config: RulesCaptureConfig, settings: RulesCaptureSettings, train: Corpus,
) -> Output[RulesCaptureArtifact]:
    """Teach one category and gate each learned rule on that category's pinned rows."""
    if train.sources_digest != settings.sources_digest:
        raise IntegrityError("Rules capture requires the configured train category sources")
    source = config.resume_run
    resume = load_capture_attempt(context, settings, source) if source is not None else None
    client = BashguardClient(settings.runtime.base_url, settings.runtime.version)
    result = capture_rules(
        _agent(context, settings.agent, settings.guide_sha256), train, client, category=settings.category, resume=resume,
    )
    if not result.complete:
        digest = record_rules_evidence(context, result)
        failures = sorted({
            diagnostic.code for batch in result.batches for diagnostic in batch.run.diagnostics
        } | {batch.run.status for batch in result.batches if batch.run.status != "finished"})
        raise Failure(
            f"Category capture did not finish every selected batch: {', '.join(failures)}",
            metadata={RULES_EVIDENCE_DIGEST: digest},
        )
    return Output(
        RulesCaptureArtifact(
            category=settings.category, sources_digest=settings.sources_digest, guide_sha256=settings.guide_sha256,
            inference_digest=settings.agent.inference_digest,
            result=result,
        ),
        metadata={
            "rules": len(result.packages), "batches": len(result.batches), "category digest": train.digest,
            **({"resumed from run": source} if source is not None else {}),
        },
    )


@asset(
    key=_DEDUP_KEY,
    group_name=RULES_GROUP,
    tags={"stage": "rules_dedup"},
    partitions_def=RULES_PARTITIONS,
    io_manager_key=RULES_CONFIG_IO,
    required_resource_keys={"rules_model_environment", RULES_CONFIG_IO},
    dagster_type=_DEDUP_TYPE,
    ins={
        "settings": mapped_input("rules_dedup", RulesDedupSettings),
        "train": _corpus_input("train"),
        "captures": AssetIn(
            key=_CAPTURE_KEY, dagster_type=Dict[str, _CAPTURE_TYPE], partition_mapping=AllPartitionMapping(),
            input_manager_key="rules_capture_inputs",
        ),
    },
)
def rules_dedup(
    context: AssetExecutionContext,
    settings: RulesDedupSettings,
    train: Corpus,
    captures: dict[str, RulesCaptureArtifact],
) -> Output[RulesDedupArtifact]:
    """Judge proposals from exactly the selected captures sharing one pinned learning lineage."""
    _require_corpus(train, settings.pins.train, "train")
    missing = set(settings.pins.categories) - captures.keys()
    if missing:
        raise IntegrityError(f"Missing selected capture partitions: {', '.join(sorted(missing))}")
    selected = tuple(captures[category] for category in settings.pins.categories)
    if any(
        item.category != category
        or item.sources_digest != settings.category_sources_digests[category]
        or item.guide_sha256 != settings.pins.guide_sha256
        or item.inference_digest != settings.agent.inference_digest
        or item.result.runtime_version != settings.runtime.version
        for category, item in zip(settings.pins.categories, selected, strict=True)
    ):
        raise IntegrityError("Capture partitions disagree with the pinned category sources, guide, runtime or model")
    client = BashguardClient(settings.runtime.base_url, settings.runtime.version)
    result = deduplicate_rules(
        _agent(context, settings.agent, settings.pins.guide_sha256), train, client, tuple(item.result for item in selected)
    )
    if not result.complete:
        record_rules_evidence(context, result)
        raise IntegrityError("Dedup did not finalize every nominated batch")
    return Output(
        RulesDedupArtifact(pins=settings.pins, result=result),
        metadata={"rules": len(result.packages), "judge batches": len(result.decisions)},
    )


@asset(
    key=_FINAL_KEY,
    group_name=RULES_GROUP,
    tags={"stage": "rules_finalize"},
    partitions_def=RULES_PARTITIONS,
    io_manager_key=RULES_CONFIG_IO,
    required_resource_keys={RULES_CONFIG_IO},
    dagster_type=_FINAL_TYPE,
    ins={
        "settings": mapped_input("rules_finalize", RulesFinalizeSettings),
        "train": _corpus_input("train"),
        "dedup": AssetIn(key=_DEDUP_KEY, dagster_type=_DEDUP_TYPE),
    },
)
def rules_finalize(
    context: AssetExecutionContext,
    settings: RulesFinalizeSettings,
    train: Corpus,
    dedup: RulesDedupArtifact,
) -> Output[FinalizedRules]:
    """Replay the complete surviving bundle against pinned train and freeze it before scoring."""
    _require_corpus(train, settings.pins.train, "train")
    if (
        dedup.pins != settings.pins
        or dedup.result.corpus_digest != train.digest
        or dedup.result.runtime_version != settings.runtime.version
    ):
        raise IntegrityError("Dedup output disagrees with the pinned training lineage")
    bundle = Bundle(rules=tuple(package.rule for package in dedup.result.packages))
    receipt = evaluate_corpus(BashguardClient(settings.runtime.base_url, settings.runtime.version), bundle, train)
    record_rules_evidence(context, receipt)
    if receipt.gate_failure() is not None:
        raise IntegrityError("Final bundle does not pass pinned train replay")
    return Output(
        FinalizedRules(
            pins=settings.pins, bundle=bundle, bundle_sha256=receipt.bundle_sha256,
            training=TrainFingerprint.from_corpus(train, categories=settings.pins.categories),
        ),
        metadata={"rules": len(bundle.rules), "bundle digest": receipt.bundle_sha256},
    )


@asset(
    key=RULES_EVAL_KEY,
    group_name=RULES_GROUP,
    tags={"stage": "rules_eval"},
    partitions_def=RULES_PARTITIONS,
    io_manager_key=TRAIN_TABLE_IO,
    required_resource_keys={RULES_CONFIG_IO},
    dagster_type=RULES_METRICS_TYPE,
    ins={
        "settings": mapped_input("rules_eval", RulesEvalSettings),
        "test": _corpus_input("test"),
        "finalized": AssetIn(key=_FINAL_KEY, dagster_type=_FINAL_TYPE),
    },
)
def rules_eval(
    context: AssetExecutionContext,
    settings: RulesEvalSettings,
    test: Corpus,
    finalized: FinalizedRules,
) -> Output[DataFrame[RulesMetricsTable]]:
    """Evaluate a frozen bundle on held-out data without model calls or reading train rows."""
    if finalized.pins != settings.pins or finalized.training.digest != settings.pins.train.digest:
        raise IntegrityError("Final bundle disagrees with the pinned learning lineage")
    _require_corpus(test, settings.test, "test")
    receipt = evaluate_corpus(
        BashguardClient(settings.runtime.base_url, settings.runtime.version), finalized.bundle, test
    )
    if receipt.bundle_sha256 != finalized.bundle_sha256:
        raise IntegrityError("Scored bundle differs from the frozen training bundle")
    metrics = score_holdout(receipt, finalized.training)
    record_rules_evidence(context, receipt)
    return Output(
        aggregate_holdout(metrics),
        metadata={
            "Outcome distribution": MetadataValue.md(holdout_chart(metrics)),
            "Held-out support": metrics.overall.support,
            "Exact matches": metrics.overall.correct,
            "Wrong matches": metrics.overall.wrong,
            "Abstentions": metrics.overall.abstain,
            "Evaluator errors": metrics.overall.error,
            "Bundle digest": metrics.bundle_sha256,
            "Test digest": metrics.test_digest,
        },
    )


RULES_ASSETS: Final = (rules_capture, rules_dedup, rules_finalize, rules_eval, rules_eval_report)
RULES_JOB_SELECTIONS: Final = {
    "register_rules": AssetSelection.assets(RULES_REGISTRATION),
    "capture_rules": AssetSelection.assets(mapping_key("rules_capture"), rules_capture),
    "learn_rules": AssetSelection.assets(
        mapping_key("rules_dedup"), mapping_key("rules_finalize"), rules_dedup, rules_finalize
    ),
    "evaluate_rules": AssetSelection.assets(mapping_key("rules_eval"), rules_eval, rules_eval_report),
}
