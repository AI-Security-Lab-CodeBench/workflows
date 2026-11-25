import json
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import ClassVar, Literal
from unittest.mock import patch
from urllib.request import Request

import pytest
from dagster import build_asset_context
from pydantic import JsonValue, TypeAdapter
from test_declared_dataset_graph import fixture_spec
from test_keenable_snippets import SNIPPET_KEY, model_reply

from codebench_pipeline.collection_stage_settings import KeenableCandidateSettings
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_branches import BRANCH_BY_NAMESPACE
from codebench_pipeline.declared_source_assets import KeenableDiscovery, declared_source_assets
from codebench_pipeline.discovery_config import DiscoveryModelOverride
from codebench_pipeline.finalization_table_asset import CollectionCacheInput, CollectionInput
from codebench_pipeline.keenable_policy import KeenableError
from codebench_pipeline.labeling_runtime import LabelingRuntime
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, collection_stage_configs


class _HTTPBody(BytesIO):
    headers: ClassVar[dict[str, str]] = {}


@dataclass(frozen=True, slots=True)
class ReasoningCase:
    effort: str | None
    expected_template: dict[str, JsonValue] | None
    binding: Literal["inherited", "same", "distinct"] = "inherited"
    truncated: bool = False


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(ReasoningCase("none", {"enable_thinking": False}), id="thinking-disabled"),
        pytest.param(ReasoningCase("low", {"reasoning_effort": "low"}), id="configured-effort"),
        pytest.param(ReasoningCase(None, None), id="unset-policy"),
        pytest.param(ReasoningCase("none", {"enable_thinking": False}, binding="same"), id="same-binding"),
        pytest.param(ReasoningCase("none", None, binding="distinct"), id="distinct-endpoint"),
        pytest.param(ReasoningCase("none", {"enable_thinking": False}, truncated=True), id="truncated"),
    ],
)
def test_registered_plain_discovery_applies_reasoning_at_the_wire(
    tmp_path: Path, case: ReasoningCase
) -> None:
    spec = fixture_spec(tmp_path)
    annotation = spec.annotation
    batch = annotation.batch
    assert batch is not None
    labeling = annotation.model_copy(
        update={
            "batch": batch.model_copy(
                update={"inference_mode": "plain", "reasoning_effort": case.effort, "capacity": None}
            )
        }
    )
    options = spec.discovery
    if case.binding != "inherited":
        assert labeling.model.base_url is not None
        same_binding = case.binding == "same"
        options = options.model_copy(
            update={
                "model": DiscoveryModelOverride(
                    provider="openai_compatible",
                    model=labeling.model.model if same_binding else "generic",
                    base_url=(
                        labeling.model.base_url + "/" if same_binding else "https://discovery.example.org/v1"
                    ),
                )
            }
        )
    configured = spec.model_copy(update={"annotation": labeling, "discovery": options})
    registered = DeclaredGlobalConfig.model_validate_json(
        DeclaredGlobalConfig.from_spec(configured).model_dump_json()
    )
    mapped = collection_stage_configs(registered, "keenable")["keenable/command_candidates"]
    assert isinstance(mapped, KeenableCandidateSettings)
    inputs = CollectionInput(settings=KeenableCandidateSettings.model_validate_json(mapped.model_dump_json()))
    inputs.bind_cache(CollectionCacheInput("fixture", None, tmp_path))
    (asset,) = declared_source_assets(BRANCH_BY_NAMESPACE["keenable"])

    def respond(request: Request, timeout: float) -> _HTTPBody:
        assert isinstance(request.data, bytes)
        body = TypeAdapter(dict[str, JsonValue]).validate_json(request.data)
        assert body["max_completion_tokens"] == 1024
        if case.expected_template is None:
            assert "chat_template_kwargs" not in body
        else:
            assert body.get("chat_template_kwargs") == case.expected_template
        if case.truncated:
            return _HTTPBody(
                json.dumps(
                    {
                        "id": "fixture-length",
                        "model": "fixture-model",
                        "choices": [
                            {
                                "index": 0,
                                "finish_reason": "length",
                                "message": {"role": "assistant", "content": None},
                            }
                        ],
                    }
                ).encode()
            )
        return _HTTPBody(model_reply({"action": "finalize", "seed": None, "context": None}).body)

    with (
        patch("codebench_pipeline.openai_transport.build_opener") as opener,
        patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient") as keenable,
        build_asset_context(
            partition_key=spec.dataset_id,
            resources={
                "keenable_credential": KeenableDiscovery(SNIPPET_KEY),
                "labeling_runtime": LabelingRuntime(tmp_path, lambda _: None),
                "dataset_io_manager": DataFrameIOManager.from_environment(spec.storage),
            },
        ) as context,
    ):
        opener.return_value.open.side_effect = respond
        decision = tmp_path / "discovery-01-decision.json"
        if case.truncated:
            with pytest.raises(KeenableError, match="discovery_model_response_invalid"):
                asset(context, mapped_config=inputs)
            assert not decision.exists()
        else:
            asset(context, mapped_config=inputs)
            assert decision.is_file()
        opener.return_value.open.assert_called_once()
        keenable.assert_not_called()
