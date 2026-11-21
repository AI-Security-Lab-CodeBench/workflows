import json
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import ClassVar
from unittest import TestCase
from unittest.mock import patch
from urllib.request import Request

from dagster import build_asset_context
from httpx import AsyncClient, MockTransport
from pydantic import JsonValue, TypeAdapter
from test_declared_dataset_graph import fixture_spec, with_keenable_declaration
from test_keenable_snippets import SNIPPET_KEY, SNIPPET_URL, KeenableWire, model_reply

from codebench_pipeline.collection_config import KeenableSnippetDeclaration
from codebench_pipeline.collection_stage_settings import KeenableCandidateSettings
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_branches import BRANCH_BY_NAMESPACE
from codebench_pipeline.declared_source_assets import KeenableDiscovery, declared_source_assets
from codebench_pipeline.discovery_config import DiscoveryOptions
from codebench_pipeline.finalization_table_asset import CollectionCacheInput, CollectionInput
from codebench_pipeline.inference_capacity import CapacityConfig
from codebench_pipeline.keenable_decisions import DiscoveryModel
from codebench_pipeline.keenable_policy import KeenableSnippetSettings
from codebench_pipeline.keenable_snippets import (
    DiscoveryRun,
    SnippetCollectionReceipt,
    provider_snippet_candidates,
)
from codebench_pipeline.labeling_runtime import LabelingRuntime
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, collection_stage_configs
from codebench_pipeline.storage import IntegrityError


class _HTTPBody(BytesIO):
    headers: ClassVar[dict[str, str]] = {}


class DiscoveryMappingTests(TestCase):
    def test_plain_override_routes_to_its_own_provider_and_resolves_its_credential(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = fixture_spec(home)
        batch = spec.annotation.batch
        assert batch is not None
        labeling = spec.annotation.model_copy(
            update={
                "model": spec.annotation.model.model_copy(update={"api_key_env": "LABELER_KEY"}),
                "batch": batch.model_copy(update={"inference_mode": "plain", "capacity": None}),
            }
        )
        options = DiscoveryOptions.model_validate_json(
            '{"timeout_seconds":12,"max_output_tokens":196,"model":{"provider":"openai_compatible",'
            '"base_url":"https://discovery.example.org/chat-api","model":"separate-discovery",'
            '"api_key_env":"DISCOVERY_KEY","structured_output":"json_object"}}'
        )
        independent = spec.model_copy(update={"annotation": labeling, "discovery": options})
        registered = DeclaredGlobalConfig.model_validate_json(
            DeclaredGlobalConfig.from_spec(independent).model_dump_json()
        )
        mapped = collection_stage_configs(registered, "keenable")["keenable/command_candidates"]
        assert isinstance(mapped, KeenableCandidateSettings)
        inputs = CollectionInput(
            settings=KeenableCandidateSettings.model_validate_json(mapped.model_dump_json())
        )
        inputs.bind_cache(CollectionCacheInput("fixture", None, home))
        (asset,) = declared_source_assets(BRANCH_BY_NAMESPACE["keenable"])
        requests: list[tuple[str, str | None, dict[str, JsonValue], float]] = []

        def respond(request: Request, timeout: float) -> BytesIO:
            assert isinstance(request.data, bytes)
            body = TypeAdapter(dict[str, JsonValue]).validate_json(request.data)
            requests.append((request.full_url, request.get_header("Authorization"), body, timeout))
            return _HTTPBody(model_reply({"action": "finalize", "seed": None}).body)

        with (
            patch("codebench_pipeline.openai_transport.build_opener") as opener,
            patch("codebench_pipeline.keenable_decisions.guarded_complete") as guarded,
            build_asset_context(
                resources={
                    "keenable_credential": KeenableDiscovery(SNIPPET_KEY),
                    "labeling_runtime": LabelingRuntime(
                        home, {"DISCOVERY_KEY": "separate-key", "LABELER_KEY": "labeler-key"}.get
                    ),
                    "dataset_io_manager": DataFrameIOManager.from_environment(independent.storage),
                }
            ) as context,
        ):
            opener.return_value.open.side_effect = respond
            asset(context, mapped_config=inputs)

        guarded.assert_not_called()
        ((url, authorization, body, timeout),) = requests
        self.assertEqual(url, "https://discovery.example.org/chat-api/chat/completions")
        self.assertEqual(authorization, "Bearer separate-key")
        self.assertEqual(body["model"], "separate-discovery")
        self.assertEqual(body["max_completion_tokens"], 196)
        self.assertEqual(body["response_format"], {"type": "json_object"})
        self.assertEqual(timeout, 12)
        self.assertEqual(registered.scopes.labeling_attempts.labeling_attempts.model, labeling.model)
        inherited = DeclaredGlobalConfig.from_spec(
            spec.model_copy(
                update={
                    "annotation": labeling,
                    "discovery": DiscoveryOptions(max_output_tokens=317, timeout_seconds=19.0),
                }
            )
        )
        self.assertIsNotNone(inherited.scopes.discovery)
        assert inherited.scopes.discovery is not None
        self.assertEqual(
            inherited.scopes.discovery.model,
            labeling.model.model_copy(update={"max_output_tokens": 317, "timeout_seconds": 19.0}),
        )

    def test_guarded_override_rejects_incompatible_bindings_during_projection(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = fixture_spec(home)
        model = spec.annotation.model
        same = {"provider": model.provider, "base_url": model.base_url, "model": model.model}
        compatible = DiscoveryOptions.model_validate({"model": same})
        guarded = DeclaredGlobalConfig.from_spec(spec.model_copy(update={"discovery": compatible}))
        assert guarded.scopes.discovery is not None
        self.assertEqual(guarded.scopes.discovery.inference_mode, "guarded")
        self.assertIsNotNone(guarded.scopes.discovery.capacity)
        for field, value in (
            ("base_url", "https://discovery.example.org/v1"),
            ("model", "different-model"),
            ("api_key_env", "SEPARATE_KEY"),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(IntegrityError, "Guarded discovery"):
                options = DiscoveryOptions.model_validate({"model": {**same, field: value}})
                DeclaredGlobalConfig.from_spec(spec.model_copy(update={"discovery": options}))

    def test_guarded_model_uses_two_authored_ids_for_one_reviewed_profile(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = fixture_spec(home)
        rules = spec.rules.model_copy(
            update={
                "risks": [
                    risk.model_copy(update={"guidance": "Fixture-specific guidance for the active taxonomy."})
                    if risk.id == "CAT-3.3"
                    else risk
                    for risk in spec.rules.risks
                ]
            }
        )
        spec = spec.model_copy(update={"rules": rules})
        assert spec.declared_collection is not None
        declaration = spec.declared_collection.keenable_declaration
        assert isinstance(declaration, KeenableSnippetDeclaration)
        seed_ids = ("cat3-disk-usage", "cat3-volume-sizes")
        docker = next(seed for seed in declaration.search.authored_seeds if seed.id == "docker_inventory")
        search = KeenableSnippetSettings(
            seeds=tuple(docker.model_copy(update={"id": seed_id}) for seed_id in seed_ids),
            max_search_calls=3,
        )
        spec = with_keenable_declaration(spec, declaration.model_copy(update={"search": search}))
        registered = DeclaredGlobalConfig.model_validate_json(
            DeclaredGlobalConfig.from_spec(spec).model_dump_json()
        )
        mapped = collection_stage_configs(registered, "keenable")["keenable/command_candidates"]
        assert isinstance(mapped, KeenableCandidateSettings)
        mapped = KeenableCandidateSettings.model_validate_json(mapped.model_dump_json())
        assert mapped.inference is not None and mapped.inference.capacity is not None
        self.assertEqual(mapped.inference.inference_mode, "guarded")
        batch = spec.annotation.batch
        assert batch is not None
        original = batch.require_capacity()
        capacity = mapped.inference.capacity.bind(original.state_directory, original.expected_model_path)
        wire = KeenableWire(
            [
                {
                    "url": SNIPPET_URL,
                    "snippet": "Docker volume sizes\n```sh\ndocker system df\nkubectl get pods\n```",
                }
            ]
        )
        client = AsyncClient(headers={"X-API-Key": SNIPPET_KEY}, transport=MockTransport(wire.respond))
        requests: list[bytes] = []

        def reply(
            endpoint: str, body: bytes, key: str | None, timeout: float, guard: CapacityConfig, out: Path
        ) -> HTTPResponse:
            self.assertEqual(endpoint, spec.annotation.model.base_url)
            self.assertEqual(guard, original)
            requests.append(body)
            if len(requests) == 3:
                return model_reply({"action": "finalize", "seed": None, "context": None})
            index = len(requests) - 1
            context = ("disk usage", "volume sizes")[index]
            return model_reply({"action": "query", "seed": seed_ids[index], "context": context})

        with (
            patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", return_value=client),
            patch("codebench_pipeline.keenable_decisions.guarded_complete", side_effect=reply),
            patch("codebench_pipeline.keenable_decisions.complete") as plain,
        ):
            frame = provider_snippet_candidates(
                mapped.search,
                DiscoveryRun(
                    home, SNIPPET_KEY, DiscoveryModel(mapped.inference.model, None, mapped.taxonomy, capacity)
                ),
            )
        plain.assert_not_called()
        self.assertEqual(frame.command.tolist(), ["docker system df"])
        self.assertTrue(wire.queries[0].endswith("Focus: disk usage."))
        self.assertTrue(wire.queries[1].endswith("Focus: volume sizes."))
        second = json.loads(requests[1])
        self.assertEqual(second["model"], spec.annotation.model.model)
        prompt = json.loads(second["messages"][1]["content"])
        risk = next(rule for rule in spec.rules.risks if rule.id == "CAT-3.3")
        self.assertEqual([item["seed"]["id"] for item in prompt["available"]], list(seed_ids))
        docker = prompt["available"][0]
        self.assertEqual(
            (docker["risk_definition"], docker["risk_guidance"]), (risk.definition, risk.guidance)
        )
        self.assertEqual(
            docker["severity_definition"],
            next(severity.definition for severity in spec.rules.severities if severity.name == "low"),
        )
        (first,) = prompt["previous"]
        receipt = SnippetCollectionReceipt.model_validate_json(
            (home / "discovery-02-result.json").read_bytes()
        )
        one, two = (search.feedback for search in receipt.searches)
        assert one is not None and two is not None
        self.assertEqual((one.seed, two.seed), seed_ids)
        self.assertEqual(
            [search.choice.decision.seed for search in receipt.searches if search.choice is not None],
            list(seed_ids),
        )
        self.assertEqual(first["query_id"], one.query_id)
        self.assertNotEqual(one.query_id, two.query_id)
        self.assertEqual(first["excerpt_context"], ["docker volume sizes"])
        self.assertEqual(
            (one.new_sources, one.new_candidates, two.new_sources, two.new_candidates), (1, 1, 0, 0)
        )
