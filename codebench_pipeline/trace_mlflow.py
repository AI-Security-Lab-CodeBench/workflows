import socket
from typing import Final, TypedDict

import httpx2
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from codebench_pipeline.trace_projection import Span

_EXPERIMENT: Final = "CodeBench"
_EXPERIMENTS: Final = "api/2.0/mlflow/experiments"
_MAX_ACK_BYTES: Final = 4 * 1024 * 1024


class TraceDeliveryError(RuntimeError):
    """A trace has not been acknowledged by the tracking server."""


class _Experiment(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    experiment_id: str = Field(pattern=r"^[0-9]+$")


class _ExperimentLookup(BaseModel):
    experiment: _Experiment


class _Problem(BaseModel):
    error_code: str


class _Partial(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    rejectedSpans: int | str = 0
    errorMessage: str = ""


class _Acknowledgment(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    partialSuccess: _Partial = Field(default_factory=_Partial)


class _Scope(TypedDict):
    name: str


class _ScopeSpans(TypedDict):
    scope: _Scope
    spans: list[Span]


class _ResourceSpans(TypedDict):
    scopeSpans: list[_ScopeSpans]


class _OtlpRequest(TypedDict):
    resourceSpans: list[_ResourceSpans]


def mlflow_client(uri: str) -> httpx2.Client:
    return httpx2.Client(
        base_url=uri,
        transport=httpx2.HTTPTransport(
            trust_env=False,
            limits=httpx2.Limits(max_connections=1, max_keepalive_connections=1),
            socket_options=[(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)],
        ),
        timeout=httpx2.Timeout(connect=3, read=5, write=5, pool=3),
        follow_redirects=False,
        trust_env=False,
        headers={"Accept": "application/json", "Accept-Encoding": "identity"},
    )


def _lookup(client: httpx2.Client) -> str | None:
    response = client.get(f"{_EXPERIMENTS}/get-by-name", params={"experiment_name": _EXPERIMENT})
    if response.status_code == 200:
        return _ExperimentLookup.model_validate_json(response.content).experiment.experiment_id
    if (
        response.status_code in (400, 404)
        and _Problem.model_validate_json(response.content).error_code == "RESOURCE_DOES_NOT_EXIST"
    ):
        return None
    raise TraceDeliveryError(f"experiment lookup returned HTTP {response.status_code}")


def experiment_id(client: httpx2.Client) -> str:
    existing = _lookup(client)
    if existing is not None:
        return existing
    response = client.post(f"{_EXPERIMENTS}/create", json={"name": _EXPERIMENT})
    if response.status_code == 200:
        return _Experiment.model_validate_json(response.content).experiment_id
    if (
        response.status_code in (400, 409)
        and _Problem.model_validate_json(response.content).error_code == "RESOURCE_ALREADY_EXISTS"
    ):
        existing = _lookup(client)
        if existing is not None:
            return existing
    raise TraceDeliveryError(f"experiment creation returned HTTP {response.status_code}")


def export_trace(client: httpx2.Client, experiment: str, spans: list[Span]) -> None:
    payload: _OtlpRequest = {
        "resourceSpans": [{"scopeSpans": [{"scope": {"name": "codebench.dagster"}, "spans": spans}]}]
    }
    with client.stream(
        "POST", "v1/traces", json=payload, headers={"X-MLflow-Experiment-Id": experiment}
    ) as response:
        if response.status_code != 200:
            raise TraceDeliveryError(f"OTLP ingestion returned HTTP {response.status_code}")
        if response.headers.get("content-type", "").partition(";")[0].lower().strip() != "application/json":
            raise TraceDeliveryError("OTLP ingestion returned a non-JSON acknowledgment")
        body = bytearray()
        for chunk in response.iter_bytes(chunk_size=65536):
            if len(body) + len(chunk) > _MAX_ACK_BYTES:
                raise TraceDeliveryError("OTLP acknowledgment exceeded the response size limit")
            body.extend(chunk)
    try:
        ack = _Acknowledgment.model_validate_json(body)
    except ValidationError:
        raise TraceDeliveryError("OTLP acknowledgment was malformed") from None
    if ack.partialSuccess.rejectedSpans not in (0, "0"):
        raise TraceDeliveryError("OTLP acknowledgment rejected spans")
