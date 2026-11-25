import json
import socket
from collections.abc import Iterator
from dataclasses import dataclass
from hashlib import sha256
from typing import Final, TypeVar

import httpx2
from pydantic import ValidationError

from codebench_pipeline.rules_capture_facts import AnalysisBatch, AnalysisRow, filename_probes
from codebench_pipeline.rules_contracts import (
    Bundle,
    Diagnostic,
    EvaluatedRow,
    EvaluationError,
    EvaluationInput,
    EvaluationReceipt,
    FinalizeError,
    FinalizeReceipt,
    FinalizeSubmission,
    Fixture,
    HitSet,
    NativeEvaluation,
    NativeFailure,
    NativeRow,
    Package,
)
from codebench_pipeline.rules_corpus import Corpus
from codebench_pipeline.typed_config import ConfigArtifact

_MAX_REQUEST_BYTES: Final = 2 << 20
_MAX_BUNDLE_BYTES: Final = 1 << 20
_MAX_RESPONSE_BYTES: Final = 32 << 20
_MAX_ROWS: Final = 128
_BatchResponse = TypeVar("_BatchResponse", bound=ConfigArtifact)


def _batch_bodies(
    prefix: bytes, rows: tuple[EvaluationInput, ...], *, max_rows: int = _MAX_ROWS
) -> Iterator[tuple[bytes, tuple[str, ...]]]:
    batch: list[bytes] = []
    ids: list[str] = []
    base_size = len(prefix) + len(b"]}")
    size = base_size
    for row in rows:
        encoded = row.model_dump_json().encode()
        if base_size + len(encoded) > _MAX_REQUEST_BYTES:
            raise EvaluationError(
                Diagnostic(code="request_too_large", message="Single row exceeds batch limit")
            )
        if batch and (len(batch) == max_rows or size + len(encoded) + 1 > _MAX_REQUEST_BYTES):
            yield prefix + b",".join(batch) + b"]}", tuple(ids)
            batch, ids, size = [], [], base_size
        size += len(encoded) + bool(batch)
        batch.append(encoded)
        ids.append(row.id)
    if batch:
        yield prefix + b",".join(batch) + b"]}", tuple(ids)


@dataclass(frozen=True, slots=True)
class BashguardClient:
    base_url: str
    runtime_version: str
    transport: httpx2.BaseTransport | None = None

    def evaluate(self, bundle: Bundle, rows: tuple[EvaluationInput, ...]) -> tuple[NativeRow, ...]:
        """Send only evaluation inputs, with byte-exact pinned rules in every bounded request."""
        if not rows or len({row.id for row in rows}) != len(rows):
            raise EvaluationError(
                Diagnostic(code="input_rows", message="Expected distinct evaluation row IDs")
            )
        raw_bundle = bundle.model_dump_json().encode()
        if len(raw_bundle) > _MAX_BUNDLE_BYTES:
            raise EvaluationError(Diagnostic(code="bundle_too_large", message="Bundle exceeds 1 MiB"))
        digest = sha256(raw_bundle).hexdigest()
        pin = json.dumps(self.runtime_version).encode()
        prefix = b'{"required_runtime_version":' + pin + b',"bundle":' + raw_bundle + b',"rows":['
        rules = {rule.id: rule.labels for rule in bundle.rules}
        results: list[NativeRow] = []
        with self._client() as client:
            for body, ids in _batch_bodies(prefix, rows):
                batch = self._post(client, "/v1/evaluate", body, NativeEvaluation)
                self._require_pin(batch.runtime_version)
                indexed = {row.id: row for row in batch.results}
                if len(batch.results) != len(ids) or set(indexed) != set(ids):
                    raise EvaluationError(
                        Diagnostic(
                            code="response_rows", message="Evaluator returned wrong or duplicate row IDs"
                        )
                    )
                if batch.bundle_sha256 != digest or any(
                    row.result.bundle_sha256 != digest for row in batch.results
                ):
                    raise EvaluationError(
                        Diagnostic(
                            code="response_digest", message="Evaluator did not attest the submitted bundle"
                        )
                    )
                for key in ids:
                    row = indexed[key]
                    matches = row.result.rule_ids
                    if len(set(matches)) != len(matches) or any(
                        match not in rules or rules[match] != row.result.labels for match in matches
                    ):
                        raise EvaluationError(
                            Diagnostic(
                                code="response_match", message="Evaluator returned inconsistent matches"
                            )
                        )
                    results.append(row)
        return tuple(results)

    def analyze(self, rows: tuple[EvaluationInput, ...]) -> tuple[AnalysisRow, ...]:
        """Get parser facts from the same pinned service that runs CEL, without executing input."""
        prefix = b'{"required_runtime_version":' + json.dumps(self.runtime_version).encode() + b',"rows":['
        results: list[AnalysisRow] = []
        with self._client() as client:
            for body, ids in _batch_bodies(prefix, rows, max_rows=8):
                batch = self._post(client, "/v1/analyze", body, AnalysisBatch)
                self._require_pin(batch.runtime_version)
                if tuple(row.id for row in batch.results) != ids:
                    raise EvaluationError(Diagnostic(code="response_rows", message="Analyzer returned wrong row IDs"))
                results.extend(batch.results)
        return tuple(results)

    def _client(self) -> httpx2.Client:
        transport = self.transport or httpx2.HTTPTransport(
            trust_env=False,
            retries=3,
            limits=httpx2.Limits(max_connections=4, max_keepalive_connections=2, keepalive_expiry=30),
            socket_options=[(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)],
        )
        return httpx2.Client(
            base_url=self.base_url,
            transport=transport,
            timeout=httpx2.Timeout(connect=5, read=35, write=10, pool=10),
            follow_redirects=False,
            trust_env=False,
            headers={"Accept": "application/json", "Accept-Encoding": "identity"},
        )

    def _post(
        self, client: httpx2.Client, path: str, payload: bytes, response_model: type[_BatchResponse]
    ) -> _BatchResponse:
        try:
            with client.stream(
                "POST", path, content=payload, headers={"Content-Type": "application/json"}
            ) as response:
                data = bytearray()
                for chunk in response.iter_bytes(chunk_size=65536):
                    if len(data) + len(chunk) > _MAX_RESPONSE_BYTES:
                        raise EvaluationError(
                            Diagnostic(
                                code="response_too_large", message="Evaluation response exceeds 32 MiB"
                            )
                        )
                    data.extend(chunk)
                if response.status_code != 200:
                    failure = NativeFailure.model_validate_json(data)
                    self._require_pin(failure.runtime_version, failure.error)
                    raise EvaluationError(failure.error)
                return response_model.model_validate_json(data)
        except ValidationError as exc:
            raise EvaluationError(
                Diagnostic(code="response_schema", message="Evaluator returned an invalid protocol response")
            ) from exc
        except httpx2.HTTPError as exc:
            raise EvaluationError(
                Diagnostic(code="service_transport", message=type(exc).__name__, phase="service")
            ) from exc

    def _require_pin(self, actual: str, diagnostic: Diagnostic | None = None) -> None:
        if actual != self.runtime_version:
            if diagnostic is not None and diagnostic.code == "runtime_version_mismatch":
                raise EvaluationError(diagnostic)
            raise EvaluationError(
                Diagnostic(
                    code="runtime_version_mismatch", message="Unexpected evaluator runtime", phase="version"
                )
            )


@dataclass(frozen=True, slots=True)
class _Evaluation:
    bundle: Bundle
    corpus: Corpus
    tests: tuple[Fixture, ...] = ()

    def run(self, client: BashguardClient) -> EvaluationReceipt:
        fixtures = tuple(
            (
                EvaluationInput(id=f"fixture:{index}", command=test.command, context=test.context),
                test.expected,
            )
            for index, test in enumerate(self.tests)
        )
        examples = tuple(
            (EvaluationInput(id=row.id, command=row.command), row.gold) for row in self.corpus.rows
        )
        pairs = (*fixtures, *examples)
        native = client.evaluate(self.bundle, tuple(row for row, _ in pairs))
        scored = tuple(EvaluatedRow.from_native(pair, row) for pair, row in zip(pairs, native, strict=True))
        training = scored[len(fixtures) :]
        return EvaluationReceipt(
            runtime_version=client.runtime_version,
            bundle_sha256=native[0].result.bundle_sha256,
            split=self.corpus.split,
            corpus_digest=self.corpus.digest,
            fixtures=scored[: len(fixtures)],
            rows=training,
            hit_sets=tuple(
                HitSet(rule_id=rule.id, row_ids=tuple(row.id for row in training if rule.id in row.rule_ids))
                for rule in self.bundle.rules
            ),
        )


def evaluate_package(client: BashguardClient, package: Package, train: Corpus) -> EvaluationReceipt:
    if train.split != "train":
        raise EvaluationError(Diagnostic(code="train_only", message="Package evaluation requires train"))
    return _Evaluation(Bundle(rules=(package.rule,)), train, package.tests).run(client)


def require_filename_invariance(
    client: BashguardClient, package: Package, receipt: EvaluationReceipt, analyses: dict[str, AnalysisRow],
) -> None:
    matched = tuple(row.input for row in receipt.rows if package.rule.id in row.rule_ids)
    probes = tuple(probe for row in matched for probe in filename_probes(row, analyses[row.id]))
    if not probes:
        return
    results = client.evaluate(Bundle(rules=(package.rule,)), probes)
    if any(row.error is not None or row.result.rule_ids != (package.rule.id,) for row in results):
        raise EvaluationError(Diagnostic(
            code="filename_overfit", phase="gate", rule_id=package.rule.id, field="when",
            message="Matching behavior changed after replacing a masked filename with a benign filename.",
        ))


def evaluate_corpus(client: BashguardClient, bundle: Bundle, corpus: Corpus) -> EvaluationReceipt:
    return _Evaluation(bundle, corpus).run(client)


@dataclass(frozen=True, slots=True)
class _Finalize:
    base: tuple[Package, ...]
    submission: FinalizeSubmission
    train: Corpus

    def run(self, client: BashguardClient) -> FinalizeReceipt:
        base_ids = {package.rule.id for package in self.base}
        drops = set(self.submission.drop)
        adds = {package.rule.id for package in self.submission.add}
        if (
            self.train.split != "train"
            or len(base_ids) != len(self.base)
            or len(drops) != len(self.submission.drop)
            or not drops.issubset(base_ids)
            or len(adds) != len(self.submission.add)
            or adds.intersection(base_ids)
        ):
            raise FinalizeError(
                Diagnostic(code="finalize_diff", message="Invalid train, drop or fresh add IDs")
            )
        packages = (
            tuple(package for package in self.base if package.rule.id not in drops) + self.submission.add
        )
        try:
            bundle = Bundle(rules=tuple(package.rule for package in packages))
        except ValidationError as exc:
            raise FinalizeError(
                Diagnostic(code="finalize_bundle", message="Bundle must have 1..128 rules")
            ) from exc
        additions: list[EvaluationReceipt] = []
        try:
            for package in self.submission.add:
                receipt = evaluate_package(client, package, self.train)
                additions.append(receipt)
                _require_gate(receipt, tuple(additions))
            tests = tuple(test for package in self.submission.add for test in package.tests)
            combined = _Evaluation(bundle, self.train, tests).run(client)
            _require_gate(combined, (*additions, combined))
        except FinalizeError:
            raise
        except EvaluationError as exc:
            raise FinalizeError(exc.diagnostic, additions) from exc
        return FinalizeReceipt(
            submission=self.submission,
            packages=packages,
            bundle=bundle,
            additions=tuple(additions),
            combined=combined,
        )


def _require_gate(receipt: EvaluationReceipt, evidence: tuple[EvaluationReceipt, ...]) -> None:
    if diagnostic := receipt.gate_failure():
        raise FinalizeError(diagnostic, evidence)


def finalize_batch(
    client: BashguardClient, base: tuple[Package, ...], submission: FinalizeSubmission, train: Corpus
) -> FinalizeReceipt:
    """Validate the submitted diff atomically; neither prior coverage nor old fixtures are a release gate."""
    return _Finalize(base, submission, train).run(client)
