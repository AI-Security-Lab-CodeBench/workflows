import json
import socket
from collections.abc import Iterator
from dataclasses import dataclass
from hashlib import sha256
from typing import Final

import httpx2
from pydantic import ValidationError

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

_MAX_REQUEST_BYTES: Final = 2 << 20
_MAX_BUNDLE_BYTES: Final = 1 << 20
_MAX_RESPONSE_BYTES: Final = 32 << 20
_MAX_ROWS: Final = 128


def _batch_bodies(
    prefix: bytes, rows: tuple[EvaluationInput, ...]
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
        if batch and (len(batch) == _MAX_ROWS or size + len(encoded) + 1 > _MAX_REQUEST_BYTES):
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
        transport = self.transport or httpx2.HTTPTransport(
            trust_env=False,
            retries=3,
            limits=httpx2.Limits(max_connections=4, max_keepalive_connections=2, keepalive_expiry=30),
            socket_options=[(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)],
        )
        with httpx2.Client(
            base_url=self.base_url,
            transport=transport,
            timeout=httpx2.Timeout(connect=5, read=35, write=10, pool=10),
            follow_redirects=False,
            trust_env=False,
            headers={"Accept": "application/json", "Accept-Encoding": "identity"},
        ) as client:
            for body, ids in _batch_bodies(prefix, rows):
                batch = self._post(client, body)
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

    def _post(self, client: httpx2.Client, payload: bytes) -> NativeEvaluation:
        try:
            with client.stream(
                "POST", "/v1/evaluate", content=payload, headers={"Content-Type": "application/json"}
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
                evaluation = NativeEvaluation.model_validate_json(data)
                self._require_pin(evaluation.runtime_version)
                return evaluation
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
