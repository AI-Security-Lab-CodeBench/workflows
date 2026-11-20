from collections import Counter, deque
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import Final

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.batch_labeling import BatchExecution, EvidenceAdmission, annotate_batches_bound
from codebench_pipeline.batch_labeling_config import BatchLabelingConfig
from codebench_pipeline.command_batches import similarity_batches
from codebench_pipeline.dataset_finalization import normalized_command
from codebench_pipeline.dataset_records import CandidateRecord, index_records, routing
from codebench_pipeline.labeling_outcomes import LabelingAttempt, failed_attempt, validate_attempts
from codebench_pipeline.labeling_stage_settings import LabelingRules
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.repository_config import SourceWeight
from codebench_pipeline.storage import IntegrityError, fingerprint

SUBRISK_QUOTA: Final = 250
QUOTA_DEFERRED: Final = "quota_deferred"
QUOTA_RECEIPT: Final = "labeling-quota.json"
_WEIGHTS: Final = TypeAdapter(dict[str, SourceWeight])
_RECEIPT: Final = TypeAdapter(dict[str, JsonValue])


@dataclass(frozen=True, slots=True)
class RankedLabeling:
    rules: LabelingRules
    model: ModelConfig
    config: BatchLabelingConfig
    out: Path
    cache: Path
    maximum_context_characters: int
    admission: EvidenceAdmission
    workers: int = 1
    execution: BatchExecution = BatchExecution(None, None)

    def _commit_packet_evidence(self, packet_path: Path) -> None:
        root = self.out.resolve()
        packet_rows = root / packet_path / "label"
        row_root = root / "label"
        if (
            not packet_rows.is_relative_to(root / "ranked-packets")
            or packet_rows.resolve() != packet_rows
            or row_root.resolve() != row_root
        ):
            raise IntegrityError("Labeling evidence directories cannot be links")
        if packet_rows.is_dir():
            directories = tuple(packet_rows.iterdir())
            if any(
                directory.is_symlink()
                or not directory.is_dir()
                or any(entry.is_symlink() for entry in directory.rglob("*"))
                for directory in directories
            ):
                raise IntegrityError("Packet labeling evidence must contain unlinked row directories")
            row_root.mkdir(exist_ok=True)
            for directory in directories:
                directory.rename(row_root / directory.name)

    def verified_support(self, attempt: LabelingAttempt) -> str:
        root = self.out.resolve()
        path = (root / attempt.attempt_evidence_reference).with_name("verified.json").resolve()
        if not path.is_relative_to(root / "label"):
            raise IntegrityError("Verified labeling support is outside the attempt workspace")
        return path.read_text(encoding="utf-8")

    def run(
        self,
        candidates: list[CandidateRecord],
        weights: dict[str, SourceWeight],
        source_failures: list[LabelingAttempt],
    ) -> tuple[list[LabelingAttempt], dict[str, JsonValue]]:
        def run_packet(packet: list[CandidateRecord], path: Path) -> list[LabelingAttempt]:
            return annotate_batches_bound(
                packet,
                [],
                self.rules,
                self.model,
                self.config,
                path,
                cache=self.cache,
                maximum_context_characters=self.maximum_context_characters,
                admission=self.admission,
                execution=self.execution,
            )

        return self.run_packets(candidates, weights, source_failures, run_packet)

    def run_packets(
        self,
        candidates: list[CandidateRecord],
        weights: dict[str, SourceWeight],
        source_failures: list[LabelingAttempt],
        run_packet: Callable[[list[CandidateRecord], Path], list[LabelingAttempt]],
    ) -> tuple[list[LabelingAttempt], dict[str, JsonValue]]:
        weights = _WEIGHTS.validate_python(weights)
        all_candidates = [*candidates, *source_failures]
        indexed = index_records(all_candidates)
        if weights.keys() != indexed.keys():
            raise IntegrityError("Source trust must cover the exact eligible candidates")

        credits: dict[str, dict[str, str]] = {risk.id: {} for risk in self.rules.risks}
        routes: dict[str, frozenset[str]] = {}
        for candidate in all_candidates:
            try:
                routes[candidate.candidate_id] = frozenset(routing(candidate, self.rules))
            except ValueError:
                routes[candidate.candidate_id] = frozenset()
        possibly_routed = {identity: allowed or credits.keys() for identity, allowed in routes.items()}
        scope_candidates: dict[str, JsonValue] = {}
        for identity, allowed in routes.items():
            route_ids: list[JsonValue] = list(sorted(allowed))
            scope_candidate: dict[str, JsonValue] = {
                "candidate": indexed[identity].model_dump_json(),
                "weight": weights[identity],
                "verified_routes": route_ids,
            }
            scope_candidates[identity] = scope_candidate
        scope = fingerprint(
            {
                "category": self.rules.category_id,
                "quota": SUBRISK_QUOTA,
                "candidates": scope_candidates,
            }
        )

        ranked = groupby(
            sorted(candidates, key=lambda row: (-weights[row.candidate_id], row.candidate_id)),
            key=lambda row: weights[row.candidate_id],
        )
        outcomes = index_records(source_failures)
        scheduled = Counter[str]()
        deferred_ids: set[str] = set()
        packet_count = 0
        in_flight: deque[tuple[Path, Counter[str], Future[list[LabelingAttempt]]]] = deque()
        reserved = Counter[str]()

        def commit_next_packet() -> None:
            """Apply results in rank order, then release unused quota reservations."""
            packet_path, packet_reservations, future = in_flight.popleft()
            attempts = future.result()
            self._commit_packet_evidence(packet_path)
            for attempt in attempts:
                identity = attempt.candidate_id
                reference = Path(attempt.attempt_evidence_reference)
                outcomes[identity] = (
                    attempt
                    if reference.parts[0] == "label"
                    else attempt.model_copy(
                        update={"attempt_evidence_reference": (packet_path / reference).as_posix()}
                    )
                )
                label = attempt.label
                if label is None:
                    continue
                if (
                    label.decision == "accept"
                    and label.category_id == self.rules.category_id
                    and label.risk_id in routes[identity]
                    and label.category_confidence
                    == label.risk_confidence
                    == label.severity_confidence
                    == label.class_confidence
                    == "high"
                ):
                    credited = credits[label.risk_id]
                    credited.setdefault(normalized_command(attempt.command), identity)
            reserved.subtract(packet_reservations)

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            for _, tier in ranked:
                tier_rows = list(tier)
                schedulable = [
                    candidate
                    for candidate in tier_rows
                    if any(len(credits[risk]) < SUBRISK_QUOTA for risk in routes[candidate.candidate_id])
                ]
                clustered = similarity_batches(
                    schedulable,
                    self.config.batch_size,
                    encoder=self.execution.encoder,
                )
                clustered_ids = {candidate.candidate_id for group in clustered for candidate in group}
                pending = deque(candidate for group in clustered for candidate in group)
                pending.extend(
                    candidate for candidate in tier_rows if candidate.candidate_id not in clustered_ids
                )
                while pending:
                    if len(in_flight) == self.workers:
                        commit_next_packet()
                    packet: list[CandidateRecord] = []
                    packet_reservations = Counter[str]()
                    while pending and len(packet) < self.config.batch_size:
                        candidate = pending[0]
                        identity = candidate.candidate_id
                        open_routes = {
                            risk for risk in possibly_routed[identity] if len(credits[risk]) < SUBRISK_QUOTA
                        }
                        if not open_routes:
                            pending.popleft()
                            deferred_ids.add(identity)
                            outcomes[identity] = failed_attempt(
                                candidate,
                                QUOTA_DEFERRED,
                                "All compatible subrisk quotas were already met; no labeling was scheduled.",
                                [],
                                fingerprint({"quota_scope": scope, "candidate_id": identity}),
                                QUOTA_RECEIPT,
                            )
                            continue
                        quota_reserved = any(
                            len(credits[risk]) + reserved[risk] + packet_reservations[risk] >= SUBRISK_QUOTA
                            for risk in open_routes
                        )
                        if in_flight and (quota_reserved or not routes[identity]):
                            commit_next_packet()
                            continue
                        if quota_reserved:
                            break
                        pending.popleft()
                        scheduled.update(possibly_routed[identity])
                        if not routes[identity]:
                            outcomes[identity] = failed_attempt(
                                candidate,
                                "routing_unverified",
                                "No verified compatible subrisk could be supplied for labeling.",
                                [],
                                fingerprint({"quota_scope": scope, "candidate_id": identity}),
                                QUOTA_RECEIPT,
                            )
                            continue
                        packet_reservations.update(open_routes)
                        packet.append(candidate)
                    if not packet:
                        continue
                    packet_count += 1
                    packet_path = Path("ranked-packets") / f"{packet_count:06}"
                    future = executor.submit(run_packet, packet, self.out / packet_path)
                    reserved.update(packet_reservations)
                    in_flight.append((packet_path, packet_reservations, future))
                while in_flight:
                    commit_next_packet()

        scoped = Counter(risk for allowed in possibly_routed.values() for risk in allowed)
        unavailable = Counter(
            risk for attempt in source_failures for risk in possibly_routed[attempt.candidate_id]
        )
        deferred = Counter(risk for identity in deferred_ids for risk in possibly_routed[identity])
        failures = {risk: Counter[str]() for risk in credits}
        for attempt in outcomes.values():
            if attempt.status == "failure" and attempt.candidate_id not in deferred_ids:
                for risk in possibly_routed[attempt.candidate_id]:
                    failures[risk][attempt.failure_code] += 1

        subrisks: dict[str, JsonValue] = {}
        for risk, credited in sorted(credits.items()):
            remaining = SUBRISK_QUOTA - len(credited)
            blocked = failures[risk].total()
            failure_counts: dict[str, JsonValue] = dict(sorted(failures[risk].items()))
            summary: dict[str, JsonValue] = {
                "eligible_candidates": scoped[risk],
                "scheduled_candidates": scheduled[risk],
                "deferred_candidates": deferred[risk],
                "source_unverified_candidates": unavailable[risk],
                "verified_candidates": scoped[risk] - blocked - deferred[risk],
                "blocked_candidates": blocked,
                "failure_counts": failure_counts,
                "credited_candidates": list(credited.values()),
                "remaining_to_target": remaining,
                "shortage": None if blocked and remaining else remaining,
                "state": "target_met" if not remaining else "scope_blocked" if blocked else "scope_exhausted",
            }
            subrisks[risk] = summary
        receipt: dict[str, JsonValue] = {
            "category_id": self.rules.category_id,
            "eligible_scope_fingerprint": scope,
            "target_per_subrisk": SUBRISK_QUOTA,
            "unverified_route_count": sum(not allowed for allowed in routes.values()),
            "subrisks": subrisks,
        }
        result = [outcomes[identity] for identity in indexed]
        validate_attempts(
            [
                CandidateRecord.model_validate(
                    candidate.model_dump(include=set(CandidateRecord.model_fields))
                )
                for candidate in all_candidates
            ],
            result,
            self.rules,
        )
        self.out.mkdir(parents=True, exist_ok=True)
        (self.out / QUOTA_RECEIPT).write_bytes(_RECEIPT.dump_json(receipt, indent=2))
        return result, receipt
