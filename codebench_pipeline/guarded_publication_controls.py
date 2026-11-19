import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

from codebench_pipeline.guarded_publication import ReleaseProposal
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_schema import timestamp
from codebench_pipeline.human_review_transactions import HumanReviewTransaction

Role = Literal["operators", "stewards"]
Action = Literal["hold", "clear", "attest", "revoke"]


class PublicationAuthorities(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    operators: tuple[int, ...]
    stewards: tuple[int, ...]


def actor_uid(home: Path, role: Role) -> int:
    """Resolve the authenticated local account against the dataset owner's private role file."""
    path = home / "publication-authorities.json"
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError as error:
        raise HumanReviewError("ineligible", "No publication authority has been configured") from error
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if info.st_uid != home.stat().st_uid or info.st_mode & 0o022:
            raise HumanReviewError(
                "ineligible", "Publication authorities must be controlled by the home owner"
            )
        authorities = PublicationAuthorities.model_validate_json(stream.read())
    actor = os.getuid()
    if actor not in (authorities.operators if role == "operators" else authorities.stewards):
        raise HumanReviewError("ineligible", f"Authenticated account is not listed in {role}")
    return actor


def require_proposal_authorized(
    tx: HumanReviewTransaction, dataset_id: str, proposal: ReleaseProposal
) -> None:
    state = next(
        tx.connection.execute(
            """SELECT
            COALESCE((SELECT action = 'hold' FROM v2_publication_controls
                WHERE dataset_id=%s AND action IN ('hold', 'clear')
                ORDER BY sequence DESC LIMIT 1), false) AS held,
            COALESCE((SELECT action = 'attest' FROM v2_publication_controls
                WHERE dataset_id=%s AND proposal=%s AND action IN ('attest', 'revoke')
                ORDER BY sequence DESC LIMIT 1), false) AS attested""",
            (dataset_id, dataset_id, proposal.digest),
        )
    )
    if state["held"] is True:
        raise HumanReviewError("pending", "Guarded release is prepared under an operator hold")
    proposal.require_resolved_sampling()
    if state["attested"] is not True:
        raise HumanReviewError(
            "pending", "Guarded release requires steward attestation for this exact proposal"
        )


def record_control(
    tx: HumanReviewTransaction, dataset_id: str, action: Action, *, actor: int, proposal: str = ""
) -> None:
    tx.lock_dataset(dataset_id)
    tx.connection.execute(
        "INSERT INTO v2_publication_controls(dataset_id, action, proposal, actor_uid, recorded_at) "
        "VALUES (%s, %s, %s, %s, %s)",
        (dataset_id, action, proposal, actor, timestamp()),
    )
