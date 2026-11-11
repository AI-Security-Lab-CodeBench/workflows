from codebench_pipeline.human_review import CompletionToken, HumanReviewError, ReviewSnapshot
from codebench_pipeline.human_review_transactions import HumanReviewTransaction
from codebench_pipeline.review_snapshots import ResolvedReview


def publication_snapshot(resolved: ResolvedReview, stage: str) -> ReviewSnapshot:
    if stage == "annotations":
        return resolved.snapshot
    raise HumanReviewError("conflict", "Only shared human review can authorize publication")


def authorize_publication(
    resolved: ResolvedReview, token: CompletionToken, transaction: HumanReviewTransaction
) -> ReviewSnapshot:
    snapshot = publication_snapshot(resolved, token.review_stage)
    transaction.authorize_completion(snapshot, token)
    return snapshot


def preparation_authorization(
    resolved: ResolvedReview, transaction: HumanReviewTransaction
) -> CompletionToken:
    human = transaction.ensure_current_batch(resolved.snapshot)
    if human.completion is None:
        raise HumanReviewError("pending", "Complete shared human review before publication")
    transaction.authorize_completion(resolved.snapshot, human.completion)
    return human.completion
