from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import ObjectStore, sha256
from codebench_pipeline.storage import IntegrityError


def report_key(digest: Digest) -> str:
    return f"reports/rules-eval/{digest}.html"


def report_url(digest: Digest) -> str:
    return f"/codebench/api/reports/rules-eval/{digest}.html"


def read_report(store: ObjectStore, digest: Digest) -> bytes:
    payload = store.read(report_key(digest))
    if sha256(payload) != digest:
        raise IntegrityError("Evaluation report does not match its content address")
    return payload
