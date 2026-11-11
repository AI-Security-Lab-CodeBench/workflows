import json
import re
from urllib.parse import urlparse

if __package__:
    from .category_config import load_taxonomy
else:
    from category_config import load_taxonomy

FIELDS = ("command", "description", "util", "severity", "category", "risk", "source", "seed", "class")
DEFAULT_TAXONOMY = load_taxonomy()
CATEGORY = DEFAULT_TAXONOMY.canonical
RISKS = DEFAULT_TAXONOMY.risk_names
PROVENANCE_FIELDS = (
    "csv_record",
    "candidate_id",
    "source_ids",
    "source_locator",
    "transformations",
    "documentation_urls",
    "tool_version",
    "label_rationale",
    "scenario_id",
    "role",
    "paired_csv_records",
    "synthetic_model",
)
CANDIDATE_FIELDS = (
    "candidate_id",
    "row",
    "source_ids",
    "source_locator",
    "evidence_context",
    "transformations",
    "documentation_urls",
    "tool_version",
    "label_rationale",
    "scenario_id",
    "role",
    "paired_candidate_ids",
    "synthetic_model",
    "status",
)
SOURCE_FIELDS = (
    "source_id",
    "url",
    "locator",
    "revision",
    "retrieved_at",
    "source_type",
    "risks",
    "accessibility",
    "relevance",
    "commands_present",
    "license",
    "license_url",
    "reuse_decision",
    "notes",
)
SECRET_PATTERNS = (
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{40,}\b"),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
    re.compile(
        r"(?i)\b(?:password|passwd|secret_access_key|api_key|access_token)\s*[=:]\s*['\"]?(?!\$|<|REDACTED\b|EXAMPLE\b|PLACEHOLDER\b)[A-Za-z0-9/+_=-]{16,}"
    ),
    re.compile(r"https?://[^\s/:]+:[^\s/@]+@"),
)


class DatasetError(Exception):
    pass


def present(value):
    return isinstance(value, str) and bool(value.strip())


def string_list(value, nonempty=False):
    return (
        isinstance(value, list)
        and (bool(value) or not nonempty)
        and all(present(item) for item in value)
        and len(value) == len(set(value))
    )


def web_url(value):
    if not present(value):
        return False
    try:
        parsed = urlparse(value)
        return (
            parsed.scheme in {"http", "https"}
            and bool(parsed.hostname)
            and not parsed.username
            and not parsed.password
        )
    except ValueError:
        return False


def contains_secret(value):
    text = json.dumps(value, ensure_ascii=False)
    return any(pattern.search(text) for pattern in SECRET_PATTERNS)
