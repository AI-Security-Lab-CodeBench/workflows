from base64 import b64encode
from hashlib import sha256
from typing import TYPE_CHECKING

from codebench_pipeline.rules_report_markup import MARKUP
from codebench_pipeline.rules_report_script import SCRIPT
from codebench_pipeline.rules_report_styles import STYLES
from codebench_pipeline.rules_report_tokens import TOKENS

if TYPE_CHECKING:
    from pandera.typing import DataFrame

    from codebench_pipeline.rules_artifacts import RulesMetricsTable

_CSS = TOKENS + STYLES


def _digest(content: str) -> str:
    return b64encode(sha256(content.encode("utf-8")).digest()).decode("ascii")


def render_rules_report(metrics: "DataFrame[RulesMetricsTable]") -> str:
    """Render validated aggregate holdout metrics as an offline UTF-8 HTML document."""
    rows = metrics[["cohort", "axis", "label", "metric", "value"]].to_json(
        orient="records", double_precision=15,
    ).replace("<", "\\u003c")
    policy = (
        f"default-src 'none'; script-src 'sha256-{_digest(SCRIPT)}'; "
        f"style-src 'sha256-{_digest(_CSS)}'; base-uri 'none'; form-action 'none'"
    )
    return MARKUP.substitute(policy=policy, styles=_CSS, rows=rows, script=SCRIPT)
