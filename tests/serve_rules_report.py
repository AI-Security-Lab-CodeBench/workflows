"""Serve synthetic aggregate metrics through the real report route on loopback."""

import argparse

import uvicorn
from rules_report_http_fixture import synthetic_report
from test_rules_report import Measurement, metrics_table

from codebench_pipeline.rules_report import render_rules_report


def outcome_counts(cohort: str, support: int, correct: int, wrong: int, abstain: int, error: int) -> list[Measurement]:
    return [
        (cohort, "outcome", "", metric, value)
        for metric, value in (
            ("support", support), ("correct", correct), ("wrong", wrong), ("abstain", abstain), ("error", error),
        )
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=18753)
    parser.add_argument(
        "--variant", choices=("comparison", "zero-recall", "no-measurements", "outcome-edges"), default="comparison",
    )
    args = parser.parse_args()
    port = int(args.port)
    metrics = metrics_table(
        ("overall", "outcome", "", "recall", 0.75),
        ("uncontaminated", "outcome", "", "recall", 85 / 113),
        ("trained", "outcome", "", "recall", 90 / 108),
        ("unseen", "outcome", "", "recall", 0),
        ("overall", "category", "CAT-3", "recall", 0.875),
        ("overall", "category", "CAT-4", "recall", 0.5),
        ("overall", "category", "CAT-5", "recall", None),
        ("unseen", "category", "CAT-4", "recall", 0),
        ("overall", "risk", "CAT-3.1", "recall", 0.875),
        ("overall", "risk", "CAT-3.2", "recall", None),
        ("overall", "risk", "CAT-4.1", "recall", 0.5),
        ("overall", "severity", "high", "recall", 0.75),
        ("overall", "action", "review", "recall", 0.625),
        *outcome_counts("overall", 120, 90, 12, 12, 6),
        *outcome_counts("uncontaminated", 113, 85, 12, 10, 6),
        *outcome_counts("trained", 108, 90, 12, 0, 6),
        *outcome_counts("unseen", 12, 0, 0, 12, 0),
        ("overall", "category", "CAT-3", "support", 90),
        ("overall", "category", "CAT-4", "support", 30),
        ("overall", "category", "CAT-5", "support", 0),
        ("unseen", "category", "CAT-4", "support", 12),
        ("overall", "category", "CAT-3", "fpr", None),
        ("overall", "overlap", "", "rows", 7),
        ("overall", "overlap", "", "unique_commands", 4),
    )
    if args.variant == "zero-recall":
        metrics = metrics_table(
            ("overall", "outcome", "", "recall", 0),
            ("trained", "outcome", "", "recall", None),
            ("overall", "category", "CAT-3", "recall", 0),
            ("overall", "outcome", "", "support", 120),
            ("overall", "category", "CAT-3", "support", 90),
        )
    elif args.variant == "no-measurements":
        metrics = metrics_table()
    elif args.variant == "outcome-edges":
        metrics = metrics_table(
            *outcome_counts("overall", 6, 0, 0, 0, 6),
            *outcome_counts("uncontaminated", 0, 0, 0, 0, 0),
            *outcome_counts("unseen", 3, 0, 0, 3, 0),
            *outcome_counts("inconsistent", 5, 1, 1, 1, 1),
            ("trained", "outcome", "", "support", 4),
            ("trained", "outcome", "", "correct", 1),
            ("trained", "outcome", "", "wrong", None),
            ("category_only", "category", "CAT-3", "support", 2),
        )
    payload = render_rules_report(metrics).encode("utf-8")
    with synthetic_report(payload, port) as (app, path):
        print(f"Rules report: http://127.0.0.1:{port}{path}", flush=True)
        uvicorn.run(app, host="127.0.0.1", port=port)


if __name__ == "__main__":
    main()
