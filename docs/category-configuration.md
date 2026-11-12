# Category-owned taxonomy

Each category's business configuration owns its taxonomy and labeling policy.
Edit `business-configs/categories/cat3/category.json`, rather than Python enums.
The generic base prompt is `business-configs/labeling-base.md`; category instructions
and examples are separate files beside `category.json`. Other categories can
use independent local configuration files, but CAT-2 is an excluded local example,
not a shipped category. Categories need not share subrisk names, counts, numbering or utilities.
Risk IDs follow `CAT-N.M`, must use their category prefix, and must be unique;
canonical CSV values include both the configured ID and name.

The current dataset pipeline carries this rules snapshot through filtering,
single-invocation per-command annotation and release. `command_patterns` routes candidates;
the complete policy, definitions and guidance are part of the isolated model
instruction. Patterns never assign a safety class or severity.

## Standalone tooling and historical CAT-3

The standalone scripts default explicitly to the shipped
business configuration. Their legacy `CATEGORY` and `RISKS` exports are
derived from the same category source, not separate taxonomies. Historical labeling packets and
export commands retain their CAT-3 workflow, now backed by this configuration.
Use the current dataset pipeline for another category's full annotation workflow.

`command_filter.py`, `prefilter.py`, `postfilter.py`,
`prepare_review_candidates.py` and `validate_dataset.py` accept
`--category-config PATH` for category-specific extraction and validation.
Supply the same category business configuration throughout these steps.
Their Python routing APIs accept a `Taxonomy` loaded with
`category_config.load_taxonomy(path)`.

For the quote-aware standalone router, a risk may define `shell_routes`:
a mapping from utility basename to a regular expression over normalized
positional arguments. Options and their known values, wrappers and executable
paths are stripped before matching; quoted data is never executed or expanded.
When a risk has no `shell_routes`, its `command_patterns` match the normalized
utility plus arguments. The shipped CAT-3 routes preserve the historical utility
and subcommand boundaries, including multi-risk inspection commands. The
current dataset filter continues matching `command_patterns` against original
command text.

`schemas.validate_commands(..., eligible=True, rules=rules)` validates eligible
IDs against the supplied `CategoryRules`. Structural models no longer contain
a five-value risk enum. `schemas.validate_export(..., rules=rules)` builds its
canonical category/risk checks from that snapshot. Omitted rules retain the
explicit shipped CAT-3 default. Missing, unknown and foreign IDs are not admitted.

The v0 class and severity vocabularies and generic shell parsing flags remain
code contracts; they are not category subrisk taxonomies. Historical CAT-3
documentation and test fixtures may still name concrete risks.

## Verification

The focused regressions use a category with two noncontiguous IDs, `CAT-91.7` and `CAT-91.19`,
and exercise configuration parsing, shell routing, eligible JSONL and CSV
validation. Existing command-filter tests cover historical CAT-3 boundaries.

From the repository environment, run:

```text
python3 -B -m unittest discover -s tests -p 'test_category_taxonomy.py' -v
python3 -B -m unittest discover -s tests -p 'test_command_filter.py' -v
python3 -B -m unittest discover -s tests -v
```

These are inert tests. Package and standalone imports are exercised in clean
subprocesses. Multicategory cases use authored synthetic fixtures; they do not
depend on excluded CAT-2 configuration or downloaded source captures.
