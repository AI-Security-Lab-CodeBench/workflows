# Historical standalone labeling and review exchange

This guide describes the standalone exchange CLI and its legacy numeric-confidence,
nine-column export contract, not the current Dagster release path. The active
dataset pipeline uses four low/medium/high label confidences and one isolated LLM
invocation per eligible command. The local agent prepares source manifests outside
Dagster after reading README/license/origin evidence and withholding unresolved
eligibility. Complete declared-source retrieval feeds extraction/filtering,
semantic docs and `proposed_annotations`; branch outputs merge before conservative
exact deduplication, uncertainty-only human review and one `dataset_release`.
There is no active independent per-command reviewer or `reviewed_annotations`
stage, and no manufactured `review_verdict` or `review_confidence`. Historical
artifacts remain readable. Any medium/low among category, risk, severity and class
confidence requires human review; all four high permits automatic admission only
for machine-eligible rows. An empty queue completes under a policy actor with zero
human decisions; a nonempty queue requires actual terminal decisions and explicit
completion. See
[confidence-review-policy.md](confidence-review-policy.md) for admission,
snapshot validity and the ten-column release contract.

Everything below describes the legacy standalone exchange, including its separate
review invocation, numeric confidences and export gate. Those commands aren't the
operational recipe for the current workflow. Retain historical policy snapshots
with historical packets rather than treating the current single-command policy
as a new standalone review contract.

This CLI uses only the Python standard library and repository inert validators.
It never invokes a provider, shell command from the dataset, network, or credential
store. A coordinator supplies each packet to a fresh isolated agent with no tools,
then saves its response. Review requires another isolated invocation; using the
same model is permitted, but continuing the labeler's conversation is not.
Model names and timestamps are audit claims, not structural proof of independence.
Agentic approval is not human approval or proof of source truth.
Both labeling and review envelopes optionally accept a nonblank string
`invocation_id` for the coordinator's native-agent or supported-CLI receipt.
It is preserved verbatim when the labeling envelope is embedded in review input;
it is not required and does not bind the exchange to any provider.

## Prepare

```text
python3 -B scripts/label_pipeline.py prepare --candidates selected.jsonl --sources sources.jsonl --semantics semantics.jsonl --out exchange --batch-size 8
```

`--candidates` is a JSONL file using the current `CANDIDATE_FIELDS`, or a directory
searched recursively for `candidates.jsonl`. `--sources` uses `SOURCE_FIELDS`.
Candidate commands and factual context are not normalized. Selected candidates
must include all referenced source IDs. Pairing and deduplication are deferred:
missing counterparts, historical links and repeated command text do not block
labeling or acceptance. Legacy status is never an acceptance gate.
All output directories must be absent: use a fresh path for each attempt. Existing
artifacts are never replaced, so no backup of old artifacts is needed.

Semantics is JSONL with exactly these fields:

```json
{"utility":"terraform","subcommands":["destroy"],"url":"https://developer.hashicorp.com/terraform/cli/commands/destroy","retrieved_at":"2026-11-04T00:00:00Z","snippet":"Inspected documentation excerpt supplied by the coordinator.","version_limits":"Record the inspected version or explicitly explain unknown limits."}
```

`subcommands` is a nonempty list of strings. Other fields are nonempty strings;
URLs must be HTTP(S). Retrieval timestamps accept ISO dates or datetimes.
Every candidate needs matching documentation by utility or command token.
The coordinator must gather sufficient snippets for every significant subcommand,
flag and composed operation; matching a utility is not a
semantic validation. The independent reviewer checks actual coverage.

Output `exchange/packets/label-00001.json` and subsequent files contain:

- `stage`, `candidate_ids`, factual `input`, and `response_schema`;
- a separate `system_prompt` policy plus user `prompt` with schema and inert input;
- `packet_sha256`, SHA-256 of the packet excluding that field, serialized as
  sorted-key, compact JSON with `ensure_ascii=False` and `allow_nan=False`.

The input includes whole command, context, origin, source ledger entries and
documentation snippets. Historical roles, scenario links, pair IDs and other
records presented as counterparts are omitted.
Previous class, severity, category, risk, description, label rationale and status
are omitted. Utility is retained as factual routing context. A candidate digest
binds the unmodified original candidate (including omitted fields) for drift
detection without exposing those verdicts.

Each fresh invocation receives `packet["system_prompt"]` as its actual system
message and `packet["prompt"]` as user input. The versioned
[`labeling-policy.md`](labeling-policy.md) incorporates the user's Google
guideline and local contracts, with the user's current-stage overrides; the
packet hash binds its exact text. All applicable rules are inline. Agents are
not expected to load a file or follow a URL to discover instructions. Independent
review inherits the exact policy snapshot used for its label packet.

Policy provenance, not additional runtime instructions: the source guideline
was retrieved November 4, 2026 from
https://docs.google.com/document/d/1fW68oYvEuQq4QJ4QqTv_22w2CxQBMNpztQ0iwaAdhoI/edit?tab=t.0 .
Local task/data contracts supply the canonical taxonomy and output format.
The user's November 5 instruction defers linking and deduplication.

The native saved-account runner creates one ephemeral, no-tools call per packet
without reading, copying or printing account tokens:

```text
python3 -B scripts/run_label_agents.py --packets exchange/packets --out responses --model chatgpt-subscription/gpt-6-luna --workers 3
```

It passes system instructions through `--system-prompt`, disables tools,
extensions and context-file discovery, validates returned JSON and retains
actual CLI events/model/invocation receipts. The CLI uses a prompted schema
followed by strict local validation, not provider-side constrained decoding.
Run it again on independent review packets with a distinct output directory.
Existing responses are not re-invoked; review/export still validates them.

Alternatively save actual structured results at a separate response directory
using the packet's exact filename, e.g. `responses/label-00001.json`:

```json
{
  "packet_sha256": "copy packet_sha256 exactly",
  "model": "actual model identifier or honest alias",
  "responded_at": "2026-11-04T00:00:00Z",
  "result": {"labels": []}
}
```

The labels array must contain exactly one object for every packet candidate ID:
`candidate_id`, `decision` (`accept`, `needs_review`, `reject`), canonical
`category`, canonical `risk`, `severity` (`critical`, `high`, `medium`, `low`),
`class` (`allow`, `deny`, `ask`), a single Russian sentence `description`, `util`,
`category_confidence`, `risk_confidence`, `severity_confidence`, `class_confidence`,
nonblank `rationale` of at most 400 characters, and nonempty HTTP(S)
`documentation_urls`. A pending rationale identifies a concrete problem and
the evidence needed to resolve it.
Documentation URLs must refer to snippets supplied for that candidate.

All four confidences are finite numbers in [0,1], excluding booleans. They express
certainty in each assigned label. Category/risk confidences are not ordinal
severity scores. Severity confidence is certainty that the selected severity is
correct, not damage magnitude. Class confidence is not a probability of safety.
No confidence threshold replaces review or establishes safety.

## Independent review

Malformed labeling responses can be repackaged without rerunning valid work:

```text
python3 -B scripts/label_pipeline.py repair --packets exchange/packets --responses responses --out repaired --batch-size 6
python3 -B scripts/run_label_agents.py --packets repaired/packets --out repaired/responses --model chatgpt-subscription/gpt-6-luna --thinking high --workers 3
```

Repair validates and preserves successful packet/response bytes, splits only
missing responses into smaller packets, and records parent hashes in
`recovery.json`. Failed invocation evidence stays in the original response tree.
Use the repaired packet and response directories for subsequent review/export.
Do not repair semantic disagreements into approvals; retain those for review.

The native runner disables local tools. A provider may still emit a hosted
tool block; the runner rejects such output rather than admitting it as isolated
annotation evidence. Failed attempts remain auditable.

```text
python3 -B scripts/label_pipeline.py review-prepare --packets exchange/packets --responses responses --out independent
```

This validates the complete labeling exchange before writing
`independent/packets/*.json`. Every review packet binds the original labeling
packet and exact response envelope. Its prompt asks a separate reviewer to
validate source truth, labels and all confidences, documentation and version
limits for each standalone record. It does not check or require pair behavior.

Save new envelopes in `reviews/`, with filenames matching the review packet:

```json
{
  "packet_sha256": "copy the review packet hash, not the labeling hash",
  "model": "actual reviewer model identifier or honest alias",
  "responded_at": "2026-11-04T00:01:00Z",
  "result": {"reviews": [{"candidate_id":"CAT-3.1-example","verdict":"needs_review","rationale":"Explain the evidence and any unresolved disagreement."}]}
}
```

`verdict` is `approve`, `needs_review`, or `reject`. Every candidate needs exactly
one review, including rejected labels. Envelopes use timezone-aware ISO datetimes.
Missing, duplicate, unknown IDs/files, altered hashes, extra/missing fields, invalid
enums, nonfinite/bool/out-of-range confidences, blank rationales and malformed
evidence fail closed. Hashes detect alteration relative to the exchange, not
authenticity against a malicious actor able to replace the entire exchange.

## Export

```text
python3 -B scripts/label_pipeline.py export --candidates selected.jsonl --sources sources.jsonl --packets exchange/packets --responses responses --review-packets independent/packets --reviews reviews --out bundle --limit-per-risk 250
```

Both exchanges and their exact linkage are validated before any output is
published. Current candidate/source content must match packet snapshots.
Only `accept` plus independent `approve` may enter the accepted set.
Risk/source metadata disagreement, missing tool version, source ineligibility,
review disagreements and individual-record cap exclusions enter a reasoned pending queue.
Schema or snapshot failures abort the entire export.

The command, source, seed, origin IDs, locator and factual context remain exact.
The five labeled row fields (category, risk, severity, class, description)
are assigned from the fresh label. Documentation URLs and label rationale in
provenance are explicitly taken from that reviewed label. Current pair fields
are empty and roles are `unpaired` legacy-schema values, not label constraints.
Every record is admitted independently in candidate-ID order under its risk's
cap. No command-text deduplication is performed; distinct record identities are
still required. `command_dedup.py` remains available for a later stage only.

The canonical `util` keeps the factual executable naming supplied with the
command. A model display such as `oci os object bulk-delete` may normalize to
`oci` only when it matches that invocation; an unrelated utility fails admission.
Original displays remain in `proposed_util` in the all-label tables, with accepted
normalizations recorded in `data/review/utility-normalization.jsonl`. No risk,
severity, class, confidence or command text is changed by this presentation step.

The in-memory bundle must pass `validate_bundle` before staging files under a
temporary sibling and publishing a new output directory. Temporary files are
cleaned automatically; originals and previous decisions stay outside the output.
The output includes:

- `commands.csv`: canonical nine fields, with no review-only fields.
- `data/processed/provenance.jsonl`, per-risk
  `data/candidates/CAT-3.N/candidates.jsonl`, and `research/sources.jsonl`.
- `data/review/pending.jsonl`: all excluded candidates with original evidence,
  proposed labels, review verdicts and exclusion reasons.
- `review/commands.csv` and `review/CAT-3.N.csv`: nine fields plus candidate ID,
  one-based CSV data record, context, exact URLs and locator, docs links, four
  confidences and labeling/review rationale. URL lists are readable line-separated
  text, not JSON arrays in cells.
- `review/pending.csv`: flat columns beginning with candidate ID, command, risk,
  class, severity, short `llm_rationale` and `exclusion_reason`. Separate decisions,
  confidences and label/review rationales remain visible. `rationale_source`
  identifies the model judgment quoted. `evidence_jsonl` and one-based
  `evidence_record` locate the complete nested evidence; no evidence object is
  embedded in a CSV cell.
- `review/labeled_all.csv` and `review/labeled-CAT-3.N.csv`, containing every
  proposed label, confidence, source link and independent verdict, including
  withheld/unpaired records with explicit exclusion reasons.
- `statistics.json`, also printed on stdout: actual counts, per-risk class,
  severity and utility coverage, pending count, `target_per_risk: 250`, and each
  risk's deficit against 250, including counterexamples. A custom cap does not
  redefine the collection target.

Empty accepted sets still produce a validated nine-column header and complete
pending ledger. This CLI performs static exchange/ledger checks only. Source
truth, license compatibility, semantic safety and genuine invocation isolation
remain responsibilities of collection, independent agentic review and subsequent
human review.

## Python interfaces

With `scripts` on `sys.path`, `label_pipeline.prepare(candidates, sources,
semantics, out, batch_size=8)` and `label_pipeline.review_prepare(packets,
responses, out)` return statistics dictionaries. `pipeline_export.export` accepts
the export CLI argument names with underscores (`review_packets`,
`limit_per_risk`) and returns the export statistics. Paths accept `Path` or strings.
Contract failures raise the repository's `DatasetError`; CLI failures return 1
and print a JSON error object. The CLI entry point is `label_pipeline.main(argv)`.
