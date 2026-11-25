# CodeBench pipeline architecture

### Inspecting a run in MLflow

Open the [local MLflow dashboard](http://localhost:5000), select the **CodeBench**
experiment, and open **Traces**. A completed Dagster run appears as one trace.
The **Input** and **Output** previews show the job name and execution status,
not the job's actual inputs or outputs. Find `capture_rules` and check its Dagster run ID
to distinguish rematerializations of the same partition. Expand the step,
then a numbered agent invocation and **Cycle #1**. Under `complete`, select
**LLM: chat_completions** to inspect an individual model call. Under `tools`,
select **Tool: …** to see which tool ran, when, and for how long. If one model
turn dispatches several tools, they appear separately under the same cycle.
The next `complete` starts a new cycle; the adaptive discovery planner uses
`choose` and `search` instead. Steps that never invoked an agent have no cycles.

A node visit is not proof of a call. Historical traces recorded before
call-level instrumentation have no call leaves; opening an old trace or
retrying its export cannot recover those measurements. Inspect a newly
executed run for the detailed hierarchy.

For payload tracing, set `CODEBENCH_TRACE_PAYLOAD_DIR` in the private pipeline
environment for **both** workers, to a directory on their shared dataset
volume (for example `/var/lib/codebench/datasets/trace-payloads`). Enable
this only for a tracking server whose readers may see the underlying data.
This is a content disclosure setting, not just a logging verbosity setting.
It is off by default.

On a newly executed **LLM: chat_completions** leaf, open **Inputs / Outputs**.
Inputs are the outgoing application request, including ordered messages, model,
offered tools and generation options when sent. Compare successive calls to see
which prior messages survived a context reduction. Outputs show the provider
response or an explicit error/omission; token usage is only available when the
provider returned it. For capacity-admitted calls, **Attributes →
`codebench.context_window`** gives the tokenizer's prompt count, effective
context limit and requested output-token budget. Plain requests do not
invent a context-window limit. These are client-visible messages, not the provider's
hidden context, model thoughts or an estimate of tokens. **Tool: …** leaves
show the arguments and return value for that dispatch. Compare the tool-call ID
with the assistant message and the following request where the protocol supplies
one. **Attributes → `codebench.call_summary`** retains the execution and business
outcomes. The graph and cycle outputs remain summaries rather than copies of
every payload.

Dagster INFO records contain only content digests, not the payload bodies.
The observer resolves those digests from the shared directory, verifies them
and sends the JSON to MLflow. Restrict access to **both** stores and include
both in retention decisions. A missing or corrupt payload is marked unavailable,
not reconstructed. Clearing the worker copy does not erase an already exported
MLflow trace. Transport credentials are not call inputs. Sensitive content may
be redacted or withheld; arbitrary user text is still sensitive. Old traces have
no recoverable payloads.

To check the view without spending provider quota, use the synthetic trace
job from the running release. This explicitly writes one `flow_trace_qa`
run to Dagster; it does not materialize pipeline assets:

```sh
docker compose -p codebench --env-file "$deployment_env" exec -T \
  -e PYTHONPATH=.:tests webserver python -m tests.e2e.test_flow_trace --deployed
```

Find the printed `CODEBENCH_TRACE_QA_RUN_ID` in **CodeBench → Traces**. For
the rules agent, expand `learn_rules → rules_capture #1 → Cycle #1`, then
inspect the `complete` and `tools` branches side by side.

Dagster is the source of truth while a run is in progress. The trace observer
reads persisted events after termination; it does not run assets, enable
materialization, or make an MLflow outage a reason to repeat a model call.
Select a **Cycle** to see its actions and observed results; select a tool
leaf for its own outcome. An accepted rule ID identifies a rule the learner
actually accepted, not just one the model proposed. A rejected action can
be followed by a successful cycle in the same run.

For the persisted result, select the producing **step** or the **run root**
and open **Inputs / Outputs**. Rules runs report their materialized assets,
partitions, available digests and counts. Evaluation reports exact matches,
wrong matches, abstentions and evaluator errors separately: a successful
Dagster run does not mean the rules matched every held-out row. When payload
capture is disabled, inspect the authorized pipeline evidence for row content.
The `mlflow_run_trace_projection` sensor checks for terminal runs at a minimum
interval of 15 seconds. Its first tick starts a new observation window; it
does not backfill runs from before tracing was enabled. A failed delivery
keeps the unacknowledged run pending. Run and span IDs remain stable on replay.

For independent category-partitioned imports that do not run collection or labeling,
see [Loading pinned train and test splits](split-loading.md).

### Teaching Bashguard rules

The `dataset_merge` group is the presentation group for `codebench/full_dataset`;
the underlying train and test members retain their separate identities. The rules
branch learns from train and scores a frozen bundle on test. Running it does not
require collection, relabeling, or approval of an existing review queue.

| Stage | Data available | Decision |
| --- | --- | --- |
| `rules_train_sample`, `rules_test_sample` | Their own persisted `train/merged_rows` or `test/merged_rows` | Materialize one private, stratified sample per split. Store only aggregate lineage metadata in Dagster. |
| `rules_capture` | One category projection of `rules_train_sample` | Propose specific CEL predicates and positive and negative fixtures. Require a correct hit, passing fixtures and no wrong matches or evaluator errors on that projection. |
| `rules_dedup` | Selected category captures and `rules_train_sample` | Replay every captured package on the global train sample before nominating similar rules and rules sharing hits. A failed replay blocks learning. The judge can inspect sampled train evidence and submit a justified add/drop diff. A rewrite is a fresh rule, not an in-place change. |
| `rules_finalize` | Deduplicated packages and `rules_train_sample` | Replay the complete surviving bundle on the global train sample and freeze it without consulting the holdout. |
| `rules_eval` | Frozen bundle, train sample lineage metadata, and `rules_test_sample` | Score only the holdout sample. Measure wrong predictions, abstentions, failures, coverage and exact-label accuracy. An uncovered category or an undefined metric is not evidence of perfect precision. |
| `rules_eval_report` | Held-out aggregate metrics only | Publish an interactive HTML summary without model calls or row-level data. |

The trained cohort includes every selected training category, even if no
learned rule survived for that category.

The rule-authoring guide and the evaluator executable are independently pinned:
the former determines what the agents see; the latter determines what their
rules do. A successful batch response must match both the requested evaluator
identity and the submitted bundle digest. The pipeline owns the data and scoring;
[the internal Bashguard service](../../bashguard/HTTP.md) only parses and
classifies the commands sent to it. Errors, including CEL syntax diagnostics,
are learning feedback, not negative matches. A fixture expecting `other` means
the rule must decline to label that example. The judge can justify removing a
retired rule's tests; every addition is evaluated anew. None of these decisions
may consult the held-out report.

Run these manual jobs with `python -m codebench_pipeline.dataset_cli` in the
worker. They do not enable a sensor.

1. On an existing deployment, run
   `rules-register --migrate --runtime-version "$RUNTIME_VERSION" --guide-sha256 "$GUIDE_SHA256"`.
   This copies the exact active categories and evaluator origin from the last
   successful rules registration, drops `train` and `test`, and approves the
   new evaluator and packaged guide identities. Use the evaluator executable's
   SHA-256 with its `sha256:` prefix and the plain SHA-256 of
   `business-configs/bashguard/guide.json`. Omit an override only when its
   identity is unchanged. Migration refuses to overwrite a different approved
   policy. For initial setup or intentional changes use
   `rules-register --config - --sha256 <digest>` with private JSON on standard
   input; hash the exact file bytes you send. The policy supplies `categories`,
   `runtime` (`base_url`, `version`) and `prompt_guide` (`path: "guide.json"`,
   `sha256`) and optional `sample`: omit it or set `{"count": 200}` to take
   up to 200 rows **per split, not per category**, or set `{"percent": 100}`
   for every row. Percentages must be greater than 0 and at most 100, and the
   resulting count rounds up. Supply only one mode. Split digest and row-count
   fields are rejected. The CLI checks the guide and refreshes only
   `datasets/global_config`; it does not rewrite the dataset registration.
2. Run `train-load --category <category>` for each configured train category
   (for CAT-6-only train: `train-load --category CAT-6`), then `train-merge`,
   then `rules-sample-train`. These commands use the same `--home`. The merge
   and sample both use the singleton `codebench` partition. Run
   `rules-capture --category <category>` for each approved learning category
   and then `rules-learn`. Each capture category partition maps to the **same**
   singleton train sample and receives only its category's rows and source URLs.
   Dedup and finalization receive the global sampled train corpus, including
   any sampled rows from non-capture categories. No learning job reads test.
3. Run `test-load --category <category>` for **every** configured holdout
   category, then `test-merge`, `rules-sample-test`, and `rules-eval`.
   The merge, sample, evaluation and report use the `codebench` partition.
   Test sampling does not filter by approved learning categories, so unseen
   categories and `modify` gold labels are eligible. Each split has one
   deterministic order: categories are interleaved by population, and within
   each category the joint risk, severity and action strata are interleaved
   the same way. Entries are spaced at the centers of equal-sized intervals
   for each group, so a singleton normally appears near the middle rather
   than at the front. Stable hashes of the original snapshot row IDs order
   rows within each stratum, without changing their IDs. Count and percent
   take prefixes of this same order, including at 100%. Train puts one row
   from each approved capture category first and fails if any such category
   is missing or the budget cannot cover them.
4. Inspect `codebench/rules_eval` for the held-out metrics and
   `codebench/rules_eval_report` for the standalone report. Neither is an
   input to learning.

Each sample is pinned to its source merge, selection policy, sampler version
and selected row IDs; train also pins the approved capture categories.
A changed merge, budget, sampler version or train capture category list requires rematerializing
the affected sample; a changed train sample also requires recapturing approved
categories and rerunning `rules-learn` before evaluation. Changing only the
test merge does not invalidate learning. Old captures and failed capture runs
from before sampling cannot be reused. These are manual jobs: downstream jobs
do not silently resample or fall back to full merges.

The rules assets and their configuration mappers are grouped under `rules`.
In their inspector, **Native evaluation scope** describes how Bashguard applies
a rule (`simple` or `ast`), not which rows finalization replays. A legacy empty
scope is displayed as `default` and behaves like `simple`.

CodeBench builds a labeled dataset of shell commands from source material. A released row must be traceable to the text it came from, the evidence for its label, and the decision that admitted it. Commands are data throughout: no stage executes a collected command.

```text
Pinned datasets ─────┐
Pinned documentation ├─→ merge candidates → filter → label against evidence
Keenable snippets ───┘                                   ├─ unsupported → audit
                                                        └─ supported → exact deduplication
                                                                        ↓
                                                                category eligibility
                                                                        ↓
                                                           resolve required review
                                                                        ↓
                                                                  dataset release
```

**Orchestration.** The [Dagster definitions](../../codebench_pipeline/definitions.py) connect persistent assets by their inputs. A partition identifies the dataset being built. Configuration assets load and validate the authored settings; each consumer receives just its settings through a mapping in its own group. The graph declares what depends on what. Dagster selects and runs the work, and the Data and Review panels read persisted outputs without rerunning it.

`global_config` publishes the validated registration, and `category_rules` extracts its taxonomy as a separate persisted asset. Mappings for repository collection, merge and deduplication read only `global_config`. Proposal validation and category eligibility read only `category_rules`. Keenable candidate collection, filtering, labeling, review queue and release mappings read both. A run that selects a mapping without its inputs loads their existing materializations for that partition.

Each operation has a dedicated typed mapping containing the fields its consumer uses. Repository capture receives pinned locations and download limits; extraction receives parser pins and preserves each candidate's origin. Only the merge mapping supplies source weights. Filtering gets routing patterns. Labeling gets its prompt taxonomy, selected examples, inference settings and live grounding policy, without source-acquisition declarations or taxonomy version metadata. Review receives label validation rules, editable columns and the publication cap that binds its completion snapshot; the cap does not select review rows. Release applies the cap and receives publication taxonomy and candidate-provenance settings. Human decisions take their dataset identity from the partition and their authorization from the completed review snapshot, so they have no configuration mapping.

**Keenable collection.** The `keenable/command_candidates` asset uses the authored inspection seeds in `business-configs/categories/cat3/keenable/declaration.json`. CAT-3 allows up to five searches across the seeds. Each turn the model sees the authored instructions, active risk definitions and guidance, severity definitions, prior query IDs, novelty counts, and a few short phrases drawn from excerpts using the seed vocabulary. It can author a new context phrase for the same seed, choose another seed, or finalize. The asset validates the phrase and adds it to the fixed benign search topic. The model never receives full excerpts, page URLs, or credentials.

The parser reads complete fenced shell examples as text and only emits literal lines that match the selected seed's allowed command syntax. It keeps the returned page URL, exact snippet and byte range for accepted spans. Each completed search records parsed candidates and hit outcomes in the attempt evidence as it runs. A later failure keeps earlier evidence and blocks publication of a partial table. A successful empty search can merge. The source type is `web_page`, and the `keenable` value identifies the snippet provider without establishing origin rights. The branch does not fetch origin pages, execute commands, or assign labels. The seed's risk family describes the type of harmful operation the benign example contrasts with; downstream labeling still checks the command and its actual impact against separate applicable documentation.

The dataset bundle sets a separate output token limit and timeout for discovery. Discovery uses the labeling endpoint, served model, credential binding and reasoning policy by default. A distinct plain discovery binding does not inherit the labeling reasoning policy. When guard admission is configured, discovery shares the capacity ledger and never falls back to plain inference. With the updated configuration registered and `KEENABLE_API_KEY` available to the service, run `python3 scripts/stack.py through datasets/keenable/command_candidates` to build the candidate table without running labeling.

The labeling, review and release decisions are explained in [How commands become release rows](algorithms.md).
