# CodeBench pipeline architecture

### Inspecting a run in MLflow

Open the [local MLflow dashboard](http://localhost:5000), select the **CodeBench**
experiment, and open **Traces**. A completed Dagster run appears as one trace.
The **Input** and **Output** previews show the job name and execution status,
not the job's actual inputs or outputs. Find `capture_rules` and check its Dagster run ID
to distinguish rematerializations of the same partition. Expand a step to
inspect its agent invocations. Each visit to a LangGraph node has its own
timing and status: a `complete → tools → complete` sequence means the model
resumed after the tool step. A step that did not invoke an agent has no such
children. The adaptive discovery planner uses `choose` and `search`.

Dagster is the source of truth while a run is in progress. The trace observer
reads persisted events after termination; it does not run assets, enable
materialization, or make an MLflow outage a reason to repeat a model call.
Traces identify execution steps, not their contents: look in the authorized
pipeline evidence, not MLflow, for commands, prompts or provider responses.
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
| `rules_capture` | One partition of `train/category_rows` | Propose specific CEL predicates and positive and negative fixtures. Require a correct hit, passing fixtures and no wrong matches or evaluator errors on that category. |
| `rules_dedup` | Selected category captures and pinned `train/merged_rows` | Replay every captured package on full train before nominating similar rules and rules sharing hits. A failed replay blocks learning. The judge can inspect full-train evidence and submit a justified add/drop diff. A rewrite is a fresh rule, not an in-place change. |
| `rules_finalize` | Deduplicated packages and pinned `train/merged_rows` | Replay the complete surviving bundle on full train and freeze it without consulting the holdout. |
| `rules_eval` | Frozen bundle and the test member | Measure wrong predictions, abstentions, failures, coverage and exact-label accuracy. An uncovered category or an undefined metric is not evidence of perfect precision. |
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
   `sha256`). Split digest and row-count fields are rejected. The CLI checks the
   guide and refreshes only `datasets/global_config`; it does not rewrite the
   dataset registration. Only capture receives the registered live-grounding settings.
2. Materialize `train/category_rows` for each selected category using
   `train-load --category <category>`, then run `rules-capture --category
   <category>` for that category. Capture needs neither merge nor split pin.
   It binds to the category's verified source manifest and the content digest
   of its materialized table. Changes to other categories or the test split
   do not invalidate an otherwise unchanged capture. Captures and failed runs
   made before category isolation must be rerun rather than reused.
3. Before `rules-learn`, make sure `train/merged_rows` is materialized with
   `train-merge`; all configured train categories must have been loaded. Dedup
   judges only the selected categories' captures. Finalization replays the
   whole surviving bundle on every merged train row, including non-selected
   categories, and freezes the bundle; it neither proposes rules nor consults test.
   Before `rules-eval`, make sure `test/merged_rows` is materialized with
   `test-merge` using the configured held-out categories loaded with `test-load`.
   No operator copies split digests or row counts. Each stage checks its declared
   input against the mapped source manifests. Dedup and finalization record the
   exact train table digest and row count; evaluation records the held-out identity
   and the frozen train lineage.
4. Inspect `codebench/rules_eval` for the held-out metrics and
   `codebench/rules_eval_report` for the standalone report. Neither is an
   input to learning.

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
