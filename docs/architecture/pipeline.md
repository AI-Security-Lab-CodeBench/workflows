# CodeBench pipeline architecture

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


Each collection operation has a typed mapping. Repository capture receives pinned locations and download limits; extraction receives parser pins and preserves each candidate's origin. Only the merge mapping supplies the weights that rank candidates from datasets, documentation and Keenable. Filtering gets routing patterns, while labeling also needs prompt text and evidence policy. Human decisions take their dataset identity from the partition and their authorization from the completed review snapshot, so they have no configuration mapping.

**Keenable collection.** The `keenable/command_candidates` asset uses the authored inspection seeds in `business-configs/categories/cat3/keenable/declaration.json`. CAT-3 allows up to five searches across the seeds. Each turn the model sees the authored instructions, active risk definitions and guidance, severity definitions, prior query IDs, novelty counts, and a few short phrases drawn from excerpts using the seed vocabulary. It can author a new context phrase for the same seed, choose another seed, or finalize. The asset validates the phrase and adds it to the fixed benign search topic. The model never receives full excerpts, page URLs, or credentials.

The parser reads complete fenced shell examples as text and only emits literal lines that match the selected seed's allowed command syntax. It keeps the returned page URL, exact snippet and byte range for accepted spans. Each completed search records parsed candidates and hit outcomes in the attempt evidence as it runs. A later failure keeps earlier evidence and blocks publication of a partial table. A successful empty search can merge. The source type is `web_page`, and the `keenable` value identifies the snippet provider without establishing origin rights. The branch does not fetch origin pages, execute commands, or assign labels. The seed's risk family describes the type of harmful operation the benign example contrasts with; downstream labeling still checks the command and its actual impact against separate applicable documentation.

The dataset bundle sets a separate output token limit and timeout for discovery. Discovery uses the effective labeling endpoint, model, credentials and transport. When guard admission is configured, discovery shares the capacity ledger and never falls back to plain inference. With the updated configuration registered and `KEENABLE_API_KEY` available to the service, run `make materialize-through STAGE=datasets/keenable/command_candidates` to build the candidate table without running labeling.

The labeling, review and release decisions are explained in [How commands become release rows](algorithms.md).
