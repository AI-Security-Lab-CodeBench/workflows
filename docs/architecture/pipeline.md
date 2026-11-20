# CodeBench pipeline architecture

CodeBench builds a labeled dataset of shell commands from source material. A released row must be traceable to the text it came from, the evidence for its label, and the decision that admitted it. Commands are data throughout: no stage executes a collected command.

```text
Pinned datasets ─────┐
Pinned documentation ├─→ merge candidates → filter → label against evidence
Reviewed web scopes ─┘                                   ├─ unsupported → audit
                                                        └─ supported → exact deduplication
                                                                        ↓
                                                                category eligibility
                                                                        ↓
                                                           resolve required review
                                                                        ↓
                                                                  dataset release
```

**Orchestration.** The [Dagster definitions](../../codebench_pipeline/definitions.py) connect persistent assets by their inputs. A partition identifies the dataset being built. Configuration assets load and validate the authored settings; each consumer receives just its settings through a mapping in its own group. The graph declares what depends on what. Dagster selects and runs the work, and the Data and Review panels read persisted outputs without rerunning it.


Each collection operation has a typed mapping. For example, repository capture receives pinned locations and download limits; extraction receives parser pins and source weights. Filtering gets routing patterns, while labeling also needs prompt text and evidence policy. Human decisions take their dataset identity from the partition and their authorization from the completed review snapshot, so they have no configuration mapping.

The labeling, review and release decisions are explained in [How commands become release rows](algorithms.md).
