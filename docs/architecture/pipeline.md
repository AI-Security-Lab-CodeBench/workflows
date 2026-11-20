# CodeBench pipeline architecture

CodeBench builds a labeled dataset of shell commands from source material. A released row must be traceable to the text it came from, the evidence for its label, and the decision that admitted it. Commands are data throughout: no stage executes a collected command.

```text
Pinned datasets ─────┐
Pinned documentation ├─→ merge candidates → filter → label against evidence
Keenable web search ─┘                                   ├─ unsupported → audit
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

**Keenable collection.** The declaration can keep searches within reviewed URL scopes or name administrative cases to search across public domains. The catalog records ranked provider hits and applies the origin budget after reserving calls for pinned license files and prioritizing declared sources. Capture uses the existing public HTTPS transport in both modes. An unlisted page is retained only if its head explicitly links to the CC BY 4.0 license and it has one identifiable main region. Capture retains the exact main-region bytes and the license notice separately from declared source permissions. Candidate receipts bind literal shell examples to the retained bytes, the source URL and the license evidence; the retained fragment's start plus a candidate's byte position locates the example in the original response. A candidate from an unlisted page does not give that page approval as a declared grounding source.

The labeling, review and release decisions are explained in [How commands become release rows](algorithms.md).
