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

**Keenable collection.** The `keenable/command_candidates` asset uses its scoped settings to request snippets for the configured use cases and extracts literal command examples from the returned text. It keeps the returned page URL, snippet and candidate position as evidence. The source type is `web_page` and the value `keenable` in the license field identifies the provider of the snippet; it does not establish any license or usage rights for the origin. The branch does not fetch origin pages or create labeling or review decisions. An empty successful search can merge with the other branches. A failed search blocks the current merge rather than substituting candidates from an earlier configuration. Labeling still needs separate applicable documentation.

CAT-3 allocates one query to each of five use cases. Its cloud storage and SQL disclosure query asks for individually quoted actor commands or audited operations, with adjacent attribution. Reported outcomes can add context but are not required. If a disclosure contains only audit or prose evidence, collection does not invent a shell command.

The labeling, review and release decisions are explained in [How commands become release rows](algorithms.md).
