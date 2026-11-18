# CAT-3 generation pilot

Run `python -B -m codebench_pipeline.synthetic_pilot` in the project Python environment with `DAGSTER_HOME` pointing to the existing shared instance. This materializes only `cat3_synthetic_pilot`; it does not load the main collection graph, run the labeler, or publish a training release.

Set these environment variables with the prefix `CODEBENCH_SYNTHETIC_PILOT_`:

| Suffix | Value |
|---|---|
| `ENDPOINT` | The existing inference service's API base ending in `/v1`, not `/v1/models`. |
| `MODEL`, `MODEL_PATH` | The served alias and the model path pinned by the shared inference guard, respectively. |
| `LEDGER_DIR` | The existing labeler's **same** reservation directory. Do not create a different directory for this pilot. |
| `VENDOR_DIR` | Absolute path to the reviewed vendor `generation-pipeline` directory. |
| `OUTPUT_DIR` | Private local directory for per-attempt bundles. |
| `API_KEY` | Optional credential provided through the environment. |

`seeds.jsonl` describes two inspection tasks per family; `prompt.md` requests one short Russian description per command. The pilot asks for four rows in two guarded calls. The host checks the four permitted command forms **after** generation, without showing the answer list to the model. A command outside the pilot scope fails the run rather than becoming an accepted candidate.

Inspect `dataset.csv`, `provenance.jsonl`, `report.json` and the `capacity/` receipts under the directory recorded in the Dagster materialization. Review the rows as text only. The pilot constrains the proposed class and severity to `allow` and `low`; neither is an independently verified label. This small run checks basic command selection and description quality, not risk coverage or model accuracy.
