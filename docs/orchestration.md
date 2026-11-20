# Operate the declared dataset pipeline

Use the shared `codebench` Compose deployment. Both workstations must point
their gitignored `.env` at the same `REMOTE_COMPOSE_DIR`. Run `make remote-up`
from the development checkout before Dagster work, and open
`http://localhost:3041` for the native UI. Follow [the deployment rules](../AGENTS.md)
for Docker socket access and the read-only checkout mount. Check the pinned
checkout's commit, cleanliness and mount before registering or building.

Run the commands below inside the existing `webserver` service with
`docker compose exec -T`, using the shared project's Compose configuration.
Run from the mounted checkout and use the service's existing `CODEBENCH_HOME`.
The webserver, daemon and CLI must use the same dataset home, Dagster instance
and PostgreSQL review settings. A different home shows different materializations
and review state.

## Inspect first

Select `cloud_infrastructure_commands_grounded_v1` in the **Partitions** view
for the current CAT-3 dataset. Open an asset's **Overview** for the algorithm,
row count, schema and bounded metadata sample. Open **Data** for persisted rows,
pagination and full cell values; inspection does not run inference.

To inspect a persisted release from the service:

```sh
python -m codebench_pipeline.dataset_cli show \
  --home "${CODEBENCH_HOME:?}" --dataset cloud_infrastructure_commands_grounded_v1 \
  --stage dataset_release --limit 10
```

If an expected artifact says **Never materialized**, check the partition, dataset
home and Dagster storage before launching anything. Historical outputs remain
evidence; a displayed historical table does not grant current release authority.

## Validate and register a declaration

Use [the current configuration guide](../business-configs/README.md) and
[the source preparation skill](../skills/codebench-source-preparation/SKILL.md).
The CAT-3 template inherits labeling and storage from the existing registration;
it cannot bootstrap a fresh home without explicit settings.

```sh
python -m codebench_pipeline.dataset_cli validate \
  --home "${CODEBENCH_HOME:?}" --config business-configs/datasets/cat3.json
python -m codebench_pipeline.dataset_cli inspect-config \
  --home "${CODEBENCH_HOME:?}" --config business-configs/datasets/cat3.json \
  --stage labeling_attempts
```

Validation and configuration inspection do not collect or label commands.
For a registration with an authored directory, run the partitioned Dagster job
`observe_dataset_configuration` to capture live edits. Processing jobs use the
persisted capture and do not reopen the directory. If the observed contents no
longer match the approved registration, the next build stops at `global_config`.
Use `register` with the same `--home` and `--config` when intentionally
applying a change. Editing authoring files alone does not update a registration.

## Build the registered dataset

```sh
python -m codebench_pipeline.dataset_cli build \
  --home "${CODEBENCH_HOME:?}" --dataset cloud_infrastructure_commands_grounded_v1
```

The default command refreshes the partition's configuration. Its
`configuration_run_id` identifies that refresh; `configuration_refreshed`
does not mean the dataset has finished building. Inspect the partition's asset
events and the `dataset_build_requests` automation sensor for processing runs.
The sensor is enabled by default. Dagster evaluates asset conditions; the CLI
does not calculate a subset of stages.

For an explicit synchronous materialization, add `--force`. Add
`--force --prepare-only` to run `prepare_dataset` through `filtered_commands`
and inspect acquisition and filtering before forcing later work. Forcing the
full build can repeat labeling even if its inputs are unchanged. Native
**Materialize all** is likewise an explicit selection.
Dagster's partition status describes recorded lineage. A fresh data version
does not establish that the registration or source rights are still current,
or that the required evidence is still available.

Read [the stage guide](pipeline-stages.md) for artifact and receipt contracts
and [the confidence policy](confidence-review-policy.md) for queue admission.
For a nonempty queue, use **Review** on the selected partition and make real
terminal decisions before explicitly completing the snapshot. An empty queue
needs no human completion. Publication still applies its machine gates and
exact-proposal controls; use `preview-release` with the same `--home` and
`--dataset` to inspect the proposal. A hold, pending attestation or waiting
review is not a release. Stale outputs cannot authorize a new release.
The `dataset_release_requests` sensor requests `release_dataset` after an
automatic or human completion when the publication gates permit it.

Run the [isolated verification suite](test-suite.md) in the Compose test
environment. Collected commands remain inert during inspection and testing.

## Historical evidence

The retired replay graph no longer registers, runs, or publishes datasets.
Existing releases, registries, and replay evidence remain on disk. The
`dataset_cli show` command and Data view still display supported historical
tables from the typed dataset pipeline, including `semantic_evidence` when a
partition has one. Historical files outside those views remain archived as
original bytes.
