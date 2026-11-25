# CodeBench workflows

See [pipeline orchestration](docs/architecture/pipeline.md) and [how commands become release rows](docs/architecture/algorithms.md).

In Dagster, open the **rules** asset group to follow capture through evaluation.
The `codebench/rules_eval` Data tab contains the raw metrics. On the
`codebench/rules_eval_report` materialization, **Open interactive report** opens
the standalone charts; its Data tab also records the browser path, storage URI,
and SHA-256. The report is served from the registered dataset store without
making the bucket public.

## Run the single Compose stack

Use the `codebench` project on the designated Docker Engine. The webserver and
daemon run the **same application image**; the native UI, pipeline and business
configuration are built into it. PostgreSQL, MinIO and the pinned Bashguard
evaluator run in the same project.
There are no source checkouts or Docker sockets mounted into the containers.

For a **fresh installation**, start from a clean checkout of the intended commit:

Load the retained evaluator image onto the designated engine first, or build it
using the [Bashguard packaging instructions](bashguard/HTTP.md#offline-image-packaging-and-replay).
Set `CODEBENCH_BASHGUARD_IMAGE` to that image in `.env`. The rules registration
pins the expected executable digest separately; changing a Compose image is
not authorization to use a new evaluator for an existing experiment.

```sh
cp .env.example .env
cp .env.pipeline.example .env.pipeline
chmod 600 .env .env.pipeline
# Set two different random database passwords and CODEBENCH_LLM_IP in .env.
# Put provider keys (including any numbered keys) in .env.pipeline.
docker compose --env-file .env -f compose.yaml config --quiet
python3 scripts/stack.py up
```

`.env` configures Compose. `CODEBENCH_PIPELINE_ENV_FILE` selects the raw
service env file passed to the webserver and daemon. Do not put the PostgreSQL
administrator password in that file. Do not print an expanded Compose config
with real credentials. A Docker administrator can inspect service environments.
Use a Docker context for a remote engine, not a hard-coded host or socket.
Use `python3 scripts/stack.py --env-file /path/to/settings.env up` for a
different deployment file. `status` is read-only and works without env files.
`up` starts missing services without replacing running ones. For an intentional
application update, drain runs and use `prepare` from the clean published
checkout. It recreates the workers after the idle check, updates ingress if
needed, and prepares through filtering with build automation stopped.
Both commands bring up the production services before building or starting
MLflow. If MLflow fails to build or become healthy, the command reports the
failure without stopping Dagster or the UI.
Run `python3 scripts/stack.py --help` for stage selection and targeted E2E.

The UI is published on the **Docker host's loopback**, port `3041`; use a
loopback-only tunnel when the engine is remote. PostgreSQL, MinIO and the
pipeline communicate over service DNS on an internal bridge. Webserver and daemon
also have ordinary outbound bridge access. PostgreSQL and MinIO do not.
Compose maps `codebench-llm` directly to `CODEBENCH_LLM_IP` for the two workers.
Keep the address in the private deployment env file; never put it in Git.
External lookups use `CODEBENCH_DNS_SERVER` (default `1.1.1.1`); no DNS sidecar,
host-list generator, proxy, or outbound allowlist is needed.

**MLflow tracking:** open `http://127.0.0.1:5000` on the Docker host (or
use a loopback-only tunnel to a remote host). Ingress publishes this port
alongside the existing UI on `3041`; MLflow itself is only on the internal
bridge and has no outbound bridge access. The webserver and daemon use
`MLFLOW_TRACKING_URI=http://mlflow:5000` over the internal bridge. No MLflow
credentials or extra `.env` settings are required. This server has no login;
keep port `5000` loopback-only and give access only to trusted users.

Compose builds `codebench/mlflow:3.18.0-isolated-v1` from the official
digest-pinned base in `deploy/mlflow.Dockerfile`. `stack.py up` builds the
wrapper when the local image is missing; to build just the wrapper from a clean
published release, run `docker compose --env-file .env build mlflow` against
the designated engine. Build `ingress` as well when rolling out the MLflow
listener (`codebench/ingress:isolated-v2`). SQLite metadata and server-proxied artifacts
live together in the separate `codebench_mlflow-data` volume. They survive
container replacement and are independent of the review database, MinIO and
dataset data. Include this volume in backups and do not remove it during
routine stack maintenance.

**Replacing a development installation:** start with empty project volumes and
rematerialize CAT-3; no historical data import is required. Drain any active
runs before retiring the old stack. Keep operator credentials and unrelated
data separate from the disposable dataset. `up` refuses to adopt unmarked
containers or volumes and never deletes them, including an unmarked volume
already using the reserved `codebench_mlflow-data` name. On the new stack
`prepare` runs through filtering; it does not start labeling.
