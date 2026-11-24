# CodeBench workflows

See [pipeline orchestration](docs/architecture/pipeline.md) and [how commands become release rows](docs/architecture/algorithms.md).

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
checkout; it leaves build automation stopped and prepares through filtering.
Run `python3 scripts/stack.py --help` for stage selection and targeted E2E.

The UI is published on the **Docker host's loopback**, port `3041`; use a
loopback-only tunnel when the engine is remote. PostgreSQL, MinIO and the
pipeline communicate over service DNS on an internal bridge. Webserver and daemon
also have ordinary outbound bridge access. PostgreSQL and MinIO do not.
Compose maps `codebench-llm` directly to `CODEBENCH_LLM_IP` for the two workers.
Keep the address in the private deployment env file; never put it in Git.
External lookups use `CODEBENCH_DNS_SERVER` (default `1.1.1.1`); no DNS sidecar,
host-list generator, proxy, or outbound allowlist is needed.

**Replacing a development installation:** start with empty project volumes and
rematerialize CAT-3; no historical data import is required. Drain any active
runs before retiring the old stack. Keep operator credentials and unrelated
data separate from the disposable dataset. `up` refuses to adopt unmarked
containers or volumes and never deletes them. On the new stack `prepare` runs
through filtering; it does not start labeling.
