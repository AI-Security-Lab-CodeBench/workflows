# CodeBench workflows

See [pipeline orchestration](docs/architecture/pipeline.md) and [how commands become release rows](docs/architecture/algorithms.md).

## Run the single Compose stack

Use the `codebench` project on the designated Docker Engine. The webserver and
daemon run the **same application image**; the native UI, pipeline and business
configuration are built into it. PostgreSQL and MinIO run in the same project.
There are no source checkouts or Docker sockets mounted into the containers.

For a **fresh installation**, start from a clean checkout of the intended commit:

```sh
cp .env.example .env
cp .env.pipeline.example .env.pipeline
chmod 600 .env .env.pipeline
# Set two different random database passwords in .env.
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
pipeline communicate over service DNS on an internal bridge. The UI proxy is
not an outbound proxy. Stages that call external documentation or model
providers need an explicitly approved route reachable from the internal
network; providing an API key does not create that route.

**Replacing a development installation:** start with empty project volumes and
rematerialize CAT-3; no historical data import is required. Drain any active
runs before retiring the old stack. Keep operator credentials and unrelated
data separate from the disposable dataset. `up` refuses to adopt unmarked
containers or volumes and never deletes them. On the new stack `prepare` runs
through filtering; it does not start labeling.
