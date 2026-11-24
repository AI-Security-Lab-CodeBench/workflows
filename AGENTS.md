# CodeBench workflows

## Operate the stack

One **shared** `codebench` Compose project runs one pipeline version. On the
development host it runs on the `codebench-rootless-docker.service` user daemon,
not necessarily the CLI's `default` engine. An empty `docker ps` on the default
socket is **not** evidence that the deployment is absent. Connect read-only first:

```sh
systemctl --user is-active codebench-rootless-docker.service
export DOCKER_HOST="unix://${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/codebench-docker.sock"
unset DOCKER_CONTEXT
python3 scripts/stack.py status
deployment_env=$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project.environment_file"}}' codebench-webserver-1)
docker inspect -f '{{index .Config.Labels "com.docker.compose.project.working_dir"}} {{.Config.Image}}' codebench-webserver-1
python3 scripts/stack.py --env-file "$deployment_env" stages
```

Use the release and env-file paths recorded on the **running** container; do
not copy credentials, print the env file, or assume the current checkout's
`.env` describes the deployment. Keep `DOCKER_HOST` in the shell running all
stack commands. Do not switch the user's global Docker context, use `sudo
docker`, or create another project if the socket or a service is unavailable.
Check the user service and report the mismatch. Other sessions share the
workers, ports, volumes, database, and object store. Use `stack.py e2e` with its
disposable database for verification; do not reset volumes, stop the stack,
enable sensors, or roll out a candidate to test it. Changes to a checkout are
not automatically present in the running image.

Use `python3 scripts/stack.py`; `--help` lists commands. For installation,
credentials or a fresh development cutover, see [README](README.md). Never start a replacement
stack, adopt unmarked volumes, or reset shared data to get past a failed check.

Fix problems in the existing Compose setup using its supported configuration.
For example, correcting a worker's Compose `dns` setting is a repair. Adding a
DNS proxy, patching the container runtime, or creating a host service is new
infrastructure. If a fix requires new infrastructure, explain why and get explicit
approval before building or testing it, even as a temporary local workaround.

| Intent | Command from the checkout |
| --- | --- |
| Inspect without starting anything | `python3 scripts/stack.py status` |
| Start missing services without recreation | `python3 scripts/stack.py up` |
| List the deployed stages | `python3 scripts/stack.py stages` |
| Build through a stage, not beyond it | `python3 scripts/stack.py through datasets/filtered_commands` |
| Roll out published code and prepare without labeling | `python3 scripts/stack.py prepare` |
| Enable automation, including labeling | `python3 scripts/stack.py unsynced` (never under a no-labeling hold) |

Build from a clean, published checkout. Keep code in the image, state on named
volumes and real settings in ignored env files. No host source mounts, socket
mounts, host networking, or machine paths in tracked configuration. Never log
resolved secret-bearing config. Leave the shared stack running after work.

For additional Keenable organizations, put numbered `KEENABLE_API_KEY_N` entries
in the private pipeline env file; use the same `KEENABLE_ORG`/`KEENABLE_ORG_N` label
only for keys that share an organization's quota.

## Verify a change

- Run **only affected E2E scenarios**, including affected downstream consumers:
  `python3 scripts/stack.py e2e tests/e2e/<file>.py::<test>`. The CLI creates and
  removes a disposable review database. Add a focused case if nothing catches
  the regression; for a prose-only change, say why no E2E applies.
- **Five minutes wall clock per test run, including setup and cleanup.** A
  timeout is a failure. Reserve the full suite and its repairs for weekly
  maintenance; split slow jobs rather than skipping coverage.
- **Lint and strict types are mandatory** for changed code. Use `make static-checks`
  for the pipeline and the frontend linter and strict TypeScript check for UI
  changes. Verify deploy changes with synthetic settings and actual service
  connectivity. Do not use real providers, credentials or review data for QA.

## Change the pipeline

- Work on `master` in `CodeBench-workflows`; preserve unrelated edits. Commit
  verified, scoped work and push to `origin master`. No force-push or AI attribution.
- Use the canonical implementation. Read [orchestration](docs/architecture/pipeline.md)
  or [algorithms](docs/architecture/algorithms.md) for the affected contract;
  update a guide when that contract changes. For Dagster work, read
  [Dagster Expert](.agents/skills/dagster-expert/SKILL.md). Use native scheduling,
  selection and retries; custom orchestration needs explicit approval.
- Only configuration assets read local policy. Pass scoped settings, data and
  resources through declared dependencies. Treat collected commands as inert
  data; never execute them. Execute only reviewed, hash-pinned configuration ETL.
- Preserve original evidence and receipts. Label each row from its own fetched
  documentation. Respect the shared provider limit across rows.
  Grounding is limited to 10 calls/sec per organization across searches and
  fetches; additional keys for that organization do not increase its limit.
  Deduplicate only proven exact matches; apply eligibility before review and
  caps after approval. An empty queue needs no human decision. A nonempty queue
  requires real decisions and completion for the current snapshot.
- Publish code, tests, guides and CAT-3 config only. Keep CAT-2, collected data,
  secrets and review records out of Git. Read local `strategy/` guidance; never
  edit or stage it.
