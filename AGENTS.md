# CodeBench workflows

## Operate the stack

One `codebench` Compose project runs one pipeline version. Use the active Docker
context and `python3 scripts/stack.py`; `--help` lists commands. For installation,
credentials or a fresh development cutover, see [README](README.md). Never start a replacement
stack, adopt unmarked volumes, or reset shared data to get past a failed check.

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
  Deduplicate only proven exact matches; apply eligibility before review and
  caps after approval. An empty queue needs no human decision. A nonempty queue
  requires real decisions and completion for the current snapshot.
- Publish code, tests, guides and CAT-3 config only. Keep CAT-2, collected data,
  secrets and review records out of Git. Read local `strategy/` guidance; never
  edit or stage it.
