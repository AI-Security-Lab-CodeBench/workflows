# Isolated unittest suite

Run `make test-suite` from the checkout under test inside the Compose test
environment. Use the existing deployment and a clean, read-only checkout mount
at the revision you intend to verify. The caller must be a non-root Linux user
with Python and the locked project dependencies, GNU `timeout`, and the
PostgreSQL executables `initdb`, `postgres`, `pg_isready`, and `createdb` on
`PATH`. `make test-suite PYTHON=/path/to/python3` selects a different installed
Python. If `CODEBENCH_BASHGUARD_AUDIT_BINARY` is unset or points to a missing
file, the runner needs Go and the authored `bashguard` source. It builds
`bashguard/cmd/bashguard-audit` once into its private scratch directory and
supplies the executable's absolute path to discovery and every worker.

The 600-second wall budget includes the audit build, PostgreSQL startup,
discovery, execution, and cleanup. The controller stops scheduling and kills
active worker process groups at 592 seconds, leaving eight seconds for
PostgreSQL shutdown, count reconciliation, and removal of its scratch directory.
GNU `timeout` enforces the outer limit. The runner doesn't connect to the
deployment's review database. It initializes a PostgreSQL cluster with a private
Unix socket and one database per worker. Each worker has its own home, XDG
directories, temporary directory, and shared location for its own `CODEBENCH_HOME`
and `DAGSTER_HOME`. Discovery gets a separate environment and database.

Worker count is bounded by CPU affinity, cgroup v2 CPU quota, and available
memory, allowing 768 MiB per worker after a 1 GiB reserve, with a minimum of one.
`CODEBENCH_TEST_WORKERS` can lower that count. Numerical libraries are limited
to one thread per worker. Workers claim from one prioritized queue, with cases
from measured slow modules scheduled individually. Modules with class or module
fixtures stay together. A claimed batch is never retried after a worker failure.

The command exits successfully only if unittest reports no failures and the
discovered IDs match the started and finished IDs exactly. `unittest_skip_reports`
counts native skip events, including optional encoder tests when
`CODEBENCH_COMMAND_ENCODER_DIR` isn't set. It does not count skips as assertions
that passed. A fixture error that blocks cases, a worker crash, discovery failure,
or a deadline makes the command fail even if every completed case passed.
Capture the command's output for evidence: it reports counts, elapsed time,
worker failures, and bounded diagnostic tails before deleting scratch files.
The controller returns 124 when it reaches the cleanup reserve; `make` reports
its recipe's nonzero status.
