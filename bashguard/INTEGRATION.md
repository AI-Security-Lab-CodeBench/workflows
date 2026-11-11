# Standalone source integration

Source branch: `feat/bash-structure`.
Source commit: `671172026a2c0f4f6ce420f5e7bef10f53fb0732`
(`feat: enable CEL bindings and AST node navigation`).
Integration date: November 11, 2026.

The source branch was clean. Go and Python source, tests, dependency manifests,
rules and synthetic JSONL fixtures were copied byte-for-byte. Only `README.md`
and `.gitignore` were adapted, and this document was added. No parent CodeBench
pipeline files or datasets are required. No Git history or metadata is embedded.

## File inventory

```text
.gitignore
INTEGRATION.md
README.md
benchmark/compare.py
benchmark/oracles.py
benchmark/requirements.txt
cmd/bashguard/benchmark.go
cmd/bashguard/main.go
go.mod
go.sum
internal/analyze/analyze.go
internal/analyze/analyze_test.go
internal/analyze/document.go
internal/analyze/json.go
internal/analyze/json_test.go
internal/analyze/mask.go
internal/analyze/mask_test.go
internal/analyze/operator.go
internal/analyze/word.go
internal/policy/ast_functions.go
internal/policy/ast_navigation.go
internal/policy/ast_provider.go
internal/policy/ast_test.go
internal/policy/ast_types.go
internal/policy/benchmark_test.go
internal/policy/compile.go
internal/policy/evaluate.go
internal/policy/navigation_test.go
internal/policy/policy_test.go
internal/policy/types.go
pyproject.toml
qa/verify.py
qa/verify_ast.py
qa/verify_navigation.py
rules/ast-examples.json
rules/cat3.json
testdata/ast-requests.jsonl
testdata/masking.jsonl
testdata/requests.jsonl
```

## Fixture admission

The actual contents were inspected, not selected solely by directory name:

- `testdata/requests.jsonl`: 18 hand-authored literal, contextual and adversarial
  requests, paired with explicit expected decisions in `qa/verify.py`.
- `testdata/ast-requests.jsonl`: 14 named structural positive/negative cases,
  paired with explicit expectations in `qa/verify_ast.py`.
- `testdata/masking.jsonl`: two repeated-word and Unicode span-edit examples.
- `qa/verify_navigation.py`: 18 inline structural cases with expected booleans.
- `benchmark/oracles.py`: 30 inline named parser fixtures with authored syntax
  and command-head expectations, not benchmark records imported from a corpus.
- Go test files contain inline source strings and rule fixtures for parser,
  policy, masking, navigation and concurrency regressions.

These fixtures are synthetic unit/QA inputs, not collected commands, human
review decisions or releasable benchmark datasets. Analyzed strings are passed
as data to parsers and the CLI, never to a shell interpreter.

## Exclusions

- `.git/` and all nested Git metadata.
- `bin/`, compiled executables, caches, virtual environments and bytecode.
- All `evidence/`, including tracked verification notes, generated JSONL,
  comparison JSON, logs, before-build binaries, screenshots and capture scripts.
- `report.html` and `RESEARCH.md`: historical comparison/report material tied
  to excluded evidence and the original CodeBench checkout. Operational,
  API, policy and reproduction documentation remains in `README.md`.
- Parent checkout datasets, CSVs, statistics, review tables, collected corpora
  and unrelated pipeline/frontend source.

## Dependencies and reproduction

Go dependencies are pinned in `go.mod` and `go.sum`; a Go 1.26 toolchain and a
C compiler for race detection are needed for the documented checks. Python QA
uses the standard library. The optional comparison requires the three pinned
packages in `benchmark/requirements.txt`; its output is generated locally.

Run the build, Go checks and all three inert QA scripts listed in `README.md`
from this folder. The scripts resolve their input paths relative to themselves.
QA creates its own evidence directories; it does not read excluded evidence.
The Go tests resolve rule fixtures within this module. No old absolute workspace
paths, credentials, cloud clients or parent repository are needed.

## Integration validation

Validated on macOS arm64 with Go 1.26.0 and Python 3.14.3. All commands below
exited 0 in this standalone folder:

```sh
go build -o bin/bashguard ./cmd/bashguard
go test -race -shuffle=on -count=1 ./...
go vet ./...
python3 -B qa/verify.py
python3 -B qa/verify_ast.py
python3 -B qa/verify_navigation.py
bin/bashguard -rules rules/cat3.json < testdata/requests.jsonl
bin/bashguard -mode analyze < testdata/requests.jsonl
bin/bashguard -mode mask < testdata/masking.jsonl
bin/bashguard -rules rules/ast-examples.json < testdata/ast-requests.jsonl
bin/bashguard -mode analyze < testdata/ast-requests.jsonl
uv venv .venv
uv pip install --python .venv/bin/python -r benchmark/requirements.txt
.venv/bin/python -B benchmark/compare.py \
  --binary bin/bashguard --output evidence/comparison-new.json --iterations 100
go test -run '^$' -bench . -benchmem -count=3 ./internal/policy
omo-strip-comments --dry-run --diff cmd internal benchmark qa
```

- Go race tests passed in `internal/analyze` and `internal/policy`; the CLI
  package has no Go test files. Vet and build passed.
- Simple QA: 18 requests, 8 matched, 10 abstained; two masking cases passed.
  Invalid edits and malformed records were rejected, following records retained,
  original source round-tripped, and the execution sentinel remained absent.
- AST QA: 14 requests, 4 matched, 10 structural negatives; typed AST export
  passed, default simple rules abstained, malformed source was rejected, and
  the execution sentinel remained absent.
- Navigation QA: 18 requests, 4 matched, 14 negatives; compiled bindings,
  parent navigation and children navigation passed.
- All five documented CLI examples passed. Optional comparison completed for
  30 synthetic parser fixtures at 100 iterations; this is a comparison report,
  not a zero-disagreement assertion for every parser. Both Go benchmarks passed
  for all three runs.
- Comment preview processed 27 source files and proposed no changes.
- Byte comparisons confirmed all 36 copied source, manifest, rule and fixture
  files unchanged. The final inventory contains 39 files: those 36, the adapted
  README and ignore file, and this document.

Generated `bin/`, `evidence/` and `.venv/` were removed after validation.
No unresolved build or QA dependency was found. Dependencies are not vendored:
a fresh installation needs the pinned Go modules and, for the optional
comparison only, the pinned Python packages. No parent Python/frontend suite,
analyzed shell string, cloud client or dataset command was executed.
