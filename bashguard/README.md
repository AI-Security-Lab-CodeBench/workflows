# Bash structural classifier

Use **mvdan/sh for parsing, a small typed representation for rules, and CEL for
compiled predicates**. This isolated implementation provides a JSONL CLI, a Go
library, category-specific rules, source-span masking, adversarial tests and a
reproducible comparison. This folder is standalone; it does not depend on the
CodeBench pipeline or its datasets. See [INTEGRATION.md](INTEGRATION.md) for source
provenance, the included file inventory and exclusions.

## What a match means

The positive event is **a Bash structure matching a reviewed policy rule under
explicitly supplied facts**, not successful execution, actual author intent or
proof of safety. Simple-rule results say `basis: "literal_cli_form"`; explicit
AST-rule matches say `basis: "bash_ast"`.

No text-only system can guarantee zero false positives about arbitrary Bash
execution: the same `redis-cli FLUSHALL` can resolve to a shell function, a wrapper
or the documented Redis client. External configuration, CLI versions, aliases,
traps, permissions and remote state are not recovered by any of these parsers.
The shipped rules assume the named programs have their documented CLI meanings.
Caller-supplied environment and approval facts must be authoritative.

The useful engineering contract is narrower: exact predicates, conservative
syntax eligibility, explicit unknowns, negative fixtures, and abstention rather
than a default label. A finite passing test corpus is not a universal soundness
proof or a calibrated false-positive rate.

## Run

Run the following commands from this folder. Go 1.26 or newer is required by
pinned mvdan/sh v3.14.1. Go's normal toolchain
selection can download the required compiler. Production classification has no
Python dependency. QA uses only the Python standard library; Python 3.12 or newer
is recommended.

```sh
go build -o bin/bashguard ./cmd/bashguard
bin/bashguard -rules rules/cat3.json < testdata/requests.jsonl
bin/bashguard -mode analyze < testdata/requests.jsonl
bin/bashguard -mode mask < testdata/masking.jsonl
go test -race -shuffle=on -count=1 ./...
go vet ./...
python3 -B qa/verify.py
python3 -B qa/verify_ast.py
python3 -B qa/verify_navigation.py
```

These commands run the analyzer, never the Bash strings in the input.
The binary is built locally, not distributed. QA creates fresh output under
`evidence/`; no pre-existing evidence is required. Build outputs, evidence and
optional Python environments are ignored and are not part of this source bundle.

One input record:

```json
{"id":"source-row-17","command":"terraform destroy -auto-approve","context":{"environment":"production"}}
```

The result contains the unchanged command and ID, `matched` or `abstain`, the
category/risk/severity/action when matched, matching rule IDs, rationales, source
URLs and the rule-bundle SHA-256. `action` maps to CodeBench's `class`; it is not
an additional canonical CSV column. Abstention has **no labels**. Unknown is not
`allow`, and is not silently mapped to `ask`.

Optional context:

- `environment`: `production`, `nonproduction`, `unknown`, or omitted.
- `approval`: `granted`, `not_granted`, `unknown`, or omitted.

The engine does not infer production from a resource name, or missing approval
from Vercel's `--yes`. Unknown context abstains for context-dependent rules.
The default `simple` rule scope accepts only one foreground simple command with
fully static words. The existing CAT-3 bundle retains this boundary.
Rules with explicit `scope: "ast"` can instead inspect complete parsed commands,
including assignments, redirects, pipelines, lists, functions and substitutions.
Parsing/encoding/complexity failures still prevent evaluation in every scope.

Malformed records produce per-record errors and exit status 1; later records are
still processed. Invalid rule bundles fail before input is classified. Valid
abstentions exit 0. JSONL records are bounded; shell source is limited to 64 KiB.

## Structural representation

`-mode analyze` retains the entire source and exports:

- `ast`: the complete native Bash AST encoded by mvdan's own `syntax/typedjson`,
  including node-specific fields, child field names, flags and source positions.
- Flat AST nodes: stable document-local ID, parent ID, kind, operator and
  half-open UTF-8 **byte** span.
- Every syntactic call, including nested calls in otherwise ineligible input.
- Word source spans, raw spelling, decoded literal value and an explicit
  `static` flag. Empty literal arguments are distinct from unknown values.
- Eligibility and reasons for the **simple** scope, independently from whether
  an AST-scoped rule can evaluate the tree.

Comments are retained. Whitespace remains in the original source. Native AST JSON
roundtrips through mvdan's decoder in tests without losing native fields.
The upstream encoding omits zero/default fields; `Type` appears on the root and
interface-valued nodes, not necessarily every object. A node occurrence does not
prove reachability or execution.
`Word.Lit()` is not used as a shell evaluator; a restricted quote-aware decoder
handles only supported literal cases. No interpreter or expansion evaluator is
imported.

## Writing compiled rules

`rules/cat3.json` contains seven narrow rules covering examples in CAT-3.1 through
CAT-3.5, not an exhaustive utility catalog. It intentionally ships no default
`allow` rule. The loader supports other category/risk codes with matching category
prefixes, and only `allow`, `deny`, `ask` plus the four canonical severities.

Each rule has an ID, CEL `when`, labels, rationale and primary documentation URLs.
`scope` is `simple` by default, or explicitly `ast`.
CEL is parsed and typechecked once at startup, then reused for the entire input
stream. Expressions must return `bool`. Available inputs:

```text
ast: syntax.File                 complete native tree, not JSON or a projection
ast_nodes: list(dyn)              every native node, in depth-first order
source: string                   original command text
calls: list(analyze.Call)         raw and decoded word records, including nested calls
nodes: list(analyze.Node)         parent/span metadata aligned by index with ast_nodes
argv: list(string)               decoded top-level argv only when simple-eligible; otherwise []
context: policy.Context          caller-supplied environment and approval facts
```

For example, an AST-scoped predicate can distinguish pipelines from conditionals:

```cel
ast.Stmts.size() == 1 &&
type(ast.Stmts[0].Cmd) == syntax.BinaryCmd &&
operator(ast.Stmts[0].Cmd) == '|' &&
type(syntax.BinaryCmd(ast.Stmts[0].Cmd).X.Cmd) == syntax.CallExpr
```

This illustrates structural access, not a sufficient safety policy by itself.
`rules/ast-examples.json` contains opt-in, narrowly guarded redirect, pipeline and
command-substitution policies with positive and negative CLI fixtures:

```sh
bin/bashguard -rules rules/ast-examples.json < testdata/ast-requests.jsonl
bin/bashguard -mode analyze < testdata/ast-requests.jsonl
python3 -B qa/verify_ast.py
```

The tree is handed to CEL as the actual parsed `*syntax.File`; no JSON
serialization is performed for rule evaluation. The adapter fills CEL-Go's
missing support for interface fields such as `Stmt.Cmd` and `Word.Parts`.
Concrete fields are statically checked. These Go interface boundaries are
necessarily CEL `dyn`; use a checked cast such as `syntax.CallExpr(node)` or
`syntax.Lit(part)` to restore static field checking beyond one. A wrong cast
returns an evaluation error, never a fabricated empty node. Guard competing
variants with `type(node) == syntax.CallExpr` before casting.

`ast_nodes.exists(n, type(n) == syntax.CmdSubst)` performs a deep tree query
without requiring recursive CEL functions. `kind(node)`, `operator(node)`,
`start(node)`, `end(node)` and `offset(position)` expose node kind, readable
operators and byte positions. The native fields remain available as well.
Native operator fields such as `Redirect.Op` are unsigned enum values; use
`operator(redirect) == '>'` for readable matching. The inspection JSON renders
those enums as strings and adds `Type`, `Pos`, and `End`; those are encoding
metadata, not identically named native CEL fields. In CEL, use `type(node)`,
`start(node)`, `end(node)` and `offset(node.OpPos)` instead.
AST object construction is disabled: rules inspect parser-owned objects rather
than synthesizing trees. No setters, interpreter or IO helpers are exposed.

### Local variables and node navigation

`cel.bind(name, value, expression)` is enabled. The name is local to the final
expression, can be reused there, and preserves the selected value's checked
type. Bindings can nest; they are not mutable assignments and may be evaluated
lazily.

```cel
ast_nodes.exists(n,
  type(n) == syntax.CallExpr &&
  cel.bind(cmd, syntax.CallExpr(n),
    cmd.Args.size() == 2 &&
    type(cmd.parent()) == syntax.Stmt &&
    !syntax.Stmt(cmd.parent()).Background &&
    cmd.children().exists(child, type(child) == syntax.Word)
  )
)
```

Every native syntax node supports these CEL receiver methods:

| Method | Result |
|---|---|
| `node.parent()` | Direct native parent; `null` for the root `File` |
| `node.children()` | List of direct native syntax-node children; `[]` for a leaf |

Children follow parser traversal order, not necessarily source-byte order.
Comments are included and use their actual statement/file ownership. Scalars,
source-position fields and auxiliary structs which do not implement
`syntax.Node` are not child nodes; inspect those through native fields.
Parent results are `dyn` or null; children are `list(dyn)`, so type guards and
checked casts remain useful. Calling either method on null, a scalar or a node
outside the current parse returns an evaluation error, not a fake empty node.

These are CEL receiver macros backed by an immutable-per-evaluation index.
The AST is not mutated, no cyclic fields are added to inspection JSON, and
compiled programs hold no shared command-specific navigation state. Building
the index is linear in the node count; parent and child-list lookup use identity
indexes rather than rescanning or matching text. Comment value copies from native
fields are resolved by their complete native value, including source position.
Identifiers beginning `__ast_` are reserved implementation details.

The redirect rule in `rules/ast-examples.json` uses bindings and both methods.
`python3 -B qa/verify_navigation.py` runs its positive/negative examples through
the real CLI. Running it writes fresh output under `evidence/navigation-v3/`.

For AST rules, review the whole relevant structure: top-level versus nested
placement, conditionals, background/negated flags, assignments, redirections,
dynamic words and contextual evidence. `scope: "ast"` is explicit policy-author
responsibility, not an assertion that every parseable tree is safe to label.
The default CAT-3 rules continue abstaining on all their prior unsupported forms.

Returned evaluation errors, unknown final values, cancellation, cost exhaustion
and conflicting labels cause abstention. All matching rules must agree on labels;
there is no first-match precedence. Each evaluation has a modeled CEL cost limit.
The CLI compiles one immutable bundle per process; hot reload is not implemented.

**CEL's logical operators can mask intermediate errors.** For example,
`argv[999] == 'x' || argv == ['redis-cli', 'FLUSHALL']` can return true.
Compilation does not prove a policy is correct. Bundles are trusted, reviewed
policy code; add both positive and adversarial cases for every new rule. No custom
IO-capable functions are registered. Cost accounting is not an RSS quota or an
OS-level CPU sandbox.

## Masking

Use original-source spans, not global `replace`. For
`printf café café`, editing bytes `[7,12)` changes only the first occurrence.
The CLI accepts `edits` containing `start`, `end` and `replacement`; all coordinates
refer to the same original UTF-8 source. Edits are sorted and applied together,
without changing the caller's edit list. Overlaps, partial words, invalid bounds
and invalid replacement encoding are rejected.

The implemented scope is complete **argument words of parsed calls**.
Heredoc delimiters/bodies, assignment values and redirection operands are not
selectable. Legacy backquotes and CR-containing word spans are rejected because
their raw span boundaries need additional treatment.

This is a display/data transformation, not a shell-safe quoting helper or a proof
of equivalent behavior. Replacements can change syntax and remove nested
operations. Classify the original, never assume a masked string preserves its
classification. The API does not detect secrets, retain reversal secrets or
validate edits against a prior source digest.

## Optional parser comparison

The runnable [benchmark](benchmark/compare.py) includes 30 authored synthetic
fixtures and pinned optional Python dependencies. It needs no collected corpus,
historical report or prior evidence. Generate a new comparison locally:

```sh
uv venv .venv
uv pip install --python .venv/bin/python -r benchmark/requirements.txt
.venv/bin/python -B benchmark/compare.py \
  --binary bin/bashguard --output evidence/comparison-new.json --iterations 100
go test -run '^$' -bench . -benchmem -count=3 ./internal/policy
```

The comparison uses pinned tree-sitter 0.26.1, tree-sitter-bash 0.25.1 and bashlex
0.18. Regex is explicitly a lexical baseline, not a grammar parser. Reported hot
parse time excludes process startup; the report separately captures batch CLI
transport time. Fixture oracles cover syntax acceptance and command heads, not
complete Bash semantics. Fixtures are synthetic tests, not new collected records.

## Pipeline boundary

Persist these results as a first-stage annotation sidecar keyed by input ID.
Preserve every input row, including duplicate command text and abstentions.
Do not treat deterministic labels as independent agentic review, human approval
or permission to publish the nine-column dataset. No live Dagster stage was
rewired in this temporary implementation.

Integration with a collection pipeline is a separate step. Preserve multiline
source and quote handling when adapting an extraction path to this JSONL API.

## Limits

AST export has depth and node limits **after parsing**. The upstream parser has
no exposed hard depth budget; the input-size limit is not a process sandbox.
Use a separately constrained worker if hostile workload resource isolation is
required. The current CLI is local, streaming and single-process, not a deployed
network service. No collected command is executed by tests or demonstrations.
