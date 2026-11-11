# Inert Bash command filtering

`scripts/command_filter.py` uses only the Python standard library. Importing its
API does not read files, load strategy YAML, call a model, access the network, or
execute commands. It locates candidates; it does not assign dataset classes,
severity, production context, approval, provenance, or safety.

## API

```python
from dataclasses import asdict
from scripts.command_filter import assess_command, classify_command, extract_commands

risks = classify_command(command)
assessment = assess_command(command)
records = [asdict(record) for record in extract_commands(text)]
```

`classify_command(str) -> tuple[str, ...]` returns sorted, unique candidate codes
such as `("CAT-3.1", "CAT-3.4")`. It parses utility and subcommand positions with
`shlex`, rather than searching prose, quoted arguments, or SQL for risk keywords.
It keeps candidates from every visible component of a compound command.

`assess_command(str) -> Extracted` assesses a structured command without extraction
or normalization. `command` and `context` equal the exact input, `start` is zero,
`end` is its length, and `transformations` is empty. This is the integration entry
point for existing structured rows, including complex commands that require
review. Unknown syntax is retained rather than silently dropped.

`extract_commands(str) -> Iterator[Extracted]` yields frozen, slotted dataclasses:

| Field | Meaning |
| --- | --- |
| `command` | Candidate text after recorded lightweight cleanup |
| `context` | Original source span plus up to 100 characters on either side |
| `start`, `end` | Zero-based, half-open Python character offsets into the original input |
| `risk_candidates` | Sorted tuple of possible CAT-3 codes, possibly empty for opaque commands |
| `transformations` | Tuple naming applied fence, prefix, prompt, comment, or continuation cleanup |
| `status` | `eligible` or `needs_review` |
| `reason` | Machine-readable explanation |

Offsets describe the original complete line or multiline span, including prompts
and Markdown prefixes but excluding its final line terminator. They are not byte
offsets and do not index normalized command text. `dataclasses.asdict` is suitable
for JSON serialization; tuples become arrays through `json.dumps`.

## Recognition and extraction

The static family table covers the authored cases in `tests/command_filter_cases.py`,
including their read-only counterparts. AWS CloudFormation is distinguished from
S3; gcloud container from storage; Azure AKS from storage; Kubernetes deletion and
resource listings from deployment apply/diff and deployment listings. Read commands
are family candidates, not automatic safety decisions.

Observed local candidate extensions include SAM, Serverless, SST, Incus, Coi,
podman-compose, additional container cleanup operations, and sudo-wrapped database
clients. Related AWS database/storage service prefixes are also recognized.
Option values for common global flags are skipped before interpreting subcommands.
Unknown options with separate values can prevent recognition; this is not a full
versioned CLI grammar.

Extraction accepts standalone commands, list/blockquote prefixes, `$` and
`user@host:path$` prompts, standalone inline backticks, and Bash/shell/console or
untyped fenced blocks. Explicit foreign-language fences are excluded. Whole
pipelines, semicolon sequences, and `&&`/`||` compositions remain single records.
Backslash continuations, continued connectors, and multiline quoted arguments are
joined. Quoted SQL semicolons and hash characters remain literal arguments.
Unquoted shell comments are removed without inspecting their content for risks.

`eligible` means only that the visible components match known command families
without detected unsupported shell syntax. It is not a Bash syntax proof or
permission to run the command. Substitutions, dynamic expansions, heredocs,
redirections, grouping, control flow, unsupported command components, and malformed
or incomplete quotes/connectors require review. Quoted shell programs are not
recursively interpreted. Classification can therefore be empty for opaque
wrappers; reviewers must inspect the entire preserved command and source context.

Complex control-flow blocks and heredocs conservatively consume the remainder of
their source region (up to a fence boundary or end of input). This avoids emitting
their embedded data as independent eligible commands but can overcapture adjacent
commands. Structured sources should use `assess_command` to preserve their actual
boundaries. Bare prose with a recognized executable and plausible subcommand can
be indistinguishable from a command; this lightweight extractor is not a natural
language parser. It does not mine commands embedded in arbitrary prose, JSON
strings, or YAML scalars. Extract those fields using the owning source adapter
and then call `assess_command`.

## CLI and focused verification

```text
python3 -B scripts/command_filter.py INPUT --output OUTPUT
python3 -B -m unittest discover -s tests -p test_command_filter.py -v
```

The CLI reads strict UTF-8 and writes UTF-8 JSONL. File and decoding errors propagate;
input commands are always inert data. Output is overwritten, so use distinct input
and output paths. Tests exercise the actual strategy fixture, family boundaries,
quoting, wrapping, composition, review cases, source offsets, and the JSONL CLI.
They never invoke collected utilities. Verification is static parsing only.
