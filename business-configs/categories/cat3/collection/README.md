# CAT-3 repository acquisition

`existing-datasets.json` declares corpus origins; `documentation-crawl.json`
declares original documentation. Each pins its own repository trees and rights
evidence. Each origin supplies `repository`, `commit`, and `tree_digest`,
and its `rights` lists the license ID, attribution, a pinned `license_file`,
and any pinned `notice_files`. These files carry both `path` and `sha256`.

`file_selector` controls which files can enter the catalog and supplies their
catalog format: `cheatsheet` and `aws_examples` emit `text`; `readme`, `compose`,
`tldr`, and `markdown_files` emit `markdown`. The `markdown_files` selector
requires explicit `included_paths`; other selectors accept reviewed
`excluded_paths` within their enumerated scope. Choose `parser` separately.
The parser's `parser_id` and `sha256` must match the separate
deployment allowlist in `codebench_pipeline/repository_etl.py`. Candidate
receipts retain the original byte range and the parser identity. Historical
receipts retain their original parser paths and hashes; only the current
reviewed Markdown parser pin is used for new runs.

Set `trust_weight` beside an origin when it needs to override the branch
default (0.60 for datasets and 0.90 for documentation). Registration enforces
documentation > datasets > Keenable. Each branch can set `maximum_file_bytes`
(default 512 KiB) and `capture_workers` (default 12). Change `refresh_revision`
to invalidate that branch's receipts when its effective configuration would
otherwise be unchanged. The collector derives the fixed rights-use label and
processes one text record per file; neither is an authoring setting.

Parser `options` are typed. `context_characters` defaults to 100 characters
on each side of an extracted example and accepts integers from 0 to 1,000.
Cheatsheets, TLDR, and AWS CLI examples accept only that option and can omit
`options` altogether. `markdown_shell` requires an explicit `options.mode`.

The `dataset` branch declares eleven independent corpora. The seven added in
`cat3-repository-collection-4` use the `readme` selector. Each pins its upstream
README and Apache-2.0 license. The exclusions close the scope to reviewed
example files at the pinned commit; a refresh must review that scope again.
Their `fenced_lines` mode uses `allowed_utilities` to select which tools to
extract from complete shell fences. This mode emits single-line commands and
preserves the allowed tool list of each origin.

Revision 5 adds official Podman and Pulumi documentation and two README files
from Pulumi Examples. Each origin pins its Apache-2.0 license, root README,
and complete Git tree. The `markdown_files` scope selects only the named files
and rejects a missing entry or a symlink. Files with unresolved local rights
notices remain excluded.

These origins use `literal` mode and name one `utility`. This mode reads
restricted literal commands in complete shell fences and, for
Pulumi documentation with `inline_usage` enabled, inline code
introduced by usage prose. It joins simple shell continuations and retains the
byte range containing the original lines. Lines inside a multiline quoted
argument are never treated as standalone commands. If an expansion makes the
next command boundary ambiguous, extraction stops for the rest of that fence.
Synopsis placeholders and unprompted output from untyped fences remain outside
its scope.

## Reusing historical capture bytes

The declared branch checks its verified capture cache first. On a miss, it can
read the historical V1 cache without changing it. The lookup is available only for sources
already selected by the current catalog and uses the pinned Git blob ID:

```text
<dataset_home>/source-downloads/sources/<git-blob-sha1>/body/current
<dataset_home>/source-downloads/sources/<git-blob-sha1>/body/<sha256>/data
```

An operator can mount the cache read-only, or point `source-downloads` in a
private QA home at the existing cache. Symlinks below that root are rejected.
The V1 `current` pointer supplies an independent SHA-256. The current collector checks it against
the body, verifies the size and Git blob SHA-1 from the pinned catalog, and
applies the ordinary UTF-8 and sensitive-content checks before publishing a
capture. Catalog rights checks still run. Missing Git keys fall through to the
pinned HTTPS source; an incomplete or corrupt V1 object fails the stage. Valid
individual captures remain reusable after a failure, but no complete documents
receipt is published until all selected sources pass.

A capture imported from V1 records `acquired_via: verified_v1_cache` and a null
HTTP response status in the documents receipt. Normal local replay records
`acquired_via: v2_cache`; the immutable migration receipt remains available
under `<dataset_home>/repository-v2/<dataset_id>/receipts/`.
