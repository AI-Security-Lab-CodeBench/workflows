# CAT-3 repository acquisition

`repositories.json` declares immutable repository trees and rights evidence.
Every origin also declares a trusted ETL path, SHA-256, entrypoint, and options.
Scope controls which files can enter the catalog; the ETL declaration controls
how a captured file is read. A parser revision must match the separate
deployment allowlist in `codebench_pipeline/repository_etl.py`. Candidate
receipts retain the original byte range and the parser identity. Historical
receipts retain their original parser paths and hashes; only the current
reviewed Markdown parser pin is used for new runs.

The `dataset` branch declares eleven independent corpora. The seven added in
`cat3-repository-collection-4` use the `readme` scope. Each pins its upstream
README and Apache-2.0 license. The exclusions close the scope to reviewed
example files at the pinned commit; a refresh must review that scope again.
Their `markdown_shell.py` bindings use `utilities` to select which tools to
extract from complete shell fences. This mode emits single-line commands and
preserves the allowed tool list of each origin.

Revision 5 adds official Podman and Pulumi documentation and two README files
from Pulumi Examples. Each origin pins its Apache-2.0 license, root README,
and complete Git tree. The `markdown_files` scope selects only the named files
and rejects a missing entry or a symlink. Files with unresolved local rights
notices remain excluded.

These origins use `markdown_shell.py` with a singular `utility` option. This
mode reads restricted literal commands in complete shell fences and, for
Pulumi documentation with `inline_usage` enabled, inline code
introduced by usage prose. It joins simple shell continuations and retains the
byte range containing the original lines. Lines inside a multiline quoted
argument are never treated as standalone commands. If an expansion makes the
next command boundary ambiguous, extraction stops for the rest of that fence.
Synopsis placeholders and unprompted output from untyped fences remain outside
its scope.

V2 checks its verified capture cache first. On a miss, it can read the existing
local V1 cache without changing it. The lookup is available only for sources
already selected by the current catalog and uses the pinned Git blob ID:

```text
<dataset_home>/source-downloads/sources/<git-blob-sha1>/body/current
<dataset_home>/source-downloads/sources/<git-blob-sha1>/body/<sha256>/data
```

An operator can mount the cache read-only, or point `source-downloads` in a
private QA home at the existing cache. Symlinks below that root are rejected.
The V1 `current` pointer supplies an independent SHA-256. V2 checks it against
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
