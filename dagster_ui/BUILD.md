# Building the CodeBench Dagster UI

Run from the repository root with the project's existing uv environment, Git,
Node >=20, network access to public GitHub and npm, and enough disk for the
Dagster workspace installation and static export:

```sh
uv run python -m codebench_pipeline.ui_build
```

The default clones public Dagster tag `1.13.29` into an owned
`TemporaryDirectory` and requires its exact commit to be
`7dca97fabebf771d4ac0515d674e50e2a742446f`. No credentials, global Yarn install,
root JavaScript installation, or additional Python dependencies are needed.

To reuse an existing pristine checkout, pass its repository root:

```sh
uv run python -m codebench_pipeline.ui_build --source /path/to/dagster-checkout
```

The checkout must have that exact commit and no modified tracked files, including
staged changes. Ignored dependency installations are permitted. The builder checks
the pin and tracked cleanliness before work and again after work, including
failure paths. It also requires the upstream `js_modules/package.json` Yarn
4.12.0 workspace declaration. It never deletes a caller-provided checkout.

## Isolated build and ownership

The builder snapshots `src/`, `tests/`, the package, Next, PostCSS and TypeScript
configs, and the three tooling config/lock files into a temporary extension
directory. Tests are included because Next's TypeScript project includes them.
The source fingerprint also includes `codebench_pipeline/ui_build.py`. Generated
files, caches, build outputs, dependency trees, and provisioning links are not
source inputs. Source/config symlink files are rejected.

It prefers `tooling/node_modules/@yarnpkg/cli-dist/bin/yarn.js`. If absent, it
downloads fixed `@yarnpkg/cli-dist@4.12.0` metadata and its fixed tarball URL from
the official npm registry, verifies the tarball's SHA512 against `dist.integrity`,
and copies only the single regular `package/bin/yarn.js` entry to its temporary
root. It does not extract archive paths or install global packages. The CLI
must report exactly 4.12.0.

The existing `tooling/package.json`, `tooling/yarn.lock`, and `.yarnrc.yml`
bootstrap a separate temporary tooling installation with `install --immutable`.
The pinned upstream workspace is also installed with `install --immutable`.
Caller tooling dependencies are only read to locate the preferred CLI, not
reinstalled. A supplied upstream checkout retains any ignored dependency/cache
files created by its workspace installation.

The temporary extension provisions these links:

- `.upstream` to the selected checkout.
- `node_modules` to its `js_modules/node_modules`.
- `public` to its `js_modules/app-oss/public`.

Existing links at these names in `dagster_ui/` are accepted only when their
resolved targets match the selected checkout. They are never replaced or removed.
An unrelated file, directory, or unexpected link is rejected. If existing links
point at a reusable checkout, use `--source` with that checkout rather than the
default fresh clone. All links created by this builder live in its owned
temporary directory and are removed on success or failure.

The builder uses an exclusive `.codebench-build.lock` directory to reject
concurrent builder invocations; normal exits remove it. An interrupted process
that cannot run cleanup may leave this lock; establish that no build is running
before removing it manually.

## Private build-time seam

The local Next config inherits unchanged upstream OSS configuration and aliases
`@shared/assets/AssetsOverviewRoot` to the local implementation. The page wrappers
re-export upstream `_app`, `_document`, and index, retaining the original app
shell. Zod resolves to isolated tooling; React, Next, and Dagster packages resolve
to the original workspace. This is a private source-level build seam, not a
supported runtime plugin API or a maintained Dagster fork.

The builder invokes `node .../next/dist/bin/next build` directly in the temporary
extension, never upstream `build.sh`. It does not patch upstream, vendor,
minified JavaScript, or HTML. The root-URL build clears inherited
`NEXT_PUBLIC_BASE_PATH` and `NEXT_PUBLIC_URL`, disables Next and Yarn telemetry,
and uses the local empty `assetPrefix` override rather than placeholder rewriting.
It then copies original files without text substitution:

- `app-oss/csp-header.txt` to `build/csp-header.txt`.
- Webserver `graphiql/` to `build/vendor/graphiql/`.
- Upstream `LICENSE` to `build/licenses/dagster-LICENSE`.
- Installed Zod `LICENSE` to `build/licenses/zod-LICENSE`.

## Generated bundle and manifest

Only a completed, packaged export is published to `dagster_ui/build/`. The current
production bundle stays in place during installation, compilation, and packaging.
The new bundle is copied into a temporary sibling and its complete file SHA256
inventory is compared with the packaged output. An existing static Next export
(`index.html` and `_next/`) is copied to a unique `build.backup-<id>` sibling and
both file inventories verified while the old build remains available. Only then
does a pair of directory renames publish the replacement. Publication failure
restores the previous directory when the destination is absent. Backups are retained for the caller.
Symlinked build paths or bundle contents, and unrelated existing build paths, are
rejected. Temporary output and the build lock are cleaned on normal failure paths.

`build/codebench-build.json` contains:

- `dagster_version`: exactly `1.13.29`.
- `upstream_commit`: exactly `7dca97fabebf771d4ac0515d674e50e2a742446f`.
- `source_files`: repository-relative input path to SHA256 mapping.
- `source_digest`: SHA256 of that mapping encoded as UTF-8 JSON with sorted keys
  and compact separators `(",", ":")`, without a trailing newline.

The project input inventory is checked again before publication to detect
concurrent edits. The manifest is build provenance, not evidence of runtime QA.
The completed static bundle is retained after temporary source and links are
removed; serving it does not require those links or another inference run.

## Pin upgrades and lead verification

An upgrade requires explicitly reviewing the new tag's commit, workspace/tooling
locks, inherited Next/PostCSS/TypeScript config, native page exports, asset-view
alias, CSP, vendor assets, and licenses. Change version and commit together and
verify a pristine-source build rather than patching an installed or minified
bundle. The successful temporary proof established this build seam for the
current pin; it does not establish compatibility with a future pin.

The lead owns command execution and verification. Before shipping, run the
repository's Python checks, strict UI typecheck and UI tests described in
`INTEGRATION.md`, then this entry point with the selected checkout. Verify default
clone and reused-source paths, clean tracked upstream before/after, successful
and failed-build cleanup, preservation of caller links/current output, retained
backups, and all manifest hashes. The runtime must enforce the manifest version
and commit and serve the resulting bundle. Check native navigation, category
partitions, GraphQL, Data/Review endpoints, root static paths, CSP and GraphiQL in
the real UI on desktop and mobile. Retain actual integration evidence locally;
generated datasets, screenshots and runtime ledgers are not publication inputs.
