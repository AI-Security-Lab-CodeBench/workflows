# Bashguard HTTP v1

The service classifies Bash source with the same parser and CEL policy engine as
the CLI. It never runs a submitted command, fetches rule sources, reads a
dataset, or stores a bundle. The Compose endpoint is `http://bashguard:8080`
on the internal `backend` network. It has no published port or credentials.

## Protocol

- `GET /healthz` and `GET /v1/version` return
  `{"runtime_version":"sha256:<lowercase executable SHA-256>"}`.
- `POST /v1/analyze` takes `required_runtime_version` and `rows` with the same row
  schema as evaluation, but no bundle and at most eight rows. It returns the pin
  and ordered `{id,analysis}` or `{id,error}` outcomes. Analysis includes native
  literal eligibility, structural nodes (ID, parent, kind, operator and UTF-8
  byte ranges), and parsed call words. This endpoint is private: call words
  include raw values for local role projection and counterfactual construction.
  Do not forward its unprojected response to a model, provider or log.
- `POST /v1/evaluate` takes one UTF-8 JSON document:
  `{"required_runtime_version":"sha256:…","bundle":{…},"rows":[{"id":"row-1","command":"pwd -P","context":{"environment":"unknown","approval":"unknown"}}]}`.
  The bundle is the native version-1 bundle object, not a string, file path, or
  object-store reference. Context is optional. Labels accept canonical IDs
  such as `CAT-6` and `CAT-6.1`, optionally followed by a space and a nonblank
  display name. The risk's category prefix must match; label text is returned
  as provided.
- A completed learning run with zero surviving rules uses
  `{"version":1,"kind":"empty_learned","rules":[]}`. This bundle produces
  `no_proven_rule` for valid inputs; parser and context errors are still
  reported separately. Ordinary bundles must still contain at least one rule.
- Success is `{"runtime_version":"sha256:…","bundle_sha256":"…","results":[{"id":"row-1","result":{…}}]}`.
  Each submitted row gets one result in input order. `result` is the native
  classification result. Per-row failures also carry an `error` object,
  distinct from a clean `abstain`. Examples include `invalid_command`,
  `invalid_context`, `cel_runtime_error`, and `evaluation_timeout`. A Bash
  syntax diagnostic includes `line` and `column` when the parser supplies them.
- Non-2xx route responses have `{"error":{"code":"…","message":"…"},"runtime_version":"sha256:…"}`.
  `409 runtime_version_mismatch` means no bundle was compiled and no rows were
  evaluated; do not retry against another runtime or fall back to the CLI.
  `400 invalid_request` covers invalid JSON or batch shape, `413
  request_too_large` covers oversized bodies, and `422 invalid_bundle` covers
  native compilation/validation failures. Diagnostics can include `phase`,
  `rule_id`, and `field`. CEL compilation messages retain the original
  source position and caret. `503` means capacity or the batch time limit was
  reached; no partial batch is returned.

The limits are 2 MiB per request, 1 MiB and at most 128 rules per bundle, 1–128 rows,
unique nonblank row IDs of at most 256 bytes, and nonblank commands of at most
64 KiB. There are at most four active evaluation requests, a 30-second
compile/evaluation budget checked between rows, and a one-second evaluation
context per row. The parser and CEL engine also enforce their native limits.

### Bundle identity and client serialization

`bundle_sha256` is lowercase hex SHA-256 of the **exact bytes of the JSON
object value** in the HTTP body, from its `{` through its matching `}`.
Whitespace outside that value is excluded. Internal whitespace, object key
order, and escapes are significant. This is the same native digest reported
in each result, not a digest of parsed/re-encoded JSON.

Serialize the bundle once, retain those UTF-8 bytes, and splice them as the
raw `bundle` value into an envelope whose version and rows are independently
JSON-encoded. Hash those same bundle bytes (without trailing line endings
outside the value), and send the assembled body unchanged. Do not pass that
envelope to a second JSON serialization step that might compact or escape the
bundle. Check the returned runtime pin, bundle digest, and row IDs before
accepting results. Keep the same pin and bundle bytes for replay.

## Offline image packaging and replay

Compile on Linux with the desired architecture, a pinned Go toolchain, and
the `go.sum`-verified modules already in its cache. The Docker packaging step
uses `scratch` and copies only the static executable; it does not download a
base image, a toolchain, or modules.

From `bashguard/`, for the local Linux architecture:

```sh
(
  set -eu
  build=$(mktemp -d)
  trap 'rm -f -- "$build/bashguard-http"; rmdir -- "$build"' EXIT
  CGO_ENABLED=0 GOTOOLCHAIN=local GOPROXY=off \
    go build -mod=readonly -trimpath -buildvcs=false \
    -o "$build/bashguard-http" ./cmd/bashguard-http
  pin=$("$build/bashguard-http" -version)
  image="codebench/bashguard:${pin#sha256:}"
  docker build --network=none --file "$PWD/Dockerfile.http" --tag "$image" "$build"
  printf 'Runtime pin: %s\nDeployment image: %s\n' "$pin" "$image"
  docker image inspect --format '{{.Id}}' "$image"
)
```

Set `CODEBENCH_BASHGUARD_IMAGE` in the private deployment env file to the
verified image reference and set the evaluator registration's expected runtime
version to the exact `sha256:…` pin. The default image name is intentionally
unconfigured, and Compose does not pull a substitute. The executable reads and
hashes its own running image at startup; neither a caller nor a container
environment variable can set the returned identity. Retain the image and its
recorded image ID (`docker image save` to the release artifact store, or an
immutable registry digest) with the runtime pin. Replays should load/use that
artifact, not rebuild it: Go toolchain or target changes can change the pin.

Masked capture requires this binary's `/v1/analyze`, learned-allow shape guard
and checked CEL literal gate. Build and publish the new immutable image using
the above procedure.
Pair the image with the new `runtime_version` and the SHA-256 of the exact
`business-configs/bashguard/guide.json` bytes in the rule registration. Confirm
`/v1/version` and `/v1/analyze` against the pinned image before enabling capture.
Old attempts do not resume under the new runtime, guide, or masked batch
descriptor; keep old images for reproducing old evidence rather than overriding
the pins.
