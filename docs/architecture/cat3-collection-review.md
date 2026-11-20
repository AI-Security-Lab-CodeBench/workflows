# CAT-3 collection increment: independent architecture review

Historical review record. Its findings, source line references and verdict refer
to the proposal under review at the time. For current operations, use
[the operator guide](../orchestration.md).

**Verdict: BLOCK.** Keep the proposed direction: extend the active graph, preserve the canonical `cloud_infrastructure_commands_grounded_v1` partition in `dataset_builds`, and publish through the existing `datasets/dataset_release`. Before approving implementation, resolve the acquisition authority, Keenable capture, guarded labeling, and publication contracts below. This is a static review of the proposal and repository, with read-only checks of public source pins. It is not a source admission receipt, deployment authorization, or claim that any runtime gate passed.

## Blocking findings and approval criteria

### 1. Make the V2 ownership change explicit before relying on it

The proposal treats moving source discovery and eligibility into Dagster as already authorized (`cat3-collection-increment.md:20–28`). The effective written policy still puts discovery, full enumeration, and source assessment outside Dagster and forbids in-pipeline source search (`AGENTS.md:83–89`, `docs/independent-branches.md:34–39`, `skills/codebench-source-preparation/SKILL.md:34–56`). The review brief identifies branch acquisition and bounded Keenable use as requirements to verify; it does not independently establish the extent of the override claimed in the design. The proposed update is scheduled in Wave 4, after Waves 1–2 would implement the changed ownership.

**Correction:** Cite the controlling decision or make approval of the V2 exception explicit in the revised design. Move the V2 policy and source-list documentation changes into the relevant implementation waves. State separately who approves origin, scope, reuse rights and exceptions, what Dagster verifies deterministically, and that only the declared Keenable branch may search. Keep V1 behavior, no selector or adapter model, and no `human_source_decisions` in the active graph. Require a branch-owned source-list reference with exact roots, predicates and exclusions, parser or mapping, rights evidence, and refresh and failure rules for each origin.

### 2. Separate Keenable discovery from authoritative capture and define failure transitions

The administrative seed families, allowlisted paths and bounded selection are appropriate. The design requires original page bytes, denied redirects, full digests and independently resolvable spans (`cat3-collection-increment.md:41–47,64–68`). Sharing the existing provider implementation cannot establish those properties by itself: the broker fetches provider JSON with `max_chars=12000`, checks the redirect of the **provider request**, and extracts the provider's `content` field and truncation metadata (`codebench_pipeline/grounding_tools.py:427–449,536–553`). That field is not proof of original response bytes or origin hop behavior. The design also allows partial resume but leaves it unclear whether selection or call accounting survives a crash before the final lock. A fresh search or retry after a restart can change the selected set or exceed the generation budget.

**Correction:** Specify Keenable as discovery and name a bounded, separately verified origin capture path, or provide a provider contract that demonstrably supplies the required bytes and origin guarantees. Define canonical URL and allowed destination checks at the actual origin fetch, rights checks before promoting or retaining content, and rejection of truncated or transformed bodies as original evidence. Persist the selected URL set, per-page state and reserved/used call budget under one generation writer before outbound work; define what resumes, what consumes budget after ambiguous failure, and what requires a new reviewed generation. Publish a complete immutable lock only once. Tests must distinguish a healthy zero-result observation, policy-withheld pages, and failed selected captures; test crash/concurrent resume, over-budget refusal and zero-network replay. State the aggregate call and byte budgets, including whether both the search and origin transport count.

### 3. Enforce guarded labeling and evidence policy on every V2 route

The proposal says every turn uses the guard, but leaves the requirement as an operator gate and a promise to retain live configuration (`cat3-collection-increment.md:70–71,84,108–109`). The checked-in CAT-3 template has `batch: null` (`business-configs/datasets/cat3.json:43`), and `labeling_attempts_table` takes the separate baseline path when `batch` is absent (`codebench_pipeline/labeling_tables.py:39–73`). In the batch path the broker assigns retrieved evidence to requested candidate IDs, and the final checks establish candidate assignment, quote membership and URL equality (`codebench_pipeline/batch_labeling.py:483–502,566–599`). Those checks alone do not establish V2 rights, exact operation/version/flag applicability, or a citation to authorized, retained bytes. Cache restoration has its own path (`codebench_pipeline/batch_labeling.py:722–751`).

**Correction:** Make CAT-3 V2 activation and the labeling asset fail closed without the guarded batch configuration; preserve V1 baseline compatibility. Give persisted seed evidence, on-demand provider evidence and cached outcomes one admission contract: candidate applicability, current rights, byte digest and exact span, and an explicit failure when no acceptable evidence is available. Explain how Context7 and Keenable fallback can meet that contract; a provider result without the required evidence must not become a publishable citation. Test V2 missing batch, stale telemetry, and blocked inference through native and CLI paths, plus a cached quote that still matches text but is no longer authorized. A neighbor-only change must still reuse an unaffected row.

### 4. Bind the quality gates and release hold to the exact artifact being committed

The runbook calls for a steward note and a check immediately before clearing a durable hold, while `dataset_release` is specified to reauthorize branch/evidence generations and the review token (`cat3-collection-increment.md:74,86,108–110`). Clearing an operator switch after inspecting a report leaves a window in which a native run or new review completion could present a different, valid snapshot that the steward did not inspect. Publication already recomputes effective rows inside `write_review_output` before it records publication (`codebench_pipeline/release_admission.py:65–89,126–137`); the shared authorizer currently validates the review token (`codebench_pipeline/publication_authorization.py:12–17`), and registry registration invalidates review under dataset transaction serialization (`codebench_pipeline/dataset_registry.py:35–55`). The design needs to say how the extra V2 quality authorization participates in that boundary.

**Correction:** Define a durable, authenticated cutover attestation bound to the registered revision, current branch/capture/evidence digests, exact review completion, and proposed selection/output digest. Specify whether it is required only for cutover or also on subsequent releases, and which machine checks run on every V2 publication. Check the hold, applicable attestation, and machine gates transactionally at the final write, including early return or cached paths that claim current publication. Treat a held build as prepared or waiting, preserving real review completion for continuation; the registered runner currently calls the release runner when completion exists (`codebench_pipeline/dataset_run.py:150–158`). Verify CLI, native and continuation paths, concurrent change after sign-off, and empty-queue policy completion. A stale or unauthorized release must never become current; historical artifacts remain inspectable. Rehearse rollback with publication fenced and without restoring a stale token.

### 5. Reconcile representative selection with the mandatory contribution gate

Branch-first rotation with nested origin rotation does not guarantee that either new corpus receives a released row before a per-risk cap is exhausted (`cat3-collection-increment.md:80–86`). For example, one risk can have old and two new corpus origins in the same branch, plus eligible rows from documentation and Keenable. A cap of three can admit one row per branch, all while selecting the old corpus origin first and neither new corpus. All sources can have unique, supported, human-cleared rows and still fail the acceptance gate. Dedup survivor attribution can independently make an apparent contribution unavailable.

**Correction:** Specify whether selection reserves feasible contributions or intentionally blocks despite available rows. If contribution is mandatory, define a deterministic feasibility and reservation rule across effective risks, then apply fair selection to remaining capacity; keep current caps, review and single-survivor provenance. If no assignment is feasible, report the exact per-origin or per-risk constraint and block cutover. Test competing origins in one branch, small caps, scarce cross-risk rows, post-correction risks, and cross-origin exact duplicates against the same selection used in the final write.

## Criteria that pass in direction, with evidence required later

| Criterion | Assessment and required evidence |
| --- | --- |
| Config-declared trusted Python ETL | Sound direction (`cat3-collection-increment.md:24–27,63–67`). The existing loader verifies the named file digest and executes those same bytes; a digest and caller-supplied `trusted=true` are not independent authorization (`codebench_pipeline/source_transform_contract.py:14–26`, `codebench_pipeline/source_transforms.py:23–30,52–83`). Make the approved deployment identity authoritative, including path containment and dependency revision, and test an unapproved module whose bytes match its supplied digest. Pin source positions, transformations and central canonical validation. |
| At least two additional eligible datasets | Two credible, distinct leads. Public GitHub commit/tree lookups resolved both exact pins in `cat3-collection-increment.md:34–39`; their pinned root licenses are Apache-2.0 and CC0-1.0 respectively. Spot checks found relevant literal examples in Kubernetes `AI/model-serving-tensorflow/README.md:97` and Docker `nginx-golang-postgres/README.md:49,66,81`; CAT-3 has corresponding routing families (`business-configs/categories/cat3/category.json:73,89–91,142–155`). These are not admissions. Define whether Kubernetes `_archived/` and index-only subdirectories are in scope, which Docker manifest names and nesting establish sample roots, and how embedded documentation, third-party notices and mutable operands are handled without rewriting literals. Before activation produce complete tree and rights receipts, source spans, and routed, supported, unique contribution counts after dedup. I verified the local manifest counts (274, 13,067 and 1,052), not live V1 path/blob parity. |
| Branch acquisition and stage necessity | Branch catalog, capture and candidates are separate inspectable boundaries. The shared `semantic_evidence` index has a distinct purpose from row-specific `labeling_documentation`: index once per capture generation, then bind evidence to each eligible row (`cat3-collection-increment.md:64–74`). Retain `document_adapters` only as a distinct inspectable resolved per-document parser/trust receipt consumed by extraction, as in `codebench_pipeline/dataset_assets_sources.py:138–185`. If it merely echoes static declarations for a branch, fold the receipt into catalog/capture and preserve V1 historical views. Do not add a search agent, independent label reviewer or second release. Include `release_admission.py`, review snapshot validation and I/O invalidation in the Wave 3 ownership/checks; they are part of the current commit boundary (`codebench_pipeline/dataframe_io.py:168–230`, `codebench_pipeline/review_snapshots.py:252–264`). |
| Manual QA and same canonical partition | Direction accepted (`cat3-collection-increment.md:61–78,95–110`). Require fixture QA with same test partition across V1→V2→V1, independently selectable acquisition, a shared preparation-only selection, Overview/Data and Review at the selected partition, inspection of origin bytes and failures, invalidation and row reuse. Require the live receipt for the existing production partition after approval, including source counts, reconciled stages, release/export digests and steward sampling; treat absent review routes as absent in the sample. Verify configured empty, unconfigured, never materialized, failed, waiting, stale and unchanged/cached are distinguishable in planning or inspection where applicable (`AGENTS.md:176–179`). Synthetic decisions stay in the isolated QA home, and production decisions require actual reviewers. |

Re-review when the revised proposal makes the five contracts testable and assigns their owners and acceptance evidence. No pipeline tests, materializations, inference, registry changes or publication were run for this review.

## Independent re-review — November 18, 2026

**Verdict: APPROVE THE ARCHITECTURE.** The revised proposal resolves all five
findings at the design level. The human owner must still approve the Mermaid
diagram derived from the direct-dependency table before any implementation or
policy edit. This verdict admits no source, establishes no reuse or retention
rights, and authorizes no production run, deployment, or release.

1. **V2 acquisition ownership — cleared.** The design records the controlling
   revision instruction and puts the narrow CAT-3 V2 policy and source-list
   exception in Wave 0, before acquisition work. The steward approves origins,
   scopes, rights, and refreshes; Dagster enumerates and verifies approved
   declarations. Only the declared Keenable collection branch searches for
   sources. V1 and legacy retain the prepared-manifest flow; there is no selector
   or adapter model or active `human_source_decisions` stage. Each branch owns
   a source-list reference carrying reproducible scope, parser, rights, and
   failure rules. **Test:** A V1 fixture decodes unchanged. A V2 fixture requires
   branch source lists and compiles to declarations without fetching or importing.
   An unresolved required scope blocks currentness. In the graph fixture, only
   Keenable collection calls search. The policy/source owner and source/ETL owner
   own the Wave 0–1 evidence (`cat3-collection-increment.md:22–30,137–139`).

2. **Keenable capture and recovery — cleared.** The provider supplies URLs,
   not origin evidence. The separate HTTPS client checks the actual destination,
   denies redirects, verifies complete body bytes, and binds exact spans to the
   digest. Rights gate the fetch and subsequent retention or use. One serialized
   ledger reserves budget before dispatch and freezes selection before origin
   calls: at most five searches plus fifteen captures, 512 KiB per response and
   10 MiB reserved total. Ambiguous dispatch consumes its reservation, fails
   the generation, and cannot be resent. Healthy empty and policy-withheld
   observations can finalize; a failed selected fetch cannot. **Test:** Stub both
   transports and count requests. Reject redirects, truncated bodies and
   requests over budget; interrupt a dispatched request before its receipt and
   race two resumes. Assert no resend or partial lock, one lock on success,
   distinct empty/withheld/failed states, and zero requests when replaying a
   verified lock. The source/ETL and provider/evidence owners cover these seams
   in Waves 1–2 (`cat3-collection-increment.md:45–51,138–139`).

3. **Guarded labeling and evidence — cleared.** V2 validates at registration
   and again at the asset boundary before dispatch or a cache hit. CLI and
   native selection share the guarded batch route; the V1 baseline remains
   available. Persisted, on-demand, and cached evidence must satisfy the same
   row-specific operation, flag, version, rights, and verified-byte-span checks.
   Context7 and Keenable provide leads; an authorized origin capture must supply
   the cited bytes within the grounding budget or the row records a failure.
   **Test:** Through CLI and native selection, exercise missing batch, stale
   telemetry, and denied capacity; assert no baseline fallback or remote
   inference. Reject provider-only text and a still-matching cached quote whose
   rights were revoked. Change only a neighboring row and assert the unaffected
   row reuses its original receipt. The label owner owns these Wave 3 checks
   (`cat3-collection-increment.md:103,105–111,140`).

4. **Exact publication authorization — cleared.** Every distinct V2 proposal
   requires an authenticated steward attestation. The digest binds the spec,
   branch/capture/evidence receipts, exact review completion, coverage and
   sample, and ordered output bytes. Hold clearance grants no authority of its
   own. `release_admission.write_review_output` re-resolves inputs and
   recomputes the preview under per-dataset serialization before recording a
   current reference. The same currentness checks cover cache and early returns;
   historical artifacts remain readable. **Test:** Drive CLI, native, and
   continuation paths against the same proposal. For both policy-completed
   empty review and human-completed review, hold it and assert that completion
   survives without publication. Clear the hold without attestation and assert
   it still waits. Revoke an attestation before a cached return; race an input
   change with the final write. An update that wins prevents publication of the
   stale proposal; a later update makes the committed result stale. Rehearse
   rollback with publication fenced and no restored stale completion. The
   release owner and graph integrator own these Wave 3 checks
   (`cat3-collection-increment.md:121–129,140,154`).

5. **Mandatory contributions under risk caps — cleared.** The design reserves
   one eligible survivor per required contribution via capacity-constrained
   matching on post-correction risks, then debits those reservations before
   fair rotation. Each survivor can satisfy at most one requirement; duplicate
   links preserve provenance without adding credit. If no full assignment
   exists, the constraint is reported and publication is blocked. **Test:** In
   the original single-risk example with cap three, the two new corpus origins
   and Keenable take all three slots; cap two must block. In a separate
   unit-cap case, if one origin can use either of two risks and another can only
   use the first, reserve the first for the constrained origin. Checked those
   three small cases by enumerating assignments. In Wave 3, the release owner
   must also test input shuffling, a risk changed by review, and cross-origin
   exact duplicates; preview and final write must agree on ordered IDs and
   output hashes (`cat3-collection-increment.md:113–119,127,140`).

This was a static review of the design and existing code seams. The two
additional corpus pins are still leads; rights and scope receipts and net-new,
eligible released contributions must be demonstrated separately. No pipeline
tests, live source checks, materializations, inference, registry changes, or
publication were run for this re-review.

## No-local-RAG delta review — November 18, 2026

**Verdict: APPROVE the V2 design and diagram delta.** The revision removes
local document retrieval without reopening the five approved architecture
findings. Human approval of the revised diagram remains required before
implementation or policy edits. This review does not admit sources or
authorize production work.

- **Graph and preparation.** The increment's direct-dependency table and both
  Mermaid panels agree on processing edges. The shared panel explicitly omits
  configuration edges for readability; the table is authoritative. Verify
  that `semantic_evidence`, `labeling_documentation`,
  `map_semantic_evidence_config` and `map_labeling_documentation_config` are
  absent from active V2 definitions, plans and inputs. Preparation ends at
  `filtered_commands` with zero label-time lookup or inference. Policy reaches
  processing only via branch and stage mappings. Keenable and Context7 are
  runtime services, not asset nodes or dependencies; the Keenable collection
  branch's candidates remain a normal persisted input to the merge.
- **Grounding and receipts.** The guarded attempts stage uses the row's own
  verified source spans when sufficient, with zero lookup calls. Otherwise it
  requests only missing behavior for the exact operation, significant flags,
  and version/scope. Exercise both routes: Keenable locates a trusted origin;
  Context7 uses an approved library ID. Neither can supply a citation without
  an authorized, independently verified origin span. Charge provider and origin
  failures against the budget. Test that wrong-operation, wrong-version,
  missing-flag, provider-only, or out-of-budget evidence yields an explicit
  row failure if support remains insufficient. CLI and native V2 must deny
  inference on missing batch configuration or stale/denied capacity without
  entering baseline labeling.
- **No hidden retrieval; reuse and history.** Candidate-local spans, exact
  receipt dereferences, and coalesced identical live lookups do not constitute
  a retrieval index. Verify no passage inventory or cross-row search over
  local captures; similarity batching only schedules inference. A valid
  unchanged row reuses its original receipts with no lookup after a neighbor
  changes. A missing object, changed span/applicability, or revoked rights
  must prevent restoration of the old success even if its quote still matches.
  In a V1→V2→V1 rehearsal, read immutable historical outputs and mappings by
  historical reference, and prove V2 never consumes them, their legacy
  snippets, or legacy label caches.
- **No policy or publication bypass.** Label-time lookup uses its own budget
  and the same origin transport, containment and rights checks. It cannot
  add collection candidates or modify a collection lock. Revalidate
  candidate-assigned citations before use, at submission, on cache restore,
  and at release. The exact-output proposal now binds the current attempts
  digest and selected-row evidence and rights receipts. Test that a changed
  receipt or revoked citation blocks the old proposal, including cached
  publication, while hold, exact review completion, source contribution and
  risk caps remain enforced at the single release boundary.

Evidence: `cat3-collection-increment.md`, “Native assets and acceptance,”
“Guarded V2 labeling and evidence admission,” “Exact publication authorization,”
and the reversible rehearsal; `cat3-collection-flow.md`, both Mermaid panels
and their notes. This was a static document review. No pipeline tests,
materializations, provider calls, or deployment ran.
