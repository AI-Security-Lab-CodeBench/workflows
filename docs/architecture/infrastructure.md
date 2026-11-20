# Infrastructure and Dagster extensions

This inventory describes the checkout inspected on November 19, 2026. Dagster
owns asset definitions and dependencies, partitioned jobs, scheduling, run and
step execution, retry and re-execution, and event history. CodeBench supplies
definition wiring, durable domain state, adapters for external systems, and
policy for which evidence, labels and releases are admissible. A materialization
event establishes execution history; it does not by itself authorize a current
read or publication. Paths and symbols below are source references, not claims
about the deployed version.

## Deployment, definitions and launch

| Feature | Implementation and caller | Dagster surface and ownership |
| --- | --- | --- |
| Shared runtime startup | [`Makefile`](../../Makefile) `remote-up` calls [`scripts/remote-up.sh`](../../scripts/remote-up.sh), which runs [`scripts/start-remote-compose.sh`](../../scripts/start-remote-compose.sh) locally or over SSH. The start script uses the server-side Compose files, starts `review-db`, `webserver` and `daemon` with `--no-recreate --wait`, and the wrapper checks the loopback UI. | Dagster webserver and daemon processes; no asset primitive. The read-only pinned source mount and separate writable storage are deployment requirements; these scripts do not prove which checkout is mounted. |
| Instance and code location | [`viewer.py`](../../codebench_pipeline/viewer.py) `viewer_environment` supplies the home alignment; [`runtime_instance.py`](../../codebench_pipeline/runtime_instance.py) `dataset_instance` opens the instance for the CLI and server; [`ui_server.py`](../../codebench_pipeline/ui_server.py) `serve` loads `codebench_pipeline.definitions` as a module code location. | `DagsterInstance`, `ModuleTarget`, `WorkspaceProcessContext`. Paths, processes and credentials are infrastructure. |
| Graph, typed ports and descriptions | [`definitions.py`](../../codebench_pipeline/definitions.py) `build_definitions` wires factories in [`declared_source_assets.py`](../../codebench_pipeline/declared_source_assets.py) `declared_source_assets`, [`dataset_assets_sources.py`](../../codebench_pipeline/dataset_assets_sources.py) `filtered_commands`, [`dataset_assets_annotations.py`](../../codebench_pipeline/dataset_assets_annotations.py) `annotation_assets`, [`review_assets.py`](../../codebench_pipeline/review_assets.py) `human_assets` and [`dataset_finalization.py`](../../codebench_pipeline/dataset_finalization.py) `finalization_assets`. [`dataset_asset_types.py`](../../codebench_pipeline/dataset_asset_types.py) `table_input` and `rules_input` bind ports; [`stage_descriptions.py`](../../codebench_pipeline/stage_descriptions.py) `describe_stage` supplies Overview descriptions. | `Definitions`, partitioned `@asset`, typed `AssetIn`, `DagsterType`, Pandera-backed types, `AssetSpec` mapping, and `Output`. Branch scopes, schemas and transforms remain business contracts; the native graph owns the edges. |
| Registration and configuration | [`dataset_registry.py`](../../codebench_pipeline/dataset_registry.py) `DatasetRegistry` is called by the CLI and definition wiring. [`config_assets.py`](../../codebench_pipeline/config_assets.py) `configuration_assets` produces the registered `global_config` and mapped settings, with `config_directory_asset` for authored-input observation. [`stage_configuration.py`](../../codebench_pipeline/stage_configuration.py) `stage_configs` projects settings; [`dataset_run_configuration.py`](../../codebench_pipeline/dataset_run_configuration.py) `dataset_run_config` and `release_run_config` form launch inputs. | `DynamicPartitionsDefinition` (`dataset_builds`), `PartitionedConfig`, `RunConfig`, and typed configuration assets. Registration versioning is infrastructure; validation and field mappings are specific to each business consumer. |
| Build entry points | [`dataset_cli.py`](../../codebench_pipeline/dataset_cli.py) `execute_registered_build` calls [`dataset_run.py`](../../codebench_pipeline/dataset_run.py) `run_registered`; `build_definitions` also wires `dataset_build_requests`. Named jobs cover independent branch acquisition, preparation, build, configuration refresh and observation, and release. | `AssetSelection`, partitioned asset jobs, `SensorDefinition`, `SensorResult`, `RunRequest` and `execute_in_process`. Dagster runs the selected assets; the app currently chooses a subset using its own currentness plan (cut 2). |
| Syntax audit | [`bashguard_audit_asset.py`](../../codebench_pipeline/bashguard_audit_asset.py) `bashguard_syntax_audit` and `bashguard_audit_job` are registered separately by `build_definitions`, with [`bashguard_audit.py`](../../codebench_pipeline/bashguard_audit.py) `BashGuardAnalyzer` injected. | Asset, job and resource; this separate audit has its own input and classifications. Registration beside the build graph does not make it a release gate. |

## Persistence, input lineage and recovery

| Feature | Implementation and caller | Dagster surface and ownership |
| --- | --- | --- |
| Byte storage and table format | [`object_store.py`](../../codebench_pipeline/object_store.py) `ObjectStore` handles local and S3-compatible objects; [`storage.py`](../../codebench_pipeline/storage.py) `open_directory`, `fingerprint` and `exclusive` support safe file access, identity and local serialization. [`dataframe_io.py`](../../codebench_pipeline/dataframe_io.py) `frame_bytes`, `read_frame` and `DataFrameIOManager.persist_value` encode and retain content; `reference_at` and `read_version` resolve and read it. | `ConfigurableIOManager` adapter on top of ordinary storage. Keep digests, serialization, immutable byte references and integrity checks independent of source/review/release eligibility. |
| Binding and configuration IO | [`dataset_storage_io.py`](../../codebench_pipeline/dataset_storage_io.py) `dataset_storage_io` binds registry-selected storage and chooses `RegisteredDatasetTableIO` when review is enabled. [`config_assets.py`](../../codebench_pipeline/config_assets.py) `ConfigRunIOManager` writes run-scoped configuration outputs and loads the bound producer. Both bindings are installed in `build_definitions`. | `IOManagerDefinition`, `InitResourceContext`, `InputContext`, `OutputContext`. The registered adapter routes policy and holds the writer transaction for guarded branch/finalization reference changes. |
| Native Overview metadata | [`dagster_table_metadata.py`](../../codebench_pipeline/dagster_table_metadata.py) `record_preview` is called by `DataFrameIOManager.record_output`, shared by the generic and registered adapters. `record_reference` records the content address. | `add_output_metadata`, `TableSchema`, `TableRecord`, `MetadataValue.table_schema` and `.table`. Schema and total row count accompany a preview bounded by configured sample rows and 500 characters for text/serialized compound cells. Stored bytes are unaffected. |
| Input producer resolution | [`input_provenance.py`](../../codebench_pipeline/input_provenance.py) `input_materialization`, `producer_run_id` and `configuration_producer` serve `DataFrameIOManager` and `ConfigRunIOManager`. Producer-bound lookup matches run, step, asset and partition; only without a producer does it query the latest materialization. | `InputContext.upstream_output`, event-log queries and private `StepOutputHandle`/placeholder adapters. Keep producer-bound reads distinct from current reads. This extraction is present in source and has a separate owner. |
| Evidence and result reuse | [`dataset_engine.py`](../../codebench_pipeline/dataset_engine.py) `stage_signature`, `prepare_table_cache` and `cached_table` bind stage inputs to cache results and receipts; [`stage_implementation.py`](../../codebench_pipeline/stage_implementation.py) `capture_implementation` and `stage_versions` fingerprint implementation. [`finalization_cache_reuse.py`](../../codebench_pipeline/finalization_cache_reuse.py) `recover_admitted_finalization` and [`admitted_tables.py`](../../codebench_pipeline/admitted_tables.py) `resolve_admitted_table` validate reuse against producer events and receipts. | Asset output metadata and Dagster run/step event history identify evidence. Content identity and successful-producer checks are infrastructure; source rights and which receipts qualify for current use are policy. A cache hit does not replace Dagster selection or execution. |
| Finalization IO and failure receipts | [`finalization_table_asset.py`](../../codebench_pipeline/finalization_table_asset.py) `finalization_table_assets` wires [`finalization_table_io.py`](../../codebench_pipeline/finalization_table_io.py) `FinalizationTableIO` and `retain_failed_finalization`. The manager assembles typed stage inputs, prepares cache state, and hands a `TableOutputAdmission` to `RegisteredDatasetTableIO`. [`finalization_publication.py`](../../codebench_pipeline/finalization_publication.py) defines that admission payload and the `FinalizationPublicationV1` producer receipt. | `IOManager`, native input/output contexts and `failure_hook`. The registered writer checks immediate inputs under lock before advancing the reference. Per-attempt bookkeeping and failed diagnostics remain in the finalization manager. |
| Currentness and run selection | [`dataset_provenance.py`](../../codebench_pipeline/dataset_provenance.py) `dagster_provenance_status` wraps Dagster's stale resolver. [`dataset_dependencies.py`](../../codebench_pipeline/dataset_dependencies.py) `materialization_order` derives parents and order from the resolved graph, but `CACHE_INPUTS` separately names signature inputs. [`dataset_plan.py`](../../codebench_pipeline/dataset_plan.py) `_plan_materializations` combines materialization events, expected signatures, stored bytes, rights and review state; [`review_output_state.py`](../../codebench_pipeline/review_output_state.py) `materialization_stale` uses an app-level plan for read checks. | `resolve_asset_graph`, event queries and private stale resolver. Observing lineage is separate from authorizing output use. Today the plan also propagates blocked/selected states to choose the CLI and build-sensor subset (cut 2). |
| Inference checkpoint binding | [`labeling_checkpoint_io.py`](../../codebench_pipeline/labeling_checkpoint_io.py) `labeling_checkpoint_resource` is injected into `annotation_assets`; its `labeling_generation_identity` binds partition, stage signature and model binding. [`batch_labeling.py`](../../codebench_pipeline/batch_labeling.py) `LabelingCheckpointCache` owns the persisted batch payload contract. | `ResourceDefinition`, `InitResourceContext`, run/step history and native resume-retry known state. The resource checks for same-partition failed labeling steps in the retry lineage. It does not launch a retry or make a successful previous materialization reusable as a checkpoint. Source boundary observed in a separately owned change. |

## Viewing, human review and release

| Feature | Implementation and caller | Dagster surface and ownership |
| --- | --- | --- |
| Native UI packaging and mount | [`ui_build.py`](../../codebench_pipeline/ui_build.py) `build` packages the pinned native export. `ui_server.py` `NativeWebserver` and `integrated_app` mount the bundle and the same-origin `/codebench/api`. [`AssetsOverviewRoot.tsx`](../../dagster_ui/src/AssetsOverviewRoot.tsx) `AssetsOverviewRoot` adds Data and eligible Review tabs through the existing feature provider. | Dagster webserver subclass and native `AssetFeatureContext`/asset tabs. Pin-sensitive integration; the custom panels do not own the asset graph or the native Overview. |
| Data browsing | [`data_api.py`](../../codebench_pipeline/data_api.py) `make_dataset_api` serves artifact partitions and data. [`artifact_views.py`](../../codebench_pipeline/artifact_views.py) `ArtifactDataService`, [`artifact_schemas.py`](../../codebench_pipeline/artifact_schemas.py) `declared_artifact_schemas`, [`config_artifact_preview.py`](../../codebench_pipeline/config_artifact_preview.py) `read_config_preview`, and [`table_query.py`](../../codebench_pipeline/table_query.py) `query_table` handle persisted snapshots, declared/storage schemas, filtering, sorting and pagination. [`data_views.py`](../../codebench_pipeline/data_views.py) `DatasetDataService` provides the dataset-scoped read surface. | Reads native identifiers, definitions and materialization/storage state without executing assets. [`ArtifactInputs.tsx`](../../dagster_ui/src/ArtifactInputs.tsx) `ArtifactInputs` queries direct native `dependencyKeys`; the Data tab shows current persisted upstream artifacts for the selected input partition, not historical bytes consumed by the selected run. |
| Review preparation and workflow | [`review_preparation.py`](../../codebench_pipeline/review_preparation.py) `write_review_queue` prepares the batch and checks its identity under lock before publishing the queue reference. [`review_service.py`](../../codebench_pipeline/review_service.py) `ReviewService` connects the review source to [`review_workflow.py`](../../codebench_pipeline/review_workflow.py) `ReviewWorkflow` for pages, evidence, decisions, completion, retraction and reopening. [`review_inputs.py`](../../codebench_pipeline/review_inputs.py) `review_input_manager` binds decisions/release inputs for `human_assets`. | Queue output, review `AssetIn` input manager, and separate decisions/release assets. Browser reads query prepared state. Queue inclusion, editable columns, uncertainty thresholds and treatment of an empty queue are business policy. |
| Durable decisions and identity | [`human_review_store.py`](../../codebench_pipeline/human_review_store.py) `HumanReviewStore` supplies PostgreSQL readers and per-dataset writer transactions; [`human_review_transactions.py`](../../codebench_pipeline/human_review_transactions.py) `HumanReviewTransaction` and [`review_postgres_lifecycle.py`](../../codebench_pipeline/review_postgres_lifecycle.py) `ReviewLifecycle` persist lifecycle and decisions. [`review_identity.py`](../../codebench_pipeline/review_identity.py) `review_identity` hashes original configured review columns. | PostgreSQL is application state, not an alternate Dagster run store. Locking, snapshot binding and optimistic revisions are infrastructure. Reuse by original review values is business policy and does not authorize release of a new snapshot. |
| Release authorization and commit | [`publication_authorization.py`](../../codebench_pipeline/publication_authorization.py) `authorize_publication` and [`guarded_publication.py`](../../codebench_pipeline/guarded_publication.py) `prepare_guarded_release` enforce current eligibility and approval. [`release_admission.py`](../../codebench_pipeline/release_admission.py) `write_review_output` owns the serialized commit; `load_authorized_release` guards downstream use. | One native `release_dataset` job and authoritative `dataset_release` asset. Completion, revalidation, category caps and revocation are policy. The publication row becomes visible after writing the attested objects. |
| Release continuation | `ui_server.serve` injects [`release_runner.py`](../../codebench_pipeline/release_runner.py) `ReleaseContinuation`, which launches via native GraphQL after `_release_request` checks authorization and existing publication/run state. `build_definitions` registers the default-running `dataset_release_requests` sensor, which calls the same helper for completed empty queues only. `dataset_run.run_registered` uses `ReleaseRunner` and `execute_in_process`. | Native GraphQL launch, run tags/filters, `SensorDefinition`, and a completion-derived `RunRequest` key. The empty-queue sensor evaluator was restored during inspection; its wiring resolves in source. A daemon tick was not observed. Explicit completion for a nonempty queue remains on the review continuation path. |

## Inference and external evidence support

| Feature | Implementation and caller | Dagster surface and ownership |
| --- | --- | --- |
| Labeling runtime and baseline transport | [`labeling_runtime.py`](../../codebench_pipeline/labeling_runtime.py) `LabelingRuntime` binds credential lookup, capacity paths and grounding execution for `annotation_assets`. [`openai_transport.py`](../../codebench_pipeline/openai_transport.py) `complete` handles completion HTTP; [`inference.py`](../../codebench_pipeline/inference.py) `infer_typed` is the baseline typed call and receipt path. | Runtime `ResourceDefinition`; transport has no orchestration role. Model choice, response schema and labeling instructions are business inputs. |
| Capacity admission | [`inference_capacity.py`](../../codebench_pipeline/inference_capacity.py) `guarded_complete` checks metrics and token budget, shares admission state across calls, and records receipts. [`batch_labeling.py`](../../codebench_pipeline/batch_labeling.py) calls it for remote batch turns. | External service admission, no Dagster scheduler primitive. It fails closed on unavailable or stale metrics, and limits admission rather than isolating GPU compute. Keep it separate from per-subrisk sampling and run concurrency. |
| Similarity and labeling execution | [`command_embeddings.py`](../../codebench_pipeline/command_embeddings.py) `encode_from_directory` provides the encoder, [`command_batches.py`](../../codebench_pipeline/command_batches.py) groups rows, and `batch_labeling.py` `run_prepared_batches` drives bounded conversations and per-row outcomes. [`batch_labeling_ranked.py`](../../codebench_pipeline/batch_labeling_ranked.py) `RankedLabeling` applies ranked labeling. | Runs inside the labeling asset; the conversation graph is local inference control flow. Embedding mechanics, transport and attempt persistence support business choices such as ranking, evidence requirements, budgets and independent label judgments. |
| Inference grounding | [`grounding_tools.py`](../../codebench_pipeline/grounding_tools.py) `GroundingBroker` handles provider and response-cache mechanics; [`grounding_policy.py`](../../codebench_pipeline/grounding_policy.py) and [`grounding_passages.py`](../../codebench_pipeline/grounding_passages.py) handle evidence applicability and passages. `LabelingRuntime.execution` binds credentials. | Resource-backed service calls. Reusing a capture or a lookup response is infrastructure; treating a passage as applicable to a candidate is policy. |
| Collection acquisition | [`keenable_transport.py`](../../codebench_pipeline/keenable_transport.py) `request_direct` and [`repository_acquisition.py`](../../codebench_pipeline/repository_acquisition.py) `CaptureRuntime` support the branch assets; `declared_source_assets.py` `KeenableDiscovery` is injected by `build_definitions`. [`repository_etl.py`](../../codebench_pipeline/repository_etl.py) `authorize_adapter` binds reviewed transforms. | `ResourceDefinition` for acquisition dependencies, partitioned assets for durable outputs. HTTP, byte capture and receipts are support; authorized origin, parser approval, rights and extraction are branch policy. |
| Boundary enforcement | [`scripts/asset_boundary.py`](../../scripts/asset_boundary.py) `main` and `Boundary` statically inspect asset-reachable code; `make lint` runs the checks and [`tests/test_asset_boundary.py`](../../tests/test_asset_boundary.py). | No runtime primitive. Guard explicit configuration edges, addressed storage and resource injection; extend the existing check when a cut creates a new boundary. |

## Highest-value remaining cuts

1. **Table admission is separated at the checked commit point.**
   `DataFrameIOManager` encodes content, records metadata, and reads producer
   references independently of dataset policy. The existing `dataset_storage_io`
   binding uses `RegisteredDatasetTableIO` for registered assets and the
   generic writer for the separate syntax audit. The registered adapter calls
   `review_preparation.write_review_queue` and
   `release_admission.write_review_output` for the review outputs. Branch
   and finalization writes call the source and collection policy owners,
   recheck the registration and consumed references where required, invalidate
   review when the current reference changes, and publish under the dataset
   writer lock. Release visibility still comes from the committed PostgreSQL
   publication; `release_admission.load_authorized_release` checks that a
   native producer, when supplied, matches that authorized table. Current
   inspection reads use `review_output_state.read_current_snapshot`. Merge
   candidate admission remains on the existing typed edges in
   `FinalizationTableIO`; the generic reader no longer dispatches on candidate
   names. Relevant fixtures:
   [`tests/test_table_admission_boundary.py`](../../tests/test_table_admission_boundary.py),
   [`tests/test_native_retry_io.py`](../../tests/test_native_retry_io.py),
   [`tests/test_finalization_output_admission.py`](../../tests/test_finalization_output_admission.py)
   and [`tests/test_review_release.py`](../../tests/test_review_release.py).
2. **Separate graph observation, signature inputs and business readiness from
   launch selection.** `materialization_order` already reads native graph
   topology; keep it native. `CACHE_INPUTS` shadows selected input names/edges,
   while `_plan_materializations` combines readiness with recursive subset
   choice used by the CLI and sensor. Give signatures one source for named
   input identity, derived from the existing bindings where they match; keep
   policy-only signature inputs explicit. Keep source rights, expired review
   authority and integrity checks on the business/read paths. First prove
   parity for currentness reporting and the signature contract; any change to
   run or selection semantics requires explicit approval. Do not add another
   planner, retry loop or run-state store. Preserve selective branch runs and
   distinguish unchanged, blocked and missing outputs in
   [`tests/test_dataset_incremental.py`](../../tests/test_dataset_incremental.py).
3. **Batch checkpoint storage is now separate from inference.**
   [`labeling_checkpoint_store.py`](../../codebench_pipeline/labeling_checkpoint_store.py)
   owns the persisted format, locking and reads/writes. `labeling_checkpoint_io.py`
   binds it to the native failed execution lineage. Inference and storage use
   [`batch_labeling_rows.py`](../../codebench_pipeline/batch_labeling_rows.py)
   for their common row types. Neither the store nor its Dagster binding decides
   which batches to infer. A fresh materialization does not acquire a prior
   run's results; retry can recover completed batches only at matching inputs.

Before adding partitioned observations to drive build selection, bind the
approved revision at launch and verify it when loading inputs. An observation
alone does not prove the queued run processed the evaluated revision.
In Dagster 1.13.29, a named job with a job automation condition rejects
partitioned run config. Without that config, the daemon queues the entire job.
A newly queued asset subset also has no fixed producer for an input that it
does not materialize. After approval changes, reading the latest producer can
select the new revision even for a run created under the old one.

The Overview metadata and producer-resolution splits are already visible in
source. Do not count them as new extraction work. Keep the native UI shim
small and pin-specific; an independent UI or a generalized adapter layer is
not required by any of these cuts.

## Maintainability baseline

The November 19 checkout contained **154 Python files and 27,536 physical
lines** under `codebench_pipeline/`. Count each `*.py` file recursively,
including blank lines before the final nonblank line; exclude tests, scripts,
frontend, and trailing blank lines. A line-oriented import scan found **46**
files importing `dagster` directly and **13** with `from dagster._…` or
`from dagster_webserver._…` imports. These are file counts, not call counts;
the second includes the webserver shim, so it is not necessarily a subset
of the first. The private-import locations were `artifact_schemas.py`,
`bashguard_audit_asset.py`, `config_assets.py`, `dataset_provenance.py`,
`dataset_storage_io.py`, `definitions.py`, `finalization_cache_reuse.py`,
`finalization_table_io.py`, `input_provenance.py`, `labeling_checkpoint_io.py`,
`release_runner.py`, `synthetic_pilot.py` and `ui_server.py`.

| Hotspot | Physical lines | What to measure after the cut |
| --- | ---: | --- |
| `dataframe_io.py` + `finalization_table_io.py` | 601 → 417; 593 → 591 | The three queue/release/branch dispatch families moved out of generic I/O. The registered adapter is in `dataset_storage_io.py` (49 → 158 lines); queue commits and guarded release reads are in their policy modules. Existing producer resolution and typed processing edges remain in use. |
| `dataset_plan.py` + `dataset_dependencies.py` | 270 + 106 | Count signature-input names shadowing native edges in `CACHE_INPUTS`; eliminate duplicate declarations where an edge already provides the identity. Keep existing selection behavior until authorized otherwise. |
| `batch_labeling.py` | 1,331 | Verify fresh-run checkpoint isolation and same-generation native retry recovery independently of the labeling judgments. |
| `dagster_table_metadata.py`, `input_provenance.py`, `labeling_checkpoint_io.py` | 53, 56, 79 | Track their caller boundaries and private Dagster imports; keep preview truncation, producer lookup and checkpoint addressing separate. |

The line counts locate review effort; shrinking or moving a file alone is not
an acceptance test. The relevant tests above are regression anchors, not claims
that this documentation pass ran them or verified the live deployment.
