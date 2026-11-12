# IcyDB database changelog

Last coordinated release: [0.294.0](../history.md). Independent database releases
begin with 0.295.0 below.

## [0.310.2] - 2026-11-12

### 🔧 Changed

- Qualify current Linux database/CLI selections for eligible source changes,
  report deployment automation time per fixture and restore bounded macOS
  compiler outputs without skipping native checks
  ([#636 — Deployment validation cost](https://github.com/dragginzgame/icydb/issues/636)).
- Align product and qualification locks on Entropy 0.1.10, Metrics 0.29.12,
  Storage 0.73.6 and Wire 0.21.5.
- Avoid fetching canonical row and journal chunk payloads when only keys are
  needed, and bound schema namespace existence probes to one key
  ([#635 — Key-only Storage scans](https://github.com/dragginzgame/icydb/issues/635)).

[Detailed changes](0.310.md).

## [0.310.1] - 2026-11-12

### 🔧 Changed

- Adopt Storage 0.73.5 in the product and qualification dependency locks.
- Install Toko Tooling 0.39.17 with aligned workspace tooling selections.
  Complete the one-off directory-module layout migration while preserving Rust
  module paths; update source checks and references for the moved files.

[Detailed changes](0.310.md).

## [0.310.0] - 2026-11-12

### ⚠️ Breaking

- Use `InputValue` for filter literals, preserving scalar types, Unit and enum
  identity instead of stringifying them. Migrate `FilterValue` callers and
  regenerate serialized filter requests for the new literal contract.

- Replace the Testkit-bound startup driver with a dependency-free typed
  readiness predicate composed with Testkit's existing polling driver.
- Remove the obsolete executor error-detail enum; executor failures retain
  their canonical diagnostic codes, origins and numeric facts.

### 🩹 Fixed

- Isolate native simulator qualification from the product dependency lock,
  allowing independent thiserror updates while preserving bounded startup tests
  ([#634 — Decouple native testing dependencies](https://github.com/dragginzgame/icydb/issues/634)).

### 🔧 Changed

- Adopt IC Build 0.83 for qualification, using direct progress events and
  explicit best-effort cache maintenance before single and batch builds.

- Adopt Toko Tooling 0.39.15 and enable `make deps-update` for root and
  qualification dependency maintenance. Retain width-bounded LOC reports and
  supplier freshness review; this adoption preserves runtime selections.

- Adopt Host Tools 0.91.0 for native qualification and Testkit 0.101.0's upstream
  client re-export in the independent simulator harness.

### 🧹 Cleanup

- Bind physical index bounds to their owning access leaves, removing positional
  traversal cursors and parallel scalar/grouped handoffs. Covering reads and
  exact counts reuse the same prepared bounds. Physical scan dispatch consumes
  the bound leaf directly, with one prefix-count admission check.

- Retain only reusable global-aggregate shape in parameter templates; construct
  the execution query and metadata eligibility once with the current filter.

- Bind SQL WHERE parameters directly while lowering immutable syntax, removing
  the per-call syntax copy and substitution pass while preserving typed refusals.

- Admit filters once against accepted schema and reuse their predicates for
  planning and metadata counts. Cached SQL recompiles after schema-scope renewal;
  partial-filter coverage and original key-input limits remain enforced.

- Remove unreachable predicate-only query intent and impossible SQL filter states;
  preserve exact-key residual filtering, cache identity and metadata count paths.

- Reuse the ordinary schema publication head and borrow accepted schema bundles
  during relation-domain preflight, preserving pending activation and replay.
- Hash canonical planned predicates without copying and normalizing them again,
  preserving continuation bytes and fail-closed encoding admission.
- Borrow executable access children instead of rebuilding union/intersection
  trees, retaining ordered traversal and leaf-owned bounds.
- Simplify filter completeness bookkeeping while preserving partial SQL
  expressions and the proof required to remove redundant exact-key filters.
- Remove unused overlay classification; exhaustive recovery dispatch and
  exact-versus-superseded retirement remain authoritative.
- Move completed migration catalogues into fallback candidates without copying
  them again, retaining reconciled-candidate precedence.
- Share owned/borrowed aggregate ingestion while preserving numeric errors and
  cloning borrowed extrema only when selected.
- Avoid a redundant grouped DISTINCT membership lookup after admission, retaining
  collision equality and duplicate-at-budget behavior.

- Remove redundant initial schema sorting and borrow named-type catalogues during
  additive entity construction, preserving accepted identities and validation.

- Borrow fold-cardinality coalescing inputs and reuse admitted publication order
  during recovery, preserving checkpoint and schema authority checks.

- Reuse resolved relation component specifications and validate tuple contracts
  without an intermediate descriptor, preserving validation order.

- Share parameterized cache finalization between rebinding and first compilation,
  retaining charging order and failure-before-publication.

### 📊 Audit

- Simplification batches: 293 fewer non-test Rust code lines and 80 additional test
  code lines; remove a diagnostic representation and repeated schema/hash work.

[Detailed changes](0.310.md).

## [0.309.0] - 2026-11-11

### ⚠️ Breaking

- Adopt Storage 0.73's new whole-backing and collection formats. Existing images
  are rejected unchanged; hosts must explicitly provide fresh/reset whole backing.
- Adopt Wire/Derive 0.21.1 and Testkit 0.100 public codec and native testing
  types. Rebuild downstream Rust consumers against the new supplier identities.
- Generated TypeScript uses readable type-derived definition names. Regenerate
  clients and replace positional type imports; endpoint byte contracts are retained.
- Startup polling now takes Testkit's lifecycle owner and a closure that captures
  its client; upstream PocketIC owns native transport behavior.
- Wire decoding now accepts redundant varint groups within width bounds.
  Regenerate browser clients and retain original frames for retries/signatures;
  persisted schema and integrity artifacts retain canonical-byte admission.

### 🔧 Changed

- Include Storage 0.73.2's tooling-only patch; library APIs and formats are unchanged.

- Update the bundled SQLite correctness oracle to rusqlite 0.42.
- Adopt Host Tools 0.90 with Runtime 0.21.1 and Types 0.19, retaining current
  Host Foundation/Text patches in the product and native qualification selections.
- Adopt Toko Tooling 0.39.10's native PocketIC test allowance for the Testkit migration.
- Regenerate browser clients with Wire 0.21.1's alias resolver; contract metadata and bytes are unchanged.

### 🧹 Cleanup

- Dependency-admission fixtures use one current-source inventory, including pending additions and removals.
- Share fixture completion and failure retention while preserving process cleanup and diagnostics.
- Remove duplicate index/page error wrappers and a redundant old/new selector; public diagnostics are unchanged.
- Use one primitive type inventory for visitor traits.
- Share DISTINCT pagination and SQL budget test setup while retaining scenario assertions.
- Share key-stream dispatch without changing concrete polling or count/page-bound hints.
- Unify lower/upper bound reduction while preserving comparison budgets and strictness.
- Centralize data/index bulk provenance preflight; retain typed receipts and schema recovery rules.
- Share canonical standard-library/Serde paths across generated derives and implementations.

### 📊 Audit

- Cleanup removes 105 non-test and 43 test Rust code lines, plus 84 shell-fixture code lines.
- Further cleanup removes 55 non-test Rust code lines and adds 45 test code lines for bulk preflight controls.

### 🩹 Fixed

- Align native qualification codec and bulk-policy tests with Wire's bounded
  redundant varints and type-derived aliases, retaining framing and field-scope checks.

- Align qualification release tests with their existing CI model-generation
  profile; keep facade Testkit coverage in the root workspace.

- Keep the shared-adapter fixture's copied scanner dependency complete after
  adopting the common fixture lifecycle helper; Cargo failure transport stays tested.

- Readmit the release mapping after moving pending notes to 0.309, allowing
  full release validation to start without renewing selected-stage qualification.

- Restore browser codec qualification after the native workspace split by
  selecting the qualification workspace for its contract generator.
- Keep index-spec invariant scanning aligned with canonical executor errors after cleanup.

[Detailed changes](0.309.md).

## [0.308.0] - 2026-11-10

### 🔧 Changed

- Adopt Entropy 0.1.6, Host Foundation 0.76.3 and Host Text 0.74.4 across
  the product and native qualification selections.

- Separate native qualification from product dependency resolution while retaining
  CI coverage and Core’s approved dev-only SQL oracle support.

- Adopt Toko Tooling 0.39.7 and retain the explicit shared release interface.
  Retire managed reference docs through the exporter in favor of pinned upstream
  links; organize native architecture, roadmap and release guidance by topic.

- Share SQL cache resident admission across literal commands and parameterized
  templates; classify INSERT omissions directly while preserving error priority.

- Reuse resolved SQL write fields across admission checks and patch construction.
  Prepare RETURNING projection once for validation, precommit bounds and response
  shaping while preserving diagnostic precedence and requested column order.

- Validate accepted predicate canonical form once per tree and render partial-index
  SQL directly from accepted metadata. Share catalog literal formatting and integer
  width selection while preserving SQL spelling and admission rules.

- Share numeric CHECK fixtures and integer range checks; remove redundant
  allocation-snapshot assertions while retaining input-admission coverage.

- Adopt Storage 0.72.0: handle opening trusts the admitted directory; explicit
  diagnostics retain integrity checks. The stored format is unchanged.


[Detailed changes](0.308.md).

## [0.307.0] - 2026-11-10

### 🔧 Changed

- Align Entropy on published 0.1.2; generator production code and IcyDB boot
  entropy policy remain unchanged.

- Include the shared qualification-release helpers in validation fingerprints
  and the actual release-plan fixture closure.

- Adopt Host Tools 0.89.0 and Testkit 0.97.0 with Platform 0.29.2; retain
  the existing Wire 0.19 and Platform Types 0.19 identities. Remove the obsolete
  one-way Wasm-export admission branch retired by Host Tools.

- Adopt Host Foundation 0.76.2 (Artifacts, Fs, Process and HTTP), aligning
  direct requirements with the selected patch and its SHA-2 0.11 integration.
  Use Host Process's script helper in the optimizer fixture to prevent inherited
  writable descriptors from making executable admission fail.

- Adopt Wire/Derive 0.19.0 and Metrics 0.29.10. Align native Wire metadata
  with the existing Platform Types 0.19 identities.

- Adopt published Toko Tooling 0.38.2 and its authenticated snapshot in both
  maintained workspaces. Retain current package layout pending the separately
  scoped qualification-workspace migration.

[Detailed changes](0.307.md).

## [0.306.0] - 2026-11-09

### ⚠️ Breaking

- Adopt Platform 0.28.0 through Testkit 0.96.0. Native testing uses canonical
  owned Principal identities and Platform-owned Candid codecs, errors and bounds;
  the root graph now shares one Types 0.19 provider. Canic remains deferred.

- Adopt Platform Types 0.19.0 for canonical Principal and Account identities.
  Remove the retired external-Principal conversion feature; transport callers
  continue through checked bytes with unchanged identity and wire semantics.

- Adopt published IC Storage 0.71.1 with bucket-backed allocation. Use fresh
  backing; reject incompatible whole-store images without automatic migration
  or reset. Existing 0.71 backing remains reopenable. The public Storage config
  loses its Wire limit and directory errors no longer include a Wire variant.
  Canic remains on its deferred independent stack.

### 🔧 Changed

- Restore execution-mode admission on consumer-owned build, test and release
  entrypoints after the shared tooling guard became recipe-scoped.

- Adopt Storage's bounded directory reads and allocation lookup/verification
  improvements while retaining the 0.71 backing format and reopen contract.

- Align direct and Storage-owned raw IC0 dependencies on Platform Runtime 0.21.0,
  retaining one Runtime provider and rejecting duplicate executor ownership.

- Adopt published IC Entropy 0.1.0 for seeded expansion, preserving native entropy
  acquisition, legacy u128 ordering and monotonic ULIDs. Retain database boot-seed
  and cursor-key policy; remove the temporary sibling dependency.
  [#633 — published generator adoption](https://github.com/dragginzgame/icydb/issues/633).

- Update direct SHA-2 hashing to 0.11.1 with unused default features disabled.
  Preserve digest bytes and domain separation; retain older transitive versions
  where their owners still require them.

- Adopt published Toko Tooling 0.37.5 with renamed commands, settings, snapshot
  receipt and engineering baseline. Align both tooling selections while keeping
  Canic's runtime graph deferred. Preserve test-closure classification and reject
  obsolete LOC metadata namespaces before counting source. Retain offline guides
  explicitly and adopt current direct-dependency guidance for future updates.

### 🧹 Cleanup

- Share live-page test traversal across dynamic and public SQL paths, retaining
  every scenario, resume assertion and lane-specific budget check.
- Keep accepted index identity and predicate metadata in one implementation,
  with distinct field-path and expression key contracts.
- Share constraint scan bounds and stored-row identity validation while retaining
  separate budget admission, findings and unique-index publication.
- Declare broad diagnostic projections once, preserving public codes and formatting.
- Share List/Set schema-generation metadata while retaining collection semantics
  and Set-specific validation.

### 📊 Audit

- Review all ten simplification candidates; preserve distinct lifecycle and
  binding flows where a common wrapper would add complexity.
  The earlier cleanup removed 285 lines (154 non-test, 131 test); further
  live-page consolidation removes 39 test lines without removing scenarios.

[Detailed changes](0.306.md).

## [0.305.0] - 2026-11-09

### ⚠️ Breaking

- Adopt Storage 0.70, Wire 0.18 and Testkit 0.95 at IcyDB's public dependency
  boundaries. Retain the current storage image and Wire contracts; no reset
  or schema regeneration is required. Canic integration remains deferred.

### 🔧 Changed

- Adopt Build 0.81 with fallible maintenance configuration and the common Host
  batch-label error. Retain Host Foundation 0.76.1, Platform 0.27.1 and Metrics
  0.29.8 while preserving unrelated lock selections.

### 🧹 Cleanup

- Share schema field preparation and reuse accepted snapshot updates while
  preserving historical values and migration admission.
- Derive journal record bytes and allocation-free size bounds from one layout;
  retain independent decoding and corruption checks.
- Generate public diagnostic projections from shared local declarations while
  preserving public types, named formatting and wire codes.
- Share constraint-validation progress handling while retaining separate proofs
  and atomic unique-index staging.
- Finalize scalar scan hints and exact-probe overrides together before execution.
- Share migration cold-restart fixtures without removing behavioral scenarios.

### 🩹 Fixed

- Readmit reviewed gate declarations after the tooling-admission and release-note
  updates so full release validation can start; preserve conservative fallback
  ([#603 — Release validation qualification](https://github.com/dragginzgame/icydb/issues/603)).

### 🧪 Testing

- Admit independent Canic tooling before adapter dependency fetching and checks;
  preserve the deferred runtime graph
  ([#373 — Canic adapter qualification](https://github.com/dragginzgame/icydb/issues/373)).
- Prevent hosted installer-fixture failures with download caching enabled while
  retaining platform URL coverage
  ([#632 — Hosted adapter assertion diagnosis](https://github.com/dragginzgame/icydb/issues/632)).

[Detailed changes](0.305.md).

## [0.304.0] - 2026-11-08

### 🩹 Fixed

- Give caller-controlled SQL an explicit public paging API with canonical read
  admission, bounded work and authenticated continuation; trusted cache reuse
  grants no extra execution authority
  ([#335 — Guarded SQL read admission](https://github.com/dragginzgame/icydb/issues/335)).

### ⚠️ Breaking

- Adopt Storage 0.69's direct collection results and host-trap contract for
  engine/codec failures. Preserve fallible admission, bounded record decoding
  and existing stored images; no reset or image migration is required.
- Split generated SQL reads into public and trusted native capabilities. Host
  adapters must select the public capability after application guard approval;
  Canic integration remains deferred. Unsafe or unsupported public shapes reject
  before execution, while explicitly trusted SQL retains its feature set.
- Keep top-level SELECT DISTINCT on trusted SQL until its materialization has a
  complete public paging contract; public calls reject before row reads.

### 🔧 Changed

- Adopt Testkit 0.94's inherent client helpers and matching Host 0.76 / Platform
  0.27 graph for native testing; preserve typed startup polling and the deferred
  Canic stack.
- Adopt published Toko Tooling 0.36.0 through exact workspace pins and inherited
  package dev-dependencies, replacing the metadata/dedicated-lock selector.
  Preserve real packages and admission before repository effects; Canic remains
  blocked by its existing qualification graph.
- Adopt Shared Tooling 0.31.12's compact sibling LOC progress and terminal name
  colours, preserving plain output and counts.
- Adopt Build 0.80, Host Tools 0.88 and direct Host Foundation 0.76, preserving
  native commands and Wasm-interface inspection.
- Record matched Linux database and CLI release comparisons; subsequent
  dependency changes retain the full-validation fallback
  ([#603 — Release validation qualification](https://github.com/dragginzgame/icydb/issues/603)).

### 🧪 Testing

- Qualify release routing and publication from fixture-owned Git workspaces,
  verifying current tooling admission before dispatch, including refusal controls.
- Retain failed static-CI automation fixtures and failing source locations so
  release-tooling regressions remain diagnosable
  ([#603 — Release validation qualification](https://github.com/dragginzgame/icydb/issues/603),
  [#632 — Hosted adapter assertion diagnosis](https://github.com/dragginzgame/icydb/issues/632)).
- Isolate substitute-Cargo dispatch controls from inherited product checkpoints,
  retaining actual Make wiring and every required-failure control
  ([#603 — Release validation qualification](https://github.com/dragginzgame/icydb/issues/603)).

[Detailed changes](0.304.md).

## [0.303.0] - 2026-11-08

### Added

- Add explicitly multi-commit typed bulk writes with canonical byte admission,
  bounded attempted work, ordered assigned-key results and committed-prefix
  progress. Split only stage-proven precommit byte refusals; keep atomic batches
  unchanged ([#323 — Bounded typed bulk writes](https://github.com/dragginzgame/icydb/issues/323)).

### 🩹 Fixed

- Preserve cursorless ordered LIMIT scan bounds through the shared covering
  pager, retaining residual filtering and missing-row refusal
  ([#345 — Covering scalar pages](https://github.com/dragginzgame/icydb/issues/345)).

### 🧪 Testing

- Keep malformed boot-record version, state and checksum controls anchored to
  the current hard-cut identity
  ([#102 — Identity checkpoint storage](https://github.com/dragginzgame/icydb/issues/102)).

- Qualify shared formatter dispatch with workspace metadata and explicit member
  selection in consumer fixtures, preserving separate product/adapter ownership.

- Cover exhaustive covering-page refusal after data or index-readiness changes;
  reject missing/rebound proofs before scanning on public and trusted paths
  ([#345 — Covering cursor source proofs](https://github.com/dragginzgame/icydb/issues/345)).

- Give native SQL harness and coverage checks one root-workspace owner, removing
  duplicate harness execution and the dedicated Canic pure-harness CI lane.
  Consolidate ordinary accepted-field fixtures without dropping boundary cases.

- Qualify Nat128 index ranges against full-scan expression comparisons across
  the signed ceiling, repeated boundary keys and the maximum unsigned value
  ([#110 — Numeric comparison acceptance](https://github.com/dragginzgame/icydb/issues/110)).

### ⚠️ Breaking

- Separate live identity updates from immutable schema checkpoints. Require fresh
  database images; reject prior images without modifying retained data
  ([#102 — Identity checkpoint storage](https://github.com/dragginzgame/icydb/issues/102)).

- Adopt Testkit 0.93 and its owned structured CBOR error contract behind the
  public testing facade. Align native Host Tools 0.86 and Platform 0.25.

### 🧹 Cleanup

- Adopt Shared 0.31.11: close locally implemented, focused-validated issues
  separately from delivery, retaining issue-specific external acceptance.
- Adopt Build 0.79 and Testkit 0.93.1. Share target-maintenance policy between
  single and batch builds while retaining Testkit-owned server acquisition.
- Adopt Shared 0.31.10 with workspace-scoped formatting, LOC module/depth
  selection, authenticated CI download caching and one full-history secret scan.

- Use the scalar pager for eligible direct covering reads, including hidden order
  fields and authenticated continuations. Preserve row-presence checks and
  specialized readers for unsupported shapes
  ([#345 — Covering scalar pages](https://github.com/dragginzgame/icydb/issues/345)).

- Derive all generated coverage-witness metadata from one declaration owner;
  retain the existing schedule and coverage identities. Reuse ordinary schema
  field fixtures at 51 further sites while keeping codec oracles explicit.

- Keep SQL interaction exemptions and required evidence beside their owning
  declarations; repair stale provider paths and retire unused coverage metadata.
- Share naming admission and canonical catalog-retention mechanics. Derive
  executor diagnostic categories from the existing typed diagnostic registry.
- Reuse Shared's bounded regular-file reader for release evidence and source
  admission; preserve full validation until current native mapping qualification
  completes ([#603 — Release validation qualification](https://github.com/dragginzgame/icydb/issues/603)).

- Remove Core and SQL helper dependencies on the Platform hashing facade;
  consume the same BLAKE3 1.8.7 implementation/features directly. Migrate the
  integration hashing imports while retaining its genuine Platform capabilities;
  independent Canic qualification remains blocked under #373
  ([#631 — Direct pure hashing](https://github.com/dragginzgame/icydb/issues/631)).
- Adopt Shared Tooling 0.31.6's hashing inventory and dependency guard. Copy
  the checker's companion inventory into Cargo-adoption fixtures.

[Detailed changes](0.303.md).

## [0.302.0] - 2026-11-07

### 🩹 Fixed

- Skip rewriting the live schema checkpoint when recovery replays an already
  materialized identity advance, preserving validation of conflicting advances
  ([#102 — Identity checkpoint writes](https://github.com/dragginzgame/icydb/issues/102)).

### 🧹 Cleanup

- Simplify targeted-rule tests around budget refusal and stable results, removing
  duplicated cases and assumptions about internal allocation/scan formulas.

- Withdraw the pending per-allocation schema accounting expansion. Retain
  existing input size/depth limits, bounded decoding and normalization budgets;
  remove allocation-specific callbacks, metadata copying and bookkeeping tests.
  Close the original normalization finding on its delivered bounded-work fix
  ([#76 — Bounded predicate normalization](https://github.com/dragginzgame/icydb/issues/76)).

### ⚠️ Breaking

- Compare mixed numeric values exactly across predicates, projections and SQL
  sorting; preserve distinct float/Decimal predicate identities. Cover exact
  range boundaries, MIN/MAX and grouped top-K ordering with regression tests.
  Preserve NULL-first query ordering across live-page and DISTINCT continuation.
  Reject prior database images and cursor tokens without deleting retained data
  ([#110 — Exact numeric comparison and ordering](https://github.com/dragginzgame/icydb/issues/110)).

- Adopt Testkit 0.92 for public native testing types and Platform 0.24 client
  errors. Direct consumers sharing those types must align.

### 🔧 Changed

- Adopt Build 0.78's heartbeat-only progress configuration while preserving
  build labels, phases, cache admission and complete failure reporting
  ([#630 — Adopt heartbeat-only Build progress](https://github.com/dragginzgame/icydb/issues/630)).
- Adopt Host Foundation 0.75, Host Tools 0.85, Platform 0.25, Storage 0.68.7,
  Wire 0.17.12 and Metrics 0.29.6. Align direct Runtime dependencies with Storage
  on 0.20, removing the duplicate 0.19 identity. Retain Wasm feature separation.
  Native Host Tools/Testkit retain their supplier-owned Platform 0.24 dependency.
- Adopt Shared Tooling 0.31.5 and its complete release-guide export and isolated
  release-command checks. Preserve full validation fallback for changed inputs.

[Detailed changes](0.302.md).

## [0.301.0] - 2026-11-07

### ⚠️ Breaking

- Adopt Testkit 0.90 for public native testing types and explicit finite request
  budgets. Native consumers sharing those types must use the same Testkit line;
  stored database formats and current-image recovery are unchanged
  ([#629 — Migrate root PocketIC request budgets](https://github.com/dragginzgame/icydb/issues/629)).

### 🐛 Fixed

- Complete host Wire metadata for typed IDs, Unit, Blob and the 31 transparent
  model base wrappers. Consumer DTO bindings retain their existing Serde bytes,
  with endpoint-owned admission limits and no Wasm metadata dependencies
  ([#628 — Complete consumer DTO metadata](https://github.com/dragginzgame/icydb/issues/628)).
- Remove validation-only field-kind copies from CHECK comparisons, length and
  cardinality checks. Borrow accepted field/catalog/literal kinds while retaining
  current admission and diagnostics
  ([#76 — Predicate construction accounting](https://github.com/dragginzgame/icydb/issues/76)).
- Reuse operation-local canonical-key buffers during CHECK Boolean sorting and
  validation, preserving byte order, duplicate elimination and typed refusals
  ([#76 — Predicate construction accounting](https://github.com/dragginzgame/icydb/issues/76)).
- Admit targeted-rule and record-member ordering through the existing caller
  budget, including sort scratch and record ordinal backing. Preserve typed
  refusal facts and historical activation authority
  ([#76 — Predicate construction accounting](https://github.com/dragginzgame/icydb/issues/76)).
- Charge CHECK and targeted-rule literal inputs to the caller's existing decode
  allowance before decoding, preserving cumulative limits and typed refusals
  ([#76 — Predicate construction accounting](https://github.com/dragginzgame/icydb/issues/76)).
- Remove newtype-resolution cycle-tracking allocations using the accepted
  catalog's finite bound, preserving borrowed results and cycle refusals
  ([#76 — Predicate construction accounting](https://github.com/dragginzgame/icydb/issues/76)).
- Admit targeted-rule field and newtype lookup work through the existing caller
  budget, reusing accepted physical slots without a second layout scan
  ([#76 — Predicate construction accounting](https://github.com/dragginzgame/icydb/issues/76)).
- Validate CHECK encoded-size limits without allocating and discarding full
  canonical keys, retaining the existing encoder, bytes and typed bounds
  ([#76 — Predicate construction accounting](https://github.com/dragginzgame/icydb/issues/76)).

### 🔧 Changed

- Remove duplicate generic tool-command fixtures from both macOS CI matrices,
  retaining IcyDB's automation, setup and Canic adapter checks.
- Adopt Build 0.77's complete batch results and raise declared Platform,
  Runtime and Types floors to the selected 0.23.6, 0.19.5 and 0.18.2 versions.
  Retain cache identity, artifact custody and complete failure reporting; align
  the post-link invariant with the shared error-and-details accessor.
- Keep release preflight aligned with the pending database notes while retaining
  full validation for changed dependencies, workflows and unqualified mappings.
- Adopt Shared Tooling 0.31.0 and its saved-context setup, status, recovery
  and guarded reconciliation commands. Use the renamed release-batch settings,
  automatic capture and schema-2 journals; retain older state with its original
  tooling. Recovery checks saved tools offline and preserves deferred edits.
- Reject malformed checksum output and incomplete snapshot annotations through
  the reviewed Shared integrity helpers.

[Detailed changes](0.301.md).

## [0.300.0] - 2026-11-06

### ⚠️ Breaking

- Adopt Testkit 0.89.2 for native public testing types. Consumers sharing
  PocketIC or polling-error types with direct Testkit imports must use the
  same Testkit line. Applications own client logging sinks; stored database
  formats and current-image recovery are unchanged.

### 🔧 Changed

- Adopt Shared Tooling 0.30.8, including frozen release-batch support and CBOR
  ownership checks. Qualify captured-source delivery and recovery on Linux;
  default release dispatch to the explicitly selected group, keeping publication
  explicit and normal-checkout dependency edits deferred.
- Separate live product invariants from automation regression fixtures and
  support qualified database/CLI selection through the existing proof and recovery
  owners. Retain all affected product and consumer coverage; unknown inputs
  keep the full gate and selected publication still verifies final archives
  ([#603 — Qualify database and CLI release plans](https://github.com/dragginzgame/icydb/issues/603)).

[Detailed changes](0.300.md).

## [0.299.0] - 2026-11-06

### ⚠️ Breaking

- Adopt Storage 0.68.0 collection engines and Testkit 0.88.0 public testing types.
  Require fresh installations; reject earlier database images before recovery
  without rewriting or deleting them. Preserve current-image recovery and
  application codecs; engine/codec failures now follow Storage’s trap contract.

### 🩹 Fixed

- Admit row-constraint integrity metadata backing through the caller's existing
  construction budget before allocation, preserving accepted-rule scope and
  typed exhaustion diagnostics
  ([#76 — Constraint integrity backing](https://github.com/dragginzgame/icydb/issues/76)).
- Admit row-program dependency-slot collection through the same cumulative
  construction budget and remove intermediate dependency vectors, preserving
  sorted unique slots and activation scope
  ([#76 — Dependency-slot backing](https://github.com/dragginzgame/icydb/issues/76)).
- Admit row-program ordering and compact-sort scratch through the caller's
  existing construction budget before sorting, preserving constraint order
  and typed refusal
  ([#76 — Row-program sort admission](https://github.com/dragginzgame/icydb/issues/76)).
- Admit CHECK-tree structural backing and construction visits before compiling,
  with exact Boolean child-vector capacities and unchanged evaluation
  ([#76 — CHECK-tree construction](https://github.com/dragginzgame/icydb/issues/76)).
- Admit row-program metadata lookup, integrity and dependency scan work through
  the existing caller budget, preserving required slots and evaluation
  ([#76 — Row-program scan admission](https://github.com/dragginzgame/icydb/issues/76)).
- Validate Boolean CHECK children before constructing recursive ordering keys,
  preserving depth, node and literal bounds on malformed input
  ([#76 — Boolean validation bounds](https://github.com/dragginzgame/icydb/issues/76)).
- Remove intermediate Boolean CHECK child-key vectors and copies while
  preserving exact canonical key bytes
  ([#76 — CHECK-key buffer reuse](https://github.com/dragginzgame/icydb/issues/76)).

### 🔧 Changed

- Adopt IC Build 0.75.0 and keep cache-maintenance qualification on its supported
  single/batch specification API. Product runtime and storage contracts are unchanged.

### 🧪 Testing

- Qualify maximum journal fill/drain/refill reuse with Storage 0.68: retain
  exact backlog limits, empty tails after every drain and stable high-water
  capacity across repeated refills after the first warm refill.
- Share budget-diagnostic assertions across planner and normalization tests,
  retaining their independent expected results, limits and refusal checks.

[Detailed changes](0.299.md).

## [0.298.0] - 2026-11-05

### ⚠️ Breaking

- Keep index names automatic, with a bounded full-definition suffix and optional
  explicit overrides. Distinct fields, expressions and partial predicates no
  longer collapse onto the same readable name; conflicts include compiler examples.
  Automatic names change in this hard cut; explicit labels can retain existing names
  ([#203 — Canonical index naming](https://github.com/dragginzgame/icydb/issues/203)).
- Require explicit `build` for native model collection and actor generation,
  and independent `wire-schema` for native binding metadata. Runtime models,
  typed adapters and explicit application behavior no longer collect a global
  authoring graph. Build scripts and shared schema libraries must opt in.

### 🧹 Cleanup

- Compile CHECK and NOT NULL rules directly into one admitted destination,
  removing intermediate compiled-rule vectors while preserving rule semantics
  ([#76 — Constraint program backing](https://github.com/dragginzgame/icydb/issues/76)).
- Remove discarded predicate serialization from unique-constraint dependency
  compilation, retaining accepted-schema validation and budgeted projection
  ([#76 — Predicate construction accounting](https://github.com/dragginzgame/icydb/issues/76)).
- Remove temporary scalar-literal vectors and growing collection buffers from
  accepted-predicate projection; reject malformed scalar literal counts
  ([#76 — Predicate normalization allocation accounting](https://github.com/dragginzgame/icydb/issues/76)).
- Remove obsolete schema-reconciliation paths from SQL DDL planning while
  preserving supported schema changes, historical values and admission errors.
- Collapse duplicate schema-addition plan representations and remove unused
  query-routing facts, preserving physical index staging, MIN/MAX eligibility
  and EXPLAIN output.

### 🩹 Fixed

- Admit constraint-source traversal and temporary backing before compilation,
  preserving typed budget refusals across request and maintenance callers
  ([#76 — Constraint compilation accounting](https://github.com/dragginzgame/icydb/issues/76)).
- Admit accepted predicate literal payload bytes through the caller's decode
  budget before decoding, across metadata and row-contract projections; preserve
  retained recovery and typed corruption diagnostics
  ([#76 — Predicate normalization accounting](https://github.com/dragginzgame/icydb/issues/76)).
- Admit accepted-predicate nodes, field lookup/name copies and collection backing
  through the caller's existing budget before construction, across query, write
  and maintenance paths; preserve retained recovery authority
  ([#76 — Accepted predicate projection admission](https://github.com/dragginzgame/icydb/issues/76)).
- Charge predicate-normalization visits, membership/set and encoded-map ordering,
  OR/AND label/payload comparisons to the existing construction budget, including
  repeated set comparisons, raw membership-key ordering, conjunction compaction/revisits,
  enum-literal collection/text validation and casefold backing; preserve canonical
  results and retained recovery
  ([#76 — Bound predicate normalization work](https://github.com/dragginzgame/icydb/issues/76)).
- Expose a native-only async typed startup wait helper over Testkit polling, preserving
  terminal errors and bounded progress without matching diagnostic strings
  ([#315 — Published typed startup driver](https://github.com/dragginzgame/icydb/issues/315)).
- Generate whole-row conversions into existing checked write inputs and typed
  keys, preserving generated fields, explicit NULLs and composite identity
  ([#322 — Canonical whole-row mutation adapters](https://github.com/dragginzgame/icydb/issues/322)).
- Report default public-read admission and its rejection reason in typed and
  SQL EXPLAIN, using the execution policy without reading rows
  ([#275 — Explain public-read admission](https://github.com/dragginzgame/icydb/issues/275)).

### 🧪 Testing

- Qualify native replay/fold aborts after prepared schema handoffs are lost or
  misrouted, retaining successful preflight/application controls
  ([#154 — Production failure-boundary qualification](https://github.com/dragginzgame/icydb/issues/154)).
- Preserve partial unique-index ownership across decode refusal and conflicting
  retries, including expression keys, legal transfers and journaled recovery
  ([#76 — Filtered uniqueness decode boundaries](https://github.com/dragginzgame/icydb/issues/76)).
- Verify decode-budget refusal for populated mixed batches and successive writes
  under one request, preserving retained entry bytes, prior committed progress
  and identity-safe retry
  ([#76 — Populated mutation decode limits](https://github.com/dragginzgame/icydb/issues/76)).
- Qualify decode-budget insert refusal before publication, exact-limit success
  and identity-safe retry; replay partial-index writes with decode capacity
  exhausted across publication interruptions
  ([#76 — Mutation and recovery decode limits](https://github.com/dragginzgame/icydb/issues/76)).
- Cover decode-budget refusal at later membership and nested operands, before
  index scans, and across shared unique-index/migration projection allowances
  ([#76 — Predicate decoding budget coverage](https://github.com/dragginzgame/icydb/issues/76)).
- Qualify partial-index recovery across four publication interruptions with
  exhausted normalization allowances, preserving rows and full index bytes
  ([#76 — Predicate normalization recovery accounting](https://github.com/dragginzgame/icydb/issues/76)).
- Simplify parser expectations with direct SQL expressions and shared explicit
  statement defaults. Keep ORDER BY expectation trees independent of parser
  execution while preserving full syntax assertions.
- Reuse session-fixture installation and initial single-key schema construction
  while retaining null-handling, range, pagination and special-catalog coverage.

### 🛠️ Tooling

- Adopt Shared Tooling 0.30.4 with the macOS LOC fix and an enforced Candid
  source boundary. Compact historical experiments into topic summaries and
  keep new raw Wasm captures under `target/evidence/`; preserve release and
  recovery records. Report uncompressed module and code-section bytes.
- Adopt Storage 0.67.1, Testkit 0.87, Wire 0.17.2, Platform 0.23.0 and
  Host Tools 0.83 as one root dependency cohort; refresh Shared Tooling 0.30.4.
  Preserve stored layouts and codec bytes; Canic composition remains separately
  unqualified ([#373 — Canic adapter qualification](https://github.com/dragginzgame/icydb/issues/373)).
- Adopt the current Shared LOC presentation: clearer column spacing, `???`
  for unclassified source and diagnostic guidance in help. Preserve JSON
  categories and source totals.
- Restore exported Shared documentation links through the canonical upstream
  repair, without copying central fleet inventories
  ([Shared #355 — Complete exported documentation links](https://github.com/dragginzgame/shared-tooling/issues/355)).
- Adopt Shared Tooling 0.30.0: explicit source categories, plural test directories,
  external test/helper modules and checked lint expectations. Make source scans
  reject missing inputs; retain the independent Canic qualification boundary.
- Add explicit parser/planner CI selections with mandatory downstream SQL,
  session and reference-query acceptance. Explain full-gate fallback in release
  logs and CI summaries; release omissions still require a matching reviewed
  native qualification ([#603](https://github.com/dragginzgame/icydb/issues/603)).
- Align full workspace and documentation gates with explicit native generation,
  preserving graph-test coverage, compiler admission and paired runtime/build
  dependency examples after the feature cut.
- Select independently qualified CLI-only release checks on Linux, keeping
  database, shared-input and unqualified-host releases on full validation.
  Carry selected evidence through release recovery and keep final Cargo package
  verification before publication
  ([#603 — Qualify separate database and CLI release plans](https://github.com/dragginzgame/icydb/issues/603)).

[Detailed changes](0.298.md).

## [0.297.0] - 2026-11-04

### ⚠️ Breaking

- Adopt Metrics 0.29 and return IcyDB-owned instruction report values instead of
  recorder types. Preserve report fields, bytes and numeric refusal while keeping
  recording separate from report validation and arithmetic
  ([Observability #130 — Instrumentation-only Metrics](https://github.com/dragginzgame/ic-observability/issues/130)).

- Restore omitted journaled stores from their exact dormant allocation quartets,
  preserving retained bytes after a failed deployment. Refuse topology changes
  while generated applications are pending
  ([#31 — Recover rejected store topology changes](https://github.com/dragginzgame/icydb/issues/31)).

- Start a fresh-installation database contract. Earlier database boot identities
  refuse before recovery; old-format orphan-index and Unicode-provenance repair
  requests are retired from this line
  ([#38 — Retired orphaned index generations](https://github.com/dragginzgame/icydb/issues/38),
  [#111 — Retired Unicode index provenance](https://github.com/dragginzgame/icydb/issues/111)).

- Canonicalize the default Account subaccount across construction, decoding,
  text and database/index identity; explicit zero becomes absence. Preserve
  native binding reflection of nonzero optional subaccounts
  ([#70 — Canonical default Account identity](https://github.com/dragginzgame/icydb/issues/70)).
- Bind database identity to its durable incarnation so unrelated store topology
  no longer re-keys lineage or pending application receipts
  ([#32 — Stable identity across store changes](https://github.com/dragginzgame/icydb/issues/32),
  [#90 — Finish pending applications after intervening changes](https://github.com/dragginzgame/icydb/issues/90)).

### 🩹 Fixed

- Allow generated migrations to retry corrected source data after abort through
  an explicit declared attempt number, preserving exact-attempt replay and
  bounded retained rejection findings
  ([#96 — Retry an aborted migration](https://github.com/dragginzgame/icydb/issues/96)).

- Add explicit administrative retirement for abandoned or corrupt shared progress
  records, using exact observed evidence and completed recovery; preserve normal
  receipt acknowledgement and already committed effects
  ([#169 — Recover abandoned progress capacity](https://github.com/dragginzgame/icydb/issues/169)).

- Retain admitted source lineage with pending generated validation and publish it
  atomically on completion after restart or unrelated schema changes; abort
  preserves the prior digest
  ([#94 — Maintain pending source lineage](https://github.com/dragginzgame/icydb/issues/94)).

- Share lazy sibling-map selection across separated projection columns and
  paths inside expressions, preserving authored errors, missing/null values
  and complete-root corruption checks
  ([#400 — Reuse nested map selection across projections](https://github.com/dragginzgame/icydb/issues/400)).

- Carry accepted filter-reader state into cursor-page projection, preserving
  lazy untouched fields, corruption refusal and typed resource limits
  ([#410 — Reuse filtered rows through projection](https://github.com/dragginzgame/icydb/issues/410)).

- Admit predicate-ordering scratch and encoded sort keys against existing caller
  budgets before sorting, preserving canonical output and retained recovery
  ([#76 — Budget predicate simplification and normalization](https://github.com/dragginzgame/icydb/issues/76), partial).

### 🛠️ Tooling

- Adopt the committed Shared Tooling 0.29.4 snapshot and its bounded retained
  release-state admission. Align Host Tools 0.81 with the existing Platform 0.21
  selection and current Foundation/Build/Text patch floors.

- Keep release qualification receipts beside Git recovery evidence so cache
  cleanup cannot erase them; missing evidence still refuses reuse. Requalify the
  unchanged stage mapping for the new database minor-line notes owner
  ([#603 — Qualify separate database and CLI release plans](https://github.com/dragginzgame/icydb/issues/603), partial).

[Detailed changes](0.297.md).

## [0.296.1] - 2026-11-03

### 🩹 Fixed

- Charge ordinary partial-index commit preparation and new normalization vectors
  to existing caller budgets, preserving typed refusal and retained recovery
  ([#76 — Budget predicate simplification and normalization](https://github.com/dragginzgame/icydb/issues/76), partial).

### 🛠️ Tooling

- Adopt Platform 0.21 for the existing crypto capability; Runtime, Types and
  database byte contracts remain unchanged.
- Adopt published Host Tools 0.80.0 for native artifact tooling.
- Adopt the Shared Tooling 0.29.1 development snapshot and strengthen release-plan
  evidence and fixture isolation; narrow release gates remain disabled
  ([#603 — Qualify separate database and CLI release plans](https://github.com/dragginzgame/icydb/issues/603), partial).
- Isolate Cargo metadata fixtures from inherited Git context and expose focused
  grouped-ordering checks ([#603 — Qualify separate database and CLI release plans](https://github.com/dragginzgame/icydb/issues/603), partial).

### 📚 Documentation

- Preserve issue-linked fixes in pending entries, standing agent instructions
  and the authoring prompt; keep fixed issues visible in each group's main changelog
  ([#619 — Preserve issue-linked changelog fixes](https://github.com/dragginzgame/icydb/issues/619)).

[Detailed changes](0.296.md).

## [0.296.0] - 2026-11-03

- Adopt current Platform, Metrics, Storage and Wire contracts while preserving
  IcyDB-owned bytes; simplify database code and add opt-in release-plan
  qualification ([details](0.296.md)).

- Align public conversions and owned codecs with Types 0.18
  ([#611 — Adopt Types 0.18](https://github.com/dragginzgame/icydb/issues/611)).
- Consolidate identical store-snapshot accessors while preserving Wire DTOs
  ([#612 — Consolidate public store-snapshot accessors](https://github.com/dragginzgame/icydb/issues/612)).
- Adopt the earlier Shared Tooling 0.27.29 snapshot and consumer wiring
  ([#613 — Adopt Shared Tooling 0.27.29](https://github.com/dragginzgame/icydb/issues/613)).
- Separate metrics recording from frontend Wire report types
  ([#614 — Separate metrics recording types from Wire DTOs](https://github.com/dragginzgame/icydb/issues/614)).

## [0.295.1] - 2026-11-02

### 🧹 Cleanup

- Unify safe stable-store opening and clarify fallible initialization errors;
  retained storage and recovery contracts remain unchanged.
- Simplify six schema fixtures to use the existing IcyDB facade without redundant
  direct model dependencies ([#601](https://github.com/dragginzgame/icydb/issues/601)).
- Fix core packaging by keeping private SQL test helpers out of published
  dependency requirements ([#602](https://github.com/dragginzgame/icydb/issues/602)).
- Share sibling-map lookups in scalar projections and grouped keys while
  preserving expression order, lazy evaluation and retained sort-value reuse
  ([#400](https://github.com/dragginzgame/icydb/issues/400), partial).
- Accommodate Storage 0.65.1's larger map handles without enlarging heap-store
  backends or changing retained storage ([#604](https://github.com/dragginzgame/icydb/issues/604)).

[Detailed changes](0.295.md).

## [0.295.0] - 2026-11-02

- Keep the database family coordinated while releasing the developer CLI independently.

- Batch sibling field-path lookups in raw and retained scalar projections, preserving
  complete value validation ([#400](https://github.com/dragginzgame/icydb/issues/400), partial).
- Remove the ineffective migration finding acknowledgement from `Advance`;
  document retained rejection findings and retry limits
  ([#96](https://github.com/dragginzgame/icydb/issues/96), partial).
- Adapt isolated Canic fixtures and Wire diagnostics for Storage; published
  executor dependencies still block current Canic qualification
  ([#373](https://github.com/dragginzgame/icydb/issues/373), partial).
- Make async request boundaries fallible and return typed cross-root refusal
  instead of trapping ([#161](https://github.com/dragginzgame/icydb/issues/161)).
- Prepare heap Identity checkpoints once per store before commit, preserving
  existing stored bytes and marker recovery ([#102](https://github.com/dragginzgame/icydb/issues/102), partial).
- Report secondary-order eligibility and rejection reasons in accepted typed
  and SQL logical EXPLAIN ([#275](https://github.com/dragginzgame/icydb/issues/275), partial).
- Adopt Storage 0.65.0 with fresh-installation-only admission and approved allocation bounds, Metrics 0.27.0,
  Platform 0.18.0, Wire 0.15.0
  and Shared Tooling 0.27.28;
  require and pin Rust 1.99.1 while retaining isolated compiler coverage ([#600](https://github.com/dragginzgame/icydb/issues/600)),
  and carry the predicate-budget correction
  ([#76](https://github.com/dragginzgame/icydb/issues/76), partial).
- Let validated schema applications finish after unrelated schema publications
  ([#90](https://github.com/dragginzgame/icydb/issues/90), partial).
- Reject unreviewed compiler Unicode-data changes affecting persisted text
  indexes ([#111](https://github.com/dragginzgame/icydb/issues/111)).
- Resume ordered grouped pages beyond completed groups, preserving page work
  limits without reprocessing earlier pages ([#50](https://github.com/dragginzgame/icydb/issues/50)).
- Preserve heap-row references to journaled targets during online recovery,
  keeping delete restrictions enforced ([#170](https://github.com/dragginzgame/icydb/issues/170)).
- Select runtime entity/record generation through `icydb::model`; direct model
  macros stay schema-only regardless of incidental runtime dependencies
  ([#179](https://github.com/dragginzgame/icydb/issues/179)). Runtime consumers
  importing those macros directly from `icydb-model` must update their imports.
- Preserve cardinality-build progress across ordinary journal folds, maintaining
  counts behind the scan checkpoint ([#81](https://github.com/dragginzgame/icydb/issues/81)).

[Detailed changes](0.295.md).
