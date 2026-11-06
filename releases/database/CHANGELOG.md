# IcyDB database changelog

Last coordinated release: [0.294.0](../history.md). Independent database releases
begin with 0.295.0 below.

## [0.299.0]

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
