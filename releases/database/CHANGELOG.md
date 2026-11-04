# IcyDB database changelog

Last coordinated release: [0.294.0](../history.md). Independent database releases
begin with 0.295.0 below.

## [0.297.0]

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
