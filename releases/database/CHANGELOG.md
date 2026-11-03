# IcyDB database changelog

Last coordinated release: [0.294.0](../history.md). Independent database releases
begin with 0.295.0 below.

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
