# IcyDB Reduced-SQL Lane Split Audit (Duplication + Consolidation Plan)

Date: 2026-03-16  
Scope: current checkout only (`0.56.x` line in this repo)

## Executive Summary

The reduced-SQL lane split is semantically correct at a high level, but the current implementation re-encodes the same lane distinction in too many places:

- parser statement kinds
- lowering command variants
- route metadata
- core session surface-specific matches and rejection text
- facade forwarding mirrors
- generated `sql_dispatch` branching and lane checks
- duplicated core/facade lane-rejection tests

Real semantic value exists in keeping row-execution lanes separate from introspection lanes. The bloat is mostly in duplicated control flow and duplicated wrong-lane rejection plumbing.

Best near-term path for `0.56`: consolidate lane authority and lane rejection handling in core/session + simplify generated dispatch branching, while preserving public API and fail-closed behavior.

## Lane Topology Map

| Stage | Authority Type | Variants / Lanes | Evidence |
|---|---|---|---|
| Parser | `SqlStatement` | `Select`, `Delete`, `Explain`, `Describe`, `ShowIndexes`, `ShowEntities` | `crates/icydb-core/src/db/sql/parser/mod.rs` |
| Lowering | `SqlCommand<E>` | `Query`, `Explain`, `ExplainGlobalAggregate`, `DescribeEntity`, `ShowIndexesEntity`, `ShowEntities` | `crates/icydb-core/src/db/sql/lowering.rs` |
| Route metadata | `SqlStatementRoute` | `Query`, `Explain`, `Describe`, `ShowIndexes`, `ShowEntities` (+ `is_*` helpers) | `crates/icydb-core/src/db/session/sql.rs` |
| Core session SQL entrypoints | Multiple public methods | `query_from_sql`, `execute_sql`, `execute_sql_projection`, `execute_sql_grouped`, `execute_sql_aggregate`, `explain_sql`, `describe_sql`, `show_indexes_sql`, `show_entities_sql` | `crates/icydb-core/src/db/session/sql.rs` |
| Facade | Forwarding wrappers | Same lane set as core | `crates/icydb/src/db/session/mod.rs` |
| Generated dispatch | `sql_dispatch::query` + route methods | Branch by route lane, then lane-specific helper execution | `crates/icydb-build/src/db.rs` |
| Lane tests | Parser/lowering/core/facade/integration | Route classification, wrong-lane rejections, unsupported-label stability | `crates/icydb-core/src/db/sql/parser/tests.rs`, `crates/icydb-core/src/db/sql/lowering.rs` tests, `crates/icydb-core/src/db/session/tests.rs`, `crates/icydb/src/db/session/mod.rs` tests, `testing/pocket-ic/tests/sql_canister.rs` |

## Duplication Inventory

| Duplication Pattern | Where Re-Encoded | Why It’s Duplicate | Class |
|---|---|---|---|
| Lane taxonomy encoded twice (`SqlStatement` and `SqlStatementRoute`) | parser + `DbSession::sql_statement_route` match tree | same statement-kind distinction re-projected in a second enum + match | accidental duplication |
| Lane taxonomy encoded again (`SqlCommand`) | parser/lowering/session | same distinction again at lowering boundary | partly legitimate (typed lowering), partly duplicative |
| Wrong-lane rejection trees repeated | `describe_sql`, `show_indexes_sql`, `query_from_sql`, `explain_sql` | each method hand-matches many “other” variants with custom message text | accidental duplication |
| Inconsistent show-entities path | `show_entities_sql` uses route-check; others use lowering command | lane check logic split across two authorities | accidental duplication |
| Compile/lower calls repeated by surface | multiple core methods call `compile_sql_command::<E>(..., Ignore)` | same setup logic copied in multiple methods | accidental duplication |
| Generated dispatch lane branching duplicated | `query(...)` `if/else` chain + lane-specific helpers each re-check lane | lane already classified, then re-validated multiple times | accidental duplication |
| Generated route execution methods duplicated | `execute_projection_rows`, `execute_explain`, `execute_describe_schema`, `execute_show_indexes` | same match-over-route pattern, different closure target | accidental duplication |
| Facade forwarding mirrors every SQL lane method | `icydb` session facade | mostly mechanical wrappers, little semantic authority | unclear (API parity legit, implementation repetitive) |
| Core/facade tests duplicate rejection matrices | session tests + facade tests | near-identical negative-path assertions across both layers | accidental test duplication |
| Similar but separate lane rejection wording core vs generated | core errors + generated `unsupported_*_statement_error` | same contract expressed in two wording tables | accidental duplication |

## Legitimate Lane Separation (Should Remain)

1. Row-execution surfaces remain separate from introspection surfaces.  
Evidence: `docs/design/0.56-describe/0.56-design.md` (“row-execution surfaces”, “DESCRIBE … dedicated lane”), `docs/contracts/SQL_SUBSET.md`.
2. `execute_sql` vs `execute_sql_projection` vs `execute_sql_grouped` vs `execute_sql_aggregate` remain separate as output/shape semantics differ materially (entity rows, projected rows, grouped paged rows, scalar aggregate value).
3. `DESCRIBE`/`SHOW INDEXES` stay dedicated introspection lanes and must not lower to executable `Query<E>` planner paths.
4. Fail-closed unsupported-lane behavior and parser unsupported-feature labels must stay contract-stable.

## Accidental Duplication (Should Be Collapsed)

1. Repeated command-lane rejection matches in core session methods.
2. Split lane-authority path for `SHOW ENTITIES` (route-only check vs lowering for other introspection lanes).
3. Generated `sql_dispatch::query` lane if-chain + helper-level lane re-checks.
4. Duplicative generated route executor methods that differ only by called target function.
5. Repeated negative-path tests in core and facade for same unsupported-feature matrix.

## Unclear Cases (Good Candidates for Small Adapter Types)

1. `SqlStatementRoute` `is_*` boolean fan-out: likely better as one `kind()` accessor plus boolean wrappers.
2. Core wrong-lane error wording: centralize through one helper map to avoid drift.
3. Introspection command family in lowering: likely cleaner as `SqlCommand::Introspection(SqlIntrospectionCommand)`.

## Refactor Options (Ranked)

### Option 1 — Conservative / Low Risk

Keep all public APIs and lane semantics. Consolidate only control-flow duplication.

- Touch files:
  - `crates/icydb-core/src/db/session/sql.rs`
  - `crates/icydb-build/src/db.rs`
  - `crates/icydb-core/src/db/session/tests.rs`
  - `crates/icydb/src/db/session/mod.rs` tests
- Authority to introduce:
  - internal `SqlLaneKind` helper in core/session
  - internal shared lane-rejection constructor
  - internal `compile_sql_command_ignore::<E>(sql)` helper
- Duplication removed:
  - repeated compile/lower setup
  - repeated wrong-lane match/rejection blocks
  - generated `if/else` lane chain and redundant helper lane checks
- User-visible API changes: none
- Risk:
  - fail-closed behavior: low (logic centralization only)
  - EXPLAIN stability: low
  - generated helper stability: low-medium (branch rewiring, same result variants)
  - typed/facade parity: low

### Option 2 — Medium Consolidation

Normalize lane modeling in lowering/session with one introspection command family and one shared core lane dispatcher.

- Touch files:
  - `crates/icydb-core/src/db/sql/lowering.rs`
  - `crates/icydb-core/src/db/session/sql.rs`
  - `crates/icydb-build/src/db.rs`
  - tests in parser/lowering/session/facade/integration
- Authority to introduce:
  - `SqlIntrospectionCommand` and `SqlCommand::Introspection(...)`
  - shared `dispatch_sql_surface(...)` internal helper in core/session
- Duplication removed:
  - separate introspection variant matching across methods
  - much of wrong-lane duplication
  - generated dispatch helper fan-out
- User-visible API changes: none required
- Risk:
  - fail-closed behavior: medium (enum reshape + dispatch rewrite)
  - EXPLAIN stability: low-medium
  - generated helper stability: medium (more generated code diff)
  - typed/facade parity: medium

### Option 3 — Ambitious (Pre-1.0 Cleanup)

Introduce one internal SQL surface execution adapter in core and have all existing methods wrap it.

- Touch files:
  - `crates/icydb-core/src/db/session/sql.rs` (large)
  - `crates/icydb/src/db/session/mod.rs`
  - `crates/icydb-build/src/db.rs`
  - broad SQL test suites
- Authority to introduce:
  - internal `SqlSurfaceRequest` + `SqlSurfaceOutput` adapter
- Duplication removed:
  - most parallel method-local branching
  - most lane-specific wrapper plumbing
- User-visible API changes:
  - optional none (if wrappers preserved)
  - optional small cleanup (if deprecating a subset) — not recommended for `0.56`
- Risk:
  - fail-closed behavior: medium-high
  - EXPLAIN stability: medium
  - generated helper stability: high
  - typed/facade parity: medium-high

## Recommended Consolidation Plan

Preferred: **Option 1 now in `0.56.x`**, with Option 2 deferred to `0.57` only if residual duplication remains painful.

Reason:

- It removes high-ROI duplication without reworking core lane semantics that `0.56` explicitly codified.
- It preserves canonical authority and fail-closed contracts.
- It is realistic to land as incremental `0.56` patch slices.

## Do Not Touch Boundaries

1. Do not fold introspection into row-execution APIs.  
Keep dedicated introspection lanes (`DESCRIBE`, `SHOW INDEXES`, `SHOW ENTITIES`) separate from `execute_sql*`.
2. Do not weaken parser unsupported-feature labels or unsupported-lane fail-closed behavior.
3. Do not change `SqlQueryResult` variant shape or explain unordered-pagination guidance semantics in `0.56`.
4. Do not bypass entity trailing-segment matching in typed lane execution.
5. Do not turn generated route resolution into fallback trial chaining; keep deterministic immediate unsupported-entity errors.

## Concrete Patch Slices (Implementation Order, `0.56.x`)

### Slice 1: Core lane helper authority (`sql.rs`)

- File: `crates/icydb-core/src/db/session/sql.rs`
- Add:
  - internal lane-kind helper (`SqlLaneKind`)
  - internal helper to map `SqlCommand<E>` to lane-kind
  - internal helper for unsupported-lane error construction
  - internal helper for `compile_sql_command::<E>(..., MissingRowPolicy::Ignore)`
- Outcome: remove repeated setup and repeated lane message boilerplate.

### Slice 2: Consolidate introspection lane execution paths

- File: `crates/icydb-core/src/db/session/sql.rs`
- Refactor:
  - `describe_sql`, `show_indexes_sql`, `show_entities_sql` through shared lane checks
  - keep public methods unchanged
- Outcome: one lane-rejection authority for introspection entrypoints.

### Slice 3: Consolidate `query_from_sql` / `explain_sql` lane gating

- File: `crates/icydb-core/src/db/session/sql.rs`
- Refactor:
  - replace method-local long `match` rejection trees with centralized helper
- Outcome: one message/routing policy table for wrong-lane errors.

### Slice 4: Simplify generated `sql_dispatch` lane branching

- File: `crates/icydb-build/src/db.rs`
- Refactor:
  - replace `if statement.is_*()` chain with `match` over `SqlStatementRoute`
  - remove redundant `if !statement.is_*()` checks in helper functions where route is already matched
  - keep output payload variants unchanged
- Outcome: less branch surface, less duplicate lane checking in generated code.

### Slice 5: Deduplicate lane rejection tests

- Files:
  - `crates/icydb-core/src/db/session/tests.rs`
  - `crates/icydb/src/db/session/mod.rs` tests
- Refactor:
  - matrix-drive lane rejection cases once per layer
  - preserve explicit assertions for contract-critical paths (`DESCRIBE`, `SHOW INDEXES`, `SHOW ENTITIES`, `EXPLAIN`)
- Outcome: smaller, clearer parity suite without losing coverage.

### Slice 6: `0.56` docs/changelog closure

- Files:
  - `docs/changelog/0.56.md`
  - `CHANGELOG.md` (summary bullet only if patch ships)
- Add:
  - consolidation/hardening note: “lane-control-flow consolidation; no semantic surface changes”
  - validation command set

## Evidence Pointers

- `crates/icydb-core/src/db/sql/parser/mod.rs`
- `crates/icydb-core/src/db/sql/lowering.rs`
- `crates/icydb-core/src/db/session/sql.rs`
- `crates/icydb/src/db/session/mod.rs`
- `crates/icydb-build/src/db.rs`
- `crates/icydb-core/src/db/session/tests.rs`
- `crates/icydb/src/db/session/mod.rs` test module
- `testing/pocket-ic/tests/sql_canister.rs`
- `docs/contracts/SQL_SUBSET.md`
- `docs/design/0.56-describe/0.56-design.md`
