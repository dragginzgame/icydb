# Fast-Path Inventory

## Purpose

This note records the current fast-path owner boundaries in `icydb-core`.
The goal is not to maximize optimization surface area. The goal is to keep
every admitted fast path attached to one canonical owner so new surfaces do
not re-derive eligibility locally and drift semantically.

It also records the current structural tripwires that guard those owner
boundaries, so route changes and surface integrations have one place to update
instead of relying on scattered institutional memory.

## Ownership Rule

New fast-path eligibility must be derived in one of the canonical owners below.
Session, SQL, fluent, CLI, and other frontend surfaces may consume those
contracts, but they must not invent new eligibility rules locally.

## Current Inventory

### 0. Exact primary-key canonicalization

Owner:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/planner/compare.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/planner/predicate.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/access/canonical.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/query/admission.rs`

Responsibilities:
- derive strict scalar primary-key `ByKey`, finite primary-key `ByKeys`, and
  contradictory exact-key `Empty` access from accepted runtime schema metadata
- keep explicit `by_id(...)` / `by_ids(...)`, fluent filters, SQL literals,
  admission, cache identity, and EXPLAIN route facts aligned on the same
  planner-selected access proof
- fail closed for wrong types, invalid residuals, over-budget key-list inputs,
  partial composite keys, expression-wrapped keys, and unsupported SQL
  parameter placeholders

Current admitted exact-key families:
- strict scalar primary-key equality
- commuted SQL literal primary-key equality
- finite scalar primary-key `IN (...)`
- contradictory exact-primary-key intersections that prove empty access

### 1. Route stream fast-path precedence

Owner:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/contracts/shape.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/fast_path.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/pipeline/runtime/fast_path/mod.rs`

Responsibilities:
- canonical fast-path precedence order
- route verification before runtime dispatch
- fallback handoff when no fast path applies
- ordered-read route facts and limit-stop ownership for pushed, residual,
  materialized, unsupported, and fallback load routes

Current route-level precedence families:
- primary-key stream
- secondary-prefix stream
- index-range stream

### 2. Scalar load terminal fast paths

Owner:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/terminal.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/covering/mod.rs`

Responsibilities:
- derive `LoadTerminalFastPathContract`
- own covering-read execution-plan construction

Current admitted load terminal family:
- covering read

### 3. Scalar aggregate terminal fast paths

Owner:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/terminal.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/terminals.rs`

Responsibilities:
- derive `CountTerminalFastPathContract`
- derive `ExistsTerminalFastPathContract`
- execute the prepared terminal boundary through the shared executor path

Current admitted count families:
- primary-key cardinality
- primary-key existing rows
- index-covering existing rows

Current admitted exists family:
- index-covering existing rows

### 4. Bytes terminal fast paths

Owner:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/terminal/bytes.rs`
- contract type still lives in `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/terminal.rs`

Responsibilities:
- derive bytes-terminal window eligibility from prepared execution state
- execute the verified bytes fast path

Current admitted bytes families:
- primary-key window
- ordered key-stream window

This is the main current exception to the otherwise route-owned terminal
derivation pattern.

### 5. Grouped dedicated aggregate fast paths

Owner:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/semantics/grouped_strategy.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/mod.rs`

Responsibilities:
- planner-owned grouped strategy selection
- runtime execution of the dedicated grouped fold paths

Current dedicated grouped family:
- grouped `COUNT(*)` path

## Current Consumer Routes

These are the current consumer surfaces that intentionally reuse the shared
owner boundaries instead of deriving fast-path eligibility locally.

### SQL count terminals with row-count semantics

Owner consumed:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/terminal.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/terminals.rs`

Consumer entrypoint:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/aggregate.rs`

Current route:
- SQL global `COUNT(*)` rebuilds one typed `Query<E>` from the lowered
  structural base query
- SQL global `COUNT(field)` may reuse the same route when:
  - the target field is schema-non-nullable
  - the aggregate is not `DISTINCT`
- shared query-plan cache resolution happens through the ordinary typed session
  query boundary
- execution then calls the shared scalar terminal request with
  `ScalarTerminalBoundaryRequest::Count`

## Surface Rule

These surfaces must stay consumers only:
- session query
- session SQL
- fluent query APIs
- CLI/shell packaging

They may choose among existing shared contracts, but they must not re-derive:
- count terminal eligibility
- exists terminal eligibility
- covering-read eligibility
- route precedence

## Current Tripwires

The current structural tripwires are:

### 1. Terminal fast-path derivation owner guard

Guard:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/tests/fast_path_guards.rs`

This guard enforces the terminal-derivation seam:
- `count()`, `exists()`, and load-terminal fast-path derivation must stay under
  the route owner boundary, with only the known shared runtime consumers allowed
  to reference those derive helpers.

### 2. SQL count consumer-route guard

Guard:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/tests/fast_path_guards.rs`

This guard checks that SQL count consumers keep using the shared scalar
terminal request instead of reopening the old structural projection-and-count
detour, and that the field-count widening stays behind one explicit
non-nullability guard helper instead of scattered local conditionals.

### 3. Stream fast-path precedence owner guard

Guard:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/tests/fast_path_guards.rs`

This guard enforces the load-stream fast-path ownership boundary:
- stream fast-path precedence must iterate through the shared route-owned
  `try_first_verified_fast_path_hit(...)` runner
- load runtime verification must consume the route-owned
  `load_fast_path_route_eligible(...)` helper instead of rebuilding route
  priority locally
- the execution-attempt runtime must enter stream fast-path resolution through
  the single `FastPathResolutionStrategy::for_route(...)` gate

## Remaining Unguarded Areas

The current tripwires do not yet lock:
- grouped dedicated fast-path ownership
- the bytes-terminal derivation exception

Those can be tightened in follow-up guards once the remaining boundary story is
stable enough to freeze.
