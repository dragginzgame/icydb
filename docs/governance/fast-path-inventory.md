# Fast-Path Inventory

## Purpose

This note records the current fast-path owner boundaries in `icydb-core`.
The goal is not to maximize optimization surface area. The goal is to keep
every admitted fast path attached to one canonical owner so new surfaces do
not re-derive eligibility locally and drift semantically.

## Ownership Rule

New fast-path eligibility must be derived in one of the canonical owners below.
Session, SQL, fluent, CLI, and other frontend surfaces may consume those
contracts, but they must not invent new eligibility rules locally.

## Current Inventory

### 1. Route stream fast-path precedence

Owner:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/contracts/shape.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/fast_path.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/pipeline/runtime/fast_path/mod.rs`

Responsibilities:
- canonical fast-path precedence order
- route verification before runtime dispatch
- fallback handoff when no fast path applies

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

## Known Gap

`SQL COUNT(*)` currently has a consumer integration gap:
- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/aggregate.rs`

It still counts rows through the structural SQL projection path instead of
reusing the shared count terminal boundary. That is a surface-routing problem,
not a missing executor fast-path contract.

## Current Guard Coverage

The structural guard added with this note enforces the terminal-derivation seam:
- `count()`, `exists()`, and load-terminal fast-path derivation must stay under
  the route owner boundary, with only the known shared runtime consumers allowed
  to reference those derive helpers.

This guard does not yet lock:
- stream fast-path precedence helpers
- grouped dedicated fast-path ownership
- the bytes-terminal derivation exception

Those can be tightened in follow-up guards once the remaining boundary story is
stable enough to freeze.
