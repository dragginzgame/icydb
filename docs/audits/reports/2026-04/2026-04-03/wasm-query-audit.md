# Focused Audit - Query Export Wasm Footprint (2026-04-03)

## Scope

- target canister: `minimal`
- profile: `wasm-release`
- SQL variant: `sql-on`
- primary question: where is the fixed wasm cost for the main query types now that per-entity growth is no longer the dominant problem?

## Current Size Snapshot

Current `minimal` size report:

- built `.wasm`: `1,300,335`
- shrunk `.wasm`: `1,212,741`
- function count: `3,227`
- data section bytes: `171,056`

Primary retained hotspot from the current shrunk wasm:

- `export "canister_query query"`: `265,172` retained bytes (`21.87%`)

Current shallow top rows:

1. `data[0]`: `170,906`
2. `code[0]`: `23,411`
3. `code[1]`: `17,843`
4. `code[2]`: `17,331`
5. `code[3]`: `14,821`
6. `code[4]`: `12,516`
7. `code[6]`: `10,167`
8. `code[7]`: `9,701`
9. `code[8]`: `9,431`
10. `code[9]`: `8,809`

Current retained top rows under the query export root:

1. `export "canister_query query"`: `265,172`
2. `code[3188]`: `265,148`
3. `code[449]`: `265,138`
4. `code[0]`: `264,654`
5. `code[10]`: `40,862`
6. `code[3]`: `35,398`
7. `code[4]`: `30,730`

## Main Read

The fixed wasm problem is still the general-purpose generated query endpoint, not per-entity scaling.

The generated canister query export is one public ABI root:

- `#[query] fn query(sql: String) -> Result<SqlQueryResult, icydb::Error>`

That endpoint retains every SQL family it can route:

- `SELECT`
- `DELETE`
- `EXPLAIN`
- `DESCRIBE`
- `SHOW INDEXES`
- `SHOW COLUMNS`
- `SHOW ENTITIES`

That means the dominant wasm question is no longer "how much does one more entity cost?" It is "how much duplicated code do all supported query families retain under the single `query(sql)` export?"

## Main Query Families

### 1. Generated Query Routing

Primary owner:

- `/home/adam/projects/icydb/crates/icydb-build/src/db.rs`

Relevant entrypoints:

- `sql_dispatch_query_surface_tokens`
- `sql_dispatch_query_api_tokens`
- `sql_dispatch_query_authority_tokens`
- `sql_dispatch_query_lane_tokens`
- `sql_dispatch_query_metadata_tokens`
- `sql_dispatch_query_explain_tokens`

This layer currently owns real route-family branching for the generated canister query surface:

- query/explain dispatch
- entity-authority lookup
- `DESCRIBE` / `SHOW *` metadata routing
- generated explain-surface error rewriting

This is the first major fixed-cost root because the generated endpoint keeps all of that code live under one export.

### 2. Core Session SQL Dispatch

Primary owner:

- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/dispatch/mod.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/dispatch/lowered.rs`

This layer repeats route-family and lane-family branching that the generated surface also does:

- computed projection lane checks
- dedicated aggregate-lane checks
- grouped/scalar lane checks
- explain routing
- metadata routing
- row-shaped select/delete execution packaging

This is the second major fixed-cost root because the generated surface is not calling one tiny canonical query executor. It is calling into another large route-aware executor.

### 3. Shared Session SQL Entry Surfaces

Primary owner:

- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/mod.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/aggregate.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/surface/lane.rs`

This layer adds another set of shared SQL boundary contracts:

- wrong-lane messages
- computed projection rejection
- aggregate-lane enforcement
- grouped/scalar enforcement

These checks are legitimate behavior, but they currently exist as another retained branch family under the generated query root.

### 4. Shared SQL Lowering Monolith

Primary owner:

- `/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering/mod.rs`

Current size signal:

- `1,476` source lines

This module owns all of the following under one file and one retained lowering authority:

- select preparation
- delete preparation
- explain lowering
- global aggregate lowering
- grouped projection lowering
- projection normalization
- order/having normalization

Even if the export surface stays the same, this is the biggest obvious structural place where fixed query-family logic is still bundled together.

### 5. Public SQL Result and Rendering Surface

Primary owner:

- `/home/adam/projects/icydb/crates/icydb/src/db/sql/mod.rs`

Current size signal:

- `574` source lines

This module is thinner than before, but the public result type is still one wide union:

- `Projection`
- `Explain`
- `Describe`
- `ShowIndexes`
- `ShowColumns`
- `ShowEntities`

And the same root retains all of the render helpers for those variants.

This is legitimate facade work, but it means the single public query export still pulls projection, explain, describe, and show rendering into one retained result family.

## What Is Not The Main Problem

### Per-Entity Growth

Per-entity growth is no longer the dominant wasm issue.

Recent cross-canister comparison showed roughly:

- about `1.8 KB` to `1.9 KB` shrunk wasm per extra entity

That is normal scaling. It is not what is driving the large `minimal` regression.

### Error Strings

Static error/message strings are present in wasm, but they are not the main bloat source.

The current signal is:

- `data[0]` is large, but its growth is much smaller than the retained `query` export growth
- the dominant issue is retained code under the query export, not text alone

So "replace strings with codes" is a cleanup direction, not a likely big-win wasm fix by itself.

## Big-Win Candidates Without Cutting Features

### Candidate 1. Collapse Generated Query Routing Into One Canonical Core

Highest-value files:

- `/home/adam/projects/icydb/crates/icydb-build/src/db.rs`
- `/home/adam/projects/icydb/crates/icydb/src/db/session/generated.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/dispatch/mod.rs`

Current problem:

- generated query routing exists in the build output
- then route-aware SQL dispatch exists again in core

Best target shape:

- build output emits only the descriptor table and the public shim
- one canonical core helper owns the generated query route family
- metadata/explain/query/delete authority routing happens in one place, not two

Why this is a real big-win candidate:

- it attacks duplicated route-family logic directly under the `query(sql)` export
- it does not remove supported statement families

### Candidate 2. Collapse The Dual Generated Query Lowering Paths

Highest-value files:

- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/surface/route.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/dispatch/lowered.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering/mod.rs`

Current problem:

- `lower_query_lane_for_entity(...)`
- `lower_generated_query_surface_for_entity(...)`
- `execute_lowered_sql_dispatch_query_for_authority(...)`

That is a strong smell of "same feature set, two retained structural paths."

Best target shape:

- one canonical lowered query envelope for generated query execution
- one canonical row-shaped result carrier for generated query select/delete
- generated surface specialization happens at the outer boundary only

Why this is a real big-win candidate:

- it attacks duplicated select/delete lowering and dispatch ownership
- it should help the fixed query export root without removing any lane

### Candidate 3. Reduce The Number Of Intermediate Row/Result Shapes

Highest-value files:

- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/projection/payload.rs`
- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/surface/route.rs`
- `/home/adam/projects/icydb/crates/icydb/src/db/sql/mod.rs`

Current problem:

The query root still carries multiple adjacent wrappers for roughly the same projection payload:

- `SqlProjectionPayload`
- `SqlDispatchResult::Projection`
- `SqlQueryRowsOutput`
- `SqlProjectionRows`

That is not necessarily large by itself, but it is one of the few places where the generated query root still obviously does repeated packaging for the same result family.

Best target shape:

- one canonical structural row payload from core to facade
- one thin public `SqlQueryResult` conversion at the final boundary

Why this is still worth auditing:

- it does not remove any public result variant
- it trims query-family packaging, not user-visible features

## Recommended Order

1. Collapse generated query routing and route-family ownership first.
2. Then collapse dual generated-query lowering/dispatch paths.
3. Only then look at row/result wrapper reduction.

This order matters because the first two are much more likely to reduce the retained `query(sql)` code root by tens of KB than string cleanup or helper reshuffling.

## Bottom Line

The current wasm problem is not "too many entities" and probably not "too many strings."

It is:

- one public `query(sql)` export
- retaining every query family
- through multiple stacked route-aware layers

So the next serious wasm work should target duplicated generated query routing and duplicated generated query lowering, not feature cuts and not string-only cleanup.

## Verification Readout

- `WASM_CANISTER_NAME=minimal WASM_PROFILE=wasm-release WASM_SQL_VARIANTS=sql-on bash scripts/ci/wasm-size-report.sh`
- `twiggy top -n 30 artifacts/wasm-size/minimal.wasm-release.dfx-shrunk.wasm`
- `twiggy dominators artifacts/wasm-size/minimal.wasm-release.dfx-shrunk.wasm`
