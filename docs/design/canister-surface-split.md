# Canister Surface Split

## Purpose

This note tracks the current split between the broad SQL demo surface, the
broad RPG test surface, the lightweight SQL smoke-test surface, and the grouped
wasm-audit fixtures.

## Current Ownership

`canisters/demo/rpg` currently owns the broad demo and perf-harness role:

- public demo query/update entrypoints
- perf and attribution harness entrypoints

`canisters/test/sql_parity` owns the broad SQL parity and explain-test role:

- generated-vs-typed parity across `User`, `Character`, and `ActiveUser`
- explain-route and witness-mode locks
- direct `LOWER(...)` / `UPPER(...)` route parity
- broad expected-row snapshots

`canisters/test/sql` owns the lightweight smoke-test surface:

- generated actor surface stability
- `SHOW ENTITIES` parity
- one deterministic projection parity query
- one deterministic `EXPLAIN EXECUTION` parity query

`canisters/audit/*` and `schema/audit/*` own the wasm-size and footprint audit
fixtures:

- `minimal`
- `one_simple`
- `one_complex`
- `ten_simple`
- `ten_complex`

## Stability Rule

The filesystem layout is now category-owned:

- `canisters/demo/rpg`
- `canisters/test/sql_parity`
- `canisters/test/sql`
- `canisters/audit/*`
- `schema/demo/rpg`
- `schema/test/sql_parity`
- `schema/test/fixtures`
- `schema/audit/*`

The Cargo package names now follow the same category-owned shape as the
filesystem layout. The logical DFX canister names remain stable.
