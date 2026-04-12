# Canister Surface Split

## Purpose

This note tracks the current split between the demo SQL surface, the lightweight
SQL smoke-test surface, and the grouped wasm-audit fixtures.

## Current Ownership

`canisters/demo/rpg` currently owns the Character-only demo role:

- public demo query/update entrypoints

`canisters/test/sql` owns the lightweight smoke-test surface:

- generated actor surface stability
- basic fixture coverage

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
- `canisters/test/sql`
- `canisters/audit/*`
- `schema/demo/rpg`
- `schema/test/fixtures`
- `schema/audit/*`

The Cargo package names now follow the same category-owned shape as the
filesystem layout. The logical DFX canister names remain stable.
