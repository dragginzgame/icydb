# SQL Surface Mapping

This note explains how the admitted IcyDB SQL language is exposed through the
current public surfaces.

`docs/contracts/SQL_SUBSET.md` is the normative language contract.
This file is intentionally implementation-facing.

## Why This File Exists

IcyDB SQL is one language subset, but it is currently exposed through multiple
entrypoints with different result-shape and lane constraints.

The main contract should answer:

- "Is this SQL shape part of the supported IcyDB SQL subset?"

This file answers:

- "Which entrypoints expose that shape today?"
- "Where does SQL already converge with typed/fluent behavior?"
- "Where is the surface still split?"

## Surface Matrix

Legend:

- `yes` means the surface exposes that statement family for the admitted
  contract shape.
- `partial` means the surface exposes that family, but through a narrower lane,
  narrower payload contract, or narrower subset than the main language contract
  suggests at first glance.
- `no` means the surface does not expose that family.

| surface | scalar `SELECT` | grouped `SELECT` | global aggregate `SELECT` | computed projection `SELECT` | `DELETE` | `INSERT` | `UPDATE` | `EXPLAIN` | `DESCRIBE` / `SHOW` |
|---|---|---|---|---|---|---|---|---|---|
| `query_from_sql` | yes | yes | no | no | yes | no | no | no | no |
| `execute_sql` | yes | no | no | no | yes | no | no | no | no |
| `execute_sql_grouped` | no | yes | no | partial | no | no | no | no | no |
| `execute_sql_aggregate` | no | no | yes | no | no | no | no | no | no |
| `execute_sql_dispatch` | yes | partial | partial | partial | yes | yes | yes | yes | yes |
| generated canister SQL query surface | yes | partial | partial | partial | yes | no | no | yes | yes |

Surface-specific constraints and payload differences are described below.

## What Is Already Stable

The strongest SQL-to-typed/fluent convergence exists for the shared query lane:

- single-entity filtering
- canonical predicate lowering
- ordering
- scalar pagination
- grouped key and aggregate lowering
- grouped `HAVING`

Representative evidence:

- `crates/icydb-core/src/db/session/tests/query_lowering.rs`
- `crates/icydb-core/src/db/sql/lowering/tests/mod.rs`

This is the part of the SQL surface that already behaves like one canonical
query/runtime model with multiple frontends.

## Stable Surface Utilities

- explain routes
- metadata/introspection routes

## Where The Surface Is Still Split

### Scalar Projection Result Shape

Scalar field-list SQL projection is language-level support, but not all
surfaces expose it the same way.

- `query_from_sql` lowers it into canonical projection intent.
- `execute_sql` still returns `EntityResponse<E>` rows rather than
  projection-shaped rows.
- dispatch-oriented SQL surfaces return projection-shaped rows and labels.

Representative evidence:

- `crates/icydb-core/src/db/session/tests/sql_surface.rs`
- `crates/icydb-core/src/db/session/tests/sql_projection.rs`
- `crates/icydb-core/src/db/query/intent/query.rs`

### Computed Text Projection

Computed text projection is shipped, but it is session-owned rather than part
of the shared canonical query lane.

- `query_from_sql` rejects it
- `execute_sql` rejects it
- `execute_sql_grouped` admits grouped computed projection only
- dispatch surfaces admit scalar and grouped computed projection

Representative evidence:

- `crates/icydb-core/src/db/session/sql/computed_projection/plan.rs`
- `crates/icydb-core/src/db/session/tests/sql_surface.rs`
- `crates/icydb-core/src/db/session/tests/sql_projection.rs`
- `crates/icydb-core/src/db/session/tests/sql_grouped.rs`

### Global Aggregate `SELECT`

Global aggregate SQL is admitted by the language contract, but execution still
uses a dedicated aggregate lane.

This is a public-surface split, not a language-boundary disagreement.

- `query_from_sql` rejects it
- `execute_sql` rejects it
- `execute_sql_grouped` rejects it
- `execute_sql_aggregate` owns it directly
- dispatch surfaces fold it back into a normal SQL result payload

Representative evidence:

- `crates/icydb-core/src/db/session/sql/mod.rs`
- `crates/icydb-core/src/db/sql/lowering/aggregate.rs`
- `crates/icydb-core/src/db/session/tests/sql_aggregate.rs`

### Writes

`INSERT` and `UPDATE` are admitted SQL statement families in the language
contract, but their current public exposure is dispatch-owned rather than
shared across all SQL surfaces.

- typed `execute_sql_dispatch` supports them
- generated canister SQL query surface does not
- they do not lower through the shared typed query lane

Representative evidence:

- `crates/icydb-core/src/db/session/sql/dispatch/mod.rs`
- `crates/icydb-core/src/db/session/tests/sql_write.rs`

## Alias Mapping Notes

The shipped alias surface is:

- one single-table alias in `SELECT`, `DELETE`, and `UPDATE`
- parser-admitted table alias forms in `INSERT`
- projection aliases with `AS`
- projection aliases with bare identifier form
- grouped and aggregate aliases for output labels
- `ORDER BY <alias>` only when the alias resolves to:
  - a plain field
  - `LOWER(field)`
  - `UPPER(field)`

Alias support is parser/session-owned normalization.
It does not imply general expression alias semantics.

Representative evidence:

- `crates/icydb-core/src/db/sql/parser/projection.rs`
- `crates/icydb-core/src/db/sql/parser/statement.rs`
- `crates/icydb-core/src/db/sql/lowering/normalize.rs`
- `crates/icydb-core/src/db/session/tests/sql_projection.rs`
- `crates/icydb-core/src/db/sql/parser/tests/mod.rs`

## Generated Canister SQL Boundary

The generated canister SQL query surface is intentionally query-focused.

Today it exposes:

- `SELECT`
- `DELETE`
- `EXPLAIN`
- `DESCRIBE`
- `SHOW INDEXES`
- `SHOW COLUMNS`
- `SHOW ENTITIES`
- dispatch-oriented projection/grouped/aggregate payloads

Today it rejects:

- `INSERT`
- `UPDATE`

Representative evidence:

- `crates/icydb-core/src/db/session/sql/dispatch/mod.rs`
- `canisters/test/sql/src/lib.rs`
- `canisters/test/sql_parity/src/tests.rs`
- `testing/pocket-ic/tests/sql_canister.rs`

## Convergence Summary

### Already Good Enough To Treat As Stable

- filtering
- ordering
- scalar pagination
- grouped key plus aggregate query semantics
- grouped `HAVING`

### Stable Surface Utilities

- explain routes
- metadata/introspection routes

### Not Yet Fully Converged

- scalar projection materialization
- computed text projection ownership
- global aggregate entrypoint split
- write-lane availability across public SQL surfaces

## Product Decisions Still Visible In The Code

The code is already clear on these current behaviors:

- `INSERT` is a supported SQL statement family
- `UPDATE` is a supported SQL statement family
- generated canister SQL remains query-only for writes
- aliases are part of the supported SQL subset

The remaining product choice is not whether these things exist.
The real choice is whether future public SQL surfaces should converge on the
same availability and result-shape contracts.
