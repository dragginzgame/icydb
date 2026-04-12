# SQL Surface Mapping

This note explains how the admitted IcyDB SQL frontend maps onto the current
public APIs.

`docs/contracts/SQL_SUBSET.md` is the normative contract.
This file is intentionally implementation-facing.

## Why This File Exists

IcyDB still supports reduced SQL parsing and execution, but it no longer keeps
route-parsing/lane-sharded SQL helpers as a separate public product surface.

The main contract should answer:

- "Is this SQL shape part of the supported IcyDB SQL subset?"

This file answers:

- "Which public entrypoints expose that shape today?"
- "Where does SQL already converge with typed/fluent behavior?"
- "Where is SQL intentionally narrower than typed/fluent APIs?"

## Default Parity Rule

If SQL admits a capability and IcyDB already has one equivalent canonical
typed or fluent model for that capability, the default expectation is that the
typed or fluent surface should expose it too.

The inverse is no longer automatic.
Typed/fluent capability does not imply that SQL text must expose the same
operation.

## Surface Matrix

Legend:

- `yes` means the surface exposes that statement family for the admitted
  contract shape.
- `partial` means the surface exposes that family, but through a narrower lane
  or a narrower helper contract.
- `no` means the surface does not expose that family.

| surface | scalar `SELECT` | grouped `SELECT` | global aggregate `SELECT` | computed projection `SELECT` | `DELETE` | `INSERT` | `UPDATE` | `EXPLAIN` | `DESCRIBE` / `SHOW` |
|---|---|---|---|---|---|---|---|---|---|
| `execute_sql_query::<E>` | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| typed/fluent writes | no | no | no | no | yes | yes | yes | no | no |

## What Is Already Stable

The strongest SQL-to-typed convergence exists for the shared query lane:

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

The strongest public SQL execution surface is now:

- `execute_sql_query::<E>(...)`

It stays single-entity and SQL-shaped, but it executes every currently
admitted single-entity SQL statement family through one outward entrypoint.

The strongest row-returning convergence exists on typed/fluent mutation APIs:

- typed `create_returning...`, `insert_returning...`, and `update_returning...`
- fluent `delete::<E>().returning...`

These surfaces share one public row/projection payload family.

## Where The Surface Is Still Split

### Computed Text Projection

Computed text projection is shipped, but it is session-owned rather than part
of the shared canonical query lane.

Representative evidence:

- `crates/icydb-core/src/db/session/sql/computed_projection/plan.rs`
- `crates/icydb-core/src/db/session/tests/sql_surface.rs`
- `crates/icydb-core/src/db/session/tests/sql_projection.rs`

### Global Aggregate `SELECT`

Global aggregate SQL is admitted by the language contract.
Canonical lowering still rejects it, but the public SQL executor runs it.

Representative evidence:

- `crates/icydb-core/src/db/session/sql/mod.rs`
- `crates/icydb-core/src/db/sql/lowering/aggregate.rs`

### Mutation Ownership

The canonical public write owners are still:

- typed `create(...)`, `insert(...)`, `update(...)`, and `replace(...)`
- typed `*_returning...` helpers for row-returning mutation outcomes
- fluent `delete::<E>()` and `delete::<E>().returning...`

Representative evidence:

- `crates/icydb/src/db/session/mod.rs`
- `crates/icydb/src/db/session/delete.rs`
- `crates/icydb-core/src/db/session/sql/mod.rs`

## Introspection Boundary

SQL parsing still owns route metadata for:

- `EXPLAIN`
- `DESCRIBE`
- `SHOW INDEXES`
- `SHOW COLUMNS`
- `SHOW ENTITIES`
- `SHOW TABLES`

But the public operational helpers remain typed/session-owned:

- `describe_entity(...)`
- `show_indexes(...)`
- `show_columns(...)`
- `show_entities()`

## Result Rule

The public rule is:

- row-producing operations use the shared row/projection payload family
- non-returning writes use `MutationResult`

That rule is owned by typed/fluent public APIs rather than by a separate SQL
result envelope.
