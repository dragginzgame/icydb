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

Test-only helper names in `icydb-core` may still mention older lane-shaped SQL
surfaces. Those helpers exist only to keep legacy matrix coverage stable; they
are not part of the live public SQL API.

## Default Parity Rule

If SQL admits a capability and IcyDB already has one equivalent canonical
typed or fluent model for that capability, the default expectation is that the
typed or fluent surface should expose it too.

For `0.77`, this is not just a directional preference.
It is the completion rule for the ordinary single-entity query surface:

* admitted ordinary SQL query capability should have one canonical fluent or
  typed representation
* admitted single-entity SQL mutation capability should have one canonical
  typed or fluent mutation representation

The inverse is still not automatic.
Typed/fluent capability does not imply that SQL text must expose the same
operation.

Operational SQL surfaces are the explicit exception:

* `DESCRIBE`
* `SHOW ...`
* `EXPLAIN ...`

Operational retrieval differences are also explicit exceptions:

* typed cursor pagination
* byte-metric diagnostics such as `bytes()` and `bytes_by(...)`

Those may remain SQL-shaped without matching fluent builder forms.

## Surface Matrix

Legend:

- `yes` means the surface exposes that statement family for the admitted
  contract shape.
- `partial` means the surface exposes that family, but through a narrower lane
  or a narrower helper contract.
- `no` means the surface does not expose that family.

| surface | scalar `SELECT` | grouped `SELECT` | global aggregate `SELECT` | computed projection `SELECT` | `DELETE` | `INSERT` | `UPDATE` | `EXPLAIN` | `DESCRIBE` / `SHOW` |
|---|---|---|---|---|---|---|---|---|---|
| `execute_sql_query::<E>` | yes | yes | yes | yes | no | no | no | yes | yes |
| `execute_sql_update::<E>` | no | no | no | no | yes | yes | yes | no | no |
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

The strongest public SQL execution split is now:

- `execute_sql_query::<E>(...)` for read, explain, and introspection SQL
- `execute_sql_update::<E>(...)` for state-changing SQL

Both stay single-entity and SQL-shaped, but neither one widens into the
other's statement family.

The strongest row-returning convergence exists on typed/fluent mutation APIs:

- typed `create_returning...`, `insert_returning...`, and `update_returning...`
- fluent `delete::<E>().returning...`

These surfaces share one public row/projection payload family.

The main fluent helper terminals are also no longer treated as a separate SQL
parity gap. They map onto admitted SQL query terms instead of requiring one
second SQL helper vocabulary:

- `exists()` / `not_exists()` -> `SELECT COUNT(*) ...`
- `min()` / `max()` -> ordered `SELECT id ... LIMIT 1`
- `min_by(field)` / `max_by(field)` / `nth_by(field, n)` ->
  ordered `SELECT id ... ORDER BY field, id`
- `sum_by(field)` / `avg_by(field)` / `count_distinct_by(field)` ->
  ordinary global aggregate SQL

Representative evidence:

- `crates/icydb-core/src/db/session/tests/sql_aggregate.rs`

Cursor pagination is different.
It is not another SQL helper spelling for the same admitted query semantics.
It is an operational retrieval contract:

- SQL owns filtering, ordering, projection, grouping, aggregation, and
  mutation semantics
- typed/fluent APIs own scalar continuation tokens, cursor traversal, and
  byte-metric diagnostics

## Where The Surface Is Still Split

### Computed Text Projection

Computed text projection is shipped and now lowers through one canonical
`Expr::FunctionCall` path with executor-owned evaluation.

It also has one canonical fluent representation through the shared
`TextProjectionExpr` builder plus fluent projection terminals such as:

- `project_values(...)`
- `project_first_value(...)`
- `project_last_value(...)`

What is still true is that this remains a narrower projection-terminal family
rather than one broad row-returning `execute()` projection model.
Grouped computed text projection is still intentionally rejected in the
current grouped SQL slice.

Representative evidence:

- `crates/icydb-core/src/db/query/builder/text_projection.rs`
- `crates/icydb-core/src/db/sql/lowering/select.rs`
- `crates/icydb-core/src/db/executor/projection/eval/text_function.rs`
- `crates/icydb-core/src/db/query/fluent/load/terminals.rs`
- `crates/icydb-core/src/db/session/tests/sql_surface.rs`
- `crates/icydb-core/src/db/session/tests/sql_projection.rs`

### Global Aggregate `SELECT`

Global aggregate SQL is admitted by the language contract.
The completion goal for `0.77` is that this admitted ordinary query shape is
described and tested as one canonical query capability rather than as a
special-case SQL success.

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

The SQL mutation mirror is now explicit rather than hidden behind a query-shaped
entrypoint:

- `execute_sql_update::<E>(...)`

That means typed write helpers remain an ergonomic owner, not a missing SQL
mutation capability.

## Introspection Boundary

SQL parsing still owns route metadata for:

- `EXPLAIN`
- `DESCRIBE`
- `SHOW INDEXES`
- `SHOW COLUMNS`
- `SHOW ENTITIES`
- `SHOW TABLES` as an alias of `SHOW ENTITIES`

But the public operational helpers remain typed/session-owned:

- `describe_entity(...)`
- `show_indexes(...)`
- `show_columns(...)`
- `show_entities()`
- `show_tables()`

## Cursor Pagination Boundary

Cursor-based pagination is not part of the scalar SQL language contract.

- scalar SQL uses `LIMIT` / `OFFSET`
- typed/fluent APIs expose cursor continuation
- this split is intentional because cursor behavior is transport-level rather
  than query-level

Grouped SQL remains the one explicit exception because grouped result payloads
already return structured `next_cursor` metadata as part of the admitted SQL
result family.

## Diagnostic Boundary

Byte-metric terminals are not part of the SQL language contract.

- `bytes()`
- `bytes_by(...)`

These are typed/fluent diagnostic helpers rather than ordinary SQL query
semantics.

## Result Rule

The public rule is:

- row-producing operations use the shared row/projection payload family
- non-returning writes use `MutationResult`

That rule is owned by typed/fluent public APIs rather than by a separate SQL
result envelope.

## 0.77 Freeze Bar

The admitted single-entity SQL surface is not considered complete for `0.77`
until all of the following are true:

- every admitted ordinary SQL query family is represented both in SQL and in
  one canonical fluent or typed query form
- every admitted SQL mutation family is represented both in SQL and in one
  canonical typed or fluent mutation form
- the live public SQL surface stays frozen to:
  - `execute_sql_query::<E>(...)`
  - `execute_sql_update::<E>(...)`
- every admitted family has direct tests on the live surface rather than only
  transitive proof through older internal helpers
