# IcyDB SQL Subset Contract

This document defines the current supported public IcyDB SQL boundary.
Anything not stated here is outside the supported SQL surface and must fail
closed.

This contract is about the public SQL frontend that remains after the old
public SQL router removal.

## Scope

- Applies to IcyDB SQL parsing, lowering, validation, and execution semantics.
- Applies only to single-entity statements.
- Defines the admitted public SQL shapes, not internal parser route metadata.
- Does not define storage internals, planner heuristics, or canister ABI shape.

## Core Rule

Every admitted executable SQL statement targets exactly one entity.

IcyDB SQL is a constrained single-entity language for:

- filtering
- ordering
- pagination
- scalar projection
- grouped queries and aggregates
- narrow built-in expression forms
- explain and schema/introspection commands

IcyDB SQL is not a general-purpose relational SQL engine.

Typed and fluent APIs are the canonical public surfaces.
The remaining public SQL surfaces are:

- `execute_sql_query::<E>(...)`
- `execute_sql_update::<E>(...)`

Both stay hard-bound to one concrete entity type and return SQL-shaped output.

## Cursor Pagination

Cursor-based pagination is not part of the scalar SQL surface.

- SQL uses `LIMIT` / `OFFSET` for scalar windowing.
- Cursor pagination is available through typed and fluent APIs.
- This is intentional: cursor semantics are transport-level, not query
  semantics.

Grouped SQL is the explicit exception.
Grouped SQL result payloads may carry `next_cursor` because grouped execution
already returns structured continuation-aware results as part of its admitted
surface.

## Operational vs Semantic Features

The SQL surface defines query semantics only.

SQL covers:

- filtering
- ordering
- projection
- grouping
- aggregation
- mutation

The following are intentionally not part of SQL:

- cursor-based pagination
- continuation tokens
- streaming controls
- byte-metric diagnostics such as `bytes()` and `bytes_by(...)`

These are available only through typed and fluent APIs.

SQL guarantees semantic equivalence for admitted query and mutation shapes, but
not transport-level or diagnostic behavior.

## Supported Public SQL Statements

### `SELECT`

Supported `SELECT` families are:

- scalar row loads
- scalar `DISTINCT` loads
- global aggregate loads with exactly one aggregate projection terminal and no
  `GROUP BY`
- grouped aggregate loads
- narrow computed projection loads

### `EXPLAIN`

Supported shapes:

- `EXPLAIN SELECT ...`
- `EXPLAIN DELETE ...`
- `EXPLAIN EXECUTION SELECT ...`
- `EXPLAIN EXECUTION DELETE ...`
- `EXPLAIN JSON SELECT ...`
- `EXPLAIN JSON DELETE ...`

`EXPLAIN` is an operational SQL surface.

### Introspection

Supported commands:

- `DESCRIBE entity`
- `SHOW INDEXES entity`
- `SHOW COLUMNS entity`
- `SHOW ENTITIES`
- `SHOW TABLES`

`SHOW TABLES` is not a separate metadata family.
It is an alias for `SHOW ENTITIES` and should return the same payload.

## Public SQL Mutation Execution

Supported public mutation shapes are:

- `INSERT`
- `UPDATE`
- `DELETE`
- admitted `... RETURNING`

Mutation ownership still primarily lives on typed and fluent APIs:

- `create(...)`
- `insert(...)`
- `update(...)`
- `replace(...)`
- `delete::<E>()`
- the corresponding typed/fluent `...returning...` helpers

Public SQL ownership is split deliberately:

- `execute_sql_query::<E>(...)` owns read, explain, and introspection SQL
- `execute_sql_update::<E>(...)` owns state-changing SQL

## Entity Naming And Aliases

The admitted single-entity naming surface is:

- unqualified entity names
- schema-qualified entity names such as `public.Customer`
- one optional single-table alias, with or without `AS`

Examples:

- `SELECT * FROM Customer c`
- `SELECT c.name FROM Customer AS c`

No statement may introduce more than one entity binding.

## Projection

Supported scalar projection forms are:

- `SELECT *`
- `SELECT field, ...`
- `SELECT DISTINCT *`
- `SELECT DISTINCT field, ...`

Supported aggregate projection forms are:

- exactly one global aggregate terminal with no `GROUP BY`
- grouped projection where grouped key items come first and aggregate items come
  after them

Supported grouped projection examples:

- `SELECT age, COUNT(*) FROM Customer GROUP BY age`
- `SELECT name, COUNT(*), SUM(age) FROM Customer GROUP BY name`

Unsupported grouped projection examples:

- grouped aggregates without grouped keys in the projection
- grouped keys appearing after aggregate outputs
- arbitrary expression widening in grouped projection
- bounded text functions inside grouped projection

## Projection Aliases

Projection aliases are supported in `SELECT` lists.

Both forms are admitted:

- `SELECT name AS display_name FROM Customer`
- `SELECT COUNT(*) total FROM Customer GROUP BY name`

Aliases may label:

- scalar field projections
- aggregate projections
- admitted scalar computed text projections

`ORDER BY <alias>` is supported only when the alias resolves to an already
supported order target:

- a plain field
- `LOWER(field)`
- `UPPER(field)`

Aliases do not widen the order-expression surface.

## Predicates

Supported `WHERE` predicate forms are:

- `AND`, `OR`, `NOT`
- parenthesized predicate trees
- comparison operators
  - `=`
  - `!=`
  - `<`
  - `<=`
  - `>`
  - `>=`
- `IN (...)`
- `NOT IN (...)`
- `BETWEEN ... AND ...`
- `IS NULL`
- `IS NOT NULL`
- prefix `LIKE 'prefix%'`
- `STARTS_WITH(field, 'prefix')`

Narrow casefolded predicate forms are also supported:

- `LOWER(field) LIKE 'prefix%'`
- `UPPER(field) LIKE 'PREFIX%'`
