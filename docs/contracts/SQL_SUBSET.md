# IcyDB SQL Subset Contract

This document defines the supported IcyDB SQL language boundary.
It is a product contract for the admitted single-entity SQL subset.
Anything not stated here is outside the supported SQL surface and must fail
closed.

This document defines the admitted SQL language, not per-entrypoint
availability. A statement family may be part of the supported SQL subset even
when some public surfaces intentionally do not expose it. Public-surface
availability and lane mapping live in
`docs/architecture/sql-surface-mapping.md`.

## Scope

- Applies to IcyDB SQL parsing, lowering, validation, and execution semantics.
- Applies only to single-entity statements.
- Defines the admitted SQL shapes, not the internal execution lanes.
- Does not define storage internals, planner heuristics, or canister ABI shape.

## Core Rule

Every admitted SQL statement targets exactly one entity.

IcyDB SQL is a constrained single-entity language for:

- CRUD over one entity
- filtering
- ordering
- pagination
- scalar projection
- grouped queries and aggregates
- narrow built-in expression forms
- explain and schema/introspection commands

IcyDB SQL is not a general-purpose relational SQL engine.

`INSERT` and `UPDATE` are first-class parts of this SQL subset contract.
They are not provisional or documentation-only statement families.

## Supported Statements

### `SELECT`

Supported `SELECT` families are:

- scalar row loads
- scalar `DISTINCT` loads
- global aggregate loads with exactly one aggregate projection terminal and no
  `GROUP BY`
- grouped aggregate loads
- narrow computed projection loads

### `INSERT`

Supported shape:

```sql
INSERT INTO entity [(column, ...)] VALUES (...), (...), ...
```

Contract rules:

- exactly one target entity
- one or more `VALUES` tuples
- explicit primary-key value is required
- omitted column list uses canonical entity field order
- `INSERT ... SELECT` is not supported

### `UPDATE`

Supported shape:

```sql
UPDATE entity [alias]
SET field = literal, ...
WHERE predicate
[ORDER BY ...]
[LIMIT n]
[OFFSET n]
```

Contract rules:

- `WHERE` is required
- primary-key mutation is not supported
- ordered/windowed update selection is supported
- if `ORDER BY` is omitted and a bounded update window is requested, the matched
  set is resolved in primary-key order for determinism

### `DELETE`

Supported shape:

```sql
DELETE FROM entity [alias]
[WHERE predicate]
[ORDER BY ...]
[LIMIT n]
[OFFSET n]
```

`DELETE` is a single-entity delete with optional ordered windowing.

### `EXPLAIN`

Supported shapes:

- `EXPLAIN SELECT ...`
- `EXPLAIN DELETE ...`
- `EXPLAIN EXECUTION SELECT ...`
- `EXPLAIN EXECUTION DELETE ...`
- `EXPLAIN JSON SELECT ...`
- `EXPLAIN JSON DELETE ...`

`EXPLAIN` is not part of write execution and does not widen the admitted SQL
statement families.

### Introspection

Supported commands:

- `DESCRIBE entity`
- `SHOW INDEXES entity`
- `SHOW COLUMNS entity`
- `SHOW ENTITIES`

## Entity Naming And Aliases

The admitted single-entity naming surface is:

- unqualified entity names
- schema-qualified entity names such as `public.Customer`
- one optional single-table alias, with or without `AS`

Examples:

- `SELECT * FROM Customer c`
- `SELECT c.name FROM Customer AS c`
- `UPDATE Customer c SET c.age = 42 WHERE c.id = 1`

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

## Projection Aliases

Projection aliases are supported in `SELECT` lists.

Both forms are admitted:

- `SELECT name AS display_name FROM Customer`
- `SELECT COUNT(*) total FROM Customer GROUP BY name`

Aliases may label:

- scalar field projections
- aggregate projections
- admitted computed text projections

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
- `STARTS_WITH(LOWER(field), 'prefix')`
- `STARTS_WITH(UPPER(field), 'PREFIX')`
- ordered text bounds over `LOWER(field)` / `UPPER(field)`

Unsupported predicate forms include:

- `LIKE` patterns beyond trailing-prefix `%`
- generic SQL function predicates
- nested function predicates such as `STARTS_WITH(TRIM(name), 'A')`

## Ordering And Pagination

Supported scalar order targets are:

- plain fields
- `LOWER(field)`
- `UPPER(field)`
- admitted `ORDER BY <alias>` rewrites onto those same targets

Supported pagination clauses are:

- `LIMIT`
- `OFFSET`

Scalar `SELECT`, `DELETE`, `UPDATE`, and global aggregate queries may use
ordered window semantics.

## Grouped Queries

Grouped SQL is supported with these rules:

- `GROUP BY` requires at least one grouped key
- grouped projection must list grouped keys first
- grouped projection must include at least one aggregate output
- `HAVING` is supported only on grouped queries
- grouped `ORDER BY`, when present, must start with the grouped-key prefix
- grouped `ORDER BY` requires `LIMIT`

Top-level `SELECT DISTINCT` on grouped queries is admitted but does not widen
grouped semantics. Contractually, grouped `SELECT DISTINCT` is treated as the
same grouped query shape as the equivalent non-`DISTINCT` grouped statement.

## Aggregates

Supported aggregate functions are:

- `COUNT(*)`
- `COUNT(field)`
- `SUM(field)`
- `AVG(field)`
- `MIN(field)`
- `MAX(field)`

Global aggregate `DISTINCT` is supported for admitted field-target aggregate
forms.

Grouped aggregates support admitted field-target terminals, including shipped
grouped `DISTINCT` aggregate forms where grouped policy allows them.

## `HAVING`

`HAVING` is supported only for grouped queries.

Admitted `HAVING` forms are compare clauses over:

- grouped key fields
- declared aggregate outputs

Admitted operators are:

- comparison operators
- `IS NULL`
- `IS NOT NULL`

`HAVING` boolean composition is intentionally narrow:

- `AND` is supported
- `OR` and `NOT` are not supported

Grouped `HAVING` with grouped `DISTINCT` remains outside the supported subset.

## Narrow Computed Expression Surface

IcyDB SQL supports a narrow built-in text-function surface in projection
position:

- `TRIM(field)`
- `LTRIM(field)`
- `RTRIM(field)`
- `LOWER(field)`
- `UPPER(field)`
- `LENGTH(field)`
- `LEFT(field, n)`
- `RIGHT(field, n)`
- `STARTS_WITH(field, text)`
- `ENDS_WITH(field, text)`
- `CONTAINS(field, text)`
- `POSITION(text, field)`
- `REPLACE(field, from, to)`
- `SUBSTRING(field, start [, length])`

Grouped computed projection is limited to:

- grouped fields
- admitted text functions over grouped fields
- aggregate outputs after those grouped items

This expression surface is intentionally narrow and does not imply support for
general SQL expression trees.

## Rejected Features

The following are outside the contract and must fail closed:

- joins
- subselects
- common table expressions
- set operations such as `UNION`, `INTERSECT`, and `EXCEPT`
- window functions
- multi-statement SQL input
- quoted identifiers
- generic SQL function namespaces beyond the admitted aggregate and text forms
- multi-entity mutation or query semantics

## Lowering Contract

For admitted SQL shapes, IcyDB guarantees:

- single-entity normalization
- alias-neutral lowering
- schema-qualified and alias-qualified identifiers normalize to the same
  canonical entity-field semantics
- equivalent admitted SQL and equivalent typed/fluent query shapes are expected
  to converge on the same canonical predicate/order/group semantics

This contract does not guarantee that every public entrypoint exposes every
admitted SQL shape. The normative question answered here is whether a shape is
part of the supported SQL language. The separate surface-mapping note answers
which entrypoints expose that shape today.

## Error Contract

Outside the listed subset, IcyDB SQL must fail closed.

Unsupported SQL shapes must not:

- silently widen into broader semantics
- fall back to approximate behavior
- reinterpret multi-entity SQL as single-entity SQL

Errors may be raised at parse, lowering, validation, or execution boundaries,
but the statement must remain rejected.

## Stability Note

This document defines the intended stable single-entity SQL boundary.

If an admitted shape is removed, widened, or re-scoped, this document must be
updated in the same change.
