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
- global aggregate loads with one or more aggregate projection terminals and no
  `GROUP BY`
- grouped aggregate loads
- narrow computed projection loads, including admitted bounded arithmetic,
  numeric scalar functions, text-function projection forms, and searched `CASE`

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
- admitted narrow `... RETURNING`

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
- `DELETE FROM Customer c WHERE c.age < 20`
- `UPDATE Customer AS c SET age = 22 WHERE c.name = 'Ada'`
- `INSERT INTO Customer c (name, age) VALUES ('Ada', 22)`

No statement may introduce more than one entity binding.

## Projection

Supported scalar projection forms are:

- `SELECT *`
- `SELECT field, ...`
- `SELECT DISTINCT *`
- `SELECT DISTINCT field, ...`
- admitted bounded scalar expression projections, including searched `CASE`

Supported aggregate projection forms are:

- one or more aggregate terminals with no `GROUP BY`
- grouped projection where grouped key items come first and aggregate or
  post-aggregate computed items come after them

Supported grouped projection examples:

- `SELECT age, COUNT(*) FROM Customer GROUP BY age`
- `SELECT name, COUNT(*), SUM(age) FROM Customer GROUP BY name`
- `SELECT age, ROUND(AVG(age), 2) FROM Customer GROUP BY age`
- `SELECT age, AVG(age + 1) + AVG(age + 1) FROM Customer GROUP BY age`
- `SELECT age, CASE WHEN COUNT(*) > 1 THEN 'multi' ELSE 'single' END FROM Customer GROUP BY age`

Unsupported grouped projection examples:

- grouped aggregates without grouped keys in the projection
- grouped keys appearing after aggregate outputs
- grouped projection terms that reference non-group fields outside the admitted
  grouped key and aggregate output authority
- bounded text functions inside grouped projection

## Shared SQL Expression Family

The admitted SQL expression family is shared across projection, aggregate
inputs, grouped/global `HAVING`, and `WHERE`.

Supported numeric scalar functions include:

- unary numeric functions: `ABS`, `CBRT`, `CEIL` / `CEILING`, `EXP`, `FLOOR`,
  `LN`, `LOG2`, `LOG10`, `SIGN`, `SQRT`
- binary numeric functions: `LOG(base, x)`, `MOD(x, y)`, `POWER(x, y)` /
  `POW(x, y)`
- scale-taking numeric functions: `ROUND(x, scale)`, `TRUNC(x, scale)` /
  `TRUNCATE(x, scale)`

The current conditional form is intentionally narrow:

- searched `CASE WHEN ... THEN ... [ELSE ...] END`

Supported searched `CASE` contexts are:

- scalar `SELECT` projections
- aggregate input expressions such as `SUM(CASE WHEN ... THEN ... ELSE ... END)`
- grouped/global aggregate `HAVING`
- `WHERE`, when the selected branch collapses onto the admitted boolean filter
  surface

Within those contexts, searched `CASE` conditions admit the same bounded
boolean/comparison expression lane used by that clause, including the admitted
postfix predicate family such as:

- `IS NULL` / `IS NOT NULL`
- `IS TRUE` / `IS FALSE` / `IS NOT TRUE` / `IS NOT FALSE`
- `LIKE` / `NOT LIKE` / `ILIKE` / `NOT ILIKE`
- `IN (...)`
- `BETWEEN ... AND ...` / `NOT BETWEEN ... AND ...`

Still intentionally excluded:

- simple `CASE value WHEN ...`
- subqueries or window expressions inside `CASE`
- `CASE` as a loophole for unsupported expression families in that clause

## Projection Aliases

Projection aliases are supported in `SELECT` lists.

Both forms are admitted:

- `SELECT name AS display_name FROM Customer`
- `SELECT COUNT(*) total FROM Customer GROUP BY name`

Aliases may label:

- scalar field projections
- aggregate projections
- admitted scalar computed projections
- admitted grouped post-aggregate computed projections

`ORDER BY <alias>` is supported only when the alias resolves to an already
supported order target:

- a plain field
- `LOWER(field)`
- `UPPER(field)`
- admitted bounded scalar computed order targets such as field-plus-literal,
  field-plus-field, and `ROUND(...)`
- admitted grouped aggregate order targets, including bounded grouped Top-K
  alias forms such as `ORDER BY avg_age DESC`

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
  - `<>`
- field-to-field comparisons on the same comparison family
- `IN (...)`
- `NOT IN (...)`
- `BETWEEN ... AND ...`
- `NOT BETWEEN ... AND ...`
- `IS NULL`
- `IS NOT NULL`
- `IS TRUE`
- `IS FALSE`
- `IS NOT TRUE`
- `IS NOT FALSE`
- prefix `LIKE 'prefix%'`
- prefix `NOT LIKE 'prefix%'`
- prefix `ILIKE 'prefix%'`
- prefix `NOT ILIKE 'prefix%'`
- `STARTS_WITH(field, 'prefix')`
- searched `CASE`, when it returns values that stay on the admitted boolean
  filtering lane

Narrow casefolded predicate forms are also supported:

- `LOWER(field) LIKE 'prefix%'`
- `UPPER(field) LIKE 'PREFIX%'`
- `STARTS_WITH(LOWER(field), 'prefix')`
- `STARTS_WITH(UPPER(field), 'PREFIX')`

Field-bound range predicates are also supported on the plain-field lane:

- `field BETWEEN lower_field AND upper_field`
- `field NOT BETWEEN lower_field AND upper_field`

Still intentionally excluded from the admitted predicate lane:

- grouped field-to-field predicates
- non-prefix `LIKE` / `NOT LIKE` / `ILIKE` / `NOT ILIKE`
- wrapped `STARTS_WITH(...)` first arguments beyond plain or `LOWER/UPPER`
  field wrappers
- grouped `HAVING` variants that reuse the plain-field boolean special forms
  or text-pattern lane directly

## `HAVING`

Supported `HAVING` forms are:

- grouped aggregate `HAVING` over grouped keys and aggregate outputs
- global aggregate `HAVING` over the implicit single aggregate group
- admitted post-aggregate scalar expressions, including bounded arithmetic,
  wrappers, and searched `CASE`

Still intentionally excluded:

- raw-row-only expressions that escape post-aggregate authority
- grouped `HAVING` reuse of the plain-field text-pattern or boolean-special
  predicate lane

## Public SQL Write `RETURNING`

Supported `RETURNING` forms are intentionally narrow:

- `RETURNING *`
- `RETURNING field, ...`

`RETURNING` is admitted on the public SQL write lane for:

- `INSERT ... RETURNING`
- `UPDATE ... RETURNING`
- `DELETE ... RETURNING`

Unsupported `RETURNING` projection forms remain fail-closed:

- computed expressions in `RETURNING`
- aggregate expressions in `RETURNING`
- other widened projection families beyond `*` or plain field lists
