# SQL Subset Contract (Reduced Parser)

This document defines the SQL surface for the reduced parser line.

Anything not listed here is out of scope and must fail closed.

## Scope

- Applies to SQL text parsing and SQL-to-planner lowering.
- Applies to the current reduced SQL parser implementation line.
- Uses existing planner/executor capabilities as the authority.

## Core Rule

The reduced SQL frontend must lower into existing `Query`/planner/session paths
without semantic emulation layers.

## Normalization Rules

The reduced parser normalizes one statement deterministically before lowering.

- SQL keywords are case-insensitive.
- Insignificant whitespace is ignored.
- A trailing statement terminator (`;`) is optional.
- Multi-statement SQL input is rejected.

## Executable Baseline (Current 0.66 Line)

The current `0.66` line ships a projection-aware scalar SQL subset,
dispatch-owned computed text projection, constrained grouped/global aggregate
SQL execution, and dedicated DESCRIBE/SHOW INDEXES/SHOW COLUMNS/SHOW ENTITIES
(plus `SHOW TABLES` alias) introspection lanes. Broader SQL grammar support
remains staged behind lowering gates.

### SELECT

Executable shape:

```sql
SELECT *
FROM <entity>
[WHERE <predicate>]
[ORDER BY <order_list>]
[LIMIT <n>]
[OFFSET <n>]
```

```sql
SELECT <field_list>
FROM <entity>
[WHERE <predicate>]
[ORDER BY <order_list>]
[LIMIT <n>]
[OFFSET <n>]
```

```sql
SELECT DISTINCT *
FROM <entity>
[WHERE <predicate>]
[ORDER BY <order_list>]
[LIMIT <n>]
[OFFSET <n>]
```

```sql
SELECT DISTINCT <field_list_including_primary_key>
FROM <entity>
[WHERE <predicate>]
[ORDER BY <order_list>]
[LIMIT <n>]
[OFFSET <n>]
```

Notes:

- `SELECT *` is executable.
- Direct field-list projection lowering is executable.
- Dispatch-oriented SQL surfaces also execute one bounded computed text
  projection family:
  - `TRIM`
  - `LTRIM`
  - `RTRIM`
  - `LOWER`
  - `UPPER`
  - `LENGTH`
  - `LEFT`
  - `RIGHT`
  - `STARTS_WITH`
  - `ENDS_WITH`
  - `CONTAINS`
  - `POSITION`
  - `REPLACE`
  - `SUBSTRING`
- This computed projection family is currently available through
  `execute_sql_dispatch(...)`, `EXPLAIN` on the dispatch lane, and generated
  `sql_dispatch::query(...)`.
- `query_from_sql(...)` remains structural-only and rejects computed text
  projection.
- Entity-qualified field identifiers are executable and normalized to canonical
  planner field names (for example `Entity.field`, `schema.Entity.field`).
- Constrained scalar DISTINCT is executable:
  - `SELECT DISTINCT *`
  - `SELECT DISTINCT <field_list>` only when projection includes primary key
    field.
- `SELECT DISTINCT` shapes that do not project primary key remain fail-closed.
- `EXPLAIN` wrappers are supported for these constrained scalar DISTINCT forms.
- Field-list projection currently affects normalized intent/planning/fingerprints.
- `execute_sql(...)` returns entity-shaped `Response<E>` rows on the public
  facade.
- `execute_sql_dispatch(...)` returns projection-shaped
  `SqlQueryResult::Projection` payloads for row-producing SQL surfaces.
- Scalar pagination still follows existing planner validation
  (for example deterministic ordering requirements).
- Schema-qualified entity names are executable when the trailing entity segment
  matches the requested model (for example `public.Entity` for model `Entity`).

Grouped aggregate executable shape:

```sql
SELECT <group_field_list>, <aggregate_list>
FROM <entity>
[WHERE <predicate>]
GROUP BY <group_field_list>
[ORDER BY <order_list>]
[LIMIT <n>]
[OFFSET <n>]
```

Grouped SQL execution notes:

- Grouped SQL uses `execute_sql_grouped(...)`.
- Reduced grouped lowering currently requires projection/group-by alignment:
  group fields first, then aggregate terminals, with projected group fields
  matching `GROUP BY` fields.
- Grouped SQL also supports normalized qualified identifiers (`schema.Entity`,
  `Entity.field`) under the same grouped validation constraints.
- Non-grouped SQL execution APIs (`execute_sql`, `execute_sql_dispatch`) continue
  to reject global aggregate projection without `GROUP BY`.

Global aggregate executable shape:

```sql
SELECT <aggregate_terminal>
FROM <entity>
[WHERE <predicate>]
[ORDER BY <order_list>]
[LIMIT <n>]
[OFFSET <n>]
```

Global aggregate execution notes:

- Global aggregate SQL uses `execute_sql_aggregate(...)`.
- This entrypoint is intentionally constrained to one terminal projection item.
- Executable terminals in the current baseline:
  - `COUNT(*)`
  - `COUNT(<field>)`
  - `SUM(<field>)`
  - `AVG(<field>)`
  - `MIN(<field>)`
  - `MAX(<field>)`
- `COUNT(<field>)` counts non-null projected values in the effective query
  window.
- Unsupported terminal shapes (mixed projections, grouped forms) remain
  fail-closed.
- `EXPLAIN` wrappers are supported for this constrained global aggregate shape.

### DELETE

Executable shape:

```sql
DELETE FROM <entity>
[WHERE <predicate>]
[ORDER BY <order_list>]
[LIMIT <n>]
```

Execution constraints remain authoritative:

- `DELETE ... LIMIT ...` requires `ORDER BY`.
- `DELETE ... OFFSET ...` is out of scope.
- Grouped delete shapes are out of scope.

### EXPLAIN

Executable wrappers:

```sql
EXPLAIN <select_or_delete>
EXPLAIN EXECUTION <select_or_delete>
EXPLAIN JSON <select_or_delete>
```

`<select_or_delete>` must itself be executable in this baseline.
Global aggregate SQL terminals in the constrained
`execute_sql_aggregate(...)` surface are also explainable in this baseline.
Qualified identifiers (`schema.Entity`, `Entity.field`) are normalized before
planning, so equivalent qualified and unqualified statements map to identical
explainable intent.

### DESCRIBE

Executable shape:

```sql
DESCRIBE <entity>
```

Execution notes:

- DESCRIBE SQL is available through `execute_sql_dispatch::<E>(...)` and
  generated `sql_dispatch::query(...)`.
- DESCRIBE returns canonical typed schema payload `EntitySchemaDescription`.
- Direct non-SQL schema introspection remains available through
  `describe_entity::<E>()`.
- DESCRIBE entity matching follows the same trailing-segment rule used by
  other SQL surfaces (`public.Entity` matches model `Entity`).
- DESCRIBE is a dedicated introspection lane and does not lower into
  executable `Query<E>` planner paths.
- `EXPLAIN DESCRIBE ...` remains out of scope in this line.

### SHOW INDEXES

Executable shape:

```sql
SHOW INDEXES <entity>
```

Execution notes:

- `SHOW INDEXES` SQL is available through `execute_sql_dispatch::<E>(...)` and
  generated `sql_dispatch::query(...)`.
- `SHOW INDEXES` returns the canonical index-listing payload from
  `show_indexes::<E>()`.
- `SHOW INDEXES` entity matching follows the same trailing-segment rule used by
  other SQL surfaces (`public.Entity` matches model `Entity`).
- `SHOW INDEXES` is a dedicated introspection lane and does not lower into
  executable `Query<E>` planner paths.
- `EXPLAIN SHOW INDEXES ...` remains out of scope in this line.

### SHOW COLUMNS

Executable shape:

```sql
SHOW COLUMNS <entity>
```

Execution notes:

- `SHOW COLUMNS` SQL is available through `execute_sql_dispatch::<E>(...)` and
  generated `sql_dispatch::query(...)`.
- `SHOW COLUMNS` returns canonical field descriptors from
  `show_columns::<E>()`.
- `SHOW COLUMNS` entity matching follows the same trailing-segment rule used by
  other SQL surfaces (`public.Entity` matches model `Entity`).
- `SHOW COLUMNS` is a dedicated introspection lane and does not lower into
  executable `Query<E>` planner paths.
- `EXPLAIN SHOW COLUMNS ...` remains out of scope in this line.

### SHOW ENTITIES

Executable shapes:

```sql
SHOW ENTITIES
SHOW TABLES
```

Execution notes:

- `SHOW ENTITIES` / `SHOW TABLES` SQL is available through
  `execute_sql_dispatch::<E>(...)` and generated `sql_dispatch::query(...)`.
- `SHOW TABLES` is a bounded alias that lowers to the same dedicated
  show-entities lane and payload.
- `SHOW ENTITIES`/`SHOW TABLES` return canonical runtime entity names from
  `show_entities()`.
- This is a dedicated introspection lane and does not lower into executable
  `Query<E>` planner paths.
- `EXPLAIN SHOW ENTITIES ...` and `EXPLAIN SHOW TABLES ...` remain out of
  scope in this line.

## Generated `sql_dispatch` Boundary (Current 0.66 Line)

Generated canister SQL helpers in the current line use one unified query
surface.

- `sql_dispatch::query(...)` accepts the public generated SQL subset:
  - row-producing `SELECT`
  - row-producing `DELETE`
  - computed text projection on the query lane
  - `EXPLAIN SELECT`
  - `EXPLAIN DELETE`
  - `DESCRIBE`
  - `SHOW INDEXES`
  - `SHOW COLUMNS`
  - `SHOW ENTITIES`
  - `SHOW TABLES`
- `sql_dispatch::query(...)` returns one `SqlQueryResult` enum payload with:
  - `Projection(SqlQueryRowsOutput)`
  - `Explain { entity, explain }`
  - `Describe(EntitySchemaDescription)`
  - `ShowIndexes { entity, indexes }`
  - `ShowColumns { entity, columns }`
  - `ShowEntities { entities }`
- `SqlQueryResult` renders deterministic shell output via:
  - `SqlQueryResult::render_lines()`
  - `SqlQueryResult::render_text()`
- The generated helper keeps one narrower internal query/explain core, but the
  public generated canister facade also owns the metadata lanes listed above.

## Surface Split Notes (Current 0.66 Line)

The current `0.66` SQL line intentionally keeps a few public surface splits.

- `query_from_sql(...)` is the structural-only boundary:
  - accepts lowered `SELECT` / `DELETE` intent
  - can build grouped structural query intent
  - rejects computed text projection
  - rejects `EXPLAIN`
  - rejects `DESCRIBE` / `SHOW *`
- `execute_sql(...)` remains the entity-row execution boundary for reduced
  `SELECT` / `DELETE`.
- `execute_sql_dispatch(...)` is the main typed unified SQL payload surface:
  - executes row-producing `SELECT` / `DELETE`
  - executes computed text projection
  - renders `EXPLAIN`
  - handles `DESCRIBE` / `SHOW *`
- `execute_sql_grouped(...)` remains the grouped execution boundary.
- `execute_sql_aggregate(...)` remains the constrained global aggregate
  execution boundary.
- generated `sql_dispatch::query(...)` mirrors the dispatch-style public SQL
  surface for canister consumers, including computed projection and metadata
  lanes.

## Parsed but Lowering-Gated (Follow-Up Slices)

The reduced parser can parse additional reduced SQL constructs that remain
intentionally non-executable in the current baseline.

### SELECT

Parsed shape:

```sql
SELECT [DISTINCT] <projection>
FROM <entity>
[WHERE <predicate>]
[GROUP BY <field_list>]
[HAVING <grouped_having_clause_list>]
[ORDER BY <order_list>]
[LIMIT <n>]
[OFFSET <n>]
```

Parsed projection forms:

- `*`
- field list (`field_a, field_b, ...`)
- aggregate terminals already supported by planner/runtime:
  - `COUNT(*)`
  - `COUNT(<field>)`
  - `SUM(<field>)`
  - `AVG(<field>)`
  - `MIN(<field>)`
  - `MAX(<field>)`

Lowering status in this baseline:

- `*` and direct field lists are executable.
- `DISTINCT` is executable only for constrained scalar shapes listed above.
- Grouped aggregate forms are executable only for the constrained grouped shape
  listed above.
- Global aggregate projection forms are executable only through
  `execute_sql_aggregate(...)` and only for the constrained terminal set listed
  above.
- Global aggregates remain lowering-gated for scalar/projection SQL surfaces
  (`execute_sql`, `execute_sql_dispatch`).

Predicate operators are limited to planner-supported predicate operators.
The current baseline also supports bounded trailing-wildcard prefix `LIKE`
families:
- `<field> LIKE '<prefix>%'` lowers to strict text prefix predicate intent.
- `LOWER(<field>) LIKE '<prefix>%'` lowers to text-casefold starts-with
  predicate intent.
- `UPPER(<field>) LIKE '<prefix>%'` lowers to the same bounded casefold
  prefix intent.
- direct predicate spellings are executable and lower to the same bounded
  planner-bearing prefix intent:
  - `WHERE STARTS_WITH(<field>, '<prefix>')` stays strict
  - `WHERE STARTS_WITH(LOWER(<field>), '<prefix>')` lowers to text-casefold
    prefix intent
  - `WHERE STARTS_WITH(UPPER(<field>), '<prefix>')` lowers to the same bounded
    casefold prefix intent
  - these accepted direct `STARTS_WITH(...)` predicate forms are valid in the
    bounded executable `WHERE` surface for both `SELECT` and `DELETE`
  - matching `EXPLAIN <select_or_delete>` wrappers preserve the same bounded
    accepted family
- broader direct predicate-function spellings remain out of scope:
  - non-casefold wrappers such as `WHERE STARTS_WITH(TRIM(field), value)`
  - the same non-casefold wrapped direct forms remain fail-closed on
    executable `DELETE`, `EXPLAIN DELETE`, and `EXPLAIN JSON DELETE`
  - generic text-function predicates beyond the bounded direct
    `STARTS_WITH(...)` family above
- non-prefix wildcard shapes remain fail-closed.
`HAVING` is executable for grouped SQL with a reduced clause shape:
- clause symbols must be grouped key fields or one aggregate terminal already
  projected in the grouped select list.
- clauses are conjunctive (`AND`) only.
- clause comparisons accept reduced compare forms:
  - `<symbol> <op> <literal>` for `<op>` in `=`, `!=`, `<`, `<=`, `>`, `>=`
  - `<symbol> IS NULL`
  - `<symbol> IS NOT NULL`
- `OR`/`NOT` and broader expression forms remain fail-closed.

## Supported Semantics and Constraints

- Single-entity queries only.
- Deterministic ordering rules and continuation/pagination semantics are owned
  by existing cursor/query contracts.
- Grouped query constraints already enforced by planner remain in force,
  including grouped `DISTINCT`/`HAVING`/`ORDER BY` restrictions.
- Unsupported but syntactically valid shapes must fail closed with typed
  lowering/planner/validation errors.

## Out of Scope (Must Reject)

- `INSERT`
- `UPDATE`
- table aliases (`FROM users u`, `FROM users AS u`)
- joins (`JOIN`, `LEFT JOIN`, etc.)
- subqueries
- `UNION` / `INTERSECT` / `EXCEPT`
- CTEs (`WITH`)
- window functions
- quoted identifiers
- multi-statement input
- SQL function namespaces and generic expression forms beyond the current
  bounded computed text projection family and supported aggregate terminals
- SQL dialect extensions not represented in current query intent/planner model

## Lowering Contract

The parser output maps to existing plan inputs:

- statement kind -> `QueryMode` (`Load` or `Delete`)
- selection list -> projection/aggregate intent
- `WHERE` -> predicate expression
- `ORDER BY` -> `OrderSpec`
- `LIMIT/OFFSET` -> page/window intent
- `GROUP BY`/`HAVING`/`DISTINCT` -> grouped/distinct intent fields

No additional intermediate semantic layer is introduced by this contract.

## Error Contract

- Invalid SQL syntax: parser error.
- Valid SQL outside this subset: unsupported-feature error.
- Reduced parser `UnsupportedFeature` labels are contract-stable within the
  current `0.66` line.
  - Session and generated SQL frontends (`query_from_sql`, `execute_sql`,
    `execute_sql_dispatch`, `execute_sql_grouped`, `execute_sql_aggregate`,
    generated `sql_dispatch::query`) preserve those labels in structured query
    error detail.
- Valid SQL parsed but non-executable in this baseline: lowering-gated
  unsupported error.
- Valid SQL in the executable subset but planner-ineligible shape: existing
  planner/validation error path.

## Versioning Note

This file is the normative SQL subset contract for the reduced parser baseline.
If subset scope changes, update this file in the same patch as parser behavior.
