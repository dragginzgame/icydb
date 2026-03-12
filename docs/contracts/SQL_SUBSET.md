# SQL Subset Contract (Reduced Parser)

This document defines the SQL surface for the reduced parser line.

Anything not listed here is out of scope and must fail closed.

## Scope

- Applies to SQL text parsing and SQL-to-planner lowering.
- Applies to the initial reduced SQL parser implementation line.
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

## Executable Baseline (Current 0.52 Line)

The current `0.52` line ships a projection-aware scalar SQL subset plus one
reduced grouped-aggregate SQL path. Broader SQL grammar support remains staged
behind lowering gates.

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
- Entity-qualified field identifiers are executable and normalized to canonical
  planner field names (for example `Entity.field`, `schema.Entity.field`).
- Constrained scalar DISTINCT is executable:
  - `SELECT DISTINCT *`
  - `SELECT DISTINCT <field_list>` only when projection includes primary key
    field.
- `SELECT DISTINCT` shapes that do not project primary key remain fail-closed.
- `EXPLAIN` wrappers are supported for these constrained scalar DISTINCT forms.
- Field-list projection currently affects normalized intent/planning/fingerprints.
- `execute_sql(...)` returns entity-shaped `EntityResponse<E>` rows for
  compatibility.
- `execute_sql_projection(...)` returns projection-shaped
  `ProjectionResponse<E>` rows.
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
- Scalar/projection SQL execution APIs (`execute_sql`, `execute_sql_projection`)
  continue to reject global aggregate projection without `GROUP BY`.

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
  (`execute_sql`, `execute_sql_projection`).

Predicate operators are limited to planner-supported predicate operators.
`HAVING` remains parser-level unsupported in this baseline.

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
- SQL function namespace beyond supported aggregate terminals
- multi-statement input
- projection expressions beyond direct fields and supported aggregate terminals
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
    current `0.52` line.
  - Session SQL frontends (`query_from_sql`, `execute_sql`,
    `execute_sql_projection`, `execute_sql_grouped`, `execute_sql_aggregate`,
    `explain_sql`) preserve those labels in structured query error detail.
- Valid SQL parsed but non-executable in this baseline: lowering-gated
  unsupported error.
- Valid SQL in the executable subset but planner-ineligible shape: existing
  planner/validation error path.

## Versioning Note

This file is the normative SQL subset contract for the reduced parser baseline.
If subset scope changes, update this file in the same patch as parser behavior.
