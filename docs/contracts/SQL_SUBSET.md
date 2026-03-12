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

## Executable Baseline (0.52.0)

`0.52.0` ships a minimum executable SQL subset while broader reduced grammar
support is staged behind lowering gates.

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

Notes:

- `SELECT *` is executable in `0.52.0`.
- Scalar pagination still follows existing planner validation
  (for example deterministic ordering requirements).

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

## Parsed but Lowering-Gated (Follow-Up Slices)

The reduced parser can parse additional reduced SQL constructs that remain
intentionally non-executable in `0.52.0`.

### SELECT

Parsed shape:

```sql
SELECT [DISTINCT] <projection>
FROM <entity>
[WHERE <predicate>]
[GROUP BY <field_list>]
[HAVING <predicate>]
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

Predicate operators are limited to planner-supported predicate operators.

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
- joins (`JOIN`, `LEFT JOIN`, etc.)
- subqueries
- `UNION` / `INTERSECT` / `EXCEPT`
- CTEs (`WITH`)
- window functions
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
- Valid SQL parsed but non-executable in this baseline: lowering-gated
  unsupported error.
- Valid SQL in the executable subset but planner-ineligible shape: existing
  planner/validation error path.

## Versioning Note

This file is the normative SQL subset contract for the reduced parser baseline.
If subset scope changes, update this file in the same patch as parser behavior.
