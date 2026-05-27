# 0.165 SQL Lowering And Query Shape Naming

## Status

Complete.

## Accepted Renames

### `PreparedSqlScalarAggregateDescriptorShape` -> `PreparedSqlScalarAggregatePlanFragment`

Role proof:

- Owning module: `db::sql::lowering::aggregate::strategy`
- Payload: compact prepared SQL scalar aggregate execution fragment consumed at
  the SQL session boundary
- Main consumers: SQL scalar aggregate execution, lowering tests, and prepared
  aggregate strategy assertions
- Chosen family: `*PlanFragment`
- Rejected alternatives:
  - `*DescriptorShape`: stacked two vague observable/structural stems around an
    execution-facing fragment
  - `*Shape`: too weak because this is not just structural classification; it
    is the compact plan fragment passed to execution
  - `*Descriptor`: would imply renderable/explain description rather than the
    execution-facing aggregate strategy fragment
- Public-surface impact: none; the alias was crate-internal/test-facing
- Hard-cut rule: remove the old alias, test helper, and descriptor wording from
  live SQL lowering code

Companion helper cleanup:

- `PreparedSqlScalarAggregateStrategy::descriptor_shape()` was removed.
- `prepared_descriptor_shape()` was replaced by
  `prepared_plan_fragment()`.

### `SqlGlobalAggregateCommandCore` -> `StructuralSqlGlobalAggregateCommand`

Role proof:

- Owning module: `db::sql::lowering::aggregate::command`
- Payload: generic-free SQL global-aggregate command bound onto a
  `StructuralQuery`, prepared scalar aggregate strategies, projection, and
  HAVING expression
- Main consumers: session SQL compile cache, SQL global-aggregate execution,
  and EXPLAIN global-aggregate rendering
- Chosen family: conventional SQL command vocabulary with a `Structural*`
  prefix
- Rejected alternatives:
  - `*Core`: too vague because this value is not an invariant payload shared by
    wrappers; it is the structural command variant of the aggregate SQL command
  - `SqlGlobalAggregateStructuralCommand`: less consistent with the existing
    `StructuralQuery` naming at the command payload boundary
  - `SqlGlobalAggregateCommandPayload`: too broad and does not name the
    structural query surface
- Public-surface impact: none; visibility remains crate-internal
- Hard-cut rule: remove the old type and `command_core` helper vocabulary from
  live code

Companion helper rename:

- `compile_sql_global_aggregate_command_core_from_prepared_with_schema(...)` ->
  `compile_structural_sql_global_aggregate_command_from_prepared_with_schema(...)`

## Kept Names

### `LoweredSelectShape`

Kept for this slice because it is a lowered structural SELECT family, not an
executor-ready plan. It carries query, projection, ordering, grouping, having,
and distinct shape after SQL lowering.

Deferred trigger:

- Revisit only if SQL lowering splits structural command families from
  executable command payloads more sharply.

### `LoweredBaseQueryShape`

Kept because it is the shared lowered structural base-query family used by
SELECT and DELETE lowering. It is not a selected access plan.

### `LoweredSqlAggregateShape`

Kept because this is a local aggregate-call structural classification used while
lowering SQL aggregate expressions. It does not cross into execution as a plan.

### `LoweredExprAnalysis`

Kept because this value is a richer descriptive analysis result, not a compact
category. The `*Analysis` suffix is appropriate here.

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "PreparedSqlScalarAggregateDescriptorShape|descriptor_shape|prepared_descriptor_shape" crates/icydb-core/src/db/sql/lowering crates/icydb-core/src/db/session/sql crates/icydb-core/src/db/session/tests
rg -n "PreparedSqlScalarAggregatePlanFragment|plan_fragment|prepared_plan_fragment" crates/icydb-core/src/db/sql/lowering crates/icydb-core/src/db/session/sql
rg -n "LoweredSelectShape|LoweredBaseQueryShape|LoweredExprAnalysis|LoweredSqlAggregateShape" crates/icydb-core/src/db/sql/lowering
rg -n "SqlGlobalAggregateCommandCore|compile_sql_global_aggregate_command_core_from_prepared_with_schema|StructuralSqlGlobalAggregateCommand|compile_structural_sql_global_aggregate_command_from_prepared_with_schema" crates/icydb-core/src/db/sql/lowering crates/icydb-core/src/db/session/sql
```

Remaining old-name hits are allowed only inside this family note as accepted
rename history and scan terms.
