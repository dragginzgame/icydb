# 0.165 Prepared Execution Naming

## Status

Complete.

## Accepted Renames

### `PreparedExecutionPlanCoreShared` -> `PreparedExecutionPlanResidents`

Role proof:

- Owning module: `db::executor::prepared_execution_plan`
- Payload: immutable logical plan, schema fingerprint, continuation contract,
  lowered index specs, and lazy prepared residents shared by prepared-plan
  wrappers
- Main consumers: typed prepared plans, generic-free load/aggregate shells,
  scalar projection runtime adapters, and aggregate-to-load fallback handoffs
- Chosen family: resident-cache terminology
- Rejected alternatives:
  - `PreparedExecutionPlanCoreShared`: stacks `Core` and `Shared` without saying
    what the value owns
  - `SharedPreparedExecutionPlanCore`: implies the core itself is the shared
    shell, while the concrete role is the resident payload behind the core
  - `PreparedExecutionPlanCache`: overstates the role because the value also
    owns non-lazy logical-plan and lowered-access residents
- Public-surface impact: none; visibility remains inside executor/prepared-plan
  internals
- Hard-cut rule: remove the old type, field, method, and active-doc vocabulary
  from live code

Companion helper rename:

- `PreparedExecutionPlanCore::into_shared()` ->
  `PreparedExecutionPlanCore::into_residents()`

The helper now names the resident payload it returns rather than the ownership
mechanism used to store it.

### `PreparedExecutionInputParts` -> `PreparedExecutionInputContext`

Role proof:

- Owning module: `db::executor::pipeline::contracts::execution`
- Payload: short-lived constructor input bundle for one prepared scalar
  execution attempt
- Main consumers: scalar load execution, aggregate fold execution, field
  extrema execution, and delete execution
- Chosen family: `*Context`
- Rejected alternatives:
  - `*Parts`: too weak because the value is the named constructor boundary for
    shared execution inputs, not only a temporary decomposition result
  - `*Inputs`: would collide with `ExecutionInputs`, which is the constructed
    immutable runtime input payload
  - `*Bundle`: less aligned with the 0.165 role-family vocabulary
- Public-surface impact: none; visibility remains executor-internal
- Hard-cut rule: remove the old type and import vocabulary from live code

## Kept Names

### `PreparedExecutionPlanCore`

Kept because it is the genuine invariant payload shared by typed
`PreparedExecutionPlan<E>`, `PreparedLoadPlan`, and `PreparedAggregatePlan`
wrappers. It owns the resident Arc and the methods that preserve cursor,
ordering, preparation, and lowered-access invariants.

Rejected alternatives:

- `PreparedExecutionPlanResidents`: this is now the inner resident payload, not
  the wrapper method surface
- `PreparedExecutionPlanPayload`: too broad and does not explain why typed and
  generic-free wrappers share it

### `SharedPreparedExecutionPlan`

Kept because `Shared` means the generic-free shared shell cached below the SQL
and fluent frontend split. The name is not a generic reusable payload and the
module comment states the shell boundary.

Rejected alternatives:

- `PreparedExecutionPlanResidents`: wrong role; this shell carries authority
  plus core and can typed-clone the prepared plan
- `PreparedProjectionPlan`: too narrow because scalar aggregate terminals also
  consume the shell

### `Prepared*RuntimeParts`

Kept for this slice because these are temporary runtime handoff decompositions
created at prepared-plan boundaries. That is an allowed `Parts` use under the
0.165 ambiguous-stem policy.

Deferred trigger:

- Rename the family together only if the executor adopts a single `*Handoff` or
  `*Inputs` vocabulary for all prepared boundary payloads. Do not rename one
  `Parts` type at a time.

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "PreparedExecutionPlanCoreShared|CoreShared|into_shared|core\\.shared\\b|self\\.shared\\b" crates/icydb-core/src/db/executor/prepared_execution_plan
rg -n "PreparedExecutionPlanResidents|into_residents|core\\.residents|self\\.residents" crates/icydb-core/src/db/executor/prepared_execution_plan
rg -n "PreparedScalarRuntimeParts|PreparedGroupedRuntimeParts|PreparedAccessPlanParts|PreparedAggregateStreamingPlanParts|SharedPreparedProjectionRuntimeParts" crates/icydb-core/src/db/executor/prepared_execution_plan crates/icydb-core/src/db/executor
rg -n "PreparedExecutionInputParts|PreparedExecutionInputContext" crates/icydb-core/src/db/executor
```

Remaining old-name hits are allowed only inside this family note as accepted
rename history and scan terms.
