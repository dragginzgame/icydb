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

### `GroupedPathRuntimeCore` -> `GroupedPathRuntimeContext`

Role proof:

- Owning module: `db::executor::pipeline::entrypoints::grouped`
- Payload: owner-local grouped runtime context containing traversal runtime,
  row store, accepted entity authority, and output observer bindings
- Main consumers: grouped path preparation, grouped stream build, grouped fold
  execution, and grouped output finalization inside one entrypoint module
- Chosen family: `*Context`
- Rejected alternatives:
  - `*Core`: too strong because this private value is not an invariant payload
    shared by wrappers; it is the grouped entrypoint's local runtime context
  - `*Parts`: too weak because the value is not just a decomposition result
  - `*Runtime`: too broad and loses the owner-local context role
- Public-surface impact: none; the type is private to the grouped entrypoint
  module
- Hard-cut rule: remove the old type and comment vocabulary from live code

### Prepared Runtime `*Parts` -> Prepared Runtime `*Handoff`

Role proof:

- Owning module: `db::executor::prepared_execution_plan::handoff`
- Payload: prepared-plan runtime handoff values that move accepted authority,
  prepared execution residents, lowered access specs, retained-slot layout, and
  prepared projection shape across executor entrypoint boundaries
- Main consumers: scalar and grouped entrypoints, delete runtime preparation,
  aggregate streaming setup, and structural projection adapters
- Chosen family: `*Handoff`
- Rejected alternatives:
  - `*Parts`: allowed only for temporary decomposition, but these values have
    become named executor boundary payloads
  - `*Inputs`: too generic because some values are consumed after planning as
    prepared runtime residents, not only constructor input bundles
  - `*Payload`: too broad and less precise than the existing executor handoff
    wording around these boundaries
- Public-surface impact: none; visibility remains executor-internal
- Hard-cut rule: remove the old type, helper, module, and active-doc vocabulary
  from live code

Accepted code examples:

```text
prepared_execution_plan::parts -> prepared_execution_plan::handoff
PreparedScalarRuntimeParts -> PreparedScalarRuntimeHandoff
PreparedAccessPlanParts -> PreparedAccessPlanHandoff
PreparedAggregateStreamingPlanParts -> PreparedAggregateStreamingPlanHandoff
SharedPreparedProjectionRuntimeParts -> SharedPreparedProjectionRuntimeHandoff
```

Companion helper renames:

```text
into_scalar_runtime_parts(...) -> into_scalar_runtime_handoff(...)
cloned_grouped_runtime_parts(...) -> cloned_grouped_runtime_handoff(...)
into_access_plan_parts(...) -> into_access_plan_handoff(...)
into_streaming_parts(...) -> into_streaming_handoff(...)
into_projection_runtime_parts(...) -> into_projection_runtime_handoff(...)
```

### Runtime Adapter Constructor `*Parts` Helpers -> Runtime Constructors

Role proof:

- Owning module: `db::executor::pipeline::runtime`
- Payload: constructors for one monomorphic execution runtime adapter from
  already resolved traversal/scalar runtime handles
- Main consumers: scalar materialized execution, scalar aggregate row sinks,
  grouped execution, delete key-stream resolution, and aggregate streaming
  execution
- Chosen family: direct runtime constructor verbs
- Rejected alternatives:
  - `from_*_runtime_parts`: inaccurate because the helpers do not consume a
    named parts payload
  - `from_*_runtime_handoff`: overstates the role because these helpers build
    an adapter from concrete runtime handles rather than moving prepared-plan
    handoff values
- Public-surface impact: none; visibility remains executor-internal
- Hard-cut rule: remove the old helper names from live code

Accepted code examples:

```text
ExecutionRuntimeAdapter::from_scalar_runtime_parts(...) -> from_scalar_runtime(...)
ExecutionRuntimeAdapter::from_stream_runtime_parts(...) -> from_stream_runtime(...)
```

### Lowered Access Handoff Unpacking

Role proof:

- Owning module: `db::access::lowering`
- Payload: lowered executable access tree plus raw index prefix/range specs
  retained by prepared execution
- Main consumers: prepared execution plan resident construction
- Chosen family: explicit executable/index-spec extraction
- Rejected alternatives:
  - `into_parts`: too weak because `LoweredAccess` is a named access-lowering
    handoff, not an arbitrary decomposition
  - `into_components`: still broad and less useful than naming the extracted
    executable/index-spec payload
- Public-surface impact: none
- Hard-cut rule: remove the old private `into_parts` helper from live lowered
  access code

Accepted code example:

```text
LoweredAccess::into_parts() -> into_executable_and_index_specs()
```

### Executor Runtime Input Helpers

Role proof:

- Owning modules: `db::executor::pipeline::entrypoints::scalar::runtime`,
  `db::executor::pipeline::runtime::grouped`, and
  `db::executor::aggregate::scalar_terminals`
- Payload: private runtime constructors and unpackers that consume already
  validated scalar route, grouped slot-layout, and scalar aggregate terminal
  inputs
- Main consumers: scalar entrypoint adapters, grouped entrypoint preparation,
  prepared execution residents, and scalar aggregate reducers
- Chosen family: explicit runtime/input vocabulary
- Rejected alternatives:
  - `*Parts`: too weak because the helpers operate at named executor runtime
    seams, not temporary decompositions
  - `*Handoff`: too strong for local scalar/grouped runtime constructors that
    do not define a prepared-plan handoff DTO
  - `*Context`: wrong because the helpers do not take one owner-local context
    object
- Public-surface impact: none
- Hard-cut rule: remove the old private helper names and active comments from
  live executor code

Accepted code examples:

```text
prepare_scalar_route_runtime_from_parts(...) -> prepare_scalar_route_runtime_from_inputs(...)
compile_grouped_row_slot_layout_from_parts(...) -> compile_grouped_row_slot_layout_from_inputs(...)
PreparedScalarAggregateTerminalSet::into_runtime_parts() -> into_runtime_inputs()
PreparedScalarAggregateTerminal::from_validated_parts(...) -> from_validated_inputs(...)
```

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

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "PreparedExecutionPlanCoreShared|CoreShared|into_shared|core\\.shared\\b|self\\.shared\\b" crates/icydb-core/src/db/executor/prepared_execution_plan
rg -n "PreparedExecutionPlanResidents|into_residents|core\\.residents|self\\.residents" crates/icydb-core/src/db/executor/prepared_execution_plan
rg -n "PreparedScalarRuntimeParts|PreparedGroupedRuntimeParts|PreparedAccessPlanParts|PreparedAggregateStreamingPlanParts|SharedPreparedProjectionRuntimeParts|from_valid_shared_parts|into_scalar_runtime_parts|cloned_grouped_runtime_parts|into_access_plan_parts|into_streaming_parts|into_projection_runtime_parts|execute_initial_scalar_retained_slot_page_from_runtime_parts|prepared_execution_plan::parts|from_scalar_runtime_parts|from_stream_runtime_parts" crates/icydb-core/src docs/design/0.165-naming-audit-and-role-alignment
rg -n "PreparedScalarRuntimeHandoff|PreparedGroupedRuntimeHandoff|PreparedAccessPlanHandoff|PreparedAggregateStreamingPlanHandoff|SharedPreparedProjectionRuntimeHandoff" crates/icydb-core/src/db/executor/prepared_execution_plan crates/icydb-core/src/db/executor
rg -n "PreparedExecutionInputParts|PreparedExecutionInputContext" crates/icydb-core/src/db/executor
rg -n "GroupedPathRuntimeCore|GroupedPathRuntimeContext" crates/icydb-core/src/db/executor/pipeline/entrypoints/grouped.rs
rg -n "LoweredAccess::into_parts|into_executable_and_index_specs|lowered\\.into_parts\\(" crates/icydb-core/src/db/access crates/icydb-core/src/db/executor docs/design/0.165-naming-audit-and-role-alignment
rg -n "prepare_scalar_route_runtime_from_parts|prepare_scalar_route_runtime_from_inputs|compile_grouped_row_slot_layout_from_parts|compile_grouped_row_slot_layout_from_inputs|into_runtime_parts|into_runtime_inputs|from_validated_parts|from_validated_inputs" crates/icydb-core/src/db/executor docs/design/0.165-naming-audit-and-role-alignment
```

Remaining old-name hits are allowed only inside this family note as accepted
rename history and scan terms.
