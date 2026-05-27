# Final Boundary Contracts

## Status

Accepted for 0.165.

## Finding

After the broad lexical pass closed, a final risk-based audit found three
remaining live boundary names that still met the rename threshold. These names
were not local wording issues; they described runtime contracts and planner or
executor handoff surfaces using `Descriptor` or `Shape`.

The old names could mislead maintainers about lifecycle and authority:

- accepted row layout metadata is a runtime contract consumed by decode/write
  paths, not a renderable descriptor
- static planner metadata is an execution planning contract frozen before
  executor use, not a loose shape
- prepared projection metadata is an executor projection contract, not just a
  structural shape

## Accepted Renames

```text
AcceptedRowLayoutRuntimeDescriptor -> AcceptedRowLayoutRuntimeContract
StaticPlanningShape -> StaticExecutionPlanningContract
PreparedProjectionShape -> PreparedProjectionContract
prepare_projection_shape_from_plan(...) -> prepare_projection_contract_from_plan(...)
prepared_projection_shape -> prepared_projection_contract
```

## Kept Names

- `SqlQueryShape`, `LoweredSelectShape`, and `LoweredDeleteShape` remain local
  SQL lowering/compile surfaces. They do not currently cross a runtime authority
  boundary in a way that justifies churn.
- Explain descriptors remain renderable/observable description surfaces and
  match the `Descriptor` policy.

## Residual Scan

```text
AcceptedRowLayoutRuntimeDescriptor|StaticPlanningShape|PreparedProjectionShape|prepare_projection_shape_from_plan|prepared_projection_shape
```
