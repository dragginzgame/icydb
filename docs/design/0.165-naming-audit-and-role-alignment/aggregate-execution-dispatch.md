# Aggregate Execution Dispatch Naming

## Status

Accepted.

## Family

Aggregate executor dispatch state.

## Problem

The private aggregate executor used `AggregateExecutionDescriptor` for a value
that is not renderable or observable. It carries the prepared aggregate spec,
selected route plan, and direction used to dispatch aggregate execution
branches.

Under the 0.165 naming policy, `Descriptor` is reserved for renderable or
observable descriptions. This value is execution authority for dispatch, so the
old name understated its runtime role.

## Accepted Renames

```text
AggregateExecutionDescriptor -> AggregateExecutionDispatch
PreparedAggregateExecutionState::descriptor -> dispatch
aggregate execution descriptor comments -> dispatch/metadata comments
```

## Kept Names

- `ExplainExecutionDescriptor` and `ExplainExecutionNodeDescriptor` remain
  because they are observable EXPLAIN payloads consumed by renderers.
- `ProjectionExplainDescriptor` remains because it is an EXPLAIN projection
  description, not executor dispatch state.
- `AcceptedRowLayoutRuntimeContract` remains because it is a schema-runtime
  decode/write trust boundary already documented in the relation metadata
  naming note.

## Old-Vocabulary Scan Terms

```text
AggregateExecutionDescriptor|PreparedAggregateExecutionState::descriptor|aggregate execution descriptor
```
