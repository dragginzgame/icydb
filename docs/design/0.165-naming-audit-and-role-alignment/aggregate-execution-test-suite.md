# Aggregate Execution Test Suite Naming

## Status

Accepted.

## Family

Executor aggregate execution tests.

## Problem

The executor aggregate test suite used `aggregate_core` module and test-name
prefixes even though the suite covers aggregate execution behavior across
scalar, grouped, secondary-index, and planner-bypass routes. It does not own a
generic aggregate core.

Under the 0.165 naming policy, `Core` is kept only for genuine invariant
payloads. Test-suite names should also avoid teaching broad core vocabulary
when the covered behavior is execution-facing.

## Accepted Renames

```text
executor::tests::aggregate_core -> executor::tests::aggregate_execution
mod aggregate_core; -> mod aggregate_execution;
aggregate_core_* tests -> aggregate_execution_* tests
```

## Kept Names

- Production aggregate modules keep their existing domain-specific names where
  they describe reducer, terminal, route, or runtime ownership.
- `aggregate_numeric`, `aggregate_path`, `aggregate_projection`, and
  `aggregate_tail` remain accurate narrower test-family names.
- `PreparedExecutionPlanCore` remains a separate retained decision because it
  is the generic-free invariant payload shared by typed prepared-plan wrappers.

## Old-Vocabulary Scan Terms

```text
aggregate_core|aggregate core|mod aggregate_core|executor::tests::aggregate_core
```
