# Query Plan Semantic Gate Naming

## Status

Accepted.

## Family

Query-plan semantic validation gates.

## Problem

The query-plan validator used a private `core` module for scalar and grouped
semantic validation orchestration. The module does not own a generic planner
core or invariant payload shared by wrappers. It coordinates semantic validation
gates before executor handoff.

Under the 0.165 naming policy, `Core` is kept only for genuine invariant
payloads. This owner is better named after the validation role it performs.

## Accepted Renames

```text
query::plan::validate::core -> query::plan::validate::semantic_gates
validate_plan_core(...) -> validate_scalar_plan_semantic_gates(...)
validate::core module comment -> semantic gate ownership comment
```

## Kept Names

- `validate_query_semantics(...)` and `validate_group_query_semantics(...)`
  remain the public-in-crate semantic validation entry points.
- `PlanError` and grouped/order policy error names remain plan-domain error
  vocabulary, not gate ownership vocabulary.
- Executor defensive validation names remain separate because they mirror, but
  do not own, user-facing query semantics.

## Old-Vocabulary Scan Terms

```text
query::plan::validate::core|validate::core|mod core;|validate_plan_core
```
