# 0.165 Grouped Runtime Payload Naming

## Status

Complete.

## Accepted Renames

### Grouped Runtime Row And Result Unpackers

Role proof:

- Owning modules: `db::executor::aggregate::runtime::grouped_row`,
  `db::executor::pipeline::contracts`, and `db::session::response::grouped`
- Payload: executor-owned grouped row/result payloads consumed at aggregate and
  session response boundaries
- Main consumers: grouped DISTINCT scalarization and grouped public response
  finalization
- Chosen family: explicit row/result component vocabulary
- Rejected alternatives:
  - `into_parts`: too weak because these helpers expose named grouped row
    values or grouped page rows plus cursor state
  - `into_components`: less precise than naming the actual grouped row/result
    fields
- Public-surface impact: none; visibility remains inside `crate::db`
- Hard-cut rule: remove the old private unpacker names from live grouped
  runtime and response code

Accepted renames:

```text
RuntimeGroupedRow::into_parts() -> into_group_key_and_aggregate_values()
StructuralGroupedProjectionResult::into_parts() -> into_rows_and_cursor()
```

### Grouped Continuation Window Unpacker

Role proof:

- Owning module: `db::query::plan::continuation`
- Payload: grouped continuation window fields consumed by prepared execution
  when constructing the executor pagination window
- Main consumers: prepared execution plan grouped pagination setup
- Chosen family: explicit pagination-window field vocabulary
- Rejected alternatives:
  - `into_parts`: too weak because the helper returns named pagination window
    fields in handoff order
  - `into_components`: still does not name the pagination-window role
- Public-surface impact: none
- Hard-cut rule: remove the old private helper name from live continuation
  code

Accepted rename:

```text
GroupedContinuationWindow::into_parts() -> into_pagination_window_fields()
```

### Grouped Test Construction Helpers

Role proof:

- Owning modules: grouped projection tests and query-plan model test helpers
- Payload: test-only construction helpers for custom field slots and compiled
  grouped projection inputs
- Main consumers: grouped projection, grouped aggregate runtime, and
  fingerprint tests
- Chosen family: explicit test-input vocabulary
- Rejected alternatives:
  - `from_parts_for_test`: too weak because these helpers construct named test
    slots or grouped projection inputs, not arbitrary parts
  - `from_components_for_test`: still hides the test slot/input role
- Public-surface impact: none; helpers are `#[cfg(test)]`
- Hard-cut rule: remove the old test helper names so tests do not preserve
  generic parts vocabulary as current

Accepted renames:

```text
FieldSlot::from_parts_for_test(...) -> from_test_slot(...)
CompiledGroupedProjectionPlan::from_parts_for_test(...) -> from_test_inputs(...)
```

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "RuntimeGroupedRow::into_parts|row\\.into_parts\\(|StructuralGroupedProjectionResult::into_parts|result\\.into_parts\\(|GroupedContinuationWindow::into_parts|window\\.into_parts\\(|CompiledGroupedProjectionPlan::from_parts_for_test|FieldSlot::from_parts_for_test|from_parts_for_test" crates/icydb-core/src/db/executor/aggregate crates/icydb-core/src/db/executor/projection crates/icydb-core/src/db/query/plan crates/icydb-core/src/db/session/response/grouped.rs crates/icydb-core/src/db/executor/pipeline/contracts/mod.rs docs/design/0.165-naming-audit-and-role-alignment
rg -n "into_group_key_and_aggregate_values|into_rows_and_cursor|into_pagination_window_fields|from_test_slot|from_test_inputs" crates/icydb-core/src/db/executor/aggregate crates/icydb-core/src/db/executor/projection crates/icydb-core/src/db/query/plan crates/icydb-core/src/db/session/response/grouped.rs crates/icydb-core/src/db/executor/pipeline/contracts/mod.rs
```

Remaining old-name hits are allowed only inside this family note as accepted
rename history and scan terms.
