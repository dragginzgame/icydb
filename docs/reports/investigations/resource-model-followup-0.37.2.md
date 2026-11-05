> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Resource Model Follow-Up Audit (2026-03-01, 0.37.2)

This audit closes the two deferred follow-ups from
`docs/design/0.37-aggregate-fluent-api-consolidation.md` section 9.

Scope:

- grouped strategy/runtime label alignment
- DISTINCT state authority clarity between grouped Class B and non-grouped
  materialized terminals

Method:

- static code audit with line-level references
- targeted regression test review

---

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| Grouped strategy labels are runtime-truthful for materialized grouped execution | `PASS` | Route grouped strategy variants are explicitly materialized (`HashMaterialized`, `OrderedMaterialized`) (`crates/icydb-core/src/db/executor/route/contracts.rs`, `crates/icydb-core/src/db/executor/route/planner/feasibility.rs`, `crates/icydb-core/src/db/executor/route/planner/mod.rs`). Grouped route tests assert ordered-eligible shapes still report `MaterializedFallback` while strategy is materialized-labeled (`crates/icydb-core/src/db/executor/tests/route/aggregate.rs`). |
| Non-grouped materialized DISTINCT terminal ownership is explicit and centralized | `PASS` | Non-grouped materialized DISTINCT insertion now routes through one helper boundary (`crates/icydb-core/src/db/executor/aggregate/materialized_distinct.rs`) consumed by scalar `count_distinct_by` and `distinct_values_by` materialized terminals (`crates/icydb-core/src/db/executor/aggregate/distinct.rs`, `crates/icydb-core/src/db/executor/aggregate/projection.rs`). Grouped Class B DISTINCT accounting remains in grouped `ExecutionContext` (`crates/icydb-core/src/db/executor/aggregate/contracts/grouped.rs`, `crates/icydb-core/src/db/executor/load/mod.rs`). |

---

## Audit Summary

- `PASS`: 2
- `PARTIAL`: 0
- `FAIL`: 0

The deferred resource follow-ups are now closed for `0.37.2`.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/03/01/resource-model-followup-0.37.2/01/report.md).
