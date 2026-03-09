# Resource Model Compliance Audit (2026-03-01, 0.36.3 Refresh)

This audit verifies grouped-execution behavior against
`docs/contracts/RESOURCE_MODEL.md` after the `0.36.3` grouped policy hardening
gate (`GROUP BY + ORDER BY` requires explicit `LIMIT`).

Scope:

- grouped execution (`GROUP BY`, grouped `HAVING`, grouped `DISTINCT`)
- zero-key global grouped DISTINCT (`COUNT(DISTINCT field)`, `SUM(DISTINCT field)`)
- grouped ordering/pagination policy interaction

Method:

- static code audit with line-level references
- existing test evidence review
- no behavior inferred without code/tests

---

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| All Class B operators route through `ExecutionContext` accounting | `PASS` | Grouped engines and grouped DISTINCT budget flow through `ExecutionContext` (`crates/icydb-core/src/db/executor/aggregate/contracts/grouped.rs`, `crates/icydb-core/src/db/executor/aggregate/contracts/state.rs`). Zero-key global grouped DISTINCT records singleton group + distinct budgets through the same grouped execution context (`crates/icydb-core/src/db/executor/load/mod.rs`). |
| All Class B DISTINCT insertions are admitted through budget-accounted boundaries | `PASS` | `ExecutionContext::admit_distinct_key` is now the grouped DISTINCT admission chokepoint, enforcing deterministic cap order and budget recording (`crates/icydb-core/src/db/executor/aggregate/contracts/grouped.rs`). Grouped terminal state DISTINCT and zero-key global DISTINCT both route through that boundary (`crates/icydb-core/src/db/executor/aggregate/contracts/state.rs`, `crates/icydb-core/src/db/executor/load/mod.rs`). |
| Zero-key grouped uses implicit-single-group admission | `PASS` | `ExecutionContext::record_implicit_single_group` exists and is invoked in global grouped DISTINCT execution before fold (`crates/icydb-core/src/db/executor/aggregate/contracts/grouped.rs`, `crates/icydb-core/src/db/executor/load/mod.rs`). |
| Ordered-group strategy labels do not imply streaming runtime behavior | `PASS` | Grouped metrics strategy labels are now explicitly materialized (`HashMaterialized`, `OrderedMaterialized`) (`crates/icydb-core/src/db/executor/plan_metrics.rs`, `crates/icydb-core/src/obs/sink.rs`, `crates/icydb-core/src/obs/metrics/mod.rs`). Grouped route execution mode remains `Materialized` and is asserted at runtime (`crates/icydb-core/src/db/executor/route/planner/execution.rs`, `crates/icydb-core/src/db/executor/load/mod.rs`). |
| `SUM(DISTINCT)` and `COUNT(DISTINCT)` cap correctly | `PASS` | Global grouped DISTINCT enforces total/per-group caps and only folds `SUM` after first distinct insert (`crates/icydb-core/src/db/executor/load/mod.rs`). Contract tests cover per-group/total cap failures and grouped paged-builder tests cover global DISTINCT success/failure paths (`crates/icydb-core/src/db/executor/tests/paged_builder.rs`, `crates/icydb-core/src/db/executor/aggregate/tests/contracts.rs`). |
| No Class C operator reachable through `HAVING + ORDER + LIMIT` composition | `PASS` | Grouped execution stays under grouped hard limits (`max_groups`, `max_group_bytes`) via `ExecutionContext` and default grouped config (`crates/icydb-core/src/db/executor/group/mod.rs`, `crates/icydb-core/src/db/executor/aggregate/contracts/grouped.rs`). `LIMIT` path keeps bounded candidate rows via `selection_bound` (`crates/icydb-core/src/db/executor/load/mod.rs`). |
| `ORDER BY` without `LIMIT` is disallowed for grouped | `PASS` | Grouped validation now rejects grouped `ORDER BY` without explicit `LIMIT` (`crates/icydb-core/src/db/query/plan/validate.rs`) with intent/plan coverage (`crates/icydb-core/src/db/query/intent/tests.rs`, `crates/icydb-core/src/db/query/plan/tests/group.rs`). |

---

## Audit Summary

- `PASS`: 7
- `PARTIAL`: 0
- `FAIL`: 0

`0.36.3` closes the grouped boundedness policy gap and resolves the remaining
resource-audit alignment issues for grouped Class B DISTINCT and grouped
strategy/runtime labeling.

---

## Follow-Up Actions

No `PARTIAL`/`FAIL` follow-up actions remain for the `0.36.3` grouped audit
scope.
