# Resource Model Compliance Audit (2026-03-01)

This audit verifies grouped-execution behavior against
`docs/contracts/RESOURCE_MODEL.md` for the `2026-03-01` snapshot.

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
| All Class B operators route through `ExecutionContext` accounting | `PARTIAL` | Grouped engines and grouped DISTINCT budget flow through `ExecutionContext` (`crates/icydb-core/src/db/executor/aggregate/contracts/grouped.rs`, `crates/icydb-core/src/db/executor/aggregate/contracts/state.rs`). Zero-key global grouped DISTINCT records singleton group + total distinct via `ExecutionContext` (`crates/icydb-core/src/db/executor/load/mod.rs`). Distinct state also exists in non-grouped materialized helpers outside grouped `ExecutionContext` (`crates/icydb-core/src/db/executor/aggregate/distinct.rs`, `crates/icydb-core/src/db/executor/aggregate/projection.rs`). |
| No distinct state exists outside budget tracking | `FAIL` | Local `GroupKeySet` distinct state exists outside grouped budget accounting in materialized scalar helpers (`crates/icydb-core/src/db/executor/aggregate/distinct.rs`, `crates/icydb-core/src/db/executor/aggregate/projection.rs`). Zero-key grouped DISTINCT also keeps a local distinct set, with mirrored cap checks + context counters (`crates/icydb-core/src/db/executor/load/mod.rs`). |
| Zero-key grouped uses implicit-single-group admission | `PASS` | `ExecutionContext::record_implicit_single_group` exists and is invoked in global grouped DISTINCT execution before fold (`crates/icydb-core/src/db/executor/aggregate/contracts/grouped.rs`, `crates/icydb-core/src/db/executor/load/mod.rs`). |
| OrderedStreaming never accumulates cross-group state | `FAIL` | Grouped route execution mode is fixed to `Materialized` (`crates/icydb-core/src/db/executor/route/planner/execution.rs`). Runtime asserts grouped mode remains materialized (`crates/icydb-core/src/db/executor/load/mod.rs`). Group finalization currently materializes grouped outputs and candidate rows into vectors (`crates/icydb-core/src/db/executor/aggregate/contracts/grouped.rs`, `crates/icydb-core/src/db/executor/load/mod.rs`). |
| `SUM(DISTINCT)` and `COUNT(DISTINCT)` cap correctly | `PASS` | Global grouped DISTINCT enforces total/per-group caps and only folds `SUM` after first distinct insert (`crates/icydb-core/src/db/executor/load/mod.rs`). Contract tests cover per-group/total cap failures and grouped paged-builder tests cover global DISTINCT success/failure paths (`crates/icydb-core/src/db/executor/tests/paged_builder.rs`, `crates/icydb-core/src/db/executor/aggregate/tests/contracts.rs`). |
| No Class C operator reachable through `HAVING + ORDER + LIMIT` composition | `PASS` | Grouped execution stays under grouped hard limits (`max_groups`, `max_group_bytes`) via `ExecutionContext` and default grouped config (`crates/icydb-core/src/db/executor/group/mod.rs`, `crates/icydb-core/src/db/executor/aggregate/contracts/grouped.rs`). `LIMIT` path keeps bounded candidate rows via `selection_bound` (`crates/icydb-core/src/db/executor/load/mod.rs`). |
| `ORDER BY` without `LIMIT` is disallowed for grouped | `FAIL` | Grouped validation enforces grouped-key prefix alignment, but has no grouped `ORDER BY requires LIMIT` rule (`crates/icydb-core/src/db/query/plan/validate.rs`). Grouped order-without-limit shapes are accepted by current tests (`crates/icydb-core/src/db/query/plan/tests/group.rs`). |

---

## Audit Summary

- `PASS`: 3
- `PARTIAL`: 1
- `FAIL`: 3

Current resource hardening is strong on grouped budget counters, zero-key grouped
admission, and distinct-cap enforcement. Remaining gaps are policy/shape
enforcement and strategy/runtime alignment, not missing baseline grouped caps.

---

## Follow-Up Actions

1. Add grouped policy gate: `GROUP BY ... ORDER BY ...` must require explicit `LIMIT` (or add an equivalent bounded-group proof gate).
2. Align strategy surface with runtime behavior: either implement true ordered streaming grouped runtime or relabel current strategy/metrics to avoid implying streaming execution.
3. Consolidate distinct-state ownership: route distinct sets through one explicit budget-accounted authority or formally classify non-grouped materialized distinct helpers as outside Class B.
