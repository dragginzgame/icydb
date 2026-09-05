# RECURRING AUDIT — Resource Model Compliance

Apply [Domain Scope And Change Triggers](../../README.md#domain-scope-and-change-triggers)
to all inventories, checks, and output sections below. Record selected and
excluded obligations before analysis; broad coverage requires a requested baseline.

## Purpose

Verify executor/planner behavior remains compliant with
`docs/contracts/RESOURCE_MODEL.md`.

This is a contract-compliance audit.
It is not a feature-design proposal.

---

## Scope

Check resource-model conformance for:

- planner proof vs runtime cap separation
- grouped budgeting and cardinality limits
- DISTINCT state accounting boundaries
- global DISTINCT field aggregate routing
- materialized non-grouped DISTINCT helper boundaries
- scalar scan-budget route gating
- grouped ordering/pagination policy guardrails
- grouped strategy labeling vs runtime behavior
- continuation/cursor interactions that affect boundedness
- runtime budget enforcement behavior under exhaustion
- budget propagation across planner/route/executor boundaries
- resource-class routing and classification coverage
- runtime observability counters that must remain diagnostic-only

---

## Required Checklist

For each run, explicitly mark `PASS` / `PARTIAL` / `FAIL` with concrete
evidence paths.

### Policy Compliance

1. All Class B operators route through budget-accounted execution context.
2. All Class B DISTINCT insertions are admitted through budget-accounted boundaries.
3. Zero-key grouped uses implicit-single-group admission.
4. `OrderedStreaming` is selected only from planner ordered proof plus executor
   revalidation; incompatible shapes remain `HashMaterialized`.
5. `SUM(DISTINCT)` and `COUNT(DISTINCT)` enforce caps deterministically.
6. No Class C shape is reachable through grouped `HAVING + ORDER + LIMIT`.
7. Grouped `ORDER BY` policy is bounded (for example, explicit `LIMIT` gate).
8. Class B operators are never routed through unbudgeted execution paths.
9. Class C shapes are rejected before execution routing.
10. Planner boundedness proof remains separate from runtime caps.
11. Global DISTINCT field aggregates route through grouped Class B accounting.
12. Non-grouped materialized DISTINCT helpers remain effective-window bounded
    and do not become grouped Class B authorities.

### Runtime Enforcement

13. Distinct/group budget exhaustion fails closed (no unbounded fallback path).
14. Aggregation budget exhaustion propagates deterministic resource-class error.
15. Budget propagation is preserved across planner -> route -> executor ->
    grouped execution boundaries.
16. Runtime usage is monotonic and cannot exceed configured caps.
17. Resource-model bounded operators do not emit unbounded intermediate state.
18. Scalar scan-budget hints are shape-gated and disabled when continuation,
    order, or filter conditions violate the scan-budget contract.

### Budget Lifecycle and Coverage

19. Per-query resource budgets reset correctly between independent executions.
20. All grouped operators are explicitly classified under the resource model.
21. Grouped continuation signatures include budget-relevant shape so cursor
    reuse cannot cross incompatible grouped limits.
22. Optional entity metrics (`hits`, `instructions_total`, and
    `instructions_max`) stay diagnostic-only and do not affect planner, route,
    or executor behavior.

---

## Output Contract

Write one dated result file for each run:

- `docs/reports/recurring/YYYY/MM/DD/resource-model-compliance/<run>/report.md`

Result must include:

- run metadata + comparability note
  - compared baseline report path (daily baseline rule: first run of day
    compares to latest prior comparable report or `N/A`; same-day reruns
    compare to that day's run `01` baseline)
  - method tag/version
  - comparability status (`comparable` or `non-comparable` with reason)
- checklist tables grouped by:
  - `Policy Compliance`
  - `Runtime Enforcement`
  - `Budget Lifecycle and Coverage`
- each checklist row must include:
  - requirement
  - status (`PASS`/`PARTIAL`/`FAIL`)
  - evidence path(s)
  - short drift/regression risk note
- short pass/partial/fail counts
- explicit follow-up actions for each `PARTIAL`/`FAIL`
- verification readout (`PASS`/`FAIL`/`BLOCKED`)

Do not overwrite prior dated results.

## Baseline Verification Selection

Apply [Executed-Test Evidence](../../README.md#executed-test-evidence) before
accepting any test result. Select current tests by the obligations below; these
paths locate owners and candidate proofs, and do not themselves establish coverage.
Record missing behavioral proof explicitly rather than dropping a required row.

Paths beginning with `db/` are relative to `crates/icydb-core/src/`.
Core unit selections use `-p icydb-core --lib --features sql`; physical migration
proof also enables `migration`. Select a named integration target separately
when the obligation crosses the canister boundary.

| Proof obligation | Current source/test owners |
| --- | --- |
| Finite grouped defaults, planner-limit handoff, and fresh per-query counters | `db/executor/group/tests.rs`, `db/executor/budget.rs` |
| Exact group/distinct limits and first over-limit rejection without unbounded fallback | `db/executor/budget.rs`, `db/executor/aggregate/runtime/grouped_distinct/` |
| Global aggregate DISTINCT routing, including COUNT, SUM, and AVG | `db/query/plan/group.rs`, `db/executor/aggregate/runtime/grouped_distinct/tests.rs` |
| Materialized DISTINCT window and state bounds | `db/executor/pipeline/operators/distinct/`, `db/executor/terminal/page/` |
| Grouped HAVING/order/limit admission and ordered-proof handoff | `db/query/plan/validate/grouped/`, `db/query/plan/group.rs` |
| Scalar scan admission and continuation-sensitive budgets | `db/query/plan/primary_key_input_resource.rs`, `db/query/plan/continuation.rs`, `db/executor/stream/access/` |
| Cursor identity and budget-relevant grouped shape | `db/cursor/tests/mod.rs`, `db/cursor/signature.rs` |
| Metrics cannot affect execution or admission | `crates/icydb-core/src/metrics/`, `db/executor/budget.rs` |

For the metrics obligation, inspect the diagnostic-only boundary and enable
`metrics` when executing a metrics-gated test. Strategy-mapping tests alone do
not prove runtime budget exhaustion; retain both obligations in the readout.
Add focused proof for any newly introduced operator or budget interaction.
