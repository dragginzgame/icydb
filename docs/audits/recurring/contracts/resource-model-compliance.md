# RECURRING AUDIT — Resource Model Compliance

## Purpose

Verify executor/planner behavior remains compliant with
`docs/contracts/RESOURCE_MODEL.md`.

This is a contract-compliance audit.
It is not a feature-design proposal.

---

## Scope

Check resource-model conformance for:

- grouped budgeting and cardinality limits
- DISTINCT state accounting boundaries
- grouped ordering/pagination policy guardrails
- grouped strategy labeling vs runtime behavior
- continuation/cursor interactions that affect boundedness
- runtime budget enforcement behavior under exhaustion
- budget propagation across planner/route/executor boundaries
- resource-class routing and classification coverage

---

## Required Checklist

For each run, explicitly mark `PASS` / `PARTIAL` / `FAIL` with concrete
evidence paths.

### Policy Compliance

1. All Class B operators route through budget-accounted execution context.
2. All Class B DISTINCT insertions are admitted through budget-accounted boundaries.
3. Zero-key grouped uses implicit-single-group admission.
4. Ordered-group strategy labels do not imply streaming runtime behavior.
5. `SUM(DISTINCT)` and `COUNT(DISTINCT)` enforce caps deterministically.
6. No Class C shape is reachable through grouped `HAVING + ORDER + LIMIT`.
7. Grouped `ORDER BY` policy is bounded (for example, explicit `LIMIT` gate).
8. Class B operators are never routed through unbudgeted execution paths.
9. Class C shapes are rejected before execution routing.

### Runtime Enforcement

10. Distinct/group budget exhaustion fails closed (no unbounded fallback path).
11. Aggregation budget exhaustion propagates deterministic resource-class error.
12. Budget propagation is preserved across planner -> route -> executor ->
    grouped execution boundaries.
13. Runtime usage is monotonic and cannot exceed configured caps.
14. Resource-model bounded operators do not emit unbounded intermediate state.

### Budget Lifecycle and Coverage

15. Per-query resource budgets reset correctly between independent executions.
16. All grouped operators are explicitly classified under the resource model.

---

## Output Contract

Write one dated result file for each run:

- `docs/audits/reports/YYYY-MM/YYYY-MM-DD/resource-model-compliance*.md`

Result must include:

- run metadata + comparability note
  - compared baseline report path (daily baseline rule: first run of day
    compares to latest prior comparable report or `N/A`; same-day reruns
    compare to that day's `resource-model-compliance.md` baseline)
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
