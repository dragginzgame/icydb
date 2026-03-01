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

---

## Required Checklist

For each run, explicitly mark `PASS` / `PARTIAL` / `FAIL`:

1. All Class B operators route through budget-accounted execution context.
2. No distinct state exists outside budget tracking for Class B paths.
3. Zero-key grouped uses implicit-single-group admission.
4. Ordered-streaming claims do not hide cross-group accumulation.
5. `SUM(DISTINCT)` and `COUNT(DISTINCT)` enforce caps deterministically.
6. No Class C shape is reachable through grouped `HAVING + ORDER + LIMIT`.
7. Grouped `ORDER BY` policy is bounded (for example, explicit `LIMIT` gate).

---

## Output Contract

Write one dated result file for each run:

- `docs/audits/reports/YYYY-MM-DD/resource-model-compliance.md`

Result must include:

- checklist table with status + concrete evidence paths
- short pass/partial/fail counts
- explicit follow-up actions for each `PARTIAL`/`FAIL`

Do not overwrite prior dated results.
