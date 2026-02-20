# Audit Governance (META-AUDIT) - 2026-02-20

Scope: audit-suite quality and governance, not code behavior.

## 1. Audit Quality Risk Index per Document

| Audit | Scope Clarity Risk Index (1-10, lower is better) | Drift Risk | Notes |
| ---- | ---- | ---- | ---- |
| boundary-semantics | 2 | Low | clear non-goals + mandatory invariant registry |
| complexity-accretion | 2 | Low | quantitative and anti-shallow constraints are explicit |
| cursor-ordering | 2 | Low | explicit attack matrix and required output format |
| dry-consolidation | 3 | Medium | broad scope but strong guardrails prevent unsafe merge advice |
| error-taxonomy | 3 | Medium | strict domains are clear; full-variant requirement is heavy |
| index-integrity | 2 | Low | detailed invariant sections and explicit attack scenarios |
| invariant-preservation | 3 | Medium | comprehensive, but broad and can overlap other audits |
| module-structure | 3 | Medium | clear structure scope; some qualitative interpretation required |
| recovery-consistency | 2 | Low | side-by-side equivalence requirement is strong |
| state-machine-integrity | 3 | Medium | good transition tables, slightly overlapping with recovery/invariant |
| velocity-preservation | 3 | Medium | high-value but partially qualitative in CAF estimation |

## 2. Structural Weaknesses per Document

| Audit | Structural Depth Risk Index (1-10, lower is better) | Missing Dimensions | Risk |
| ---- | ---- | ---- | ---- |
| boundary-semantics | 2 | none major | Low |
| complexity-accretion | 3 | relies on local metric conventions | Medium |
| cursor-ordering | 2 | none major | Low |
| dry-consolidation | 4 | does not require hard file-count metrics by default | Medium |
| error-taxonomy | 4 | exhaustive variant enumeration can become operationally heavy | Medium |
| invariant-preservation | 4 | overlaps with recovery/index details | Medium |
| module-structure | 3 | lacks explicit numeric threshold for overexposure | Medium |
| recovery-consistency | 2 | none major | Low |
| state-machine-integrity | 4 | overlap with recovery + invariant parity | Medium |
| velocity-preservation | 4 | depends on historical context quality | Medium |

## 3. Overlap Matrix

| Invariant Category | Audits Covering It | Necessary Overlap? | Redundant? |
| ---- | ---- | ---- | ---- |
| Ordering | boundary, cursor, invariant, index | Yes | Partial |
| Envelope safety | boundary, cursor, invariant | Yes | Low redundancy |
| Identity enforcement | invariant, index, recovery | Yes | Partial |
| Index consistency | index, invariant, recovery | Yes | Partial |
| Reverse relation symmetry | index, invariant, recovery, state-machine | Yes | Medium redundancy |
| Recovery idempotence | recovery, invariant, state-machine | Yes | Medium redundancy |
| Plan immutability | invariant, state-machine, cursor | Yes | Low redundancy |
| Layer/visibility discipline | module-structure, velocity | Yes | Low redundancy |
| Complexity growth | complexity, velocity | Yes | Low redundancy |
| DRY divergence | dry-consolidation, complexity | Yes | Low redundancy |

## 4. Drift Warnings

| Area | Previous | Current | Drift | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Audit count | 11 operational + howto | unchanged (+ optional META) | stable | Low |
| Scope expansion | moderate overlap accepted | still present in invariant/recovery/state-machine trio | unchanged | Medium |
| Scoring consistency | 1-10 lower-is-better across docs | consistent | stable | Low |

## 5. Missing Dimensions

| Missing Dimension | Impact | Recommend New Audit? |
| ---- | ---- | ---- |
| Plan-cache correctness | future caching work lacks dedicated guardrail | Yes (when cache introduced) |
| Feature-flag compatibility drift | low current impact | No immediate |
| Cross-crate facade API creep trend (core vs meta crate) | medium long-term API governance impact | Yes (lightweight quarterly API-surface audit) |

## 6. Consolidation Opportunities

- Keep `recovery-consistency` separate (high value, explicit equivalence proof).
- Keep `boundary-semantics` and `cursor-ordering` separate (different granularity).
- Potential narrowing opportunity: reduce overlap prompts between `invariant-preservation` and `state-machine-integrity` by explicitly delegating replay details to recovery audit.

## 7. Governance Risk Index

| Dimension | Risk Index (1-10, lower is better) |
| ---- | ---- |
| Scope Discipline | 3 |
| Invariant Precision | 3 |
| Structural Depth | 4 |
| Redundancy Control | 4 |
| Drift Detection | 3 |
| Risk Clarity | 2 |

Overall Audit Governance Risk Index (1-10, lower is better): **3/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
