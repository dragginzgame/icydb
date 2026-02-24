# Audit Governance (META-AUDIT) - 2026-02-24

Scope: audit-suite quality/governance, not runtime code behavior.

## 1. Audit Quality Risk Index per Document

| Audit | Scope Clarity Risk Index (1-10, lower is better) | Drift Risk | Notes |
| ---- | ---- | ---- | ---- |
| boundary-semantics | 2 | Low | clear envelope-only scope with stepwise tables |
| complexity-accretion | 2 | Low | quantitative structure and anti-shallow constraints |
| cursor-ordering | 2 | Low | explicit output format and attack matrix |
| dry-consolidation | 3 | Medium | broad scope but explicit guardrails |
| error-taxonomy | 3 | Medium | strict but operationally heavy |
| index-integrity | 2 | Low | detailed integrity-first sections |
| invariant-preservation | 3 | Medium | broad and overlap-prone by design |
| module-structure | 3 | Medium | structure-specific but partially qualitative |
| recovery-consistency | 2 | Low | side-by-side equivalence structure is strong |
| state-machine-integrity | 3 | Medium | overlaps some recovery/invariant concerns |
| velocity-preservation | 3 | Medium | relies on contextual calibration |

## 2. Structural Weaknesses per Document

| Audit | Structural Depth Risk Index (1-10, lower is better) | Missing Dimensions | Risk |
| ---- | ---- | ---- | ---- |
| boundary-semantics | 2 | none major | Low |
| complexity-accretion | 3 | lacks hard thresholding on some metrics | Medium |
| cursor-ordering | 2 | none major | Low |
| dry-consolidation | 4 | no mandatory hard-count thresholds | Medium |
| error-taxonomy | 4 | expensive full-variant inventory burden | Medium |
| invariant-preservation | 4 | overlap with recovery/state-machine sections | Medium |
| module-structure | 3 | limited explicit exposure thresholds | Medium |
| recovery-consistency | 2 | none major | Low |
| state-machine-integrity | 4 | overlap with recovery and invariants | Medium |
| velocity-preservation | 4 | CAF values partially analyst-calibrated | Medium |

## 3. Overlap Matrix

| Invariant Category | Audits Covering It | Necessary Overlap? | Redundant? |
| ---- | ---- | ---- | ---- |
| Ordering | boundary, cursor, invariant, index | Yes | Partial |
| Envelope safety | boundary, cursor, invariant | Yes | Low redundancy |
| Recovery idempotence | recovery, invariant, state-machine | Yes | Medium redundancy |
| Index consistency | index, invariant, recovery | Yes | Partial |
| Layer visibility discipline | module-structure, velocity | Yes | Low redundancy |
| Complexity/drag | complexity, velocity, dry | Yes | Partial |

## 4. Drift Warnings

| Area | Previous (2026-02-20 meta-audit) | Current | Drift | Risk |
| ---- | ---- | ---- | ---- | ---- |
| audit count | stable | stable | none | Low |
| scope overlap | medium overlap accepted | unchanged | none | Medium |
| scoring model consistency | 1-10 lower-is-better | unchanged | none | Low |
| how-to governance rules | less explicit split `cloc` guidance | updated on 2026-02-24 | improved telemetry discipline | Low |

## 5. Missing Dimensions

| Missing Dimension | Impact | Recommend New Audit? |
| ---- | ---- | ---- |
| plan cache correctness (if introduced) | high future impact | Yes, when feature exists |
| cross-crate facade API creep trend | medium long-term API governance impact | Yes (quarterly light audit) |
| benchmark regression envelope for query-route additions | medium | Optional |

## 6. Consolidation Opportunities

- Keep `boundary-semantics` and `cursor-ordering` separate; granularity differs and both add signal.
- Keep `recovery-consistency` separate from `state-machine-integrity`; replay equivalence deserves dedicated focus.
- Potential narrowing: make `invariant-preservation` reference, not repeat, detailed replay rows from `recovery-consistency`.

## 7. Governance Risk Index

| Dimension           | Risk Index (1-10, lower is better) |
| ------------------- | ----------------------------------- |
| Scope Discipline    | 3 |
| Invariant Precision | 3 |
| Structural Depth    | 4 |
| Redundancy Control  | 4 |
| Drift Detection     | 3 |
| Risk Clarity        | 2 |

Overall Audit Governance Risk Index (1-10, lower is better): **3/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
