# DRY / Redundancy / Consolidation Audit - 2026-03-08

Scope: duplication and divergence pressure with boundary ownership preserved.

## Structural Duplication Scan

| Pattern | Classification | Drift Risk |
| ---- | ---- | ---- |
| Planner boundedness policy checks + executor/runtime defensive rechecks | Intentional boundary duplication | Medium |
| Continuation invariants guarded in both cursor planning and index scan traversal | Defensive duplication | Medium |
| Commit marker lifecycle checks in guard + recovery replay | Intentional boundary duplication | Low-Medium |
| Grouped distinct budget checks appearing in shared contract and grouped fold callsites | Defensive duplication | Low |

## Pattern-Level Assessment

| Area | Consolidation Guidance | Risk |
| ---- | ---- | ---- |
| Planner vs executor policy gates | Keep split; semantics and fail-closed runtime guard are separate trust boundaries | Medium |
| Cursor decode vs scan envelope checks | Keep split; token trust and storage traversal trust are distinct | Medium |
| Commit guard vs replay behavior | Keep split; in-process lifecycle and durable replay authority are distinct | Low-Medium |

## Dangerous Consolidations (Do Not Merge)

- Planner policy validation and executor runtime guard layers.
- Cursor shape validation and storage traversal envelope guard.
- Commit guard success/failure marker semantics and recovery replay authority.

## Quantitative Summary

- Duplication patterns noted: 9
- High-risk divergence-prone patterns: 1
- Overall DRY Risk Index: **5/10**

Key conclusion:
- Duplication pressure is mostly intentional and boundary-protective; current high-risk accidental divergence remains limited.
