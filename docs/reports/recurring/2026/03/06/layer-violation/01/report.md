# Cross-Cutting Layer Violation Audit - 2026-03-06

Scope: `crates/icydb-core/src/db/` non-test runtime modules.

Layer direction reference: `intent -> query/plan -> access -> executor -> index/storage -> codec`.

This run audits semantic authority ownership (not import direction only).

## STEP 1 - Policy Re-Derivation Scan

| Policy | Owner Layer | Non-Owner Layers | Drift Risk | Risk Level |
| --- | --- | --- | --- | --- |
| Grouped DISTINCT legality and runtime guard alignment | `query/plan` | `executor/load` (defensive runtime checks) | delegated contract usage | Low |
| Cursor paging order/limit contracts | `query/plan` | query surface wrappers only | no semantic fork | Low |
| Delete ordering/limit safety | `query/plan` + runtime guard boundary | `executor/kernel` | intentional defensive overlap | Low |

Layer-authority invariant output:
- `Cross-layer policy re-derivations: 0`

## STEP 2 - Ordering Authority Leakage

| Comparator Logic | Result | Risk |
| --- | --- | --- |
| Comparator definitions outside `index/*` | none detected | Low |
| Envelope containment ownership | delegated to index-layer envelope helpers | Low |

Layer-authority invariant output:
- `Comparator definitions outside index: 0`

## STEP 3 - Access Capability Fan-Out

| Surface | Current | Previous (2026-03-05) | Delta | Risk |
| --- | ---: | ---: | ---: | --- |
| `AccessPath::` references (non-test runtime) | 116 | 116 (stable proxy baseline) | 0 | Medium |
| `AccessPath::` files | 13 | 13 (stable proxy baseline) | 0 | Medium |
| Enum fan-out >2 layers (authority check) | 2 | 2 | 0 | Medium |

## STEP 4 - Error Classification Drift

| Error Concept | Mapping Sites | Class Differences? | Risk |
| --- | --- | --- | --- |
| `InternalError` -> query execute boundary | query intent error mapping | No | Low |
| executor internal mapping to public error surfaces | executor boundary wrappers | No | Low |

## Output Summary

### High-Risk Cross-Cutting Violations

- None observed in this run.

### Medium-Risk Drift Surfaces

- `AccessPath` fan-out across planner/access/executor/cursor remains the principal cross-layer coordination pressure.

### Low-Risk / Intentional Redundancy

- Comparator and continuation authority remains centralized with delegation-only usage.

### Quantitative Snapshot

- Policy duplications found: **3**
- Comparator leaks: **0**
- Capability fan-out >2 layers: **2**
- Cross-Cutting Risk Index (1-10): **5**

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
