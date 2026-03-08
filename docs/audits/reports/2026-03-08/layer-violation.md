# Cross-Cutting Layer Violation Audit - 2026-03-08

Scope: `crates/icydb-core/src/db/` non-test runtime modules.

Layer model: `intent -> query/plan -> access -> executor -> index/storage -> codec`.

## Step 1 - Policy Re-Derivation

| Policy | Owner Layer | Non-Owner Layers | Drift Risk | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Grouped order/limit policy | `query/plan` | `executor` (defensive runtime checks only) | delegated | Low |
| Cursor paging order+limit compatibility | `query/plan` | query intent/wrapper surfaces | no semantic fork | Low |
| Delete ordering/limit contract | `query/plan` + executor phase guard | `executor/kernel` | intentional overlap | Low |

Layer-authority output:
- `Cross-layer policy re-derivations: 0`

## Step 2 - Ordering and Continuation Authority Leakage

| Check | Result | Risk |
| ---- | ---- | ---- |
| Comparator logic outside `index/*` | none detected | Low |
| Envelope helper ownership (`anchor_within_envelope`, `resume_bounds_from_refs`, `continuation_advanced`) | centralized in `db/index/envelope.rs` | Low |
| Commit-marker low-level store access leakage | none detected outside `db/commit/*` | Low |

## Step 3 - Capability Fan-Out Snapshot

| Surface | Current | Previous (2026-03-06) | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AccessPath::` references | 89 | 116 | -27 | Medium |
| `AccessPath::` files | 8 | 13 | -5 | Medium-Low |
| Enum fan-out >2 layers | 1 | 2 | -1 | Medium-Low |

## Quantitative Snapshot

- Policy duplications: **3**
- Comparator leaks: **0**
- Cross-layer predicate duplication count: **3**
- Cross-Cutting Risk Index: **4/10**

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
