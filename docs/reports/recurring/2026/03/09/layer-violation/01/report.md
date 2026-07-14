# Cross-Cutting Layer Violation Audit - 2026-03-09

## Report Preamble

- scope: `crates/icydb-core/src/db/` non-test runtime modules
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-08/layer-violation.md`
- code snapshot identifier: `b29df45d`
- method tag/version: `Method V3`
- comparability status: `comparable`

Layer model: `intent -> query/plan -> access -> executor -> index/storage -> codec`.

## Step 1 - Policy Re-Derivation

| Policy | Owner Layer | Non-Owner Layers | Drift Risk | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Grouped order/limit policy | `query/plan` | `executor` (defensive runtime checks only) | delegated | Low |
| Cursor paging order+limit compatibility | `query/plan` | query intent/wrapper surfaces | no semantic fork | Low |
| Delete ordering/limit contract | `query/plan` + executor phase guard | `executor/kernel` | intentional overlap | Low |

Layer-authority output:
- `Cross-layer policy re-derivations: 0`
- `Cross-layer predicate duplication count: 0`

## Step 2 - Ordering and Continuation Authority Leakage

| Check | Result | Risk |
| ---- | ---- | ---- |
| Comparator logic outside `index/*` | none detected | Low |
| Envelope helper ownership (`anchor_within_envelope`, `resume_bounds_from_refs`, `continuation_advanced`) | centralized in `db/index/envelope.rs` | Low |
| Commit-marker low-level store access leakage | none detected outside `db/commit/*` | Low |

## Step 3 - Capability Fan-Out Snapshot

| Surface | Previous (2026-03-08) | Current (2026-03-09) | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AccessPath::` references | 89 | 121 | +32 | Medium-High |
| `AccessPath::` files | 8 | 12 | +4 | Medium |
| Enum fan-out >2 layers | 1 | 1 | 0 | Medium-Low |

## Quantitative Snapshot

- Policy duplications found: **3**
- Comparator leaks: **0**
- Capability fan-out >2 layers: **1**
- Invariants enforced in >3 sites: **3**
- Protective redundancies: **4**
- Cross-Cutting Risk Index: **5/10**

## Follow-Up Actions

- None required for this run.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
