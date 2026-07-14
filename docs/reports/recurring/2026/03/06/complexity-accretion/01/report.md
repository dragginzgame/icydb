# Complexity Accretion Audit - 2026-03-06

Scope: conceptual growth, branch pressure, flow multiplication, and drift sensitivity in `icydb-core` (`crates/icydb-core/src/db`, non-test runtime files).

This run starts the 2026-03-06 crosscutting audit cycle with fresh mechanical metrics and deltas against `2026-03-05`.

## Step 0 - Baseline Capture

| Metric | Previous (2026-03-05) | Current (2026-03-06) | Delta |
| ---- | ----: | ----: | ----: |
| Runtime files in scope | 276 | 280 | +4 |
| Runtime LOC (`db/`, non-test) | 52,529 | 57,209 | +4,680 |
| Runtime files >=600 LOC | 11 | 15 | +4 |
| `continuation|anchor` mentions | 891 | 936 | +45 |
| `continuation|anchor` files | 79 | 82 | +3 |
| Continuation decision owners | 10 | 10 (spot-check unchanged) | 0 |
| Continuation execution consumers | 48 | 48 (spot-check unchanged) | 0 |
| Continuation plumbing modules | 21 | 21 (spot-check unchanged) | 0 |

## Step 1 - Variant Surface + Branch Multiplier (Proxy)

| Enum | Variants | Reference Sites | Multiplier Proxy | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AccessPath` | 7 | 116 | 812 | High |
| `ContinuationMode` | 3 | 2 | 6 | Low-Medium |
| `RouteShapeKind` | 5 | 1 | 5 | Low-Medium |
| `ExecutionOrdering` (planner) | 3 | 1 | 3 | Low |

Notes:
- `AccessPath` now includes `IndexMultiLookup`, increasing shape surface.
- Reference-site count is mechanical (`AccessPath::` token count in non-test runtime files).

## Step 2 - Branching Pressure (Trend)

| Area | Previous | Current | Delta | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `query/plan/expr/type_inference.rs` (`if|match` tokens) | 31 | 31 | stable | Medium-High |
| `executor/route` (`if`, `match` tokens) | `if=57`, `match=13` | `if=73`, `match=17` | `+16 if`, `+4 match` | Medium-High |
| `executor/load` (`if`, `match` tokens) | `if=60`, `match=41` | `if=68`, `match=41` | `+8 if`, `0 match` | Medium |
| `query/plan/validate` (`if`, `match` tokens) | `if=43`, `match=10` | `if=43`, `match=10` | stable | Medium |

## Step 3 - Cross-Cutting Spread Signals

- Layer authority check snapshot:
  - `Cross-layer policy re-derivations: 0`
  - `Enum fan-out > 2 layers: 2`
  - `Comparator definitions outside index: 0`
- Continuation surface still remains the dominant complexity drag signal (`936` mentions across `82` runtime files).

## Complexity Risk Index

**6/10**

Key conclusion:
- Complexity pressure increased versus `2026-03-05` due larger runtime surface and branch token growth in `executor/route`.
- No new semantic-owner drift was detected in this pass; growth pressure is mostly coordination/branching, not authority leakage.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
