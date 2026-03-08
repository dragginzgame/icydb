# Complexity Accretion Audit - 2026-03-08

Scope: conceptual growth and branch-surface pressure in `crates/icydb-core/src/db` runtime (non-test).

## Step 0 - Baseline Capture

| Metric | Previous (2026-03-06) | Current (2026-03-08) | Delta |
| ---- | ----: | ----: | ----: |
| Runtime files in scope | 280 | 297 | +17 |
| Runtime LOC | 57,209 | 61,217 | +4,008 |
| Runtime files >= 600 LOC | 15 | 14 | -1 |
| `continuation|anchor` mentions | 936 | 925 | -11 |
| `continuation|anchor` files | 82 | 86 | +4 |
| Continuation decision owners | 10 (prior baseline) | 10 (spot-check unchanged) | 0 |
| Continuation execution consumers | 48 (prior baseline) | 48 (spot-check unchanged) | 0 |
| Continuation plumbing modules | 21 (prior baseline) | 21 (spot-check unchanged) | 0 |

## Step 1 - Variant Surface and Multipliers

| Enum | Variants | Switch/Reference Sites (proxy) | Multiplier Proxy | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AccessPath` | 7 | 89 | 623 | High |
| `ContinuationMode` | 3 | 2 | 6 | Low-Medium |
| `RouteShapeKind` | 5 | 1 | 5 | Low-Medium |
| `ErrorClass` | 6 | 6 | 36 | Medium |

## Step 2 - Branching Pressure

| Area | Previous (2026-03-06) | Current (2026-03-08) | Delta | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `executor/route` (`if`, `match`) | `if=73`, `match=17` | `if=77`, `match=16` | `+4 if`, `-1 match` | Medium-High |
| `executor/load` (`if`, `match`) | `if=68`, `match=41` | `if=85`, `match=45` | `+17 if`, `+4 match` | High |
| `query/plan/validate` (`if`, `match`) | `if=43`, `match=10` | `if=65`, `match=10` | `+22 if`, `0 match` | High |
| `query/plan/expr/type_inference.rs` (`if`, `match`) | `if=31`, `match=0` (prior method) | `if=22`, `match=9` | method-adjusted mix | Medium |

## Step 3 - Cross-Cutting Spread Signals

- Layer-authority check snapshot:
  - `Cross-layer policy re-derivations: 0`
  - `Comparator definitions outside index: 0`
  - `Enum fan-out > 2 layers: 1`
- Continuation surface remains broad but did not increase in raw mention count.

## Complexity Risk Index

**6/10**

Key conclusion:
- Complexity pressure is elevated by net runtime growth and higher branch density in load/plan validation paths.
- Semantic authority drift remains contained (no owner-leak findings in this run).

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
