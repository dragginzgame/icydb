# Complexity Accretion Audit - 2026-03-09

## Report Preamble

- scope: conceptual growth and branch-surface pressure in `crates/icydb-core/src/db` runtime (non-test)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-08/complexity-accretion.md`
- code snapshot identifier: `b29df45d`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Step 0 - Baseline Capture

| Metric | Previous (2026-03-08) | Current (2026-03-09) | Delta |
| ---- | ----: | ----: | ----: |
| Runtime files in scope | 297 | 310 | +13 |
| Runtime LOC | 61,217 | 63,342 | +2,125 |
| Runtime files >= 600 LOC | 14 | 17 | +3 |
| `continuation|anchor` mentions | 925 | 1,016 | +91 |
| `continuation|anchor` files | 86 | 89 | +3 |
| AccessPath decision owners | 4 | 5 | +1 |
| RouteShape decision owners | 3 | 5 | +2 |
| Predicate coercion decision owners | 4 | 4 | 0 |

## Step 1 - Variant Surface and Multipliers

| Enum | Variants | Switch/Reference Sites (proxy) | Multiplier Proxy | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AccessPath` | 7 | 121 | 847 | High |
| `ContinuationMode` | 3 | 2 | 6 | Low-Medium |
| `RouteShapeKind` | 5 | 1 | 5 | Low-Medium |
| `ErrorClass` | 6 | 6 | 36 | Medium |

## Step 2 - Branching Pressure

| Area | Previous (2026-03-08) | Current (2026-03-09) | Delta | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `executor/route` (`if`, `match`) | `if=77`, `match=16` | `if=89`, `match=16` | `+12 if`, `0 match` | High |
| `executor/load` (`if`, `match`) | `if=85`, `match=45` | `if=69`, `match=51` | `-16 if`, `+6 match` | Medium-High |
| `query/plan/validate` (`if`, `match`) | `if=65`, `match=10` | `if=74`, `match=12` | `+9 if`, `+2 match` | High |
| `query/plan/expr/type_inference.rs` (`if`, `match`) | `if=22`, `match=9` | `if=22`, `match=9` | stable | Medium |

## Step 3 - Cross-Cutting Spread Signals

- Layer-authority check snapshot:
  - `Cross-layer policy re-derivations: 0`
  - `Comparator definitions outside index: 0`
  - `Enum fan-out > 2 layers: 1`
- Growth pressure increased in both continuation-surface breadth and route/planner branching.

## Complexity Risk Index

**7/10**

Key conclusion:
- Complexity pressure increased materially versus the previous run, driven by runtime growth, larger continuation surface, and higher route/planner branching pressure.
- Semantic-owner drift remains contained, but coordination cost is rising.

## Follow-Up Actions

- owner boundary: `executor/route`; action: reduce route decision-owner spread from 5 toward 3 by consolidating route-shape policy evaluation points; target report date/run: `docs/audits/reports/2026-03/2026-03-12/complexity-accretion.md`
- owner boundary: `query/plan/validate`; action: split high-branch validation paths into narrower helpers with single-policy ownership per helper; target report date/run: `docs/audits/reports/2026-03/2026-03-12/complexity-accretion.md`

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
