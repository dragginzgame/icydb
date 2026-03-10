# Complexity Accretion Audit - 2026-03-09 (Rerun 4)

## Report Preamble

- scope: conceptual growth and branch-surface pressure in `crates/icydb-core/src/db` runtime (non-test)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-09/complexity-accretion.md`
- code snapshot identifier: `051af8bd` (working-tree rerun)
- method tag/version: `Method V3`
- comparability status: `comparable`

## Step 0 - Baseline Capture

| Metric | Baseline (2026-03-09 first run) | Current (2026-03-09 rerun 4) | Delta |
| ---- | ----: | ----: | ----: |
| Runtime files in scope | 310 | 344 | +34 |
| Runtime LOC | 63,342 | 63,248 | -94 |
| Runtime files >= 600 LOC | 17 | 12 | -5 |
| `continuation|anchor` mentions | 1,016 | 1,059 | +43 |
| `continuation|anchor` files | 89 | 102 | +13 |
| AccessPath decision owners | 5 | 3 | -2 |
| RouteShape decision owners | 5 | 3 | -2 |
| Predicate coercion decision owners | 4 | 4 | 0 |

## Step 1 - Variant Surface and Multipliers

| Enum | Variants | Switch/Reference Sites (proxy) | Multiplier Proxy | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AccessPath` | 7 | 121 | 847 | High |
| `ContinuationMode` | 3 | 2 | 6 | Low-Medium |
| `RouteShapeKind` | 5 | 1 | 5 | Low-Medium |
| `ErrorClass` | 6 | 6 | 36 | Medium |

## Step 2 - Branching Pressure

| Area | Baseline (2026-03-09 first run) | Current (2026-03-09 rerun 4) | Delta | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `executor/route` (`if`, `match`) | `if=89`, `match=16` | `if=18`, `match=23` | `-71 if`, `+7 match` | Medium |
| `executor/load` (`if`, `match`) | `if=69`, `match=51` | `if=67`, `match=51` | `-2 if`, `0 match` | Medium-High |
| `query/plan/validate` (`if`, `match`) | `if=74`, `match=12` | `if=9`, `match=12` | `-65 if`, `0 match` | Medium |
| `query/plan/expr/type_inference` (`if`, `match`) | `if=22`, `match=9` | `if=9`, `match=9` | `-13 if`, `0 match` | Medium-Low |

## Step 3 - Cross-Cutting Spread Signals

- Layer-authority check snapshot:
  - `Cross-layer policy re-derivations: 0`
  - `Comparator definitions outside index: 0`
  - `Enum fan-out > 2 layers: 1`
- Decision-owner containment remains locked (`AccessPath=3`, `RouteShape=3`).
- This rerun reflects additional load-module decomposition (`entrypoints/scalar`, `execute/contracts`) without branch or ownership regression.

## Complexity Risk Index

**6/10**

Key conclusion:
- Complexity pressure remains stable.
- Recent decomposition slices increased module granularity while preserving branch and authority invariants.
- Remaining pressure is still concentrated in load-hub size and continuation spread.

## Follow-Up Actions

- owner boundary: `executor/load`; action: continue decomposition toward `dispatch/strategy/terminal` seams and reduce fan-in concentration while preserving behavior; target report date/run: `docs/audits/reports/2026-03/2026-03-12/complexity-accretion.md`
- owner boundary: `cursor + route continuation contracts`; action: contain continuation/anchor spread through capability-first helpers with no decision-owner growth; target report date/run: `docs/audits/reports/2026-03/2026-03-12/complexity-accretion.md`

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `make check-invariants` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo clippy -p icydb-core -- -D warnings` -> PASS
- `cargo test -p icydb-core -q` -> PASS
