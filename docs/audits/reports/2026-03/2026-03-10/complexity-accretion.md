# Complexity Accretion Audit - 2026-03-10

## Report Preamble

- scope: conceptual growth and branch-surface pressure in `crates/icydb-core/src/db` runtime (non-test)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-09/complexity-accretion-5.md`
- code snapshot identifier: `051af8bd` (working-tree first run of day)
- method tag/version: `Method V3`
- comparability status: `comparable`

## Step 0 - Baseline Capture

| Metric | Baseline (2026-03-09 rerun 5) | Current (2026-03-10 first run) | Delta |
| ---- | ----: | ----: | ----: |
| Runtime files in scope | 345 | 365 | +20 |
| Runtime LOC | 63,269 | 63,205 | -64 |
| Runtime files >= 600 LOC | 12 | 12 | 0 |
| `continuation|anchor` mentions | 1,059 | 1,048 | -11 |
| `continuation|anchor` files | 103 | 105 | +2 |
| AccessPath decision owners | 3 | 3 | 0 |
| RouteShape decision owners | 3 | 3 | 0 |
| Predicate coercion decision owners | 4 | 4 | 0 |

Metric scope filter for this run:
- `-g '!**/tests/**' -g '!**/*tests.rs'`

## Step 1 - Variant Surface and Multipliers

| Enum | Variants | Switch/Reference Sites (proxy) | Multiplier Proxy | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AccessPath` | 7 | 121 | 847 | High |
| `ContinuationMode` | 3 | 2 | 6 | Low-Medium |
| `RouteShapeKind` | 5 | 1 | 5 | Low-Medium |
| `ErrorClass` | 6 | 6 | 36 | Medium |

## Step 2 - Branching Pressure

| Area | Baseline (2026-03-09 rerun 5) | Current (2026-03-10 first run) | Delta | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `executor/route` (`if`, `match`) | `if=18`, `match=23` | `if=18`, `match=23` | `0 if`, `0 match` | Medium |
| `executor/load` (`if`, `match`) | `if=67`, `match=51` | `if=67`, `match=51` | `0 if`, `0 match` | Medium-High |
| `query/plan/validate` (`if`, `match`) | `if=9`, `match=12` | `if=9`, `match=12` | `0 if`, `0 match` | Medium |
| `query/plan/expr/type_inference` (`if`, `match`) | `if=9`, `match=9` | `if=9`, `match=9` | `0 if`, `0 match` | Medium-Low |

## Step 3 - Cross-Cutting Spread Signals

- Layer-authority check snapshot:
  - `Cross-layer policy re-derivations: 0`
  - `Comparator definitions outside index: 0`
  - `Enum fan-out > 2 layers: 1`
- Decision-owner containment remains locked (`AccessPath=3`, `RouteShape=3`).
- Continuation hotspot consolidation landed in `db/index/scan.rs` by sharing
  directional resume/guard flow across both range entrypoints; local
  `continuation|anchor` mentions in this file dropped (`72 -> 62`).

## Complexity Risk Index

**5.5/10**

Key conclusion:
- Branch pressure and authority boundaries remain stable.
- Continuation spread is lower than the previous comparable run (`1,059 -> 1,048`) while owner counts stayed flat.
- Remaining complexity pressure is concentrated in load-hub surface and continuation usage breadth, not owner fragmentation.

## Follow-Up Actions

- owner boundary: `cursor + route continuation contracts`; action: keep continuation surface at or below current (`<=1,048`) while preserving `AccessPath/RouteShape` decision-owner caps (`<=3`); target report date/run: `docs/audits/reports/2026-03/2026-03-12/complexity-accretion.md`
- owner boundary: `query/plan/route` + `executor/route`; action: close remaining high-risk DRY seam without introducing new continuation decision owners; target report date/run: `docs/audits/reports/2026-03/2026-03-12/dry-consolidation.md`

## Verification Readout

- `cargo check -p icydb-core` -> PASS
- `cargo clippy -p icydb-core --all-targets -- -D warnings` -> PASS
- `make check-invariants` -> PASS
- `cargo test -p icydb-core -q` -> PASS
