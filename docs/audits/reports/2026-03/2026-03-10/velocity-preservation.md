# Velocity Preservation Audit - 2026-03-10

## Report Preamble

- scope: feature agility and cross-layer amplification risk in recent development slices
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-09/velocity-preservation-5.md`
- code snapshot identifier: `051af8bd` (working-tree first run of day)
- method tag/version: `Method V3`
- comparability status: `comparable`

Method: revised CAF model (`revised_caf = max(subsystems, layers) x flow_axes`) with slice-sampled history.

## Step 0 - Baseline Capture

| Metric | Previous (2026-03-09 rerun 5) | Current (2026-03-10 first run) | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index | 5.0 | 4.5 | -0.5 |
| Cross-layer leakage crossings (tracked boundaries) | 1 | 1 | 0 |
| Avg files touched per feature slice (slice-sampled) | 15.0 | 3.4 | -11.6 |
| Median files touched (slice-sampled) | 16 | 3 | -13 |
| p95 files touched (slice-sampled) | 17 | 5 | -12 |
| Top gravity-well fan-in proxy | 198 | 195 | -3 |
| Route-planner HIP cross-layer count | 1 | 1 | 0 |

## Step 1 - Change Surface Mapping (Revised CAF)

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `0.47` grouped-distinct runtime split | 5 | 2 | 2 | 1 | 2 | 0.80 | 0.25 | Low-Medium |
| `0.47` grouped-output boundary split | 4 | 1 | 2 | 1 | 2 | 0.90 | 0.20 | Low |
| `0.47` ranking terminal API split | 3 | 1 | 2 | 1 | 2 | 0.92 | 0.18 | Low |
| `0.47` projection binary-operator split | 3 | 1 | 2 | 1 | 2 | 0.94 | 0.18 | Low |
| `0.47` fast-path strategy extraction | 2 | 2 | 2 | 1 | 2 | 0.88 | 0.25 | Low-Medium |

## Step 2 - Boundary Leakage (Mechanical)

| Boundary | Import/Type Crossings | Previous (2026-03-09 rerun 5) | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| planner -> executor types | 0 | 0 | 0 | Low |
| executor -> planner validation helpers | 1 boundary family | 1 | stable | Medium-Low |
| index -> query-layer types | 0 | 0 | 0 | Low |
| cursor -> executable-plan internals | 0 | 0 | 0 | Low |
| recovery -> query semantics | 0 | 0 | 0 | Low |

## Step 3 - Gravity Well Growth

| Module | Current Signal | Risk |
| ---- | ---- | ---- |
| `db/executor/load/*` | 6,086 LOC aggregate, fan-in proxy 195 | Medium-High |
| `db/query/plan/*` | 7,847 LOC aggregate | Medium-High |
| `db/query/explain/mod.rs` | 1,775 LOC single-file concentration | Medium |

## Hub Import Pressure (Required Metric)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Previous |
| ---- | ---- | ----: | ----: | ---- |
| `executor/route/planner/mod.rs` | `executor(2)`, `query(1)`, `access(1)` | 1 | 1 | stable |
| `executor/load/mod.rs` | `executor(6)`, `access(1)` | 1 | 0 | stable |
| `access/execution_contract/mod.rs` | `access(1)` | 1 | 0 | stable |

## Velocity Risk Index

**4.5/10**

Key conclusion:
- Slice locality is materially improved in the latest `0.47` decomposition set (median/p95 file-touch metrics now within guardrails).
- Hard boundary leakage remains controlled.
- The remaining drag is concentrated gravity-well fan-in (`195`), still above the `<=160` containment target.

## Follow-Up Actions

- owner boundary: `executor/load`; action: continue load-hub decomposition to reduce fan-in proxy from `195` toward `<=160` while keeping route/load contracts stable; target report date/run: `docs/audits/reports/2026-03/2026-03-12/velocity-preservation.md`
- owner boundary: `crosscutting process`; action: keep slice locality trend at or below current (`median<=8`, `p95<=15`) for next comparable run; target report date/run: `docs/audits/reports/2026-03/2026-03-12/velocity-preservation.md`

## Verification Readout

- `cargo check -p icydb-core` -> PASS
- `cargo clippy -p icydb-core --all-targets -- -D warnings` -> PASS
- `make check-invariants` -> PASS
- `cargo test -p icydb-core -q` -> PASS
