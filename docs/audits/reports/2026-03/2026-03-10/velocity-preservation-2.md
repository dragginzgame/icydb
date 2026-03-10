# Velocity Preservation Audit - 2026-03-10 (Rerun 2)

## Report Preamble

- scope: feature agility and cross-layer amplification risk after continued `0.47` load-hub decomposition
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-10/velocity-preservation.md`
- code snapshot identifier: `051af8bd` (working-tree rerun)
- method tag/version: `Method V3`
- comparability status: `comparable`

Method: revised CAF model (`revised_caf = max(subsystems, layers) x flow_axes`) with slice-sampled history.

## Step 0 - Baseline Capture

| Metric | Previous (2026-03-10 baseline) | Current (2026-03-10 rerun 2) | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index | 4.5 | 4.0 | -0.5 |
| Cross-layer leakage crossings (tracked boundaries) | 1 | 1 | 0 |
| Avg files touched per feature slice (slice-sampled) | 3.4 | 3.0 | -0.4 |
| Median files touched (slice-sampled) | 3 | 3 | 0 |
| p95 files touched (slice-sampled) | 5 | 5 | 0 |
| Top gravity-well fan-in proxy | 195 | 156 | -39 |
| Route-planner HIP cross-layer count | 1 | 1 | 0 |

## Step 1 - Change Surface Mapping (Revised CAF)

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `0.47` grouped-route decomposition (`grouped_route -> mod/metrics/resolve`) | 3 | 1 | 2 | 1 | 2 | 0.94 | 0.16 | Low |
| `0.47` fast-stream decomposition (`fast_stream runtime/tests split`) | 3 | 1 | 2 | 1 | 2 | 0.94 | 0.16 | Low |

## Step 2 - Boundary Leakage (Mechanical)

| Boundary | Import/Type Crossings | Previous (2026-03-10 baseline) | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| planner -> executor types | 0 | 0 | 0 | Low |
| executor -> planner validation helpers | 1 boundary family | 1 | stable | Medium-Low |
| index -> query-layer types | 0 | 0 | 0 | Low |
| cursor -> executable-plan internals | 0 | 0 | 0 | Low |
| recovery -> query semantics | 0 | 0 | 0 | Low |

## Step 3 - Gravity Well Growth

| Module | Current Signal | Risk |
| ---- | ---- | ---- |
| `db/executor/load/*` | 6,688 LOC aggregate, fan-in proxy 156 | Medium |
| `db/query/plan/*` | 7,847 LOC aggregate | Medium-High |
| `db/query/explain/mod.rs` | 1,775 LOC single-file concentration | Medium |

Fan-in proxy evidence command (runtime, tests excluded):
- `rg -n "load::" crates/icydb-core/src/db/executor -g '*.rs' -g '!**/tests/**' | wc -l` -> `156`

## Hub Import Pressure (Required Metric)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Baseline |
| ---- | ---- | ----: | ----: | ---- |
| `executor/route/planner/mod.rs` | `executor(2)`, `query(1)`, `access(1)` | 1 | 1 | stable |
| `executor/load/mod.rs` | `executor(6)`, `access(1)` | 1 | 0 | stable |
| `access/execution_contract/mod.rs` | `access(1)` | 1 | 0 | stable |

## Velocity Risk Index

**4.0/10**

Key conclusion:
- Slice locality guardrails remain green (`median=3`, `p95=5`).
- Route-planner hub pressure remains contained (`cross-layer count = 1`).
- `executor/load` fan-in proxy dropped from `195` to `156`, satisfying the frozen item-1 containment target (`<=160`).

## Follow-Up Actions

- owner boundary: `executor/load`; action: keep fan-in at or below `<=160` while continuing decomposition only when behavior-preserving seams are available; target report date/run: `docs/audits/reports/2026-03/2026-03-12/velocity-preservation.md`
- owner boundary: `crosscutting process`; action: hold slice locality trend at or below current (`median<=8`, `p95<=15`) in the next comparable run; target report date/run: `docs/audits/reports/2026-03/2026-03-12/velocity-preservation.md`

## Verification Readout

- `cargo check -p icydb-core` -> PASS
- `cargo clippy -p icydb-core --all-targets -- -D warnings` -> PASS
- `make check-invariants` -> PASS
- `cargo test -p icydb-core -q` -> PASS
