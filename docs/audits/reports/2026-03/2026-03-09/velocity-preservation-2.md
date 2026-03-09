# Velocity Preservation Audit - 2026-03-09 (Rerun 2)

## Report Preamble

- scope: feature agility and cross-layer amplification risk in recent development slices
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-09/velocity-preservation.md`
- code snapshot identifier: `ce7845ff` (working-tree rerun)
- method tag/version: `Method V3`
- comparability status: `comparable`

Method: revised CAF model (`revised_caf = max(subsystems, layers) x flow_axes`) with slice-sampled history.

## Step 0 - Baseline Capture

| Metric | Previous (2026-03-09 earlier run) | Current (2026-03-09 rerun) | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index | 6.0 | 5.0 | -1.0 |
| Cross-layer leakage crossings (tracked boundaries) | 1 | 1 | 0 |
| Avg files touched per feature slice (slice-sampled) | 15.0 | 15.0 | 0.0 |
| Median files touched (slice-sampled) | 16 | 16 | 0 |
| p95 files touched (slice-sampled) | 17 | 17 | 0 |
| Top gravity-well fan-in proxy | 255 | 190 | -65 |
| Route-planner HIP cross-layer count | 2 | 1 | -1 |

## Step 1 - Change Surface Mapping (Revised CAF)

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `0.46.11` finish pass (`b8f5e8ba`) | 12 | 7 | 5 | 2 | 14 | 0.55 | 0.86 | Medium-High |
| `0.46.10` stabilization pass (`c05fadbf`) | 16 | 4 | 4 | 2 | 8 | 0.50 | 0.89 | Medium |
| `0.46.9` checkpoint pass (`dacf38b9`) | 17 | 7 | 6 | 2 | 14 | 0.42 | 0.80 | High |

## Step 2 - Boundary Leakage (Mechanical)

| Boundary | Import/Type Crossings | Previous (2026-03-09 earlier run) | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| planner -> executor types | 0 | 0 | 0 | Low |
| executor -> planner validation helpers | 1 boundary family | 1 | stable | Medium-Low |
| index -> query-layer types | 0 | 0 | 0 | Low |
| cursor -> executable-plan internals | 0 | 0 | 0 | Low |
| recovery -> query semantics | 0 | 0 | 0 | Low |

## Step 3 - Gravity Well Growth

| Module | Current Signal | Risk |
| ---- | ---- | ---- |
| `db/executor/load/*` | 5,925 LOC aggregate, fan-in proxy 190 | Medium-High |
| `db/query/plan/*` | 7,847 LOC aggregate | Medium-High |
| `db/query/explain/mod.rs` | 1,759 LOC single-file concentration | Medium |

## Hub Import Pressure (Required Metric)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Previous |
| ---- | ---- | ----: | ----: | ---- |
| `executor/route/planner/mod.rs` | `executor(2)` | 1 | 1 | improved (`2 -> 1`) |
| `executor/load/mod.rs` | `executor(6)` | 1 | 0 | stable |
| `access/execution_contract/mod.rs` | `access(1)` | 1 | 0 | stable |

## Velocity Risk Index

**5/10**

Key conclusion:
- Branch and ownership containment work reduced route-planner coordination pressure and lowered gravity-well fan-in.
- Slice touch-size history is still heavy (`avg=15`, `p95=17`), so velocity is improved but not yet fully normalized.

## Follow-Up Actions

- owner boundary: `executor/load`; action: continue decomposition and reduce fan-in proxy toward <=160 while keeping route/load contracts stable; target report date/run: `docs/audits/reports/2026-03/2026-03-12/velocity-preservation.md`
- owner boundary: `crosscutting process`; action: keep slice-size tracking active and bring median files touched to <=8 across comparable upcoming slices; target report date/run: `docs/audits/reports/2026-03/2026-03-12/velocity-preservation.md`

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `make check-invariants` -> PASS
- `cargo test -p icydb-core route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` -> PASS
- `cargo clippy -p icydb-core -- -D warnings` -> PASS
