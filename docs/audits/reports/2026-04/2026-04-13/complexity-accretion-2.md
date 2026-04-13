# Complexity Accretion Audit - 2026-04-13 (Rerun 2)

## Report Preamble

- scope: conceptual growth, branch pressure, hotspot concentration, and authority spread in `crates/icydb-core/src` runtime modules (non-test)
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-13/complexity-accretion.md`
- code snapshot identifier: `4be4c7e23` (`dirty` working tree)
- method tag/version: `CA-1.3`
- method manifest:
  - `method_version = CA-1.3`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`
- comparability status: `comparable` against the same-day baseline because the method manifest and generator are unchanged; this rerun reflects later projection-surface code changes in the current working tree

## Evidence Artifacts

- `docs/audits/reports/2026-04/2026-04-13/artifacts/complexity-accretion-2/runtime-metrics.tsv`
- `docs/audits/reports/2026-04/2026-04-13/artifacts/complexity-accretion-2/module-branch-hotspots.tsv`

## Baseline Delta Snapshot

Evidence mode: `mechanical`

| Metric | Baseline (2026-04-13 first run) | Current (2026-04-13 rerun 2) | Delta |
| ---- | ----: | ----: | ----: |
| Total runtime files in scope | 504 | 500 | -4 |
| Runtime LOC | 88,584 | 88,404 | -180 |
| Runtime fanout (sum) | 1,037 | 1,031 | -6 |
| Modules with fanout > 12 | 0 | 0 | 0 |
| Modules with `branch_sites_total >= 40` | 10 | 11 | +1 |
| Total branch sites | 3,212 | 3,204 | -8 |
| Top-10 branch concentration | 0.1684 | 0.1667 | -0.0017 |
| Top-10 fanout concentration | 0.0569 | 0.0572 | +0.0003 |
| Modules with `max_branch_depth >= 3` | 20 | 21 | +1 |

## Current Hotspot Read

Top branch-site modules in the rerun dataset:

| module [M] | LOC [M] | fanout [M] | branch_sites_total [D] |
| ---- | ----: | ----: | ----: |
| `types::decimal` | 860 | 2 | 69 |
| `db::predicate::runtime` | 1127 | 3 | 63 |
| `db::sql::parser::statement` | 631 | 2 | 62 |
| `db::reduced_sql::lexer` | 248 | 1 | 60 |
| `db::session::sql::execute` | 1060 | 6 | 55 |
| `db::executor::aggregate::contracts::state` | 765 | 4 | 53 |
| `db::executor::terminal::page` | 1112 | 3 | 46 |
| `db::query::plan::access_choice::evaluator::range` | 358 | 2 | 42 |
| `db::sql::lowering::select` | 720 | 4 | 42 |
| `value` | 916 | 3 | 42 |

## Structural Interpretation

- The first report was stale with respect to the latest working tree. This rerun changes the SQL/session reading materially.
- The most important correction is that `db::session::sql::execute` is still a hotspot, but smaller than the baseline reported:
  - branch sites: `64 -> 55` (`-9`)
  - LOC: `1,166 -> 1,060` (`-106`)
- Overall branch pressure eased slightly:
  - total branch sites: `3,212 -> 3,204` (`-8`)
  - top-10 branch concentration: `0.1684 -> 0.1667`
- Fanout still does not indicate hub sprawl:
  - `0` modules over `fanout > 12`
  - fanout sum fell `1,037 -> 1,031`
  - layer invariants stayed flat:
    - upward imports: `0`
    - cross-layer policy re-derivations: `0`
    - `AccessPath` owners: `2`
    - `RouteShape` owners: `3`
    - predicate coercion owners: `4`
- The one worsening signal is hotspot count:
  - modules at `branch_sites_total >= 40`: `10 -> 11`
  - that increase is broad, not concentrated in the same SQL owner, because `db::sql::lowering::select` now joins the `42` branch-site tier

## Overall Complexity Risk Index

**5.4/10**

Interpretation:

- This rerun is still in the moderate band.
- It is lower than the stale first run because the latest projection-driven changes reduced the measured SQL/session hotspot pressure.
- It stays above the late-March line because the runtime still carries `3,204` total branch sites and `11` hotspot modules.

## Outcome

- complexity trajectory vs same-day baseline: `slightly improved`
- release risk from complexity accretion: `Medium`
- blocking recommendation: `none`
- follow-up recommendation:
  - keep the SQL/session cleanup pressure on `db::session::sql::execute`, but the rerun no longer shows it as the dominant new hotspot
  - the next structural watch items are now split between:
    - `db::predicate::runtime`
    - `db::sql::parser::statement`
    - `db::sql::lowering::select`

## Required Summary

0. Run metadata + comparability note
- `CA-1.3` rerun on `4be4c7e23` (`dirty` working tree), compared against the same-day baseline `docs/audits/reports/2026-04/2026-04-13/complexity-accretion.md`, and marked `comparable`.

1. Overall complexity risk index
- overall complexity risk index is `5.4/10`, with same-day branch pressure easing slightly (`3,212 -> 3,204`) but hotspot count rising (`10 -> 11`).

2. Fastest growing concept families
- no new concept family exploded in the rerun; the biggest correction is actually contraction in the SQL/session owner `db::session::sql::execute` (`64 -> 55` branch sites).

3. Highest branch multipliers
- the leading branch-pressure anchors are now `types::decimal = 69`, `db::predicate::runtime = 63`, `db::sql::parser::statement = 62`, and `db::reduced_sql::lexer = 60`.

4. Branch distribution drift (`AccessPath` / `RouteShape`)
- there is still no fanout-led routing drift signal; the same-day movement remains inside runtime branch hotspots rather than in decision-owner growth.

5. Flow multiplication risks (axis-based)
- this rerun did not surface new lane or route-family multiplication; the changed code reduced one SQL execution owner rather than adding a new flow family.

6. Semantic authority vs execution spread risks
- authority anchors remain stable at `AccessPath = 2`, `RouteShape = 3`, and predicate coercion `= 4`, with `0` cross-layer policy re-derivations.

7. Ownership drift + fanout pressure
- fanout pressure remains low (`0` modules above `fanout > 12`), and total fanout improved slightly (`1,037 -> 1,031`).

8. Super-node + call-depth warnings
- no fanout super-node appeared, but modules at `max_branch_depth >= 3` increased `20 -> 21`, so deep branching pressure did not fully retreat with the SQL/session contraction.

9. Trend-interpretation filter outcomes
- this rerun is a genuine stale-report correction, not a method change: the lower SQL/session hotspot numbers come from later code changes in the current tree, while the broader hotspot count still rose by `+1`.

10. Complexity trend table
- against today’s baseline, the shape is flatter than first reported: lower LOC, lower total branch sites, lower branch concentration, but one extra hotspot module.

11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)
- runtime metrics generation passed, both architecture invariant checks passed, and `cargo check -p icydb-core` passed.

## Verification Readout

- `scripts/audit/runtime_metrics.sh docs/audits/reports/2026-04/2026-04-13/artifacts/complexity-accretion-2/runtime-metrics.tsv` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
