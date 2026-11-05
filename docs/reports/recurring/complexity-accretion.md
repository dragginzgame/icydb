> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Complexity Accretion Audit - 2026-06-05

## Report Preamble

| Field [M] | Value |
| ---- | ---- |
| `method_version` | `CA-1.4` |
| `completion_status` | `complete` |
| `risk_index_kind` | `overall` |
| `baseline_report` | `docs/audits/reports/2026-05/2026-05-10/complexity-accretion.md` |
| `comparability_status` | `comparable with caveat` |
| `missing_sections` | `none` |

- scope: `crates/icydb-core/src` runtime modules, excluding tests, benches,
  and examples.

- code snapshot identifier: `fc4dc729e` plus local uncommitted audit,
  module-split cleanup, and value-surface cleanup changes.

- baseline caveat: the previous CA-1.4 report was `mechanical-only`; mechanical
  metrics are comparable, while classified owner and flow deltas use `N/A`.

- method manifest:
  - `method_version = CA-1.4`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`

## Required Summary

0. Run metadata + comparability note

- `CA-1.4` completed on `fc4dc729e` plus local uncommitted audit/cleanup
  changes; mechanical comparison uses the 2026-05-10 CA-1.4 partial baseline.

1. Overall complexity risk index

- overall complexity risk index is `4.8/10`, anchored by `5,712` branch sites,
  `14` branch hotspots, and `0` fanout hotspots.

2. Fastest growing concept families

- branch hotspots rose `4 -> 14`; the strongest current concept pressures are
  predicate AST (`14` variants, multiplier `9,352`), AccessPath (`7` variants,
  `27` branch modules), and schema transition admission (`56` branch sites).

3. Highest branch multipliers

- `Predicate` has multiplier `9,352`; `QueryError` has multiplier `1,476`;
  `AccessPath` has multiplier `784`.

4. Branch distribution drift (`AccessPath` / `RouteShape`)

- `AccessPath` appears across `27` branch/reference modules with `2` owner
  boundaries; `RouteShapeKind` appears across `6` modules with `1` owner.

## Verification Readout

- `PASS`: canonical runtime metrics generated.

- `PASS`: required CA-1.4 artifact set generated.

- `PASS`: report generated with baseline and invalidating-signal caveats.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/06/05/complexity-accretion/01/report.md).
