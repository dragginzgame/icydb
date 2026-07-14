# Complexity Accretion Audit - 2026-03-30

## Report Preamble

- scope: conceptual growth, branch pressure, and hotspot concentration in `crates/icydb-core/src` runtime modules after `0.66.2` release prep
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-28/complexity-accretion.md`
- code snapshot identifier: `906f0e70` (`dirty` working tree)
- method tag/version: `CA-1.3`
- method manifest:
  - `method_version = CA-1.3`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`
- comparability status: `comparable` for generator-backed runtime totals, branch-site concentration, and hotspot ranking against `2026-03-28`; this run does not regenerate the older classified enum/authority tables

## Evidence Artifacts

- `docs/audits/reports/2026-03/2026-03-30/artifacts/complexity-accretion/runtime-metrics.tsv`
- `docs/audits/reports/2026-03/2026-03-30/artifacts/complexity-accretion/module-branch-hotspots.tsv`

## STEP -1 — Runtime Module Enumeration

Evidence mode: `mechanical`

- full runtime dataset (`505` modules):
  - `docs/audits/reports/2026-03/2026-03-30/artifacts/complexity-accretion/runtime-metrics.tsv`
- derived branch-hotspot view:
  - `docs/audits/reports/2026-03/2026-03-30/artifacts/complexity-accretion/module-branch-hotspots.tsv`

Top branch-site modules from the required enumeration table:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |
| `types::decimal` | `crates/icydb-core/src/types/decimal.rs` | 842 | 4 | 18 | 4.50 | 66 | 65 | 2 | 2 | 69 |
| `db::query::plan::access_choice::evaluator` | `crates/icydb-core/src/db/query/plan/access_choice/evaluator.rs` | 595 | 14 | 61 | 4.36 | 47 | 47 | 2 | 3 | 61 |
| `db::predicate::runtime` | `crates/icydb-core/src/db/predicate/runtime.rs` | 998 | 39 | 164 | 4.21 | 20 | 20 | 3 | 3 | 59 |
| `db::executor::explain::descriptor::shared` | `crates/icydb-core/src/db/executor/explain/descriptor/shared.rs` | 840 | 20 | 76 | 3.80 | 35 | 34 | 2 | 2 | 54 |
| `db::sql::lowering` | `crates/icydb-core/src/db/sql/lowering/mod.rs` | 1006 | 21 | 73 | 3.48 | 26 | 26 | 2 | 2 | 47 |
| `db::access::canonical` | `crates/icydb-core/src/db/access/canonical.rs` | 395 | 9 | 36 | 4.00 | 32 | 32 | 2 | 2 | 41 |
| `db::sql::parser` | `crates/icydb-core/src/db/sql/parser/mod.rs` | 535 | 2 | 9 | 4.50 | 41 | 38 | 2 | 2 | 40 |
| `value` | `crates/icydb-core/src/value/mod.rs` | 724 | 19 | 86 | 4.53 | 20 | 20 | 2 | 3 | 39 |
| `db::reduced_sql` | `crates/icydb-core/src/db/reduced_sql/mod.rs` | 618 | 9 | 147 | 16.33 | 29 | 28 | 2 | 3 | 37 |
| `db::executor::aggregate::contracts::state` | `crates/icydb-core/src/db/executor/aggregate/contracts/state.rs` | 548 | 22 | 85 | 3.86 | 13 | 13 | 2 | 4 | 35 |

## STEP 0 — Baseline Capture

Evidence mode: `semi-mechanical`

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | 483 | 505 | +22 |
| Runtime LOC | `[M]` | primary | 72,799 | 74,055 | +1,256 |
| Runtime fanout (sum) | `[M]` | primary | 973 | 1,010 | +37 |
| Modules with fanout > 12 | `[D]` | primary | 0 | 0 | 0 |
| Modules with `branch_sites_total >= 40` | `[D]` | primary | 8 | 7 | -1 |
| Total branch sites | `[M]` | primary | 2,547 | 2,590 | +43 |
| Top-10 branch concentration | `[D]` | primary | 0.2030 | 0.1861 | -0.0169 |
| Top-10 fanout concentration | `[D]` | primary | 0.0606 | 0.0584 | -0.0022 |

## Current Complexity Signals

- Runtime scope still grew materially, but hotspot concentration improved instead of broadening.
- The parser cleanup meaningfully reduced one real hotspot:
  - `db::sql::parser`: `715 LOC / 70 branch sites -> 535 LOC / 40 branch sites`
  - new owner-local child `db::sql::parser::projection`: `226 LOC / 12 branch sites`
- After that split, `db::sql::parser` dropped out of the top hotspot slot entirely.
- The former large-root cleanup targets stayed decomposed:
  - `db::data::persisted_row`: `32` branch sites
  - `db::query::plan::access_choice`: root `3`, evaluator `61`, model `2`
  - `db::executor::explain::descriptor`: root `0`, shared `54`, load `7`, aggregate `2`
- The `db::session::sql` split avoided creating a new hotspot family:
  - `computed_projection::eval`: `20`
  - `computed_projection::plan`: `13`
  - `dispatch`: `6`
  - no `db::session::sql::*` module crossed the `40` branch-site hotspot threshold

## 0.66.2 Release Read

- The `0.66` decomposition work is holding structurally:
  - module count increased because large owners were split into directory children
  - branch pressure moved into narrower owner-local files rather than remaining in monolith roots
- The parser no longer presents as the top branch-pressure hotspot after the projection split.
- The main remaining complexity risks before release are now:
  - `db::query::plan::access_choice::evaluator`
  - `db::executor::explain::descriptor::shared`
- `db::query::plan::access_choice::evaluator` and `db::executor::explain::descriptor::shared` remain concentrated hotspots, but they are now explicit owning children rather than mixed roots.
- `db::session::sql` does not present as a release-blocking complexity cluster in this run.

## Outcome

- complexity trajectory: `mixed but acceptable`
- release risk from complexity accretion: `Medium`
- blocking recommendation: `none`
- follow-up recommendation:
  - if more `0.66.x` SQL surface is added, keep expanding `db::sql::parser` through owner-local children instead of reopening the root
  - keep future text-semantics expansion out of the typed lowering lane unless parser and planner scope are intentionally reopened
