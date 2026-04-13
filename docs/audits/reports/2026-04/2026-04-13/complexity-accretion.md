# Complexity Accretion Audit - 2026-04-13

## Report Preamble

- scope: conceptual growth, branch pressure, hotspot concentration, and authority spread in `crates/icydb-core/src` runtime modules (non-test)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-30/complexity-accretion.md`
- code snapshot identifier: `d23cd2cf5` (`dirty` working tree)
- method tag/version: `CA-1.3`
- method manifest:
  - `method_version = CA-1.3`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`
- comparability status: `comparable` for generator-backed runtime totals, hotspot counts, and concentration ratios against `2026-03-30`; this run does not regenerate the older classified enum/authority tables

## Evidence Artifacts

- `docs/audits/reports/2026-04/2026-04-13/artifacts/complexity-accretion/runtime-metrics.tsv`
- `docs/audits/reports/2026-04/2026-04-13/artifacts/complexity-accretion/module-branch-hotspots.tsv`

## STEP -1 — Runtime Module Enumeration

Evidence mode: `mechanical`

- full runtime dataset (`504` modules):
  - `docs/audits/reports/2026-04/2026-04-13/artifacts/complexity-accretion/runtime-metrics.tsv`
- derived branch-hotspot view:
  - `docs/audits/reports/2026-04/2026-04-13/artifacts/complexity-accretion/module-branch-hotspots.tsv`

Top branch-site modules from the required enumeration table:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |
| `types::decimal` | `crates/icydb-core/src/types/decimal.rs` | 860 | 4 | 18 | 4.50 | 66 | 65 | 2 | 2 | 69 |
| `db::session::sql::execute` | `crates/icydb-core/src/db/session/sql/execute/mod.rs` | 1166 | 25 | 84 | 3.36 | 40 | 39 | 3 | 6 | 64 |
| `db::predicate::runtime` | `crates/icydb-core/src/db/predicate/runtime.rs` | 1127 | 39 | 178 | 4.56 | 24 | 24 | 3 | 3 | 63 |
| `db::sql::parser::statement` | `crates/icydb-core/src/db/sql/parser/statement.rs` | 631 | 7 | 22 | 3.14 | 58 | 55 | 2 | 2 | 62 |
| `db::reduced_sql::lexer` | `crates/icydb-core/src/db/reduced_sql/lexer.rs` | 248 | 4 | 67 | 16.75 | 57 | 56 | 2 | 1 | 60 |
| `db::executor::aggregate::contracts::state` | `crates/icydb-core/src/db/executor/aggregate/contracts/state.rs` | 765 | 29 | 96 | 3.31 | 24 | 24 | 2 | 4 | 53 |
| `db::executor::terminal::page` | `crates/icydb-core/src/db/executor/terminal/page.rs` | 1112 | 11 | 30 | 2.73 | 36 | 35 | 3 | 3 | 46 |
| `db::query::plan::access_choice::evaluator::range` | `crates/icydb-core/src/db/query/plan/access_choice/evaluator/range.rs` | 358 | 9 | 23 | 2.56 | 33 | 33 | 3 | 2 | 42 |
| `value` | `crates/icydb-core/src/value/mod.rs` | 916 | 22 | 158 | 7.18 | 20 | 20 | 2 | 3 | 42 |
| `db::data::structural_field::value_storage` | `crates/icydb-core/src/db/data/structural_field/value_storage.rs` | 646 | 14 | 96 | 6.86 | 26 | 26 | 2 | 4 | 40 |

## STEP 0 — Baseline Capture

Evidence mode: `semi-mechanical`

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | 505 | 504 | -1 |
| Runtime LOC | `[M]` | primary | 74,055 | 88,584 | +14,529 |
| Runtime fanout (sum) | `[M]` | primary | 1,010 | 1,037 | +27 |
| Modules with fanout > 12 | `[D]` | primary | 0 | 0 | 0 |
| Modules with `branch_sites_total >= 40` | `[D]` | primary | 7 | 10 | +3 |
| Total branch sites | `[M]` | primary | 2,590 | 3,212 | +622 |
| Top-10 branch concentration | `[D]` | primary | 0.1861 | 0.1684 | -0.0177 |
| Top-10 fanout concentration | `[D]` | primary | 0.0584 | 0.0569 | -0.0015 |

## Current Complexity Signals

- Runtime scope barely changed in file count (`505 -> 504`) but grew sharply in total control-plane surface:
  - LOC: `74,055 -> 88,584` (`+14,529`)
  - total branch sites: `2,590 -> 3,212` (`+622`)
  - hotspot modules at `branch_sites_total >= 40`: `7 -> 10`
- Concentration improved instead of broadening:
  - top-10 branch-site concentration: `0.1861 -> 0.1684`
  - top-10 fanout concentration: `0.0584 -> 0.0569`
- That means the new pressure is real but not monolithic. Complexity grew across a wider runtime slice rather than only in one root.
- The most important new hotspot is now the SQL/session execution surface:
  - `db::session::sql::execute`: `64` branch sites, `1,166` LOC, `fanout = 6`
  - `db::sql::parser::statement`: `62` branch sites, `631` LOC
  - `db::session::sql::computed_projection::plan`: `25` branch sites
  - `db::sql::projection::runtime`: `14` branch sites
- The older non-SQL hotspot family remains active, not displaced:
  - `db::predicate::runtime`: `63`
  - `db::executor::aggregate::contracts::state`: `53`
  - `db::executor::terminal::page`: `46`
  - `db::query::plan::access_choice::evaluator::range`: `42`
- Fanout still does not indicate hub sprawl:
  - no module crossed the `fanout > 12` threshold
  - maximum branch depth only reached `4`
  - layer snapshot still reports `0` upward imports and `0` cross-layer policy re-derivations

## Structural Interpretation

- This run is not showing classic fanout-driven architectural leakage. The pressure increase is branch-heavy rather than dependency-heavy.
- The main structural drift since `2026-03-30` is that recent SQL convergence work moved complexity into one live owner:
  - `db::session::sql::execute`
  - plus the adjacent parser statement surface
- That is a cleaner failure mode than reopening multiple public lanes, but it is still a real complexity accretion signal because the single owner now sits in the top hotspot tier beside long-standing predicate and terminal/runtime hubs.
- The reduced concentration ratios are a partial invalidating signal: the system is not becoming more centralized in one super-node.
- The `+622` branch-site jump and `+3` new hotspots are stronger than that invalidating signal, so the adjusted interpretation is:
  - `benign broad growth` is not sufficient
  - current state is `moderate pressure with one newly concentrated SQL owner`

## Complexity Trend Table

Evidence mode: `mechanical`

| Metric [M] | 2026-03-26 | 2026-03-28 | 2026-03-30 | 2026-04-13 |
| ---- | ----: | ----: | ----: | ----: |
| runtime files in scope | 483 | 483 | 505 | 504 |
| runtime LOC | 71,835 | 72,799 | 74,055 | 88,584 |
| runtime fanout (sum) | 973 | 973 | 1,010 | 1,037 |
| modules with `branch_sites_total >= 40` | 9 | 8 | 7 | 10 |
| total branch sites | N/A (not recorded in report) | 2,547 | 2,590 | 3,212 |
| top-10 branch concentration | 0.2027 | 0.2030 | 0.1861 | 0.1684 |
| top-10 fanout concentration | 0.0606 | 0.0606 | 0.0584 | 0.0569 |

## Overall Complexity Risk Index

**5.6/10**

Interpretation:

- The line stays in the moderate band.
- The score is higher than the late-March runs because branch pressure and hotspot count both moved up materially.
- The score stays below the high-risk band because fanout, concentration, and layer-authority signals did not deteriorate in parallel.

## Outcome

- complexity trajectory: `upward but still contained`
- release risk from complexity accretion: `Medium`
- blocking recommendation: `none`
- follow-up recommendation:
  - if `0.77.x` keeps widening SQL semantics, cut branch pressure inside `db::session::sql::execute` before adding new statement families
  - do not let parser statement branching and session SQL execution branching grow independently without a shared contraction pass
  - keep new SQL work from reopening extra runtime routing owners outside the current session/parser boundary

## Required Summary

0. Run metadata + comparability note
- `CA-1.3` run on `d23cd2cf5` (`dirty` working tree), compared against `docs/audits/reports/2026-03/2026-03-30/complexity-accretion.md`, and marked `comparable` for generator-backed totals and hotspot ratios.

1. Overall complexity risk index
- overall complexity risk index is `5.6/10`, driven mainly by `+622` total branch sites and hotspot-count growth `7 -> 10`.

2. Fastest growing concept families
- SQL/session execution is the fastest-growing visible family in this run because `db::session::sql::execute` reached `64` branch sites and `db::sql::parser::statement` reached `62`, both in the top hotspot tier.

3. Highest branch multipliers
- the strongest current branch-pressure anchors are `types::decimal = 69`, `db::session::sql::execute = 64`, `db::predicate::runtime = 63`, and `db::sql::parser::statement = 62`.

4. Branch distribution drift (`AccessPath` / `RouteShape`)
- no new branch-distribution evidence suggests fanout-led routing drift; the main branch growth is in SQL/session and parser statement execution rather than expanded access-shape hub spread.

5. Flow multiplication risks (axis-based)
- this run did not measure a new effective-flow table, but the new SQL hotspot growth landed in statement execution and parsing, not in new public lanes or new route families.

6. Semantic authority vs execution spread risks
- layer invariants still show bounded authority anchors (`AccessPath owners = 2`, `RouteShape owners = 3`, `Predicate coercion owners = 4`) with `0` cross-layer policy re-derivations.

7. Ownership drift + fanout pressure
- fanout pressure is still low (`0` modules over `fanout > 12`), and top-10 fanout concentration improved `0.0584 -> 0.0569` even while total fanout rose `+27`.

8. Super-node + call-depth warnings
- no fanout super-node emerged, but `20` modules now hit `max_branch_depth >= 3`, and the SQL/session owner is one of the new deep-branch hotspots.

9. Trend-interpretation filter outcomes
- lower concentration ratios (`branch 0.1861 -> 0.1684`, fanout `0.0584 -> 0.0569`) are a valid dampening signal, but they do not cancel the stronger `+14,529 LOC` and `+622` branch-site expansion.

10. Complexity trend table
- the current trend table shows stable low fanout pressure but a clear late-run branch and LOC rise, especially after the March SQL cleanup line.

11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)
- runtime metrics generation passed, both architecture invariant checks passed, and `cargo check -p icydb-core` passed.

## Verification Readout

- `scripts/audit/runtime_metrics.sh docs/audits/reports/2026-04/2026-04-13/artifacts/complexity-accretion/runtime-metrics.tsv` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
