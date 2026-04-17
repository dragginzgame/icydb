# Complexity Accretion Audit - 2026-04-17

## Report Preamble

- scope: conceptual growth, branch pressure, hotspot concentration, and authority spread in `crates/icydb-core/src` runtime modules (non-test)
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-13/complexity-accretion-2.md`
- code snapshot identifier: `b06d94b33` (`dirty` working tree)
- method tag/version: `CA-1.3`
- method manifest:
  - `method_version = CA-1.3`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`
- comparability status: `comparable` against `2026-04-13/complexity-accretion-2.md` because the method manifest, runtime scope, and metric generator are unchanged; this run re-measures the latest working tree and keeps the same generator-backed totals and hotspot ratios

## Evidence Artifacts

- `docs/audits/reports/2026-04/2026-04-17/artifacts/complexity-accretion/runtime-metrics.tsv`
- `docs/audits/reports/2026-04/2026-04-17/artifacts/complexity-accretion/module-branch-hotspots.tsv`

## STEP -1 — Runtime Module Enumeration

Evidence mode: `mechanical`

- full runtime dataset (`523` modules):
  - `docs/audits/reports/2026-04/2026-04-17/artifacts/complexity-accretion/runtime-metrics.tsv`
- derived branch-hotspot view:
  - `docs/audits/reports/2026-04/2026-04-17/artifacts/complexity-accretion/module-branch-hotspots.tsv`

Top branch-site modules from the required enumeration table:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |
| `db::predicate::runtime` | `crates/icydb-core/src/db/predicate/runtime/mod.rs` | 1,256 | 48 | 199 | 4.15 | 26 | 26 | 3 | 3 | 74 |
| `types::decimal` | `crates/icydb-core/src/types/decimal.rs` | 848 | 4 | 18 | 4.50 | 65 | 64 | 2 | 2 | 68 |
| `db::sql_shared::lexer` | `crates/icydb-core/src/db/sql_shared/lexer.rs` | 249 | 4 | 69 | 17.25 | 55 | 54 | 2 | 1 | 58 |
| `db::executor::aggregate::contracts::state` | `crates/icydb-core/src/db/executor/aggregate/contracts/state.rs` | 765 | 29 | 96 | 3.31 | 24 | 24 | 2 | 4 | 53 |
| `db::predicate::parser` | `crates/icydb-core/src/db/predicate/parser/mod.rs` | 506 | 9 | 18 | 2.00 | 44 | 44 | 3 | 2 | 53 |
| `db::session::sql::projection::runtime` | `crates/icydb-core/src/db/session/sql/projection/runtime.rs` | 1,370 | 5 | 38 | 7.60 | 44 | 44 | 3 | 4 | 49 |
| `db::data::structural_field::value_storage` | `crates/icydb-core/src/db/data/structural_field/value_storage.rs` | 978 | 7 | 64 | 9.14 | 40 | 39 | 2 | 4 | 46 |
| `db::data::structural_field::storage_key` | `crates/icydb-core/src/db/data/structural_field/storage_key.rs` | 569 | 11 | 47 | 4.27 | 30 | 30 | 3 | 5 | 41 |
| `value` | `crates/icydb-core/src/value/mod.rs` | 839 | 21 | 135 | 6.43 | 20 | 20 | 2 | 3 | 41 |
| `db::access::canonical` | `crates/icydb-core/src/db/access/canonical.rs` | 376 | 10 | 39 | 3.90 | 30 | 30 | 2 | 3 | 40 |

## STEP 0 — Baseline Capture

Evidence mode: `semi-mechanical`

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | 500 | 523 | +23 |
| Runtime LOC | `[M]` | primary | 88,404 | 100,746 | +12,342 |
| Runtime fanout (sum) | `[M]` | primary | 1,031 | 1,089 | +58 |
| Modules with fanout > 12 | `[D]` | primary | 0 | 0 | 0 |
| Modules with `branch_sites_total >= 40` | `[D]` | primary | 11 | 11 | 0 |
| Total branch sites | `[M]` | primary | 3,204 | 3,553 | +349 |
| Top-10 branch concentration | `[D]` | primary | 0.1667 | 0.1472 | -0.0195 |
| Top-10 fanout concentration | `[D]` | primary | 0.0572 | 0.0551 | -0.0021 |
| Modules with `max_branch_depth >= 3` | `[D]` | primary | 21 | 24 | +3 |

## Current Complexity Signals

- Runtime scope grew materially again, but the growth stayed distributed instead of collapsing into one new hub:
  - files: `500 -> 523`
  - LOC: `88,404 -> 100,746`
  - total branch sites: `3,204 -> 3,553`
- Hotspot count stayed flat at the top tier:
  - modules with `branch_sites_total >= 40`: `11 -> 11`
  - no module crossed the `fanout > 12` threshold
- Concentration improved even while the codebase grew:
  - top-10 branch concentration: `0.1667 -> 0.1472`
  - top-10 fanout concentration: `0.0572 -> 0.0551`
- The strongest current branch-pressure anchors are now:
  - `db::predicate::runtime = 74`
  - `types::decimal = 68`
  - `db::sql_shared::lexer = 58`
  - `db::executor::aggregate::contracts::state = 53`
  - `db::predicate::parser = 53`
  - `db::session::sql::projection::runtime = 49`
- Same-method module deltas show that some earlier monoliths really were decomposed, but the branch pressure mostly reappeared in sibling owners:
  - `db::session::sql::execute`: `55 -> 6`
  - `db::executor::terminal::page`: `46 -> 5`
  - but new or expanded sibling surfaces now carry that logic:
    - `db::session::sql::projection::runtime`: `14 -> 49`
    - `db::session::sql::execute::write`: `0 -> 29`
    - `db::executor::terminal::page::post_access`: `0 -> 27`
    - `db::executor::terminal::page::plan`: `0 -> 11`
- SQL/reduced-SQL cleanup also shifted branch pressure rather than removing it:
  - `db::reduced_sql::lexer`: `60 -> 0`
  - `db::sql_shared::lexer`: `0 -> 58`
  - `db::sql::parser::statement`: `62 -> 33`
  - `db::sql::parser::clauses`: `10 -> 30`
  - `db::sql::lowering::select::projection`: `0 -> 31`
- Non-SQL growth remains active and now sits beside the SQL surfaces:
  - `db::predicate::runtime`: `63 -> 74`
  - `db::predicate::parser`: `29 -> 53`
  - `db::data::structural_field::storage_key`: `15 -> 41`
  - `db::query::fingerprint::hash_parts`: `26 -> 40`

## Structural Interpretation

- This run still does not show classic dependency-sprawl failure:
  - no fanout super-node emerged
  - no module crossed `fanout > 12`
  - layer snapshot still reports `0` upward imports and `0` cross-layer policy re-derivations
- The main accretion signal is therefore branch redistribution under active decomposition, not architecture leakage.
- That redistribution is partly healthy:
  - `db::session::sql::execute` and `db::executor::terminal::page` no longer hold all of their former branching in one root file
  - routing logic is more owner-local than it was in the prior run
- But the net complexity still increased:
  - total branch sites rose `+349`
  - deep-branch modules (`max_branch_depth >= 3`) rose `21 -> 24`
  - several sibling owners now sit near or above the hotspot tier instead of one root keeping the whole branch budget
- The clearest complexity family in the current tree is now a three-way cluster rather than one single hotspot:
  - SQL projection/runtime shaping
  - predicate parsing and runtime execution
  - structural binary / fingerprint key handling
- Because concentration improved while total branch surface grew, the correct interpretation is:
  - `broad accretion under local decomposition`
  - not `one new central super-node`

## Overall Complexity Risk Index

**5.6/10**

Interpretation:

- The line remains in the moderate band.
- Risk is slightly higher than the `2026-04-13` rerun because runtime size and total branch surface both moved up materially.
- Risk stays below the high band because hotspot count stayed flat, concentration improved, and the authority/fanout invariants stayed bounded.

## Outcome

- complexity trajectory: `upward but still contained`
- release risk from complexity accretion: `Medium`
- blocking recommendation: `none`
- follow-up recommendation:
  - keep SQL cleanup focused below the session root; the next contraction target is `db::session::sql::projection::runtime`, not `db::session::sql::execute` itself
  - treat `db::predicate::{parser,runtime}` as a standing hotspot family and avoid adding new wrapper/operator forms without a local contraction pass
  - watch the structural-key and fingerprint surfaces (`db::data::structural_field::storage_key`, `db::query::fingerprint::hash_parts`) because they have joined the top branch tier without matching fanout pressure, which usually means local semantic overloading rather than module-boundary failure

## Required Summary

0. Run metadata + comparability note
- `CA-1.3` run on `b06d94b33` (`dirty` working tree), compared against `docs/audits/reports/2026-04/2026-04-13/complexity-accretion-2.md`, and marked `comparable`.

1. Overall complexity risk index
- overall complexity risk index is `5.6/10`, driven mainly by `+12,342` runtime LOC and `+349` total branch sites.

2. Fastest growing concept families
- the fastest-growing current family is SQL projection and shared SQL parsing/shaping, with major increases in `db::session::sql::projection::runtime`, `db::sql_shared::lexer`, `db::sql::parser::clauses`, and `db::sql::lowering::select::projection`.

3. Highest branch multipliers
- the strongest current branch-pressure anchors are `db::predicate::runtime = 74`, `types::decimal = 68`, `db::sql_shared::lexer = 58`, `db::executor::aggregate::contracts::state = 53`, `db::predicate::parser = 53`, and `db::session::sql::projection::runtime = 49`.

4. Branch distribution drift (`AccessPath` / `RouteShape`)
- no new fanout-led routing drift appeared; decomposition reduced root-level SQL/session and terminal/page owners, but the branch budget redistributed into sibling owners instead of shrinking.

5. Flow multiplication risks (axis-based)
- this run shows more owner-local flow surfaces in SQL projection, parser clauses, and cursor/token handling, but it does not show a new public route family or fanout-led lane explosion.

6. Semantic authority vs execution spread risks
- authority anchors remain stable at `AccessPath = 2`, `RouteShape = 3`, and predicate coercion `= 4`, with `0` cross-layer policy re-derivations.

7. Ownership drift + fanout pressure
- fanout pressure remains low (`0` modules above `fanout > 12`), and top-10 fanout concentration improved `0.0572 -> 0.0551` even while total fanout rose `+58`.

8. Super-node + call-depth warnings
- no fanout super-node emerged, but modules with `max_branch_depth >= 3` increased `21 -> 24`, and several sibling hotspots now share what used to be root-level branch pressure.

9. Trend-interpretation filter outcomes
- lower concentration ratios are a real dampening signal, but they do not cancel the larger `+23` runtime files, `+12,342` LOC, and `+349` branch-site expansion.

10. Complexity trend table
- against the latest comparable baseline, the system is broader and more branch-heavy, while still avoiding hub concentration and layer-authority drift.

11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)
- runtime metrics generation passed, both architecture invariant checks passed, and `cargo check -p icydb-core` passed.

## Verification Readout

- `scripts/audit/runtime_metrics.sh docs/audits/reports/2026-04/2026-04-17/artifacts/complexity-accretion/runtime-metrics.tsv` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
