# Complexity Accretion Audit - 2026-04-17

## Report Preamble

- scope: conceptual growth, branch pressure, hotspot concentration, and authority spread in `crates/icydb-core/src` runtime modules (non-test)
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-13/complexity-accretion-2.md`
- code snapshot identifier: `8ffba6a5c` (`dirty` working tree)
- method tag/version: `CA-1.3`
- method manifest:
  - `method_version = CA-1.3`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`
- comparability status: `comparable` against `2026-04-13/complexity-accretion-2.md` because the method manifest, runtime scope, and metric generator are unchanged; this run re-measures the latest working tree after the `0.88.x` cleanup pass on the same generator-backed basis

## Evidence Artifacts

- `docs/audits/reports/2026-04/2026-04-17/artifacts/complexity-accretion/runtime-metrics.tsv`
- `docs/audits/reports/2026-04/2026-04-17/artifacts/complexity-accretion/module-branch-hotspots.tsv`

## STEP -1 — Runtime Module Enumeration

Evidence mode: `mechanical`

- full runtime dataset (`563` modules):
  - `docs/audits/reports/2026-04/2026-04-17/artifacts/complexity-accretion/runtime-metrics.tsv`
- derived branch-hotspot view:
  - `docs/audits/reports/2026-04/2026-04-17/artifacts/complexity-accretion/module-branch-hotspots.tsv`

Top branch-site modules from the required enumeration table:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |
| `types::decimal` | `crates/icydb-core/src/types/decimal.rs` | 848 | 4 | 18 | 4.50 | 65 | 64 | 2 | 2 | 68 |
| `db::executor::aggregate::contracts::state` | `crates/icydb-core/src/db/executor/aggregate/contracts/state.rs` | 765 | 29 | 96 | 3.31 | 24 | 24 | 2 | 4 | 53 |
| `db::data::structural_field::value_storage` | `crates/icydb-core/src/db/data/structural_field/value_storage.rs` | 978 | 7 | 64 | 9.14 | 40 | 39 | 2 | 4 | 46 |
| `value` | `crates/icydb-core/src/value/mod.rs` | 839 | 21 | 135 | 6.43 | 20 | 20 | 2 | 3 | 41 |
| `db::access::canonical` | `crates/icydb-core/src/db/access/canonical.rs` | 376 | 10 | 39 | 3.90 | 30 | 30 | 2 | 3 | 40 |
| `db::query::plan::access_choice::evaluator::range` | `crates/icydb-core/src/db/query/plan/access_choice/evaluator/range.rs` | 366 | 9 | 23 | 2.56 | 29 | 29 | 3 | 2 | 38 |
| `db::query::plan::expr::ast` | `crates/icydb-core/src/db/query/plan/expr/ast.rs` | 492 | 11 | 63 | 5.73 | 27 | 26 | 2 | 2 | 37 |
| `db::executor::aggregate::runtime::grouped_fold` | `crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/mod.rs` | 1,384 | 10 | 26 | 2.60 | 26 | 26 | 2 | 4 | 36 |
| `db::executor::explain::descriptor::shared` | `crates/icydb-core/src/db/executor/explain/descriptor/shared/mod.rs` | 663 | 16 | 65 | 4.06 | 20 | 19 | 2 | 2 | 35 |
| `db::data::persisted_row::reader` | `crates/icydb-core/src/db/data/persisted_row/reader.rs` | 944 | 16 | 66 | 4.12 | 18 | 18 | 2 | 4 | 34 |

## STEP 0 — Baseline Capture

Evidence mode: `semi-mechanical`

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | 500 | 563 | +63 |
| Runtime LOC | `[M]` | primary | 88,404 | 101,875 | +13,471 |
| Runtime fanout (sum) | `[M]` | primary | 1,031 | 1,154 | +123 |
| Modules with fanout > 12 | `[D]` | primary | 0 | 0 | 0 |
| Modules with `branch_sites_total >= 40` | `[D]` | primary | 11 | 5 | -6 |
| Total branch sites | `[M]` | primary | 3,204 | 3,559 | +355 |
| Top-10 branch concentration | `[D]` | primary | 0.1667 | 0.1203 | -0.0464 |
| Top-10 fanout concentration | `[D]` | primary | 0.0572 | 0.0520 | -0.0052 |
| Modules with `max_branch_depth >= 3` | `[D]` | primary | 21 | 26 | +5 |

## Current Complexity Signals

- Runtime scope is still materially larger than the April 13 baseline:
  - files: `500 -> 563`
  - LOC: `88,404 -> 101,875`
  - total branch sites: `3,204 -> 3,559`
- But the hotspot tier contracted sharply under the same-method rerun:
  - modules with `branch_sites_total >= 40`: `11 -> 5`
  - top-10 branch concentration: `0.1667 -> 0.1203`
  - top-10 fanout concentration: `0.0572 -> 0.0520`
- Relative to the earlier April 17 rerun on this same branch family, the follow-up cleanup did real work instead of just moving names around:
  - total branch sites only moved `3,553 -> 3,559` (`+6`)
  - hotspot count dropped `11 -> 5`
  - top-10 branch concentration dropped again `0.1472 -> 0.1203`
- The strongest current branch-pressure anchors are now:
  - `types::decimal = 68`
  - `db::executor::aggregate::contracts::state = 53`
  - `db::data::structural_field::value_storage = 46`
  - `value = 41`
  - `db::access::canonical = 40`
- The earlier SQL, parser, and fingerprint hotspot family was materially flattened:
  - `db::sql_shared::lexer`: `58 -> family { root = 0, scan = 11, token_body = 4, keywords = 2 }`
  - `db::predicate::parser`: `53 -> family { root = 3, lowering = 9, operand::text = 11, atom::field::plain::special = 12 }`
  - `db::session::sql::projection::runtime`: `49 -> family { root = 4, covering::pure = 12, materialize = 10, render = 5 }`
  - `db::query::fingerprint::hash_parts`: `40 -> family { root = 18, grouping = 8, grouping::having = 9, profile = 5 }`
  - `db::data::structural_field::storage_key`: `41 -> family { root = 1, decode = 12, encode = 8, scalar leaves <= 8 }`
- The current top tier is therefore no longer dominated by the recent `0.88` SQL/predicate cleanup line; it has shifted back toward older core runtime owners that were already structurally dense before this audit pass:
  - decimal arithmetic / formatting
  - aggregate state contracts
  - value and structural storage
  - canonical access planning

## Structural Interpretation

- This run still does not show classic architecture-sprawl failure:
  - no fanout super-node emerged
  - no module crossed `fanout > 12`
  - layer snapshot still reports `0` upward imports and `0` cross-layer policy re-derivations
- The prior April 17 read was best described as `broad accretion under local decomposition`.
- This refreshed read is different:
  - the decomposition work actually lowered hotspot concentration
  - the old SQL/parser/fingerprint family no longer dominates the top branch tier
  - total branch surface stayed almost flat across the follow-up cleanup while module count kept growing
- The remaining complexity is now more localized and older in character:
  - dense arithmetic/value domains
  - aggregate state contracts
  - canonical access planning
- That means the system is still broader than baseline, but the fresh signal is not runaway redistribution anymore. It is:
  - `broader runtime with successful hotspot contraction`

## Overall Complexity Risk Index

**4.8/10**

Interpretation:

- Risk is still in the moderate band because runtime scope and total branch surface remain materially above the April 13 baseline.
- Risk is lower than the earlier April 17 rerun because the top hotspot tier was cut almost in half and concentration ratios improved substantially.
- Risk stays below the high band because authority/fanout invariants remain bounded and the old SQL/predicate expansion line has been structurally flattened.

## Outcome

- complexity trajectory: `broader than baseline, but locally improving`
- release risk from complexity accretion: `Medium`
- blocking recommendation: `none`
- follow-up recommendation:
  - stop micro-splitting the parser tree unless a file still mixes genuinely different policy families
  - treat `types::decimal`, `db::executor::aggregate::contracts::state`, `db::data::structural_field::value_storage`, and `db::access::canonical` as the next real contraction candidates
  - rerun the audit after the next aggregate/value cleanup slice, because the remaining pressure is now concentrated in long-lived core runtime owners rather than the recent SQL feature line

## Required Summary

0. Run metadata + comparability note
- `CA-1.3` run on `8ffba6a5c` (`dirty` working tree), compared against `docs/audits/reports/2026-04/2026-04-13/complexity-accretion-2.md`, and marked `comparable`.

1. Overall complexity risk index
- overall complexity risk index is `4.8/10`, driven by materially larger runtime scope than baseline but tempered by the sharp contraction in hotspot count and concentration.

2. Fastest growing concept families
- against the April 13 baseline, the main growth family is still the SQL/predicate/fingerprint/storage-key decomposition line, but the latest rerun shows that family no longer dominates hotspot concentration after the local cleanup pass.

3. Highest branch multipliers
- the strongest current branch-pressure anchors are `types::decimal = 68`, `db::executor::aggregate::contracts::state = 53`, `db::data::structural_field::value_storage = 46`, `value = 41`, and `db::access::canonical = 40`.

4. Branch distribution drift (`AccessPath` / `RouteShape`)
- no new fanout-led route drift appeared; instead, the current rerun shows hotspot contraction in the earlier SQL/parser/fingerprint family while total branch sites stayed nearly flat.

5. Flow multiplication risks (axis-based)
- recent parser and projection cleanup reduced mixed-owner control flow in the `0.88` SQL line; the remaining flow multiplication risk is now more concentrated in aggregate contracts, value storage, and canonical access planning.

6. Semantic authority vs execution spread risks
- authority anchors remain stable at `AccessPath = 2`, `RouteShape = 3`, and predicate coercion `= 4`, with `0` cross-layer policy re-derivations.

7. Ownership drift + fanout pressure
- fanout pressure remains low (`0` modules above `fanout > 12`), and top-10 fanout concentration improved `0.0572 -> 0.0520` even while total fanout rose `+123`.

8. Super-node + call-depth warnings
- no fanout super-node emerged, but modules with `max_branch_depth >= 3` increased `21 -> 26`, so deep local control flow still exists even though hotspot concentration improved.

9. Trend-interpretation filter outcomes
- concentration improvements are now strong enough to matter operationally: the system is broader than baseline, but the cleanup pass reduced hotspot count from `11` to `5` and improved top-10 branch concentration to `0.1203`.

10. Complexity trend table
- against the latest comparable baseline, the system is larger and slightly more branch-heavy overall, but it is materially less top-heavy than the earlier rerun and no longer reads as broad redistribution under decomposition.

11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)
- runtime metrics generation passed, both architecture invariant checks passed, and `cargo check -p icydb-core` passed.

## Verification Readout

- `scripts/audit/runtime_metrics.sh docs/audits/reports/2026-04/2026-04-17/artifacts/complexity-accretion/runtime-metrics.tsv` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
