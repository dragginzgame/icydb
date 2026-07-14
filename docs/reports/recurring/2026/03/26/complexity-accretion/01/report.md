# Complexity Accretion Audit - 2026-03-26

## Report Preamble

- scope: conceptual growth, branch pressure, flow multiplication, and authority spread in `crates/icydb-core/src` runtime modules (non-test)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/complexity-accretion.md`
- code snapshot identifier: `a956de44`
- method tag/version: `CA-1.3`
- method manifest:
  - `method_version = CA-1.3`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`
- comparability status: `non-comparable` (the 2026-03-24 report used the fallback extractor because the canonical generator was missing in that checkout; this run uses the restored canonical `scripts/audit/runtime_metrics.sh`, so generator-derived deltas are `N/A (method change)`)

## Method Changes

- the canonical runtime metrics generator is present and was used directly in this run
- the prior baseline report was produced under the fallback extractor path
- generator-derived totals, concentrations, and branch-site comparisons are therefore `N/A (method change)` against the 2026-03-24 report, while owner-count anchors from the layer check remain comparable

## Evidence Artifacts

- `docs/audits/reports/2026-03/2026-03-26/artifacts/complexity-accretion/runtime-metrics.tsv`

## STEP -1 — Runtime Module Enumeration

Evidence mode: `mechanical`

- full runtime dataset (`483` modules):
  - `docs/audits/reports/2026-03/2026-03-26/artifacts/complexity-accretion/runtime-metrics.tsv`

Top branch-site modules from the required enumeration table:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |
| `types::decimal` | `crates/icydb-core/src/types/decimal.rs` | 842 | 4 | 18 | 4.50 | 66 | 65 | 2 | 2 | 69 |
| `db::query::plan::access_choice` | `crates/icydb-core/src/db/query/plan/access_choice/mod.rs` | 754 | 14 | 68 | 4.86 | 51 | 51 | 3 | 3 | 65 |
| `db::predicate::runtime` | `crates/icydb-core/src/db/predicate/runtime.rs` | 983 | 41 | 169 | 4.12 | 23 | 23 | 3 | 3 | 64 |
| `db::executor::explain::descriptor` | `crates/icydb-core/src/db/executor/explain/descriptor.rs` | 997 | 21 | 78 | 3.71 | 41 | 39 | 2 | 3 | 60 |
| `db::sql::parser` | `crates/icydb-core/src/db/sql/parser/mod.rs` | 555 | 3 | 15 | 5.00 | 50 | 47 | 2 | 2 | 50 |
| `db::sql::lowering` | `crates/icydb-core/src/db/sql/lowering/mod.rs` | 988 | 21 | 73 | 3.48 | 26 | 26 | 2 | 2 | 47 |
| `db::access::canonical` | `crates/icydb-core/src/db/access/canonical.rs` | 395 | 9 | 36 | 4.00 | 32 | 32 | 2 | 2 | 41 |
| `db::data::persisted_row` | `crates/icydb-core/src/db/data/persisted_row.rs` | 1522 | 23 | 121 | 5.26 | 17 | 17 | 2 | 7 | 40 |
| `value` | `crates/icydb-core/src/value/mod.rs` | 730 | 19 | 86 | 4.53 | 21 | 21 | 2 | 3 | 40 |
| `db::reduced_sql` | `crates/icydb-core/src/db/reduced_sql/mod.rs` | 609 | 9 | 149 | 16.56 | 29 | 28 | 2 | 3 | 37 |

## STEP 0 — Baseline Capture

Evidence mode: `semi-mechanical`

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | 485 | 483 | N/A (method change) |
| Runtime LOC | `[M]` | primary | 92,096 | 71,835 | N/A (method change) |
| Runtime fanout (sum) | `[M]` | primary | 155 | 973 | N/A (method change) |
| Modules with fanout > 12 | `[D]` | primary | 0 | 0 | N/A (method change) |
| Super-nodes (`fanout > 20 OR domain_count >= 3`) | `[D]` | primary | 18 | N/A (canonical dataset does not emit `domain_count`) | N/A (method change) |
| AccessPath decision owners | `[C]` | primary | 3 | 3 | 0 |
| RouteShape decision owners | `[C]` | primary | 2 | 2 | 0 |
| Predicate coercion decision owners | `[C]` | primary | 4 | 4 | 0 |
| Cross-layer policy re-derivations | `[M]` | primary | 0 | 0 | 0 |
| Comparator definitions outside index | `[M]` | primary | 0 | 0 | 0 |

## Current Complexity Signals

- Branch pressure remains concentrated rather than broad:
  - top-10 branch-site concentration from the canonical dataset is `0.2027`
  - modules with `branch_sites_total >= 40`: `9`
  - modules with `max_branch_depth >= 3`: `18`
- Authority ownership stayed flat on the key audit anchors:
  - `AccessPath` decision owners: `3`
  - `RouteShape` decision owners: `2`
  - predicate coercion owners: `4`
- The main current hotspots are still the same conceptual families:
  - planner access choice
  - predicate runtime
  - explain descriptor
  - SQL lowering / parser
  - canonical row boundary work in `db::data::persisted_row`

## Structural Interpretation

- Complexity remains in the moderate band, but it is still concentrated in a small set of known hubs rather than diffusing across the workspace.
- `db::query::plan::access_choice` and `db::predicate::runtime` remain the most important runtime control-plane hotspots. Those are still the places where branch growth matters most.
- `db::data::persisted_row` is now visibly in the top branch-site tier, but the recent `0.65` invariant hardening and cleanup kept that complexity owner-local inside the persisted-row boundary rather than spreading it across projection, relation, or predicate consumers.
- Owner-count anchors are stable: this run does not show new semantic fragmentation even though the row boundary itself became stricter.

## Overall Complexity Risk Index

**4.9/10**

## Follow-Up Actions

- No mandatory follow-up actions for this run.
- Monitoring-only: keep `db::query::plan::access_choice`, `db::predicate::runtime`, and `db::data::persisted_row` from taking on new decision axes without equivalent owner-local consolidation.

## Verification Readout

- `scripts/audit/runtime_metrics.sh` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
