# Complexity Accretion Audit - 2026-05-10

## Report Preamble

| Field [M] | Value |
| ---- | ---- |
| `method_version` | `CA-1.4` |
| `completion_status` | `partial` |
| `risk_index_kind` | `mechanical-only` |
| `baseline_report` | `docs/audits/reports/2026-04/2026-04-30/complexity-accretion.md` |
| `comparability_status` | `non-comparable` |
| `missing_sections` | STEP 1, STEP 2, STEP 2A, STEP 3, STEP 4, STEP 4A, STEP 6 |

- scope: canonical complexity snapshot for `crates/icydb-core/src` runtime modules, excluding tests, benches, and examples
- code snapshot identifier: `0815883bc`
- dirty-tree caveat: the working tree includes the CA-1.4 audit-method edit, the `AGENTS.md` local-tooling clarification, and this audit output
- generator note: canonical `scripts/audit/runtime_metrics.sh` completed after `AGENTS.md` clarified that Codex-local Python is allowed for one-off audit extraction
- shell fallback note: earlier `*-shell.tsv` artifacts are retained as superseded context and ignored for conclusions
- method manifest:
  - `method_version = CA-1.4`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`

## Artifact Coverage

| Artifact [M] | Status [C] | Reason [C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| `runtime-metrics.tsv` | PASS | canonical generator completed | non-comparable with CA-1.3 |
| `module-branch-hotspots.tsv` | PASS | derived from canonical runtime metrics | non-comparable with CA-1.3 |
| `concentration-ratios.tsv` | PASS | derived from canonical runtime metrics | non-comparable with CA-1.3 |
| `risk-buckets.tsv` | PASS | mechanical-only signal from canonical metrics | partial run |
| `invalidating-signals.tsv` | PASS | invalidating signals recorded for CA-1.4 transition | non-comparable |
| `issue-ledger.tsv` | PASS | owner-scoped follow-up rows from canonical hotspots | partial run |
| `runtime-metrics-shell.tsv` | N/A | superseded fallback retained as context | ignored |
| `module-branch-hotspots-shell.tsv` | N/A | superseded fallback retained as context | ignored |
| `enum-surface.tsv` | BLOCKED | semi-mechanical enum extraction not run in this partial audit | partial run |
| `enum-switch-sites.tsv` | BLOCKED | semi-mechanical enum switch-site extraction not run in this partial audit | partial run |
| `function-branch-hotspots.tsv` | BLOCKED | function-level extraction not run in this partial audit | partial run |
| `concept-branch-summary.tsv` | BLOCKED | concept branch extraction not run in this partial audit | partial run |
| `concept-branch-map.tsv` | BLOCKED | concept branch map extraction not run in this partial audit | partial run |
| `flow-constraint-ledger.tsv` | BLOCKED | classified flow ledger not produced in this partial audit | partial run |
| `flow-counts.tsv` | BLOCKED | effective flow totals require the missing flow ledger | partial run |
| `semantic-spread.tsv` | BLOCKED | classified semantic spread table not produced in this partial audit | partial run |
| `ownership-drift.tsv` | BLOCKED | classified ownership drift table not produced in this partial audit | partial run |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP -1 | PASS canonical | `runtime-metrics.tsv` | non-comparable with CA-1.3 method tag |
| STEP 0 | PASS canonical | report table below | non-comparable with CA-1.3 method tag |
| STEP 1 | BLOCKED | `artifact-coverage.tsv` | partial run |
| STEP 2 | BLOCKED | `artifact-coverage.tsv` | partial run |
| STEP 2A | BLOCKED | `artifact-coverage.tsv` | partial run |
| STEP 3 | BLOCKED | `artifact-coverage.tsv` | partial run |
| STEP 4 | BLOCKED | `artifact-coverage.tsv` | partial run |
| STEP 4A | BLOCKED | `artifact-coverage.tsv` | partial run |
| STEP 4B | PASS canonical | `runtime-metrics.tsv` | non-comparable with CA-1.3 method tag |
| STEP 5 | PASS canonical | `runtime-metrics.tsv` | non-comparable with CA-1.3 method tag |
| STEP 5A | PASS canonical | `concentration-ratios.tsv` | non-comparable with CA-1.3 method tag |
| STEP 6 | BLOCKED | `artifact-coverage.tsv` | partial run |
| STEP 7 | PASS mechanical-only signal | `risk-buckets.tsv` | no overall index |
| STEP 8 | PASS | report body | non-comparable |
| STEP 8A | PASS canonical | report body | non-comparable |
| STEP 8B | PASS | `invalidating-signals.tsv` | non-comparable |
| STEP 9 | PASS | `issue-ledger.tsv` | partial run |

## STEP -1 - Runtime Module Enumeration

Evidence mode: `mechanical`, canonical generator.

The canonical generator completed and produced
`docs/audits/reports/2026-05/2026-05-10/artifacts/complexity-accretion/runtime-metrics.tsv`.
This run is still non-comparable with the prior CA-1.3 report because CA-1.4
adds completion gates and explicit artifact coverage rules.

Top branch-site modules from canonical metrics:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |
| `db::data::persisted_row::contract` | `crates/icydb-core/src/db/data/persisted_row/contract.rs` | 841 | 23 | 66 | 2.87 | 23 | 23 | 4 | 5 | 46 |
| `metrics::sink` | `crates/icydb-core/src/metrics/sink.rs` | 1991 | 43 | 236 | 5.49 | 2 | 2 | 2 | 3 | 45 |
| `db::query::plan::access_choice::evaluator::range` | `crates/icydb-core/src/db/query/plan/access_choice/evaluator/range.rs` | 407 | 10 | 26 | 2.60 | 34 | 34 | 3 | 2 | 44 |
| `db::access::canonical` | `crates/icydb-core/src/db/access/canonical.rs` | 361 | 10 | 39 | 3.90 | 30 | 30 | 2 | 3 | 40 |
| `db::data::persisted_row::reader::core` | `crates/icydb-core/src/db/data/persisted_row/reader/core.rs` | 583 | 9 | 25 | 2.78 | 28 | 28 | 4 | 4 | 37 |

## STEP 0 - Baseline Capture

Evidence mode: `semi-mechanical`.

Prior comparable baseline is unavailable for CA-1.4 because the method version
changed. Treat this canonical generator-backed snapshot as a candidate CA-1.4
mechanical baseline, not as a continuation of the CA-1.3 trend line.

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | N/A | 779 | N/A |
| Runtime LOC | `[M]` | primary | N/A | 141,121 | N/A |
| Runtime fanout (sum) | `[M]` | primary | N/A | 1,546 | N/A |
| Modules with fanout > 12 | `[D]` | primary | N/A | 0 | N/A |
| Super-nodes (`fanout > 20 OR domain_count >= 3`) | `[D]` | primary | N/A | N/A | N/A |
| Continuation decision owners | `[C]` | primary | N/A | N/A | N/A |
| Continuation execution consumers | `[C]` | primary | N/A | N/A | N/A |
| Continuation plumbing modules | `[C]` | primary | N/A | N/A | N/A |
| AccessPath decision owners | `[C]` | primary | N/A | N/A | N/A |
| AccessPath executor dispatch sites | `[M]` | primary | N/A | N/A | N/A |
| AccessPath branch modules | `[M]` | primary | N/A | N/A | N/A |
| RouteShape decision owners | `[C]` | primary | N/A | N/A | N/A |
| RouteShape branch modules | `[M]` | primary | N/A | N/A | N/A |
| Predicate coercion decision owners | `[C]` | primary | N/A | N/A | N/A |
| Continuation mentions (context only) | `[M]` | weak | N/A | N/A | N/A |
| Total branch sites | `[M]` | primary | N/A | 4,777 | N/A |
| Modules with `branch_sites_total >= 40` | `[D]` | primary | N/A | 4 | N/A |

## Mechanical-Only Signals

- The canonical generator found `779` runtime files and `141,121` runtime LOC.
- Total canonical branch sites are `4,777`.
- Four modules are at or above the `branch_sites_total >= 40` hotspot threshold.
- Fanout pressure is low under the canonical extractor: `0` modules above
  `fanout > 12`, with total fanout `1,546`.
- Top-10 branch concentration is `0.0808`; top-10 fanout concentration is
  `0.0408`. These are candidate CA-1.4 baseline values, not CA-1.3 trend data.

## STEP 5A - Complexity Concentration Ratios

Evidence mode: `mechanical`, canonical generator.

| Metric [M] | Current [D] | Previous [D] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| Fanout concentration (top 10 modules) | 0.0408 | N/A | N/A | non-comparable CA-1.4 baseline candidate |
| Branch-site concentration (top 10 modules) | 0.0808 | N/A | N/A | non-comparable CA-1.4 baseline candidate |
| AccessPath branch concentration (top 3 modules) | N/A | N/A | N/A | blocked |
| RouteShape branch concentration (top 3 modules) | N/A | N/A | N/A | blocked |

## STEP 7 - Complexity Risk Index

Evidence mode: `semi-mechanical`.

No overall complexity risk index is published. This is a `mechanical-only`
partial run because classified CA-1.4 sections are blocked. The current signal
is:

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |
| Branching pressure mechanical signal | N/A | N/A | N/A |
| Fanout pressure mechanical signal | N/A | N/A | N/A |
| Concentration mechanical signal | N/A | N/A | N/A |
| Overall complexity risk index | N/A | N/A | N/A |

## STEP 8 - Trend Interpretation Filter

Evidence mode: `semi-mechanical`.

| Signal [M/C] | Raw Trend [M/D] | Filter Result [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| Method changed to CA-1.4 | CA-1.3 -> CA-1.4 | non-comparable | start a new completion-gated baseline from canonical generator output |
| Canonical generator completed | `runtime-metrics.tsv` produced | baseline candidate | mechanical values can anchor CA-1.4, but not CA-1.3 trend |
| Branch hotspots from canonical generator | 4 modules >= 40 branch sites | watch list | use as owner-boundary cue and future CA-1.4 trend seed |
| Fanout threshold | 0 modules > 12 | low fanout pressure | no current generator-backed hub-sprawl signal |

## STEP 8A - Complexity Trend Table

Evidence mode: `mechanical`.

No comparable time-series table is published for this CA-1.4 partial run. The
canonical generator-backed snapshot can serve as the first CA-1.4 mechanical
baseline candidate if accepted.

## STEP 8B - Invalidating Signals

Evidence mode: `classified`.

| Signal [M/C] | Present? [C] | Expected Distortion [C] | Handling Rule [C] |
| ---- | ---- | ---- | ---- |
| large module moves | unknown | cannot classify without full CA-1.4 artifacts | mark classified steps blocked |
| file splits without semantic change | unknown | cannot classify in this partial run | no trend score |
| generated code expansion | unknown | not classified in this partial run | no trend score |
| parser/table-driven conversion replacing branch expressions | unknown | branch counts may fall without semantic simplification | no trend score |
| branch consolidation into helper functions | unknown | hotspot movement may look like improvement | no trend score |
| method version changed | yes | CA-1.4 changes completion and artifact rules | no CA-1.3 comparison |

## STEP 9 - Issue Ledger

Evidence mode: `classified` from canonical hotspots.

| Finding [C] | Anchor Metric [M/D] | Owner Boundary [C] | Trigger Threshold [M/D] | Action [C] | Next Check [M/C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| persisted row contract is the top canonical branch hotspot | `branch_sites_total = 46` | `db::data::persisted_row::contract` | `branch_sites_total >= 40` | keep accepted/generated contract cleanup localized; avoid adding new branch families without splitting by contract concern | next comparable CA run |
| access range evaluator remains a recurring branch hotspot | `branch_sites_total = 44` | `db::query::plan::access_choice::evaluator::range` | `branch_sites_total >= 40` | keep accepted-index authority work from adding new range-shape branches outside the range evaluator boundary | next comparable CA run |
| access canonical remains at the branch hotspot threshold | `branch_sites_total = 40` | `db::access::canonical` | `branch_sites_total >= 40` | route new canonical access-shape rules through existing helpers rather than duplicating checks in planner or executor layers | next comparable CA run |

## Required Summary

0. Run metadata + comparability note
- `CA-1.4` partial run on `0815883bc`, marked `non-comparable` because prior reports use the CA-1.3 method tag. The canonical runtime generator completed and produced the source dataset for mechanical metrics.

1. Overall complexity risk index
- no overall complexity risk index is published; the only current scoreable signal is mechanical-only context: `4,777` canonical branch sites, `4` canonical hotspots, and `0` modules with fanout > `12`.

2. Fastest growing concept families
- blocked; no comparable concept-family trend was produced.

3. Highest branch multipliers
- blocked; enum surface and switch-site artifacts were not produced.

4. Branch distribution drift (`AccessPath` / `RouteShape`)
- blocked; concept branch maps were not produced.

5. Flow multiplication risks (axis-based)
- blocked; no constraint ledger was produced, so no effective-flow totals are published.

6. Semantic authority vs execution spread risks
- blocked; role-aware semantic spread table was not produced.

7. Ownership drift + fanout pressure
- ownership drift is blocked; canonical fanout pressure is low with `0` modules above `fanout > 12`.

8. Super-node + call-depth warnings
- super-node/domain-count and call-depth checks are blocked; canonical metrics identify `4` module-level branch hotspots.

9. Trend-interpretation filter outcomes
- method-version change forces `non-comparable`; canonical mechanical metrics are a candidate CA-1.4 baseline, not a CA-1.3 trend continuation.

10. Complexity trend table
- blocked for comparable trend; this run should not be inserted into the CA-1.3 trend line.

11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)
- canonical runtime artifact generation passed; module hotspot derivation passed; enum, function, concept, flow, semantic-spread, and ownership-drift sections are blocked.

12. Issue ledger summary
- three owner-scoped follow-ups were recorded for `db::data::persisted_row::contract`, `db::query::plan::access_choice::evaluator::range`, and `db::access::canonical`.

## Verification Readout

- canonical `scripts/audit/runtime_metrics.sh docs/audits/reports/2026-05/2026-05-10/artifacts/complexity-accretion/runtime-metrics.tsv` -> PASS
- canonical module hotspot artifact -> PASS
- superseded shell fallback artifacts -> ignored
