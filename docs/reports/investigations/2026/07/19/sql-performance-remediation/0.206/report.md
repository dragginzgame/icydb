# 0.206 SQL Performance Remediation Investigation

Status: Slice 1 opening review complete; selection awaits the typed retained-candidate rerun.

## Decision

The accepted post-0.205 cohort freezes a 14-family leading set. It does not yet
freeze an `OptimizeNow` selection. The best evidenced candidate is
`scale.user.unsupported_order.all.window10`: at 2,048 rows it costs 104,312,631
instructions, including 35,243,583 in the kernel order-window phase, and the
current source retains the complete qualifying row set before applying the
bounded expression order. The opening artifact did not transport the exact
peak retained-candidate count required by the 0.206 design, so selecting it
from instruction evidence alone would violate the design.

The Slice 1 measurement patch adds only
`kernel_row_peak_retained_candidates` at the canonical kernel scan collector.
It changes neither semantics nor dispatch. Selection freezes only after a clean
current-source run proves the exact opening value and raw Wasm identity. The
expected structural observation is 2,048 retained candidates for the
2,048-row expression-order sentinel; that expectation is not treated as
measured evidence until the rerun records it.

No directly coupled family is proposed. The leading incompatible-filter/order
family already uses direct-field bounded kernel collection and does not share
the missing expression-order key materialization invariant.

## Evidence Identity

The opening evidence is the accepted exact 0.205.2 closeout cohort. Raw bundles
remain workflow artifacts and are not copied into the repository.

| Role | Run ID | Source revision | Profile / environment identity | Artifact | SHA-256 |
| --- | --- | --- | --- | --- | --- |
| opening P2 | `29691074553` | `8bba5367e58a9fffc39754cdb0b117c768cb87e1` | profile 1; P1 `a6823a84`; P2 `306c6bd3`; accepted snapshot `04c1f5d6` | `sql_perf_p2_report.json` | `19bf38762537b5699c0d535b410ac4b2f0e57525502f947e10fe35ca86f0dc10` |
| opening scale | `29691074553` | `8bba5367e58a9fffc39754cdb0b117c768cb87e1` | profile 1; scale `afa7c342`; fixture `66ae745f` | `sql_perf_scale_report.json` | `94b33a88f03cfdcee4f157003ebf9126eb734ddd6b4afc95b576f0a7a8f0d664` |
| opening P1 | `29691074553` | `8bba5367e58a9fffc39754cdb0b117c768cb87e1` | profile 1; P1 `a6823a84`; accepted snapshot `04c1f5d6` | `sql_perf_deterministic_matrix.json` | `f7f747ac26b4694aaf350d9606597c971d04a2015187083373709eac7cde3474` |
| cohort review | `29693321199` | `8bba5367e58a9fffc39754cdb0b117c768cb87e1` | cohort `0.205.2-8bba5367e5-closeout`; three accepted ordinals | `sql_perf_calibration_review.json` | `43dcb041a58442ca9e2738fb5d2e52f9c7b26605ee1c944a0f95b69baf2b82a7` |

The opening subject is raw Wasm SHA-256
`e40cf2756a9b714d232eff6a488b81db6939a0a3ed882863f82716e0b91008fb`,
3,915,460 non-gzipped bytes. The environment uses Rust 1.97.0, PocketIC
14.0.0, `wasm-release`, and the `diagnostics`, `sql`, and `sql-explain`
features. All three cohort ordinals produced the same P2 selection hash and
the calibration review accepted the cohort.

The measurement rerun will be appended as a new immutable reference. It uses
diagnostics-attribution schema 2 because the retained-candidate field changes
the typed artifact shape; there is no schema-1 compatibility decoder. It will
not overwrite these opening facts.

## Deterministic Leading Set

Families are deduplicated after taking the top five for each design-owned
ranking. The normalized ranking deliberately retains its declared denominator;
scores with different denominators are not interpreted as interchangeable
efficiency ratios.

### Total Instructions At 2,048 Rows

| Rank | Family | Instructions |
| ---: | --- | ---: |
| 1 | `scale.user.grouped_hash.many_groups.having_sum.window16` | 129,266,789 |
| 2 | `scale.user.grouped_hash.many_groups.sum.window16` | 128,790,464 |
| 3 | `scale.user.unsupported_order.all.window10` | 104,312,631 |
| 4 | `scale.user.grouped_hash.few_groups.distinct_nat.window16` | 83,539,111 |
| 5 | `scale.user.grouped_ordered.many_groups.count.window100` | 81,216,832 |

### Marginal Instructions Per Row, 256 To 2,048

| Rank | Family | Instructions / added row |
| ---: | --- | ---: |
| 1 | `scale.user.grouped_hash.many_groups.having_sum.window16` | 61,754.08 |
| 2 | `scale.user.grouped_hash.many_groups.sum.window16` | 61,698.99 |
| 3 | `scale.user.unsupported_order.all.window10` | 50,820.34 |
| 4 | `scale.user.grouped_hash.few_groups.distinct_nat.window16` | 40,526.20 |
| 5 | `scale.user.grouped_ordered.few_groups.count.window10` | 39,099.85 |

### Maintained Normalized Cost

| Rank | Family | Denominator | Instructions / unit |
| ---: | --- | --- | ---: |
| 1 | `scale.user.not_paginated.aggregate_quarter` | returned row | 68,123,194.00 |
| 2 | `scale.user.grouped_hash.many_groups.having_sum.window16` | index range scan | 64,633,394.50 |
| 3 | `scale.user.grouped_hash.many_groups.sum.window16` | index range scan | 64,395,232.00 |
| 4 | `scale.user.grouped_hash.few_groups.sum.window1` | returned row | 56,002,042.00 |
| 5 | `scale.user.incompatible_filter_order.quarter.window10` | index range scan | 18,324,783.00 |

### Absolute Cold Compile Plus Planner

| Rank | Family | Scenario | Instructions |
| ---: | --- | --- | ---: |
| 1 | `route.sparse_in.page_only` | `token.collection_id.sparse_in.page_only.limit50` | 9,628,941 |
| 2 | `route.sparse_in.count` | `token.collection_id.sparse_in.count` | 3,540,108 |
| 3 | `route.branch_over_cap_pruned.page_only` | `token.collection_stage_id.overcap_pruned.page_only.limit50` | 3,437,306 |
| 4 | `route.branch_set.wide_noncovered_page_only` | `token.collection_stage_id.branch_set.wide_noncovered_page_only.limit50` | 2,079,325 |
| 5 | `route.branch_set.wide_page_only` | `token.collection_stage_id.branch_set.wide_page_only.limit50` | 2,059,361 |

## Typed Classification Before Source Inspection

| ID | Family | Typed evidence | Initial classification |
| --- | --- | --- | --- |
| 206-001 | grouped hash HAVING SUM | 2,048 rows, 100 groups/states, 2 range scans, 4,096 index entries, no early stop | hash grouping is required, but the duplicate one-sided range traversal is suspicious and lacks child-level instruction attribution |
| 206-002 | grouped hash SUM | 2,048 rows, 100 groups/states, 2 range scans, 4,096 index entries, no early stop | same mixed evidence as 206-001; the artifacts do not isolate instructions spent by the duplicate traversal |
| 206-003 | unsupported expression order | 2,048 gets, 35,243,583 order-window instructions, 10 rows returned | avoidable candidate retention suspected; peak fact missing |
| 206-004 | grouped hash DISTINCT | 2,048 rows, 5 groups/states and 5 peak distinct values | required grouped DISTINCT state under current route |
| 206-005 | ordered many-group COUNT | 2,048 rows, 100 groups finalized, peak one group/state | full result consumes every group; ordered state already bounded |
| 206-006 | ordered few-group COUNT | 2,048 rows, 5 groups finalized, peak one group/state | full result consumes every row; ordered state already bounded |
| 206-007 | scalar COUNT over `active` | 2,048 gets, one result, scalar attribution absent | schema-dependent scan; reducer localization unmeasured |
| 206-008 | grouped hash few-group SUM | 2,048 rows, 4 groups/states, no early stop | required hash grouping work under current route |
| 206-009 | incompatible filter/order | 512 index entries and gets, two retained slots, 10 rows | route mismatch requires candidate scan without a compatible index |
| 206-010 | sparse `IN` page | 2,132,345 compile + 7,496,596 planner | cold work is large; canonicalization/pass count absent |
| 206-011 | sparse `IN` count | 2,304,221 compile + 1,235,887 planner | cold work is large; canonicalization/pass count absent |
| 206-012 | pruned over-cap branch page | 1,340,706 compile + 2,096,600 planner | pruning cost is large; per-member/pass count absent |
| 206-013 | wide non-covering branch page | 543,842 compile + 1,535,483 planner | branch planning cost is large; structural pass count absent |
| 206-014 | wide covering branch page | 536,992 compile + 1,522,369 planner | branch planning cost is large; structural pass count absent |

## Source Ownership And Disposition

Source inspection was performed only after the table above was frozen.

| ID | Current owner | Source finding | Disposition |
| --- | --- | --- | --- |
| 206-001 | AND-range planner | `range::extract::index_range_from_and` requires every AND child to be a literal comparison, so the residual `name = name` child prevents lower/upper range fusion; `predicate::plan_predicate` recursively produces two one-sided range children and the grouped stream traverses both before the required hash fold | `NeedsTypedMeasurement` |
| 206-002 | AND-range planner | same planner shape and symbols as 206-001; HAVING is not the material difference, and current phase attribution does not isolate the instruction cost of either child traversal | `NeedsTypedMeasurement` |
| 206-003 | scalar page order collector | `terminal/page/plan.rs`, `scan.rs`, and `executor/order.rs` bound direct-field order collection, but expression order is collected in full before cached key selection | `NeedsTypedMeasurement` |
| 206-004 | grouped aggregate bundle | `aggregate/runtime/grouped_fold/bundle.rs` owns per-group DISTINCT state; evidence matches five live distinct values | `ContractRequired` |
| 206-005 | ordered grouped COUNT fold | `aggregate/runtime/grouped_fold/count/mod.rs` already retains one group; LIMIT 100 consumes the complete 100-group result | `ContractRequired` |
| 206-006 | ordered grouped COUNT fold | same owner; all five groups and all rows are needed to finalize exact counts | `ContractRequired` |
| 206-007 | application schema | `schema/audit/sql_perf/src/sql_perf.rs` has no `active` index; engine route upgrades or hidden indexes are forbidden | `SchemaDependent` |
| 206-008 | grouped generic fold | hash mode has four required groups and no order proof for early closure | `ContractRequired` |
| 206-009 | access planner plus application schema | the existing `age,id` and `name` indexes cannot jointly satisfy age filtering and name ordering | `SchemaDependent` |
| 206-010 | prefix access planner | `query/plan/planner/prefix.rs` owns sparse membership access construction, but existing evidence cannot count repeated member work | `NeedsTypedMeasurement` |
| 206-011 | SQL predicate lowering | `sql/lowering/select/binding.rs` and predicate normalization own the compile-heavy membership form; exact repeated work is not measured | `NeedsTypedMeasurement` |
| 206-012 | prefix access planner | `query/plan/planner/prefix.rs` owns exclusion pruning and branch-cap admission; exact passes are not measured | `NeedsTypedMeasurement` |
| 206-013 | prefix access planner | branch construction is shared with 206-014, but projection coverage is a separate downstream fact and no eliminated-work counter exists | `NeedsTypedMeasurement` |
| 206-014 | prefix access planner | same planning owner as 206-013; current evidence supplies instructions only | `NeedsTypedMeasurement` |

`SchemaDependent` does not recommend changing the benchmark fixture. It states
that the required engine work is removable only through an application-declared
compatible index or a different query. Adding an index to claim an engine win
would violate 0.206.

The grouped hash state in 206-001 and 206-002 is contract-required, but their
complete current cost is not. Existing storage counters prove two full index
traversals and 4,096 index-entry reads for 2,048 input rows. Source inspection
explains the shape, but the opening artifact does not attribute instructions to
the individual access children. The findings therefore remain
`NeedsTypedMeasurement`; they are not relabeled `ContractRequired` merely
because the downstream hash fold is semantically necessary, and they cannot
outrank 206-003 under the design's typed avoidable-phase scoring rule.

## Pending Selection Budget

If the measurement rerun records the expected structural fact, Slice 1 will
freeze 206-003 as the sole primary `OptimizeNow` finding with this budget:

- family: `scale.user.unsupported_order.all.window10`;
- primary scenario: `scale.user.unsupported_order.all.window10.rows2048`;
- guard cardinalities: 16, 256, and 2,048 rows;
- mode: isolated cold scale execution;
- structural metric: `kernel_row_peak_retained_candidates`;
- opening value: supplied by the measurement rerun, expected 2,048;
- required target: at most `offset + limit + lookahead`, which is 11 for the
  primary scenario;
- sampling variance: zero for the structural metric;
- instruction guardrails: total and kernel order-window medians must not
  increase; the changed family must improve outside the checked-in ordinary
  regression threshold;
- semantic guards: identical result signature, route facts, order direction,
  null and tie behavior, cursor/offset behavior, corruption failures, cache
  identity, scan bounds, and output cardinality;
- resource guards: existing scan/group/output/instruction limits remain in
  force; no byte or heap improvement is claimed without typed evidence.

If the rerun does not record the expected peak, 206-003 remains
`NeedsTypedMeasurement` and the selection is not silently substituted.

## Remaining Risks

- The counter transport changes the diagnostics/performance artifact shape and
  therefore requires a new exact raw-Wasm and environment identity before
  selection.
- `KernelRowAttribution` is public only under the diagnostics feature. The new
  field is retained as a maintained exact regression metric in the checked-in
  performance baseline rather than as slice-only optimization proof; the
  ordinary non-diagnostics query API is unchanged.
- Expression-order bounded collection must cache each expression key once per
  input row and compare the complete deterministic order, including the primary
  key tie-break. A direct-field-only heap is not a valid replacement.
- The full scan remains contract-required. The proposed target removes retained
  candidate state; it does not claim index pushdown or early scan stop.
- Peak retained candidates are not peak heap bytes. No memory-byte claim is
  authorized by this counter.

## Validation State

The opening 0.205.2 cohort and strict review passed. The local measurement
transport has focused compilation and unit coverage; its required Wasm-scale
measurement run is pending publication of the slice. Full repository tests
remain user-owned under repository policy.
