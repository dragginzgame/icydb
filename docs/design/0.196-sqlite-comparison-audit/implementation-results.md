# 0.196 Implementation Results

Date: 2026-07-04

## Summary

The 0.196 slice now has full-matrix evidence rather than focused-only evidence.
The saved-report delta passed the closeout gate with no new failures, no
resolved failures, and no result or cursor signature changes.

The production changes are narrow:

- selected index-range lower/upper bounds are stripped from residual predicates
  when the chosen access path already proves them;
- exact primary-key public reads may use the key-count upper bound instead of a
  redundant `LIMIT`;
- route classification, structured limit-stop attribution, signatures, and full
  union-key delta artifacts are emitted by the matrix harness.

## Artifacts

- Before full matrix:
  `/tmp/icydb-196-current-full/sql_perf_196_current_full_matrix.json`
- Before full matrix Markdown:
  `/tmp/icydb-196-current-full/sql_perf_196_current_full_matrix.md`
- After full matrix:
  `/tmp/icydb-196-after-full/sql_perf_196_after_full_matrix.json`
- After full matrix Markdown:
  `/tmp/icydb-196-after-full/sql_perf_196_after_full_matrix.md`
- Delta JSON:
  `/tmp/icydb-196-after-full/sql_perf_196_full_matrix_delta.json`
- Delta Markdown:
  `/tmp/icydb-196-after-full/sql_perf_196_full_matrix_delta.md`

The raw JSON files are about 15 MB each and are intentionally kept under `/tmp`
unless release packaging wants to archive them separately. These scratch paths
are not durable; regenerate and archive fresh full-matrix artifacts before using
them as release attachments.

## Order-Hint Artifact Refresh

After adding `order_by_idx_hint` to execution diagnostics and matrix samples, a
fresh deterministic full matrix was captured at:

- `/tmp/icydb-196-after-order-hint/sql_perf_196_after_order_hint_full_matrix.json`
- `/tmp/icydb-196-after-order-hint/sql_perf_196_after_order_hint_full_matrix.md`

The saved-report delta against the prior after matrix was captured at:

- `/tmp/icydb-196-after-order-hint/sql_perf_196_order_hint_delta.json`
- `/tmp/icydb-196-after-order-hint/sql_perf_196_order_hint_delta.md`

That refresh passed the closeout gate with 1,756 union scenarios, 1,675 common
successful scenarios, 81 common failures, 0 new failures, 0 resolved failures,
and 0 result or cursor signature changes. It populated order-hint transitions
for 1,663 scenarios so the full matrix delta now exposes the requested
deterministic ordering shape beside each route-family/outcome transition.

## Blob Classifier Hardening

After the order-hint refresh, the matrix classifier was tightened for Blob
`ORDER BY bucket, id` scenarios. The Blob ordered metadata index is
`(bucket, label, id)`, so those routes now classify as `secondary_order` /
`missing_tie_breaker` with reason `index_order_suffix_gap` instead of looking
like generic eligible-but-unpushed secondary-order routes. Blob
`ORDER BY bucket, label, id` scenarios remain pushable.

This is report attribution only; it does not change production execution,
cursor, cache, persisted format, or public API behavior. The full matrix was not
rerun after this classifier-only hardening.

## Limit-Stop Attribution

Matrix samples now emit `limit_stop_after` as a structured proof object. Pushed
routes record the returned limit, lookahead, observed returned matches, and
index entries. Non-pushed routes record the same limit/lookahead context plus a
stable disabled reason such as `index_order_suffix_gap`, `no_order_by`, or the
route reason. Delta Markdown also prints the before/after limit-stop transition.

This is report attribution only; it does not change production execution,
cursor, cache, persisted format, or public API behavior. The full matrix was not
rerun after this matrix-schema hardening.

## Verbose Limit-Stop Diagnostic

Verbose EXPLAIN route diagnostics now emit `diag.r.limit_stop_after`. Directly
streaming bounded ordered routes report the planned limit, lookahead, and fetch
window, for example `possible(limit=3,lookahead=1,fetch=6)`. Routes that cannot
prove final limit-stop behavior report a stable disabled reason such as
`requires_materialized_sort`, `residual_filter_blocks_direct_streaming`,
`continuation_applied`, `no_limit`, or `no_bounded_fetch`.

This is explain attribution only; it does not change production execution,
cursor, cache, persisted format, or public API behavior.

## Canonical Descriptor Limit-Stop Property

Canonical execution descriptor roots now include a `limit_stop_after` node
property when the route has a bounded fetch proof. Direct streaming routes
record the same `possible(limit=...,lookahead=...,fetch=...)` label as verbose
EXPLAIN. Routes with a bounded candidate fetch but materialized final order
record the stable disabled reason, for example
`disabled(residual_filter_blocks_direct_streaming)`.

This is descriptor attribution only; it does not change production execution,
cursor, cache, persisted format, or public read-admission behavior.

Focused validation covered the new root property, the existing canonical EXPLAIN
suite, verbose EXPLAIN diagnostics, executor semantics snapshots, core clippy,
JSON validity, and whitespace checks. The full matrix was not rerun for this
descriptor-only projection.

## Canonical Descriptor Route Class Properties

Scalar-load canonical execution descriptor roots now include `route_family`,
`route_outcome`, and `route_reason` node properties. These labels are derived
from executor-owned route facts and the selected access path, not from SQL text
or matrix scenario names. They classify pushed ordered reads, residual
unbounded routes, materialized routes, post-access cursor routes, unsupported
access kinds, and not-ordered/not-paginated loads.

Grouped aggregate descriptors are intentionally excluded because they already
publish grouped-specific route fields. This avoids mixing scalar-load
route-family labels into the grouped route contract.

This is descriptor attribution only; it does not change production execution,
cursor, cache, persisted format, public API behavior, or public read-admission
behavior.

## Descriptor Route-Diagnostics Matrix Refresh

After adding scalar-load descriptor route-class properties, a fresh deterministic
full matrix was captured against a clean `0.196.3` baseline so the descriptor
diagnostic overhead and route-class coverage were measured instead of inferred.

- Baseline full matrix:
  `/tmp/icydb-196-matrix-baseline-report/sql_perf_196_3_baseline_full_matrix.json`
- Baseline full matrix Markdown:
  `/tmp/icydb-196-matrix-baseline-report/sql_perf_196_3_baseline_full_matrix.md`
- Current full matrix:
  `/tmp/icydb-196-matrix-current-report/sql_perf_196_current_route_diagnostics_full_matrix.json`
- Current full matrix Markdown:
  `/tmp/icydb-196-matrix-current-report/sql_perf_196_current_route_diagnostics_full_matrix.md`
- Delta JSON:
  `/tmp/icydb-196-matrix-delta/sql_perf_196_route_diagnostics_delta.json`
- Delta Markdown:
  `/tmp/icydb-196-matrix-delta/sql_perf_196_route_diagnostics_delta.md`

This refresh compares `0.196.3` to the current uncommitted descriptor
route-diagnostics slice. It is not evidence of a new runtime pushdown win; the
expected improvement and focused-target counts were both zero.

| Metric | Result |
| --- | ---: |
| Union scenarios | 1,756 |
| Common successful scenarios | 1,675 |
| Common failures | 81 |
| Improved scenarios | 192 |
| Regressed scenarios | 1,483 |
| New failures | 0 |
| Resolved failures | 0 |
| Result signature changes | 0 |
| Cursor signature changes | 0 |
| Route fact changes | 0 |
| `data_store.get` delta | 0 |
| Index range delta | 0 |
| Index entry delta | 0 |
| Rows returned delta | 0 |
| Aggregate total instructions | +4,381,279 / 4,817,348,069 baseline |
| Regression gate crossings (`>=10%` and `>=100k`) | 0 |
| Closeout gate | PASS |

The route-diagnostics slice therefore preserves result semantics, cursor
semantics, route decisions, and access counters. The aggregate instruction
movement is about +0.09%, which is attributable to descriptor/reporting changes
and measurement noise rather than route changes.

Route-family/outcome coverage for common successful scenarios:

| Route family/outcome | Scenarios | Total instruction delta |
| --- | ---: | ---: |
| `materialized_order` / `materialized` | 144 | +1,926,175 |
| `incompatible_filter_first_order` / `materialized` | 603 | +846,552 |
| `primary_order` / `pushed` | 397 | +718,061 |
| `primary_order` / `eligible_but_not_pushed` | 129 | +390,309 |
| `secondary_order` / `pushed` | 259 | +336,089 |
| `secondary_order` / `eligible_but_not_pushed` | 67 | +73,106 |
| `secondary_order` / `missing_tie_breaker` | 60 | +70,498 |
| `not_ordered_or_not_paginated` / `unchanged_or_not_applicable` | 13 | +14,633 |
| `equality_prefix_ordered_suffix` / `pushed` | 1 | +4,006 |
| `unsupported_access_kind` / `unsupported` | 2 | +1,850 |

## 0.196.5 Classifier Hardening Matrix Refresh

After the descriptor route-diagnostics refresh, the matrix classifier was
tightened again so residual candidate scans, materialized order windows, and
unsupported expression orders stop appearing as pushdown candidates. This is
diagnostic hardening only: no production execution, cursor, cache, persisted
format, public API, or public read-admission behavior changed.

- Current full matrix:
  `/tmp/icydb-196-5-classifier-report/sql_perf_196_5_classifier_full_matrix.json`
- Current full matrix Markdown:
  `/tmp/icydb-196-5-classifier-report/sql_perf_196_5_classifier_full_matrix.md`
- Delta JSON:
  `/tmp/icydb-196-5-classifier-delta/sql_perf_196_5_classifier_delta.json`
- Delta Markdown:
  `/tmp/icydb-196-5-classifier-delta/sql_perf_196_5_classifier_delta.md`

The refresh compares the previous descriptor route-diagnostics matrix to the
0.196.5 classifier-hardened matrix. It is not evidence of a new runtime
pushdown win; expected improvement and focused-target counts were both zero.

| Metric | Result |
| --- | ---: |
| Generated scenarios | 1,756 |
| Executed scenarios | 1,675 |
| Failed scenarios | 81 |
| Union scenarios | 1,756 |
| Common successful scenarios | 1,675 |
| Common failures | 81 |
| Improved scenarios | 456 |
| Regressed scenarios | 1,219 |
| New failures | 0 |
| Resolved failures | 0 |
| Result signature changes | 0 |
| Cursor signature changes | 0 |
| Route fact changes | 549 |
| Limit-stop attribution changes | 549 |
| `data_store.get` delta | 0 |
| Index range delta | 0 |
| Index entry delta | 0 |
| Rows returned delta | 0 |
| Aggregate total instructions | -1,704,672 / 4,821,729,348 baseline |
| Compile instruction delta | +881,042 |
| Execute instruction delta | -2,585,714 |
| Planner instruction delta | +1,894,802 |
| Executor instruction delta | -4,232,655 |
| Store instruction delta | -244,825 |
| Regression gate crossings (`>=10%` and `>=100k`) | 0 |
| Closeout gate | PASS |

The classifier now leaves only 12 `eligible_but_not_pushed` scenarios, all in
the secondary-order candidate family. It also corrects 228 prior
`primary_order` / `pushed` classifications to residual unbounded candidate
scans when the predicate cannot prove bounded admission.

Route-family/outcome coverage for the refreshed matrix:

| Route family/outcome/reason | Scenarios | Total instructions | `data_store.get` | Index ranges | Index entries | Rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `equality_prefix_ordered_suffix` / `pushed` / `equality_prefix_ordered_suffix_limit_stop_proven` | 1 | 2,476,324 | 0 | 1 | 50 | 50 |
| `incompatible_filter_first_order` / `materialized` / `filter_order_mismatch` | 498 | 679,339,040 | 2,089 | 651 | 2,210 | 1,220 |
| `materialized_order` / `materialized` / `requires_materialized_sort` | 114 | 150,007,784 | 570 | 78 | 354 | 291 |
| `materialized_order` / `materialized` / `storage_mirror_has_primary_index_only` | 144 | 2,826,027,305 | 73,728 | 0 | 0 | 672 |
| `not_ordered_or_not_paginated` / `unchanged_or_not_applicable` / `not_a_paginated_select` | 13 | 16,489,578 | 6 | 0 | 0 | 35 |
| `primary_order` / `pushed` / `primary_order_limit_stop_proven` | 169 | 212,133,202 | 758 | 32 | 1,016 | 1,069 |
| `residual_filter_ordered_scan` / `residual_unbounded` / `residual_filter_requires_candidate_scan` | 315 | 405,478,754 | 1,366 | 263 | 484 | 884 |
| `secondary_order` / `eligible_but_not_pushed` / `secondary_order_candidate` | 12 | 15,229,146 | 20 | 30 | 34 | 18 |
| `secondary_order` / `missing_tie_breaker` / `index_order_suffix_gap` | 60 | 82,052,969 | 225 | 45 | 135 | 135 |
| `secondary_order` / `pushed` / `secondary_order_limit_stop_proven` | 227 | 263,579,385 | 464 | 333 | 698 | 631 |
| `unsupported_access_kind` / `unsupported` / `order_expression_not_classified` | 122 | 167,211,189 | 612 | 90 | 240 | 314 |

Route-fact changes from the previous descriptor route-diagnostics matrix:

| Before | After | Scenarios |
| --- | --- | ---: |
| `incompatible_filter_first_order` / `materialized` / `filter_order_mismatch` | `unsupported_access_kind` / `unsupported` / `order_expression_not_classified` | 105 |
| `primary_order` / `eligible_but_not_pushed` / `primary_order_candidate` | `materialized_order` / `materialized` / `requires_materialized_sort` | 78 |
| `primary_order` / `eligible_but_not_pushed` / `primary_order_candidate` | `residual_filter_ordered_scan` / `residual_unbounded` / `residual_filter_requires_candidate_scan` | 15 |
| `primary_order` / `eligible_but_not_pushed` / `storage_mirror_primary_order_candidate` | `residual_filter_ordered_scan` / `residual_unbounded` / `residual_filter_requires_candidate_scan` | 36 |
| `primary_order` / `pushed` / `primary_order_limit_stop_proven` | `residual_filter_ordered_scan` / `residual_unbounded` / `residual_filter_requires_candidate_scan` | 228 |
| `secondary_order` / `eligible_but_not_pushed` / `secondary_order_candidate` | `materialized_order` / `materialized` / `requires_materialized_sort` | 36 |
| `secondary_order` / `eligible_but_not_pushed` / `secondary_order_candidate` | `residual_filter_ordered_scan` / `residual_unbounded` / `residual_filter_requires_candidate_scan` | 4 |
| `secondary_order` / `eligible_but_not_pushed` / `secondary_order_candidate` | `unsupported_access_kind` / `unsupported` / `order_expression_not_classified` | 15 |
| `secondary_order` / `pushed` / `secondary_order_limit_stop_proven` | `residual_filter_ordered_scan` / `residual_unbounded` / `residual_filter_requires_candidate_scan` | 32 |

## 0.196.0 To End-Of-196 Matrix Delta

A clean `v0.196.0` baseline was captured in a detached worktree and compared to
the current end-of-196 worktree state. This answers the line-level question:
what changed after the initial `0.196.0` release point.

- `0.196.0` baseline full matrix:
  `/tmp/icydb-196-line-baseline-report/sql_perf_196_0_full_matrix.json`
- `0.196.0` baseline full matrix Markdown:
  `/tmp/icydb-196-line-baseline-report/sql_perf_196_0_full_matrix.md`
- Current end-of-196 full matrix:
  `/tmp/icydb-196-5-classifier-report/sql_perf_196_5_classifier_full_matrix.json`
- Current end-of-196 full matrix Markdown:
  `/tmp/icydb-196-5-classifier-report/sql_perf_196_5_classifier_full_matrix.md`
- Line delta JSON:
  `/tmp/icydb-196-line-delta-1965/sql_perf_196_0_to_196_5_delta.json`
- Line delta Markdown:
  `/tmp/icydb-196-line-delta-1965/sql_perf_196_0_to_196_5_delta.md`

| Metric | Result |
| --- | ---: |
| Union scenarios | 1,756 |
| Common successful scenarios | 1,675 |
| Common failures | 81 |
| Improved scenarios | 114 |
| Regressed scenarios | 1,561 |
| New failures | 0 |
| Resolved failures | 0 |
| Result signature changes | 0 |
| Cursor signature changes | 0 |
| Route fact changes | 609 |
| Order-hint changes | 1,663 |
| Limit-stop attribution changes | 1,675 |
| `data_store.get` delta | 0 |
| Index range delta | 0 |
| Index entry delta | 0 |
| Rows returned delta | 0 |
| Aggregate total instructions | +5,692,977 / 4,814,331,699 baseline |
| Compile instruction delta | +3,710,200 |
| Execute instruction delta | +1,982,777 |
| Planner instruction delta | +3,785,732 |
| Executor instruction delta | -1,376,527 |
| Store instruction delta | -64,281 |
| Regression gate crossings (`>=10%` and `>=100k`) | 0 |
| Closeout gate | PASS |

The 609 route-fact changes include the earlier Blob `ORDER BY bucket, id`
suffix-gap reclassification plus the 0.196.5 classifier hardening that separates
materialized order windows, residual candidate scans, and unsupported expression
orders from true pushdown candidates. No row, cursor, access-counter, or result
signature changed.

The post-`0.196.0` line is therefore diagnostic hardening rather than a new
runtime optimisation: order hints became visible for 1,663 scenarios,
limit-stop proof attribution became explicit for all 1,675 common successful
scenarios, scalar-load descriptor route facts became visible, and the matrix now
leaves only 12 true `eligible_but_not_pushed` scenarios. The aggregate
instruction movement is about +0.12%, below the closeout regression gate.

Route-family/outcome deltas from `0.196.0` to the current end-of-196 state:

| Route family/outcome | Scenarios | Total instruction delta |
| --- | ---: | ---: |
| `incompatible_filter_first_order` / `materialized` | 498 | +2,505,118 |
| `residual_filter_ordered_scan` / `residual_unbounded` | 315 | +1,838,889 |
| `secondary_order` / `pushed` | 227 | +830,217 |
| `materialized_order` / `materialized` | 258 | -788,439 |
| `primary_order` / `pushed` | 169 | +520,933 |
| `unsupported_access_kind` / `unsupported` | 122 | +497,752 |
| `secondary_order` / `missing_tie_breaker` | 60 | +192,339 |
| `not_ordered_or_not_paginated` / `unchanged_or_not_applicable` | 13 | +52,745 |
| `secondary_order` / `eligible_but_not_pushed` | 12 | +39,007 |
| `equality_prefix_ordered_suffix` / `pushed` | 1 | +4,416 |

## Full Matrix Delta

| Metric | Result |
| --- | ---: |
| Union scenarios | 1,756 |
| Common successful scenarios | 1,675 |
| Improved scenarios | 699 |
| Regressed scenarios | 976 |
| Neutral scenarios | 0 |
| New failures | 0 |
| Resolved failures | 0 |
| Closeout gate | PASS |

The before and after runs used the same canister wasm profile: `debug`.

## Route Delta

| Route family/outcome | Scenarios | Total instruction delta |
| --- | ---: | ---: |
| `secondary_order` / `pushed` | 255 | -6,488,741 |
| `secondary_order` / `eligible_but_not_pushed` | 116 | -3,806,227 |
| `primary_order` / `eligible_but_not_pushed` | 129 | -2,033,301 |
| `primary_order` / `pushed` | 397 | -1,158,849 |
| `equality_prefix_ordered_suffix` / `pushed` | 1 | -192,393 |
| `incompatible_filter_first_order` / `materialized` | 618 | -5,974,224 |
| `materialized_order` / `materialized` | 144 | +4,393,947 |

Ten scenarios changed route facts from eligible to pushed. The largest intended
wins were:

- `blob.select.lengths.bucket_range.bucket_label_asc.limit3`: -979,543 total
  instructions, `data_store.get` -6, index ranges -2, index entries -11;
- `blob.select.lengths.bucket_range.bucket_label_asc.limit1`: -826,953 total
  instructions, `data_store.get` -3, index ranges -3, index entries -14;
- `user.select.text_expr.age_range.age_desc.limit1`: -509,495 total
  instructions, `data_store.get` -2, index ranges -2, index entries -7;
- `user.select.numeric_expr.age_range.age_desc.limit1`: -508,390 total
  instructions, `data_store.get` -2, index ranges -2, index entries -7;
- `user.select.text_expr.age_range.age_asc.limit1`: -506,339 total
  instructions, `data_store.get` -2, index ranges -2, index entries -7;
- `user.select.numeric_expr.age_range.age_asc.limit1`: -505,639 total
  instructions, `data_store.get` -2, index ranges -2, index entries -7.

The largest regressions were storage-mirror materialized-order scenarios. The
gate accepted them because they were below the configured 10% and 100k
instruction regression threshold or did not indicate semantic drift.

## Correctness

- No common-success result signatures changed.
- No cursor signatures changed.
- No new failures appeared.
- No public read-admission fallback bypass was introduced.
- Exact primary-key public reads are now admitted only from the selected
  `ByKey` / `ByKeys` proof; oversized `ByKeys` sets still fail returned-row
  policy.

## Validation

- `cargo fmt --all`: pass.
- `cargo test -p icydb-core --all-features db::query::admission::tests -- --nocapture`: pass.
- `cargo test -p icydb-core --all-features primary_key_lookup_without_limit -- --nocapture`: pass.
- `cargo test -p icydb-core --all-features planner_index_range_residual_stripping -- --nocapture`: pass.
- `cargo test -p icydb-core --all-features planner_composite_index_range_residual_stripping -- --nocapture`: pass.
- `cargo test -p icydb-core --all-features read_admission -- --nocapture`: pass.
- `cargo test -p icydb-testing-integration --test sql_perf_matrix_audit`: pass.
- `cargo test -p icydb-testing-integration --test sql_perf_matrix_audit -- --nocapture`: pass.
- `cargo clippy -p icydb-core --all-features --tests -- -D warnings`: pass.
- `cargo clippy -p icydb-testing-integration --all-features --test sql_perf_matrix_audit -- -D warnings`: pass.
- `cargo clippy -p icydb-testing-integration --tests -- -D warnings`: pass.
- `jq empty docs/design/0.196-sqlite-comparison-audit/implementation-results.json`: pass.
- `git diff --check`: pass.
- Descriptor limit-stop property focused EXPLAIN test: pass.
- Scalar-load descriptor route-family focused EXPLAIN test: pass.
- Canonical EXPLAIN intent suite: pass.
- Verbose EXPLAIN intent suite: pass.
- Executor semantics snapshot suite: pass.
- Full after deterministic matrix: pass.
- Full before/after delta helper: pass.
- Fresh order-hint deterministic matrix refresh: pass.
- Saved-report order-hint delta helper: pass.
- Fresh descriptor route-diagnostics deterministic matrix refresh: pass.
- Saved-report descriptor route-diagnostics delta helper: pass.
- Fresh `v0.196.0` deterministic matrix baseline: pass.
- Saved-report `v0.196.0` to end-of-196 delta helper: pass.
- `python3 -m json.tool docs/design/0.196-sqlite-comparison-audit/implementation-results.json`: pass.

One malformed local command failed before validation because `cargo test` accepts
only one test-name filter before `--`; it was rerun with valid filters.

## Complexity, Perf, And Wasm Delta

- Files touched in this slice: 11 tracked files, plus the 0.196 design result
  files.
- Approximate tracked line delta at last check: +2,845 / -79.
- Implementation shape: more instrumented but still localized. The production
  changes are small; most line growth is matrix/delta evidence plumbing and
  adversarial tests.
- Perf delta: full matrix delta passed with the route and scenario numbers
  above.
- Wasm-size delta: not measured. The full matrix recorded the same `debug`
  canister wasm profile before and after; no wasm-size artifact was produced.
