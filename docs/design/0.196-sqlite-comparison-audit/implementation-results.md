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

## Classifier Hardening

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
- Canonical EXPLAIN intent suite: pass.
- Verbose EXPLAIN intent suite: pass.
- Executor semantics snapshot suite: pass.
- Full after deterministic matrix: pass.
- Full before/after delta helper: pass.
- Fresh order-hint deterministic matrix refresh: pass.
- Saved-report order-hint delta helper: pass.
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
