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
- route classification, signatures, and full union-key delta artifacts are
  emitted by the matrix harness.

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
unless release packaging wants to archive them separately.

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
- `cargo clippy -p icydb-core --all-features --tests -- -D warnings`: pass.
- `cargo clippy -p icydb-testing-integration --all-features --test sql_perf_matrix_audit -- -D warnings`: pass.
- `jq empty docs/design/0.196-sqlite-comparison-audit/implementation-results.json`: pass.
- `git diff --check`: pass.
- Full after deterministic matrix: pass.
- Full before/after delta helper: pass.

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
