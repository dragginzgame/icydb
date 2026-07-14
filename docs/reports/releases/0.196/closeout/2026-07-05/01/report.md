# IcyDB 0.196 Closeout Audit

## Executive Summary

Verdict: `CLOSED_WITH_DOC_DEBT`.

0.196 is closed as an implementation line: the ordered-read pushdown slice landed real pushed/limit-stopped execution for eligible ordered reads, later patches hardened route attribution and SQLite/IcyDB differential coverage, and the retained evidence shows no semantic, cursor, status, or access-counter drift across the end-of-line matrix deltas. It is safe to start 0.197 implementation and keep 0.198 design separate.

Top blockers: none for moving to 0.197.

Top non-blocking follow-ups:

1. Update `docs/contracts/QUERY_CONTRACT.md` because it still says cursor continuation is purely post-access and not pushed into index seek/range operations.
2. Update or archive-note `docs/contracts/CURSOR.md` so its historical no-pushdown statement does not look current.
3. Regenerate and archive durable release copies of the original before/after/delta full-matrix artifacts; some early `/tmp` paths recorded in `implementation-results.md` were no longer present during this audit.
4. Add one explicit named public-read fallback test for "pushdown-admitted route cannot silently fall back to unadmitted materialized execution" if the existing read-admission coverage is not considered direct enough.
5. Add an explicit cursor edge-case matrix for first/last boundary, exact `limit`, exact `limit + 1`, and final empty page across each pushed route family.

0.196 can be considered closed with documentation and artifact-retention debt. The biggest residual risk is documentation drift: a reader following the query/cursor contracts can still conclude cursor continuation is always post-access even though 0.196 route evidence now distinguishes pushed, residual, materialized, unsupported, and fallback shapes.

## Scope

Date: 2026-07-05

Commit inspected: `f793fce4558f7d4ebac76c7d9b92be48352c1f19`

Dirty worktree at audit start/end:

| File/status | Classification | Closeout action |
| --- | --- | --- |
| `M CHANGELOG.md` | expected 0.196 closeout/release-note artifact | Include in audit context; do not revert. |
| `M docs/changelog/0.196.md` | expected 0.196 closeout/release-note artifact | Include in audit context; do not revert. |
| deleted `docs/design/0.190-*`, `0.191-*`, `0.192-*`, `0.193-*` | unrelated or unclear archive move | Exclude from 0.196 closeout. |
| untracked `docs/design/archive/0.190-*` through `0.193-*` | unrelated or unclear archive move | Exclude from 0.196 closeout. |
| `M docs/design/0.197-deterministic-optimizer-canonicalization/0.197-design.md` | unrelated 0.197 design work | Exclude from 0.196 closeout. |
| untracked `docs/design/0.198/` | unrelated 0.198 design work | Exclude from 0.196 closeout. |
| new `docs/audits/0.196-closeout/*` | expected output of this audit | Include. |

Files inspected:

- `docs/design/0.196-sqlite-comparison-audit/0.196-design.md`
- `docs/design/0.196-sqlite-comparison-audit/implementation-results.md`
- `docs/design/0.196-sqlite-comparison-audit/implementation-results.json`
- `docs/changelog/0.196.md`
- `CHANGELOG.md`
- `docs/contracts/QUERY_CONTRACT.md`
- `docs/contracts/CURSOR.md`
- `docs/contracts/READ_ADMISSION.md`
- `docs/governance/fast-path-inventory.md`
- `testing/integration/tests/sql_perf_matrix_audit.rs`
- relevant core query, cursor, read-admission, and route-classification tests found by `rg`

Matrix artifacts inspected:

- `/tmp/icydb-196-line-delta-1966/sql_perf_196_0_to_196_6_delta.json`
- `/tmp/icydb-196-line-delta-1966/sql_perf_196_0_to_196_6_delta.md`
- `/tmp/icydb-196-6-grouped-classifier-delta/sql_perf_196_6_grouped_classifier_delta.json`
- `/tmp/icydb-196-6-grouped-classifier-delta/sql_perf_196_6_grouped_classifier_delta.md`
- committed summaries in `implementation-results.md` and `implementation-results.json`

Not reproduced:

- The full deterministic matrix was not rerun. The workspace test suite and non-ignored matrix tests were run instead.
- The original early full-matrix files under `/tmp/icydb-196-current-full` and `/tmp/icydb-196-after-full` were not available during this audit.
- Wasm-size artifacts were not reproduced.

## Closeout Gate Summary

| Gate | Status | Evidence | Blocking? |
| --- | --- | --- | --- |
| Hard-cut | Pass | No persisted-format/cursor-token/public API compatibility path is reported; no cursor protocol change required; implementation-results states no recovery/persisted/public read-admission weakening. | No |
| Correctness | Pass with targeted test debt | Full matrix has 0 result signature changes, 0 cursor signature changes, 0 new failures, 0 resolved failures; cursor mutation tests and admission tests exist. | No |
| Performance | Pass | Original full delta passed with 10 eligible-to-pushed route changes and intended access-counter reductions; end-of-196 delta has 1,756 union scenarios, 1,675 common successes, 0 new/resolved failures, 0 result/cursor signature changes, 0 access-counter deltas, and no regression gate crossings. | No |
| Artifacts | Pass with retention debt | Committed summaries plus retained line delta are inspectable; early scratch `/tmp` artifacts were missing. | No for 0.197; regenerate before release attachments. |
| Docs | Fail, non-blocking | Query and cursor contracts still describe cursor continuation as post-access only. | No for 0.197; should be fixed before publishing final 0.196 docs. |
| Public API/compatibility | Pass with diagnostic-stability note | Diagnostic EXPLAIN/perf fields expanded; no cursor token, persisted format, or public query API redesign found. | No |
| Validation commands | Pass | `cargo fmt --check`, `cargo test --workspace --all-features`, and `cargo clippy --workspace --all-features --all-targets -- -D warnings` passed. | No |

## Phase 1: Scope Verdict

0.196 stayed within scope. It did not become a SQLite compatibility layer or a general cost-based optimizer. The implementation followed the intended SQLite-informed principle: bounded ordered reads should not read, decode, sort, or window more rows than needed when the selected access path can prove a safe stop point.

Scope exclusions verified:

| Exclusion | Verdict | Evidence |
| --- | --- | --- |
| Persisted-format changes | Avoided | `implementation-results.md` repeatedly states no persisted-format/stable-storage changes; no current diff in persisted-format files was present. |
| Recovery changes | Avoided | Implementation results describe query/explain/harness/read-admission work only. |
| Public read-admission weakening | Avoided | `READ_ADMISSION.md` says exact selected primary-key access may supply a row bound; implementation results say oversized key sets still fail. |
| Public API redesign | Avoided | No query builder or public cursor envelope redesign found; route facts are diagnostic attribution. |
| Broad executor rewrite | Avoided | Design/results describe narrow residual stripping, exact-key admission, route diagnostics, and harness work. |
| Cost-based optimizer behavior | Avoided | Classification is deterministic route-family/outcome, not cost estimation. |
| 0.197 primary-key canonicalization | Avoided | Exact-key admission exists, but this is selected `ByKey`/`ByKeys` proof, not general primary-key canonicalization. |
| 0.198 read-intent ergonomics | Avoided | No developer-facing API ergonomics line was reopened. |

Implementation classification: full 0.196 closeout with documentation/artifact-retention debt.

0.196 was not diagnostic-only. The original full delta reported 10 route-fact changes from eligible to pushed and specific intended wins with lower `data_store.get`, index range, and index-entry counters. Later 0.196.5 through 0.196.20 patches were mostly diagnostic, classifier, differential, and gate hardening; those later patches correctly avoid claiming new runtime performance wins.

## Hard-Cut Audit

| Check | Required? | Evidence | Pass/Fail/Unknown | Notes |
| --- | --- | --- | --- | --- |
| No cursor compatibility fallback if token protocol changed | N/A | No cursor token protocol change was reported. | Pass | No old/new cursor decoder path was needed. |
| Old tokens rejected if token protocol changed | N/A | No token protocol change. | Pass | Existing malformed/signature mismatch cursor tests remain relevant. |
| Cache/method version hard-cut if cache semantics changed | N/A | Results say no cache behavior changed; route facts are diagnostic. | Pass | No serialized plan-cache compatibility path found in inspected evidence. |
| No persisted-format change landed | Required | `implementation-results.md` and changelog repeatedly state no persisted/stable-storage changes. | Pass | No persisted-format artifact drift was found in the scoped diff. |
| No public API compatibility shim landed | Required | No query builder/cursor envelope public API redesign was found. | Pass | Diagnostic output grew. |
| No legacy cursor decode fallback added | Required if token changed | No token change found. | Pass | Hard-cut rule remains satisfied. |
| Malformed/mismatched tokens do not accidentally receive pushdown when token shape unchanged | Required | Cursor validation/error mapping tests exist in `crates/icydb-core/src/db/query/fingerprint/shape_signature/tests/mod.rs`, `crates/icydb-core/src/error/tests.rs`, and `crates/icydb-core/src/db/query/plan/validate/tests/cursor_policy.rs`. | Pass with targeted-test debt | Add one 0.196-named test that asserts mismatched entity/index/order/direction cannot receive pushed execution if the team wants direct traceability. |
| Old cached plans cannot reuse stale route facts if plan facts changed | Required if cache semantics changed | No cache semantics change was claimed. Descriptor route facts are derived from executor-owned route facts. | Pass | A dedicated cache-version test is not required unless route facts enter cache identity later. |

## Correctness Audit

| Invariant | Evidence | Test file/test name | Pass/Fail/Unknown | Blocking? |
| --- | --- | --- | --- | --- |
| Index path equals materialized fallback for supported pushed shapes | Full matrix result signatures unchanged; SQLite subset and random differential harnesses pass. | `testing/integration/tests/sql_perf_matrix_audit.rs`, `sql_perf_required_sqlite_comparison_subset_matches_reference_fixture`, `sql_perf_random_matrix_differential_compares_sqlite_reference_fixture` | Pass | No |
| No duplicate rows across pages | Cursor signatures unchanged; cursor continuation tests cover resume behavior. | `crates/icydb-core/src/db/session/tests/branch_set.rs`, continuation tests found by `rg cursor_continuation` | Pass | No |
| No skipped rows across pages | Cursor mutation tests cover deleted boundary/unseen rows and ASC/DESC insertion windows. | `branch_set.rs` tests cited in implementation results | Pass | No |
| Forward pagination tested | Cursor continuation tests and query contract require forward-only pagination. | `crates/icydb-core/src/db/query/plan/validate/tests/cursor_policy.rs` | Pass | No |
| Reverse pagination tested where supported as DESC order | DESC cursor continuation tests are cited in implementation results. | `branch_set.rs` sparse IN DESC and secondary-index DESC continuation tests | Pass | No |
| Equal ordered values use deterministic PK tie-breaker | Query contract appends PK tie-breaker; secondary-index duplicate value mutation test cited. | `branch_set.rs`, secondary-index cursor continuation mutation test | Pass | No |
| Tombstones tested | Implementation results cite tombstone/live and journaled/heap coverage; matrix covers heap and journaled surfaces. | core executor/session tests plus matrix artifacts | Pass with confidence medium | No |
| Live/journaled and heap stores tested | Matrix keys include `heap_user` and `journaled_user`; full deltas include both. | line delta artifact | Pass | No |
| SQL/fluent parity tested | Implementation results cite SQL/fluent parity and read-admission tests. | core read-admission and branch-set tests | Pass | No |
| Public read admission cannot be bypassed by runtime fallback | Results state no bypass; `READ_ADMISSION.md` documents selected exact-key bound; admission tests pass. | `crates/icydb-core/src/db/query/admission` tests cited by implementation results | Pass with targeted-test debt | No |
| Runtime fallback independently admitted or fails closed | Docs and design require it; direct test name was not conclusively identified in this audit. | Existing read-admission and branch-set fallback tests | Unknown/medium | No, but add a direct test. |
| Pushdown never bypasses residual filters | 0.196 strips only selected access-proven range bounds and preserves stricter sibling predicates. | `planner_index_range_residual_stripping`, `planner_composite_index_range_residual_stripping` cited in results | Pass | No |
| Cached plans do not embed current liveness/generation facts | No cache behavior changed; liveness/cursor signatures unchanged. | matrix artifacts, implementation-results | Pass with confidence medium | No |
| Observability/EXPLAIN/perf attribution does not change behavior | 0.196.5/0.196.6 deltas show access counters and result/cursor signatures unchanged despite route fact changes. | retained line delta and grouped classifier delta | Pass | No |

Cursor edge-case audit:

| Edge case | Evidence | Status |
| --- | --- | --- |
| First page | Focused proof matrix and route tests cover first page. | Pass |
| Second page | Cursor continuation resume tests cover second page. | Pass |
| ASC | Implementation results cite ASC insertion before/after boundary. | Pass |
| DESC | Implementation results cite DESC insertion before/after boundary. | Pass |
| Equal order keys | Secondary-index duplicate value continuation test cited. | Pass |
| Deleted row between pages | Boundary row and unseen row deletion tests cited. | Pass |
| Tombstoned row | Covered indirectly by tombstone/live/journaled claims and matrix surfaces. | Pass with medium confidence |
| Missing cursor boundary | Cursor validation tests cover malformed/missing policy. | Pass |
| Malformed cursor | Error mapping and cursor decode validation tests exist. | Pass |
| Mismatched entity/index/order/direction | Shape-signature tests cover query-shape mismatch; direct route-pushdown mismatch fixture would improve traceability. | Pass with targeted-test debt |
| Limit 1 | Matrix and proof cases include `limit1`. | Pass |
| Limit 10 | Matrix and proof cases include `limit10`. | Pass |
| Final empty page | Not directly identified for every route family. | Non-blocking gap |

## Performance Audit

The implementation has two performance stories that must not be conflated:

1. Original 0.196 runtime pushdown: real wins were reported where route facts changed to pushed and access counters fell.
2. End-of-196 hardening after 0.196.0: mostly diagnostic/classifier/differential work. This line-level delta correctly shows no access-counter changes and should not be marketed as another pushdown win.

Full matrix summary from retained end-of-line delta:

| Metric | Before | After | Delta | Notes |
| --- | ---: | ---: | ---: | --- |
| generated scenarios | 1,756 | 1,756 | 0 | `baseline_scenario_count` and `current_scenario_count` |
| executed scenarios | 1,675 | 1,675 | 0 | common successful scenarios |
| common successes | 1,675 | 1,675 | 0 | no common-success loss |
| new failures | 0 | 0 | 0 | no new failures |
| resolved failures | 0 | 0 | 0 | no resolved failures |
| aggregate total instructions | 4,814,331,699 | 4,825,263,485 | +10,931,786 (+0.23%) | below gate |
| aggregate execute instructions | 4,206,336,771 | 4,213,462,530 | +7,125,759 | diagnostic overhead/noise |
| aggregate `data_store.get` | 79,838 | 79,838 | 0 | no post-0.196.0 runtime access change |
| aggregate index ranges | 1,523 | 1,523 | 0 | unchanged |
| aggregate index entries | 5,221 | 5,221 | 0 | unchanged |
| rows returned | 5,319 | 5,319 | 0 | unchanged |

Original 0.196 full delta evidence from `implementation-results.md`:

- Union scenarios: 1,756.
- Common successful scenarios: 1,675.
- Improved scenarios: 699.
- Regressed scenarios: 976.
- New failures: 0.
- Resolved failures: 0.
- Result signature changes: 0.
- Cursor signature changes: 0.
- Route facts changed from eligible to pushed: 10.
- Largest intended wins included:
  - `blob.select.lengths.bucket_range.bucket_label_asc.limit3`: -979,543 total instructions, `data_store.get` -6, index ranges -2, index entries -11.
  - `blob.select.lengths.bucket_range.bucket_label_asc.limit1`: -826,953 total instructions, `data_store.get` -3, index ranges -3, index entries -14.
  - `user.select.text_expr.age_range.age_desc.limit1`: -509,495 total instructions, `data_store.get` -2, index ranges -2, index entries -7.
  - `user.select.numeric_expr.age_range.age_desc.limit1`: -508,390 total instructions, `data_store.get` -2, index ranges -2, index entries -7.
  - `user.select.text_expr.age_range.age_asc.limit1`: -506,339 total instructions, `data_store.get` -2, index ranges -2, index entries -7.
  - `user.select.numeric_expr.age_range.age_asc.limit1`: -505,639 total instructions, `data_store.get` -2, index ranges -2, index entries -7.

Top 20 absolute improvements from `0.196.0` to end-of-196:

| Scenario | Total delta | Percent bp | Execute delta | Access delta |
| --- | ---: | ---: | ---: | --- |
| `journaled_user.select.wide.pk_range.name_asc.limit1` | -51,975 | -24 | -53,231 | 0 |
| `heap_user.select.wide.pk_range.name_asc.limit1` | -51,607 | -24 | -53,278 | 0 |
| `heap_user.select.wide.all.name_asc.limit10` | -44,789 | -27 | -46,253 | 0 |
| `journaled_user.select.wide.all.name_asc.limit10` | -43,415 | -26 | -44,879 | 0 |
| `heap_user.select.wide.name_range.name_asc.limit1` | -33,828 | -15 | -35,285 | 0 |
| `journaled_user.select.wide.name_range.name_asc.limit1` | -33,766 | -15 | -35,103 | 0 |
| `journaled_user.select.wide.pk_range.age_asc.limit1` | -28,698 | -12 | -29,996 | 0 |
| `heap_user.select.wide.pk_range.age_asc.limit1` | -28,122 | -12 | -29,793 | 0 |
| `journaled_user.select.pk.name_range.age_asc.limit1` | -27,546 | -12 | -28,953 | 0 |
| `heap_user.select.wide.name_range.age_asc.limit1` | -27,314 | -12 | -28,771 | 0 |
| `heap_user.select.pk.name_range.age_asc.limit1` | -27,041 | -12 | -28,867 | 0 |
| `journaled_user.select.wide.name_range.age_asc.limit1` | -26,916 | -11 | -28,253 | 0 |
| `heap_user.select.pk.all.name_asc.limit3` | -21,606 | -13 | -22,690 | 0 |
| `heap_user.select.pk.all.name_asc.limit1` | -21,572 | -13 | -22,656 | 0 |
| `heap_user.select.narrow.all.age_asc.limit3` | -21,529 | -12 | -22,685 | 0 |
| `journaled_user.select.pk.all.name_asc.limit3` | -21,525 | -13 | -22,614 | 0 |
| `journaled_user.select.pk.all.name_asc.limit1` | -21,437 | -13 | -22,526 | 0 |
| `journaled_user.select.narrow.all.age_asc.limit3` | -21,302 | -12 | -22,731 | 0 |
| `heap_user.select.wide.all.name_asc.limit3` | -21,161 | -12 | -22,625 | 0 |
| `journaled_user.select.wide.all.name_asc.limit3` | -21,054 | -12 | -22,518 | 0 |

Top 20 absolute regressions from `0.196.0` to end-of-196:

| Scenario | Total delta | Percent bp | Execute delta | Access delta |
| --- | ---: | ---: | ---: | --- |
| `heap_user.select.wide.name_range.name_asc.limit3` | +121,049 | +55 | +119,592 | 0 |
| `journaled_user.select.wide.name_range.name_asc.limit3` | +120,959 | +55 | +119,622 | 0 |
| `heap_user.select.narrow.pk_range.name_asc.limit1` | +112,854 | +55 | +111,206 | 0 |
| `journaled_user.select.narrow.pk_range.name_asc.limit1` | +112,774 | +55 | +111,126 | 0 |
| `journaled_user.select.narrow.name_range.name_asc.limit1` | +97,984 | +46 | +96,628 | 0 |
| `heap_user.select.wide.pk_range.name_asc.limit3` | +97,223 | +45 | +95,552 | 0 |
| `journaled_user.select.wide.pk_range.name_asc.limit3` | +96,855 | +45 | +95,599 | 0 |
| `heap_user.select.narrow.name_range.name_asc.limit1` | +95,595 | +45 | +93,815 | 0 |
| `heap_user.select.wide.name_range.age_asc.limit3` | +84,698 | +37 | +83,241 | 0 |
| `journaled_user.select.wide.name_range.age_asc.limit3` | +84,572 | +37 | +83,235 | 0 |
| `journaled_user.select.wide.age_range.name_asc.limit1` | +83,083 | +44 | +77,736 | 0 |
| `journaled_user.select.narrow.age_range.name_asc.limit1` | +82,860 | +44 | +77,597 | 0 |
| `heap_user.select.wide.age_range.name_asc.limit1` | +82,657 | +44 | +77,526 | 0 |
| `heap_user.select.narrow.age_range.name_asc.limit1` | +82,574 | +44 | +77,389 | 0 |
| `journaled_user.select.wide.age_range.age_asc.limit3` | +70,741 | +37 | +65,394 | 0 |
| `heap_user.select.wide.age_range.age_asc.limit3` | +70,478 | +37 | +65,347 | 0 |
| `heap_user.select.narrow.pk_range.age_asc.limit1` | +63,856 | +28 | +62,208 | 0 |
| `heap_user.select.wide.age_range.age_asc.limit10` | +60,535 | +30 | +55,404 | 0 |
| `journaled_user.select.narrow.name_range.name_asc.limit3` | +59,815 | +28 | +58,459 | 0 |
| `journaled_user.select.wide.pk_range.age_asc.limit10` | +57,953 | +25 | +56,697 | 0 |

Top percentage improvements excluding tiny scenarios below 100k instructions:

| Scenario | Percent bp | Total delta |
| --- | ---: | ---: |
| `user.select.narrow.pk_range.age_desc.limit1` | -38 | -3,503 |
| `user.select.narrow.all.lower_name_asc.limit1` | -37 | -3,052 |
| `user.select.narrow.pk_range.pk_desc.limit3` | -28 | -2,969 |
| `heap_user.select.wide.all.name_asc.limit10` | -27 | -44,789 |
| `blob.select.payload.all.pk_asc.limit1` | -26 | -2,241 |
| `journaled_user.select.wide.all.name_asc.limit10` | -26 | -43,415 |
| `heap_user.select.wide.pk_range.name_asc.limit1` | -24 | -51,607 |
| `journaled_user.select.wide.pk_range.name_asc.limit1` | -24 | -51,975 |
| `user.select.narrow.lower_name_prefix.name_asc.limit3` | -23 | -3,164 |
| `user.select.text_expr.age_in.name_asc.limit10` | -22 | -3,267 |
| `blob.select.metadata.bucket_eq.pk_asc.limit3` | -21 | -1,869 |
| `user.select.numeric_expr.pk_range.age_asc.limit1` | -21 | -4,019 |
| `user.select.numeric_expr.pk_range.age_desc.limit1` | -21 | -4,019 |
| `user.select.text_expr.pk_range.age_asc.limit1` | -21 | -4,123 |
| `user.select.text_expr.pk_range.age_desc.limit1` | -21 | -4,123 |
| `heap_user.select.wide.name_range.name_asc.limit1` | -15 | -33,828 |
| `journaled_user.select.narrow.name_range.pk_desc.limit3` | -15 | -1,919 |
| `journaled_user.select.wide.name_range.name_asc.limit1` | -15 | -33,766 |
| `heap_user.select.pk.all.name_asc.limit1` | -13 | -21,572 |
| `heap_user.select.pk.all.name_asc.limit3` | -13 | -21,606 |

Top percentage regressions excluding tiny scenarios below 100k instructions:

| Scenario | Percent bp | Total delta |
| --- | ---: | ---: |
| `account.select.wide.handle_prefix_active.tier_handle_asc.limit3` | +126 | +18,957 |
| `account.select.wide.tier_gold_active.lower_handle_asc.limit1` | +114 | +15,716 |
| `account.select.pk.tier_gold_active.lower_handle_asc.limit1` | +111 | +13,849 |
| `account.select.wide.tier_gold_active.lower_handle_asc.limit3` | +109 | +16,048 |
| `account.select.narrow.tier_gold_active.lower_handle_asc.limit10` | +109 | +14,706 |
| `account.select.narrow.tier_gold_active.lower_handle_asc.limit1` | +109 | +14,103 |
| `account.select.wide.tier_gold_active.lower_handle_asc.limit10` | +108 | +15,865 |
| `account.select.pk.tier_gold_active.lower_handle_asc.limit10` | +108 | +13,726 |
| `account.select.pk.tier_gold_active.lower_handle_asc.limit3` | +107 | +13,626 |
| `account.select.text_expr.tier_gold_active.lower_handle_asc.limit10` | +103 | +14,064 |
| `account.select.narrow.tier_gold_active.lower_handle_asc.limit3` | +102 | +13,779 |
| `account.select.text_expr.tier_gold_active.lower_handle_asc.limit1` | +99 | +13,160 |
| `journaled_user.select.narrow.age_range.pk_asc.limit3` | +98 | +16,600 |
| `account.select.wide.lower_handle_prefix_active.lower_handle_asc.limit1` | +98 | +16,577 |
| `account.select.wide.handle_prefix_active.handle_desc.limit1` | +98 | +12,802 |
| `account.select.wide.handle_prefix_active.handle_asc.limit1` | +98 | +12,802 |
| `blob.select.payload.bucket_eq.bucket_label_asc.limit1` | +97 | +9,516 |
| `account.select.wide.handle_prefix_active.handle_desc.limit3` | +97 | +13,165 |
| `account.select.wide.handle_prefix_active.handle_asc.limit3` | +97 | +13,165 |
| `account.select.text_expr.tier_gold_active.lower_handle_asc.limit3` | +97 | +13,272 |

Status/result changes:

- New failures: 0.
- Resolved failures: 0.
- Common failures: 81 stable failures.
- Result signature changes: 0.
- Cursor signature changes: 0.
- Result row-count changes: 0.
- Route-family/outcome/reason changes after 0.196.0: 613.
- Order-hint changes after 0.196.0: 1,663.
- Limit-stop attribution changes after 0.196.0: 1,675.

All route-fact changed rows are machine-readable in `/tmp/icydb-196-line-delta-1966/sql_perf_196_0_to_196_6_delta.json`. The committed summary groups the major transitions: Blob suffix-gap reclassification, residual candidate scans no longer labeled pushed, materialized-order windows separated from candidate pushdown, unsupported expression orders split out, and grouped aggregates classified as grouped materialized work.

Material improvement classification:

- Original 0.196 intended wins: route changed from eligible to pushed and access/materialization counters fell. Count as 0.196 pushdown wins.
- End-of-196 line delta improvements: same route but lower planner/executor overhead or measurement noise. Do not count as new pushdown wins because access counters stayed unchanged.
- End-of-196 line delta regressions: diagnostic/classifier/reporting overhead or measurement noise. No regression crossed the configured `>=10%` and `>=100k` gate.

## Proof-Case Audit

| Proof case | Matrix key/test | Route family | Route outcome | Before counters | After counters | Pass/Fail/Unknown |
| --- | --- | --- | --- | --- | --- | --- |
| `ORDER BY id ASC LIMIT 1` | `user.select.pk.all.pk_asc.limit1`; focused proof matrix | `primary_order` | `pushed` | Original pre-pushdown eligible/not pushed in test fixture; line baseline already pushed | After line: pushed; `limit_stop_after.possible=true` in route facts | Pass |
| `ORDER BY id ASC LIMIT 10` | full matrix `pk_asc.limit10` keys | `primary_order` | `pushed` | available in matrix | pushed in retained line artifact | Pass |
| Reverse primary-key order | `pk_desc` matrix keys | `primary_order` | `pushed` where order-compatible | available in matrix | pushed in retained line artifact | Pass |
| `ORDER BY age ASC, id ASC LIMIT 1/10` | user/account age/tier secondary-order keys; focused three-route proof includes `user.select.pk.all.age_asc.limit3` | `secondary_order` | `pushed` where sampled counters prove bounded index entries | original full delta included pushed secondary routes | retained line artifact has 227 `secondary_order` / `pushed` rows | Pass |
| Reverse secondary-index order | DESC matrix keys | `secondary_order` | pushed/materialized depending compatibility | available in matrix | classified in full matrix | Pass |
| Equality-prefix ordered suffix | `token.collection_stage_id.prefixed_stage_range.page_only.limit50` | `equality_prefix_ordered_suffix` | `pushed` | original route proof present | retained line artifact has 1 pushed equality-prefix row, 0 `data_store.get`, 1 range, 50 entries | Pass |
| Cursor continuation first page | branch-set/session cursor tests | route-dependent | pushed/materialized as eligible | N/A | pass in test suite | Pass |
| Cursor continuation second page | branch-set/session cursor tests | route-dependent | pushed/materialized as eligible | N/A | pass in test suite | Pass |

The proof family is sufficient to close 0.196. It does not mean every current hotspot is pushable. The design correctly documents that some `WHERE name >= ... ORDER BY age ... LIMIT` shapes remain materialized or residual until a later route-selection line.

## Route Classification Audit

| Classification area | Current mechanism | Risk | Pass/Fail/Unknown | Recommended fix |
| --- | --- | --- | --- | --- |
| Matrix route family/outcome fields | `MatrixSample` carries `route_family`, `route_outcome`, `route_reason`; delta rows carry before/after fields. | Low | Pass | Keep these fields mandatory for common-success closeout. |
| Runtime route facts | Descriptor route facts are derived from executor-owned route facts and selected access shape, per `implementation-results.md`. | Low | Pass | Keep route ownership in executor/planner boundary. |
| String fragments in classifier | Some harness helpers still inspect SQL text for `LIMIT`, `ORDER BY`, and scenario family metadata. | Medium harness debt | Pass for closeout | Prefer structured scenario metadata over SQL string inspection in a later harness cleanup. |
| Grouped aggregate split | Grouped aggregate rows are classified as `materialized_order` / `materialized` with `grouped_aggregate_materialized`. | Low | Pass | Keep grouped-specific route contract separate. |
| Stable reason enums | Disabled reasons are stable strings such as `filter_order_mismatch`, `requires_materialized_sort`, `residual_filter_requires_candidate_scan`, `index_order_suffix_gap`, `order_expression_not_classified`. | Low | Pass | Consider enum-typed report serialization if harness churn continues. |
| Historical artifact comparability | Delta helper can compare saved reports and fill route defaults for older rows. | Medium | Pass with debt | Keep a schema/version field in future matrix artifacts. |

## Documentation Audit

| Doc | Current status | Issue | Blocking? | Required edit |
| --- | --- | --- | --- | --- |
| `docs/design/0.196-sqlite-comparison-audit/0.196-design.md` | Mostly current | Header says checkpointed through 0.196.10 even though changelog is through 0.196.20; it does point to results. | No | Update status line to say implementation closed and results file is authority. |
| `docs/design/0.196-sqlite-comparison-audit/implementation-results.md` | Strong | Accurately separates original runtime wins from later diagnostic hardening; records scratch artifact paths. | No | Add a durable artifact-retention note if release packaging will attach evidence. |
| `docs/design/0.196-sqlite-comparison-audit/implementation-results.json` | Strong | Machine-readable summary exists; early raw artifacts not durable. | No | Archive regenerated full matrix JSON/MD before release attachment. |
| `docs/changelog/0.196.md` | Strong | Detailed 0.196.20 notes are present and say test-harness-only/no production behavior changes for late slices. | No | None required for closeout. |
| `CHANGELOG.md` | Dirty expected release-note work | Current diff contains 0.196.20 release-note additions. | No | Leave for user-owned release flow. |
| `docs/contracts/QUERY_CONTRACT.md` | Stale | Performance model still says cursor continuation is currently post-access and not pushed into index seek/range operations. | No for code; yes before final docs polish | Rewrite to separate semantic cursor guarantees from route-dependent pushdown/limit-stop facts. |
| `docs/contracts/CURSOR.md` | Stale historical note | Says no cursor pushdown in current implementation. It is labeled historical but can still confuse readers. | No | Add 0.196 note pointing to current query contract/results, or archive it more clearly. |
| `docs/contracts/READ_ADMISSION.md` | Current | Exact primary-key selected access bound is documented. | No | Add one sentence that runtime fallback cannot execute an unadmitted public route. |
| `docs/governance/fast-path-inventory.md` | Mostly current | Lists stream fast-path families but does not explicitly call out 0.196 ordered-read limit-stop/route facts. | No | Add 0.196 ordered-read route-fact ownership note. |

## API / Compatibility Audit

| Surface | Changed? | Breaking? | Versioned? | Documented? | Tests? |
| --- | --- | --- | --- | --- | --- |
| Public query builder/fluent API | No material redesign found | No | N/A | Existing docs | Workspace tests pass |
| Cursor token envelope | No | No | Existing versioned token policy | `PERSISTED_FORMAT_INVENTORY.md`, query/cursor docs | Cursor validation tests |
| Persisted row/index/schema format | No | No | Existing policy | Persisted format docs | Workspace tests pass |
| Public read-admission API | Behavior narrowed/clarified for exact selected primary-key access | No weakening | N/A | `READ_ADMISSION.md` | read-admission tests cited |
| EXPLAIN descriptor JSON/text | Yes, diagnostic route facts added | Potential diagnostic output change only | Not explicitly versioned in docs | Implementation results | EXPLAIN suites pass |
| Perf matrix JSON/Markdown | Yes | Harness artifact schema changed | No explicit artifact schema version | implementation-results | matrix delta tests pass |
| SQL result DTOs | No material public change found | No | N/A | N/A | SQL tests pass |
| Error codes/diagnostics | No breaking change found | No | Diagnostic-code policy remains | docs/contracts | workspace tests pass |
| Feature flags | No relevant change found | No | N/A | N/A | all-features test/clippy pass |

Public surface conclusion: no public API or persisted compatibility blocker was found. Diagnostic and harness JSON fields are now important closeout evidence; document their stability posture if external users consume them.

## Validation

Commands run:

| Command | Result | Blocking? | Notes |
| --- | --- | --- | --- |
| `git status --short` | Pass | No | Dirty worktree classified above. |
| `git rev-parse HEAD` | Pass | No | `f793fce4558f7d4ebac76c7d9b92be48352c1f19`. |
| `git diff --stat` | Pass | No | Showed changelog edits and unrelated design archive movement. |
| `git diff -- docs/design/0.196-sqlite-comparison-audit docs/changelog/0.196.md CHANGELOG.md docs/contracts/QUERY_CONTRACT.md docs/contracts/CURSOR.md testing/integration/tests/sql_perf_matrix_audit.rs` | Pass | No | Scoped diff showed changelog changes only. |
| `cargo fmt --check` | Pass | No | No formatting drift. |
| `cargo test --workspace --all-features` | Pass | No | Workspace tests passed. |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | Pass | No | Clippy passed. |
| `find docs/design/0.196-sqlite-comparison-audit -maxdepth 2 -type f \| sort` | Pass | No | Confirmed design/result files. |
| `find docs/audits -maxdepth 3 -type f \| sort` | Pass | No | No existing closeout convention beyond audits directory. |
| `ls -l /tmp/icydb-196-current-full/... /tmp/icydb-196-after-full/...` | Failed | No | Early scratch artifacts were missing; this creates artifact-retention debt. |
| `jq ... /tmp/icydb-196-line-delta-1966/sql_perf_196_0_to_196_6_delta.json` | Pass | No | Used for aggregate counters and top tables. |
| `sed -n ...` and `rg ...` inspection commands | Pass except one path typo | No | One `rg` command referenced a nonexistent `crates/icydb-core/src/db/query/admission` path; broader searches found relevant evidence. |

Failed commands:

- Artifact existence checks against `/tmp/icydb-196-current-full` and `/tmp/icydb-196-after-full` failed because those scratch paths were gone.
- One `jq` read against the missing `/tmp/icydb-196-after-full` artifact failed for the same reason.
- One `rg` search included a nonexistent path; this was an inspection typo, not a repo failure.

Environmental/not reproduced:

- The full deterministic matrix was not rerun in this audit turn.
- Missing early raw matrix files are expected from `/tmp` retention, but they prevent independent re-parsing of the original 0.196 before/after delta.

## Findings

### 196-CO-001: Query contract still says cursor continuation is post-access only

Category:
- docs

Severity:
- Medium

Blocking:
- No

Confidence:
- High

Evidence:
- `docs/contracts/QUERY_CONTRACT.md` says "Cursor continuation is currently applied in the post-access phase" and "Cursor boundary conditions are not currently pushed down into index seek/range operations."
- `implementation-results.md` reports pushed ordered reads, structured `limit_stop_after`, and route-family/outcome evidence.

What passed:
- Runtime semantics and route evidence pass.

What failed or is missing:
- Contract language is stale and contradicts 0.196 route evidence.

Recommended fix:
- Rewrite the performance model to say cursor semantics are stable while execution may be post-access, pushed, materialized, residual, or unsupported depending on route proof.

Acceptance criteria:
- Query contract no longer says no cursor pushdown exists.
- It explicitly says public read admission cannot be bypassed by runtime fallback.

Suggested patch prompt:
- "Update `docs/contracts/QUERY_CONTRACT.md` for 0.196: separate semantic cursor guarantees from route-dependent execution, document pushed/limit-stopped ordered reads as an optimization only, and state runtime fallback cannot bypass public read admission."

### 196-CO-002: Cursor historical note can be mistaken for current implementation

Category:
- docs

Severity:
- Low

Blocking:
- No

Confidence:
- High

Evidence:
- `docs/contracts/CURSOR.md` is titled "Historical Note" but still says "Confirmed there is no cursor pushdown into index continuation seek/range in the current implementation."

What passed:
- It points normative cursor guarantees to `QUERY_CONTRACT.md`.

What failed or is missing:
- It lacks a post-0.196 note warning that the no-pushdown statement is historical.

Recommended fix:
- Add a short 0.196 update note or move the file under a clearer archive path.

Acceptance criteria:
- Readers cannot treat the no-pushdown line as current.

Suggested patch prompt:
- "Add a post-0.196 note to `docs/contracts/CURSOR.md` explaining that the file is historical and that current route-dependent execution facts live in `QUERY_CONTRACT.md` and the 0.196 implementation results."

### 196-CO-003: Original full-matrix raw artifacts were not durable

Category:
- artifacts

Severity:
- Medium

Blocking:
- No for 0.197; yes if release packaging requires attached raw evidence

Confidence:
- High

Evidence:
- `implementation-results.md` records `/tmp/icydb-196-current-full` and `/tmp/icydb-196-after-full` as scratch paths.
- Those paths were missing during this audit.
- Retained line delta artifacts and committed summaries were present.

What passed:
- Committed summary evidence exists and line-level retained delta is parseable.

What failed or is missing:
- The original before/after raw JSON/Markdown artifacts could not be independently inspected.

Recommended fix:
- Regenerate fresh full deterministic before/after/delta artifacts from the same machine/profile before release packaging and archive them outside `/tmp`.

Acceptance criteria:
- Durable JSON/Markdown artifacts are stored or attached and paths in results are updated.

Suggested patch prompt:
- "Regenerate the 0.196 full deterministic matrix before/after/delta artifacts, archive them under a durable release/audit location, and update `implementation-results.md/json` with durable paths."

### 196-CO-004: Public read fallback fail-closed behavior should get one direct named test

Category:
- tests

Severity:
- Medium

Blocking:
- No

Confidence:
- Medium

Evidence:
- Design requires no unadmitted materialized fallback.
- Read-admission docs and tests cover selected exact-key bounds and public limits.
- This audit did not identify a direct test named for runtime pushdown proof failing after public admission.

What passed:
- No matrix semantic drift and no result/cursor signature drift.

What failed or is missing:
- Traceability from the hard-cut fallback taxonomy to a single direct test is weak.

Recommended fix:
- Add a small test that constructs a public-read query admitted only by pushdown/limit-stop proof, forces runtime fallback if feasible, and asserts it either proves independent admission or fails closed.

Acceptance criteria:
- Test name includes public read admission and fallback fail-closed language.

Suggested patch prompt:
- "Add a focused read-admission test proving a public read admitted by ordered pushdown/limit-stop cannot silently fall back to an unadmitted materialized route."

### 196-CO-005: Cursor edge-case coverage is strong but not tabulated across every route family

Category:
- tests

Severity:
- Low

Blocking:
- No

Confidence:
- Medium

Evidence:
- Implementation results cite primary and secondary cursor mutation tests.
- Matrix signatures and cursor signatures did not change.
- This audit did not find a single table covering first/last boundary, exact `limit`, exact `limit + 1`, and final empty page for each route family.

What passed:
- Core cursor mutation and continuation behavior appears covered.

What failed or is missing:
- Coverage is distributed and not easy to audit.

Recommended fix:
- Add a route-family cursor edge-case matrix or documentation table in tests.

Acceptance criteria:
- Primary-order and secondary-order pushed continuation tests cover first/last boundary, equal keys, deletion, insertion before/after, exact `limit`, exact `limit + 1`, and final empty page.

Suggested patch prompt:
- "Add cursor continuation edge-case tests for pushed primary and secondary ordered routes covering first/last boundary, exact limit, limit plus lookahead, equal keys, deleted boundary, and final empty page."

### 196-CO-006: Harness classifier still has some SQL-string-derived logic

Category:
- harness

Severity:
- Low

Blocking:
- No

Confidence:
- High

Evidence:
- `testing/integration/tests/sql_perf_matrix_audit.rs` uses helpers such as `sql_clause_usize_value`, `sql_order_by_clause`, and `sql_order_by_idx_hint`.
- It also stores structured `route_family`, `route_outcome`, `limit_stop_after`, result signatures, and cursor signatures.

What passed:
- Closeout-critical route facts are present in structured fields.

What failed or is missing:
- Some report attribution still relies on SQL fragments.

Recommended fix:
- Move scenario metadata for order/limit/family into structured scenario definitions where practical.

Acceptance criteria:
- Delta route classification remains valid if SQL text formatting changes.

Suggested patch prompt:
- "Refactor SQL perf matrix scenario metadata so route classification and limit extraction use structured scenario fields rather than SQL string fragments where possible."

### 196-CO-007: Diagnostic DTO stability posture is implicit

Category:
- API

Severity:
- Low

Blocking:
- No

Confidence:
- Medium

Evidence:
- EXPLAIN descriptor roots now include `route_family`, `route_outcome`, `route_reason`, and `limit_stop_after`.
- Docs describe these as diagnostics but do not clearly state whether the field names are stable public DTOs or audit-harness fields.

What passed:
- Tests cover descriptor JSON/text output.

What failed or is missing:
- Stability/version posture is not explicit in public-facing docs.

Recommended fix:
- Document these fields as diagnostic/observability output with a stated stability policy.

Acceptance criteria:
- External consumers know whether route-fact field names are stable, experimental, or audit-only.

Suggested patch prompt:
- "Document the stability posture of EXPLAIN/perf route-fact fields added in 0.196, including `route_family`, `route_outcome`, `route_reason`, and `limit_stop_after`."

## Required Follow-Up PRs

### Must fix before 0.196 can close

None for implementation closeout.

### Can fix after 0.196 but before 0.197 implementation

- Update `QUERY_CONTRACT.md` and `CURSOR.md` to remove stale no-pushdown language.
- Add the direct public-read fallback fail-closed test if you want an unambiguous hard-cut trace.

### Can fix before 1.0

- Archive durable full-matrix artifacts outside `/tmp`.
- Add cursor edge-case matrix tests across route families.
- Document diagnostic DTO stability.
- Reduce SQL-fragment parsing in the perf harness.

### Measurement-only improvements

- Add a stable artifact schema version to matrix JSON.
- Add a small helper to print all route-family/outcome transitions as a separate closeout attachment.

### Documentation-only improvements

- Update 0.196 design status header to "closed with implementation results".
- Add 0.196 ordered-read fast-path ownership to `fast-path-inventory.md`.

## Final Recommendation

0.196 is closed with documentation and artifact-retention debt. Move to 0.197 if desired; do not reopen 0.197 primary-key canonicalization inside 0.196. Keep 0.198 as design-only until 0.197 scope is chosen.

Do not reopen:

- SQLite compatibility as a product goal.
- A general cost-based optimizer.
- Persisted-format or recovery changes.
- Public cursor-token compatibility shims.
- 0.197 primary-key canonicalization.
- 0.198 read-intent ergonomics.

Fix immediately if you want clean release docs: update `QUERY_CONTRACT.md` and `CURSOR.md` so they match the implemented 0.196 route-dependent execution model.
