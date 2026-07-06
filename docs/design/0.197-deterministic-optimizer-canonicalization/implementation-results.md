# 0.197 Implementation Results

Date: 2026-07-06

Commit inspected while creating this ledger:
`3faf7b7655aabc8175b9bc2e10933b5f92883443`

Status: implementation in progress, correctness evidence and a current focused
PocketIC capture recorded, before/delta performance closeout not claimed.

0.197 is the deterministic optimizer canonicalization line. It is not a
cost-based optimizer line and not a 0.198 read-intent ergonomics line. The
implementation through the current closeout follow-up has landed primary-key
exact-access behavior, public-read admission hardening, and a focused current
PocketIC artifact. This document intentionally does not claim final 0.197
performance closeout because no same-machine focused before artifact, focused
delta, or fresh full deterministic SQL matrix pair has been recorded here.

## Summary

| Area | Current status | Evidence | Closeout status |
| --- | --- | --- | --- |
| Fluent scalar primary-key equality | Implemented | `public_read_fluent_admission_admits_primary_key_filter_without_limit`; `default_fluent_try_entity_admits_primary_key_filter_without_limit` | Covered |
| Explicit `by_id(...)` parity | Implemented | `public_read_fluent_admission_admits_primary_key_lookup_without_limit`; `session_explain_execution_external_primary_key_filter_and_by_id_use_same_access_path` | Covered |
| External primary-key equality | Implemented | `public_read_fluent_admission_admits_external_primary_key_filter_without_limit`; `default_fluent_try_entity_matches_by_id_for_external_primary_key_filter_without_limit` | Covered |
| Fluent finite primary-key `IN (...)` | Implemented | `public_read_fluent_admission_admits_primary_key_in_filter_without_limit`; `default_fluent_execute_rows_dedups_primary_key_in_filter_without_limit` | Covered |
| Explicit `by_ids(...)` resource caps | Implemented | `public_read_fluent_by_ids_rejects_duplicate_raw_input_terms_above_policy` | Covered |
| SQL literal primary-key equality | Implemented | `public_read_sql_admits_primary_key_filter_without_limit` | Covered |
| SQL commuted primary-key equality | Implemented | `public_read_sql_admits_commuted_primary_key_filter_without_limit`; `sql_explain_commuted_primary_key_filter_canonicalizes_to_exact_key` | Covered |
| SQL parameter primary-key equality | Documented out of scope for current SQL contract | `public_read_sql_primary_key_parameter_shape_fails_before_admission`; `prepare_sql_statement_rejects_parameters_before_lowering`; `SQL_SUBSET.md` | Covered as unsupported/fail-closed boundary |
| SQL finite primary-key `IN (...)` | Implemented | `public_read_sql_admits_primary_key_in_filter_without_limit`; `public_read_sql_primary_key_in_filter_orders_deterministically_without_limit` | Covered |
| Wrong-type primary-key literals | Fail closed | `public_read_sql_primary_key_wrong_type_literal_fails_before_admission`; `public_read_sql_commuted_primary_key_wrong_type_literal_fails_before_admission` | Covered |
| Invalid residual predicates | Fail before admission | `public_read_fluent_admission_fails_invalid_residual_after_primary_key_filter` | Covered |
| Residual filters after exact key | Implemented | `default_fluent_execute_rows_applies_residual_after_primary_key_filter_without_limit`; `public_read_sql_applies_residual_after_primary_key_filter_without_limit` | Covered |
| Residual filters after exact key set | Implemented | `default_fluent_execute_rows_applies_residual_after_primary_key_in_filter_without_limit`; `public_read_sql_applies_residual_after_primary_key_in_filter_without_limit` | Covered |
| Empty exact-key result | Implemented | `public_read_fluent_admission_canonicalizes_empty_primary_key_filters_without_limit`; `default_fluent_execute_rows_returns_empty_for_empty_primary_key_filters_without_limit` | Covered |
| Empty exact-key route proof | Implemented | `planner_excluding_primary_key_eq_and_in_child_routes_to_empty_access`; `planner_disjoint_primary_key_in_children_route_to_empty_access`; `planner_intersects_primary_key_in_children_before_secondary_candidates` | Covered |
| Empty exact-key terminals | Implemented | `default_fluent_count_returns_zero_for_empty_primary_key_filters_without_limit`; `default_fluent_require_one_reports_not_found_for_empty_primary_key_filters_without_limit` | Covered |
| Equality plus `IN` narrowing | Implemented | `public_read_fluent_admission_narrows_primary_key_eq_and_in_filter_without_limit` | Covered |
| Deterministic `ByKeys` ordering | Implemented | `default_fluent_execute_rows_orders_primary_key_in_filters_deterministically_without_limit`; `public_read_sql_primary_key_in_filter_orders_deterministically_without_limit` | Covered |
| Finite key-set non-key ordering | Implemented | `default_fluent_primary_key_in_filter_materializes_finite_non_key_order`; `public_read_sql_primary_key_in_filter_materializes_finite_non_key_order` | Covered |
| Heap and journaled stores | Implemented | `public_read_fluent_admission_admits_heap_and_journaled_primary_key_filters_without_limit`; `default_fluent_try_entity_returns_none_for_missing_heap_and_journaled_primary_key_filters` | Covered |
| Deleted/tombstoned key | Implemented | `default_fluent_try_entity_returns_none_for_deleted_heap_and_journaled_primary_key_filters` | Covered |
| Unique secondary equality exclusion | Implemented | `public_read_fluent_admission_keeps_unique_secondary_equality_off_primary_key_access` | Covered |
| Partial composite primary-key exclusion | Implemented | `public_read_fluent_admission_rejects_partial_composite_primary_key_as_full_scan` | Covered |
| Expression-wrapped primary-key exclusion | Implemented for SQL EXPLAIN | `sql_explain_expression_wrapped_primary_key_does_not_canonicalize_to_exact_key` | Covered |
| EXPLAIN route facts | Implemented | `session_explain_execution_primary_key_filter_canonicalization_route_facts_are_stable` | Covered |
| Query-cache shape boundaries | Covered by existing structural tests | `structural_query_cache_key_treats_equivalent_in_list_permutations_as_identical`; `structural_query_cache_key_treats_duplicate_in_list_literals_as_identical`; `structural_query_cache_key_distinguishes_strict_from_text_casefold_coercion` | Covered for current non-parameter SQL/fluent surfaces |
| Focused current perf artifact | Recorded | `sql_perf_197_pk_canonicalization_after.json`; `sql_perf_197_pk_canonicalization_after.md`; PocketIC capture test `pk_canonicalization_focused_current_artifact_writes_from_pocketic` | Supports current behavior evidence |
| Focused before/delta perf artifacts | Helper present, before/delta missing | `pk_canonicalization_focused_delta_writes_from_saved_before_after_artifacts` can emit delta JSON/Markdown from saved captures, but no same-machine before file exists yet | Blocks performance closeout |
| Fresh full deterministic SQL matrix | Missing from this design directory | No fresh full before/after matrix pair recorded here | Blocks final performance closeout |

## Behavior Result

The line has moved beyond diagnostic-only behavior. Exact scalar primary-key
filters and finite primary-key `IN (...)` filters now feed the same bounded
`ByKey` / `ByKeys` proof family used by explicit key APIs, and public read
admission consumes that proof without requiring fake `.limit(1)` or `.limit(N)`
ceremony. The focused PocketIC capture also exposed and verified one
signed-numeric exact-key bug fix: string-backed fluent `FilterExpr::eq` on a
signed primary key now reaches `ByKey` / `ByKeys` instead of full-scan
admission rejection.

The implementation stayed within the intended 0.197 scope:

- no persisted-format change;
- no cursor-token format change;
- no recovery change;
- no cost-based optimizer;
- no 0.198 read-intent API;
- no generated-model runtime fallback;
- no public read-admission weakening for invalid, over-budget, or non-exact
  shapes.

## Patch Ledger Through 0.197.12

| Patch | Result |
| --- | --- |
| `0.197.0` | Landed exact primary-key filter, finite `IN (...)`, residual, external-primary-key, and EXPLAIN route proof coverage. |
| `0.197.1` | Added direct external-key filter-vs-`by_id` evidence and fail-closed invalid-residual / empty-key guards. |
| `0.197.2` | Hardened SQL cache and parameter boundaries, including literal-value cache separation and deterministic `IN` ordering. |
| `0.197.3` | Extended exact-key evidence across heap and journaled stores and non-canonical primary-key shapes. |
| `0.197.4` | Added commuted SQL primary-key equality and empty terminal behavior evidence. |
| `0.197.5` | Added SQL finite primary-key `IN (...)` ordering evidence for duplicate and unsorted input lists. |
| `0.197.7` | Pinned deduplicated key-count row-budget rejection for fluent and SQL key sets. |
| `0.197.8` | Added raw input-term and encoded-payload public-read caps for primary-key `IN (...)`. |
| `0.197.9` | Extended raw input resource caps to explicit typed `by_ids(...)`. |
| `0.197.10` | Cleaned up the typed `by_ids(...)` projection path to avoid duplicate typed-key conversion in admission planning. |
| `0.197.11` | Recorded the implementation-results ledger and focused exact-key scenario manifest. |
| `0.197.12` | Completed the 33-scenario focused manifest JSON, added saved before/after delta helpers and manifest gates, recorded a current PocketIC focused artifact, documented SQL placeholder / read-admission / fast-path boundaries, fixed signed numeric fluent exact-key predicates, and pinned exact-key `Empty` route proof for excluded `eq + IN` and disjoint `IN + IN` shapes. |

## Required Focused Artifact Status

The focused scenario manifest is now recorded in
[`focused-matrix-manifest.md`](focused-matrix-manifest.md) and
[`focused-matrix-manifest.json`](focused-matrix-manifest.json). The manifest is
not a measured before/after artifact. It is the scenario contract that the
future focused runner or artifact helper must emit.

| Required artifact | Status | Notes |
| --- | --- | --- |
| `sql_perf_197_pk_canonicalization_before.json` | Missing | Needed for before/after performance closeout. |
| `sql_perf_197_pk_canonicalization_after.json` | Recorded | Current PocketIC capture: 33 scenarios, 20 measured admitted rows, 3 public-policy contract rejections, 2 external-key contract rows, 1 not-found row, 24 aggregate `data_store.get` calls. |
| `sql_perf_197_pk_canonicalization_after.md` | Recorded | Human-readable current capture generated beside the JSON artifact. |
| `sql_perf_197_pk_canonicalization_delta.json` | Generator present, artifact missing | Generated by the ignored saved before/after helper once focused captures exist. |
| `sql_perf_197_pk_canonicalization_delta.md` | Generator present, artifact missing | Generated beside the delta JSON for human review once focused captures exist. |
| Fresh full deterministic SQL matrix before JSON/Markdown | Missing here | Historical artifacts are context only. |
| Fresh full deterministic SQL matrix after JSON/Markdown | Missing here | Required before broad performance claims. |

## PocketIC Measurement Attempts

The closeout follow-up added and ran the focused current capture:

```text
env IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1 TMPDIR=/home/adam/projects/icydb/.cache \
  cargo test -p icydb-testing-integration --test pk_canonicalization_focused_artifact \
  pk_canonicalization_focused_current_artifact_writes_from_pocketic -- --ignored --nocapture
```

Result: pass outside the sandbox. PocketIC listened on a loopback port and the
test generated:

- `sql_perf_197_pk_canonicalization_after.json`
- `sql_perf_197_pk_canonicalization_after.md`

The current capture is not a full performance closeout because there is no
matching before artifact or focused delta yet. It is current route/status/counter
evidence for the focused manifest.

The closeout follow-up attempted the existing manual SQL perf audit with the
repo-supported PocketIC download environment:

```text
env IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1 TMPDIR=/home/adam/projects/icydb/.cache \
  cargo test -p icydb-testing-integration --test sql_perf_audit \
  sql_perf_audit_harness_reports_instruction_samples -- --ignored --nocapture
```

First result: environmental failure under the sandbox. The PocketIC 14.0.0
binary resolved at
`/home/adam/projects/icydb/.cache/pocket-ic-server-14.0.0/pocket-ic`, but the
server process panicked while binding `127.0.0.1:0`, and the test harness then
hung until interrupted.

Retry result: pass outside the sandbox. PocketIC listened on a loopback port and
`sql_perf_audit_harness_reports_instruction_samples` completed successfully
with `1 passed`.

This proves the PocketIC-backed SQL perf harness can run in the current
worktree when local loopback binding is available. It does not by itself
produce the required 0.197 focused before/after delta or full deterministic
matrix artifacts.

## Current Closeout Classification

Classification: partial correctness closeout with before/delta and full-matrix
performance blockers.

0.197 can continue with narrow correctness, evidence, and cleanup slices. It
should not be declared fully closed until:

1. focused primary-key canonicalization before and delta artifacts exist and
   cover the scenario manifest;
2. a fresh full deterministic SQL matrix pair exists;
3. result/status/access deltas are classified;
4. any changed behavior is recorded as intended exact-key admission behavior;
5. broad performance claims are tied to route facts and counters.

The closeout audit follow-up resolved two documentation/proof mismatches:

- SQL placeholder parameters are now explicitly outside the current 0.197 SQL
  subset and remain fail-closed until a future SQL-parameterization line.
- The accepted implementation artifact model is the planner-selected access
  family plus key-input resource and explain snapshots; no frontend may
  rederive exact-key eligibility locally.

It also added planner-level evidence that contradictory exact-key intersections
select an empty access route instead of fetching one candidate and relying on
residual filtering, and it fixed signed-numeric exact-key fluent predicates so
accepted schema-compatible `Int64` literals may use exact primary-key access
under the planner-owned proof gate.

## Recommended Next Slices

1. Capture or reconstruct the same-machine focused 0.197 before artifact if a
   performance delta is still required.
2. Generate the focused delta from the saved before/current captures and run
   the saved-artifact gate.
3. Run the full deterministic SQL matrix after the focused artifacts can classify
   exact-key route/status deltas.
4. Only then prepare final 0.197 closeout wording.

## Validation Recorded For This Ledger

This ledger is based on source inspection, focused validation, one successful
PocketIC-backed focused current capture, and one successful PocketIC-backed SQL
perf audit retry. It did not complete the full workspace test suite, focused
before/delta matrix, full SQL matrix, or wasm-size measurement.
