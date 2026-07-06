# 0.197 Implementation Results

Date: 2026-07-06

Commit inspected while creating this ledger:
`3faf7b7655aabc8175b9bc2e10933b5f92883443`

Status: implementation in progress, correctness evidence recorded, performance
closeout not claimed.

0.197 is the deterministic optimizer canonicalization line. It is not a
cost-based optimizer line and not a 0.198 read-intent ergonomics line. The
implementation through `0.197.10` has landed primary-key exact-access behavior
and public-read admission hardening, but this document intentionally does not
claim final 0.197 closeout because fresh focused before/after performance
artifacts and a fresh full deterministic SQL matrix pair have not been recorded
here.

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
| SQL parameter primary-key equality | Unsupported by SQL parameter contract | `public_read_sql_primary_key_parameter_shape_fails_before_admission`; `prepare_sql_statement_rejects_parameters_before_lowering` | Non-blocking until SQL parameters are supported |
| SQL finite primary-key `IN (...)` | Implemented | `public_read_sql_admits_primary_key_in_filter_without_limit`; `public_read_sql_primary_key_in_filter_orders_deterministically_without_limit` | Covered |
| Wrong-type primary-key literals | Fail closed | `public_read_sql_primary_key_wrong_type_literal_fails_before_admission`; `public_read_sql_commuted_primary_key_wrong_type_literal_fails_before_admission` | Covered |
| Invalid residual predicates | Fail before admission | `public_read_fluent_admission_fails_invalid_residual_after_primary_key_filter` | Covered |
| Residual filters after exact key | Implemented | `default_fluent_execute_rows_applies_residual_after_primary_key_filter_without_limit`; `public_read_sql_applies_residual_after_primary_key_filter_without_limit` | Covered |
| Residual filters after exact key set | Implemented | `default_fluent_execute_rows_applies_residual_after_primary_key_in_filter_without_limit`; `public_read_sql_applies_residual_after_primary_key_in_filter_without_limit` | Covered |
| Empty exact-key result | Implemented | `public_read_fluent_admission_canonicalizes_empty_primary_key_filters_without_limit`; `default_fluent_execute_rows_returns_empty_for_empty_primary_key_filters_without_limit` | Covered |
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
| Focused before/after perf artifacts | Missing | No `sql_perf_197_pk_canonicalization_before.json`, `after.json`, or `delta.md` exists yet | Blocks performance closeout |
| Fresh full deterministic SQL matrix | Missing from this design directory | No fresh full before/after matrix pair recorded here | Blocks final performance closeout |

## Behavior Result

The line has moved beyond diagnostic-only behavior. Exact scalar primary-key
filters and finite primary-key `IN (...)` filters now feed the same bounded
`ByKey` / `ByKeys` proof family used by explicit key APIs, and public read
admission consumes that proof without requiring fake `.limit(1)` or `.limit(N)`
ceremony.

The implementation stayed within the intended 0.197 scope:

- no persisted-format change;
- no cursor-token format change;
- no recovery change;
- no cost-based optimizer;
- no 0.198 read-intent API;
- no generated-model runtime fallback;
- no public read-admission weakening for invalid, over-budget, or non-exact
  shapes.

## Patch Ledger Through 0.197.10

| Patch | Result |
| --- | --- |
| `0.197.0` | Landed exact primary-key filter, finite `IN (...)`, residual, external-primary-key, and EXPLAIN route proof coverage. |
| `0.197.1` | Added direct external-key filter-vs-`by_id` evidence and fail-closed invalid-residual / empty-key guards. |
| `0.197.2` | Hardened SQL cache and parameter boundaries, including literal-value cache separation and deterministic `IN` ordering. |
| `0.197.3` | Extended exact-key evidence across heap and journaled stores and non-canonical primary-key shapes. |
| `0.197.4` | Added commuted SQL primary-key equality and empty terminal behavior evidence. |
| `0.197.5` | Added SQL finite primary-key `IN (...)` ordering evidence for duplicate and unsorted input lists. |
| `0.197.6` | Admitted finite exact-key sets with non-key materialized ordering when the key set bounds the candidate rows. |
| `0.197.7` | Pinned deduplicated key-count row-budget rejection for fluent and SQL key sets. |
| `0.197.8` | Added raw input-term and encoded-payload public-read caps for primary-key `IN (...)`. |
| `0.197.9` | Extended raw input resource caps to explicit typed `by_ids(...)`. |
| `0.197.10` | Cleaned up the typed `by_ids(...)` projection path to avoid duplicate typed-key conversion in admission planning. |

## Required Focused Artifact Status

The focused scenario manifest is now recorded in
[`focused-matrix-manifest.md`](focused-matrix-manifest.md) and
[`focused-matrix-manifest.json`](focused-matrix-manifest.json). The manifest is
not a measured before/after artifact. It is the scenario contract that the
future focused runner or artifact helper must emit.

| Required artifact | Status | Notes |
| --- | --- | --- |
| `sql_perf_197_pk_canonicalization_before.json` | Missing | Needed for performance closeout. |
| `sql_perf_197_pk_canonicalization_after.json` | Missing | Needed for performance closeout. |
| `sql_perf_197_pk_canonicalization_delta.md` | Missing | Needed for performance closeout and release notes. |
| Fresh full deterministic SQL matrix before JSON/Markdown | Missing here | Historical artifacts are context only. |
| Fresh full deterministic SQL matrix after JSON/Markdown | Missing here | Required before broad performance claims. |

## Current Closeout Classification

Classification: partial correctness closeout with performance/artifact blockers.

0.197 can continue with narrow correctness, evidence, and cleanup slices. It
should not be declared fully closed until:

1. focused primary-key canonicalization before/after artifacts exist and cover
   the scenario manifest;
2. a fresh full deterministic SQL matrix pair exists;
3. result/status/access deltas are classified;
4. any changed behavior is recorded as intended exact-key admission behavior;
5. broad performance claims are tied to route facts and counters.

## Recommended Next Slices

1. Add a focused 0.197 exact-key matrix runner or artifact helper that consumes
   the scenario manifest and emits the required before/after/delta files.
2. Record current focused results for every required manifest scenario.
3. Run the full deterministic SQL matrix after the focused artifact can classify
   exact-key route/status deltas.
4. Only then prepare final 0.197 closeout wording.

## Validation Recorded For This Ledger

This ledger is based on source inspection and existing changelog entries. It did
not rerun the full workspace test suite, focused performance matrix, full SQL
matrix, or wasm-size measurement.
