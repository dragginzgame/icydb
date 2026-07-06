# 0.197 Focused Matrix Manifest

Date: 2026-07-06

Status: scenario contract recorded; measured focused artifacts not captured.

This manifest defines the minimum exact-key scenarios that a 0.197 focused
artifact helper must emit before final performance closeout. It is intentionally
separate from the full deterministic SQL matrix. The focused matrix proves the
exact-key canonicalization line directly; the full matrix proves the wider SQL
surface did not regress.

Required measured artifacts:

- `sql_perf_197_pk_canonicalization_before.json`
- `sql_perf_197_pk_canonicalization_after.json`
- `sql_perf_197_pk_canonicalization_delta.json`
- `sql_perf_197_pk_canonicalization_delta.md`

Required full-matrix artifacts for final line closeout:

- fresh before full deterministic SQL matrix JSON/Markdown
- fresh after full deterministic SQL matrix JSON/Markdown
- full union-key delta JSON/Markdown

## Required Fields

Every focused scenario row should record:

- `scenario_key`
- `surface`
- `store`
- `primary_key_kind`
- `query_shape`
- `terminal`
- before/after selected access
- before/after admission result
- before/after error code, if failed
- before/after total, planner, execute, and store instruction counts
- before/after `data_store.get`, index ranges, rows decoded, rows returned
- before/after deterministic result signatures
- canonicalization result: `ByKey`, `ByKeys`, `Empty`, `NotApplied`,
  `ValidationFailure`, or `UnsupportedByContract`
- raw and deduplicated key counts
- whether the result signature changed
- expected behavior change
- explanation for any status, result, or route change

## Required Scenario Keys

| Scenario key | Surface | Store | PK kind | Query shape | Current semantic evidence | Focused artifact status |
| --- | --- | --- | --- | --- | --- | --- |
| `pk.scalar.generated.filter.existing.try_one` | fluent | stable | generated | `filter(id = existing).try_one()` | `default_fluent_try_entity_admits_primary_key_filter_without_limit` | Missing |
| `pk.scalar.generated.filter.missing.try_one` | fluent | heap+journaled | generated | `filter(id = missing).try_one()` | `default_fluent_try_entity_returns_none_for_missing_heap_and_journaled_primary_key_filters` | Missing |
| `pk.scalar.generated.by_id.existing.try_one` | fluent | stable | generated | `by_id(existing).try_one()` | `default_fluent_try_entity_admits_primary_key_lookup_without_limit` | Missing |
| `pk.scalar.external.filter.existing.try_one` | fluent | stable | external | `filter(pid = existing).try_one()` | `default_fluent_try_entity_matches_by_id_for_external_primary_key_filter_without_limit` | Missing |
| `pk.scalar.external.by_id.existing.try_one` | fluent | stable | external | `by_id(Id::from_key(pid)).try_one()` | `default_fluent_try_entity_matches_by_id_for_external_primary_key_filter_without_limit` | Missing |
| `pk.sql.literal.generated.existing` | SQL | stable | generated | `WHERE id = literal` | `public_read_sql_admits_primary_key_filter_without_limit` | Missing |
| `pk.sql.literal.generated.commuted` | SQL | stable | generated | `WHERE literal = id` | `public_read_sql_admits_commuted_primary_key_filter_without_limit` | Missing |
| `pk.sql.parameter.unsupported` | SQL | stable | generated | `WHERE id = ?` | `public_read_sql_primary_key_parameter_shape_fails_before_admission` | Missing |
| `pk.sql.literal.generated.wrong_type` | SQL | stable | generated | `WHERE id = wrong_type` | `public_read_sql_primary_key_wrong_type_literal_fails_before_admission` | Missing |
| `pk.in.fluent.empty` | fluent | stable | generated | `filter(id IN ())` | `public_read_fluent_admission_canonicalizes_empty_primary_key_filters_without_limit` | Missing |
| `pk.in.fluent.one` | fluent | stable | generated | `filter(id IN (a))` | `typed_by_ids_matches_by_id_access`; `public_read_fluent_admission_admits_primary_key_in_filter_without_limit` | Missing |
| `pk.in.fluent.duplicates` | fluent | stable | generated | `filter(id IN (b, a, b))` | `default_fluent_execute_rows_orders_primary_key_in_filters_deterministically_without_limit` | Missing |
| `pk.in.fluent.multiple_mixed` | fluent | stable | generated | `filter(id IN (existing, missing, existing))` | `default_fluent_execute_rows_dedups_primary_key_in_filter_without_limit` | Missing |
| `pk.in.fluent.raw_terms_over_budget` | fluent | stable | generated | duplicate-heavy `IN` above raw input cap | `public_read_fluent_rejects_primary_key_in_input_terms_above_policy` | Missing |
| `pk.in.fluent.deduped_over_budget` | fluent | stable | generated | deduped key count above row cap | `public_read_fluent_rejects_primary_key_in_deduped_count_above_policy` | Missing |
| `pk.in.fluent.by_ids.raw_terms_over_budget` | fluent | stable | generated | duplicate-heavy `by_ids(...)` above raw input cap | `public_read_fluent_by_ids_rejects_duplicate_raw_input_terms_above_policy` | Missing |
| `pk.in.sql.duplicates.order_asc` | SQL | stable | generated | SQL `IN` duplicates ordered by primary key | `public_read_sql_primary_key_in_filter_orders_deterministically_without_limit` | Missing |
| `pk.in.sql.payload_over_budget` | SQL | stable | generated | SQL `IN` encoded payload above cap | `public_read_sql_rejects_primary_key_in_payload_bytes_above_policy` | Missing |
| `pk.residual.eq.true` | fluent | stable | generated | `id = value AND residual_true` | `default_fluent_execute_rows_applies_residual_after_primary_key_filter_without_limit` | Missing |
| `pk.residual.eq.false` | fluent+SQL | stable | generated | `id = value AND residual_false` | `default_fluent_execute_rows_applies_residual_after_primary_key_filter_without_limit`; `public_read_sql_applies_residual_after_primary_key_filter_without_limit` | Missing |
| `pk.residual.eq.invalid_existing` | fluent | stable | generated | `id = existing AND unknown_field = x` | `public_read_fluent_admission_fails_invalid_residual_after_primary_key_filter` | Missing |
| `pk.residual.eq.invalid_missing` | fluent | stable | generated | `id = missing AND unknown_field = x` | `public_read_fluent_admission_fails_invalid_residual_after_primary_key_filter` | Missing |
| `pk.empty.contradictory_eq` | fluent | stable | generated | `id = a AND id = b` | `default_fluent_execute_rows_returns_empty_for_empty_primary_key_filters_without_limit` | Missing |
| `pk.empty.eq_and_excluding_in` | fluent | stable | generated | `id = a AND id IN (b)` | `public_read_fluent_admission_narrows_primary_key_eq_and_in_filter_without_limit` | Missing |
| `pk.empty.count` | fluent | stable | generated | empty exact-key count terminal | `default_fluent_count_returns_zero_for_empty_primary_key_filters_without_limit` | Missing |
| `pk.empty.require_one` | fluent | stable | generated | empty exact-key required-one terminal | `default_fluent_require_one_reports_not_found_for_empty_primary_key_filters_without_limit` | Missing |
| `pk.store.heap.existing` | fluent | heap | generated | heap `id = existing` | `public_read_fluent_admission_admits_heap_and_journaled_primary_key_filters_without_limit` | Missing |
| `pk.store.journaled.existing` | fluent | journaled | generated | journaled `id = existing` | `public_read_fluent_admission_admits_heap_and_journaled_primary_key_filters_without_limit` | Missing |
| `pk.store.heap.deleted` | fluent | heap | generated | deleted heap `id = value` | `default_fluent_try_entity_returns_none_for_deleted_heap_and_journaled_primary_key_filters` | Missing |
| `pk.store.journaled.deleted` | fluent | journaled | generated | deleted journaled `id = value` | `default_fluent_try_entity_returns_none_for_deleted_heap_and_journaled_primary_key_filters` | Missing |
| `pk.noncanonical.unique_secondary` | fluent | stable | generated | unique secondary equality | `public_read_fluent_admission_keeps_unique_secondary_equality_off_primary_key_access` | Missing |
| `pk.noncanonical.partial_composite` | fluent | stable | composite | partial composite primary-key equality | `public_read_fluent_admission_rejects_partial_composite_primary_key_as_full_scan` | Missing |
| `pk.noncanonical.expression_wrapped` | SQL | stable | generated | expression-wrapped primary-key compare | `sql_explain_expression_wrapped_primary_key_does_not_canonicalize_to_exact_key` | Missing |

The noncanonical scenarios are valid non-application cases and should report
`NotApplied`, not validation failure, unless the query is otherwise invalid.

## Closeout Gate

The focused 0.197 artifact gate should fail if:

- any required scenario key is missing;
- any common-success scenario lacks route/access facts;
- any common-success scenario lacks instruction totals;
- any expected exact-key scenario does not select `ByKey`, `ByKeys`, or `Empty`;
- any invalid, wrong-type, unsupported, or over-budget scenario falls back to a
  scan;
- any result signature changes without an explicit intended-behavior
  explanation;
- any focused target lacks before or after data;
- any performance claim lacks access-counter evidence.

## Current Interpretation

The current source tree has strong semantic coverage for these scenarios, but
the measured focused matrix artifacts do not exist yet. Therefore `.11` may
record coverage and closeout requirements, but final 0.197 performance closeout
must wait for fresh focused and full-matrix artifacts.

The saved-artifact gate lives in
`testing/integration/tests/pk_canonicalization_focused_artifact.rs`. Run the
ignored generator after focused before/after capture with:

```text
ICYDB_197_PK_FOCUSED_BEFORE_JSON=<path-to-before-json> \
ICYDB_197_PK_FOCUSED_AFTER_JSON=<path-to-after-json> \
ICYDB_197_PK_FOCUSED_DELTA_OUT=<path-to-delta-json> \
cargo test -p icydb-testing-integration --test pk_canonicalization_focused_artifact \
  pk_canonicalization_focused_delta_writes_from_saved_before_after_artifacts -- --ignored
```

The generator writes both `<path-to-delta-json>` and the matching `.md` file,
then immediately applies the manifest gate to the generated delta.

Run the ignored delta gate directly after capture with:

```text
ICYDB_197_PK_FOCUSED_DELTA_JSON=<path-to-delta-json> \
cargo test -p icydb-testing-integration --test pk_canonicalization_focused_artifact \
  pk_canonicalization_focused_delta_covers_manifest -- --ignored
```
