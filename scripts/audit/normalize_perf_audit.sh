#!/usr/bin/env bash

set -euo pipefail

artifact_dir="${1:?usage: normalize_perf_audit.sh <artifact-dir>}"
samples_path="${artifact_dir}/demo_rpg-samples.json"
manifest_path="${artifact_dir}/scenario-manifest.json"
rows_path="${artifact_dir}/instruction-rows.json"

method_tag="PERF-0.3-demo_rpg-pocketic-surface-sampling-expanded"
generated_at_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
freshness_model="fresh_canister_per_scenario + repeated_within_one_query_call"

jq \
    --arg method_tag "${method_tag}" \
    --arg generated_at_utc "${generated_at_utc}" \
    --arg freshness_model "${freshness_model}" \
    '
    def key: .scenario_key;
    def key_has($needle): key | contains($needle);

    def catalog_entity:
      if key_has("show_entities") then "Catalog" else (.sample.outcome.entity // "User") end;

    def entry_surface:
      if .sample.surface == "GeneratedDispatch" then "test_canister_sql_dispatch"
      elif .sample.surface == "TypedDispatchUser" then "execute_sql_dispatch"
      elif .sample.surface == "TypedQueryFromSqlUserExecute" then "query_from_sql_execute_query"
      elif .sample.surface == "TypedExecuteSqlUser" then "execute_sql"
      elif .sample.surface == "TypedInsertUser" then "typed_insert"
      elif (.sample.surface | startswith("TypedInsertManyAtomicUser")) then "typed_insert_many_atomic"
      elif (.sample.surface | startswith("TypedInsertManyNonAtomicUser")) then "typed_insert_many_non_atomic"
      elif .sample.surface == "TypedUpdateUser" then "typed_update"
      elif .sample.surface == "FluentDeleteUserOrderIdLimit1Count" then "fluent_delete"
      elif .sample.surface == "FluentDeletePerfUserCount" then "fluent_delete"
      elif .sample.surface == "TypedExecuteSqlGroupedUser" then "execute_sql_grouped"
      elif .sample.surface == "TypedExecuteSqlGroupedUserSecondPage" then "execute_sql_grouped_second_page"
      elif .sample.surface == "TypedExecuteSqlAggregateUser" then "execute_sql_aggregate"
      elif (.sample.surface | startswith("FluentLoad")) then "fluent_load"
      elif (.sample.surface | startswith("FluentPaged")) then "fluent_paged"
      else .sample.surface
      end;

    def query_family:
      if key_has("show_indexes") or key_has("show_columns") or key_has("show_entities") or key_has("describe") then "metadata_lane"
      elif key_has("explain") then "explain"
      elif key_has("delete") then "delete_mutation"
      elif key_has("insert") or key_has("update") or key_has("replace") then "write_mutation"
      elif key_has("starts_with") then "predicate_load"
      elif key_has("computed_projection") then "computed_projection"
      elif key_has("grouped") then "grouped_aggregate"
      elif key_has("aggregate") then "global_aggregate"
      elif key_has("rejection") then "rejection_path"
      else "scalar_load"
      end;

    def arg_class:
      if key_has("invalid_cursor") then "invalid_cursor_path"
      elif (.sample.outcome.success | not) then "rejection_unsupported_path"
      elif key_has("show_entities") or key_has("show_indexes") or key_has("show_columns") or key_has("describe") then "minimal_valid_query"
      else "representative_valid_query"
      end;

    def predicate_shape:
      if key_has("name_eq") then "name_eq"
      elif key_has("starts_with") then "starts_with"
      elif key_has("having_empty") then "having_count_gt_1000"
      elif key_has("invalid_cursor") then "invalid_cursor"
      elif (.sample.outcome.success | not) and key_has("rejection") then "unsupported"
      else "none"
      end;

    def projection_shape:
      if .sample.outcome.result_kind == "write_response" then "write_response"
      elif .sample.outcome.result_kind == "write_batch_response" then "write_batch_response"
      elif .sample.outcome.result_kind == "delete_count" then "delete_count_only"
      elif key_has("computed_projection") then "computed_projection"
      elif key_has("starts_with") then "field_projection"
      elif key_has("delete") then "delete_projection"
      elif key_has("projection") then "field_projection"
      elif key_has("grouped") then "group_key_plus_aggregate"
      elif key_has("aggregate") then "scalar_value"
      elif query_family == "metadata_lane" then "metadata_payload"
      elif query_family == "explain" then "explain_text"
      elif (key | startswith("fluent.")) or key_has("scalar_limit") then "whole_row"
      else "none"
      end;

    def aggregate_shape:
      if key_has("grouped.user_age_count") then "count_star_grouped"
      elif key_has("aggregate.user_count") then "count_star_global"
      else "none"
      end;

    def order_shape:
      if .sample.sql | contains("ORDER BY id") then "order_by_id"
      elif .sample.sql | contains("ORDER BY age") then "order_by_age"
      else "none"
      end;

    def page_shape:
      if key_has("first_page") or key_has("second_page") then "paged_limit_2"
      elif key_has("invalid_cursor") then "paged_limit_2_invalid_cursor"
      elif .sample.sql | contains("LIMIT") then "limit_only"
      else "none"
      end;

    def cursor_state:
      if key_has("first_page") then "first_page"
      elif key_has("second_page") then "second_page"
      elif key_has("invalid_cursor") then "invalid"
      else "none"
      end;

    def result_cardinality_class:
      if (.sample.outcome.success | not) then "error"
      elif .sample.outcome.result_kind == "aggregate_value" then "scalar_value"
      elif .sample.outcome.result_kind == "explain" then "text_output"
      elif query_family == "metadata_lane" then "metadata_payload"
      elif .sample.outcome.row_count == 0 then "empty"
      elif .sample.outcome.row_count == 1 then "one_row"
      elif .sample.outcome.row_count == 2 then "two_rows"
      elif .sample.outcome.row_count == 3 then "three_rows"
      else "many_rows"
      end;

    {
      method_tag: $method_tag,
      generated_at_utc: $generated_at_utc,
      status: "numeric_baseline_partial",
      freshness_model: $freshness_model,
      scenarios: map({
        scenario_key,
        entity: catalog_entity,
        entry_surface: entry_surface,
        query_family: query_family,
        arg_class: arg_class,
        predicate_shape: predicate_shape,
        projection_shape: projection_shape,
        aggregate_shape: aggregate_shape,
        order_shape: order_shape,
        page_shape: page_shape,
        cursor_state: cursor_state,
        result_cardinality_class: result_cardinality_class,
        store_state: "fresh_demo_rpg_fixture_canister",
        index_state: "demo_rpg_default_indexes",
        freshness_model: $freshness_model,
        method_tag: $method_tag,
        verification_status: "measured"
      })
    }
    ' \
    "${samples_path}" \
    > "${manifest_path}"

jq \
    --arg method_tag "${method_tag}" \
    --arg generated_at_utc "${generated_at_utc}" \
    '
    def key: .scenario_key;
    def key_has($needle): key | contains($needle);

    def entry_surface:
      if .sample.surface == "GeneratedDispatch" then "test_canister_sql_dispatch"
      elif .sample.surface == "TypedDispatchUser" then "execute_sql_dispatch"
      elif .sample.surface == "TypedQueryFromSqlUserExecute" then "query_from_sql_execute_query"
      elif .sample.surface == "TypedExecuteSqlUser" then "execute_sql"
      elif .sample.surface == "TypedInsertUser" then "typed_insert"
      elif (.sample.surface | startswith("TypedInsertManyAtomicUser")) then "typed_insert_many_atomic"
      elif (.sample.surface | startswith("TypedInsertManyNonAtomicUser")) then "typed_insert_many_non_atomic"
      elif .sample.surface == "TypedUpdateUser" then "typed_update"
      elif .sample.surface == "FluentDeleteUserOrderIdLimit1Count" then "fluent_delete"
      elif .sample.surface == "FluentDeletePerfUserCount" then "fluent_delete"
      elif .sample.surface == "TypedExecuteSqlGroupedUser" then "execute_sql_grouped"
      elif .sample.surface == "TypedExecuteSqlGroupedUserSecondPage" then "execute_sql_grouped_second_page"
      elif .sample.surface == "TypedExecuteSqlAggregateUser" then "execute_sql_aggregate"
      elif (.sample.surface | startswith("FluentLoad")) then "fluent_load"
      elif (.sample.surface | startswith("FluentPaged")) then "fluent_paged"
      else .sample.surface
      end;

    def phase_kind:
      if key_has("explain") then "explain"
      elif key_has("delete") then "delete_mutation"
      elif key_has("insert") or key_has("update") or key_has("replace") then "write_mutation"
      elif key_has("invalid_cursor") or key_has("first_page") or key_has("second_page") then "cursor"
      elif key_has("starts_with") then "predicate"
      elif key_has("computed_projection") then "projection"
      elif key_has("grouped") then "grouped"
      elif key_has("aggregate") then "aggregate"
      else null
      end;

    {
      method_tag: $method_tag,
      generated_at_utc: $generated_at_utc,
      status: "partial_authoritative_capture",
      rows: map({
        subject_kind: "query_surface",
        subject_label: .scenario_key,
        entry_surface: entry_surface,
        count: .sample.repeat_count,
        total_local_instructions: .sample.total_local_instructions,
        avg_local_instructions: .sample.avg_local_instructions,
        scenario_key,
        scenario_labels: (.scenario_key | split(".")),
        entity_scope: (.sample.outcome.entity // (if key_has("show_entities") then "Catalog" else "User" end)),
        query_shape_key: .scenario_key,
        query_shape_labels: (.scenario_key | split(".")),
        sample_origin: "instruction_harness",
        phase_kind: phase_kind,
        result_kind: .sample.outcome.result_kind,
        success: .sample.outcome.success
      })
    }
    ' \
    "${samples_path}" \
    > "${rows_path}"
