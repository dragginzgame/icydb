//! Module: metrics::tests
//! Covers metrics sink and state behavior used by runtime instrumentation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::metrics::{
    sink::{
        CacheKind, CacheMissReason, CacheOutcome, MetricsEvent, PlanChoiceReason,
        PreparedShapeFinalizationOutcome, SchemaReconcileOutcome, SchemaTransitionOutcome,
        SqlCompileRejectPhase, record,
    },
    state::{
        EntityCounters, EventOps, MetricRatio, report_window_start, reset_all, with_state,
        with_state_mut,
    },
};
use candid::types::{CandidType, Label, Type, TypeInner};

fn expect_record_fields(ty: Type) -> Vec<String> {
    match ty.as_ref() {
        TypeInner::Record(fields) => fields
            .iter()
            .map(|field| match field.id.as_ref() {
                Label::Named(name) => name.clone(),
                other => panic!("expected named record field, got {other:?}"),
            })
            .collect(),
        other => panic!("expected candid record, got {other:?}"),
    }
}

#[test]
fn reset_all_clears_state() {
    with_state_mut(|m| {
        m.ops.load_calls = 3;
        m.ops.index_inserts = 2;
        m.perf.save_inst_max = 9;
        m.entities.insert(
            "alpha".to_string(),
            EntityCounters {
                load_calls: 1,
                ..Default::default()
            },
        );
    });

    reset_all();

    with_state(|m| {
        assert_eq!(m.ops.load_calls, 0);
        assert_eq!(m.ops.index_inserts, 0);
        assert_eq!(m.perf.save_inst_max, 0);
        assert!(m.entities.is_empty());
    });
}

#[test]
fn report_sorts_entities_by_visible_activity() {
    reset_all();
    with_state_mut(|m| {
        m.entities.insert(
            "alpha".to_string(),
            EntityCounters {
                load_calls: 2,
                rows_loaded: 6,
                ..Default::default()
            },
        );
        m.entities.insert(
            "beta".to_string(),
            EntityCounters {
                save_calls: 1,
                rows_saved: 9,
                ..Default::default()
            },
        );
        m.entities.insert(
            "gamma".to_string(),
            EntityCounters {
                load_calls: 2,
                rows_loaded: 6,
                ..Default::default()
            },
        );
    });

    let report = report_window_start(None);
    let summaries = report.entity_counters();
    let paths: Vec<_> = summaries
        .iter()
        .map(super::state::EntitySummary::path)
        .collect();

    // Order by total visible activity desc, then row counters, then path asc.
    assert_eq!(paths, ["beta", "alpha", "gamma"]);
}

#[test]
fn event_report_candid_shape_is_stable() {
    reset_all();
    with_state_mut(|state| {
        state.ops.load_calls = 1;
        state.ops.rows_loaded = 2;
        state.ops.rows_scanned = 3;
        state.ops.non_atomic_partial_rows_committed = 4;
        state.perf.load_inst_total = 11;
        state.perf.load_inst_max = 12;
        state.entities.insert(
            "alpha".to_string(),
            EntityCounters {
                load_calls: 5,
                rows_loaded: 8,
                ..Default::default()
            },
        );
        state.window_start_ms = 99;
    });
    let report = report_window_start(None);

    let report_fields = expect_record_fields(crate::metrics::state::EventReport::ty());
    for field in [
        "counters",
        "entity_counters",
        "window_filter_matched",
        "requested_window_start_ms",
        "active_window_start_ms",
    ] {
        assert!(
            report_fields.iter().any(|candidate| candidate == field),
            "EventReport must keep `{field}` as Candid field key",
        );
    }

    let counters_fields = expect_record_fields(crate::metrics::state::EventCounters::ty());
    for field in [
        "ops",
        "perf",
        "window_start_ms",
        "window_end_ms",
        "window_duration_ms",
    ] {
        assert!(
            counters_fields.iter().any(|candidate| candidate == field),
            "EventCounters must keep `{field}` as Candid field key",
        );
    }

    let counters = report
        .counters()
        .expect("event report fixture should retain counters for populated state");
    assert!(report.window_filter_matched());
    assert_eq!(report.requested_window_start_ms(), None);
    assert_eq!(report.active_window_start_ms(), 99);
    assert_eq!(counters.window_start_ms(), 99);
    assert!(counters.window_end_ms() >= counters.window_start_ms());
    assert_eq!(
        counters.window_duration_ms(),
        counters
            .window_end_ms()
            .saturating_sub(counters.window_start_ms()),
    );
}

// The stable Candid shape test intentionally keeps the public field inventory
// in one array so new metrics counters cannot update the DTO without updating
// the compatibility assertion.
#[expect(clippy::too_many_lines)]
#[test]
fn event_ops_candid_shape_exposes_detailed_plan_counters() {
    let fields = expect_record_fields(crate::metrics::state::EventOps::ty());

    for field in [
        "save_insert_calls",
        "save_update_calls",
        "save_replace_calls",
        "exec_success",
        "exec_error_corruption",
        "exec_error_incompatible_persisted_format",
        "exec_error_not_found",
        "exec_error_internal",
        "exec_error_conflict",
        "exec_error_unsupported",
        "exec_error_invariant_violation",
        "exec_aborted",
        "cache_shared_query_plan_hits",
        "cache_shared_query_plan_misses",
        "cache_shared_query_plan_inserts",
        "cache_shared_query_plan_entries",
        "cache_shared_query_plan_miss_cold",
        "cache_shared_query_plan_miss_distinct_key",
        "cache_shared_query_plan_miss_method_version",
        "cache_shared_query_plan_miss_schema_fingerprint",
        "cache_shared_query_plan_miss_visibility",
        "cache_sql_compiled_command_hits",
        "cache_sql_compiled_command_misses",
        "cache_sql_compiled_command_inserts",
        "cache_sql_compiled_command_entries",
        "cache_sql_compiled_command_miss_cold",
        "cache_sql_compiled_command_miss_distinct_key",
        "cache_sql_compiled_command_miss_method_version",
        "cache_sql_compiled_command_miss_schema_fingerprint",
        "cache_sql_compiled_command_miss_surface",
        "schema_reconcile_checks",
        "schema_reconcile_exact_match",
        "schema_reconcile_first_create",
        "schema_reconcile_latest_snapshot_corrupt",
        "schema_reconcile_rejected_field_slot",
        "schema_reconcile_rejected_other",
        "schema_reconcile_rejected_row_layout",
        "schema_reconcile_rejected_schema_version",
        "schema_reconcile_store_write_error",
        "schema_transition_checks",
        "schema_transition_append_only_nullable_fields",
        "schema_transition_exact_match",
        "schema_transition_rejected_entity_identity",
        "schema_transition_rejected_field_contract",
        "schema_transition_rejected_field_slot",
        "schema_transition_rejected_row_layout",
        "schema_transition_rejected_schema_version",
        "schema_transition_rejected_snapshot",
        "schema_store_snapshots",
        "schema_store_encoded_bytes",
        "schema_store_latest_snapshot_bytes",
        "accepted_schema_fields",
        "accepted_schema_nested_leaf_facts",
        "sql_compile_rejects",
        "sql_compile_reject_cache_key",
        "sql_compile_reject_parse",
        "sql_compile_reject_semantic",
        "plan_by_key",
        "plan_by_keys",
        "plan_key_range",
        "plan_index_prefix",
        "plan_index_multi_lookup",
        "plan_index_range",
        "plan_explicit_full_scan",
        "plan_union",
        "plan_intersection",
        "plan_choice_conflicting_primary_key_children_access_preferred",
        "plan_choice_constant_false_predicate",
        "plan_choice_empty_child_access_preferred",
        "plan_choice_full_scan_access",
        "plan_choice_intent_key_access_override",
        "plan_choice_limit_zero_window",
        "plan_choice_non_index_access",
        "plan_choice_planner_composite_non_index",
        "plan_choice_planner_full_scan_fallback",
        "plan_choice_planner_key_set_access",
        "plan_choice_planner_primary_key_lookup",
        "plan_choice_planner_primary_key_range",
        "plan_choice_required_order_primary_key_range_preferred",
        "plan_choice_singleton_primary_key_child_access_preferred",
        "prepared_shape_already_finalized",
        "prepared_shape_generated_fallback",
        "rows_inserted",
        "rows_updated",
        "rows_replaced",
        "load_candidate_rows_scanned",
        "load_candidate_rows_filtered",
        "load_result_rows_emitted",
        "sql_insert_calls",
        "sql_insert_select_calls",
        "sql_update_calls",
        "sql_delete_calls",
        "sql_write_matched_rows",
        "sql_write_mutated_rows",
        "sql_write_returning_rows",
        "sql_write_error_insert",
        "sql_write_error_insert_select",
        "sql_write_error_update",
        "sql_write_error_delete",
        "sql_write_error_corruption",
        "sql_write_error_incompatible_persisted_format",
        "sql_write_error_not_found",
        "sql_write_error_internal",
        "sql_write_error_conflict",
        "sql_write_error_unsupported",
        "sql_write_error_invariant_violation",
        "write_rows_touched",
        "write_index_entries_changed",
        "write_reverse_index_entries_changed",
        "write_relation_checks",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "EventOps must keep `{field}` as Candid field key",
        );
    }
}

#[test]
fn schema_reconcile_metrics_accumulate_by_outcome_and_entity() {
    reset_all();

    for outcome in [
        SchemaReconcileOutcome::FirstCreate,
        SchemaReconcileOutcome::ExactMatch,
        SchemaReconcileOutcome::LatestSnapshotCorrupt,
        SchemaReconcileOutcome::RejectedFieldSlot,
        SchemaReconcileOutcome::RejectedOther,
        SchemaReconcileOutcome::RejectedRowLayout,
        SchemaReconcileOutcome::RejectedSchemaVersion,
        SchemaReconcileOutcome::StoreWriteError,
    ] {
        record(MetricsEvent::SchemaReconcile {
            entity_path: "metrics::tests::SchemaEntity",
            outcome,
        });
    }

    let report = report_window_start(None);
    let counters = report
        .counters()
        .expect("schema reconciliation fixture should produce counters");
    let ops = counters.ops();
    assert_eq!(ops.schema_reconcile_checks(), 8);
    assert_eq!(ops.schema_reconcile_first_create(), 1);
    assert_eq!(ops.schema_reconcile_exact_match(), 1);
    assert_eq!(ops.schema_reconcile_latest_snapshot_corrupt(), 1);
    assert_eq!(ops.schema_reconcile_rejected_field_slot(), 1);
    assert_eq!(ops.schema_reconcile_rejected_other(), 1);
    assert_eq!(ops.schema_reconcile_rejected_row_layout(), 1);
    assert_eq!(ops.schema_reconcile_rejected_schema_version(), 1);
    assert_eq!(ops.schema_reconcile_store_write_error(), 1);
    assert_eq!(
        ops.schema_reconcile_checks(),
        ops.schema_reconcile_first_create()
            .saturating_add(ops.schema_reconcile_exact_match())
            .saturating_add(ops.schema_reconcile_latest_snapshot_corrupt())
            .saturating_add(ops.schema_reconcile_rejected_field_slot())
            .saturating_add(ops.schema_reconcile_rejected_other())
            .saturating_add(ops.schema_reconcile_rejected_row_layout())
            .saturating_add(ops.schema_reconcile_rejected_schema_version())
            .saturating_add(ops.schema_reconcile_store_write_error()),
        "schema reconcile total must equal its outcome buckets",
    );

    let summary = report
        .entity_counters()
        .first()
        .expect("schema reconciliation fixture should produce an entity summary");
    assert_eq!(summary.path(), "metrics::tests::SchemaEntity");
    assert_eq!(summary.schema_reconcile_checks(), 8);
    assert_eq!(summary.schema_reconcile_first_create(), 1);
    assert_eq!(summary.schema_reconcile_exact_match(), 1);
    assert_eq!(summary.schema_reconcile_latest_snapshot_corrupt(), 1);
    assert_eq!(summary.schema_reconcile_rejected_field_slot(), 1);
    assert_eq!(summary.schema_reconcile_rejected_other(), 1);
    assert_eq!(summary.schema_reconcile_rejected_row_layout(), 1);
    assert_eq!(summary.schema_reconcile_rejected_schema_version(), 1);
    assert_eq!(summary.schema_reconcile_store_write_error(), 1);
    assert_eq!(
        summary.schema_reconcile_checks(),
        summary
            .schema_reconcile_first_create()
            .saturating_add(summary.schema_reconcile_exact_match())
            .saturating_add(summary.schema_reconcile_latest_snapshot_corrupt())
            .saturating_add(summary.schema_reconcile_rejected_field_slot())
            .saturating_add(summary.schema_reconcile_rejected_other())
            .saturating_add(summary.schema_reconcile_rejected_row_layout())
            .saturating_add(summary.schema_reconcile_rejected_schema_version())
            .saturating_add(summary.schema_reconcile_store_write_error()),
        "entity schema reconcile total must equal its outcome buckets",
    );
}

#[test]
fn schema_transition_metrics_accumulate_by_outcome_and_entity() {
    reset_all();

    for outcome in [
        SchemaTransitionOutcome::AppendOnlyNullableFields,
        SchemaTransitionOutcome::ExactMatch,
        SchemaTransitionOutcome::RejectedEntityIdentity,
        SchemaTransitionOutcome::RejectedFieldContract,
        SchemaTransitionOutcome::RejectedFieldSlot,
        SchemaTransitionOutcome::RejectedRowLayout,
        SchemaTransitionOutcome::RejectedSchemaVersion,
        SchemaTransitionOutcome::RejectedSnapshot,
    ] {
        record(MetricsEvent::SchemaTransition {
            entity_path: "metrics::tests::SchemaEntity",
            outcome,
        });
    }

    let report = report_window_start(None);
    let counters = report
        .counters()
        .expect("schema transition fixture should produce counters");
    let ops = counters.ops();
    assert_eq!(ops.schema_transition_checks(), 8);
    assert_eq!(ops.schema_transition_append_only_nullable_fields(), 1);
    assert_eq!(ops.schema_transition_exact_match(), 1);
    assert_eq!(ops.schema_transition_rejected_entity_identity(), 1);
    assert_eq!(ops.schema_transition_rejected_field_contract(), 1);
    assert_eq!(ops.schema_transition_rejected_field_slot(), 1);
    assert_eq!(ops.schema_transition_rejected_row_layout(), 1);
    assert_eq!(ops.schema_transition_rejected_schema_version(), 1);
    assert_eq!(ops.schema_transition_rejected_snapshot(), 1);
    assert_eq!(
        ops.schema_transition_checks(),
        ops.schema_transition_append_only_nullable_fields()
            .saturating_add(ops.schema_transition_exact_match())
            .saturating_add(ops.schema_transition_rejected_entity_identity())
            .saturating_add(ops.schema_transition_rejected_field_contract())
            .saturating_add(ops.schema_transition_rejected_field_slot())
            .saturating_add(ops.schema_transition_rejected_row_layout())
            .saturating_add(ops.schema_transition_rejected_schema_version())
            .saturating_add(ops.schema_transition_rejected_snapshot()),
        "schema transition total must equal its outcome buckets",
    );

    let summary = report
        .entity_counters()
        .first()
        .expect("schema transition fixture should produce an entity summary");
    assert_eq!(summary.path(), "metrics::tests::SchemaEntity");
    assert_eq!(summary.schema_transition_checks(), 8);
    assert_eq!(summary.schema_transition_append_only_nullable_fields(), 1);
    assert_eq!(summary.schema_transition_exact_match(), 1);
    assert_eq!(summary.schema_transition_rejected_entity_identity(), 1);
    assert_eq!(summary.schema_transition_rejected_field_contract(), 1);
    assert_eq!(summary.schema_transition_rejected_field_slot(), 1);
    assert_eq!(summary.schema_transition_rejected_row_layout(), 1);
    assert_eq!(summary.schema_transition_rejected_schema_version(), 1);
    assert_eq!(summary.schema_transition_rejected_snapshot(), 1);
    assert_eq!(
        summary.schema_transition_checks(),
        summary
            .schema_transition_append_only_nullable_fields()
            .saturating_add(summary.schema_transition_exact_match())
            .saturating_add(summary.schema_transition_rejected_entity_identity())
            .saturating_add(summary.schema_transition_rejected_field_contract())
            .saturating_add(summary.schema_transition_rejected_field_slot())
            .saturating_add(summary.schema_transition_rejected_row_layout())
            .saturating_add(summary.schema_transition_rejected_schema_version())
            .saturating_add(summary.schema_transition_rejected_snapshot()),
        "entity schema transition total must equal its outcome buckets",
    );
}

#[test]
fn schema_store_footprint_metrics_replace_entity_gauge_contributions() {
    reset_all();

    record(MetricsEvent::SchemaStoreFootprint {
        entity_path: "metrics::tests::SchemaStoreEntity",
        snapshots: 1,
        encoded_bytes: 100,
        latest_snapshot_bytes: 100,
    });
    record(MetricsEvent::SchemaStoreFootprint {
        entity_path: "metrics::tests::OtherSchemaStoreEntity",
        snapshots: 2,
        encoded_bytes: 300,
        latest_snapshot_bytes: 180,
    });
    record(MetricsEvent::SchemaStoreFootprint {
        entity_path: "metrics::tests::SchemaStoreEntity",
        snapshots: 3,
        encoded_bytes: 500,
        latest_snapshot_bytes: 220,
    });

    let report = report_window_start(None);
    let counters = report
        .counters()
        .expect("schema-store footprint fixture should produce aggregate counters");
    let ops = counters.ops();
    assert_eq!(ops.schema_store_snapshots(), 5);
    assert_eq!(ops.schema_store_encoded_bytes(), 800);
    assert_eq!(ops.schema_store_latest_snapshot_bytes(), 400);

    let summaries = report.entity_counters();
    let summary = summaries
        .iter()
        .find(|summary| summary.path() == "metrics::tests::SchemaStoreEntity")
        .expect("schema-store fixture should produce updated entity summary");
    assert_eq!(summary.schema_store_snapshots(), 3);
    assert_eq!(summary.schema_store_encoded_bytes(), 500);
    assert_eq!(summary.schema_store_latest_snapshot_bytes(), 220);
}

#[test]
fn accepted_schema_footprint_metrics_replace_entity_gauge_contributions() {
    reset_all();

    record(MetricsEvent::AcceptedSchemaFootprint {
        entity_path: "metrics::tests::AcceptedSchemaEntity",
        fields: 2,
        nested_leaf_facts: 1,
    });
    record(MetricsEvent::AcceptedSchemaFootprint {
        entity_path: "metrics::tests::OtherAcceptedSchemaEntity",
        fields: 4,
        nested_leaf_facts: 3,
    });
    record(MetricsEvent::AcceptedSchemaFootprint {
        entity_path: "metrics::tests::AcceptedSchemaEntity",
        fields: 5,
        nested_leaf_facts: 8,
    });

    let report = report_window_start(None);
    let counters = report
        .counters()
        .expect("accepted-schema footprint fixture should produce aggregate counters");
    let ops = counters.ops();
    assert_eq!(ops.accepted_schema_fields(), 9);
    assert_eq!(ops.accepted_schema_nested_leaf_facts(), 11);

    let summaries = report.entity_counters();
    let summary = summaries
        .iter()
        .find(|summary| summary.path() == "metrics::tests::AcceptedSchemaEntity")
        .expect("accepted-schema fixture should produce updated entity summary");
    assert_eq!(summary.accepted_schema_fields(), 5);
    assert_eq!(summary.accepted_schema_nested_leaf_facts(), 8);
}

#[test]
fn sql_compile_reject_metrics_accumulate_by_phase_and_entity() {
    reset_all();

    for phase in [
        SqlCompileRejectPhase::CacheKey,
        SqlCompileRejectPhase::Parse,
        SqlCompileRejectPhase::Semantic,
    ] {
        record(MetricsEvent::SqlCompileReject {
            entity_path: "metrics::tests::SqlCompileEntity",
            phase,
        });
    }

    let report = report_window_start(None);
    let counters = report
        .counters()
        .expect("SQL compile reject fixture should produce aggregate counters");
    let ops = counters.ops();
    assert_eq!(ops.sql_compile_rejects(), 3);
    assert_eq!(ops.sql_compile_reject_cache_key(), 1);
    assert_eq!(ops.sql_compile_reject_parse(), 1);
    assert_eq!(ops.sql_compile_reject_semantic(), 1);
    assert_eq!(
        ops.sql_compile_rejects(),
        ops.sql_compile_reject_cache_key()
            .saturating_add(ops.sql_compile_reject_parse())
            .saturating_add(ops.sql_compile_reject_semantic()),
        "SQL compile reject total must equal its phase buckets",
    );

    let summary = report
        .entity_counters()
        .first()
        .expect("SQL compile reject fixture should produce an entity summary");
    assert_eq!(summary.path(), "metrics::tests::SqlCompileEntity");
    assert_eq!(summary.sql_compile_rejects(), 3);
    assert_eq!(summary.sql_compile_reject_cache_key(), 1);
    assert_eq!(summary.sql_compile_reject_parse(), 1);
    assert_eq!(summary.sql_compile_reject_semantic(), 1);
    assert_eq!(
        summary.sql_compile_rejects(),
        summary
            .sql_compile_reject_cache_key()
            .saturating_add(summary.sql_compile_reject_parse())
            .saturating_add(summary.sql_compile_reject_semantic()),
        "entity SQL compile reject total must equal its phase buckets",
    );
}

#[test]
fn cache_metrics_accumulate_by_cache_kind_and_entity() {
    reset_all();

    for (kind, outcome) in [
        (CacheKind::SharedQueryPlan, CacheOutcome::Hit),
        (CacheKind::SharedQueryPlan, CacheOutcome::Miss),
        (CacheKind::SharedQueryPlan, CacheOutcome::Insert),
        (CacheKind::SqlCompiledCommand, CacheOutcome::Hit),
        (CacheKind::SqlCompiledCommand, CacheOutcome::Miss),
        (CacheKind::SqlCompiledCommand, CacheOutcome::Insert),
    ] {
        record(MetricsEvent::Cache {
            entity_path: "metrics::tests::CacheEntity",
            kind,
            outcome,
        });
    }
    record(MetricsEvent::CacheEntries {
        kind: CacheKind::SharedQueryPlan,
        entries: 7,
    });
    record(MetricsEvent::CacheEntries {
        kind: CacheKind::SqlCompiledCommand,
        entries: 11,
    });

    let report = report_window_start(None);
    let counters = report
        .counters()
        .expect("cache fixture should produce aggregate counters");
    let ops = counters.ops();
    assert_eq!(ops.cache_shared_query_plan_hits(), 1);
    assert_eq!(ops.cache_shared_query_plan_misses(), 1);
    assert_eq!(ops.cache_shared_query_plan_inserts(), 1);
    assert_eq!(ops.cache_shared_query_plan_entries(), 7);
    assert_eq!(ops.cache_sql_compiled_command_hits(), 1);
    assert_eq!(ops.cache_sql_compiled_command_misses(), 1);
    assert_eq!(ops.cache_sql_compiled_command_inserts(), 1);
    assert_eq!(ops.cache_sql_compiled_command_entries(), 11);

    let summary = report
        .entity_counters()
        .first()
        .expect("cache fixture should produce an entity summary");
    assert_eq!(summary.path(), "metrics::tests::CacheEntity");
    assert_eq!(summary.cache_shared_query_plan_hits(), 1);
    assert_eq!(summary.cache_shared_query_plan_misses(), 1);
    assert_eq!(summary.cache_shared_query_plan_inserts(), 1);
    assert_eq!(summary.cache_sql_compiled_command_hits(), 1);
    assert_eq!(summary.cache_sql_compiled_command_misses(), 1);
    assert_eq!(summary.cache_sql_compiled_command_inserts(), 1);
}

#[test]
fn cache_miss_reason_metrics_accumulate_by_cache_kind_and_entity() {
    reset_all();

    for (kind, reason) in [
        (CacheKind::SharedQueryPlan, CacheMissReason::Cold),
        (CacheKind::SharedQueryPlan, CacheMissReason::DistinctKey),
        (CacheKind::SharedQueryPlan, CacheMissReason::MethodVersion),
        (
            CacheKind::SharedQueryPlan,
            CacheMissReason::SchemaFingerprint,
        ),
        (CacheKind::SharedQueryPlan, CacheMissReason::Visibility),
        (CacheKind::SqlCompiledCommand, CacheMissReason::Cold),
        (CacheKind::SqlCompiledCommand, CacheMissReason::DistinctKey),
        (
            CacheKind::SqlCompiledCommand,
            CacheMissReason::MethodVersion,
        ),
        (
            CacheKind::SqlCompiledCommand,
            CacheMissReason::SchemaFingerprint,
        ),
        (CacheKind::SqlCompiledCommand, CacheMissReason::Surface),
    ] {
        record(MetricsEvent::CacheMissReason {
            entity_path: "metrics::tests::CacheReasonEntity",
            kind,
            reason,
        });
    }

    let report = report_window_start(None);
    let counters = report
        .counters()
        .expect("cache miss reason fixture should produce aggregate counters");
    let ops = counters.ops();
    assert_eq!(ops.cache_shared_query_plan_miss_cold(), 1);
    assert_eq!(ops.cache_shared_query_plan_miss_distinct_key(), 1);
    assert_eq!(ops.cache_shared_query_plan_miss_method_version(), 1);
    assert_eq!(ops.cache_shared_query_plan_miss_schema_fingerprint(), 1);
    assert_eq!(ops.cache_shared_query_plan_miss_visibility(), 1);
    assert_eq!(ops.cache_sql_compiled_command_miss_cold(), 1);
    assert_eq!(ops.cache_sql_compiled_command_miss_distinct_key(), 1);
    assert_eq!(ops.cache_sql_compiled_command_miss_method_version(), 1);
    assert_eq!(ops.cache_sql_compiled_command_miss_schema_fingerprint(), 1);
    assert_eq!(ops.cache_sql_compiled_command_miss_surface(), 1);

    let summary = report
        .entity_counters()
        .first()
        .expect("cache miss reason fixture should produce an entity summary");
    assert_eq!(summary.path(), "metrics::tests::CacheReasonEntity");
    assert_eq!(summary.cache_shared_query_plan_miss_cold(), 1);
    assert_eq!(summary.cache_shared_query_plan_miss_distinct_key(), 1);
    assert_eq!(summary.cache_shared_query_plan_miss_method_version(), 1);
    assert_eq!(summary.cache_shared_query_plan_miss_schema_fingerprint(), 1);
    assert_eq!(summary.cache_shared_query_plan_miss_visibility(), 1);
    assert_eq!(summary.cache_sql_compiled_command_miss_cold(), 1);
    assert_eq!(summary.cache_sql_compiled_command_miss_distinct_key(), 1);
    assert_eq!(summary.cache_sql_compiled_command_miss_method_version(), 1);
    assert_eq!(
        summary.cache_sql_compiled_command_miss_schema_fingerprint(),
        1
    );
    assert_eq!(summary.cache_sql_compiled_command_miss_surface(), 1);
}

#[test]
fn plan_choice_reason_metrics_accumulate_by_reason_and_entity() {
    reset_all();

    for reason in [
        PlanChoiceReason::ConflictingPrimaryKeyChildrenAccessPreferred,
        PlanChoiceReason::ConstantFalsePredicate,
        PlanChoiceReason::EmptyChildAccessPreferred,
        PlanChoiceReason::FullScanAccess,
        PlanChoiceReason::IntentKeyAccessOverride,
        PlanChoiceReason::LimitZeroWindow,
        PlanChoiceReason::NonIndexAccess,
        PlanChoiceReason::PlannerCompositeNonIndex,
        PlanChoiceReason::PlannerFullScanFallback,
        PlanChoiceReason::PlannerKeySetAccess,
        PlanChoiceReason::PlannerPrimaryKeyLookup,
        PlanChoiceReason::PlannerPrimaryKeyRange,
        PlanChoiceReason::RequiredOrderPrimaryKeyRangePreferred,
        PlanChoiceReason::SingletonPrimaryKeyChildAccessPreferred,
    ] {
        record(MetricsEvent::PlanChoice {
            entity_path: "metrics::tests::PlanChoiceEntity",
            reason,
        });
    }

    let report = report_window_start(None);
    let counters = report
        .counters()
        .expect("plan choice fixture should produce aggregate counters");
    let ops = counters.ops();
    assert_eq!(
        ops.plan_choice_conflicting_primary_key_children_access_preferred(),
        1
    );
    assert_eq!(ops.plan_choice_constant_false_predicate(), 1);
    assert_eq!(ops.plan_choice_empty_child_access_preferred(), 1);
    assert_eq!(ops.plan_choice_full_scan_access(), 1);
    assert_eq!(ops.plan_choice_intent_key_access_override(), 1);
    assert_eq!(ops.plan_choice_limit_zero_window(), 1);
    assert_eq!(ops.plan_choice_non_index_access(), 1);
    assert_eq!(ops.plan_choice_planner_composite_non_index(), 1);
    assert_eq!(ops.plan_choice_planner_full_scan_fallback(), 1);
    assert_eq!(ops.plan_choice_planner_key_set_access(), 1);
    assert_eq!(ops.plan_choice_planner_primary_key_lookup(), 1);
    assert_eq!(ops.plan_choice_planner_primary_key_range(), 1);
    assert_eq!(
        ops.plan_choice_required_order_primary_key_range_preferred(),
        1
    );
    assert_eq!(
        ops.plan_choice_singleton_primary_key_child_access_preferred(),
        1
    );

    let summary = report
        .entity_counters()
        .first()
        .expect("plan choice fixture should produce an entity summary");
    assert_eq!(summary.path(), "metrics::tests::PlanChoiceEntity");
    assert_eq!(
        summary.plan_choice_conflicting_primary_key_children_access_preferred(),
        1
    );
    assert_eq!(summary.plan_choice_constant_false_predicate(), 1);
    assert_eq!(summary.plan_choice_empty_child_access_preferred(), 1);
    assert_eq!(summary.plan_choice_full_scan_access(), 1);
    assert_eq!(summary.plan_choice_intent_key_access_override(), 1);
    assert_eq!(summary.plan_choice_limit_zero_window(), 1);
    assert_eq!(summary.plan_choice_non_index_access(), 1);
    assert_eq!(summary.plan_choice_planner_composite_non_index(), 1);
    assert_eq!(summary.plan_choice_planner_full_scan_fallback(), 1);
    assert_eq!(summary.plan_choice_planner_key_set_access(), 1);
    assert_eq!(summary.plan_choice_planner_primary_key_lookup(), 1);
    assert_eq!(summary.plan_choice_planner_primary_key_range(), 1);
    assert_eq!(
        summary.plan_choice_required_order_primary_key_range_preferred(),
        1
    );
    assert_eq!(
        summary.plan_choice_singleton_primary_key_child_access_preferred(),
        1
    );
}

#[test]
fn prepared_shape_finalization_metrics_accumulate_by_outcome_and_entity() {
    reset_all();

    for outcome in [
        PreparedShapeFinalizationOutcome::AlreadyFinalized,
        PreparedShapeFinalizationOutcome::GeneratedFallback,
    ] {
        record(MetricsEvent::PreparedShapeFinalization {
            entity_path: "metrics::tests::PreparedShapeEntity",
            outcome,
        });
    }

    let report = report_window_start(None);
    let counters = report
        .counters()
        .expect("prepared-shape finalization fixture should produce counters");
    let ops = counters.ops();
    assert_eq!(ops.prepared_shape_already_finalized(), 1);
    assert_eq!(ops.prepared_shape_generated_fallback(), 1);

    let summary = report
        .entity_counters()
        .first()
        .expect("prepared-shape finalization fixture should produce entity summary");
    assert_eq!(summary.path(), "metrics::tests::PreparedShapeEntity");
    assert_eq!(summary.prepared_shape_already_finalized(), 1);
    assert_eq!(summary.prepared_shape_generated_fallback(), 1);
}

#[test]
fn derived_ratio_helpers_use_raw_counter_totals_without_changing_report_shape() {
    let ops = EventOps {
        load_candidate_rows_scanned: 16,
        load_candidate_rows_filtered: 12,
        load_result_rows_emitted: 4,
        sql_write_matched_rows: 8,
        sql_write_mutated_rows: 4,
        sql_write_returning_rows: 2,
        write_rows_touched: 4,
        write_index_entries_changed: 8,
        write_reverse_index_entries_changed: 6,
        write_relation_checks: 2,
        ..Default::default()
    };

    assert_eq!(
        ops.load_selectivity_ratio().map(MetricRatio::into_parts),
        Some((4, 16))
    );
    assert_eq!(
        ops.load_filter_ratio().map(MetricRatio::into_parts),
        Some((12, 16))
    );
    assert_eq!(
        ops.sql_write_mutation_ratio().map(MetricRatio::into_parts),
        Some((4, 8))
    );
    assert_eq!(
        ops.sql_write_returning_ratio().map(MetricRatio::into_parts),
        Some((2, 4))
    );
    assert_eq!(
        ops.write_index_entries_per_row()
            .map(MetricRatio::into_parts),
        Some((8, 4))
    );
    assert_eq!(
        ops.write_reverse_index_entries_per_row()
            .map(MetricRatio::into_parts),
        Some((6, 4)),
    );
    assert_eq!(
        ops.write_relation_checks_per_row()
            .map(MetricRatio::into_parts),
        Some((2, 4))
    );

    let empty = EventOps::default();
    assert_eq!(empty.load_selectivity_ratio(), None);
    assert_eq!(empty.load_filter_ratio(), None);
    assert_eq!(empty.sql_write_mutation_ratio(), None);
    assert_eq!(empty.sql_write_returning_ratio(), None);
    assert_eq!(empty.write_index_entries_per_row(), None);
    assert_eq!(empty.write_reverse_index_entries_per_row(), None);
    assert_eq!(empty.write_relation_checks_per_row(), None);
}

// Fixture with every per-entity field populated so the Candid-shape test also
// proves report projection does not drop newly added counters.
#[expect(clippy::too_many_lines)]
const fn populated_entity_counters_fixture() -> EntityCounters {
    EntityCounters {
        load_calls: 5,
        save_calls: 7,
        delete_calls: 6,
        save_insert_calls: 24,
        save_update_calls: 25,
        save_replace_calls: 26,
        exec_success: 45,
        exec_error_corruption: 46,
        exec_error_incompatible_persisted_format: 47,
        exec_error_not_found: 48,
        exec_error_internal: 49,
        exec_error_conflict: 50,
        exec_error_unsupported: 51,
        exec_error_invariant_violation: 52,
        exec_aborted: 53,
        cache_shared_query_plan_hits: 54,
        cache_shared_query_plan_misses: 55,
        cache_shared_query_plan_inserts: 56,
        cache_shared_query_plan_miss_cold: 57,
        cache_shared_query_plan_miss_distinct_key: 157,
        cache_shared_query_plan_miss_method_version: 158,
        cache_shared_query_plan_miss_schema_fingerprint: 159,
        cache_shared_query_plan_miss_visibility: 160,
        cache_sql_compiled_command_hits: 58,
        cache_sql_compiled_command_misses: 59,
        cache_sql_compiled_command_inserts: 60,
        cache_sql_compiled_command_miss_cold: 161,
        cache_sql_compiled_command_miss_distinct_key: 162,
        cache_sql_compiled_command_miss_method_version: 163,
        cache_sql_compiled_command_miss_schema_fingerprint: 164,
        cache_sql_compiled_command_miss_surface: 165,
        schema_reconcile_checks: 86,
        schema_reconcile_exact_match: 87,
        schema_reconcile_first_create: 88,
        schema_reconcile_latest_snapshot_corrupt: 89,
        schema_reconcile_rejected_field_slot: 90,
        schema_reconcile_rejected_other: 91,
        schema_reconcile_rejected_row_layout: 92,
        schema_reconcile_rejected_schema_version: 93,
        schema_reconcile_store_write_error: 94,
        schema_transition_checks: 191,
        schema_transition_append_only_nullable_fields: 199,
        schema_transition_exact_match: 192,
        schema_transition_rejected_entity_identity: 193,
        schema_transition_rejected_field_contract: 194,
        schema_transition_rejected_field_slot: 195,
        schema_transition_rejected_row_layout: 196,
        schema_transition_rejected_schema_version: 197,
        schema_transition_rejected_snapshot: 198,
        schema_store_snapshots: 184,
        schema_store_encoded_bytes: 185,
        schema_store_latest_snapshot_bytes: 186,
        accepted_schema_fields: 189,
        accepted_schema_nested_leaf_facts: 190,
        sql_compile_rejects: 180,
        sql_compile_reject_cache_key: 181,
        sql_compile_reject_parse: 182,
        sql_compile_reject_semantic: 183,
        plan_index: 30,
        plan_keys: 31,
        plan_range: 32,
        plan_full_scan: 33,
        plan_by_key: 34,
        plan_by_keys: 35,
        plan_key_range: 36,
        plan_index_prefix: 37,
        plan_index_multi_lookup: 38,
        plan_index_range: 39,
        plan_explicit_full_scan: 40,
        plan_union: 41,
        plan_intersection: 42,
        plan_grouped_hash_materialized: 43,
        plan_grouped_ordered_materialized: 44,
        plan_choice_conflicting_primary_key_children_access_preferred: 166,
        plan_choice_constant_false_predicate: 167,
        plan_choice_empty_child_access_preferred: 168,
        plan_choice_full_scan_access: 169,
        plan_choice_intent_key_access_override: 170,
        plan_choice_limit_zero_window: 171,
        plan_choice_non_index_access: 172,
        plan_choice_planner_composite_non_index: 173,
        plan_choice_planner_full_scan_fallback: 174,
        plan_choice_planner_key_set_access: 175,
        plan_choice_planner_primary_key_lookup: 176,
        plan_choice_planner_primary_key_range: 177,
        plan_choice_required_order_primary_key_range_preferred: 178,
        plan_choice_singleton_primary_key_child_access_preferred: 179,
        prepared_shape_already_finalized: 187,
        prepared_shape_generated_fallback: 188,
        rows_loaded: 8,
        rows_saved: 23,
        rows_inserted: 27,
        rows_updated: 28,
        rows_replaced: 29,
        rows_scanned: 9,
        rows_filtered: 20,
        rows_aggregated: 21,
        rows_emitted: 22,
        load_candidate_rows_scanned: 61,
        load_candidate_rows_filtered: 62,
        load_result_rows_emitted: 63,
        sql_insert_calls: 68,
        sql_insert_select_calls: 69,
        sql_update_calls: 70,
        sql_delete_calls: 71,
        sql_write_matched_rows: 72,
        sql_write_mutated_rows: 73,
        sql_write_returning_rows: 74,
        sql_write_error_insert: 75,
        sql_write_error_insert_select: 76,
        sql_write_error_update: 77,
        sql_write_error_delete: 78,
        sql_write_error_corruption: 79,
        sql_write_error_incompatible_persisted_format: 80,
        sql_write_error_not_found: 81,
        sql_write_error_internal: 82,
        sql_write_error_conflict: 83,
        sql_write_error_unsupported: 84,
        sql_write_error_invariant_violation: 85,
        rows_deleted: 10,
        index_inserts: 11,
        index_removes: 12,
        reverse_index_inserts: 13,
        reverse_index_removes: 14,
        relation_reverse_lookups: 15,
        relation_delete_blocks: 16,
        write_rows_touched: 64,
        write_index_entries_changed: 65,
        write_reverse_index_entries_changed: 66,
        write_relation_checks: 67,
        unique_violations: 17,
        non_atomic_partial_commits: 18,
        non_atomic_partial_rows_committed: 19,
    }
}

// Keep the expected field list near the projection assertions without letting
// the main test body grow past the repository lint budget.
#[expect(clippy::too_many_lines)]
fn assert_entity_summary_fields_are_present(fields: &[String]) {
    for field in [
        "path",
        "load_calls",
        "save_calls",
        "delete_calls",
        "save_insert_calls",
        "save_update_calls",
        "save_replace_calls",
        "exec_success",
        "exec_error_corruption",
        "exec_error_incompatible_persisted_format",
        "exec_error_not_found",
        "exec_error_internal",
        "exec_error_conflict",
        "exec_error_unsupported",
        "exec_error_invariant_violation",
        "exec_aborted",
        "cache_shared_query_plan_hits",
        "cache_shared_query_plan_misses",
        "cache_shared_query_plan_inserts",
        "cache_shared_query_plan_miss_cold",
        "cache_shared_query_plan_miss_distinct_key",
        "cache_shared_query_plan_miss_method_version",
        "cache_shared_query_plan_miss_schema_fingerprint",
        "cache_shared_query_plan_miss_visibility",
        "cache_sql_compiled_command_hits",
        "cache_sql_compiled_command_misses",
        "cache_sql_compiled_command_inserts",
        "cache_sql_compiled_command_miss_cold",
        "cache_sql_compiled_command_miss_distinct_key",
        "cache_sql_compiled_command_miss_method_version",
        "cache_sql_compiled_command_miss_schema_fingerprint",
        "cache_sql_compiled_command_miss_surface",
        "schema_reconcile_checks",
        "schema_reconcile_exact_match",
        "schema_reconcile_first_create",
        "schema_reconcile_latest_snapshot_corrupt",
        "schema_reconcile_rejected_field_slot",
        "schema_reconcile_rejected_other",
        "schema_reconcile_rejected_row_layout",
        "schema_reconcile_rejected_schema_version",
        "schema_reconcile_store_write_error",
        "schema_transition_checks",
        "schema_transition_append_only_nullable_fields",
        "schema_transition_exact_match",
        "schema_transition_rejected_entity_identity",
        "schema_transition_rejected_field_contract",
        "schema_transition_rejected_field_slot",
        "schema_transition_rejected_row_layout",
        "schema_transition_rejected_schema_version",
        "schema_transition_rejected_snapshot",
        "schema_store_snapshots",
        "schema_store_encoded_bytes",
        "schema_store_latest_snapshot_bytes",
        "accepted_schema_fields",
        "accepted_schema_nested_leaf_facts",
        "sql_compile_rejects",
        "sql_compile_reject_cache_key",
        "sql_compile_reject_parse",
        "sql_compile_reject_semantic",
        "plan_index",
        "plan_keys",
        "plan_range",
        "plan_full_scan",
        "plan_by_key",
        "plan_by_keys",
        "plan_key_range",
        "plan_index_prefix",
        "plan_index_multi_lookup",
        "plan_index_range",
        "plan_explicit_full_scan",
        "plan_union",
        "plan_intersection",
        "plan_grouped_hash_materialized",
        "plan_grouped_ordered_materialized",
        "plan_choice_conflicting_primary_key_children_access_preferred",
        "plan_choice_constant_false_predicate",
        "plan_choice_empty_child_access_preferred",
        "plan_choice_full_scan_access",
        "plan_choice_intent_key_access_override",
        "plan_choice_limit_zero_window",
        "plan_choice_non_index_access",
        "plan_choice_planner_composite_non_index",
        "plan_choice_planner_full_scan_fallback",
        "plan_choice_planner_key_set_access",
        "plan_choice_planner_primary_key_lookup",
        "plan_choice_planner_primary_key_range",
        "plan_choice_required_order_primary_key_range_preferred",
        "plan_choice_singleton_primary_key_child_access_preferred",
        "prepared_shape_already_finalized",
        "prepared_shape_generated_fallback",
        "rows_loaded",
        "rows_saved",
        "rows_inserted",
        "rows_updated",
        "rows_replaced",
        "rows_scanned",
        "rows_filtered",
        "rows_aggregated",
        "rows_emitted",
        "load_candidate_rows_scanned",
        "load_candidate_rows_filtered",
        "load_result_rows_emitted",
        "sql_insert_calls",
        "sql_insert_select_calls",
        "sql_update_calls",
        "sql_delete_calls",
        "sql_write_matched_rows",
        "sql_write_mutated_rows",
        "sql_write_returning_rows",
        "sql_write_error_insert",
        "sql_write_error_insert_select",
        "sql_write_error_update",
        "sql_write_error_delete",
        "sql_write_error_corruption",
        "sql_write_error_incompatible_persisted_format",
        "sql_write_error_not_found",
        "sql_write_error_internal",
        "sql_write_error_conflict",
        "sql_write_error_unsupported",
        "sql_write_error_invariant_violation",
        "rows_deleted",
        "index_inserts",
        "index_removes",
        "reverse_index_inserts",
        "reverse_index_removes",
        "relation_reverse_lookups",
        "relation_delete_blocks",
        "write_rows_touched",
        "write_index_entries_changed",
        "write_reverse_index_entries_changed",
        "write_relation_checks",
        "unique_violations",
        "non_atomic_partial_commits",
        "non_atomic_partial_rows_committed",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "EntitySummary must keep `{field}` as Candid field key",
        );
    }
}

// Keep the stable Candid projection and getter assertions in one place so a
// newly added entity field cannot update only half of the contract.
#[expect(clippy::too_many_lines)]
#[test]
fn entity_summary_candid_shape_is_stable() {
    reset_all();
    with_state_mut(|state| {
        state
            .entities
            .insert("alpha".to_string(), populated_entity_counters_fixture());
    });
    let report = report_window_start(None);
    let summary = report
        .entity_counters()
        .first()
        .expect("entity summary should exist for populated state");
    let fields = expect_record_fields(crate::metrics::state::EntitySummary::ty());
    assert_entity_summary_fields_are_present(&fields);

    assert_eq!(summary.path(), "alpha");
    assert_eq!(summary.save_calls(), 7);
    assert_eq!(summary.save_insert_calls(), 24);
    assert_eq!(summary.save_update_calls(), 25);
    assert_eq!(summary.save_replace_calls(), 26);
    assert_eq!(summary.exec_success(), 45);
    assert_eq!(summary.exec_error_corruption(), 46);
    assert_eq!(summary.exec_error_incompatible_persisted_format(), 47);
    assert_eq!(summary.exec_error_not_found(), 48);
    assert_eq!(summary.exec_error_internal(), 49);
    assert_eq!(summary.exec_error_conflict(), 50);
    assert_eq!(summary.exec_error_unsupported(), 51);
    assert_eq!(summary.exec_error_invariant_violation(), 52);
    assert_eq!(summary.exec_aborted(), 53);
    assert_eq!(summary.cache_shared_query_plan_hits(), 54);
    assert_eq!(summary.cache_shared_query_plan_misses(), 55);
    assert_eq!(summary.cache_shared_query_plan_inserts(), 56);
    assert_eq!(summary.cache_shared_query_plan_miss_cold(), 57);
    assert_eq!(summary.cache_shared_query_plan_miss_distinct_key(), 157);
    assert_eq!(summary.cache_shared_query_plan_miss_method_version(), 158);
    assert_eq!(
        summary.cache_shared_query_plan_miss_schema_fingerprint(),
        159
    );
    assert_eq!(summary.cache_shared_query_plan_miss_visibility(), 160);
    assert_eq!(summary.cache_sql_compiled_command_hits(), 58);
    assert_eq!(summary.cache_sql_compiled_command_misses(), 59);
    assert_eq!(summary.cache_sql_compiled_command_inserts(), 60);
    assert_eq!(summary.cache_sql_compiled_command_miss_cold(), 161);
    assert_eq!(summary.cache_sql_compiled_command_miss_distinct_key(), 162);
    assert_eq!(
        summary.cache_sql_compiled_command_miss_method_version(),
        163
    );
    assert_eq!(
        summary.cache_sql_compiled_command_miss_schema_fingerprint(),
        164
    );
    assert_eq!(summary.cache_sql_compiled_command_miss_surface(), 165);
    assert_eq!(summary.schema_reconcile_checks(), 86);
    assert_eq!(summary.schema_reconcile_exact_match(), 87);
    assert_eq!(summary.schema_reconcile_first_create(), 88);
    assert_eq!(summary.schema_reconcile_latest_snapshot_corrupt(), 89);
    assert_eq!(summary.schema_reconcile_rejected_field_slot(), 90);
    assert_eq!(summary.schema_reconcile_rejected_other(), 91);
    assert_eq!(summary.schema_reconcile_rejected_row_layout(), 92);
    assert_eq!(summary.schema_reconcile_rejected_schema_version(), 93);
    assert_eq!(summary.schema_reconcile_store_write_error(), 94);
    assert_eq!(summary.schema_transition_checks(), 191);
    assert_eq!(summary.schema_transition_append_only_nullable_fields(), 199);
    assert_eq!(summary.schema_transition_exact_match(), 192);
    assert_eq!(summary.schema_transition_rejected_entity_identity(), 193);
    assert_eq!(summary.schema_transition_rejected_field_contract(), 194);
    assert_eq!(summary.schema_transition_rejected_field_slot(), 195);
    assert_eq!(summary.schema_transition_rejected_row_layout(), 196);
    assert_eq!(summary.schema_transition_rejected_schema_version(), 197);
    assert_eq!(summary.schema_transition_rejected_snapshot(), 198);
    assert_eq!(summary.schema_store_snapshots(), 184);
    assert_eq!(summary.schema_store_encoded_bytes(), 185);
    assert_eq!(summary.schema_store_latest_snapshot_bytes(), 186);
    assert_eq!(summary.accepted_schema_fields(), 189);
    assert_eq!(summary.accepted_schema_nested_leaf_facts(), 190);
    assert_eq!(summary.sql_compile_rejects(), 180);
    assert_eq!(summary.sql_compile_reject_cache_key(), 181);
    assert_eq!(summary.sql_compile_reject_parse(), 182);
    assert_eq!(summary.sql_compile_reject_semantic(), 183);
    assert_eq!(summary.plan_index(), 30);
    assert_eq!(summary.plan_keys(), 31);
    assert_eq!(summary.plan_range(), 32);
    assert_eq!(summary.plan_full_scan(), 33);
    assert_eq!(summary.plan_by_key(), 34);
    assert_eq!(summary.plan_by_keys(), 35);
    assert_eq!(summary.plan_key_range(), 36);
    assert_eq!(summary.plan_index_prefix(), 37);
    assert_eq!(summary.plan_index_multi_lookup(), 38);
    assert_eq!(summary.plan_index_range(), 39);
    assert_eq!(summary.plan_explicit_full_scan(), 40);
    assert_eq!(summary.plan_union(), 41);
    assert_eq!(summary.plan_intersection(), 42);
    assert_eq!(summary.plan_grouped_hash_materialized(), 43);
    assert_eq!(summary.plan_grouped_ordered_materialized(), 44);
    assert_eq!(
        summary.plan_choice_conflicting_primary_key_children_access_preferred(),
        166
    );
    assert_eq!(summary.plan_choice_constant_false_predicate(), 167);
    assert_eq!(summary.plan_choice_empty_child_access_preferred(), 168);
    assert_eq!(summary.plan_choice_full_scan_access(), 169);
    assert_eq!(summary.plan_choice_intent_key_access_override(), 170);
    assert_eq!(summary.plan_choice_limit_zero_window(), 171);
    assert_eq!(summary.plan_choice_non_index_access(), 172);
    assert_eq!(summary.plan_choice_planner_composite_non_index(), 173);
    assert_eq!(summary.plan_choice_planner_full_scan_fallback(), 174);
    assert_eq!(summary.plan_choice_planner_key_set_access(), 175);
    assert_eq!(summary.plan_choice_planner_primary_key_lookup(), 176);
    assert_eq!(summary.plan_choice_planner_primary_key_range(), 177);
    assert_eq!(
        summary.plan_choice_required_order_primary_key_range_preferred(),
        178
    );
    assert_eq!(
        summary.plan_choice_singleton_primary_key_child_access_preferred(),
        179
    );
    assert_eq!(summary.prepared_shape_already_finalized(), 187);
    assert_eq!(summary.prepared_shape_generated_fallback(), 188);
    assert_eq!(summary.rows_saved(), 23);
    assert_eq!(summary.rows_inserted(), 27);
    assert_eq!(summary.rows_updated(), 28);
    assert_eq!(summary.rows_replaced(), 29);
    assert_eq!(summary.rows_filtered(), 20);
    assert_eq!(summary.rows_aggregated(), 21);
    assert_eq!(summary.rows_emitted(), 22);
    assert_eq!(summary.load_candidate_rows_scanned(), 61);
    assert_eq!(summary.load_candidate_rows_filtered(), 62);
    assert_eq!(summary.load_result_rows_emitted(), 63);
    assert_eq!(summary.sql_insert_calls(), 68);
    assert_eq!(summary.sql_insert_select_calls(), 69);
    assert_eq!(summary.sql_update_calls(), 70);
    assert_eq!(summary.sql_delete_calls(), 71);
    assert_eq!(summary.sql_write_matched_rows(), 72);
    assert_eq!(summary.sql_write_mutated_rows(), 73);
    assert_eq!(summary.sql_write_returning_rows(), 74);
    assert_eq!(summary.sql_write_error_insert(), 75);
    assert_eq!(summary.sql_write_error_insert_select(), 76);
    assert_eq!(summary.sql_write_error_update(), 77);
    assert_eq!(summary.sql_write_error_delete(), 78);
    assert_eq!(summary.sql_write_error_corruption(), 79);
    assert_eq!(summary.sql_write_error_incompatible_persisted_format(), 80);
    assert_eq!(summary.sql_write_error_not_found(), 81);
    assert_eq!(summary.sql_write_error_internal(), 82);
    assert_eq!(summary.sql_write_error_conflict(), 83);
    assert_eq!(summary.sql_write_error_unsupported(), 84);
    assert_eq!(summary.sql_write_error_invariant_violation(), 85);
    assert_eq!(summary.index_inserts(), 11);
    assert_eq!(summary.index_removes(), 12);
    assert_eq!(summary.reverse_index_inserts(), 13);
    assert_eq!(summary.reverse_index_removes(), 14);
    assert_eq!(summary.relation_reverse_lookups(), 15);
    assert_eq!(summary.relation_delete_blocks(), 16);
    assert_eq!(summary.write_rows_touched(), 64);
    assert_eq!(summary.write_index_entries_changed(), 65);
    assert_eq!(summary.write_reverse_index_entries_changed(), 66);
    assert_eq!(summary.write_relation_checks(), 67);
    assert_eq!(summary.unique_violations(), 17);
    assert_eq!(summary.non_atomic_partial_commits(), 18);
    assert_eq!(summary.non_atomic_partial_rows_committed(), 19);
}

#[test]
fn entity_summary_derived_ratio_helpers_use_projected_counter_totals() {
    reset_all();
    with_state_mut(|state| {
        state.entities.insert(
            "ratio".to_string(),
            EntityCounters {
                load_candidate_rows_scanned: 20,
                load_candidate_rows_filtered: 5,
                load_result_rows_emitted: 10,
                sql_write_matched_rows: 8,
                sql_write_mutated_rows: 6,
                sql_write_returning_rows: 3,
                write_rows_touched: 4,
                write_index_entries_changed: 2,
                write_reverse_index_entries_changed: 8,
                write_relation_checks: 1,
                ..Default::default()
            },
        );
    });

    let report = report_window_start(None);
    let summary = report
        .entity_counters()
        .first()
        .expect("entity summary should exist for ratio state");
    assert_eq!(
        summary
            .load_selectivity_ratio()
            .map(MetricRatio::into_parts),
        Some((10, 20)),
    );
    assert_eq!(
        summary.load_filter_ratio().map(MetricRatio::into_parts),
        Some((5, 20)),
    );
    assert_eq!(
        summary
            .sql_write_mutation_ratio()
            .map(MetricRatio::into_parts),
        Some((6, 8)),
    );
    assert_eq!(
        summary
            .sql_write_returning_ratio()
            .map(MetricRatio::into_parts),
        Some((3, 6)),
    );
    assert_eq!(
        summary
            .write_index_entries_per_row()
            .map(MetricRatio::into_parts),
        Some((2, 4)),
    );
    assert_eq!(
        summary
            .write_reverse_index_entries_per_row()
            .map(MetricRatio::into_parts),
        Some((8, 4)),
    );
    assert_eq!(
        summary
            .write_relation_checks_per_row()
            .map(MetricRatio::into_parts),
        Some((1, 4)),
    );
}
