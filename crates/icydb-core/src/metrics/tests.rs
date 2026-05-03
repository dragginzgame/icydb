//! Module: metrics::tests
//! Covers metrics sink and state behavior used by runtime instrumentation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::metrics::state::{
    EntityCounters, report_window_start, reset_all, with_state, with_state_mut,
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
        "plan_by_key",
        "plan_by_keys",
        "plan_key_range",
        "plan_index_prefix",
        "plan_index_multi_lookup",
        "plan_index_range",
        "plan_explicit_full_scan",
        "plan_union",
        "plan_intersection",
        "rows_inserted",
        "rows_updated",
        "rows_replaced",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "EventOps must keep `{field}` as Candid field key",
        );
    }
}

// Fixture with every per-entity field populated so the Candid-shape test also
// proves report projection does not drop newly added counters.
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
        rows_loaded: 8,
        rows_saved: 23,
        rows_inserted: 27,
        rows_updated: 28,
        rows_replaced: 29,
        rows_scanned: 9,
        rows_filtered: 20,
        rows_aggregated: 21,
        rows_emitted: 22,
        rows_deleted: 10,
        index_inserts: 11,
        index_removes: 12,
        reverse_index_inserts: 13,
        reverse_index_removes: 14,
        relation_reverse_lookups: 15,
        relation_delete_blocks: 16,
        unique_violations: 17,
        non_atomic_partial_commits: 18,
        non_atomic_partial_rows_committed: 19,
    }
}

// Keep the expected field list near the projection assertions without letting
// the main test body grow past the repository lint budget.
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
        "rows_loaded",
        "rows_saved",
        "rows_inserted",
        "rows_updated",
        "rows_replaced",
        "rows_scanned",
        "rows_filtered",
        "rows_aggregated",
        "rows_emitted",
        "rows_deleted",
        "index_inserts",
        "index_removes",
        "reverse_index_inserts",
        "reverse_index_removes",
        "relation_reverse_lookups",
        "relation_delete_blocks",
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
    assert_eq!(summary.rows_saved(), 23);
    assert_eq!(summary.rows_inserted(), 27);
    assert_eq!(summary.rows_updated(), 28);
    assert_eq!(summary.rows_replaced(), 29);
    assert_eq!(summary.rows_filtered(), 20);
    assert_eq!(summary.rows_aggregated(), 21);
    assert_eq!(summary.rows_emitted(), 22);
    assert_eq!(summary.index_inserts(), 11);
    assert_eq!(summary.index_removes(), 12);
    assert_eq!(summary.reverse_index_inserts(), 13);
    assert_eq!(summary.reverse_index_removes(), 14);
    assert_eq!(summary.relation_reverse_lookups(), 15);
    assert_eq!(summary.relation_delete_blocks(), 16);
    assert_eq!(summary.unique_violations(), 17);
    assert_eq!(summary.non_atomic_partial_commits(), 18);
    assert_eq!(summary.non_atomic_partial_rows_committed(), 19);
}
