//! Module: metrics::tests
//! Responsibility: module-local ownership and contracts for metrics::tests.
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
fn report_sorts_entities_by_raw_row_counters() {
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
                load_calls: 1,
                rows_loaded: 5,
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

    // Order by rows_loaded desc, then rows_scanned desc, then rows_deleted desc, then path asc.
    assert_eq!(paths, ["alpha", "gamma", "beta"]);
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
    for field in ["counters", "entity_counters"] {
        assert!(
            report_fields.iter().any(|candidate| candidate == field),
            "EventReport must keep `{field}` as Candid field key",
        );
    }

    let counters_fields = expect_record_fields(crate::metrics::state::EventCounters::ty());
    for field in ["ops", "perf", "window_start_ms"] {
        assert!(
            counters_fields.iter().any(|candidate| candidate == field),
            "EventCounters must keep `{field}` as Candid field key",
        );
    }

    assert!(
        report.counters().is_some(),
        "event report fixture should retain counters for populated state",
    );
}

#[test]
fn entity_summary_candid_shape_is_stable() {
    reset_all();
    with_state_mut(|state| {
        state.entities.insert(
            "alpha".to_string(),
            EntityCounters {
                load_calls: 5,
                save_calls: 7,
                delete_calls: 6,
                rows_loaded: 8,
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
            },
        );
    });
    let report = report_window_start(None);
    let summary = report
        .entity_counters()
        .first()
        .expect("entity summary should exist for populated state");
    let fields = expect_record_fields(crate::metrics::state::EntitySummary::ty());

    for field in [
        "path",
        "load_calls",
        "delete_calls",
        "rows_loaded",
        "rows_scanned",
        "rows_deleted",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "EntitySummary must keep `{field}` as Candid field key",
        );
    }

    assert_eq!(summary.path(), "alpha");
}
