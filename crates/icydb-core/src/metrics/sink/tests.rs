use super::*;
use crate::{
    db::{
        MutationJobPhase, MutationJobRestartReason, MutationJobTargetFailureReason, MutationMode,
    },
    error::ErrorClass,
};
use std::cell::Cell;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

struct CountingSink {
    calls: Rc<AtomicUsize>,
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one closed matrix proves every mutation-job lifecycle and reason bucket"
)]
fn mutation_job_events_preserve_bounded_lifecycle_reason_and_resource_facts() {
    metrics::reset_all();
    for event in [
        MutationJobLifecycleEvent::AdvanceExactReplay,
        MutationJobLifecycleEvent::CancelUnadvanced,
        MutationJobLifecycleEvent::Complete,
        MutationJobLifecycleEvent::ForwardToVerify,
        MutationJobLifecycleEvent::InventoryLoaded,
        MutationJobLifecycleEvent::StartExactReplay,
        MutationJobLifecycleEvent::StartInserted,
        MutationJobLifecycleEvent::StateLoaded,
        MutationJobLifecycleEvent::TerminalAcknowledged,
        MutationJobLifecycleEvent::VerifyRestartResidualWork,
        MutationJobLifecycleEvent::VerifyRestartRevisionDrift,
    ] {
        record(MetricsEvent::MutationJobLifecycle { event });
    }
    for reason in [
        MutationJobRestartReason::AcceptedSchemaChanged,
        MutationJobRestartReason::BatchPolicyChanged,
        MutationJobRestartReason::CandidateExceedsBatchPolicy,
        MutationJobRestartReason::ExecutionBudgetPolicyExceeded,
        MutationJobRestartReason::IntentIneligible,
        MutationJobRestartReason::ManagedTimestampRegression,
        MutationJobRestartReason::TargetAllocationChanged,
        MutationJobRestartReason::UnsupportedContinuation,
    ] {
        record(MetricsEvent::MutationJobRestart { reason });
    }
    for reason in [
        MutationJobTargetFailureReason::StagingByteBudgetExceeded,
        MutationJobTargetFailureReason::Other,
    ] {
        record(MetricsEvent::MutationJobTargetFailure { reason });
    }
    record(MetricsEvent::MutationJobStep {
        phase: MutationJobPhase::Forward,
        keys_scanned: 11,
        rows_updated: 3,
        scan_bytes: 101,
        staged_bytes: 202,
        keys_scanned_total: 11,
        rows_updated_total: 3,
        verify_restarts_total: 0,
    });
    record(MetricsEvent::MutationJobStep {
        phase: MutationJobPhase::Verify,
        keys_scanned: 7,
        rows_updated: 0,
        scan_bytes: 55,
        staged_bytes: 0,
        keys_scanned_total: 18,
        rows_updated_total: 3,
        verify_restarts_total: 1,
    });
    record(MetricsEvent::MutationJobCapacity {
        retained_count: 9,
        hard_limit: 64,
        reserved_integrity_headroom: 8,
        integrity_count: 2,
        resumable_count: 3,
        mutation_count: 4,
        retained_record_bytes: 5_000,
    });

    let report = metrics_report(None);
    let jobs = report
        .counters()
        .expect("current metrics window should exist")
        .ops()
        .mutation_jobs();
    assert_eq!(jobs.starts_inserted(), 1);
    assert_eq!(jobs.starts_exact_replayed(), 1);
    assert_eq!(jobs.states_loaded(), 1);
    assert_eq!(jobs.advances_exact_replayed(), 1);
    assert_eq!(jobs.cancellations(), 1);
    assert_eq!(jobs.terminal_acknowledgements(), 1);
    assert_eq!(jobs.inventories_loaded(), 1);
    assert_eq!(jobs.forward_to_verify_transitions(), 1);
    assert_eq!(jobs.verify_restarts_revision_drift(), 1);
    assert_eq!(jobs.verify_restarts_residual_work(), 1);
    assert_eq!(jobs.completions(), 1);
    assert_eq!(jobs.forward_steps_committed(), 1);
    assert_eq!(jobs.verify_steps_committed(), 1);
    assert_eq!(jobs.keys_scanned(), 18);
    assert_eq!(jobs.rows_updated(), 3);
    assert_eq!(jobs.scan_bytes(), 156);
    assert_eq!(jobs.staged_bytes(), 202);
    assert_eq!(jobs.keys_scanned_cumulative_max(), 18);
    assert_eq!(jobs.rows_updated_cumulative_max(), 3);
    assert_eq!(jobs.verify_restarts_cumulative_max(), 1);
    assert_eq!(jobs.terminal_restarts(), 8);
    assert_eq!(
        jobs.restart_count(MutationJobRestartReason::ExecutionBudgetPolicyExceeded),
        1
    );
    assert_eq!(
        jobs.target_failure_count(MutationJobTargetFailureReason::StagingByteBudgetExceeded),
        1
    );
    assert_eq!(jobs.retained_count(), 9);
    assert_eq!(jobs.hard_limit(), 64);
    assert_eq!(jobs.reserved_integrity_headroom(), 8);
    assert_eq!(jobs.integrity_count(), 2);
    assert_eq!(jobs.resumable_count(), 3);
    assert_eq!(jobs.mutation_count(), 4);
    assert_eq!(jobs.retained_record_bytes(), 5_000);
    metrics::reset_all();
}

impl MetricsSink for CountingSink {
    fn record(&self, _: MetricsEvent) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn with_metrics_sink_routes_and_restores_nested_overrides() {
    SINK_OVERRIDE.with(|stack| {
        stack.borrow_mut().clear();
    });

    let outer_calls = Rc::new(AtomicUsize::new(0));
    let inner_calls = Rc::new(AtomicUsize::new(0));
    let outer = Rc::new(CountingSink {
        calls: Rc::clone(&outer_calls),
    });
    let inner = Rc::new(CountingSink {
        calls: Rc::clone(&inner_calls),
    });

    // No override installed yet.
    record(MetricsEvent::Plan {
        entity_path: "metrics::tests::Entity".into(),
        kind: PlanKind::ByKey,
        grouped_execution_mode: None,
    });
    assert_eq!(outer_calls.load(Ordering::SeqCst), 0);
    assert_eq!(inner_calls.load(Ordering::SeqCst), 0);

    with_shared_metrics_sink(outer, || {
        record(MetricsEvent::Plan {
            entity_path: "metrics::tests::Entity".into(),
            kind: PlanKind::IndexPrefix,
            grouped_execution_mode: None,
        });
        assert_eq!(outer_calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner_calls.load(Ordering::SeqCst), 0);

        with_shared_metrics_sink(inner, || {
            record(MetricsEvent::Plan {
                entity_path: "metrics::tests::Entity".into(),
                kind: PlanKind::KeyRange,
                grouped_execution_mode: None,
            });
        });

        // Inner override was restored to outer override.
        record(MetricsEvent::Plan {
            entity_path: "metrics::tests::Entity".into(),
            kind: PlanKind::FullScan,
            grouped_execution_mode: None,
        });
    });

    assert_eq!(outer_calls.load(Ordering::SeqCst), 2);
    assert_eq!(inner_calls.load(Ordering::SeqCst), 1);

    // Outer override was restored to previous (none).
    SINK_OVERRIDE.with(|stack| {
        assert!(stack.borrow().is_empty());
    });

    record(MetricsEvent::Plan {
        entity_path: "metrics::tests::Entity".into(),
        kind: PlanKind::ByKey,
        grouped_execution_mode: None,
    });
    assert_eq!(outer_calls.load(Ordering::SeqCst), 2);
    assert_eq!(inner_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn with_metrics_sink_restores_override_on_panic() {
    SINK_OVERRIDE.with(|stack| {
        stack.borrow_mut().clear();
    });

    let calls = Rc::new(AtomicUsize::new(0));
    let sink = Rc::new(CountingSink {
        calls: Rc::clone(&calls),
    });

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        with_shared_metrics_sink(sink, || {
            record(MetricsEvent::Plan {
                entity_path: "metrics::tests::Entity".into(),
                kind: PlanKind::IndexPrefix,
                grouped_execution_mode: None,
            });
            panic!("intentional panic for guard test");
        });
    }))
    .is_err();
    assert!(panicked);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Guard restored TLS slot after unwind.
    SINK_OVERRIDE.with(|stack| {
        assert!(stack.borrow().is_empty());
    });

    record(MetricsEvent::Plan {
        entity_path: "metrics::tests::Entity".into(),
        kind: PlanKind::KeyRange,
        grouped_execution_mode: None,
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn query_context_suppresses_only_the_default_global_sink_and_restores_nesting() {
    metrics_reset_all();
    SINK_OVERRIDE.with(|stack| stack.borrow_mut().clear());
    QUERY_CONTEXT_DEPTH.with(|depth| depth.set(0));

    assert!(event_is_observable());
    with_query_metrics_context(|| {
        assert!(!event_is_observable());
        with_query_metrics_context(|| assert!(!event_is_observable()));
        assert!(!event_is_observable());
        record(MetricsEvent::Plan {
            entity_path: "metrics::tests::Suppressed".into(),
            kind: PlanKind::ByKey,
            grouped_execution_mode: None,
        });
    });

    assert!(event_is_observable());
    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("unfiltered metrics report should contain global counters");
    assert_eq!(counters.ops.plan_by_key, 0);
}

#[test]
fn query_context_avoids_path_span_ownership_without_a_scoped_sink() {
    SINK_OVERRIDE.with(|stack| stack.borrow_mut().clear());
    QUERY_CONTEXT_DEPTH.with(|depth| depth.set(0));

    with_query_metrics_context(|| {
        let span = PathSpan::new(ExecKind::Load, "metrics::tests::SuppressedSpan");
        assert!(!span.owns_entity_path());
        assert!(span.is_finished());
    });
}

#[test]
fn scoped_sink_retains_precedence_inside_query_context() {
    SINK_OVERRIDE.with(|stack| stack.borrow_mut().clear());
    QUERY_CONTEXT_DEPTH.with(|depth| depth.set(0));

    let calls = Rc::new(AtomicUsize::new(0));
    let sink = Rc::new(CountingSink {
        calls: Rc::clone(&calls),
    });

    with_query_metrics_context(|| {
        assert!(!event_is_observable());
        with_shared_metrics_sink(sink, || {
            assert!(event_is_observable());
            record(MetricsEvent::Plan {
                entity_path: "metrics::tests::ScopedQuery".into(),
                kind: PlanKind::IndexPrefix,
                grouped_execution_mode: None,
            });
        });
        assert!(!event_is_observable());
    });

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(event_is_observable());
}

#[test]
fn query_context_restores_after_host_unwind() {
    QUERY_CONTEXT_DEPTH.with(|depth| depth.set(0));

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        with_query_metrics_context(|| {
            assert!(!event_is_observable());
            panic!("intentional panic for query-context guard test");
        });
    }))
    .is_err();

    assert!(panicked);
    assert!(event_is_observable());
}

#[test]
fn query_context_ends_before_a_returned_future_is_polled() {
    QUERY_CONTEXT_DEPTH.with(|depth| depth.set(0));

    let observed_inside_closure = Cell::new(false);
    let future = with_query_metrics_context(|| {
        observed_inside_closure.set(query_context_is_active());
        async { query_context_is_active() }
    });

    assert!(observed_inside_closure.get());
    assert!(!query_context_is_active());
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(future.as_mut().poll(&mut context), Poll::Ready(false));
}

struct ReentrantSink {
    calls: Rc<AtomicUsize>,
}

impl MetricsSink for ReentrantSink {
    fn record(&self, _: MetricsEvent) {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            record(MetricsEvent::Plan {
                entity_path: "metrics::tests::Entity".into(),
                kind: PlanKind::FullScan,
                grouped_execution_mode: None,
            });
        }
    }
}

#[test]
fn with_metrics_sink_allows_reentrant_recording() {
    SINK_OVERRIDE.with(|stack| {
        stack.borrow_mut().clear();
    });

    let calls = Rc::new(AtomicUsize::new(0));
    let sink = Rc::new(ReentrantSink {
        calls: Rc::clone(&calls),
    });

    with_shared_metrics_sink(sink, || {
        record(MetricsEvent::Plan {
            entity_path: "metrics::tests::Entity".into(),
            kind: PlanKind::IndexPrefix,
            grouped_execution_mode: None,
        });
    });

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    SINK_OVERRIDE.with(|stack| {
        assert!(stack.borrow().is_empty());
    });
}

#[test]
fn metrics_report_without_window_start_returns_counters() {
    metrics_reset_all();
    record(MetricsEvent::Plan {
        entity_path: "metrics::tests::Entity".into(),
        kind: PlanKind::IndexPrefix,
        grouped_execution_mode: None,
    });

    let report = metrics_report(None);
    assert!(report.window_filter_matched());
    let counters = report
        .counters()
        .expect("metrics report should include counters without since filter");
    assert_eq!(counters.ops.plan_index_prefix, 1);
}

#[test]
fn metrics_report_window_start_before_window_returns_counters() {
    metrics_reset_all();
    let window_start = metrics::with_state(|m| m.window_start_ms);
    record(MetricsEvent::Plan {
        entity_path: "metrics::tests::Entity".into(),
        kind: PlanKind::ByKey,
        grouped_execution_mode: None,
    });

    let report = metrics_report(Some(window_start.saturating_sub(1)));
    assert!(report.window_filter_matched());
    assert_eq!(
        report.requested_window_start_ms(),
        Some(window_start.saturating_sub(1)),
    );
    assert_eq!(report.active_window_start_ms(), window_start);
    let counters = report
        .counters()
        .expect("metrics report should include counters when window_start_ms is before window");
    assert_eq!(counters.ops.plan_by_key, 1);
}

#[test]
fn metrics_report_window_start_after_window_returns_empty() {
    metrics_reset_all();
    let window_start = metrics::with_state(|m| m.window_start_ms);
    record(MetricsEvent::Plan {
        entity_path: "metrics::tests::Entity".into(),
        kind: PlanKind::FullScan,
        grouped_execution_mode: None,
    });

    let report = metrics_report(Some(window_start.saturating_add(1)));
    assert!(!report.window_filter_matched());
    assert_eq!(
        report.requested_window_start_ms(),
        Some(window_start.saturating_add(1)),
    );
    assert_eq!(report.active_window_start_ms(), window_start);
    assert!(report.counters().is_none());
    assert!(report.entity_counters().is_empty());
}

#[test]
fn metrics_report_grouped_execution_mode_counters_accumulate() {
    metrics_reset_all();
    record(MetricsEvent::Plan {
        entity_path: "metrics::tests::Entity".into(),
        kind: PlanKind::IndexPrefix,
        grouped_execution_mode: Some(GroupedPlanExecutionMode::HashMaterialized),
    });
    record(MetricsEvent::Plan {
        entity_path: "metrics::tests::Entity".into(),
        kind: PlanKind::KeyRange,
        grouped_execution_mode: Some(GroupedPlanExecutionMode::OrderedStreaming),
    });

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("metrics report should include counters");
    assert_eq!(counters.ops.plan_index_prefix, 1);
    assert_eq!(counters.ops.plan_key_range, 1);
    assert_eq!(counters.ops.plan_grouped_hash_materialized, 1);
    assert_eq!(counters.ops.plan_grouped_ordered_streaming, 1);

    let entity = report
        .entity_counters()
        .first()
        .expect("grouped plan metrics should retain per-entity counters");
    assert_eq!(entity.plan_grouped_hash_materialized(), 1);
    assert_eq!(entity.plan_grouped_ordered_streaming(), 1);
}

#[test]
fn detailed_plan_metrics_accumulate_by_exact_route() {
    metrics_reset_all();

    for kind in [
        PlanKind::ByKey,
        PlanKind::ByKeys,
        PlanKind::KeyRange,
        PlanKind::IndexBranchSet,
        PlanKind::IndexPrefix,
        PlanKind::IndexMultiLookup,
        PlanKind::IndexRange,
        PlanKind::FullScan,
        PlanKind::Union,
        PlanKind::Intersection,
    ] {
        record(MetricsEvent::Plan {
            entity_path: "metrics::tests::Entity".into(),
            kind,
            grouped_execution_mode: None,
        });
    }

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("metrics report should include counters");
    assert_eq!(counters.ops.plan_by_key, 1);
    assert_eq!(counters.ops.plan_by_keys, 1);
    assert_eq!(counters.ops.plan_key_range, 1);
    assert_eq!(counters.ops.plan_index_branch_set, 1);
    assert_eq!(counters.ops.plan_index_prefix, 1);
    assert_eq!(counters.ops.plan_index_multi_lookup, 1);
    assert_eq!(counters.ops.plan_index_range, 1);
    assert_eq!(counters.ops.plan_explicit_full_scan, 1);
    assert_eq!(counters.ops.plan_union, 1);
    assert_eq!(counters.ops.plan_intersection, 1);

    let entity = report
        .entity_counters()
        .first()
        .expect("plan metrics should retain per-entity counters");
    assert_eq!(entity.path(), "metrics::tests::Entity");
    assert_eq!(entity.plan_by_key(), 1);
    assert_eq!(entity.plan_by_keys(), 1);
    assert_eq!(entity.plan_key_range(), 1);
    assert_eq!(entity.plan_index_branch_set(), 1);
    assert_eq!(entity.plan_index_prefix(), 1);
    assert_eq!(entity.plan_index_multi_lookup(), 1);
    assert_eq!(entity.plan_index_range(), 1);
    assert_eq!(entity.plan_explicit_full_scan(), 1);
    assert_eq!(entity.plan_union(), 1);
    assert_eq!(entity.plan_intersection(), 1);
}

#[test]
fn save_finish_metrics_accumulate_saved_rows() {
    metrics_reset_all();

    record(MetricsEvent::ExecFinish {
        kind: ExecKind::Save,
        entity_path: "metrics::tests::Entity".into(),
        rows_touched: 4,
        inst_delta: 11,
        outcome: ExecOutcome::Success,
    });

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("metrics report should include counters");
    assert_eq!(counters.ops.rows_saved, 4);
    assert_eq!(counters.ops.write_rows_touched, 4);
    assert_eq!(counters.perf.save_inst_total, 11);

    let entity = report
        .entity_counters()
        .first()
        .expect("save finish should retain per-entity counters");
    assert_eq!(entity.rows_saved(), 4);
    assert_eq!(entity.write_rows_touched(), 4);
}

#[test]
fn exec_finish_metrics_accumulate_outcomes_by_entity() {
    metrics_reset_all();

    for outcome in [
        ExecOutcome::Success,
        ExecOutcome::ErrorUnsupported,
        ExecOutcome::ErrorCorruption,
        ExecOutcome::Aborted,
    ] {
        record(MetricsEvent::ExecFinish {
            kind: ExecKind::Load,
            entity_path: "metrics::tests::Entity".into(),
            rows_touched: 0,
            inst_delta: 0,
            outcome,
        });
    }

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("metrics report should include counters");
    assert_eq!(counters.ops.exec_success(), 1);
    assert_eq!(counters.ops.exec_error_unsupported(), 1);
    assert_eq!(counters.ops.exec_error_corruption(), 1);
    assert_eq!(counters.ops.exec_aborted(), 1);

    let entity = report
        .entity_counters()
        .first()
        .expect("outcome metrics should retain per-entity counters");
    assert_eq!(entity.exec_success(), 1);
    assert_eq!(entity.exec_error_unsupported(), 1);
    assert_eq!(entity.exec_error_corruption(), 1);
    assert_eq!(entity.exec_aborted(), 1);
}

#[test]
fn exec_error_metrics_count_attempt_and_outcome_without_rows() {
    metrics_reset_all();

    record(MetricsEvent::ExecError {
        kind: ExecKind::Load,
        entity_path: "metrics::tests::Entity".into(),
        outcome: ExecOutcome::ErrorInternal,
    });

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("metrics report should include counters");
    assert_eq!(counters.ops.load_calls(), 1);
    assert_eq!(counters.ops.exec_error_internal(), 1);
    assert_eq!(counters.ops.rows_loaded(), 0);

    let entity = report
        .entity_counters()
        .first()
        .expect("exec error should retain per-entity counters");
    assert_eq!(entity.load_calls(), 1);
    assert_eq!(entity.exec_error_internal(), 1);
    assert_eq!(entity.rows_loaded(), 0);
}

#[test]
fn save_mutation_metrics_accumulate_by_mode() {
    metrics_reset_all();

    for (mode, rows_touched) in [
        (MutationMode::Insert, 2),
        (MutationMode::Update, 3),
        (MutationMode::Replace, 4),
    ] {
        record(MetricsEvent::SaveMutation {
            entity_path: "metrics::tests::Entity".into(),
            mode,
            rows_touched,
        });
    }

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("metrics report should include counters");
    assert_eq!(counters.ops.save_insert_calls, 1);
    assert_eq!(counters.ops.save_update_calls, 1);
    assert_eq!(counters.ops.save_replace_calls, 1);
    assert_eq!(counters.ops.rows_inserted, 2);
    assert_eq!(counters.ops.rows_updated, 3);
    assert_eq!(counters.ops.rows_replaced, 4);

    let entity = report
        .entity_counters()
        .first()
        .expect("save mutation should retain per-entity counters");
    assert_eq!(entity.save_insert_calls(), 1);
    assert_eq!(entity.save_update_calls(), 1);
    assert_eq!(entity.save_replace_calls(), 1);
    assert_eq!(entity.rows_inserted(), 2);
    assert_eq!(entity.rows_updated(), 3);
    assert_eq!(entity.rows_replaced(), 4);
}

#[test]
fn sql_write_metrics_accumulate_by_command_shape() {
    metrics_reset_all();

    for (kind, staged_rows, matched_rows, mutated_rows, returning_rows) in [
        (SqlWriteKind::Insert, 2, 2, 2, 0),
        (SqlWriteKind::InsertSelect, 3, 3, 3, 3),
        (SqlWriteKind::Update, 6, 5, 4, 4),
        (SqlWriteKind::Delete, 2, 2, 2, 1),
    ] {
        record(MetricsEvent::SqlWrite {
            entity_path: "metrics::tests::Entity".into(),
            kind,
            staged_rows,
            matched_rows,
            mutated_rows,
            returning_rows,
        });
    }

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("metrics report should include counters");
    assert_eq!(counters.ops.sql_insert_calls(), 1);
    assert_eq!(counters.ops.sql_insert_select_calls(), 1);
    assert_eq!(counters.ops.sql_update_calls(), 1);
    assert_eq!(counters.ops.sql_delete_calls(), 1);
    assert_eq!(counters.ops.sql_write_staged_rows(), 13);
    assert_eq!(counters.ops.sql_write_matched_rows(), 12);
    assert_eq!(counters.ops.sql_write_mutated_rows(), 11);
    assert_eq!(counters.ops.sql_write_returning_rows(), 8);

    let entity = report
        .entity_counters()
        .first()
        .expect("sql write metrics should retain per-entity counters");
    assert_eq!(entity.sql_insert_calls(), 1);
    assert_eq!(entity.sql_insert_select_calls(), 1);
    assert_eq!(entity.sql_update_calls(), 1);
    assert_eq!(entity.sql_delete_calls(), 1);
    assert_eq!(entity.sql_write_staged_rows(), 13);
    assert_eq!(entity.sql_write_matched_rows(), 12);
    assert_eq!(entity.sql_write_mutated_rows(), 11);
    assert_eq!(entity.sql_write_returning_rows(), 8);
}

#[test]
fn sql_write_error_metrics_accumulate_by_command_shape_and_class() {
    metrics_reset_all();

    for (kind, class) in [
        (SqlWriteKind::Insert, ErrorClass::Unsupported),
        (SqlWriteKind::InsertSelect, ErrorClass::Conflict),
        (SqlWriteKind::Update, ErrorClass::InvariantViolation),
        (SqlWriteKind::Delete, ErrorClass::Internal),
        (SqlWriteKind::Update, ErrorClass::Corruption),
        (
            SqlWriteKind::Delete,
            ErrorClass::IncompatiblePersistedFormat,
        ),
        (SqlWriteKind::Insert, ErrorClass::NotFound),
    ] {
        record(MetricsEvent::SqlWriteError {
            entity_path: "metrics::tests::Entity".into(),
            kind,
            class,
        });
    }

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("metrics report should include counters");
    assert_eq!(counters.ops.sql_write_error_insert(), 2);
    assert_eq!(counters.ops.sql_write_error_insert_select(), 1);
    assert_eq!(counters.ops.sql_write_error_update(), 2);
    assert_eq!(counters.ops.sql_write_error_delete(), 2);
    assert_eq!(counters.ops.sql_write_error_corruption(), 1);
    assert_eq!(
        counters.ops.sql_write_error_incompatible_persisted_format(),
        1,
    );
    assert_eq!(counters.ops.sql_write_error_not_found(), 1);
    assert_eq!(counters.ops.sql_write_error_internal(), 1);
    assert_eq!(counters.ops.sql_write_error_conflict(), 1);
    assert_eq!(counters.ops.sql_write_error_unsupported(), 1);
    assert_eq!(counters.ops.sql_write_error_invariant_violation(), 1);

    let entity = report
        .entity_counters()
        .first()
        .expect("sql write error metrics should retain per-entity counters");
    assert_eq!(entity.sql_write_error_insert(), 2);
    assert_eq!(entity.sql_write_error_insert_select(), 1);
    assert_eq!(entity.sql_write_error_update(), 2);
    assert_eq!(entity.sql_write_error_delete(), 2);
    assert_eq!(entity.sql_write_error_corruption(), 1);
    assert_eq!(entity.sql_write_error_incompatible_persisted_format(), 1);
    assert_eq!(entity.sql_write_error_not_found(), 1);
    assert_eq!(entity.sql_write_error_internal(), 1);
    assert_eq!(entity.sql_write_error_conflict(), 1);
    assert_eq!(entity.sql_write_error_unsupported(), 1);
    assert_eq!(entity.sql_write_error_invariant_violation(), 1);
}

#[test]
fn reverse_and_relation_metrics_events_accumulate() {
    metrics_reset_all();

    record(MetricsEvent::IndexDelta {
        entity_path: "metrics::tests::Entity".into(),
        inserts: 4,
        removes: 1,
    });
    record(MetricsEvent::ReverseIndexDelta {
        entity_path: "metrics::tests::Entity".into(),
        inserts: 3,
        removes: 2,
    });
    record(MetricsEvent::RelationValidation {
        entity_path: "metrics::tests::Entity".into(),
        reverse_lookups: 5,
        blocked_deletes: 1,
    });

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("metrics report should include counters");
    assert_eq!(counters.ops.index_inserts, 4);
    assert_eq!(counters.ops.index_removes, 1);
    assert_eq!(counters.ops.write_index_entries_changed, 5);
    assert_eq!(counters.ops.reverse_index_inserts, 3);
    assert_eq!(counters.ops.reverse_index_removes, 2);
    assert_eq!(counters.ops.write_reverse_index_entries_changed, 5);
    assert_eq!(counters.ops.relation_reverse_lookups, 5);
    assert_eq!(counters.ops.relation_delete_blocks, 1);
    assert_eq!(counters.ops.write_relation_checks, 5);

    let entity = report
        .entity_counters()
        .first()
        .expect("maintenance events should retain per-entity counters");
    assert_eq!(entity.index_inserts(), 4);
    assert_eq!(entity.index_removes(), 1);
    assert_eq!(entity.write_index_entries_changed(), 5);
    assert_eq!(entity.reverse_index_inserts(), 3);
    assert_eq!(entity.reverse_index_removes(), 2);
    assert_eq!(entity.write_reverse_index_entries_changed(), 5);
    assert_eq!(entity.relation_reverse_lookups(), 5);
    assert_eq!(entity.relation_delete_blocks(), 1);
    assert_eq!(entity.write_relation_checks(), 5);
}

#[test]
fn row_flow_metrics_events_accumulate() {
    metrics_reset_all();

    record(MetricsEvent::RowsFiltered {
        entity_path: "metrics::tests::Entity".into(),
        rows_filtered: 9,
    });
    record(MetricsEvent::RowsAggregated {
        entity_path: "metrics::tests::Entity".into(),
        rows_aggregated: 4,
    });
    record(MetricsEvent::RowsEmitted {
        entity_path: "metrics::tests::Entity".into(),
        rows_emitted: 3,
    });
    record(MetricsEvent::LoadRowEfficiency {
        entity_path: "metrics::tests::Entity".into(),
        candidate_rows_scanned: 11,
        candidate_rows_filtered: 8,
        result_rows_emitted: 3,
    });

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("metrics report should include counters");
    assert_eq!(counters.ops.rows_filtered, 9);
    assert_eq!(counters.ops.rows_aggregated, 4);
    assert_eq!(counters.ops.rows_emitted, 3);
    assert_eq!(counters.ops.load_candidate_rows_scanned, 11);
    assert_eq!(counters.ops.load_candidate_rows_filtered, 8);
    assert_eq!(counters.ops.load_result_rows_emitted, 3);
}
