use super::{
    MetricsReport, STATE, SchemaLifecycleMeasurements, SchemaLifecycleMetrics, metrics_reset_all,
    metrics_snapshot, record_entity_execution, record_owner_execution,
};
use crate::metrics::{ExecutionMetricsPhase, ExecutionMetricsSpan};

#[test]
fn entity_report_decode_uses_shared_numeric_admission() {
    for fields in [
        (0u64, 1u128, None, false, false),
        (1, 1, None, false, false),
        (1, 1, Some(1u64), true, false),
        (1, 1, Some(1), false, true),
    ] {
        let bytes = icydb_schema::wire::encode_one(("entity", fields)).unwrap();
        assert!(icydb_schema::wire::decode_one::<super::EntityMetrics>(&bytes).is_err());
    }
}

#[test]
fn repeated_entity_observations_preserve_distinct_paths_and_reset() {
    metrics_reset_all();
    record_entity_execution("entity::a", 0);
    record_entity_execution("entity::b", 7);
    record_entity_execution("entity::a", u64::MAX);
    record_entity_execution("entity::a", 1);
    let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    assert_eq!(report.total_entities(), 2);
    let a = report
        .entities()
        .iter()
        .find(|entry| entry.path() == "entity::a")
        .expect("observed entity");
    assert_eq!(a.instructions().samples(), 3);
    assert_eq!(a.instructions().total(), u128::from(u64::MAX) + 1);
    assert_eq!(a.instructions().maximum().unwrap_or(0), u64::MAX);
    let b = report
        .entities()
        .iter()
        .find(|entry| entry.path() == "entity::b")
        .expect("distinct entity");
    assert_eq!(b.instructions().samples(), 1);
    assert_eq!(b.instructions().total(), 7);
    metrics_reset_all();
    record_entity_execution("entity::a", 2);
    let reset = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    assert_ne!(reset.window_id(), report.window_id());
    assert_eq!(reset.total_entities(), 1);
    assert_eq!(reset.entities()[0].instructions().samples(), 1);
    assert_eq!(reset.entities()[0].instructions().total(), 2);
}

#[test]
fn convergence_movements_mark_overflow_and_reset_independently_of_debt() {
    use crate::db::ExactBacklogMeasurement as Debt;
    metrics_reset_all();
    super::record_journal_movement(false, Debt::new(u64::MAX, 2, 3));
    super::record_journal_movement(false, Debt::new(1, 4, 5));
    super::record_journal_movement(true, Debt::new(1, 2, 3));
    record_owner_execution(&ExecutionMetricsPhase::JournalFold, 42);
    let debt = Debt::new(7, 8, 9);
    let report = metrics_snapshot(debt);
    assert!(report.convergence().overflowed());
    assert_eq!(report.convergence().appended(), Debt::new(u64::MAX, 6, 8));
    assert_eq!(report.convergence().retired(), Debt::new(1, 2, 3));
    assert_eq!(report.convergence().journal_fold().total(), 42);
    let encoded =
        icydb_schema::wire::encode_one(&report).expect("convergence report should encode");
    let decoded: MetricsReport =
        icydb_schema::wire::decode_one(&encoded).expect("report should decode");
    assert_eq!(decoded, report);
    metrics_reset_all();
    let reset = metrics_snapshot(debt);
    assert_eq!(reset.journal_debt(), debt);
    assert_eq!(reset.convergence(), &super::ConvergenceMetrics::default());
    assert_ne!(reset.window_id(), report.window_id());
}

#[test]
fn convergence_report_preserves_exact_caps_and_clips_each_movement_dimension() {
    use crate::db::ExactBacklogMeasurement as Debt;
    for retirement in [false, true] {
        for dimension in 0..3 {
            metrics_reset_all();
            let mut values = [0; 3];
            values[dimension] = u64::MAX;
            let capped = Debt::new(values[0], values[1], values[2]);
            super::record_journal_movement(retirement, capped);
            assert!(!metrics_snapshot(Debt::default()).convergence().overflowed());
            values[dimension] = 1;
            for _ in 0..2 {
                super::record_journal_movement(
                    retirement,
                    Debt::new(values[0], values[1], values[2]),
                );
                let report = metrics_snapshot(Debt::default());
                assert!(report.convergence().overflowed());
                let observed = if retirement {
                    report.convergence().retired()
                } else {
                    report.convergence().appended()
                };
                assert_eq!(observed, capped);
            }
        }
    }
}

#[test]
fn lifecycle_observations_are_separate_saturating_and_reset_with_the_window() {
    metrics_reset_all();
    for (phase, instructions) in [
        (ExecutionMetricsPhase::Lowering, u64::MAX),
        (ExecutionMetricsPhase::Lowering, 1),
        (ExecutionMetricsPhase::Publication, 12),
        (ExecutionMetricsPhase::RuntimeCompilation, 23),
        (ExecutionMetricsPhase::Cardinality, 34),
        (ExecutionMetricsPhase::StartupRecovery, 45),
    ] {
        record_owner_execution(&phase, instructions);
    }
    let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    let lifecycle = report.schema_lifecycle();
    assert_eq!(lifecycle.lowering().samples(), 2);
    assert_eq!(lifecycle.lowering().total(), u128::from(u64::MAX) + 1);
    assert_eq!(lifecycle.lowering().maximum().unwrap_or(0), u64::MAX);
    assert_eq!(lifecycle.publication().total(), 12);
    assert_eq!(lifecycle.runtime_compilation().total(), 23);
    assert_eq!(lifecycle.cardinality().total(), 34);
    assert_eq!(lifecycle.startup_recovery().total(), 45);
    assert_eq!(report.total_entities(), 0);
    let encoded =
        icydb_schema::wire::encode_one(&report).expect("encode observed lifecycle report");
    let decoded: MetricsReport =
        icydb_schema::wire::decode_one(&encoded).expect("decode lifecycle report");
    assert_eq!(decoded, report);
    metrics_reset_all();
    assert_eq!(
        metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY).schema_lifecycle(),
        &SchemaLifecycleMetrics::default()
    );
    assert_ne!(
        metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY).window_id(),
        report.window_id()
    );
}

#[test]
fn tracked_quality_survives_every_report_boundary() {
    use super::ConvergenceMeasurements;
    use ic_metrics::MeasurementAggregate;

    let exact_count =
        ic_metrics::persistence::restore_aggregate(u64::MAX, 0, Some(0), false, false).unwrap();
    let mut overflowed_count = exact_count;
    overflowed_count.record(0);
    let mut exact_total = ic_metrics::persistence::restore_aggregate(
        u64::MAX,
        u128::MAX - 1,
        Some(u64::MAX),
        true,
        false,
    )
    .unwrap();
    exact_total.record(1);
    let mut overflowed_total = exact_total;
    overflowed_total.record(1);
    let mut measured_zero = MeasurementAggregate::EMPTY;
    measured_zero.record(0);
    for (measurement, samples_saturated, total_saturated) in [
        (MeasurementAggregate::EMPTY, false, false),
        (measured_zero, false, false),
        (exact_count, false, false),
        (overflowed_count, true, false),
        (exact_total, true, false),
        (overflowed_total, true, true),
    ] {
        metrics_reset_all();
        STATE.with_borrow_mut(|window| {
            let state = window.state_mut();
            state.entities.insert("entity".into(), measurement);
            state.schema_lifecycle = SchemaLifecycleMeasurements {
                lowering: measurement,
                publication: measurement,
                runtime_compilation: measurement,
                cardinality: measurement,
                startup_recovery: measurement,
            };
            state.convergence = ConvergenceMeasurements {
                journal_fold: measurement,
                ..ConvergenceMeasurements::default()
            };
        });
        let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        let encoded = icydb_schema::wire::encode_one(&report).unwrap();
        let decoded: MetricsReport = icydb_schema::wire::decode_one(&encoded).unwrap();
        assert_eq!(decoded, report);
        let entity = &decoded.entities()[0];
        assert_eq!(
            entity.instructions().samples_overflowed(),
            samples_saturated
        );
        assert_eq!(entity.instructions().total_overflowed(), total_saturated);
        let lifecycle = decoded.schema_lifecycle();
        for owner in [
            lifecycle.lowering(),
            lifecycle.publication(),
            lifecycle.runtime_compilation(),
            lifecycle.cardinality(),
            lifecycle.startup_recovery(),
            decoded.convergence().journal_fold(),
        ] {
            assert_eq!(owner.samples(), measurement.samples());
            assert_eq!(owner.total(), measurement.total());
            assert_eq!(
                owner.maximum().unwrap_or(0),
                measurement.maximum().unwrap_or(0)
            );
            assert_eq!(owner.samples_overflowed(), samples_saturated);
            assert_eq!(owner.total_overflowed(), total_saturated);
        }
    }
    metrics_reset_all();
}

#[test]
fn failed_owner_attempts_are_observed_when_the_span_exits() {
    fn failed_attempt() -> Result<(), ()> {
        let _span = ExecutionMetricsSpan::new(ExecutionMetricsPhase::Lowering);
        Err(())
    }
    metrics_reset_all();
    assert!(failed_attempt().is_err());
    assert_eq!(
        metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY)
            .schema_lifecycle()
            .lowering()
            .samples(),
        1
    );
    assert_eq!(
        metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY)
            .schema_lifecycle()
            .publication()
            .samples(),
        0
    );
}

#[test]
fn source_selection_is_a_bounded_lexical_prefix_not_global_top_cost() {
    metrics_reset_all();
    for index in (0..4096).rev() {
        record_entity_execution(&format!("entity::{index:04}"), index);
    }
    let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    assert_eq!(report.total_entities(), 4096);
    assert_eq!(report.entities().len(), MetricsReport::MAX_ENTITIES);
    assert_eq!(report.entities()[0].path(), "entity::0063");
    assert_eq!(report.entities()[63].path(), "entity::0000");
    assert_eq!(report, metrics_report_with_end_time(report.window_end_ms()));
}

// Ignore wall-clock movement when checking that observing does not mutate counters.
fn metrics_report_with_end_time(end_ms: u64) -> MetricsReport {
    let mut report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    report.window_end_ms = end_ms;
    report
}

#[test]
fn source_selection_accepts_exact_limits_and_reports_omissions() {
    metrics_reset_all();
    for index in 0..MetricsReport::MAX_ENTITIES {
        record_entity_execution(&format!("entity::{index:04}"), 1);
    }
    let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    assert_eq!(report.entities().len() as u64, report.total_entities());
    record_entity_execution("zz-extra", 100);
    let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    assert_eq!(report.entities().len(), MetricsReport::MAX_ENTITIES);
    assert_eq!(report.total_entities(), 65);

    metrics_reset_all();
    let exact_path = "a".repeat(MetricsReport::MAX_PATH_BYTES);
    record_entity_execution(&exact_path, 1);
    record_entity_execution("z", 1);
    let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    assert_eq!(report.entities().len(), 1);
    assert_eq!(report.entities()[0].path(), exact_path);
    assert_eq!(report.total_entities(), 2);

    metrics_reset_all();
    record_entity_execution(&"a".repeat(MetricsReport::MAX_PATH_BYTES + 1), 1);
    record_entity_execution("z", 1);
    let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    assert!(report.entities().is_empty());
    assert_eq!(report.total_entities(), 2);

    metrics_reset_all();
    // Count UTF-8 bytes across paths, not characters or a per-path allowance.
    record_entity_execution(&"a".repeat(MetricsReport::MAX_PATH_BYTES - 2), 1);
    record_entity_execution("é", 1);
    record_entity_execution("ê", 1);
    let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    assert_eq!(report.entities().len(), 2);
    assert_eq!(report.total_entities(), 3);
    assert_eq!(
        report
            .entities()
            .iter()
            .map(|row| row.path().len())
            .sum::<usize>(),
        MetricsReport::MAX_PATH_BYTES
    );
}

#[test]
fn reset_identity_advances_without_relying_on_clock_resolution() {
    metrics_reset_all();
    let first = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    record_entity_execution("entity", 1);
    metrics_reset_all();
    let second = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    assert_eq!(second.window_id(), first.window_id().map(|id| id + 1));
    assert_eq!(second.total_entities(), 0);
    assert!(second.entities().is_empty());
}

#[test]
fn report_round_trips_current_wire_shape() {
    metrics_reset_all();
    for _ in 0..2 {
        record_entity_execution("entity", u64::MAX);
        record_owner_execution(&ExecutionMetricsPhase::Lowering, u64::MAX);
        record_owner_execution(&ExecutionMetricsPhase::JournalFold, u64::MAX);
    }
    let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    let total = u128::from(u64::MAX) * 2;
    assert_eq!(report.entities()[0].instructions().total(), total);
    assert_eq!(report.schema_lifecycle().lowering().total(), total);
    assert_eq!(report.convergence().journal_fold().total(), total);
    let encoded = icydb_schema::wire::encode_one(&report).expect("encode current report");
    let decoded: MetricsReport =
        icydb_schema::wire::decode_one(&encoded).expect("decode current report");
    assert_eq!(decoded, report);
}

#[test]
fn report_orders_entities_by_total_cost_then_hits_then_path() {
    metrics_reset_all();
    record_entity_execution("store::beta", 10);
    record_entity_execution("store::alpha", 5);
    record_entity_execution("store::alpha", 5);
    record_entity_execution("store::gamma", 10);

    let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
    let paths = report
        .entities()
        .iter()
        .map(super::EntityMetrics::path)
        .collect::<Vec<_>>();

    assert_eq!(paths, vec!["store::alpha", "store::beta", "store::gamma"]);
    assert_eq!(report.entities()[0].instructions().samples(), 2);
    assert_eq!(report.entities()[0].instructions().total(), 10);
    assert_eq!(
        report.entities()[0].instructions().maximum().unwrap_or(0),
        5
    );
}
