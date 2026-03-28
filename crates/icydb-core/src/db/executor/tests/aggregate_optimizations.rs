//! Module: db::executor::tests::aggregate_optimizations
//! Responsibility: live aggregate optimization-adjacent contracts that still
//! matter on the current executor surface.
//! Does not own: removed internal hit-counter seams from the old aggregate
//! matrix backlog.
//! Boundary: keeps only the current observable behaviors that replaced the
//! stale counter-driven matrix family.

use super::*;
use crate::{
    db::{
        data::DataKey,
        executor::ScalarTerminalBoundaryRequest,
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::builder::aggregate,
    },
    error::{ErrorClass, InternalError},
    metrics::sink::{MetricsEvent, MetricsSink, with_metrics_sink},
    traits::{EntityKind, EntityValue},
    types::Ulid,
    value::Value,
};
use std::cell::RefCell;

///
/// AggregateOptimizationsCaptureSink
///
/// Small metrics sink used to keep the remaining scan-budget contracts local
/// after removing the dead optimization-counter test surface.
///

#[derive(Default)]
struct AggregateOptimizationsCaptureSink {
    events: RefCell<Vec<MetricsEvent>>,
}

impl AggregateOptimizationsCaptureSink {
    fn into_events(self) -> Vec<MetricsEvent> {
        self.events.into_inner()
    }
}

impl MetricsSink for AggregateOptimizationsCaptureSink {
    fn record(&self, event: MetricsEvent) {
        self.events.borrow_mut().push(event);
    }
}

fn rows_scanned_for_entity(events: &[MetricsEvent], entity_path: &'static str) -> usize {
    events.iter().fold(0usize, |acc, event| {
        let scanned = match event {
            MetricsEvent::RowsScanned {
                entity_path: path,
                rows_scanned,
            } if *path == entity_path => usize::try_from(*rows_scanned).unwrap_or(usize::MAX),
            _ => 0,
        };

        acc.saturating_add(scanned)
    })
}

fn capture_rows_scanned_for_entity<R>(
    entity_path: &'static str,
    run: impl FnOnce() -> R,
) -> (R, usize) {
    let sink = AggregateOptimizationsCaptureSink::default();
    let output = with_metrics_sink(&sink, run);
    let rows_scanned = rows_scanned_for_entity(&sink.into_events(), entity_path);

    (output, rows_scanned)
}

fn field_slot_for_test<E>(field: &str) -> crate::db::query::plan::FieldSlot
where
    E: EntityKind,
{
    crate::db::query::plan::FieldSlot::resolve(E::MODEL, field).unwrap_or_else(|| {
        crate::db::query::plan::FieldSlot::from_parts_for_test(usize::MAX, field.to_string())
    })
}

fn remove_pushdown_row_data(id: u128) {
    let raw_key = DataKey::try_new::<PushdownParityEntity>(Ulid::from_u128(id))
        .expect("pushdown data key should build")
        .to_raw()
        .expect("pushdown data key should encode");

    DATA_STORE.with(|store| {
        let removed = store.borrow_mut().remove(&raw_key);
        assert!(
            removed.is_some(),
            "expected pushdown row to exist before data-only removal",
        );
    });
}

fn seed_simple_entities(rows: &[u128]) {
    reset_store();
    let save = SaveExecutor::<SimpleEntity>::new(DB, false);

    for id in rows {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(*id),
        })
        .expect("aggregate optimization simple seed save should succeed");
    }
}

fn u32_eq_predicate_strict(field: &str, value: u32) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::Eq,
        Value::Uint(u64::from(value)),
        CoercionId::Strict,
    ))
}

fn execute_count_terminal<E>(
    load: &LoadExecutor<E>,
    plan: crate::db::executor::ExecutablePlan<E>,
) -> Result<u32, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(plan, ScalarTerminalBoundaryRequest::Count)?
        .into_count()
}

#[test]
fn aggregate_optimizations_bytes_by_strict_mode_surfaces_missing_row_corruption() {
    reset_store();

    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for (id, group, rank) in [(8_961u128, 7u32, 20u32), (8_962, 7, 20), (8_963, 7, 30)] {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(id),
            group,
            rank,
            label: format!("g{group}-r{rank}"),
        })
        .expect("strict bytes_by seed row save should succeed");
    }

    remove_pushdown_row_data(8_962);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let err = load
        .bytes_by_slot(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Error)
                .filter(u32_eq_predicate_strict("group", 7))
                .order_by("rank")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("strict bytes_by plan should build"),
            field_slot_for_test::<PushdownParityEntity>("rank"),
        )
        .expect_err("strict bytes_by should fail on missing primary rows");

    assert_eq!(
        err.class,
        ErrorClass::Corruption,
        "strict bytes_by must preserve missing-row corruption classification",
    );
    assert!(
        err.message.contains("missing row"),
        "strict bytes_by must preserve missing-row error context",
    );
}

#[test]
fn aggregate_optimizations_by_ids_count_dedups_before_windowing() {
    seed_simple_entities(&[8_651, 8_652, 8_653, 8_654, 8_655]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        execute_count_terminal(
            &load,
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_ids([
                    Ulid::from_u128(8_654),
                    Ulid::from_u128(8_652),
                    Ulid::from_u128(8_652),
                    Ulid::from_u128(8_651),
                ])
                .order_by("id")
                .offset(1)
                .limit(1)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("by_ids dedup COUNT plan should build"),
        )
        .expect("by_ids dedup COUNT should succeed")
    });

    assert_eq!(count, 1, "by_ids dedup COUNT should keep one in-window row");
    assert_eq!(
        scanned, 2,
        "ordered by_ids dedup COUNT should scan only offset + limit rows",
    );
}

#[test]
fn aggregate_optimizations_by_ids_count_desc_window_preserves_scan_budget() {
    seed_simple_entities(&[8_656, 8_657, 8_658, 8_659, 8_660]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        execute_count_terminal(
            &load,
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_ids([
                    Ulid::from_u128(8_659),
                    Ulid::from_u128(8_657),
                    Ulid::from_u128(8_657),
                    Ulid::from_u128(8_656),
                ])
                .order_by_desc("id")
                .offset(1)
                .limit(1)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("ordered by_ids DESC COUNT plan should build"),
        )
        .expect("ordered by_ids DESC COUNT should succeed")
    });

    assert_eq!(
        count, 1,
        "ordered by_ids DESC COUNT should keep one in-window row",
    );
    assert_eq!(
        scanned, 2,
        "ordered by_ids DESC COUNT should scan only offset + limit rows",
    );
}

#[test]
fn aggregate_optimizations_unordered_by_ids_count_preserves_canonical_dedup() {
    seed_simple_entities(&[8_701, 8_702, 8_703, 8_704]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let count = execute_count_terminal(
        &load,
        Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .by_ids([
                Ulid::from_u128(8_704),
                Ulid::from_u128(8_702),
                Ulid::from_u128(8_702),
                Ulid::from_u128(8_701),
            ])
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("unordered by-ids COUNT plan should build"),
    )
    .expect("unordered by-ids COUNT should succeed");

    assert_eq!(
        count, 3,
        "unordered by-ids COUNT should preserve canonical dedup semantics",
    );
}

#[test]
fn aggregate_optimizations_secondary_aggregate_explain_tracks_covering_projection() {
    let covering_exists = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(u32_eq_predicate_strict("group", 7))
        .explain_aggregate_terminal(aggregate::exists())
        .expect("strict-compatible EXISTS explain should build");
    let ordered_exists = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(u32_eq_predicate_strict("group", 7))
        .order_by("rank")
        .explain_aggregate_terminal(aggregate::exists())
        .expect("ordered EXISTS explain should build");
    let uncertain_exists = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            u32_eq_predicate_strict("group", 7),
            Predicate::TextContains {
                field: "label".to_string(),
                value: Value::Text("keep".to_string()),
            },
        ]))
        .explain_aggregate_terminal(aggregate::exists())
        .expect("strict-uncertain EXISTS explain should build");
    let covering_count = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(u32_eq_predicate_strict("group", 7))
        .explain_aggregate_terminal(aggregate::count())
        .expect("strict-compatible COUNT explain should build");
    let ordered_count = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(u32_eq_predicate_strict("group", 7))
        .order_by("rank")
        .explain_aggregate_terminal(aggregate::count())
        .expect("ordered COUNT explain should build");

    assert!(
        covering_exists.execution().covering_projection(),
        "strict secondary EXISTS explain should mark covering projection",
    );
    assert!(
        !ordered_exists.execution().covering_projection(),
        "ordered EXISTS explain should fall back from covering projection",
    );
    assert!(
        !uncertain_exists.execution().covering_projection(),
        "strict-uncertain EXISTS explain should fall back from covering projection",
    );
    assert!(
        covering_count.execution().covering_projection(),
        "strict secondary COUNT explain should mark covering projection",
    );
    assert!(
        !ordered_count.execution().covering_projection(),
        "ordered COUNT explain should fall back from covering projection",
    );
}
