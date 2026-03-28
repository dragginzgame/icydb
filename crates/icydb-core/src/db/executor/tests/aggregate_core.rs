use super::*;
use crate::{
    db::{
        executor::{
            ExecutablePlan,
            aggregate::{
                AggregateKind, ScalarTerminalBoundaryRequest,
                field::{
                    AggregateFieldValueError, resolve_orderable_aggregate_target_slot_with_model,
                },
            },
        },
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::{builder::AggregateExpr, intent::Query, plan::FieldSlot as PlannedFieldSlot},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    metrics::sink::{MetricsEvent, MetricsSink, with_metrics_sink},
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
    types::Ulid,
    value::Value,
};
use std::cell::RefCell;

type AggregateId<E> = crate::types::Id<E>;
type AggregateIdPair<E> = Option<(AggregateId<E>, AggregateId<E>)>;

///
/// AggregateCaptureSink
///
/// Small metrics sink used to keep aggregate scan-budget assertions live
/// without reviving the old aggregate matrix wrapper modules.
///

#[derive(Default)]
struct AggregateCaptureSink {
    events: RefCell<Vec<MetricsEvent>>,
}

impl AggregateCaptureSink {
    fn into_events(self) -> Vec<MetricsEvent> {
        self.events.into_inner()
    }
}

impl MetricsSink for AggregateCaptureSink {
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
    let sink = AggregateCaptureSink::default();
    let output = with_metrics_sink(&sink, run);
    let rows_scanned = rows_scanned_for_entity(&sink.into_events(), entity_path);

    (output, rows_scanned)
}

fn seed_pushdown_entities(rows: &[(u128, u32, u32)]) {
    reset_store();
    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);

    for (id, group, rank) in rows {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(*id),
            group: *group,
            rank: *rank,
            label: format!("group-{group}-rank-{rank}"),
        })
        .expect("aggregate pushdown seed save should succeed");
    }
}

fn seed_phase_entities(rows: &[(u128, u32)]) {
    reset_store();
    let save = SaveExecutor::<PhaseEntity>::new(DB, false);

    for (id, rank) in rows {
        save.insert(PhaseEntity {
            id: Ulid::from_u128(*id),
            opt_rank: None,
            rank: *rank,
            tags: vec![*rank],
            label: format!("phase-{rank}"),
        })
        .expect("aggregate phase seed save should succeed");
    }
}

fn planned_slot<E>(field: &str) -> PlannedFieldSlot
where
    E: EntityKind,
{
    let index = resolve_field_slot(E::MODEL, field).unwrap_or(0);

    PlannedFieldSlot {
        index,
        field: field.to_string(),
    }
}

fn u32_eq_predicate(field: &str, value: u32) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::Eq,
        Value::Uint(u64::from(value)),
        CoercionId::Strict,
    ))
}

fn execute_min_by_slot_terminal<E>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
    target_field: PlannedFieldSlot,
) -> Result<Option<crate::types::Id<E>>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(
        plan,
        ScalarTerminalBoundaryRequest::IdBySlot {
            kind: AggregateKind::Min,
            target_field,
        },
    )?
    .into_id::<E>()
}

fn execute_max_by_slot_terminal<E>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
    target_field: PlannedFieldSlot,
) -> Result<Option<crate::types::Id<E>>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(
        plan,
        ScalarTerminalBoundaryRequest::IdBySlot {
            kind: AggregateKind::Max,
            target_field,
        },
    )?
    .into_id::<E>()
}

fn execute_nth_by_slot_terminal<E>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
    target_field: PlannedFieldSlot,
    nth: usize,
) -> Result<Option<crate::types::Id<E>>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(
        plan,
        ScalarTerminalBoundaryRequest::NthBySlot { target_field, nth },
    )?
    .into_id::<E>()
}

fn execute_median_by_slot_terminal<E>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
    target_field: PlannedFieldSlot,
) -> Result<Option<crate::types::Id<E>>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(
        plan,
        ScalarTerminalBoundaryRequest::MedianBySlot { target_field },
    )?
    .into_id::<E>()
}

fn execute_min_max_by_slot_terminal<E>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
    target_field: PlannedFieldSlot,
) -> Result<AggregateIdPair<E>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(
        plan,
        ScalarTerminalBoundaryRequest::MinMaxBySlot { target_field },
    )?
    .into_id_pair::<E>()
}

// Execute the minimal executor-side validation that remains meaningful when a
// test bypasses planner aggregate-shape checks entirely.
fn execute_bypassed_field_target_validation<E>(
    aggregate: AggregateExpr,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    // Phase 1: preserve the executor invariant for field-target non-extrema
    // shapes when planner validation is intentionally bypassed in tests.
    if aggregate.target_field().is_some()
        && !matches!(aggregate.kind(), AggregateKind::Min | AggregateKind::Max)
    {
        return Err(InternalError::query_executor_invariant(format!(
            "field-target aggregate requires MIN/MAX terminal after planning: found {:?}",
            aggregate.kind()
        )));
    }

    // Phase 2: preserve field-target resolution taxonomy for unsupported and
    // unknown target fields without entering any runtime execution path.
    if let Some(target_field) = aggregate.target_field() {
        resolve_orderable_aggregate_target_slot_with_model(E::MODEL, target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;
    }

    Ok(())
}

#[test]
fn aggregate_core_field_target_non_extrema_is_executor_invariant_only_when_planner_is_bypassed() {
    seed_pushdown_entities(&[(8_021, 7, 10), (8_022, 7, 20), (8_023, 7, 30)]);

    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        execute_bypassed_field_target_validation::<PushdownParityEntity>(
            crate::db::query::builder::aggregate::count_by("rank"),
        )
    });
    let Err(err) = result else {
        panic!("bypassed field-target COUNT should fail with executor invariant");
    };

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
    assert_eq!(
        scanned, 0,
        "bypassed field-target COUNT should fail before any scan-budget consumption",
    );
    assert!(
        err.message
            .contains("field-target aggregate requires MIN/MAX terminal after planning"),
        "bypassed field-target non-extrema shape should preserve executor invariant taxonomy: {err:?}",
    );
}

#[test]
fn aggregate_core_unknown_rank_targets_fail_without_scan() {
    seed_pushdown_entities(&[(8_1981, 7, 10), (8_1982, 7, 20), (8_1983, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("aggregate unknown-field target plan should build")
    };
    let missing_field = planned_slot::<PushdownParityEntity>("missing_field");

    // Phase 1: execute every ranked field-target terminal against one missing
    // planner slot so the live harness locks the fail-closed taxonomy.
    let (top_rows_result, top_rows_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_slot(build_plan(), missing_field.clone(), 2)
        });
    let (bottom_rows_result, bottom_rows_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_slot(build_plan(), missing_field.clone(), 2)
        });
    let (top_values_result, top_values_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_values_slot(build_plan(), missing_field.clone(), 2)
        });
    let (bottom_values_result, bottom_values_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_values_slot(build_plan(), missing_field.clone(), 2)
        });
    let (top_values_with_ids_result, top_values_with_ids_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_with_ids_slot(build_plan(), missing_field.clone(), 2)
        });
    let (bottom_values_with_ids_result, bottom_values_with_ids_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_with_ids_slot(build_plan(), missing_field, 2)
        });

    // Phase 2: assert unsupported executor taxonomy and zero scan-budget use.
    for (label, result, scanned) in [
        ("top_k_by", top_rows_result.map(|_| ()), top_rows_scanned),
        (
            "bottom_k_by",
            bottom_rows_result.map(|_| ()),
            bottom_rows_scanned,
        ),
        (
            "top_k_by_values",
            top_values_result.map(|_| ()),
            top_values_scanned,
        ),
        (
            "bottom_k_by_values",
            bottom_values_result.map(|_| ()),
            bottom_values_scanned,
        ),
        (
            "top_k_by_with_ids",
            top_values_with_ids_result.map(|_| ()),
            top_values_with_ids_scanned,
        ),
        (
            "bottom_k_by_with_ids",
            bottom_values_with_ids_result.map(|_| ()),
            bottom_values_with_ids_scanned,
        ),
    ] {
        let Err(err) = result else {
            panic!("{label}(missing_field, k) should be rejected");
        };

        assert_eq!(
            err.class,
            ErrorClass::Unsupported,
            "{label}(missing_field, k) should keep unsupported executor classification",
        );
        assert_eq!(
            err.origin,
            ErrorOrigin::Executor,
            "{label}(missing_field, k) should keep executor origin",
        );
        assert_eq!(
            scanned, 0,
            "{label}(missing_field, k) should fail before scan-budget consumption",
        );
    }
}

#[test]
fn aggregate_core_non_orderable_rank_targets_fail_without_scan() {
    seed_phase_entities(&[(8_1991, 10), (8_1992, 20), (8_1993, 30)]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let build_plan = || {
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("aggregate non-orderable target plan should build")
    };
    let tags_slot = planned_slot::<PhaseEntity>("tags");

    let (top_result, top_scanned) = capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
        load.top_k_by_slot(build_plan(), tags_slot.clone(), 2)
    });
    let (bottom_result, bottom_scanned) =
        capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
            load.bottom_k_by_slot(build_plan(), tags_slot, 2)
        });

    let Err(top_err) = top_result else {
        panic!("top_k_by(tags, 2) should be rejected");
    };
    let Err(bottom_err) = bottom_result else {
        panic!("bottom_k_by(tags, 2) should be rejected");
    };

    assert_eq!(top_err.class, ErrorClass::Unsupported);
    assert_eq!(top_err.origin, ErrorOrigin::Executor);
    assert_eq!(top_scanned, 0);
    assert!(
        top_err.message.contains("does not support ordering"),
        "top_k_by(tags, 2) should preserve non-orderable field taxonomy: {top_err:?}",
    );
    assert_eq!(bottom_err.class, ErrorClass::Unsupported);
    assert_eq!(bottom_err.origin, ErrorOrigin::Executor);
    assert_eq!(bottom_scanned, 0);
    assert!(
        bottom_err.message.contains("does not support ordering"),
        "bottom_k_by(tags, 2) should preserve non-orderable field taxonomy: {bottom_err:?}",
    );
}

#[test]
fn aggregate_core_field_target_extrema_select_deterministic_ids() {
    seed_pushdown_entities(&[
        (8_031, 7, 20),
        (8_032, 7, 10),
        (8_033, 7, 10),
        (8_034, 7, 30),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("aggregate extrema plan should build")
    };

    let (min_id, scanned_min) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        execute_min_by_slot_terminal(
            &load,
            build_plan(),
            planned_slot::<PushdownParityEntity>("rank"),
        )
        .expect("field-target MIN should execute")
    });
    let (max_id, scanned_max) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        execute_max_by_slot_terminal(
            &load,
            build_plan(),
            planned_slot::<PushdownParityEntity>("rank"),
        )
        .expect("field-target MAX should execute")
    });

    assert_eq!(
        min_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_032)),
        "field-target MIN should select the smallest field value with pk-asc tie-break",
    );
    assert_eq!(
        max_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_034)),
        "field-target MAX should select the largest field value",
    );
    assert!(
        scanned_min > 0 && scanned_max > 0,
        "field-target extrema execution should consume scan budget once supported",
    );
}

#[test]
fn aggregate_core_field_target_tie_breaks_on_primary_key_ascending() {
    seed_pushdown_entities(&[
        (8_061, 7, 10),
        (8_062, 7, 10),
        (8_063, 7, 20),
        (8_064, 7, 20),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let min_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("field-target MIN tie-break plan should build");
    let max_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("field-target MAX tie-break plan should build");

    let min_id = execute_min_by_slot_terminal(
        &load,
        min_plan,
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("field-target MIN tie-break should succeed");
    let max_id = execute_max_by_slot_terminal(
        &load,
        max_plan,
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("field-target MAX tie-break should succeed");

    assert_eq!(
        min_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_061)),
        "field-target MIN tie-break should pick primary key ascending when values tie",
    );
    assert_eq!(
        max_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_063)),
        "field-target MAX tie-break should pick primary key ascending when values tie",
    );
}

#[test]
fn aggregate_core_nth_by_rank_selects_deterministic_positions() {
    seed_pushdown_entities(&[
        (8_142, 7, 10),
        (8_141, 7, 10),
        (8_144, 7, 30),
        (8_143, 7, 20),
        (8_145, 8, 5),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("field-target nth plan should build")
    };

    assert_eq!(
        execute_nth_by_slot_terminal(
            &load,
            build_plan(),
            planned_slot::<PushdownParityEntity>("rank"),
            0,
        )
        .expect("nth_by(rank, 0) should succeed")
        .map(|id| id.key()),
        Some(Ulid::from_u128(8_141)),
        "nth_by(rank, 0) should select the smallest rank with pk-asc tie-break",
    );
    assert_eq!(
        execute_nth_by_slot_terminal(
            &load,
            build_plan(),
            planned_slot::<PushdownParityEntity>("rank"),
            1,
        )
        .expect("nth_by(rank, 1) should succeed")
        .map(|id| id.key()),
        Some(Ulid::from_u128(8_142)),
        "nth_by(rank, 1) should advance through equal-rank ties using pk-asc order",
    );
    assert_eq!(
        execute_nth_by_slot_terminal(
            &load,
            build_plan(),
            planned_slot::<PushdownParityEntity>("rank"),
            2,
        )
        .expect("nth_by(rank, 2) should succeed")
        .map(|id| id.key()),
        Some(Ulid::from_u128(8_143)),
        "nth_by(rank, 2) should select the next field-ordered candidate",
    );
    assert_eq!(
        execute_nth_by_slot_terminal(
            &load,
            build_plan(),
            planned_slot::<PushdownParityEntity>("rank"),
            3,
        )
        .expect("nth_by(rank, 3) should succeed")
        .map(|id| id.key()),
        Some(Ulid::from_u128(8_144)),
        "nth_by(rank, 3) should select the highest rank in-window candidate",
    );
    assert_eq!(
        execute_nth_by_slot_terminal(
            &load,
            build_plan(),
            planned_slot::<PushdownParityEntity>("rank"),
            4,
        )
        .expect("nth_by(rank, 4) should succeed"),
        None,
        "nth_by(rank, 4) should return None when ordinal is outside the result window",
    );
}

#[test]
fn aggregate_core_nth_unknown_and_non_orderable_targets_fail_without_scan() {
    seed_pushdown_entities(&[(8_151, 7, 10), (8_152, 7, 20)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let unknown_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("field-target nth unknown-field plan should build");
    let (unknown_result, unknown_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_nth_by_slot_terminal(
                &load,
                unknown_plan,
                planned_slot::<PushdownParityEntity>("missing_field"),
                0,
            )
        });
    let Err(unknown_err) = unknown_result else {
        panic!("nth_by(missing_field, 0) should be rejected");
    };
    assert_eq!(unknown_err.class, ErrorClass::Unsupported);
    assert_eq!(unknown_err.origin, ErrorOrigin::Executor);
    assert_eq!(unknown_scanned, 0);

    seed_phase_entities(&[(8_161, 10), (8_162, 20)]);
    let phase_load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let non_orderable_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("field-target nth non-orderable plan should build");
    let (non_orderable_result, non_orderable_scanned) =
        capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
            execute_nth_by_slot_terminal(
                &phase_load,
                non_orderable_plan,
                planned_slot::<PhaseEntity>("tags"),
                0,
            )
        });
    let Err(non_orderable_err) = non_orderable_result else {
        panic!("nth_by(tags, 0) should be rejected");
    };
    assert_eq!(non_orderable_err.class, ErrorClass::Unsupported);
    assert_eq!(non_orderable_err.origin, ErrorOrigin::Executor);
    assert_eq!(non_orderable_scanned, 0);
}

#[test]
fn aggregate_core_median_even_window_uses_lower_policy() {
    seed_pushdown_entities(&[
        (8_181, 7, 10),
        (8_182, 7, 20),
        (8_183, 7, 30),
        (8_184, 7, 40),
        (8_185, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .limit(4)
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("field-target median plan should build")
    };

    let median = execute_median_by_slot_terminal(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("median_by(rank) should succeed");

    assert_eq!(
        median.map(|id| id.key()),
        Some(Ulid::from_u128(8_182)),
        "median_by(rank) should use lower-median policy for even-length windows",
    );
}

#[test]
fn aggregate_core_median_and_min_max_unknown_field_fail_without_scan() {
    seed_pushdown_entities(&[(8_1981, 7, 10), (8_1982, 7, 20), (8_1983, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("unknown-field terminal plan should build")
    };

    let (median_result, median_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_median_by_slot_terminal(
                &load,
                build_plan(),
                planned_slot::<PushdownParityEntity>("missing_field"),
            )
        });
    let Err(median_err) = median_result else {
        panic!("median_by(missing_field) should be rejected");
    };
    assert_eq!(median_err.class, ErrorClass::Unsupported);
    assert_eq!(median_err.origin, ErrorOrigin::Executor);
    assert_eq!(median_scanned, 0);

    let (min_max_result, min_max_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_min_max_by_slot_terminal(
                &load,
                build_plan(),
                planned_slot::<PushdownParityEntity>("missing_field"),
            )
        });
    let Err(min_max_err) = min_max_result else {
        panic!("min_max_by(missing_field) should be rejected");
    };
    assert_eq!(min_max_err.class, ErrorClass::Unsupported);
    assert_eq!(min_max_err.origin, ErrorOrigin::Executor);
    assert_eq!(min_max_scanned, 0);
}

#[test]
fn aggregate_core_min_max_matches_individual_extrema() {
    seed_pushdown_entities(&[
        (8_2011, 7, 10),
        (8_2012, 7, 10),
        (8_2013, 7, 40),
        (8_2014, 7, 40),
        (8_2015, 7, 25),
        (8_2016, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("field-target min-max plan should build")
    };

    let min_max = execute_min_max_by_slot_terminal(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("min_max_by(rank) should succeed");
    let min_by = execute_min_by_slot_terminal(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("min_by(rank) should succeed");
    let max_by = execute_max_by_slot_terminal(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("max_by(rank) should succeed");

    assert_eq!(
        min_max,
        min_by.zip(max_by),
        "min_max_by(rank) should match individual min_by/max_by terminals",
    );
    assert_eq!(
        min_max.map(|(min_id, _)| min_id.key()),
        Some(Ulid::from_u128(8_2011)),
        "min_max_by(rank) min tie-break should use primary key ascending",
    );
    assert_eq!(
        min_max.map(|(_, max_id)| max_id.key()),
        Some(Ulid::from_u128(8_2013)),
        "min_max_by(rank) max tie-break should use primary key ascending",
    );
}
