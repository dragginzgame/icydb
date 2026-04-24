//! Module: db::executor::tests::aggregate_numeric
//! Responsibility: live numeric aggregate semantics and fast-path contracts.
//! Does not own: generic aggregate parity helpers or non-numeric projection behavior.
//! Boundary: keeps numeric aggregate execution checks local to the revived executor harness.

use super::support::*;
use crate::{
    db::{
        access::{AccessPath, AccessPathKind, lower_executable_access_plan},
        executor::{
            PreparedExecutionPlan,
            aggregate::{AggregateKind, ScalarNumericFieldBoundaryRequest},
        },
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::intent::Query,
    },
    traits::{EntityKind, EntityValue},
    types::{Decimal, Ulid},
    value::Value,
};

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
        .expect("aggregate numeric seed save should succeed");
    }
}

fn planned_slot<E>(field: &str) -> crate::db::query::plan::FieldSlot
where
    E: EntityKind,
{
    let resolved_index = E::MODEL.resolve_field_slot(field);
    let index = resolved_index.unwrap_or(0);

    crate::db::query::plan::FieldSlot {
        index,
        field: field.to_string(),
        kind: resolved_index.and_then(|index| E::MODEL.fields.get(index).map(|field| field.kind)),
    }
}

fn execute_rank_sum<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
) -> Result<Option<Decimal>, crate::error::InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_numeric_field_boundary(
        plan,
        planned_slot::<E>("rank"),
        ScalarNumericFieldBoundaryRequest::Sum,
    )
}

fn execute_rank_avg<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
) -> Result<Option<Decimal>, crate::error::InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_numeric_field_boundary(
        plan,
        planned_slot::<E>("rank"),
        ScalarNumericFieldBoundaryRequest::Avg,
    )
}

fn execute_min_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
) -> Result<Option<crate::types::Id<E>>, crate::error::InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(
        plan,
        ScalarTerminalBoundaryRequest::IdTerminal {
            kind: AggregateKind::Min,
        },
    )?
    .into_id()
}

fn execute_max_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
) -> Result<Option<crate::types::Id<E>>, crate::error::InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(
        plan,
        ScalarTerminalBoundaryRequest::IdTerminal {
            kind: AggregateKind::Max,
        },
    )?
    .into_id()
}

fn execute_first_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
) -> Result<Option<crate::types::Id<E>>, crate::error::InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(
        plan,
        ScalarTerminalBoundaryRequest::IdTerminal {
            kind: AggregateKind::First,
        },
    )?
    .into_id()
}

fn execute_last_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
) -> Result<Option<crate::types::Id<E>>, crate::error::InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(
        plan,
        ScalarTerminalBoundaryRequest::IdTerminal {
            kind: AggregateKind::Last,
        },
    )?
    .into_id()
}

fn u32_eq_predicate(field: &str, value: u32) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::Eq,
        Value::Uint(u64::from(value)),
        CoercionId::Strict,
    ))
}

#[test]
fn aggregate_numeric_constant_false_window_returns_terminal_zeros_without_scan_budget() {
    seed_pushdown_entities(&[(8_011, 7, 10), (8_012, 7, 20), (8_013, 8, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Error)
            .filter_predicate(Predicate::False)
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("constant-false aggregate plan should build")
    };

    let (count, scanned_count) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_count_terminal(&load, build_plan())
                .expect("constant-false COUNT should succeed")
        });
    let (exists, scanned_exists) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_exists_terminal(&load, build_plan())
                .expect("constant-false EXISTS should succeed")
        });
    let (min_id, scanned_min) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        execute_min_terminal(&load, build_plan()).expect("constant-false MIN should succeed")
    });
    let (max_id, scanned_max) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        execute_max_terminal(&load, build_plan()).expect("constant-false MAX should succeed")
    });
    let (first_id, scanned_first) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_first_terminal(&load, build_plan())
                .expect("constant-false FIRST should succeed")
        });
    let (last_id, scanned_last) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_last_terminal(&load, build_plan()).expect("constant-false LAST should succeed")
        });
    let (sum_rank, scanned_sum) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.execute_numeric_field_boundary(
                build_plan(),
                planned_slot::<PushdownParityEntity>("rank"),
                ScalarNumericFieldBoundaryRequest::Sum,
            )
            .expect("constant-false SUM(field) should succeed")
        });
    let (avg_rank, scanned_avg) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.execute_numeric_field_boundary(
                build_plan(),
                planned_slot::<PushdownParityEntity>("rank"),
                ScalarNumericFieldBoundaryRequest::Avg,
            )
            .expect("constant-false AVG(field) should succeed")
        });

    assert_eq!(count, 0);
    assert!(!exists);
    assert_eq!(min_id, None);
    assert_eq!(max_id, None);
    assert_eq!(first_id, None);
    assert_eq!(last_id, None);
    assert_eq!(sum_rank, None);
    assert_eq!(avg_rank, None);
    assert_eq!(scanned_count, 0);
    assert_eq!(scanned_exists, 0);
    assert_eq!(scanned_min, 0);
    assert_eq!(scanned_max, 0);
    assert_eq!(scanned_first, 0);
    assert_eq!(scanned_last, 0);
    assert_eq!(scanned_sum, 0);
    assert_eq!(scanned_avg, 0);
}

#[test]
fn aggregate_numeric_sum_and_avg_use_decimal_projection() {
    seed_pushdown_entities(&[
        (8_091, 7, 10),
        (8_092, 7, 20),
        (8_093, 7, 35),
        (8_094, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter_predicate(u32_eq_predicate("group", 7))
            .order_term(crate::db::asc("rank"))
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("numeric field aggregate plan should build")
    };

    let sum = load
        .execute_numeric_field_boundary(
            build_plan(),
            planned_slot::<PushdownParityEntity>("rank"),
            ScalarNumericFieldBoundaryRequest::Sum,
        )
        .expect("sum_by(rank) should succeed");
    let avg = load
        .execute_numeric_field_boundary(
            build_plan(),
            planned_slot::<PushdownParityEntity>("rank"),
            ScalarNumericFieldBoundaryRequest::Avg,
        )
        .expect("avg_by(rank) should succeed");
    let expected_avg = Decimal::from_num(65u64).expect("sum decimal")
        / Decimal::from_num(3u64).expect("count decimal");

    assert_eq!(sum, Decimal::from_num(65u64));
    assert_eq!(avg, Some(expected_avg));
}

#[test]
fn aggregate_numeric_predicate_page_window_keeps_filtered_sum_and_avg() {
    seed_pushdown_entities(&[
        (8_9251, 7, 30),
        (8_9252, 7, 10),
        (8_9253, 7, 40),
        (8_9254, 8, 20),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter_predicate(u32_eq_predicate("group", 7))
            .order_term(crate::db::asc("id"))
            .offset(1)
            .limit(2)
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("predicate paged numeric aggregate plan should build")
    };

    let sum = execute_rank_sum(&load, build_plan()).expect("sum_by(rank) should succeed");
    let avg = execute_rank_avg(&load, build_plan()).expect("avg_by(rank) should succeed");

    assert_eq!(sum, Decimal::from_num(50u64));
    assert_eq!(avg, Decimal::from_num(25u64));
}

#[test]
fn aggregate_numeric_pk_page_window_keeps_effective_sum_and_avg() {
    seed_pushdown_entities(&[
        (8_9301, 7, 30),
        (8_9302, 7, 10),
        (8_9303, 7, 40),
        (8_9304, 7, 20),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_term(crate::db::asc("id"))
            .offset(1)
            .limit(2)
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("pk-ordered paged numeric aggregate plan should build")
    };

    let sum = execute_rank_sum(&load, build_plan()).expect("sum_by(rank) should succeed");
    let avg = execute_rank_avg(&load, build_plan()).expect("avg_by(rank) should succeed");

    assert_eq!(sum, Decimal::from_num(50u64));
    assert_eq!(avg, Decimal::from_num(25u64));
}

#[test]
fn aggregate_numeric_pk_page_window_scans_offset_plus_limit_rows() {
    seed_pushdown_entities(&[
        (8_9501, 7, 7),
        (8_9502, 7, 11),
        (8_9503, 7, 13),
        (8_9504, 7, 17),
        (8_9505, 7, 19),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("id"))
        .offset(1)
        .limit(2)
        .plan()
        .map(PreparedExecutionPlan::from)
        .expect("pk-ordered paged numeric aggregate plan should build");
    let (sum, rows_scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        execute_rank_sum(&load, plan).expect("sum_by(rank) should succeed")
    });

    assert_eq!(sum, Decimal::from_num(24u64));
    assert_eq!(rows_scanned, 3);
}

#[test]
fn aggregate_numeric_pk_desc_page_window_keeps_effective_sum_and_avg() {
    seed_pushdown_entities(&[
        (8_9401, 7, 30),
        (8_9402, 7, 10),
        (8_9403, 7, 40),
        (8_9404, 7, 20),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_term(crate::db::desc("id"))
            .offset(1)
            .limit(2)
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("pk-desc ordered paged numeric aggregate plan should build")
    };

    let sum = execute_rank_sum(&load, build_plan()).expect("sum_by(rank) should succeed");
    let avg = execute_rank_avg(&load, build_plan()).expect("avg_by(rank) should succeed");

    assert_eq!(sum, Decimal::from_num(50u64));
    assert_eq!(avg, Decimal::from_num(25u64));
}

#[test]
fn aggregate_numeric_by_ids_page_window_keeps_deduped_sum_and_avg() {
    seed_pushdown_entities(&[
        (8_9601, 7, 30),
        (8_9602, 7, 10),
        (8_9603, 7, 40),
        (8_9604, 7, 20),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .by_ids([
                Ulid::from_u128(8_9604),
                Ulid::from_u128(8_9602),
                Ulid::from_u128(8_9602),
                Ulid::from_u128(8_9601),
            ])
            .order_term(crate::db::asc("id"))
            .offset(1)
            .limit(2)
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("ordered by-ids paged numeric aggregate plan should build")
    };

    let sum = execute_rank_sum(&load, build_plan()).expect("sum_by(rank) should succeed");
    let avg = execute_rank_avg(&load, build_plan()).expect("avg_by(rank) should succeed");

    assert_eq!(sum, Decimal::from_num(30u64));
    assert_eq!(avg, Decimal::from_num(15u64));
}

#[test]
fn aggregate_numeric_by_ids_page_window_scans_offset_plus_limit_rows() {
    seed_pushdown_entities(&[
        (8_9651, 7, 30),
        (8_9652, 7, 10),
        (8_9653, 7, 40),
        (8_9654, 7, 20),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .by_ids([
            Ulid::from_u128(8_9654),
            Ulid::from_u128(8_9652),
            Ulid::from_u128(8_9652),
            Ulid::from_u128(8_9651),
        ])
        .order_term(crate::db::asc("id"))
        .offset(1)
        .limit(2)
        .plan()
        .map(PreparedExecutionPlan::from)
        .expect("ordered by-ids paged numeric aggregate plan should build");
    let (sum, rows_scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        execute_rank_sum(&load, plan).expect("sum_by(rank) should succeed")
    });

    assert_eq!(sum, Decimal::from_num(30u64));
    assert_eq!(rows_scanned, 3);
}

#[test]
fn aggregate_numeric_index_multi_lookup_keeps_shape_and_sum_avg_parity() {
    seed_pushdown_entities(&[
        (8_9681, 7, 10),
        (8_9682, 8, 20),
        (8_9683, 7, 30),
        (8_9684, 8, 40),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        let logical_plan = crate::db::query::plan::AccessPlannedQuery::new(
            AccessPath::IndexMultiLookup {
                index: PUSHDOWN_PARITY_INDEX_MODELS[0],
                values: vec![Value::Uint(7), Value::Uint(8)],
            },
            MissingRowPolicy::Ignore,
        );

        PreparedExecutionPlan::<PushdownParityEntity>::new(logical_plan)
    };
    let plan = build_plan();
    let executable = lower_executable_access_plan(plan.access());
    let Some(path) = executable.as_path() else {
        panic!("explicit index multi-lookup aggregate plan should stay a single-path access shape");
    };

    assert_eq!(path.capabilities().kind(), AccessPathKind::IndexMultiLookup);

    let sum = execute_rank_sum(&load, build_plan()).expect("sum_by(rank) should succeed");
    let avg = execute_rank_avg(&load, build_plan()).expect("avg_by(rank) should succeed");

    assert_eq!(sum, Decimal::from_num(100u64));
    assert_eq!(avg, Decimal::from_num(25u64));
}

#[test]
fn aggregate_numeric_by_id_keeps_single_row_sum_and_avg() {
    seed_pushdown_entities(&[(8_9701, 7, 30), (8_9702, 7, 10), (8_9703, 7, 40)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .by_id(Ulid::from_u128(8_9702))
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("by-id numeric aggregate plan should build")
    };

    let sum = execute_rank_sum(&load, build_plan()).expect("sum_by(rank) should succeed");
    let avg = execute_rank_avg(&load, build_plan()).expect("avg_by(rank) should succeed");

    assert_eq!(sum, Decimal::from_num(10u64));
    assert_eq!(avg, Decimal::from_num(10u64));
}

#[test]
fn aggregate_numeric_paged_by_id_keeps_single_row_sum_and_avg() {
    seed_pushdown_entities(&[(8_9751, 7, 30), (8_9752, 7, 10), (8_9753, 7, 40)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .by_id(Ulid::from_u128(8_9752))
            .order_term(crate::db::asc("id"))
            .offset(0)
            .limit(1)
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("paged by-id numeric aggregate plan should build")
    };

    let sum = execute_rank_sum(&load, build_plan()).expect("sum_by(rank) should succeed");
    let avg = execute_rank_avg(&load, build_plan()).expect("avg_by(rank) should succeed");

    assert_eq!(sum, Decimal::from_num(10u64));
    assert_eq!(avg, Decimal::from_num(10u64));
}

#[test]
fn aggregate_numeric_paged_by_id_scans_exactly_one_row() {
    seed_pushdown_entities(&[(8_9761, 7, 30), (8_9762, 7, 10), (8_9763, 7, 40)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .by_id(Ulid::from_u128(8_9762))
        .order_term(crate::db::asc("id"))
        .offset(0)
        .limit(1)
        .plan()
        .map(PreparedExecutionPlan::from)
        .expect("paged by-id numeric aggregate plan should build");
    let (sum, rows_scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        execute_rank_sum(&load, plan).expect("sum_by(rank) should succeed")
    });

    assert_eq!(sum, Decimal::from_num(10u64));
    assert_eq!(rows_scanned, 1);
}
