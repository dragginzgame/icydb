mod core_contract_matrices;
mod field_projection_matrices;
mod path_parity_matrices;
mod projection_matrices;
mod ranked_matrices;
mod secondary_index_matrices;
mod session_matrices;
mod tail_matrices;

use super::*;
use crate::{
    db::{
        access::AccessPath,
        data::DataKey,
        executor::{
            ExecutablePlan, ExecutionKernel,
            aggregate::{AggregateKind, AggregateSpec},
            route::ExecutionMode,
        },
        query::{
            explain::ExplainAccessPath,
            plan::{AccessPlannedQuery, OrderDirection, OrderSpec, PageSpec},
        },
        response::Response,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{MetricsEvent, MetricsSink, with_metrics_sink},
    types::{Decimal, Id},
};
use std::cell::RefCell;
use std::ops::Bound;

///
/// AggregateCaptureSink
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

fn seed_simple_entities(ids: &[u128]) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in ids {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(*id),
        })
        .expect("seed row save should succeed");
    }
}

fn seed_pushdown_entities(rows: &[(u128, u32, u32)]) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for (id, group, rank) in rows {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(*id),
            group: *group,
            rank: *rank,
            label: format!("g{group}-r{rank}"),
        })
        .expect("seed pushdown row save should succeed");
    }
}

fn seed_unique_index_range_entities(rows: &[(u128, u32)]) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    for (id, code) in rows {
        save.insert(UniqueIndexRangeEntity {
            id: Ulid::from_u128(*id),
            code: *code,
            label: format!("code-{code}"),
        })
        .expect("seed unique-index row save should succeed");
    }
}

fn seed_phase_entities(rows: &[(u128, u32)]) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for (id, rank) in rows {
        save.insert(PhaseEntity {
            id: Ulid::from_u128(*id),
            opt_rank: Some(*rank),
            rank: *rank,
            tags: vec![*rank],
            label: format!("phase-{rank}"),
        })
        .expect("seed phase row save should succeed");
    }
}

fn seed_phase_entities_custom(rows: Vec<PhaseEntity>) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in rows {
        save.insert(row)
            .expect("seed custom phase row save should succeed");
    }
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
            "expected row to exist before data-only removal"
        );
    });
}

///
/// AggregateParityValue
///
/// Canonical parity assertion payload used by the aggregate terminal matrix.
/// This keeps terminal result comparison uniform as the matrix expands.
///

#[derive(Eq, PartialEq)]
enum AggregateParityValue<E: EntityKind> {
    Count(u32),
    Exists(bool),
    Id(Option<Id<E>>),
}

type AggregateParityExpectedFn<E> = fn(&Response<E>) -> AggregateParityValue<E>;
type AggregateParityActualFn<E> =
    fn(&LoadExecutor<E>, ExecutablePlan<E>) -> Result<AggregateParityValue<E>, InternalError>;

///
/// AggregateParityCase
///
/// One aggregate terminal parity row used by the table-driven parity harness.
/// Each row defines expected materialized projection + actual aggregate call.
///

struct AggregateParityCase<E: EntityKind + EntityValue> {
    label: &'static str,
    expected: AggregateParityExpectedFn<E>,
    actual: AggregateParityActualFn<E>,
}

fn parity_expected_count<E: EntityKind>(response: &Response<E>) -> AggregateParityValue<E> {
    AggregateParityValue::Count(response.count())
}

fn parity_expected_exists<E: EntityKind>(response: &Response<E>) -> AggregateParityValue<E> {
    AggregateParityValue::Exists(!response.is_empty())
}

fn parity_expected_min<E: EntityKind>(response: &Response<E>) -> AggregateParityValue<E> {
    AggregateParityValue::Id(response.ids().into_iter().min())
}

fn parity_expected_max<E: EntityKind>(response: &Response<E>) -> AggregateParityValue<E> {
    AggregateParityValue::Id(response.ids().into_iter().max())
}

fn parity_expected_first<E: EntityKind>(response: &Response<E>) -> AggregateParityValue<E> {
    AggregateParityValue::Id(response.id())
}

fn parity_expected_last<E: EntityKind>(response: &Response<E>) -> AggregateParityValue<E> {
    AggregateParityValue::Id(response.ids().last().copied())
}

fn parity_actual_count<E: EntityKind + EntityValue>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
) -> Result<AggregateParityValue<E>, InternalError> {
    Ok(AggregateParityValue::Count(load.aggregate_count(plan)?))
}

fn parity_actual_exists<E: EntityKind + EntityValue>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
) -> Result<AggregateParityValue<E>, InternalError> {
    Ok(AggregateParityValue::Exists(load.aggregate_exists(plan)?))
}

fn parity_actual_min<E: EntityKind + EntityValue>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
) -> Result<AggregateParityValue<E>, InternalError> {
    Ok(AggregateParityValue::Id(load.aggregate_min(plan)?))
}

fn parity_actual_max<E: EntityKind + EntityValue>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
) -> Result<AggregateParityValue<E>, InternalError> {
    Ok(AggregateParityValue::Id(load.aggregate_max(plan)?))
}

fn parity_actual_first<E: EntityKind + EntityValue>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
) -> Result<AggregateParityValue<E>, InternalError> {
    Ok(AggregateParityValue::Id(load.aggregate_first(plan)?))
}

fn parity_actual_last<E: EntityKind + EntityValue>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
) -> Result<AggregateParityValue<E>, InternalError> {
    Ok(AggregateParityValue::Id(load.aggregate_last(plan)?))
}

fn aggregate_id_terminal_parity_cases<E: EntityKind + EntityValue>() -> [AggregateParityCase<E>; 6]
{
    [
        AggregateParityCase {
            label: "count",
            expected: parity_expected_count::<E>,
            actual: parity_actual_count::<E>,
        },
        AggregateParityCase {
            label: "exists",
            expected: parity_expected_exists::<E>,
            actual: parity_actual_exists::<E>,
        },
        AggregateParityCase {
            label: "min",
            expected: parity_expected_min::<E>,
            actual: parity_actual_min::<E>,
        },
        AggregateParityCase {
            label: "max",
            expected: parity_expected_max::<E>,
            actual: parity_actual_max::<E>,
        },
        AggregateParityCase {
            label: "first",
            expected: parity_expected_first::<E>,
            actual: parity_actual_first::<E>,
        },
        AggregateParityCase {
            label: "last",
            expected: parity_expected_last::<E>,
            actual: parity_actual_last::<E>,
        },
    ]
}

#[derive(PartialEq)]
enum FieldAggregateParityValue {
    Id(Option<Id<PushdownParityEntity>>),
    Decimal(Option<Decimal>),
    Count(u32),
    IdPair(Option<(Id<PushdownParityEntity>, Id<PushdownParityEntity>)>),
}

type FieldAggregateParityExpectedFn =
    fn(&Response<PushdownParityEntity>) -> FieldAggregateParityValue;
type FieldAggregateParityActualFn = fn(
    &LoadExecutor<PushdownParityEntity>,
    ExecutablePlan<PushdownParityEntity>,
) -> Result<FieldAggregateParityValue, InternalError>;

struct FieldAggregateParityCase {
    label: &'static str,
    expected: FieldAggregateParityExpectedFn,
    actual: FieldAggregateParityActualFn,
}

const FIELD_PARITY_NTH_ORDINAL: usize = 1;

fn expected_min_by_rank_id(
    response: &Response<PushdownParityEntity>,
) -> Option<Id<PushdownParityEntity>> {
    let mut best: Option<(Id<PushdownParityEntity>, u32)> = None;
    for (id, entity) in &response.0 {
        if best.is_none_or(|(best_id, best_rank)| {
            entity.rank < best_rank || (entity.rank == best_rank && id.key() < best_id.key())
        }) {
            best = Some((*id, entity.rank));
        }
    }

    best.map(|(id, _)| id)
}

fn expected_max_by_rank_id(
    response: &Response<PushdownParityEntity>,
) -> Option<Id<PushdownParityEntity>> {
    let mut best: Option<(Id<PushdownParityEntity>, u32)> = None;
    for (id, entity) in &response.0 {
        if best.is_none_or(|(best_id, best_rank)| {
            entity.rank > best_rank || (entity.rank == best_rank && id.key() < best_id.key())
        }) {
            best = Some((*id, entity.rank));
        }
    }

    best.map(|(id, _)| id)
}

fn expected_nth_by_rank_id(
    response: &Response<PushdownParityEntity>,
    nth: usize,
) -> Option<Id<PushdownParityEntity>> {
    let ordered = ordered_rank_ids(response);

    ordered.get(nth).copied()
}

fn expected_median_by_rank_id(
    response: &Response<PushdownParityEntity>,
) -> Option<Id<PushdownParityEntity>> {
    let ordered = ordered_rank_ids(response);
    if ordered.is_empty() {
        return None;
    }

    let median_index = if ordered.len().is_multiple_of(2) {
        ordered.len() / 2 - 1
    } else {
        ordered.len() / 2
    };

    ordered.get(median_index).copied()
}

fn expected_count_distinct_by_rank(response: &Response<PushdownParityEntity>) -> u32 {
    let mut seen_ranks: Vec<u32> = Vec::new();
    let mut count = 0u32;
    for (_, entity) in &response.0 {
        if seen_ranks.iter().any(|existing| existing == &entity.rank) {
            continue;
        }

        seen_ranks.push(entity.rank);
        count = count.saturating_add(1);
    }

    count
}

fn expected_values_by_rank(response: &Response<PushdownParityEntity>) -> Vec<Value> {
    response
        .0
        .iter()
        .map(|(_, entity)| Value::Uint(u64::from(entity.rank)))
        .collect()
}

fn expected_values_by_rank_with_ids(
    response: &Response<PushdownParityEntity>,
) -> Vec<(Id<PushdownParityEntity>, Value)> {
    response
        .0
        .iter()
        .map(|(id, entity)| (*id, Value::Uint(u64::from(entity.rank))))
        .collect()
}

fn expected_distinct_values_by_rank(response: &Response<PushdownParityEntity>) -> Vec<Value> {
    let mut distinct_values = Vec::new();
    for (_, entity) in &response.0 {
        let value = Value::Uint(u64::from(entity.rank));
        if distinct_values.iter().any(|existing| existing == &value) {
            continue;
        }
        distinct_values.push(value);
    }

    distinct_values
}

fn expected_first_value_by_rank(response: &Response<PushdownParityEntity>) -> Option<Value> {
    response
        .0
        .first()
        .map(|(_, entity)| Value::Uint(u64::from(entity.rank)))
}

fn expected_last_value_by_rank(response: &Response<PushdownParityEntity>) -> Option<Value> {
    response
        .0
        .last()
        .map(|(_, entity)| Value::Uint(u64::from(entity.rank)))
}

fn expected_min_max_by_rank_ids(
    response: &Response<PushdownParityEntity>,
) -> Option<(Id<PushdownParityEntity>, Id<PushdownParityEntity>)> {
    expected_min_by_rank_id(response).zip(expected_max_by_rank_id(response))
}

fn ordered_rank_ids(response: &Response<PushdownParityEntity>) -> Vec<Id<PushdownParityEntity>> {
    let mut ordered = response
        .0
        .iter()
        .map(|(id, entity)| (entity.rank, *id))
        .collect::<Vec<_>>();
    ordered.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        left_rank
            .cmp(right_rank)
            .then_with(|| left_id.key().cmp(&right_id.key()))
    });

    ordered.into_iter().map(|(_, id)| id).collect()
}

fn expected_sum_by_rank(response: &Response<PushdownParityEntity>) -> Option<Decimal> {
    if response.is_empty() {
        return None;
    }

    let mut sum = Decimal::ZERO;
    for (_, entity) in &response.0 {
        let rank = Decimal::from_num(u64::from(entity.rank)).expect("rank decimal conversion");
        sum += rank;
    }

    Some(sum)
}

fn expected_avg_by_rank(response: &Response<PushdownParityEntity>) -> Option<Decimal> {
    let sum = expected_sum_by_rank(response)?;
    let count = Decimal::from_num(u64::from(response.count())).expect("row count decimal");
    Some(sum / count)
}

fn field_parity_expected_min_by_rank(
    response: &Response<PushdownParityEntity>,
) -> FieldAggregateParityValue {
    FieldAggregateParityValue::Id(expected_min_by_rank_id(response))
}

fn field_parity_expected_max_by_rank(
    response: &Response<PushdownParityEntity>,
) -> FieldAggregateParityValue {
    FieldAggregateParityValue::Id(expected_max_by_rank_id(response))
}

fn field_parity_expected_nth_by_rank(
    response: &Response<PushdownParityEntity>,
) -> FieldAggregateParityValue {
    FieldAggregateParityValue::Id(expected_nth_by_rank_id(response, FIELD_PARITY_NTH_ORDINAL))
}

fn field_parity_expected_sum_by_rank(
    response: &Response<PushdownParityEntity>,
) -> FieldAggregateParityValue {
    FieldAggregateParityValue::Decimal(expected_sum_by_rank(response))
}

fn field_parity_expected_avg_by_rank(
    response: &Response<PushdownParityEntity>,
) -> FieldAggregateParityValue {
    FieldAggregateParityValue::Decimal(expected_avg_by_rank(response))
}

fn field_parity_expected_median_by_rank(
    response: &Response<PushdownParityEntity>,
) -> FieldAggregateParityValue {
    FieldAggregateParityValue::Id(expected_median_by_rank_id(response))
}

fn field_parity_expected_count_distinct_by_rank(
    response: &Response<PushdownParityEntity>,
) -> FieldAggregateParityValue {
    FieldAggregateParityValue::Count(expected_count_distinct_by_rank(response))
}

fn field_parity_expected_min_max_by_rank(
    response: &Response<PushdownParityEntity>,
) -> FieldAggregateParityValue {
    FieldAggregateParityValue::IdPair(expected_min_max_by_rank_ids(response))
}

fn field_parity_actual_min_by_rank(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
) -> Result<FieldAggregateParityValue, InternalError> {
    Ok(FieldAggregateParityValue::Id(
        load.aggregate_min_by(plan, "rank")?,
    ))
}

fn field_parity_actual_max_by_rank(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
) -> Result<FieldAggregateParityValue, InternalError> {
    Ok(FieldAggregateParityValue::Id(
        load.aggregate_max_by(plan, "rank")?,
    ))
}

fn field_parity_actual_nth_by_rank(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
) -> Result<FieldAggregateParityValue, InternalError> {
    Ok(FieldAggregateParityValue::Id(load.aggregate_nth_by(
        plan,
        "rank",
        FIELD_PARITY_NTH_ORDINAL,
    )?))
}

fn field_parity_actual_sum_by_rank(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
) -> Result<FieldAggregateParityValue, InternalError> {
    Ok(FieldAggregateParityValue::Decimal(
        load.aggregate_sum_by(plan, "rank")?,
    ))
}

fn field_parity_actual_avg_by_rank(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
) -> Result<FieldAggregateParityValue, InternalError> {
    Ok(FieldAggregateParityValue::Decimal(
        load.aggregate_avg_by(plan, "rank")?,
    ))
}

fn field_parity_actual_median_by_rank(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
) -> Result<FieldAggregateParityValue, InternalError> {
    Ok(FieldAggregateParityValue::Id(
        load.aggregate_median_by(plan, "rank")?,
    ))
}

fn field_parity_actual_count_distinct_by_rank(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
) -> Result<FieldAggregateParityValue, InternalError> {
    Ok(FieldAggregateParityValue::Count(
        load.aggregate_count_distinct_by(plan, "rank")?,
    ))
}

fn field_parity_actual_min_max_by_rank(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
) -> Result<FieldAggregateParityValue, InternalError> {
    Ok(FieldAggregateParityValue::IdPair(
        load.aggregate_min_max_by(plan, "rank")?,
    ))
}

fn aggregate_field_terminal_parity_cases() -> [FieldAggregateParityCase; 8] {
    [
        FieldAggregateParityCase {
            label: "min_by(rank)",
            expected: field_parity_expected_min_by_rank,
            actual: field_parity_actual_min_by_rank,
        },
        FieldAggregateParityCase {
            label: "max_by(rank)",
            expected: field_parity_expected_max_by_rank,
            actual: field_parity_actual_max_by_rank,
        },
        FieldAggregateParityCase {
            label: "nth_by(rank, 1)",
            expected: field_parity_expected_nth_by_rank,
            actual: field_parity_actual_nth_by_rank,
        },
        FieldAggregateParityCase {
            label: "sum_by(rank)",
            expected: field_parity_expected_sum_by_rank,
            actual: field_parity_actual_sum_by_rank,
        },
        FieldAggregateParityCase {
            label: "avg_by(rank)",
            expected: field_parity_expected_avg_by_rank,
            actual: field_parity_actual_avg_by_rank,
        },
        FieldAggregateParityCase {
            label: "median_by(rank)",
            expected: field_parity_expected_median_by_rank,
            actual: field_parity_actual_median_by_rank,
        },
        FieldAggregateParityCase {
            label: "count_distinct_by(rank)",
            expected: field_parity_expected_count_distinct_by_rank,
            actual: field_parity_actual_count_distinct_by_rank,
        },
        FieldAggregateParityCase {
            label: "min_max_by(rank)",
            expected: field_parity_expected_min_max_by_rank,
            actual: field_parity_actual_min_max_by_rank,
        },
    ]
}

fn assert_aggregate_parity_for_query<E>(
    load: &LoadExecutor<E>,
    make_query: impl Fn() -> Query<E>,
    context: &str,
) where
    E: EntityKind<Canister = TestCanister> + EntityValue,
{
    // Execute canonical materialized baseline once per query shape.
    let expected_response = load
        .execute(
            make_query()
                .plan()
                .expect("baseline materialized plan should build"),
        )
        .expect("baseline materialized execution should succeed");

    // Execute aggregate terminals against the same logical query shape through
    // one matrix so parity additions remain one-row changes.
    for case in aggregate_id_terminal_parity_cases::<E>() {
        let expected = (case.expected)(&expected_response);
        let actual = (case.actual)(
            load,
            make_query()
                .plan()
                .expect("aggregate parity matrix plan should build"),
        )
        .expect("aggregate parity matrix terminal should succeed");

        assert!(
            actual == expected,
            "{context}: {} parity failed",
            case.label
        );
    }
}

fn assert_field_aggregate_parity_for_query(
    load: &LoadExecutor<PushdownParityEntity>,
    make_query: impl Fn() -> Query<PushdownParityEntity>,
    context: &str,
) {
    let expected_response = load
        .execute(
            make_query()
                .plan()
                .expect("baseline field aggregate parity plan should build"),
        )
        .expect("baseline field aggregate parity execution should succeed");

    for case in aggregate_field_terminal_parity_cases() {
        let expected = (case.expected)(&expected_response);
        let actual = (case.actual)(
            load,
            make_query()
                .plan()
                .expect("field aggregate parity matrix plan should build"),
        )
        .expect("field aggregate parity matrix terminal should succeed");

        assert!(
            actual == expected,
            "{context}: {} parity failed",
            case.label
        );
    }
}

fn assert_count_parity_for_query<E>(
    load: &LoadExecutor<E>,
    make_query: impl Fn() -> Query<E>,
    context: &str,
) where
    E: EntityKind<Canister = TestCanister> + EntityValue,
{
    let expected_count = load
        .execute(
            make_query()
                .plan()
                .expect("baseline materialized plan should build"),
        )
        .expect("baseline materialized execution should succeed")
        .count();

    let actual_count = load
        .aggregate_count(
            make_query()
                .plan()
                .expect("aggregate COUNT plan should build"),
        )
        .expect("aggregate COUNT should succeed");

    assert_eq!(
        actual_count, expected_count,
        "{context}: count parity failed"
    );
}

fn id_in_predicate(ids: &[u128]) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::In,
        Value::List(
            ids.iter()
                .copied()
                .map(|id| Value::Ulid(Ulid::from_u128(id)))
                .collect(),
        ),
        CoercionId::Strict,
    ))
}

fn explain_access_supports_count_pushdown(access: &ExplainAccessPath) -> bool {
    match access {
        ExplainAccessPath::FullScan | ExplainAccessPath::KeyRange { .. } => true,
        ExplainAccessPath::Union(_)
        | ExplainAccessPath::Intersection(_)
        | ExplainAccessPath::ByKey { .. }
        | ExplainAccessPath::ByKeys { .. }
        | ExplainAccessPath::IndexPrefix { .. }
        | ExplainAccessPath::IndexRange { .. } => false,
    }
}

fn count_pushdown_contract_eligible<E>(plan: &crate::db::executor::ExecutablePlan<E>) -> bool
where
    E: EntityKind<Canister = TestCanister> + EntityValue,
{
    ExecutionKernel::is_streaming_access_shape_safe::<E, _>(plan.as_inner())
        && explain_access_supports_count_pushdown(&plan.explain().access)
}

fn u32_eq_predicate_with_coercion(field: &str, value: u32, coercion: CoercionId) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::Eq,
        Value::Uint(u64::from(value)),
        coercion,
    ))
}

fn u32_in_predicate_with_coercion(field: &str, values: &[u32], coercion: CoercionId) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::In,
        Value::List(
            values
                .iter()
                .copied()
                .map(u64::from)
                .map(Value::Uint)
                .collect(),
        ),
        coercion,
    ))
}

fn u32_eq_predicate(field: &str, value: u32) -> Predicate {
    u32_eq_predicate_with_coercion(field, value, CoercionId::NumericWiden)
}

fn u32_eq_predicate_strict(field: &str, value: u32) -> Predicate {
    u32_eq_predicate_with_coercion(field, value, CoercionId::Strict)
}

fn u32_in_predicate(field: &str, values: &[u32]) -> Predicate {
    u32_in_predicate_with_coercion(field, values, CoercionId::NumericWiden)
}

fn u32_in_predicate_strict(field: &str, values: &[u32]) -> Predicate {
    u32_in_predicate_with_coercion(field, values, CoercionId::Strict)
}

fn u32_range_predicate(field: &str, lower_inclusive: u32, upper_exclusive: u32) -> Predicate {
    Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            field,
            CompareOp::Gte,
            Value::Uint(u64::from(lower_inclusive)),
            CoercionId::NumericWiden,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            field,
            CompareOp::Lt,
            Value::Uint(u64::from(upper_exclusive)),
            CoercionId::NumericWiden,
        )),
    ])
}

fn secondary_group_rank_order_plan(
    consistency: MissingRowPolicy,
    direction: crate::db::query::plan::OrderDirection,
    offset: u32,
) -> crate::db::executor::ExecutablePlan<PushdownParityEntity> {
    let mut logical_plan = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::IndexPrefix {
            index: PUSHDOWN_PARITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        consistency,
    );
    logical_plan.order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![
            ("rank".to_string(), direction),
            ("id".to_string(), direction),
        ],
    });
    logical_plan.page = Some(crate::db::query::plan::PageSpec {
        limit: None,
        offset,
    });

    crate::db::executor::ExecutablePlan::<PushdownParityEntity>::new(logical_plan)
}

#[test]
#[expect(clippy::too_many_lines)]
fn aggregate_empty_window_semantics_match_between_streaming_and_materialized_routes() {
    let mut rows = Vec::new();
    for rank in 0u32..160u32 {
        rows.push((12_601u128.saturating_add(u128::from(rank)), 7, rank));
    }
    for rank in 0u32..40u32 {
        rows.push((13_001u128.saturating_add(u128::from(rank)), 8, rank));
    }
    seed_pushdown_entities(rows.as_slice());
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let strict_filter = Predicate::And(vec![
        u32_eq_predicate_strict("group", 7),
        u32_in_predicate_strict("rank", &[151, 152, 153]),
    ]);
    let widen_filter = Predicate::And(vec![
        u32_eq_predicate("group", 7),
        u32_in_predicate("rank", &[151, 152, 153]),
    ]);

    // Build matching empty windows: three matching rows with offset beyond
    // result cardinality guarantees an empty aggregate input window.
    let strict_query = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(strict_filter.clone())
            .order_by("rank")
            .offset(10)
            .limit(5)
    };
    let widen_query = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(widen_filter.clone())
            .order_by("rank")
            .offset(10)
            .limit(5)
    };

    // Sanity-check route modes for non-count scalar terminals: strict route
    // stays streaming while widen route forces materialized fallback.
    for kind in [
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        let strict_route =
            LoadExecutor::<PushdownParityEntity>::build_execution_route_plan_for_aggregate(
                strict_query()
                    .plan()
                    .expect("strict empty-window route plan should build")
                    .as_inner(),
                kind,
            );
        let widen_route =
            LoadExecutor::<PushdownParityEntity>::build_execution_route_plan_for_aggregate(
                widen_query()
                    .plan()
                    .expect("widen empty-window route plan should build")
                    .as_inner(),
                kind,
            );

        assert_eq!(
            strict_route.execution_mode,
            ExecutionMode::Streaming,
            "strict empty-window route should stay streaming for terminal={kind:?}"
        );
        assert_eq!(
            widen_route.execution_mode,
            ExecutionMode::Materialized,
            "widen empty-window route should force materialized for terminal={kind:?}"
        );
    }

    // Validate canonical empty-input outputs for non-count scalar terminals.
    let strict_results = (
        load.aggregate_exists(
            strict_query()
                .plan()
                .expect("strict exists empty-window plan should build"),
        )
        .expect("strict exists empty-window should succeed"),
        load.aggregate_min(
            strict_query()
                .plan()
                .expect("strict min empty-window plan should build"),
        )
        .expect("strict min empty-window should succeed"),
        load.aggregate_max(
            strict_query()
                .plan()
                .expect("strict max empty-window plan should build"),
        )
        .expect("strict max empty-window should succeed"),
        load.aggregate_first(
            strict_query()
                .plan()
                .expect("strict first empty-window plan should build"),
        )
        .expect("strict first empty-window should succeed"),
        load.aggregate_last(
            strict_query()
                .plan()
                .expect("strict last empty-window plan should build"),
        )
        .expect("strict last empty-window should succeed"),
    );
    let widen_results = (
        load.aggregate_exists(
            widen_query()
                .plan()
                .expect("widen exists empty-window plan should build"),
        )
        .expect("widen exists empty-window should succeed"),
        load.aggregate_min(
            widen_query()
                .plan()
                .expect("widen min empty-window plan should build"),
        )
        .expect("widen min empty-window should succeed"),
        load.aggregate_max(
            widen_query()
                .plan()
                .expect("widen max empty-window plan should build"),
        )
        .expect("widen max empty-window should succeed"),
        load.aggregate_first(
            widen_query()
                .plan()
                .expect("widen first empty-window plan should build"),
        )
        .expect("widen first empty-window should succeed"),
        load.aggregate_last(
            widen_query()
                .plan()
                .expect("widen last empty-window plan should build"),
        )
        .expect("widen last empty-window should succeed"),
    );

    // COUNT has a dedicated route gate: exercise one explicit streaming-empty
    // shape (full-scan pushdown-safe) and one explicit materialized-empty shape
    // (widened uncertain predicate) and assert canonical zero semantics.
    let streaming_count_query = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .offset(500)
            .limit(5)
    };
    let streaming_count_route =
        LoadExecutor::<PushdownParityEntity>::build_execution_route_plan_for_aggregate(
            streaming_count_query()
                .plan()
                .expect("streaming count empty-window route plan should build")
                .as_inner(),
            AggregateKind::Count,
        );
    let materialized_count_route =
        LoadExecutor::<PushdownParityEntity>::build_execution_route_plan_for_aggregate(
            widen_query()
                .plan()
                .expect("materialized count empty-window route plan should build")
                .as_inner(),
            AggregateKind::Count,
        );
    assert_eq!(
        streaming_count_route.execution_mode,
        ExecutionMode::Streaming,
        "full-scan empty-window count route should stay streaming"
    );
    assert_eq!(
        materialized_count_route.execution_mode,
        ExecutionMode::Materialized,
        "uncertain empty-window count route should force materialized"
    );
    let streaming_count = load
        .aggregate_count(
            streaming_count_query()
                .plan()
                .expect("streaming count empty-window plan should build"),
        )
        .expect("streaming count empty-window should succeed");
    let materialized_count = load
        .aggregate_count(
            widen_query()
                .plan()
                .expect("materialized count empty-window plan should build"),
        )
        .expect("materialized count empty-window should succeed");

    let expected_empty = (
        false,
        None::<Id<PushdownParityEntity>>,
        None::<Id<PushdownParityEntity>>,
        None::<Id<PushdownParityEntity>>,
        None::<Id<PushdownParityEntity>>,
    );
    assert_eq!(
        strict_results, expected_empty,
        "strict empty-window scalar aggregate semantics must stay canonical"
    );
    assert_eq!(
        widen_results, expected_empty,
        "materialized empty-window scalar aggregate semantics must stay canonical"
    );
    assert_eq!(
        strict_results, widen_results,
        "streaming and materialized empty-window scalar terminal results must match"
    );
    assert_eq!(
        streaming_count, 0,
        "streaming empty-window count should return zero"
    );
    assert_eq!(
        materialized_count, 0,
        "materialized empty-window count should return zero"
    );
    assert_eq!(
        streaming_count, materialized_count,
        "streaming and materialized empty-window count results must match"
    );
}

///
/// RankedDirectionBehaviorCell
///
/// Canonical behavior-matrix cell for ranked terminal direction invariance.
/// Axes are capability x path x nullability x grouping mode.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RankedDirectionBehaviorCell {
    capability: &'static str,
    path: &'static str,
    nullability: &'static str,
    grouping: &'static str,
}

///
/// RankedDirectionResult
///
/// Canonical result payload used by ranked direction-invariance matrix rows.
/// This allows one table-driven invariant across id/value projection forms.
///

#[derive(Debug, PartialEq)]
enum RankedDirectionResult {
    Ids(Vec<Id<PushdownParityEntity>>),
    Values(Vec<Value>),
    ValuesWithIds(Vec<(Id<PushdownParityEntity>, Value)>),
}
