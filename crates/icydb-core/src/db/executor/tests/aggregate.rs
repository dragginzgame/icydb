use super::*;
use crate::{
    db::{
        access::AccessPath,
        data::DataKey,
        executor::{
            ExecutablePlan, ExecutionKernel,
            aggregate::{AggregateKind, AggregateSpec},
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
    consistency: ReadConsistency,
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
fn aggregate_parity_ordered_page_window_asc() {
    seed_simple_entities(&[8101, 8102, 8103, 8104, 8105, 8106, 8107, 8108]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .offset(2)
                .limit(3)
        },
        "ordered ASC page window",
    );
}

#[test]
fn aggregate_parity_matrix_harness_covers_all_id_terminals() {
    let labels = aggregate_id_terminal_parity_cases::<SimpleEntity>().map(|case| case.label);

    assert_eq!(labels, ["count", "exists", "min", "max", "first", "last"]);
}

#[test]
fn aggregate_spec_field_target_non_extrema_surfaces_unsupported_taxonomy() {
    seed_pushdown_entities(&[(8_021, 7, 10), (8_022, 7, 20), (8_023, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .plan()
        .expect("field-target non-extrema aggregate plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        ExecutionKernel::execute_aggregate_spec(
            &load,
            plan,
            AggregateSpec::for_target_field(AggregateKind::Count, "rank"),
        )
    });
    let Err(err) = result else {
        panic!("field-target COUNT should be rejected by unsupported taxonomy");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "unsupported field-target COUNT should fail before any scan-budget consumption"
    );
    assert!(
        err.message.contains("only supported for min/max terminals"),
        "field-target non-extrema taxonomy should be explicit: {err:?}"
    );
}

#[test]
fn aggregate_spec_field_target_extrema_selects_deterministic_ids() {
    seed_pushdown_entities(&[
        (8_031, 7, 20),
        (8_032, 7, 10),
        (8_033, 7, 10),
        (8_034, 7, 30),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .plan()
            .expect("field-target extrema aggregate plan should build")
    };

    let (min_id, scanned_min) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_min_by(build_plan(), "rank")
            .expect("field-target MIN should execute")
    });
    let (max_id, scanned_max) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_max_by(build_plan(), "rank")
            .expect("field-target MAX should execute")
    });

    assert_eq!(
        min_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_032)),
        "field-target MIN should select the smallest field value with pk-asc tie-break"
    );
    assert_eq!(
        max_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_034)),
        "field-target MAX should select the largest field value"
    );
    assert!(
        scanned_min > 0 && scanned_max > 0,
        "field-target extrema execution should consume scan budget once supported"
    );
}

#[test]
fn aggregate_spec_field_target_unknown_field_surfaces_unsupported_without_scan() {
    seed_pushdown_entities(&[(8_041, 7, 10), (8_042, 7, 20), (8_043, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .plan()
        .expect("field-target unknown-field aggregate plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        ExecutionKernel::execute_aggregate_spec(
            &load,
            plan,
            AggregateSpec::for_target_field(AggregateKind::Min, "missing_field"),
        )
    });
    let Err(err) = result else {
        panic!("field-target unknown field should be rejected until the 0.25 capability ships");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "field-target unknown-field MIN should fail before any scan-budget consumption"
    );
    assert!(
        err.message.contains("unknown aggregate target field"),
        "unknown field taxonomy should remain explicit: {err:?}"
    );
}

#[test]
fn aggregate_spec_field_target_non_orderable_field_surfaces_unsupported_without_scan() {
    seed_phase_entities(&[(8_051, 10), (8_052, 20), (8_053, 30)]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .plan()
        .expect("field-target non-orderable aggregate plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
        ExecutionKernel::execute_aggregate_spec(
            &load,
            plan,
            AggregateSpec::for_target_field(AggregateKind::Min, "tags"),
        )
    });
    let Err(err) = result else {
        panic!("field-target MIN on list field should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "field-target non-orderable MIN should fail before any scan-budget consumption"
    );
    assert!(
        err.message.contains("does not support ordering"),
        "non-orderable field taxonomy should remain explicit: {err:?}"
    );
}

#[test]
fn aggregate_spec_field_target_tie_breaks_on_primary_key_ascending() {
    seed_pushdown_entities(&[
        (8_061, 7, 10),
        (8_062, 7, 10),
        (8_063, 7, 20),
        (8_064, 7, 20),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let min_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("id")
        .plan()
        .expect("field-target MIN tie-break plan should build");
    let max_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("id")
        .plan()
        .expect("field-target MAX tie-break plan should build");

    let min_id = load
        .aggregate_min_by(min_plan, "rank")
        .expect("field-target MIN tie-break should succeed");
    let max_id = load
        .aggregate_max_by(max_plan, "rank")
        .expect("field-target MAX tie-break should succeed");

    assert_eq!(
        min_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_061)),
        "field-target MIN tie-break should pick primary key ascending when values tie"
    );
    assert_eq!(
        max_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_063)),
        "field-target MAX tie-break should pick primary key ascending when values tie"
    );
}

#[test]
fn aggregate_field_target_secondary_index_min_uses_index_leading_order() {
    seed_pushdown_entities(&[
        (8_071, 7, 30),
        (8_072, 7, 10),
        (8_073, 7, 20),
        (8_074, 8, 5),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = secondary_group_rank_order_plan(
        ReadConsistency::MissingOk,
        crate::db::query::plan::OrderDirection::Asc,
        0,
    );

    let min_id = load
        .aggregate_min_by(plan, "rank")
        .expect("secondary-index field-target MIN should succeed");

    assert_eq!(
        min_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_072)),
        "secondary-index field-target MIN should return the lowest rank id"
    );
}

#[test]
fn aggregate_field_target_secondary_index_max_tie_breaks_primary_key_ascending() {
    seed_pushdown_entities(&[
        (8_081, 7, 20),
        (8_082, 7, 40),
        (8_083, 7, 40),
        (8_084, 7, 10),
        (8_085, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = secondary_group_rank_order_plan(
        ReadConsistency::MissingOk,
        crate::db::query::plan::OrderDirection::Desc,
        0,
    );

    let max_id = load
        .aggregate_max_by(plan, "rank")
        .expect("secondary-index field-target MAX should succeed");

    assert_eq!(
        max_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_082)),
        "secondary-index field-target MAX should pick primary key ascending within max-value ties"
    );
}

#[test]
fn aggregate_field_target_nth_selects_deterministic_position() {
    seed_pushdown_entities(&[
        (8_142, 7, 10),
        (8_141, 7, 10),
        (8_144, 7, 30),
        (8_143, 7, 20),
        (8_145, 8, 5),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .plan()
            .expect("field-target nth plan should build")
    };

    let nth_0 = load
        .aggregate_nth_by(build_plan(), "rank", 0)
        .expect("nth_by(rank, 0) should succeed");
    let nth_1 = load
        .aggregate_nth_by(build_plan(), "rank", 1)
        .expect("nth_by(rank, 1) should succeed");
    let nth_2 = load
        .aggregate_nth_by(build_plan(), "rank", 2)
        .expect("nth_by(rank, 2) should succeed");
    let nth_3 = load
        .aggregate_nth_by(build_plan(), "rank", 3)
        .expect("nth_by(rank, 3) should succeed");
    let nth_4 = load
        .aggregate_nth_by(build_plan(), "rank", 4)
        .expect("nth_by(rank, 4) should succeed");

    assert_eq!(
        nth_0.map(|id| id.key()),
        Some(Ulid::from_u128(8_141)),
        "nth_by(rank, 0) should select the smallest rank with pk-asc tie-break"
    );
    assert_eq!(
        nth_1.map(|id| id.key()),
        Some(Ulid::from_u128(8_142)),
        "nth_by(rank, 1) should advance through equal-rank ties using pk-asc order"
    );
    assert_eq!(
        nth_2.map(|id| id.key()),
        Some(Ulid::from_u128(8_143)),
        "nth_by(rank, 2) should select the next field-ordered candidate"
    );
    assert_eq!(
        nth_3.map(|id| id.key()),
        Some(Ulid::from_u128(8_144)),
        "nth_by(rank, 3) should select the highest rank in-window candidate"
    );
    assert_eq!(
        nth_4, None,
        "nth_by(rank, 4) should return None when ordinal is outside the result window"
    );
}

#[test]
fn aggregate_field_target_nth_unknown_field_fails_without_scan() {
    seed_pushdown_entities(&[(8_151, 7, 10), (8_152, 7, 20)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .plan()
        .expect("field-target nth unknown-field plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_nth_by(plan, "missing_field", 0)
    });
    let Err(err) = result else {
        panic!("nth_by(missing_field, 0) should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "unknown nth target should fail before scan-budget consumption"
    );
}

#[test]
fn aggregate_field_target_nth_non_orderable_field_fails_without_scan() {
    seed_phase_entities(&[(8_161, 10), (8_162, 20)]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .plan()
        .expect("field-target nth non-orderable plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
        load.aggregate_nth_by(plan, "tags", 0)
    });
    let Err(err) = result else {
        panic!("nth_by(tags, 0) should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "non-orderable nth target should fail before scan-budget consumption"
    );
}

#[test]
fn aggregate_field_target_nth_boundary_matrix_respects_window_and_out_of_range() {
    seed_pushdown_entities(&[
        (8_171, 7, 10),
        (8_172, 7, 10),
        (8_173, 7, 20),
        (8_174, 7, 30),
        (8_175, 7, 40),
        (8_176, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let base_query = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(3)
    };
    let expected_response = load
        .execute(
            base_query()
                .plan()
                .expect("nth boundary baseline plan should build"),
        )
        .expect("nth boundary baseline execute should succeed");
    let expected_len = expected_response.0.len();

    for nth in [0usize, 1, 2, 3, usize::MAX] {
        let actual = load
            .aggregate_nth_by(
                base_query().plan().expect("nth boundary plan should build"),
                "rank",
                nth,
            )
            .expect("nth boundary aggregate should succeed");
        let expected = expected_nth_by_rank_id(&expected_response, nth);

        assert_eq!(actual, expected, "nth boundary parity failed for n={nth}");
    }

    let empty_window_nth_zero = load
        .aggregate_nth_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("id")
                .offset(50)
                .limit(3)
                .plan()
                .expect("empty-window nth plan should build"),
            "rank",
            0,
        )
        .expect("empty-window nth should succeed");

    assert_eq!(
        expected_len, 3,
        "baseline window length should lock nth boundary expectations"
    );
    assert_eq!(
        empty_window_nth_zero, None,
        "empty-window nth_by should return None"
    );
}

#[test]
fn aggregate_field_target_median_even_window_uses_lower_policy() {
    seed_pushdown_entities(&[
        (8_181, 7, 10),
        (8_182, 7, 20),
        (8_183, 7, 30),
        (8_184, 7, 40),
        (8_185, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .limit(4)
            .plan()
            .expect("field-target median plan should build")
    };

    let expected_response = load
        .execute(build_plan())
        .expect("field-target median baseline execute should succeed");
    let median = load
        .aggregate_median_by(build_plan(), "rank")
        .expect("median_by(rank) should succeed");

    assert_eq!(
        median,
        expected_median_by_rank_id(&expected_response),
        "median_by(rank) should match deterministic parity projection"
    );
    assert_eq!(
        median.map(|id| id.key()),
        Some(Ulid::from_u128(8_182)),
        "median_by(rank) should use lower-median policy for even-length windows"
    );
}

#[test]
fn aggregate_field_target_count_distinct_counts_window_values() {
    seed_pushdown_entities(&[
        (8_191, 7, 10),
        (8_192, 7, 10),
        (8_193, 7, 20),
        (8_194, 7, 30),
        (8_195, 7, 30),
        (8_196, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .limit(5)
            .plan()
            .expect("field-target count-distinct plan should build")
    };

    let expected_response = load
        .execute(build_plan())
        .expect("field-target count-distinct baseline execute should succeed");
    let distinct_count = load
        .aggregate_count_distinct_by(build_plan(), "rank")
        .expect("count_distinct_by(rank) should succeed");
    let empty_window_count = load
        .aggregate_count_distinct_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("id")
                .offset(50)
                .limit(5)
                .plan()
                .expect("empty-window count-distinct plan should build"),
            "rank",
        )
        .expect("empty-window count_distinct_by(rank) should succeed");

    assert_eq!(
        distinct_count,
        expected_count_distinct_by_rank(&expected_response),
        "count_distinct_by(rank) should match distinct values in the effective window"
    );
    assert_eq!(
        empty_window_count, 0,
        "count_distinct_by(rank) should return zero for empty windows"
    );
}

#[test]
fn aggregate_field_target_count_distinct_supports_non_orderable_fields() {
    seed_phase_entities(&[(8_197, 10), (8_198, 20), (8_199, 10)]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let distinct_count = load
        .aggregate_count_distinct_by(
            Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("non-orderable count-distinct plan should build"),
            "tags",
        )
        .expect("count_distinct_by(tags) should succeed");

    assert_eq!(
        distinct_count, 2,
        "count_distinct_by(tags) should support structured field equality"
    );
}

#[test]
fn aggregate_field_target_count_distinct_list_order_semantics_are_stable() {
    seed_phase_entities_custom(vec![
        PhaseEntity {
            id: Ulid::from_u128(819_701),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1, 2],
            label: "a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(819_702),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2, 1],
            label: "b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(819_703),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![1, 2],
            label: "c".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(819_704),
            opt_rank: Some(40),
            rank: 40,
            tags: vec![1, 2, 3],
            label: "d".to_string(),
        },
    ]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let distinct_count = load
        .aggregate_count_distinct_by(
            Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("list-order count-distinct plan should build"),
            "tags",
        )
        .expect("count_distinct_by(tags) should succeed");

    assert_eq!(
        distinct_count, 3,
        "count_distinct_by(tags) should preserve list-order equality semantics"
    );
}

#[test]
fn aggregate_field_target_count_distinct_residual_retry_parity_and_scan_budget_match_execute() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();
    let save = SaveExecutor::<IndexedMetricsEntity>::new(DB, false);
    for (id, tag, label) in [
        (8_3101u128, 10u32, "drop-t10"),
        (8_3102, 11, "drop-t11"),
        (8_3103, 12, "drop-t12"),
        (8_3104, 13, "keep-t13"),
        (8_3105, 14, "keep-t14"),
        (8_3106, 15, "keep-t15"),
    ] {
        save.insert(IndexedMetricsEntity {
            id: Ulid::from_u128(id),
            tag,
            label: label.to_string(),
        })
        .expect("indexed metrics seed row save should succeed");
    }
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let build_plan = || {
        let mut logical = AccessPlannedQuery::new(
            AccessPath::index_range(
                INDEXED_METRICS_INDEX_MODELS[0],
                Vec::new(),
                Bound::Included(Value::Uint(10)),
                Bound::Excluded(Value::Uint(16)),
            ),
            ReadConsistency::MissingOk,
        );
        logical.predicate = Some(Predicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        });
        logical.order = Some(OrderSpec {
            fields: vec![
                ("tag".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        logical.page = Some(PageSpec {
            limit: Some(2),
            offset: 0,
        });

        ExecutablePlan::<IndexedMetricsEntity>::new(logical)
    };

    let (distinct_count, scanned_count_distinct) =
        capture_rows_scanned_for_entity(IndexedMetricsEntity::PATH, || {
            load.aggregate_count_distinct_by(build_plan(), "tag")
                .expect("residual-retry count_distinct_by(tag) should succeed")
        });
    let (response, scanned_execute) =
        capture_rows_scanned_for_entity(IndexedMetricsEntity::PATH, || {
            load.execute(build_plan())
                .expect("residual-retry execute baseline should succeed")
        });
    let expected_count = {
        let mut seen_values: Vec<Value> = Vec::new();
        let mut count = 0u32;
        for (_, entity) in &response.0 {
            let value = Value::Uint(u64::from(entity.tag));
            if seen_values.iter().any(|existing| existing == &value) {
                continue;
            }
            seen_values.push(value);
            count = count.saturating_add(1);
        }
        count
    };

    assert_eq!(
        distinct_count, expected_count,
        "count_distinct_by(tag) should preserve canonical fallback parity for residual-retry index-range shapes"
    );
    assert_eq!(
        scanned_count_distinct, scanned_execute,
        "count_distinct_by(tag) should preserve scan-budget parity with execute() on residual-retry index-range shapes"
    );
}

#[test]
fn aggregate_field_target_count_distinct_is_direction_invariant() {
    seed_pushdown_entities(&[
        (8_3201, 7, 10),
        (8_3202, 7, 20),
        (8_3203, 7, 20),
        (8_3204, 7, 30),
        (8_3205, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let asc_count = load
        .aggregate_count_distinct_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(u32_eq_predicate("group", 7))
                .order_by("rank")
                .plan()
                .expect("direction-invariant ASC plan should build"),
            "rank",
        )
        .expect("direction-invariant ASC count_distinct_by(rank) should succeed");
    let desc_count = load
        .aggregate_count_distinct_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("rank")
                .order_by_desc("id")
                .plan()
                .expect("direction-invariant DESC plan should build"),
            "rank",
        )
        .expect("direction-invariant DESC count_distinct_by(rank) should succeed");

    assert_eq!(
        asc_count, desc_count,
        "count_distinct_by(rank) should be invariant to traversal direction over the same effective window"
    );
}

#[test]
fn aggregate_field_target_count_distinct_optional_field_null_values_are_rejected_consistently() {
    seed_phase_entities_custom(vec![
        PhaseEntity {
            id: Ulid::from_u128(8_3301),
            opt_rank: None,
            rank: 1,
            tags: vec![1],
            label: "phase-1".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3302),
            opt_rank: Some(10),
            rank: 2,
            tags: vec![2],
            label: "phase-2".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3303),
            opt_rank: Some(10),
            rank: 3,
            tags: vec![3],
            label: "phase-3".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3304),
            opt_rank: None,
            rank: 4,
            tags: vec![4],
            label: "phase-4".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3305),
            opt_rank: Some(20),
            rank: 5,
            tags: vec![5],
            label: "phase-5".to_string(),
        },
    ]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let build_plan_asc = || {
        Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
            .order_by("rank")
            .plan()
            .expect("optional-field null-semantics ASC plan should build")
    };
    let build_plan_desc = || {
        Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
            .order_by_desc("rank")
            .order_by_desc("id")
            .plan()
            .expect("optional-field null-semantics DESC plan should build")
    };
    let asc_err = load
        .aggregate_count_distinct_by(build_plan_asc(), "opt_rank")
        .expect_err("count_distinct_by(opt_rank) ASC should reject null field values");
    let desc_err = load
        .aggregate_count_distinct_by(build_plan_desc(), "opt_rank")
        .expect_err("count_distinct_by(opt_rank) DESC should reject null field values");

    assert_eq!(
        asc_err.class,
        ErrorClass::InvariantViolation,
        "count_distinct_by(opt_rank) should classify null-value mismatch as invariant violation"
    );
    assert_eq!(
        desc_err.class,
        ErrorClass::InvariantViolation,
        "descending count_distinct_by(opt_rank) should classify null-value mismatch as invariant violation"
    );
    assert!(
        asc_err
            .message
            .contains("aggregate target field value type mismatch"),
        "count_distinct_by(opt_rank) should expose type-mismatch reason for null values"
    );
    assert!(
        desc_err
            .message
            .contains("aggregate target field value type mismatch"),
        "descending count_distinct_by(opt_rank) should expose type-mismatch reason for null values"
    );
    assert!(
        asc_err.message.contains("value=Null") && desc_err.message.contains("value=Null"),
        "count_distinct_by(opt_rank) should report null payload mismatch consistently across directions"
    );
}

#[test]
fn aggregate_field_target_top_k_by_optional_field_null_values_match_projection_errors() {
    seed_phase_entities_custom(vec![
        PhaseEntity {
            id: Ulid::from_u128(8_3301),
            opt_rank: None,
            rank: 1,
            tags: vec![1],
            label: "phase-1".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3302),
            opt_rank: Some(10),
            rank: 2,
            tags: vec![2],
            label: "phase-2".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3303),
            opt_rank: Some(20),
            rank: 3,
            tags: vec![3],
            label: "phase-3".to_string(),
        },
    ]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let build_plan = || {
        Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
            .order_by("rank")
            .plan()
            .expect("optional-field projection/top-k null-semantics plan should build")
    };
    let values_err = load
        .values_by(build_plan(), "opt_rank")
        .expect_err("values_by(opt_rank) should reject null field values");
    let top_k_err = load
        .top_k_by(build_plan(), "opt_rank", 2)
        .expect_err("top_k_by(opt_rank, 2) should reject null field values");

    assert_eq!(
        values_err.class,
        ErrorClass::InvariantViolation,
        "values_by(opt_rank) should classify null-value mismatch as invariant violation"
    );
    assert_eq!(
        top_k_err.class,
        ErrorClass::InvariantViolation,
        "top_k_by(opt_rank, 2) should classify null-value mismatch as invariant violation"
    );
    assert!(
        values_err
            .message
            .contains("aggregate target field value type mismatch"),
        "values_by(opt_rank) should expose type-mismatch reason for null values"
    );
    assert!(
        top_k_err
            .message
            .contains("aggregate target field value type mismatch"),
        "top_k_by(opt_rank, 2) should expose type-mismatch reason for null values"
    );
    assert!(
        values_err.message.contains("value=Null") && top_k_err.message.contains("value=Null"),
        "top_k_by(opt_rank, 2) should report null payload mismatch consistently with values_by(opt_rank)"
    );
}

#[test]
fn aggregate_field_target_bottom_k_by_optional_field_null_values_match_projection_errors() {
    seed_phase_entities_custom(vec![
        PhaseEntity {
            id: Ulid::from_u128(8_3301),
            opt_rank: None,
            rank: 1,
            tags: vec![1],
            label: "phase-1".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3302),
            opt_rank: Some(10),
            rank: 2,
            tags: vec![2],
            label: "phase-2".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3303),
            opt_rank: Some(20),
            rank: 3,
            tags: vec![3],
            label: "phase-3".to_string(),
        },
    ]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let build_plan = || {
        Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
            .order_by("rank")
            .plan()
            .expect("optional-field projection/bottom-k null-semantics plan should build")
    };
    let values_err = load
        .values_by(build_plan(), "opt_rank")
        .expect_err("values_by(opt_rank) should reject null field values");
    let bottom_k_err = load
        .bottom_k_by(build_plan(), "opt_rank", 2)
        .expect_err("bottom_k_by(opt_rank, 2) should reject null field values");

    assert_eq!(
        values_err.class,
        ErrorClass::InvariantViolation,
        "values_by(opt_rank) should classify null-value mismatch as invariant violation"
    );
    assert_eq!(
        bottom_k_err.class,
        ErrorClass::InvariantViolation,
        "bottom_k_by(opt_rank, 2) should classify null-value mismatch as invariant violation"
    );
    assert!(
        values_err
            .message
            .contains("aggregate target field value type mismatch"),
        "values_by(opt_rank) should expose type-mismatch reason for null values"
    );
    assert!(
        bottom_k_err
            .message
            .contains("aggregate target field value type mismatch"),
        "bottom_k_by(opt_rank, 2) should expose type-mismatch reason for null values"
    );
    assert!(
        values_err.message.contains("value=Null") && bottom_k_err.message.contains("value=Null"),
        "bottom_k_by(opt_rank, 2) should report null payload mismatch consistently with values_by(opt_rank)"
    );
}

#[test]
fn aggregate_field_target_top_k_by_values_optional_field_null_values_match_projection_errors() {
    seed_phase_entities_custom(vec![
        PhaseEntity {
            id: Ulid::from_u128(8_3301),
            opt_rank: None,
            rank: 1,
            tags: vec![1],
            label: "phase-1".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3302),
            opt_rank: Some(10),
            rank: 2,
            tags: vec![2],
            label: "phase-2".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3303),
            opt_rank: Some(20),
            rank: 3,
            tags: vec![3],
            label: "phase-3".to_string(),
        },
    ]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let build_plan = || {
        Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
            .order_by("rank")
            .plan()
            .expect("optional-field projection/top-k-values null-semantics plan should build")
    };
    let values_err = load
        .values_by(build_plan(), "opt_rank")
        .expect_err("values_by(opt_rank) should reject null field values");
    let top_k_values_err = load
        .top_k_by_values(build_plan(), "opt_rank", 2)
        .expect_err("top_k_by_values(opt_rank, 2) should reject null field values");

    assert_eq!(
        values_err.class,
        ErrorClass::InvariantViolation,
        "values_by(opt_rank) should classify null-value mismatch as invariant violation"
    );
    assert_eq!(
        top_k_values_err.class,
        ErrorClass::InvariantViolation,
        "top_k_by_values(opt_rank, 2) should classify null-value mismatch as invariant violation"
    );
    assert!(
        values_err
            .message
            .contains("aggregate target field value type mismatch"),
        "values_by(opt_rank) should expose type-mismatch reason for null values"
    );
    assert!(
        top_k_values_err
            .message
            .contains("aggregate target field value type mismatch"),
        "top_k_by_values(opt_rank, 2) should expose type-mismatch reason for null values"
    );
    assert!(
        values_err.message.contains("value=Null")
            && top_k_values_err.message.contains("value=Null"),
        "top_k_by_values(opt_rank, 2) should report null payload mismatch consistently with values_by(opt_rank)"
    );
}

#[test]
fn aggregate_field_target_bottom_k_by_values_optional_field_null_values_match_projection_errors() {
    seed_phase_entities_custom(vec![
        PhaseEntity {
            id: Ulid::from_u128(8_3301),
            opt_rank: None,
            rank: 1,
            tags: vec![1],
            label: "phase-1".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3302),
            opt_rank: Some(10),
            rank: 2,
            tags: vec![2],
            label: "phase-2".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3303),
            opt_rank: Some(20),
            rank: 3,
            tags: vec![3],
            label: "phase-3".to_string(),
        },
    ]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let build_plan = || {
        Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
            .order_by("rank")
            .plan()
            .expect("optional-field projection/bottom-k-values null-semantics plan should build")
    };
    let values_err = load
        .values_by(build_plan(), "opt_rank")
        .expect_err("values_by(opt_rank) should reject null field values");
    let bottom_k_values_err = load
        .bottom_k_by_values(build_plan(), "opt_rank", 2)
        .expect_err("bottom_k_by_values(opt_rank, 2) should reject null field values");

    assert_eq!(
        values_err.class,
        ErrorClass::InvariantViolation,
        "values_by(opt_rank) should classify null-value mismatch as invariant violation"
    );
    assert_eq!(
        bottom_k_values_err.class,
        ErrorClass::InvariantViolation,
        "bottom_k_by_values(opt_rank, 2) should classify null-value mismatch as invariant violation"
    );
    assert!(
        values_err
            .message
            .contains("aggregate target field value type mismatch"),
        "values_by(opt_rank) should expose type-mismatch reason for null values"
    );
    assert!(
        bottom_k_values_err
            .message
            .contains("aggregate target field value type mismatch"),
        "bottom_k_by_values(opt_rank, 2) should expose type-mismatch reason for null values"
    );
    assert!(
        values_err.message.contains("value=Null")
            && bottom_k_values_err.message.contains("value=Null"),
        "bottom_k_by_values(opt_rank, 2) should report null payload mismatch consistently with values_by(opt_rank)"
    );
}

#[test]
fn aggregate_field_target_top_k_by_with_ids_optional_field_null_values_match_projection_errors() {
    seed_phase_entities_custom(vec![
        PhaseEntity {
            id: Ulid::from_u128(8_3301),
            opt_rank: None,
            rank: 1,
            tags: vec![1],
            label: "phase-1".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3302),
            opt_rank: Some(10),
            rank: 2,
            tags: vec![2],
            label: "phase-2".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3303),
            opt_rank: Some(20),
            rank: 3,
            tags: vec![3],
            label: "phase-3".to_string(),
        },
    ]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let build_plan = || {
        Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
            .order_by("rank")
            .plan()
            .expect("optional-field projection/top-k-with-ids null-semantics plan should build")
    };
    let values_with_ids_err = load
        .values_by_with_ids(build_plan(), "opt_rank")
        .expect_err("values_by_with_ids(opt_rank) should reject null field values");
    let top_k_with_ids_err = load
        .top_k_by_with_ids(build_plan(), "opt_rank", 2)
        .expect_err("top_k_by_with_ids(opt_rank, 2) should reject null field values");

    assert_eq!(
        values_with_ids_err.class,
        ErrorClass::InvariantViolation,
        "values_by_with_ids(opt_rank) should classify null-value mismatch as invariant violation"
    );
    assert_eq!(
        top_k_with_ids_err.class,
        ErrorClass::InvariantViolation,
        "top_k_by_with_ids(opt_rank, 2) should classify null-value mismatch as invariant violation"
    );
    assert!(
        values_with_ids_err
            .message
            .contains("aggregate target field value type mismatch"),
        "values_by_with_ids(opt_rank) should expose type-mismatch reason for null values"
    );
    assert!(
        top_k_with_ids_err
            .message
            .contains("aggregate target field value type mismatch"),
        "top_k_by_with_ids(opt_rank, 2) should expose type-mismatch reason for null values"
    );
    assert!(
        values_with_ids_err.message.contains("value=Null")
            && top_k_with_ids_err.message.contains("value=Null"),
        "top_k_by_with_ids(opt_rank, 2) should report null payload mismatch consistently with values_by_with_ids(opt_rank)"
    );
}

#[test]
fn aggregate_field_target_bottom_k_by_with_ids_optional_field_null_values_match_projection_errors()
{
    seed_phase_entities_custom(vec![
        PhaseEntity {
            id: Ulid::from_u128(8_3301),
            opt_rank: None,
            rank: 1,
            tags: vec![1],
            label: "phase-1".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3302),
            opt_rank: Some(10),
            rank: 2,
            tags: vec![2],
            label: "phase-2".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3303),
            opt_rank: Some(20),
            rank: 3,
            tags: vec![3],
            label: "phase-3".to_string(),
        },
    ]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let build_plan = || {
        Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
            .order_by("rank")
            .plan()
            .expect("optional-field projection/bottom-k-with-ids null-semantics plan should build")
    };
    let values_with_ids_err = load
        .values_by_with_ids(build_plan(), "opt_rank")
        .expect_err("values_by_with_ids(opt_rank) should reject null field values");
    let bottom_k_with_ids_err = load
        .bottom_k_by_with_ids(build_plan(), "opt_rank", 2)
        .expect_err("bottom_k_by_with_ids(opt_rank, 2) should reject null field values");

    assert_eq!(
        values_with_ids_err.class,
        ErrorClass::InvariantViolation,
        "values_by_with_ids(opt_rank) should classify null-value mismatch as invariant violation"
    );
    assert_eq!(
        bottom_k_with_ids_err.class,
        ErrorClass::InvariantViolation,
        "bottom_k_by_with_ids(opt_rank, 2) should classify null-value mismatch as invariant violation"
    );
    assert!(
        values_with_ids_err
            .message
            .contains("aggregate target field value type mismatch"),
        "values_by_with_ids(opt_rank) should expose type-mismatch reason for null values"
    );
    assert!(
        bottom_k_with_ids_err
            .message
            .contains("aggregate target field value type mismatch"),
        "bottom_k_by_with_ids(opt_rank, 2) should expose type-mismatch reason for null values"
    );
    assert!(
        values_with_ids_err.message.contains("value=Null")
            && bottom_k_with_ids_err.message.contains("value=Null"),
        "bottom_k_by_with_ids(opt_rank, 2) should report null payload mismatch consistently with values_by_with_ids(opt_rank)"
    );
}

#[test]
fn aggregate_field_target_top_k_by_missing_field_parity_matches_values_by() {
    seed_pushdown_entities(&[(8_3381, 7, 10), (8_3382, 7, 20), (8_3383, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .plan()
            .expect("missing-field parity plan should build")
    };
    let values_err = load
        .values_by(build_plan(), "missing_field")
        .expect_err("values_by(missing_field) should be rejected");
    let top_k_err = load
        .top_k_by(build_plan(), "missing_field", 2)
        .expect_err("top_k_by(missing_field, 2) should be rejected");

    assert_eq!(
        top_k_err.class, values_err.class,
        "top_k_by(missing_field, 2) should classify unknown-field failures the same way as values_by(missing_field)"
    );
    assert_eq!(
        top_k_err.origin, values_err.origin,
        "top_k_by(missing_field, 2) should preserve unknown-field origin parity with values_by(missing_field)"
    );
    assert!(
        top_k_err.message.contains("unknown aggregate target field"),
        "top_k_by(missing_field, 2) should surface the same unknown-field reason"
    );
}

#[test]
fn aggregate_field_target_top_k_by_values_missing_field_parity_matches_values_by() {
    seed_pushdown_entities(&[(8_3381, 7, 10), (8_3382, 7, 20), (8_3383, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .plan()
            .expect("missing-field parity plan should build")
    };
    let values_err = load
        .values_by(build_plan(), "missing_field")
        .expect_err("values_by(missing_field) should be rejected");
    let top_k_values_err = load
        .top_k_by_values(build_plan(), "missing_field", 2)
        .expect_err("top_k_by_values(missing_field, 2) should be rejected");

    assert_eq!(
        top_k_values_err.class, values_err.class,
        "top_k_by_values(missing_field, 2) should classify unknown-field failures the same way as values_by(missing_field)"
    );
    assert_eq!(
        top_k_values_err.origin, values_err.origin,
        "top_k_by_values(missing_field, 2) should preserve unknown-field origin parity with values_by(missing_field)"
    );
    assert!(
        top_k_values_err
            .message
            .contains("unknown aggregate target field"),
        "top_k_by_values(missing_field, 2) should surface the same unknown-field reason"
    );
}

#[test]
fn aggregate_field_target_bottom_k_by_missing_field_parity_matches_values_by() {
    seed_pushdown_entities(&[(8_3381, 7, 10), (8_3382, 7, 20), (8_3383, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .plan()
            .expect("missing-field parity plan should build")
    };
    let values_err = load
        .values_by(build_plan(), "missing_field")
        .expect_err("values_by(missing_field) should be rejected");
    let bottom_k_err = load
        .bottom_k_by(build_plan(), "missing_field", 2)
        .expect_err("bottom_k_by(missing_field, 2) should be rejected");

    assert_eq!(
        bottom_k_err.class, values_err.class,
        "bottom_k_by(missing_field, 2) should classify unknown-field failures the same way as values_by(missing_field)"
    );
    assert_eq!(
        bottom_k_err.origin, values_err.origin,
        "bottom_k_by(missing_field, 2) should preserve unknown-field origin parity with values_by(missing_field)"
    );
    assert!(
        bottom_k_err
            .message
            .contains("unknown aggregate target field"),
        "bottom_k_by(missing_field, 2) should surface the same unknown-field reason"
    );
}

#[test]
fn aggregate_field_target_bottom_k_by_values_missing_field_parity_matches_values_by() {
    seed_pushdown_entities(&[(8_3381, 7, 10), (8_3382, 7, 20), (8_3383, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .plan()
            .expect("missing-field parity plan should build")
    };
    let values_err = load
        .values_by(build_plan(), "missing_field")
        .expect_err("values_by(missing_field) should be rejected");
    let bottom_k_values_err = load
        .bottom_k_by_values(build_plan(), "missing_field", 2)
        .expect_err("bottom_k_by_values(missing_field, 2) should be rejected");

    assert_eq!(
        bottom_k_values_err.class, values_err.class,
        "bottom_k_by_values(missing_field, 2) should classify unknown-field failures the same way as values_by(missing_field)"
    );
    assert_eq!(
        bottom_k_values_err.origin, values_err.origin,
        "bottom_k_by_values(missing_field, 2) should preserve unknown-field origin parity with values_by(missing_field)"
    );
    assert!(
        bottom_k_values_err
            .message
            .contains("unknown aggregate target field"),
        "bottom_k_by_values(missing_field, 2) should surface the same unknown-field reason"
    );
}

#[test]
fn aggregate_field_target_top_k_by_with_ids_missing_field_parity_matches_values_by_with_ids() {
    seed_pushdown_entities(&[(8_3381, 7, 10), (8_3382, 7, 20), (8_3383, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .plan()
            .expect("missing-field parity plan should build")
    };
    let values_with_ids_err = load
        .values_by_with_ids(build_plan(), "missing_field")
        .expect_err("values_by_with_ids(missing_field) should be rejected");
    let top_k_with_ids_err = load
        .top_k_by_with_ids(build_plan(), "missing_field", 2)
        .expect_err("top_k_by_with_ids(missing_field, 2) should be rejected");

    assert_eq!(
        top_k_with_ids_err.class, values_with_ids_err.class,
        "top_k_by_with_ids(missing_field, 2) should classify unknown-field failures the same way as values_by_with_ids(missing_field)"
    );
    assert_eq!(
        top_k_with_ids_err.origin, values_with_ids_err.origin,
        "top_k_by_with_ids(missing_field, 2) should preserve unknown-field origin parity with values_by_with_ids(missing_field)"
    );
    assert!(
        top_k_with_ids_err
            .message
            .contains("unknown aggregate target field"),
        "top_k_by_with_ids(missing_field, 2) should surface the same unknown-field reason"
    );
}

#[test]
fn aggregate_field_target_bottom_k_by_with_ids_missing_field_parity_matches_values_by_with_ids() {
    seed_pushdown_entities(&[(8_3381, 7, 10), (8_3382, 7, 20), (8_3383, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .plan()
            .expect("missing-field parity plan should build")
    };
    let values_with_ids_err = load
        .values_by_with_ids(build_plan(), "missing_field")
        .expect_err("values_by_with_ids(missing_field) should be rejected");
    let bottom_k_with_ids_err = load
        .bottom_k_by_with_ids(build_plan(), "missing_field", 2)
        .expect_err("bottom_k_by_with_ids(missing_field, 2) should be rejected");

    assert_eq!(
        bottom_k_with_ids_err.class, values_with_ids_err.class,
        "bottom_k_by_with_ids(missing_field, 2) should classify unknown-field failures the same way as values_by_with_ids(missing_field)"
    );
    assert_eq!(
        bottom_k_with_ids_err.origin, values_with_ids_err.origin,
        "bottom_k_by_with_ids(missing_field, 2) should preserve unknown-field origin parity with values_by_with_ids(missing_field)"
    );
    assert!(
        bottom_k_with_ids_err
            .message
            .contains("unknown aggregate target field"),
        "bottom_k_by_with_ids(missing_field, 2) should surface the same unknown-field reason"
    );
}

#[test]
fn aggregate_field_target_count_distinct_distinct_modifier_tracks_effective_window_rows() {
    seed_pushdown_entities(&[
        (8_1971, 7, 10),
        (8_1972, 7, 20),
        (8_1973, 7, 30),
        (8_1974, 7, 40),
        (8_1975, 8, 50),
        (8_1976, 8, 60),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let overlapping_predicate = Predicate::Or(vec![
        id_in_predicate(&[8_1971, 8_1972, 8_1973, 8_1974]),
        id_in_predicate(&[8_1972, 8_1973, 8_1975, 8_1976]),
    ]);
    let build_query = |distinct: bool| {
        let mut query = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(overlapping_predicate.clone());
        if distinct {
            query = query.distinct();
        }

        query.order_by_desc("id").offset(1).limit(4)
    };

    let non_distinct_response = load
        .execute(
            build_query(false)
                .plan()
                .expect("non-distinct count-distinct baseline plan should build"),
        )
        .expect("non-distinct count-distinct baseline execute should succeed");
    let distinct_response = load
        .execute(
            build_query(true)
                .plan()
                .expect("distinct count-distinct baseline plan should build"),
        )
        .expect("distinct count-distinct baseline execute should succeed");

    let non_distinct_count = load
        .aggregate_count_distinct_by(
            build_query(false)
                .plan()
                .expect("non-distinct count-distinct plan should build"),
            "rank",
        )
        .expect("non-distinct count_distinct_by(rank) should succeed");
    let distinct_count = load
        .aggregate_count_distinct_by(
            build_query(true)
                .plan()
                .expect("distinct count-distinct plan should build"),
            "rank",
        )
        .expect("distinct count_distinct_by(rank) should succeed");

    assert_eq!(
        non_distinct_count,
        expected_count_distinct_by_rank(&non_distinct_response),
        "non-distinct count_distinct_by(rank) should match effective-window field distinct count"
    );
    assert_eq!(
        distinct_count,
        expected_count_distinct_by_rank(&distinct_response),
        "distinct count_distinct_by(rank) should match effective-window field distinct count"
    );
}

#[test]
fn aggregate_field_target_values_by_distinct_remains_row_level() {
    seed_pushdown_entities(&[
        (8_1971, 7, 10),
        (8_1972, 7, 10),
        (8_1973, 7, 20),
        (8_1974, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let values = load
        .values_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(u32_eq_predicate("group", 7))
                .distinct()
                .order_by("id")
                .plan()
                .expect("values_by distinct plan should build"),
            "rank",
        )
        .expect("values_by(rank) should succeed");

    assert_eq!(
        values,
        vec![Value::Uint(10), Value::Uint(10), Value::Uint(20)],
        "query-level DISTINCT must remain row-level; equal projected values may repeat"
    );
}

#[test]
fn aggregate_field_target_distinct_values_by_matches_effective_window_projection() {
    seed_pushdown_entities(&[
        (8_1971, 7, 10),
        (8_1972, 7, 10),
        (8_1973, 7, 20),
        (8_1974, 7, 30),
        (8_1975, 7, 20),
        (8_1976, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
            .plan()
            .expect("distinct_values_by plan should build")
    };

    let expected = load
        .execute(build_plan())
        .expect("baseline execute for distinct_values_by should succeed");
    let actual = load
        .distinct_values_by(build_plan(), "rank")
        .expect("distinct_values_by(rank) should succeed");

    assert_eq!(
        actual,
        expected_distinct_values_by_rank(&expected),
        "distinct_values_by(rank) should match effective-window first-observed distinct projection"
    );
}

#[test]
fn aggregate_field_target_distinct_values_by_matches_values_by_first_observed_dedup() {
    seed_pushdown_entities(&[
        (8_1971, 7, 10),
        (8_1972, 7, 10),
        (8_1973, 7, 20),
        (8_1974, 7, 30),
        (8_1975, 7, 20),
        (8_1976, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
            .plan()
            .expect("distinct-values invariant plan should build")
    };

    let values = load
        .values_by(build_plan(), "rank")
        .expect("values_by(rank) should succeed");
    let distinct_values = load
        .distinct_values_by(build_plan(), "rank")
        .expect("distinct_values_by(rank) should succeed");

    let mut expected_distinct_from_values = Vec::new();
    for value in &values {
        if expected_distinct_from_values
            .iter()
            .any(|existing| existing == value)
        {
            continue;
        }
        expected_distinct_from_values.push(value.clone());
    }

    assert!(
        values.len() >= distinct_values.len(),
        "values_by(field).len() must be >= distinct_values_by(field).len()"
    );
    assert_eq!(
        distinct_values, expected_distinct_from_values,
        "distinct_values_by(field) must equal values_by(field) deduped by first occurrence"
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn aggregate_field_target_new_terminals_unknown_field_fail_without_scan() {
    seed_pushdown_entities(&[(8_1981, 7, 10), (8_1982, 7, 20), (8_1983, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .plan()
            .expect("unknown-field terminal plan should build")
    };

    let (median_result, median_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_median_by(build_plan(), "missing_field")
        });
    let Err(median_err) = median_result else {
        panic!("median_by(missing_field) should be rejected");
    };
    assert_eq!(median_err.class, ErrorClass::Unsupported);
    assert_eq!(median_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        median_scanned, 0,
        "median_by unknown-field target should fail before scan-budget consumption"
    );

    let (count_result, count_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_count_distinct_by(build_plan(), "missing_field")
        });
    let Err(count_err) = count_result else {
        panic!("count_distinct_by(missing_field) should be rejected");
    };
    assert_eq!(count_err.class, ErrorClass::Unsupported);
    assert_eq!(count_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        count_scanned, 0,
        "count_distinct_by unknown-field target should fail before scan-budget consumption"
    );

    let (min_max_result, min_max_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min_max_by(build_plan(), "missing_field")
        });
    let Err(min_max_err) = min_max_result else {
        panic!("min_max_by(missing_field) should be rejected");
    };
    assert_eq!(min_max_err.class, ErrorClass::Unsupported);
    assert_eq!(min_max_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        min_max_scanned, 0,
        "min_max_by unknown-field target should fail before scan-budget consumption"
    );

    let (values_result, values_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.values_by(build_plan(), "missing_field")
        });
    let Err(values_err) = values_result else {
        panic!("values_by(missing_field) should be rejected");
    };
    assert_eq!(values_err.class, ErrorClass::Unsupported);
    assert_eq!(values_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        values_scanned, 0,
        "values_by unknown-field target should fail before scan-budget consumption"
    );

    let (distinct_values_result, distinct_values_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.distinct_values_by(build_plan(), "missing_field")
        });
    let Err(distinct_values_err) = distinct_values_result else {
        panic!("distinct_values_by(missing_field) should be rejected");
    };
    assert_eq!(distinct_values_err.class, ErrorClass::Unsupported);
    assert_eq!(distinct_values_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        distinct_values_scanned, 0,
        "distinct_values_by unknown-field target should fail before scan-budget consumption"
    );

    let (values_with_ids_result, values_with_ids_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.values_by_with_ids(build_plan(), "missing_field")
        });
    let Err(values_with_ids_err) = values_with_ids_result else {
        panic!("values_by_with_ids(missing_field) should be rejected");
    };
    assert_eq!(values_with_ids_err.class, ErrorClass::Unsupported);
    assert_eq!(values_with_ids_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        values_with_ids_scanned, 0,
        "values_by_with_ids unknown-field target should fail before scan-budget consumption"
    );

    let (first_value_result, first_value_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.first_value_by(build_plan(), "missing_field")
        });
    let Err(first_value_err) = first_value_result else {
        panic!("first_value_by(missing_field) should be rejected");
    };
    assert_eq!(first_value_err.class, ErrorClass::Unsupported);
    assert_eq!(first_value_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        first_value_scanned, 0,
        "first_value_by unknown-field target should fail before scan-budget consumption"
    );

    let (last_value_result, last_value_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.last_value_by(build_plan(), "missing_field")
        });
    let Err(last_value_err) = last_value_result else {
        panic!("last_value_by(missing_field) should be rejected");
    };
    assert_eq!(last_value_err.class, ErrorClass::Unsupported);
    assert_eq!(last_value_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        last_value_scanned, 0,
        "last_value_by unknown-field target should fail before scan-budget consumption"
    );

    let (top_k_result, top_k_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by(build_plan(), "missing_field", 2)
        });
    let Err(top_k_err) = top_k_result else {
        panic!("top_k_by(missing_field, k) should be rejected");
    };
    assert_eq!(top_k_err.class, ErrorClass::Unsupported);
    assert_eq!(top_k_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        top_k_scanned, 0,
        "top_k_by unknown-field target should fail before scan-budget consumption"
    );

    let (bottom_k_result, bottom_k_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by(build_plan(), "missing_field", 2)
        });
    let Err(bottom_k_err) = bottom_k_result else {
        panic!("bottom_k_by(missing_field, k) should be rejected");
    };
    assert_eq!(bottom_k_err.class, ErrorClass::Unsupported);
    assert_eq!(bottom_k_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        bottom_k_scanned, 0,
        "bottom_k_by unknown-field target should fail before scan-budget consumption"
    );

    let (top_k_values_result, top_k_values_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_values(build_plan(), "missing_field", 2)
        });
    let Err(top_k_values_err) = top_k_values_result else {
        panic!("top_k_by_values(missing_field, k) should be rejected");
    };
    assert_eq!(top_k_values_err.class, ErrorClass::Unsupported);
    assert_eq!(top_k_values_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        top_k_values_scanned, 0,
        "top_k_by_values unknown-field target should fail before scan-budget consumption"
    );

    let (bottom_k_values_result, bottom_k_values_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_values(build_plan(), "missing_field", 2)
        });
    let Err(bottom_k_values_err) = bottom_k_values_result else {
        panic!("bottom_k_by_values(missing_field, k) should be rejected");
    };
    assert_eq!(bottom_k_values_err.class, ErrorClass::Unsupported);
    assert_eq!(bottom_k_values_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        bottom_k_values_scanned, 0,
        "bottom_k_by_values unknown-field target should fail before scan-budget consumption"
    );

    let (top_k_with_ids_result, top_k_with_ids_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_with_ids(build_plan(), "missing_field", 2)
        });
    let Err(top_k_with_ids_err) = top_k_with_ids_result else {
        panic!("top_k_by_with_ids(missing_field, k) should be rejected");
    };
    assert_eq!(top_k_with_ids_err.class, ErrorClass::Unsupported);
    assert_eq!(top_k_with_ids_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        top_k_with_ids_scanned, 0,
        "top_k_by_with_ids unknown-field target should fail before scan-budget consumption"
    );

    let (bottom_k_with_ids_result, bottom_k_with_ids_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_with_ids(build_plan(), "missing_field", 2)
        });
    let Err(bottom_k_with_ids_err) = bottom_k_with_ids_result else {
        panic!("bottom_k_by_with_ids(missing_field, k) should be rejected");
    };
    assert_eq!(bottom_k_with_ids_err.class, ErrorClass::Unsupported);
    assert_eq!(bottom_k_with_ids_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        bottom_k_with_ids_scanned, 0,
        "bottom_k_by_with_ids unknown-field target should fail before scan-budget consumption"
    );
}

#[test]
fn aggregate_field_target_top_and_bottom_k_by_non_orderable_field_fail_without_scan() {
    seed_phase_entities(&[(8_1991, 10), (8_1992, 20), (8_1993, 30)]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let build_plan = || {
        Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .plan()
            .expect("top/bottom non-orderable target plan should build")
    };

    let (top_k_result, top_k_scanned) = capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
        load.top_k_by(build_plan(), "tags", 2)
    });
    let Err(top_k_err) = top_k_result else {
        panic!("top_k_by(tags, 2) should be rejected");
    };
    assert_eq!(top_k_err.class, ErrorClass::Unsupported);
    assert_eq!(top_k_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        top_k_scanned, 0,
        "top_k_by non-orderable field target should fail before scan-budget consumption"
    );
    assert!(
        top_k_err.message.contains("does not support ordering"),
        "top_k_by(tags, 2) should preserve non-orderable field taxonomy: {top_k_err:?}"
    );

    let (bottom_k_result, bottom_k_scanned) =
        capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
            load.bottom_k_by(build_plan(), "tags", 2)
        });
    let Err(bottom_k_err) = bottom_k_result else {
        panic!("bottom_k_by(tags, 2) should be rejected");
    };
    assert_eq!(bottom_k_err.class, ErrorClass::Unsupported);
    assert_eq!(bottom_k_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        bottom_k_scanned, 0,
        "bottom_k_by non-orderable field target should fail before scan-budget consumption"
    );
    assert!(
        bottom_k_err.message.contains("does not support ordering"),
        "bottom_k_by(tags, 2) should preserve non-orderable field taxonomy: {bottom_k_err:?}"
    );
}

#[test]
fn aggregate_field_target_min_max_matches_individual_extrema() {
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
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .plan()
            .expect("field-target min-max plan should build")
    };

    let min_max = load
        .aggregate_min_max_by(build_plan(), "rank")
        .expect("min_max_by(rank) should succeed");
    let min_by = load
        .aggregate_min_by(build_plan(), "rank")
        .expect("min_by(rank) should succeed");
    let max_by = load
        .aggregate_max_by(build_plan(), "rank")
        .expect("max_by(rank) should succeed");
    let expected_pair = min_by.zip(max_by);

    assert_eq!(
        min_max, expected_pair,
        "min_max_by(rank) should match individual min_by/max_by terminals"
    );
    assert_eq!(
        min_max.map(|(min_id, _)| min_id.key()),
        Some(Ulid::from_u128(8_2011)),
        "min_max_by(rank) min tie-break should use primary key ascending"
    );
    assert_eq!(
        min_max.map(|(_, max_id)| max_id.key()),
        Some(Ulid::from_u128(8_2013)),
        "min_max_by(rank) max tie-break should use primary key ascending"
    );
}

#[test]
fn aggregate_field_target_min_max_metamorphic_matrix_matches_individual_extrema() {
    seed_pushdown_entities(&[
        (8_2021, 7, 10),
        (8_2022, 7, 10),
        (8_2023, 7, 20),
        (8_2024, 7, 30),
        (8_2025, 7, 40),
        (8_2026, 7, 40),
        (8_2027, 8, 15),
        (8_2028, 8, 25),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let overlapping_predicate = Predicate::Or(vec![
        id_in_predicate(&[8_2021, 8_2022, 8_2023, 8_2024, 8_2025, 8_2026]),
        id_in_predicate(&[8_2022, 8_2023, 8_2026, 8_2027, 8_2028]),
    ]);

    for (label, distinct, desc, offset, limit) in [
        ("asc/no-distinct/unbounded", false, false, 0u32, None),
        ("asc/no-distinct/windowed", false, false, 1u32, Some(4u32)),
        ("asc/distinct/windowed", true, false, 1u32, Some(4u32)),
        ("desc/no-distinct/windowed", false, true, 1u32, Some(4u32)),
        ("desc/distinct/windowed", true, true, 2u32, Some(3u32)),
        ("desc/distinct/empty-window", true, true, 50u32, Some(3u32)),
    ] {
        let build_query = || {
            let mut query = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(overlapping_predicate.clone());
            if distinct {
                query = query.distinct();
            }
            query = if desc {
                query.order_by_desc("id")
            } else {
                query.order_by("id")
            };
            query = query.offset(offset);
            if let Some(limit) = limit {
                query = query.limit(limit);
            }

            query
        };

        let min_max = load
            .aggregate_min_max_by(
                build_query()
                    .plan()
                    .expect("metamorphic min_max plan should build"),
                "rank",
            )
            .expect("metamorphic min_max_by(rank) should succeed");
        let min_by = load
            .aggregate_min_by(
                build_query()
                    .plan()
                    .expect("metamorphic min plan should build"),
                "rank",
            )
            .expect("metamorphic min_by(rank) should succeed");
        let max_by = load
            .aggregate_max_by(
                build_query()
                    .plan()
                    .expect("metamorphic max plan should build"),
                "rank",
            )
            .expect("metamorphic max_by(rank) should succeed");

        assert_eq!(
            min_max,
            min_by.zip(max_by),
            "metamorphic min_max parity failed for case={label}"
        );
    }
}

#[test]
fn aggregate_field_target_min_max_empty_window_returns_none() {
    seed_pushdown_entities(&[(8_2031, 7, 10), (8_2032, 7, 20), (8_2033, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let min_max = load
        .aggregate_min_max_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(u32_eq_predicate("group", 7))
                .order_by("id")
                .offset(50)
                .limit(2)
                .plan()
                .expect("empty-window min_max plan should build"),
            "rank",
        )
        .expect("empty-window min_max_by(rank) should succeed");

    assert_eq!(min_max, None, "empty-window min_max_by should return None");
}

#[test]
fn aggregate_field_target_min_max_single_row_returns_same_id_pair() {
    seed_pushdown_entities(&[(8_2041, 7, 10), (8_2042, 7, 20), (8_2043, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let min_max = load
        .aggregate_min_max_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(u32_eq_predicate("group", 7))
                .order_by("id")
                .offset(1)
                .limit(1)
                .plan()
                .expect("single-row min_max plan should build"),
            "rank",
        )
        .expect("single-row min_max_by(rank) should succeed");

    assert_eq!(
        min_max.map(|(min_id, max_id)| (min_id.key(), max_id.key())),
        Some((Ulid::from_u128(8_2042), Ulid::from_u128(8_2042))),
        "single-row min_max_by should return the same id for both extrema"
    );
}

#[test]
fn aggregate_field_target_median_order_direction_invariant_on_same_window() {
    seed_pushdown_entities(&[
        (8_2051, 7, 10),
        (8_2052, 7, 20),
        (8_2053, 7, 20),
        (8_2054, 7, 40),
        (8_2055, 7, 50),
        (8_2056, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let asc_median = load
        .aggregate_median_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(u32_eq_predicate("group", 7))
                .order_by("id")
                .plan()
                .expect("median ASC plan should build"),
            "rank",
        )
        .expect("median_by(rank) ASC should succeed");
    let desc_median = load
        .aggregate_median_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("id")
                .plan()
                .expect("median DESC plan should build"),
            "rank",
        )
        .expect("median_by(rank) DESC should succeed");

    assert_eq!(
        asc_median, desc_median,
        "median_by(rank) should be invariant to query order direction on the same row window"
    );
}

#[test]
fn aggregate_numeric_field_sum_and_avg_use_decimal_projection() {
    seed_pushdown_entities(&[
        (8_091, 7, 10),
        (8_092, 7, 20),
        (8_093, 7, 35),
        (8_094, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
            .plan()
            .expect("numeric field aggregate plan should build")
    };

    let sum = load
        .aggregate_sum_by(build_plan(), "rank")
        .expect("sum_by(rank) should succeed");
    let avg = load
        .aggregate_avg_by(build_plan(), "rank")
        .expect("avg_by(rank) should succeed");
    let expected_avg = Decimal::from_num(65u64).expect("sum decimal")
        / Decimal::from_num(3u64).expect("count decimal");

    assert_eq!(
        sum,
        Decimal::from_num(65u64),
        "sum_by(rank) should match row set"
    );
    assert_eq!(
        avg,
        Some(expected_avg),
        "avg_by(rank) should use decimal division semantics"
    );
}

#[test]
fn aggregate_numeric_field_unknown_target_fails_without_scan() {
    seed_pushdown_entities(&[(8_101, 7, 10), (8_102, 7, 20)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .plan()
        .expect("numeric field unknown-target plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_sum_by(plan, "missing_field")
    });
    let Err(err) = result else {
        panic!("sum_by(missing_field) should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "unknown numeric target should fail before scan-budget consumption"
    );
}

#[test]
fn aggregate_numeric_field_non_numeric_target_fails_without_scan() {
    seed_pushdown_entities(&[(8_111, 7, 10), (8_112, 7, 20)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .plan()
        .expect("numeric field non-numeric target plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_avg_by(plan, "label")
    });
    let Err(err) = result else {
        panic!("avg_by(label) should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "non-numeric target should fail before scan-budget consumption"
    );
}

#[test]
fn aggregate_parity_ordered_page_window_desc() {
    seed_simple_entities(&[8201, 8202, 8203, 8204, 8205, 8206, 8207, 8208]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by_desc("id")
                .offset(1)
                .limit(4)
        },
        "ordered DESC page window",
    );
}

#[test]
fn aggregate_parity_by_id_and_by_ids_paths() {
    seed_simple_entities(&[8601, 8602, 8603, 8604]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || Query::<SimpleEntity>::new(ReadConsistency::MissingOk).by_id(Ulid::from_u128(8602)),
        "by_id path",
    );

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk).by_ids([
                Ulid::from_u128(8604),
                Ulid::from_u128(8601),
                Ulid::from_u128(8604),
            ])
        },
        "by_ids path",
    );
}

#[test]
fn aggregate_parity_by_id_window_shape() {
    seed_simple_entities(&[8611]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .by_id(Ulid::from_u128(8611))
                .order_by("id")
                .offset(1)
                .limit(1)
        },
        "by_id windowed shape",
    );
}

#[test]
fn aggregate_by_id_windowed_count_scans_one_candidate_key() {
    seed_simple_entities(&[8621]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .by_id(Ulid::from_u128(8621))
                .order_by("id")
                .offset(1)
                .limit(1)
                .plan()
                .expect("by_id windowed COUNT plan should build"),
        )
        .expect("by_id windowed COUNT should succeed")
    });

    assert_eq!(count, 0, "offset window should exclude the only row");
    assert_eq!(
        scanned, 1,
        "single-key windowed COUNT should scan only one candidate key"
    );
}

#[test]
fn aggregate_by_id_strict_missing_surfaces_corruption_error() {
    seed_simple_entities(&[8631]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let err = load
        .aggregate_exists(
            Query::<SimpleEntity>::new(ReadConsistency::Strict)
                .by_id(Ulid::from_u128(8632))
                .plan()
                .expect("strict by_id EXISTS plan should build"),
        )
        .expect_err("strict by_id aggregate should fail when row is missing");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "strict by_id aggregate missing row should classify as corruption"
    );
}

#[test]
fn aggregate_parity_by_ids_window_shape_with_duplicates() {
    seed_simple_entities(&[8641, 8642, 8643, 8644, 8645]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .by_ids([
                    Ulid::from_u128(8645),
                    Ulid::from_u128(8642),
                    Ulid::from_u128(8642),
                    Ulid::from_u128(8644),
                    Ulid::from_u128(8641),
                ])
                .order_by("id")
                .offset(1)
                .limit(2)
        },
        "by_ids windowed + duplicates shape",
    );
}

#[test]
fn aggregate_by_ids_count_dedups_before_windowing() {
    seed_simple_entities(&[8651, 8652, 8653, 8654, 8655]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .by_ids([
                    Ulid::from_u128(8654),
                    Ulid::from_u128(8652),
                    Ulid::from_u128(8652),
                    Ulid::from_u128(8651),
                ])
                .order_by("id")
                .offset(1)
                .limit(1)
                .plan()
                .expect("by_ids dedup COUNT plan should build"),
        )
        .expect("by_ids dedup COUNT should succeed")
    });

    assert_eq!(count, 1, "by_ids dedup COUNT should keep one in-window row");
    assert_eq!(
        scanned, 3,
        "by_ids dedup COUNT should preserve parity via materialized fallback when COUNT pushdown is ineligible"
    );
}

#[test]
fn aggregate_by_ids_strict_missing_surfaces_corruption_error() {
    seed_simple_entities(&[8661]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let err = load
        .aggregate_count(
            Query::<SimpleEntity>::new(ReadConsistency::Strict)
                .by_ids([Ulid::from_u128(8662)])
                .order_by("id")
                .plan()
                .expect("strict by_ids COUNT plan should build"),
        )
        .expect_err("strict by_ids aggregate should fail when row is missing");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "strict by_ids aggregate missing row should classify as corruption"
    );
}

#[test]
fn aggregate_count_full_scan_window_scans_offset_plus_limit() {
    seed_simple_entities(&[8671, 8672, 8673, 8674, 8675, 8676, 8677]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .offset(2)
                .limit(2)
                .plan()
                .expect("full-scan COUNT plan should build"),
        )
        .expect("full-scan COUNT should succeed")
    });

    assert_eq!(count, 2, "full-scan COUNT should honor the page window");
    assert_eq!(
        scanned, 4,
        "full-scan COUNT should scan exactly offset + limit keys"
    );
}

#[test]
fn aggregate_count_key_range_window_scans_offset_plus_limit() {
    seed_simple_entities(&[8681, 8682, 8683, 8684, 8685, 8686, 8687]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let mut logical_plan = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::KeyRange {
            start: Ulid::from_u128(8682),
            end: Ulid::from_u128(8686),
        },
        ReadConsistency::MissingOk,
    );
    logical_plan.order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![(
            "id".to_string(),
            crate::db::query::plan::OrderDirection::Asc,
        )],
    });
    logical_plan.page = Some(crate::db::query::plan::PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let key_range_plan = crate::db::executor::ExecutablePlan::<SimpleEntity>::new(logical_plan);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(key_range_plan)
            .expect("key-range COUNT should succeed")
    });

    assert_eq!(count, 2, "key-range COUNT should honor the page window");
    assert_eq!(
        scanned, 3,
        "key-range COUNT should scan exactly offset + limit keys"
    );
}

#[test]
fn aggregate_exists_index_range_window_scans_offset_plus_one() {
    seed_unique_index_range_entities(&[
        (8691, 100),
        (8692, 101),
        (8693, 102),
        (8694, 103),
        (8695, 104),
        (8696, 105),
    ]);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);

    let mut logical_plan = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::index_range(
            UNIQUE_INDEX_RANGE_INDEX_MODELS[0],
            vec![],
            std::ops::Bound::Included(Value::Uint(101)),
            std::ops::Bound::Excluded(Value::Uint(106)),
        ),
        ReadConsistency::MissingOk,
    );
    logical_plan.order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![
            (
                "code".to_string(),
                crate::db::query::plan::OrderDirection::Asc,
            ),
            (
                "id".to_string(),
                crate::db::query::plan::OrderDirection::Asc,
            ),
        ],
    });
    logical_plan.page = Some(crate::db::query::plan::PageSpec {
        limit: None,
        offset: 2,
    });
    let index_range_plan =
        crate::db::executor::ExecutablePlan::<UniqueIndexRangeEntity>::new(logical_plan);

    let (exists, scanned) = capture_rows_scanned_for_entity(UniqueIndexRangeEntity::PATH, || {
        load.aggregate_exists(index_range_plan)
            .expect("index-range EXISTS should succeed")
    });

    assert!(
        exists,
        "index-range EXISTS window should find a matching row"
    );
    assert_eq!(
        scanned, 3,
        "index-range EXISTS window should scan exactly offset + 1 keys"
    );
}

#[test]
fn aggregate_parity_distinct_asc() {
    seed_simple_entities(&[8301, 8302, 8303, 8304, 8305, 8306]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let predicate = Predicate::Or(vec![
        id_in_predicate(&[8301, 8302, 8303, 8304]),
        id_in_predicate(&[8303, 8304, 8305, 8306]),
    ]);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .distinct()
                .order_by("id")
                .offset(1)
                .limit(3)
        },
        "distinct ASC",
    );
}

#[test]
fn aggregate_parity_distinct_desc() {
    seed_simple_entities(&[8401, 8402, 8403, 8404, 8405, 8406]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let predicate = Predicate::Or(vec![
        id_in_predicate(&[8401, 8402, 8403, 8404]),
        id_in_predicate(&[8403, 8404, 8405, 8406]),
    ]);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .distinct()
                .order_by_desc("id")
                .offset(1)
                .limit(3)
        },
        "distinct DESC",
    );
}

#[test]
fn aggregate_field_parity_matrix_harness_covers_all_rank_terminals() {
    let labels = aggregate_field_terminal_parity_cases().map(|case| case.label);

    assert_eq!(
        labels,
        [
            "min_by(rank)",
            "max_by(rank)",
            "nth_by(rank, 1)",
            "sum_by(rank)",
            "avg_by(rank)",
            "median_by(rank)",
            "count_distinct_by(rank)",
            "min_max_by(rank)",
        ]
    );
}

#[test]
fn aggregate_field_terminal_parity_distinct_asc() {
    seed_pushdown_entities(&[
        (8_201, 7, 40),
        (8_202, 7, 10),
        (8_203, 7, 20),
        (8_204, 7, 20),
        (8_205, 7, 30),
        (8_206, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let predicate = Predicate::Or(vec![
        id_in_predicate(&[8_201, 8_202, 8_203, 8_204]),
        id_in_predicate(&[8_203, 8_204, 8_205, 8_206]),
    ]);

    assert_field_aggregate_parity_for_query(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .distinct()
                .order_by("id")
                .offset(1)
                .limit(4)
        },
        "field terminals distinct ASC",
    );
}

#[test]
fn aggregate_field_terminal_parity_distinct_desc() {
    seed_pushdown_entities(&[
        (8_211, 7, 40),
        (8_212, 7, 10),
        (8_213, 7, 20),
        (8_214, 7, 20),
        (8_215, 7, 30),
        (8_216, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let predicate = Predicate::Or(vec![
        id_in_predicate(&[8_211, 8_212, 8_213, 8_214]),
        id_in_predicate(&[8_213, 8_214, 8_215, 8_216]),
    ]);

    assert_field_aggregate_parity_for_query(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .distinct()
                .order_by_desc("id")
                .offset(1)
                .limit(4)
        },
        "field terminals distinct DESC",
    );
}

#[test]
fn aggregate_parity_union_and_intersection_paths() {
    seed_simple_entities(&[8701, 8702, 8703, 8704, 8705, 8706]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let union_predicate = Predicate::Or(vec![
        id_in_predicate(&[8701, 8702, 8703, 8704]),
        id_in_predicate(&[8703, 8704, 8705, 8706]),
    ]);
    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(union_predicate.clone())
                .order_by("id")
                .offset(1)
                .limit(4)
        },
        "union path",
    );

    let intersection_predicate = Predicate::And(vec![
        id_in_predicate(&[8701, 8702, 8703, 8704]),
        id_in_predicate(&[8703, 8704, 8705, 8706]),
    ]);
    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(intersection_predicate.clone())
                .order_by_desc("id")
                .offset(0)
                .limit(2)
        },
        "intersection path",
    );
}

#[test]
fn aggregate_composite_count_direct_path_scan_does_not_exceed_fallback() {
    seed_phase_entities(&[
        (8751, 10),
        (8752, 20),
        (8753, 30),
        (8754, 40),
        (8755, 50),
        (8756, 60),
    ]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let build_composite_count_plan = |order_field: &str| {
        let first = vec![
            Ulid::from_u128(8751),
            Ulid::from_u128(8752),
            Ulid::from_u128(8753),
            Ulid::from_u128(8754),
        ];
        let second = vec![
            Ulid::from_u128(8753),
            Ulid::from_u128(8754),
            Ulid::from_u128(8755),
            Ulid::from_u128(8756),
        ];
        let access = crate::db::access::AccessPlan::Union(vec![
            crate::db::access::AccessPlan::path(crate::db::access::AccessPath::ByKeys(first)),
            crate::db::access::AccessPlan::path(crate::db::access::AccessPath::ByKeys(second)),
        ]);
        let mut logical_plan = crate::db::query::plan::AccessPlannedQuery::new(
            crate::db::access::AccessPath::FullScan,
            ReadConsistency::MissingOk,
        );
        logical_plan.access = access;
        logical_plan.order = Some(crate::db::query::plan::OrderSpec {
            fields: vec![(
                order_field.to_string(),
                crate::db::query::plan::OrderDirection::Asc,
            )],
        });

        crate::db::executor::ExecutablePlan::<PhaseEntity>::new(logical_plan)
    };

    let direct_plan = build_composite_count_plan("id");
    assert!(
        ExecutionKernel::is_streaming_access_shape_safe::<PhaseEntity, _>(direct_plan.as_inner()),
        "direct composite COUNT shape should be streaming-safe"
    );
    assert!(
        matches!(
            direct_plan.explain().access,
            ExplainAccessPath::Union(_) | ExplainAccessPath::Intersection(_)
        ),
        "direct COUNT shape should compile to a composite access path"
    );

    let fallback_plan = build_composite_count_plan("label");
    assert!(
        !ExecutionKernel::is_streaming_access_shape_safe::<PhaseEntity, _>(
            fallback_plan.as_inner(),
        ),
        "fallback composite COUNT shape should be streaming-unsafe"
    );
    assert!(
        matches!(
            fallback_plan.explain().access,
            ExplainAccessPath::Union(_) | ExplainAccessPath::Intersection(_)
        ),
        "fallback COUNT shape should still compile to a composite access path"
    );

    let (direct_count, direct_scanned) = capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
        load.aggregate_count(direct_plan)
            .expect("direct composite COUNT should succeed")
    });
    let (fallback_count, fallback_scanned) =
        capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
            load.aggregate_count(fallback_plan)
                .expect("fallback composite COUNT should succeed")
        });

    assert_eq!(
        direct_count, fallback_count,
        "composite direct/fallback COUNT should preserve count parity"
    );
    assert!(
        direct_scanned <= fallback_scanned,
        "composite direct COUNT should not scan more rows than fallback for equivalent composite filter"
    );
}

#[test]
fn aggregate_composite_exists_direct_path_scan_does_not_exceed_fallback() {
    seed_phase_entities(&[
        (8761, 10),
        (8762, 20),
        (8763, 30),
        (8764, 40),
        (8765, 50),
        (8766, 60),
    ]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let build_composite_exists_plan = |order_field: &str| {
        let first = vec![
            Ulid::from_u128(8761),
            Ulid::from_u128(8762),
            Ulid::from_u128(8763),
            Ulid::from_u128(8764),
        ];
        let second = vec![
            Ulid::from_u128(8763),
            Ulid::from_u128(8764),
            Ulid::from_u128(8765),
            Ulid::from_u128(8766),
        ];
        let access = crate::db::access::AccessPlan::Union(vec![
            crate::db::access::AccessPlan::path(crate::db::access::AccessPath::ByKeys(first)),
            crate::db::access::AccessPlan::path(crate::db::access::AccessPath::ByKeys(second)),
        ]);
        let mut logical_plan = crate::db::query::plan::AccessPlannedQuery::new(
            crate::db::access::AccessPath::FullScan,
            ReadConsistency::MissingOk,
        );
        logical_plan.access = access;
        logical_plan.order = Some(crate::db::query::plan::OrderSpec {
            fields: vec![(
                order_field.to_string(),
                crate::db::query::plan::OrderDirection::Asc,
            )],
        });

        crate::db::executor::ExecutablePlan::<PhaseEntity>::new(logical_plan)
    };

    let direct_plan = build_composite_exists_plan("id");
    assert!(
        ExecutionKernel::is_streaming_access_shape_safe::<PhaseEntity, _>(direct_plan.as_inner()),
        "direct composite EXISTS shape should be streaming-safe"
    );
    assert!(
        matches!(
            direct_plan.explain().access,
            ExplainAccessPath::Union(_) | ExplainAccessPath::Intersection(_)
        ),
        "direct EXISTS shape should compile to a composite access path"
    );

    let fallback_plan = build_composite_exists_plan("label");
    assert!(
        !ExecutionKernel::is_streaming_access_shape_safe::<PhaseEntity, _>(
            fallback_plan.as_inner(),
        ),
        "fallback composite EXISTS shape should be streaming-unsafe"
    );
    assert!(
        matches!(
            fallback_plan.explain().access,
            ExplainAccessPath::Union(_) | ExplainAccessPath::Intersection(_)
        ),
        "fallback EXISTS shape should still compile to a composite access path"
    );

    let (direct_exists, direct_scanned) =
        capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
            load.aggregate_exists(direct_plan)
                .expect("direct composite EXISTS should succeed")
        });
    let (fallback_exists, fallback_scanned) =
        capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
            load.aggregate_exists(fallback_plan)
                .expect("fallback composite EXISTS should succeed")
        });

    assert_eq!(
        direct_exists, fallback_exists,
        "composite direct/fallback EXISTS should preserve parity"
    );
    assert!(
        direct_scanned <= fallback_scanned,
        "composite direct EXISTS should not scan more rows than fallback for equivalent composite filter"
    );
}

#[test]
fn aggregate_parity_secondary_index_order_shape() {
    seed_pushdown_entities(&[
        (8801, 7, 40),
        (8802, 7, 10),
        (8803, 7, 30),
        (8804, 7, 20),
        (8805, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(group_seven.clone())
                .order_by("rank")
                .offset(1)
                .limit(2)
        },
        "secondary-index order shape",
    );
}

#[test]
fn aggregate_parity_secondary_index_order_shape_desc_with_explicit_pk_tie_break() {
    seed_pushdown_entities(&[
        (8801, 7, 40),
        (8802, 7, 10),
        (8803, 7, 30),
        (8804, 7, 20),
        (8805, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(group_seven.clone())
                .order_by_desc("rank")
                .order_by_desc("id")
                .offset(1)
                .limit(2)
        },
        "secondary-index order shape DESC with explicit PK tie-break",
    );
}

#[test]
fn aggregate_exists_secondary_index_window_preserves_missing_ok_scan_safety() {
    seed_pushdown_entities(&[
        (8811, 7, 10),
        (8812, 7, 20),
        (8813, 7, 30),
        (8814, 7, 40),
        (8815, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    let (exists, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_exists(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(group_seven.clone())
                .order_by("rank")
                .offset(2)
                .plan()
                .expect("secondary-index EXISTS window plan should build"),
        )
        .expect("secondary-index EXISTS window should succeed")
    });

    assert!(
        exists,
        "secondary-index EXISTS window should find a matching row"
    );
    assert_eq!(
        scanned, 5,
        "secondary-index EXISTS window should keep full prefix scan budget under MissingOk safety"
    );
}

#[test]
fn aggregate_exists_secondary_index_strict_missing_surfaces_corruption_error() {
    seed_pushdown_entities(&[(8821, 7, 10), (8822, 7, 20), (8823, 7, 30)]);
    remove_pushdown_row_data(8821);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let mut logical_plan = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::IndexPrefix {
            index: PUSHDOWN_PARITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        ReadConsistency::Strict,
    );
    logical_plan.order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![
            (
                "rank".to_string(),
                crate::db::query::plan::OrderDirection::Asc,
            ),
            (
                "id".to_string(),
                crate::db::query::plan::OrderDirection::Asc,
            ),
        ],
    });
    let strict_plan =
        crate::db::executor::ExecutablePlan::<PushdownParityEntity>::new(logical_plan);

    let err = load
        .aggregate_exists(strict_plan)
        .expect_err("strict secondary-index aggregate should fail when row is missing");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "strict secondary-index aggregate missing row should classify as corruption"
    );
}

#[test]
fn aggregate_secondary_index_extrema_strict_single_step_scans_offset_plus_one() {
    seed_pushdown_entities(&[
        (8831, 7, 10),
        (8832, 7, 20),
        (8833, 7, 30),
        (8834, 7, 40),
        (8835, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let (min_asc, scanned_min_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min(secondary_group_rank_order_plan(
                ReadConsistency::Strict,
                crate::db::query::plan::OrderDirection::Asc,
                2,
            ))
            .expect("strict secondary MIN ASC should succeed")
        });
    let (max_desc, scanned_max_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max(secondary_group_rank_order_plan(
                ReadConsistency::Strict,
                crate::db::query::plan::OrderDirection::Desc,
                2,
            ))
            .expect("strict secondary MAX DESC should succeed")
        });

    assert_eq!(min_asc.map(|id| id.key()), Some(Ulid::from_u128(8833)));
    assert_eq!(max_desc.map(|id| id.key()), Some(Ulid::from_u128(8832)));
    assert_eq!(
        scanned_min_asc, 3,
        "strict secondary MIN ASC should scan exactly offset + 1 keys"
    );
    assert_eq!(
        scanned_max_desc, 3,
        "strict secondary MAX DESC should scan exactly offset + 1 keys"
    );
}

#[test]
fn aggregate_secondary_index_extrema_missing_ok_clean_single_step_scans_offset_plus_one() {
    seed_pushdown_entities(&[
        (8841, 7, 10),
        (8842, 7, 20),
        (8843, 7, 30),
        (8844, 7, 40),
        (8845, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let (min_asc, scanned_min_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min(secondary_group_rank_order_plan(
                ReadConsistency::MissingOk,
                crate::db::query::plan::OrderDirection::Asc,
                2,
            ))
            .expect("missing-ok secondary MIN ASC should succeed")
        });
    let (max_desc, scanned_max_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max(secondary_group_rank_order_plan(
                ReadConsistency::MissingOk,
                crate::db::query::plan::OrderDirection::Desc,
                2,
            ))
            .expect("missing-ok secondary MAX DESC should succeed")
        });

    assert_eq!(min_asc.map(|id| id.key()), Some(Ulid::from_u128(8843)));
    assert_eq!(max_desc.map(|id| id.key()), Some(Ulid::from_u128(8842)));
    assert_eq!(
        scanned_min_asc, 3,
        "missing-ok secondary MIN ASC should scan exactly offset + 1 keys when leading keys are valid"
    );
    assert_eq!(
        scanned_max_desc, 3,
        "missing-ok secondary MAX DESC should scan exactly offset + 1 keys when leading keys are valid"
    );
}

#[test]
fn aggregate_field_extrema_secondary_index_eligible_shape_locks_scan_budget() {
    seed_pushdown_entities(&[
        (8_281, 7, 10),
        (8_282, 7, 20),
        (8_283, 7, 30),
        (8_284, 7, 40),
        (8_285, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let (min_by_asc, scanned_min_by_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min_by(
                secondary_group_rank_order_plan(
                    ReadConsistency::MissingOk,
                    crate::db::query::plan::OrderDirection::Asc,
                    0,
                ),
                "rank",
            )
            .expect("missing-ok secondary MIN(field) eligible shape should succeed")
        });
    let (max_by_desc, scanned_max_by_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max_by(
                secondary_group_rank_order_plan(
                    ReadConsistency::MissingOk,
                    crate::db::query::plan::OrderDirection::Desc,
                    0,
                ),
                "rank",
            )
            .expect("missing-ok secondary MAX(field) eligible shape should succeed")
        });

    assert_eq!(
        min_by_asc.map(|id| id.key()),
        Some(Ulid::from_u128(8_281)),
        "missing-ok secondary MIN(field) eligible shape should return the first ordered candidate"
    );
    assert_eq!(
        max_by_desc.map(|id| id.key()),
        Some(Ulid::from_u128(8_284)),
        "missing-ok secondary MAX(field) eligible shape should return the first ordered DESC candidate"
    );
    assert_eq!(
        scanned_min_by_asc, 4,
        "missing-ok secondary MIN(field) eligible shape should scan the full group window under current contract"
    );
    assert_eq!(
        scanned_max_by_desc, 4,
        "missing-ok secondary MAX(field) eligible shape should scan the full group window under current contract"
    );
}

#[test]
fn aggregate_secondary_index_extrema_missing_ok_stale_leading_probe_falls_back() {
    seed_pushdown_entities(&[
        (8851, 7, 10),
        (8852, 7, 20),
        (8853, 7, 30),
        (8854, 7, 40),
        (8855, 8, 50),
    ]);
    remove_pushdown_row_data(8851);
    remove_pushdown_row_data(8854);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let expected_min_asc = load
        .execute(secondary_group_rank_order_plan(
            ReadConsistency::MissingOk,
            crate::db::query::plan::OrderDirection::Asc,
            0,
        ))
        .expect("stale-leading MIN ASC baseline execute should succeed")
        .ids()
        .into_iter()
        .min();
    let expected_max_desc = load
        .execute(secondary_group_rank_order_plan(
            ReadConsistency::MissingOk,
            crate::db::query::plan::OrderDirection::Desc,
            0,
        ))
        .expect("stale-leading MAX DESC baseline execute should succeed")
        .ids()
        .into_iter()
        .max();

    let (min_asc, scanned_min_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min(secondary_group_rank_order_plan(
                ReadConsistency::MissingOk,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ))
            .expect("stale-leading secondary MIN ASC should succeed")
        });
    let (max_desc, scanned_max_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max(secondary_group_rank_order_plan(
                ReadConsistency::MissingOk,
                crate::db::query::plan::OrderDirection::Desc,
                0,
            ))
            .expect("stale-leading secondary MAX DESC should succeed")
        });

    assert_eq!(
        min_asc, expected_min_asc,
        "stale-leading MIN ASC should preserve materialized parity"
    );
    assert_eq!(
        max_desc, expected_max_desc,
        "stale-leading MAX DESC should preserve materialized parity"
    );
    assert!(
        scanned_min_asc >= 2,
        "stale-leading MIN ASC should scan past bounded probe and retry unbounded"
    );
    assert!(
        scanned_max_desc >= 2,
        "stale-leading MAX DESC should scan past bounded probe and retry unbounded"
    );
}

#[test]
fn aggregate_secondary_index_extrema_strict_stale_leading_surfaces_corruption_error() {
    seed_pushdown_entities(&[
        (8861, 7, 10),
        (8862, 7, 20),
        (8863, 7, 30),
        (8864, 7, 40),
        (8865, 8, 50),
    ]);
    remove_pushdown_row_data(8861);
    remove_pushdown_row_data(8864);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let min_err = load
        .aggregate_min(secondary_group_rank_order_plan(
            ReadConsistency::Strict,
            crate::db::query::plan::OrderDirection::Asc,
            0,
        ))
        .expect_err("strict secondary MIN should fail when leading key is stale");
    let max_err = load
        .aggregate_max(secondary_group_rank_order_plan(
            ReadConsistency::Strict,
            crate::db::query::plan::OrderDirection::Desc,
            0,
        ))
        .expect_err("strict secondary MAX should fail when leading key is stale");

    assert_eq!(
        min_err.class,
        crate::error::ErrorClass::Corruption,
        "strict secondary MIN stale-leading key should classify as corruption"
    );
    assert_eq!(
        max_err.class,
        crate::error::ErrorClass::Corruption,
        "strict secondary MAX stale-leading key should classify as corruption"
    );
}

#[test]
fn aggregate_field_extrema_missing_ok_stale_leading_probe_falls_back() {
    seed_pushdown_entities(&[
        (8_261, 7, 10),
        (8_262, 7, 20),
        (8_263, 7, 30),
        (8_264, 7, 40),
        (8_265, 8, 50),
    ]);
    remove_pushdown_row_data(8_261);
    remove_pushdown_row_data(8_264);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let expected_min_by = expected_min_by_rank_id(
        &load
            .execute(secondary_group_rank_order_plan(
                ReadConsistency::MissingOk,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ))
            .expect("missing-ok field MIN baseline execute should succeed"),
    );
    let expected_max_by = expected_max_by_rank_id(
        &load
            .execute(secondary_group_rank_order_plan(
                ReadConsistency::MissingOk,
                crate::db::query::plan::OrderDirection::Desc,
                0,
            ))
            .expect("missing-ok field MAX baseline execute should succeed"),
    );

    let (min_by, scanned_min_by) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min_by(
                secondary_group_rank_order_plan(
                    ReadConsistency::MissingOk,
                    crate::db::query::plan::OrderDirection::Asc,
                    0,
                ),
                "rank",
            )
            .expect("missing-ok field MIN should succeed")
        });
    let (max_by, scanned_max_by) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max_by(
                secondary_group_rank_order_plan(
                    ReadConsistency::MissingOk,
                    crate::db::query::plan::OrderDirection::Desc,
                    0,
                ),
                "rank",
            )
            .expect("missing-ok field MAX should succeed")
        });

    assert_eq!(
        min_by, expected_min_by,
        "missing-ok field MIN should preserve materialized parity under stale-leading keys"
    );
    assert_eq!(
        max_by, expected_max_by,
        "missing-ok field MAX should preserve materialized parity under stale-leading keys"
    );
    assert!(
        scanned_min_by >= 2,
        "missing-ok field MIN should scan past bounded probe and retry unbounded"
    );
    assert!(
        scanned_max_by >= 2,
        "missing-ok field MAX should scan past bounded probe and retry unbounded"
    );
}

#[test]
fn aggregate_field_extrema_strict_stale_leading_surfaces_corruption_error() {
    seed_pushdown_entities(&[
        (8_271, 7, 10),
        (8_272, 7, 20),
        (8_273, 7, 30),
        (8_274, 7, 40),
        (8_275, 8, 50),
    ]);
    remove_pushdown_row_data(8_271);
    remove_pushdown_row_data(8_274);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let min_err = load
        .aggregate_min_by(
            secondary_group_rank_order_plan(
                ReadConsistency::Strict,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ),
            "rank",
        )
        .expect_err("strict field MIN should fail when leading key is stale");
    let max_err = load
        .aggregate_max_by(
            secondary_group_rank_order_plan(
                ReadConsistency::Strict,
                crate::db::query::plan::OrderDirection::Desc,
                0,
            ),
            "rank",
        )
        .expect_err("strict field MAX should fail when leading key is stale");

    assert_eq!(
        min_err.class,
        crate::error::ErrorClass::Corruption,
        "strict field MIN stale-leading key should classify as corruption"
    );
    assert_eq!(
        max_err.class,
        crate::error::ErrorClass::Corruption,
        "strict field MAX stale-leading key should classify as corruption"
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn aggregate_field_terminal_error_classification_matrix() {
    seed_pushdown_entities(&[(8_291, 7, 10), (8_292, 7, 20), (8_293, 7, 30)]);
    let pushdown_load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let unknown_field_min_error = pushdown_load
        .aggregate_min_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("unknown-field MIN(field) plan should build"),
            "missing_field",
        )
        .expect_err("unknown field MIN(field) should fail");
    let unknown_field_median_error = pushdown_load
        .aggregate_median_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("unknown-field MEDIAN(field) plan should build"),
            "missing_field",
        )
        .expect_err("unknown field MEDIAN(field) should fail");
    let unknown_field_count_distinct_error = pushdown_load
        .aggregate_count_distinct_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("unknown-field COUNT_DISTINCT(field) plan should build"),
            "missing_field",
        )
        .expect_err("unknown field COUNT_DISTINCT(field) should fail");
    let unknown_field_min_max_error = pushdown_load
        .aggregate_min_max_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("unknown-field MIN_MAX(field) plan should build"),
            "missing_field",
        )
        .expect_err("unknown field MIN_MAX(field) should fail");
    let non_numeric_error = pushdown_load
        .aggregate_sum_by(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("non-numeric SUM(field) plan should build"),
            "label",
        )
        .expect_err("non-numeric SUM(field) should fail");
    remove_pushdown_row_data(8_291);
    let strict_stale_error = pushdown_load
        .aggregate_min_by(
            secondary_group_rank_order_plan(
                ReadConsistency::Strict,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ),
            "rank",
        )
        .expect_err("strict stale-leading MIN(field) should fail");
    let strict_stale_median_error = pushdown_load
        .aggregate_median_by(
            secondary_group_rank_order_plan(
                ReadConsistency::Strict,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ),
            "rank",
        )
        .expect_err("strict stale-leading MEDIAN(field) should fail");
    let strict_stale_count_distinct_error = pushdown_load
        .aggregate_count_distinct_by(
            secondary_group_rank_order_plan(
                ReadConsistency::Strict,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ),
            "rank",
        )
        .expect_err("strict stale-leading COUNT_DISTINCT(field) should fail");
    let strict_stale_min_max_error = pushdown_load
        .aggregate_min_max_by(
            secondary_group_rank_order_plan(
                ReadConsistency::Strict,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ),
            "rank",
        )
        .expect_err("strict stale-leading MIN_MAX(field) should fail");

    seed_phase_entities(&[(8_294, 10), (8_295, 20)]);
    let phase_load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let non_orderable_min_error = phase_load
        .aggregate_min_by(
            Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("non-orderable MIN(field) plan should build"),
            "tags",
        )
        .expect_err("non-orderable MIN(field) should fail");
    let non_orderable_median_error = phase_load
        .aggregate_median_by(
            Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("non-orderable MEDIAN(field) plan should build"),
            "tags",
        )
        .expect_err("non-orderable MEDIAN(field) should fail");
    let non_orderable_min_max_error = phase_load
        .aggregate_min_max_by(
            Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("non-orderable MIN_MAX(field) plan should build"),
            "tags",
        )
        .expect_err("non-orderable MIN_MAX(field) should fail");

    assert_eq!(
        unknown_field_min_error.class,
        ErrorClass::Unsupported,
        "unknown field MIN(field) should classify as Unsupported"
    );
    assert_eq!(
        unknown_field_median_error.class,
        ErrorClass::Unsupported,
        "unknown field MEDIAN(field) should classify as Unsupported"
    );
    assert_eq!(
        unknown_field_count_distinct_error.class,
        ErrorClass::Unsupported,
        "unknown field COUNT_DISTINCT(field) should classify as Unsupported"
    );
    assert_eq!(
        unknown_field_min_max_error.class,
        ErrorClass::Unsupported,
        "unknown field MIN_MAX(field) should classify as Unsupported"
    );
    assert_eq!(
        non_orderable_min_error.class,
        ErrorClass::Unsupported,
        "non-orderable MIN(field) should classify as Unsupported"
    );
    assert_eq!(
        non_orderable_median_error.class,
        ErrorClass::Unsupported,
        "non-orderable MEDIAN(field) should classify as Unsupported"
    );
    assert_eq!(
        non_orderable_min_max_error.class,
        ErrorClass::Unsupported,
        "non-orderable MIN_MAX(field) should classify as Unsupported"
    );
    assert_eq!(
        non_numeric_error.class,
        ErrorClass::Unsupported,
        "non-numeric SUM(field) should classify as Unsupported"
    );
    assert_eq!(
        strict_stale_error.class,
        ErrorClass::Corruption,
        "strict stale-leading MIN(field) should classify as Corruption"
    );
    assert_eq!(
        strict_stale_median_error.class,
        ErrorClass::Corruption,
        "strict stale-leading MEDIAN(field) should classify as Corruption"
    );
    assert_eq!(
        strict_stale_count_distinct_error.class,
        ErrorClass::Corruption,
        "strict stale-leading COUNT_DISTINCT(field) should classify as Corruption"
    );
    assert_eq!(
        strict_stale_min_max_error.class,
        ErrorClass::Corruption,
        "strict stale-leading MIN_MAX(field) should classify as Corruption"
    );
}

#[test]
fn aggregate_field_extrema_negative_lock_distinct_and_offset_shapes_avoid_single_step_probe() {
    seed_pushdown_entities(&[
        (8_301, 7, 10),
        (8_302, 7, 20),
        (8_303, 7, 30),
        (8_304, 7, 40),
        (8_305, 7, 50),
        (8_306, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let (distinct_min, scanned_distinct_min) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min_by(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(u32_eq_predicate("group", 7))
                    .distinct()
                    .order_by("rank")
                    .plan()
                    .expect("distinct MIN(field) plan should build"),
                "rank",
            )
            .expect("distinct MIN(field) should succeed")
        });
    let (offset_max, scanned_offset_max) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max_by(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(u32_eq_predicate("group", 7))
                    .order_by("rank")
                    .offset(2)
                    .plan()
                    .expect("offset MAX(field) plan should build"),
                "rank",
            )
            .expect("offset MAX(field) should succeed")
        });

    assert_eq!(
        distinct_min.map(|id| id.key()),
        Some(Ulid::from_u128(8_301)),
        "distinct MIN(field) should preserve canonical parity"
    );
    assert_eq!(
        offset_max.map(|id| id.key()),
        Some(Ulid::from_u128(8_305)),
        "offset MAX(field) should preserve canonical parity"
    );
    assert!(
        scanned_distinct_min >= 2,
        "distinct MIN(field) should not collapse to single-step probe"
    );
    assert!(
        scanned_offset_max >= 3,
        "offset MAX(field) should remain bounded by window traversal, not single-step probe"
    );
}

#[test]
fn aggregate_parity_index_range_shape() {
    seed_unique_index_range_entities(&[
        (8901, 100),
        (8902, 101),
        (8903, 102),
        (8904, 103),
        (8905, 104),
        (8906, 105),
    ]);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let range_predicate = u32_range_predicate("code", 101, 105);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
                .filter(range_predicate.clone())
                .order_by_desc("code")
                .offset(1)
                .limit(2)
        },
        "index-range shape",
    );
}

#[test]
fn aggregate_parity_strict_consistency() {
    seed_simple_entities(&[9001, 9002, 9003, 9004, 9005]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::Strict)
                .order_by_desc("id")
                .offset(1)
                .limit(3)
        },
        "strict consistency",
    );
}

#[test]
fn aggregate_parity_limit_zero_window() {
    seed_simple_entities(&[9101, 9102, 9103, 9104]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .offset(2)
                .limit(0)
        },
        "limit zero window",
    );
}

#[test]
fn session_load_aggregate_terminals_match_execute() {
    seed_simple_entities(&[8501, 8502, 8503, 8504, 8505]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<SimpleEntity>()
            .order_by("id")
            .offset(1)
            .limit(3)
    };

    let expected = load_window()
        .execute()
        .expect("baseline session execute should succeed");
    let expected_count = expected.count();
    let expected_exists = !expected.is_empty();
    let expected_min = expected.ids().into_iter().min();
    let expected_max = expected.ids().into_iter().max();
    let expected_min_by_id = expected.ids().into_iter().min();
    let expected_max_by_id = expected.ids().into_iter().max();
    let mut expected_ordered_ids = expected.ids();
    expected_ordered_ids.sort_unstable();
    let expected_nth_by_id = expected_ordered_ids.get(1).copied();
    let expected_first = expected.id();
    let expected_last = expected.ids().last().copied();

    let actual_count = load_window().count().expect("session count should succeed");
    let actual_exists = load_window()
        .exists()
        .expect("session exists should succeed");
    let actual_min = load_window().min().expect("session min should succeed");
    let actual_max = load_window().max().expect("session max should succeed");
    let actual_min_by_id = load_window()
        .min_by("id")
        .expect("session min_by(id) should succeed");
    let actual_max_by_id = load_window()
        .max_by("id")
        .expect("session max_by(id) should succeed");
    let actual_nth_by_id = load_window()
        .nth_by("id", 1)
        .expect("session nth_by(id, 1) should succeed");
    let actual_first = load_window().first().expect("session first should succeed");
    let actual_last = load_window().last().expect("session last should succeed");

    assert_eq!(actual_count, expected_count, "session count parity failed");
    assert_eq!(
        actual_exists, expected_exists,
        "session exists parity failed"
    );
    assert_eq!(actual_min, expected_min, "session min parity failed");
    assert_eq!(actual_max, expected_max, "session max parity failed");
    assert_eq!(
        actual_min_by_id, expected_min_by_id,
        "session min_by(id) parity failed"
    );
    assert_eq!(
        actual_max_by_id, expected_max_by_id,
        "session max_by(id) parity failed"
    );
    assert_eq!(
        actual_nth_by_id, expected_nth_by_id,
        "session nth_by(id, 1) parity failed"
    );
    assert_eq!(actual_first, expected_first, "session first parity failed");
    assert_eq!(actual_last, expected_last, "session last parity failed");
}

#[test]
fn session_load_numeric_field_aggregates_match_execute() {
    seed_pushdown_entities(&[
        (8_121, 7, 10),
        (8_122, 7, 20),
        (8_123, 7, 35),
        (8_124, 8, 99),
    ]);
    let session = DbSession::new(DB);

    let expected_response = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .execute()
        .expect("baseline execute for numeric field aggregates should succeed");
    let mut expected_sum = Decimal::ZERO;
    let mut expected_count = 0u64;
    for (_, entity) in expected_response {
        let rank = Decimal::from_num(u64::from(entity.rank)).expect("rank decimal");
        expected_sum += rank;
        expected_count = expected_count.saturating_add(1);
    }
    let expected_sum_decimal = expected_sum;
    let expected_sum = Some(expected_sum_decimal);
    let expected_avg = if expected_count == 0 {
        None
    } else {
        Some(expected_sum_decimal / Decimal::from_num(expected_count).expect("count decimal"))
    };

    let actual_sum = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .sum_by("rank")
        .expect("session sum_by(rank) should succeed");
    let actual_avg = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .avg_by("rank")
        .expect("session avg_by(rank) should succeed");

    assert_eq!(
        actual_sum, expected_sum,
        "session sum_by(rank) parity failed"
    );
    assert_eq!(
        actual_avg, expected_avg,
        "session avg_by(rank) parity failed"
    );
}

#[test]
fn session_load_new_field_aggregates_match_execute() {
    seed_pushdown_entities(&[
        (8_311, 7, 10),
        (8_312, 7, 10),
        (8_313, 7, 20),
        (8_314, 7, 30),
        (8_315, 7, 40),
        (8_316, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for new field aggregates should succeed");
    let expected_median = expected_median_by_rank_id(&expected);
    let expected_count_distinct = expected_count_distinct_by_rank(&expected);
    let expected_min_max = expected_min_max_by_rank_ids(&expected);

    let actual_median = load_window()
        .median_by("rank")
        .expect("session median_by(rank) should succeed");
    let actual_count_distinct = load_window()
        .count_distinct_by("rank")
        .expect("session count_distinct_by(rank) should succeed");
    let actual_min_max = load_window()
        .min_max_by("rank")
        .expect("session min_max_by(rank) should succeed");

    assert_eq!(
        actual_median, expected_median,
        "session median_by(rank) parity failed"
    );
    assert_eq!(
        actual_count_distinct, expected_count_distinct,
        "session count_distinct_by(rank) parity failed"
    );
    assert_eq!(
        actual_min_max, expected_min_max,
        "session min_max_by(rank) parity failed"
    );
}

#[test]
fn session_load_values_by_matches_execute_projection() {
    seed_pushdown_entities(&[
        (8_321, 7, 10),
        (8_322, 7, 10),
        (8_323, 7, 20),
        (8_324, 7, 30),
        (8_325, 7, 40),
        (8_326, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for values_by should succeed");
    let actual = load_window()
        .values_by("rank")
        .expect("session values_by(rank) should succeed");

    assert_eq!(
        actual,
        expected_values_by_rank(&expected),
        "session values_by(rank) parity failed"
    );
}

#[test]
fn session_load_take_matches_execute_prefix() {
    seed_pushdown_entities(&[
        (8_3601, 7, 10),
        (8_3602, 7, 20),
        (8_3603, 7, 30),
        (8_3604, 7, 40),
        (8_3605, 7, 50),
        (8_3606, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for take should succeed");
    let actual_take_two = load_window()
        .take(2)
        .expect("session take(2) should succeed");
    let actual_take_ten = load_window()
        .take(10)
        .expect("session take(10) should succeed");
    let expected_take_two_ids: Vec<Id<PushdownParityEntity>> =
        expected.ids().into_iter().take(2).collect();

    assert_eq!(
        actual_take_two.ids(),
        expected_take_two_ids,
        "session take(2) should match first two execute() rows in effective response order"
    );
    assert_eq!(
        actual_take_ten.ids(),
        expected.ids(),
        "session take(k) with k above response size should preserve full effective response"
    );
}

#[test]
fn session_load_top_k_by_matches_execute_field_ordering() {
    seed_pushdown_entities(&[
        (8_3701, 7, 20),
        (8_3702, 7, 40),
        (8_3703, 7, 40),
        (8_3704, 7, 10),
        (8_3705, 7, 30),
        (8_3706, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(0)
            .limit(5)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for top_k_by should succeed");
    let actual_top_three = load_window()
        .top_k_by("rank", 3)
        .expect("session top_k_by(rank, 3) should succeed");
    let mut expected_rank_order = expected
        .0
        .iter()
        .map(|(id, entity)| (entity.rank, *id))
        .collect::<Vec<_>>();
    expected_rank_order.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        right_rank
            .cmp(left_rank)
            .then_with(|| left_id.key().cmp(&right_id.key()))
    });
    let expected_top_three_ids: Vec<Id<PushdownParityEntity>> = expected_rank_order
        .into_iter()
        .take(3)
        .map(|(_, id)| id)
        .collect();

    assert_eq!(
        actual_top_three.ids(),
        expected_top_three_ids,
        "session top_k_by(rank, 3) should match execute() reduced by deterministic (rank desc, id asc) ordering"
    );
}

#[test]
fn session_load_bottom_k_by_matches_execute_field_ordering() {
    seed_pushdown_entities(&[
        (8_3721, 7, 20),
        (8_3722, 7, 40),
        (8_3723, 7, 40),
        (8_3724, 7, 10),
        (8_3725, 7, 30),
        (8_3726, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(0)
            .limit(5)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for bottom_k_by should succeed");
    let actual_bottom_three = load_window()
        .bottom_k_by("rank", 3)
        .expect("session bottom_k_by(rank, 3) should succeed");
    let mut expected_rank_order = expected
        .0
        .iter()
        .map(|(id, entity)| (entity.rank, *id))
        .collect::<Vec<_>>();
    expected_rank_order.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        left_rank
            .cmp(right_rank)
            .then_with(|| left_id.key().cmp(&right_id.key()))
    });
    let expected_bottom_three_ids: Vec<Id<PushdownParityEntity>> = expected_rank_order
        .into_iter()
        .take(3)
        .map(|(_, id)| id)
        .collect();

    assert_eq!(
        actual_bottom_three.ids(),
        expected_bottom_three_ids,
        "session bottom_k_by(rank, 3) should match execute() reduced by deterministic (rank asc, id asc) ordering"
    );
}

#[test]
fn session_load_top_k_by_is_direction_invariant_for_same_effective_window() {
    seed_pushdown_entities(&[
        (8_3711, 7, 10),
        (8_3712, 7, 40),
        (8_3713, 7, 20),
        (8_3714, 7, 30),
        (8_3715, 7, 40),
        (8_3716, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let asc = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("id")
        .top_k_by("rank", 3)
        .expect("session top_k_by(rank, 3) ASC base order should succeed");
    let desc = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by_desc("id")
        .top_k_by("rank", 3)
        .expect("session top_k_by(rank, 3) DESC base order should succeed");

    assert_eq!(
        asc.ids(),
        desc.ids(),
        "top_k_by(rank, k) should be invariant to ASC/DESC base scan direction over the same effective row set"
    );
}

#[test]
fn session_load_bottom_k_by_is_direction_invariant_for_same_effective_window() {
    seed_pushdown_entities(&[
        (8_3731, 7, 10),
        (8_3732, 7, 40),
        (8_3733, 7, 20),
        (8_3734, 7, 30),
        (8_3735, 7, 40),
        (8_3736, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let asc = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("id")
        .bottom_k_by("rank", 3)
        .expect("session bottom_k_by(rank, 3) ASC base order should succeed");
    let desc = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by_desc("id")
        .bottom_k_by("rank", 3)
        .expect("session bottom_k_by(rank, 3) DESC base order should succeed");

    assert_eq!(
        asc.ids(),
        desc.ids(),
        "bottom_k_by(rank, k) should be invariant to ASC/DESC base scan direction over the same effective row set"
    );
}

#[test]
fn session_load_top_k_by_values_matches_top_k_by_projection() {
    seed_pushdown_entities(&[
        (8_3771, 7, 20),
        (8_3772, 7, 40),
        (8_3773, 7, 40),
        (8_3774, 7, 10),
        (8_3775, 7, 30),
        (8_3776, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(0)
            .limit(5)
    };

    let ranked_rows = load_window()
        .top_k_by("rank", 3)
        .expect("session top_k_by(rank, 3) should succeed");
    let ranked_values = load_window()
        .top_k_by_values("rank", 3)
        .expect("session top_k_by_values(rank, 3) should succeed");

    assert_eq!(
        ranked_values,
        expected_values_by_rank(&ranked_rows),
        "session top_k_by_values(rank, 3) should match top_k_by(rank, 3) projected values"
    );
}

#[test]
fn session_load_bottom_k_by_values_matches_bottom_k_by_projection() {
    seed_pushdown_entities(&[
        (8_3781, 7, 20),
        (8_3782, 7, 40),
        (8_3783, 7, 40),
        (8_3784, 7, 10),
        (8_3785, 7, 30),
        (8_3786, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(0)
            .limit(5)
    };

    let ranked_rows = load_window()
        .bottom_k_by("rank", 3)
        .expect("session bottom_k_by(rank, 3) should succeed");
    let ranked_values = load_window()
        .bottom_k_by_values("rank", 3)
        .expect("session bottom_k_by_values(rank, 3) should succeed");

    assert_eq!(
        ranked_values,
        expected_values_by_rank(&ranked_rows),
        "session bottom_k_by_values(rank, 3) should match bottom_k_by(rank, 3) projected values"
    );
}

#[test]
fn session_load_top_k_by_values_is_direction_invariant_for_same_effective_window() {
    seed_pushdown_entities(&[
        (8_3791, 7, 10),
        (8_3792, 7, 40),
        (8_3793, 7, 20),
        (8_3794, 7, 30),
        (8_3795, 7, 40),
        (8_3796, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let asc = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("id")
        .top_k_by_values("rank", 3)
        .expect("session top_k_by_values(rank, 3) ASC base order should succeed");
    let desc = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by_desc("id")
        .top_k_by_values("rank", 3)
        .expect("session top_k_by_values(rank, 3) DESC base order should succeed");

    assert_eq!(
        asc, desc,
        "top_k_by_values(rank, k) should be invariant to ASC/DESC base scan direction over the same effective row set"
    );
}

#[test]
fn session_load_bottom_k_by_values_is_direction_invariant_for_same_effective_window() {
    seed_pushdown_entities(&[
        (8_3801, 7, 10),
        (8_3802, 7, 40),
        (8_3803, 7, 20),
        (8_3804, 7, 30),
        (8_3805, 7, 40),
        (8_3806, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let asc = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("id")
        .bottom_k_by_values("rank", 3)
        .expect("session bottom_k_by_values(rank, 3) ASC base order should succeed");
    let desc = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by_desc("id")
        .bottom_k_by_values("rank", 3)
        .expect("session bottom_k_by_values(rank, 3) DESC base order should succeed");

    assert_eq!(
        asc, desc,
        "bottom_k_by_values(rank, k) should be invariant to ASC/DESC base scan direction over the same effective row set"
    );
}

#[test]
fn session_load_top_k_by_with_ids_matches_top_k_by_projection() {
    seed_pushdown_entities(&[
        (8_3807, 7, 20),
        (8_3808, 7, 40),
        (8_3809, 7, 40),
        (8_3810, 7, 10),
        (8_3811, 7, 30),
        (8_3812, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(0)
            .limit(5)
    };

    let ranked_rows = load_window()
        .top_k_by("rank", 3)
        .expect("session top_k_by(rank, 3) should succeed");
    let ranked_values_with_ids = load_window()
        .top_k_by_with_ids("rank", 3)
        .expect("session top_k_by_with_ids(rank, 3) should succeed");

    assert_eq!(
        ranked_values_with_ids,
        expected_values_by_rank_with_ids(&ranked_rows),
        "session top_k_by_with_ids(rank, 3) should match top_k_by(rank, 3) projected id/value pairs"
    );
}

#[test]
fn session_load_bottom_k_by_with_ids_matches_bottom_k_by_projection() {
    seed_pushdown_entities(&[
        (8_3813, 7, 20),
        (8_3814, 7, 40),
        (8_3815, 7, 40),
        (8_3816, 7, 10),
        (8_3817, 7, 30),
        (8_3818, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(0)
            .limit(5)
    };

    let ranked_rows = load_window()
        .bottom_k_by("rank", 3)
        .expect("session bottom_k_by(rank, 3) should succeed");
    let ranked_values_with_ids = load_window()
        .bottom_k_by_with_ids("rank", 3)
        .expect("session bottom_k_by_with_ids(rank, 3) should succeed");

    assert_eq!(
        ranked_values_with_ids,
        expected_values_by_rank_with_ids(&ranked_rows),
        "session bottom_k_by_with_ids(rank, 3) should match bottom_k_by(rank, 3) projected id/value pairs"
    );
}

#[test]
fn session_load_top_k_by_with_ids_is_direction_invariant_for_same_effective_window() {
    seed_pushdown_entities(&[
        (8_3819, 7, 10),
        (8_3820, 7, 40),
        (8_3821, 7, 20),
        (8_3822, 7, 30),
        (8_3823, 7, 40),
        (8_3824, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let asc = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("id")
        .top_k_by_with_ids("rank", 3)
        .expect("session top_k_by_with_ids(rank, 3) ASC base order should succeed");
    let desc = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by_desc("id")
        .top_k_by_with_ids("rank", 3)
        .expect("session top_k_by_with_ids(rank, 3) DESC base order should succeed");

    assert_eq!(
        asc, desc,
        "top_k_by_with_ids(rank, k) should be invariant to ASC/DESC base scan direction over the same effective row set"
    );
}

#[test]
fn session_load_bottom_k_by_with_ids_is_direction_invariant_for_same_effective_window() {
    seed_pushdown_entities(&[
        (8_3825, 7, 10),
        (8_3826, 7, 40),
        (8_3827, 7, 20),
        (8_3828, 7, 30),
        (8_3829, 7, 40),
        (8_3830, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let asc = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("id")
        .bottom_k_by_with_ids("rank", 3)
        .expect("session bottom_k_by_with_ids(rank, 3) ASC base order should succeed");
    let desc = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by_desc("id")
        .bottom_k_by_with_ids("rank", 3)
        .expect("session bottom_k_by_with_ids(rank, 3) DESC base order should succeed");

    assert_eq!(
        asc, desc,
        "bottom_k_by_with_ids(rank, k) should be invariant to ASC/DESC base scan direction over the same effective row set"
    );
}

#[test]
fn aggregate_field_target_top_k_by_direction_invariance_across_forced_access_shapes() {
    // Phase 1: force a full-scan shape and assert ASC/DESC base-order invariance.
    seed_simple_entities(&[8_3941, 8_3942, 8_3943, 8_3944, 8_3945, 8_3946]);
    let simple_load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let full_scan_top_ids_for = |direction: OrderDirection| {
        let query = Query::<SimpleEntity>::new(ReadConsistency::MissingOk);
        let query = match direction {
            OrderDirection::Asc => query.order_by("id"),
            OrderDirection::Desc => query.order_by_desc("id"),
        };
        let plan = query
            .plan()
            .expect("top_k_by full-scan direction-invariance plan should build");
        assert!(
            matches!(plan.explain().access, ExplainAccessPath::FullScan),
            "top_k_by full-scan direction invariance test must force FullScan"
        );

        simple_load
            .top_k_by(plan, "id", 3)
            .expect("top_k_by(id, 3) should succeed for full-scan direction matrix")
            .ids()
    };
    let full_scan_asc = full_scan_top_ids_for(OrderDirection::Asc);
    let full_scan_desc = full_scan_top_ids_for(OrderDirection::Desc);
    assert_eq!(
        full_scan_asc, full_scan_desc,
        "top_k_by(id, k) should be invariant to ASC/DESC base order under forced FullScan"
    );

    // Phase 2: force an index-range shape and assert ASC/DESC base-order invariance.
    seed_unique_index_range_entities(&[
        (8_3951, 100),
        (8_3952, 101),
        (8_3953, 102),
        (8_3954, 103),
        (8_3955, 104),
        (8_3956, 105),
    ]);
    let range_load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let code_range = u32_range_predicate("code", 101, 106);
    let index_range_top_ids_for = |direction: OrderDirection| {
        let query = Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
            .filter(code_range.clone());
        let query = match direction {
            OrderDirection::Asc => query.order_by("code"),
            OrderDirection::Desc => query.order_by_desc("code"),
        };
        let plan = query
            .plan()
            .expect("top_k_by index-range direction-invariance plan should build");
        assert!(
            matches!(plan.explain().access, ExplainAccessPath::IndexRange { .. }),
            "top_k_by index-range direction invariance test must force IndexRange"
        );

        range_load
            .top_k_by(plan, "code", 3)
            .expect("top_k_by(code, 3) should succeed for index-range direction matrix")
            .ids()
    };
    let index_range_asc = index_range_top_ids_for(OrderDirection::Asc);
    let index_range_desc = index_range_top_ids_for(OrderDirection::Desc);
    assert_eq!(
        index_range_asc, index_range_desc,
        "top_k_by(code, k) should be invariant to ASC/DESC base order under forced IndexRange"
    );
}

#[test]
fn session_load_ranked_rows_are_invariant_to_insertion_order() {
    let rows_a = [
        (8_3961, 7, 10),
        (8_3962, 7, 40),
        (8_3963, 7, 20),
        (8_3964, 7, 30),
        (8_3965, 7, 40),
    ];
    let rows_b = [
        (8_3965, 7, 40),
        (8_3963, 7, 20),
        (8_3961, 7, 10),
        (8_3964, 7, 30),
        (8_3962, 7, 40),
    ];
    let ranked_ids_for = |rows: &[(u128, u32, u32)]| {
        seed_pushdown_entities(rows);
        let session = DbSession::new(DB);
        let top_ids = session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by("id")
            .top_k_by("rank", 3)
            .expect("top_k_by(rank, 3) insertion-order invariance query should succeed")
            .ids();
        let bottom_ids = session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by("id")
            .bottom_k_by("rank", 3)
            .expect("bottom_k_by(rank, 3) insertion-order invariance query should succeed")
            .ids();

        (top_ids, bottom_ids)
    };

    let (top_a, bottom_a) = ranked_ids_for(&rows_a);
    let (top_b, bottom_b) = ranked_ids_for(&rows_b);

    assert_eq!(
        top_a, top_b,
        "top_k_by(rank, k) should be invariant to seed insertion order for equivalent rows"
    );
    assert_eq!(
        bottom_a, bottom_b,
        "bottom_k_by(rank, k) should be invariant to seed insertion order for equivalent rows"
    );
}

#[test]
fn aggregate_field_target_top_k_by_k_one_matches_max_by_ids_with_ties() {
    seed_pushdown_entities(&[
        (8_3741, 7, 90),
        (8_3742, 7, 40),
        (8_3743, 7, 90),
        (8_3744, 7, 20),
        (8_3745, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .limit(4)
            .plan()
            .expect("top_k_by(rank, 1) equivalence plan should build")
    };

    let (top_one, scanned_top) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by(build_plan(), "rank", 1)
                .expect("top_k_by(rank, 1) should succeed")
        });
    let (max_id, scanned_max) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_max_by(build_plan(), "rank")
            .expect("max_by(rank) should succeed")
    });
    let top_one_ids = top_one.ids();
    let expected_max_ids: Vec<Id<PushdownParityEntity>> = max_id.into_iter().collect();

    assert_eq!(
        top_one_ids, expected_max_ids,
        "top_k_by(rank, 1) should match max_by(rank) over the same effective response window"
    );
    assert_eq!(
        top_one_ids.first().map(Id::key),
        Some(Ulid::from_u128(8_3741)),
        "top_k_by(rank, 1) should preserve deterministic pk-ascending tie-breaks"
    );
    assert!(
        scanned_top >= scanned_max,
        "top_k_by(rank, 1) may scan equal or more rows than max_by(rank), but must not scan fewer"
    );
}

#[test]
fn aggregate_field_target_top_k_by_values_k_one_matches_max_by_projection_with_ties() {
    seed_pushdown_entities(&[
        (8_3811, 7, 90),
        (8_3812, 7, 40),
        (8_3813, 7, 90),
        (8_3814, 7, 20),
        (8_3815, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .limit(4)
            .plan()
            .expect("top_k_by_values(rank, 1) equivalence plan should build")
    };

    let (top_one_values, scanned_top_values) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_values(build_plan(), "rank", 1)
                .expect("top_k_by_values(rank, 1) should succeed")
        });
    let (max_id, scanned_max) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_max_by(build_plan(), "rank")
            .expect("max_by(rank) should succeed")
    });
    let expected_top_values = max_id
        .and_then(|target_id| {
            load.execute(build_plan())
                .expect("execute baseline for top_k_by_values(rank, 1) should succeed")
                .0
                .into_iter()
                .find(|(id, _)| *id == target_id)
                .map(|(_, entity)| Value::Uint(u64::from(entity.rank)))
        })
        .into_iter()
        .collect::<Vec<_>>();

    assert_eq!(
        top_one_values, expected_top_values,
        "top_k_by_values(rank, 1) should match max_by(rank) projected value over the same effective response window"
    );
    assert!(
        scanned_top_values >= scanned_max,
        "top_k_by_values(rank, 1) may scan equal or more rows than max_by(rank), but must not scan fewer"
    );
}

#[test]
fn aggregate_field_target_bottom_k_by_k_one_matches_min_by_ids_with_ties() {
    seed_pushdown_entities(&[
        (8_3751, 7, 10),
        (8_3752, 7, 30),
        (8_3753, 7, 10),
        (8_3754, 7, 40),
        (8_3755, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .limit(4)
            .plan()
            .expect("bottom_k_by(rank, 1) equivalence plan should build")
    };

    let (bottom_one, scanned_bottom) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by(build_plan(), "rank", 1)
                .expect("bottom_k_by(rank, 1) should succeed")
        });
    let (min_id, scanned_min) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_min_by(build_plan(), "rank")
            .expect("min_by(rank) should succeed")
    });
    let bottom_one_ids = bottom_one.ids();
    let expected_min_ids: Vec<Id<PushdownParityEntity>> = min_id.into_iter().collect();

    assert_eq!(
        bottom_one_ids, expected_min_ids,
        "bottom_k_by(rank, 1) should match min_by(rank) over the same effective response window"
    );
    assert_eq!(
        bottom_one_ids.first().map(Id::key),
        Some(Ulid::from_u128(8_3751)),
        "bottom_k_by(rank, 1) should preserve deterministic pk-ascending tie-breaks"
    );
    assert!(
        scanned_bottom >= scanned_min,
        "bottom_k_by(rank, 1) may scan equal or more rows than min_by(rank), but must not scan fewer"
    );
}

#[test]
fn aggregate_field_target_bottom_k_by_values_k_one_matches_min_by_projection_with_ties() {
    seed_pushdown_entities(&[
        (8_3821, 7, 10),
        (8_3822, 7, 30),
        (8_3823, 7, 10),
        (8_3824, 7, 40),
        (8_3825, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .limit(4)
            .plan()
            .expect("bottom_k_by_values(rank, 1) equivalence plan should build")
    };

    let (bottom_one_values, scanned_bottom_values) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_values(build_plan(), "rank", 1)
                .expect("bottom_k_by_values(rank, 1) should succeed")
        });
    let (min_id, scanned_min) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_min_by(build_plan(), "rank")
            .expect("min_by(rank) should succeed")
    });
    let expected_bottom_values = min_id
        .and_then(|target_id| {
            load.execute(build_plan())
                .expect("execute baseline for bottom_k_by_values(rank, 1) should succeed")
                .0
                .into_iter()
                .find(|(id, _)| *id == target_id)
                .map(|(_, entity)| Value::Uint(u64::from(entity.rank)))
        })
        .into_iter()
        .collect::<Vec<_>>();

    assert_eq!(
        bottom_one_values, expected_bottom_values,
        "bottom_k_by_values(rank, 1) should match min_by(rank) projected value over the same effective response window"
    );
    assert!(
        scanned_bottom_values >= scanned_min,
        "bottom_k_by_values(rank, 1) may scan equal or more rows than min_by(rank), but must not scan fewer"
    );
}

#[test]
fn aggregate_field_target_top_k_by_with_ids_k_one_matches_max_by_projection_with_ties() {
    seed_pushdown_entities(&[
        (8_3831, 7, 90),
        (8_3832, 7, 40),
        (8_3833, 7, 90),
        (8_3834, 7, 20),
        (8_3835, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .limit(4)
            .plan()
            .expect("top_k_by_with_ids(rank, 1) equivalence plan should build")
    };

    let (top_one_values_with_ids, scanned_top_values_with_ids) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_with_ids(build_plan(), "rank", 1)
                .expect("top_k_by_with_ids(rank, 1) should succeed")
        });
    let (max_id, scanned_max) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_max_by(build_plan(), "rank")
            .expect("max_by(rank) should succeed")
    });
    let expected_top_values_with_ids = max_id
        .and_then(|target_id| {
            load.execute(build_plan())
                .expect("execute baseline for top_k_by_with_ids(rank, 1) should succeed")
                .0
                .into_iter()
                .find(|(id, _)| *id == target_id)
                .map(|(_, entity)| (target_id, Value::Uint(u64::from(entity.rank))))
        })
        .into_iter()
        .collect::<Vec<_>>();

    assert_eq!(
        top_one_values_with_ids, expected_top_values_with_ids,
        "top_k_by_with_ids(rank, 1) should match max_by(rank) projected id/value pair over the same effective response window"
    );
    assert!(
        scanned_top_values_with_ids >= scanned_max,
        "top_k_by_with_ids(rank, 1) may scan equal or more rows than max_by(rank), but must not scan fewer"
    );
}

#[test]
fn aggregate_field_target_bottom_k_by_with_ids_k_one_matches_min_by_projection_with_ties() {
    seed_pushdown_entities(&[
        (8_3836, 7, 10),
        (8_3837, 7, 30),
        (8_3838, 7, 10),
        (8_3839, 7, 40),
        (8_3840, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .limit(4)
            .plan()
            .expect("bottom_k_by_with_ids(rank, 1) equivalence plan should build")
    };

    let (bottom_one_values_with_ids, scanned_bottom_values_with_ids) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_with_ids(build_plan(), "rank", 1)
                .expect("bottom_k_by_with_ids(rank, 1) should succeed")
        });
    let (min_id, scanned_min) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_min_by(build_plan(), "rank")
            .expect("min_by(rank) should succeed")
    });
    let expected_bottom_values_with_ids = min_id
        .and_then(|target_id| {
            load.execute(build_plan())
                .expect("execute baseline for bottom_k_by_with_ids(rank, 1) should succeed")
                .0
                .into_iter()
                .find(|(id, _)| *id == target_id)
                .map(|(_, entity)| (target_id, Value::Uint(u64::from(entity.rank))))
        })
        .into_iter()
        .collect::<Vec<_>>();

    assert_eq!(
        bottom_one_values_with_ids, expected_bottom_values_with_ids,
        "bottom_k_by_with_ids(rank, 1) should match min_by(rank) projected id/value pair over the same effective response window"
    );
    assert!(
        scanned_bottom_values_with_ids >= scanned_min,
        "bottom_k_by_with_ids(rank, 1) may scan equal or more rows than min_by(rank), but must not scan fewer"
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn aggregate_field_target_take_and_rank_terminals_k_zero_return_empty_with_execute_scan_parity() {
    seed_pushdown_entities(&[
        (8_3761, 7, 10),
        (8_3762, 7, 20),
        (8_3763, 7, 30),
        (8_3764, 7, 40),
        (8_3765, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(3)
            .plan()
            .expect("k-zero terminal plan should build")
    };

    let (_, scanned_execute) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.execute(build_plan())
            .expect("execute baseline for k-zero terminal parity should succeed")
    });
    let (take_zero, scanned_take_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.take(build_plan(), 0)
                .expect("take(0) should succeed and return an empty response")
        });
    let (top_k_zero, scanned_top_k_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by(build_plan(), "rank", 0)
                .expect("top_k_by(rank, 0) should succeed and return an empty response")
        });
    let (bottom_k_zero, scanned_bottom_k_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by(build_plan(), "rank", 0)
                .expect("bottom_k_by(rank, 0) should succeed and return an empty response")
        });
    let (top_k_values_zero, scanned_top_k_values_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_values(build_plan(), "rank", 0)
                .expect("top_k_by_values(rank, 0) should succeed and return an empty response")
        });
    let (bottom_k_values_zero, scanned_bottom_k_values_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_values(build_plan(), "rank", 0)
                .expect("bottom_k_by_values(rank, 0) should succeed and return an empty response")
        });
    let (top_k_with_ids_zero, scanned_top_k_with_ids_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_with_ids(build_plan(), "rank", 0)
                .expect("top_k_by_with_ids(rank, 0) should succeed and return an empty response")
        });
    let (bottom_k_with_ids_zero, scanned_bottom_k_with_ids_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_with_ids(build_plan(), "rank", 0)
                .expect("bottom_k_by_with_ids(rank, 0) should succeed and return an empty response")
        });

    assert!(
        take_zero.is_empty(),
        "take(0) should return an empty response"
    );
    assert!(
        top_k_zero.is_empty(),
        "top_k_by(rank, 0) should return an empty response"
    );
    assert!(
        bottom_k_zero.is_empty(),
        "bottom_k_by(rank, 0) should return an empty response"
    );
    assert!(
        top_k_values_zero.is_empty(),
        "top_k_by_values(rank, 0) should return an empty response"
    );
    assert!(
        bottom_k_values_zero.is_empty(),
        "bottom_k_by_values(rank, 0) should return an empty response"
    );
    assert!(
        top_k_with_ids_zero.is_empty(),
        "top_k_by_with_ids(rank, 0) should return an empty response"
    );
    assert!(
        bottom_k_with_ids_zero.is_empty(),
        "bottom_k_by_with_ids(rank, 0) should return an empty response"
    );
    assert_eq!(
        scanned_take_zero, scanned_execute,
        "take(0) should preserve execute() scan-budget consumption before truncation"
    );
    assert_eq!(
        scanned_top_k_zero, scanned_execute,
        "top_k_by(rank, 0) should preserve execute() scan-budget consumption before truncation"
    );
    assert_eq!(
        scanned_bottom_k_zero, scanned_execute,
        "bottom_k_by(rank, 0) should preserve execute() scan-budget consumption before truncation"
    );
    assert_eq!(
        scanned_top_k_values_zero, scanned_execute,
        "top_k_by_values(rank, 0) should preserve execute() scan-budget consumption before truncation"
    );
    assert_eq!(
        scanned_bottom_k_values_zero, scanned_execute,
        "bottom_k_by_values(rank, 0) should preserve execute() scan-budget consumption before truncation"
    );
    assert_eq!(
        scanned_top_k_with_ids_zero, scanned_execute,
        "top_k_by_with_ids(rank, 0) should preserve execute() scan-budget consumption before truncation"
    );
    assert_eq!(
        scanned_bottom_k_with_ids_zero, scanned_execute,
        "bottom_k_by_with_ids(rank, 0) should preserve execute() scan-budget consumption before truncation"
    );
}

#[test]
fn session_load_values_by_with_ids_matches_execute_projection() {
    seed_pushdown_entities(&[
        (8_3311, 7, 10),
        (8_3312, 7, 10),
        (8_3313, 7, 20),
        (8_3314, 7, 30),
        (8_3315, 7, 40),
        (8_3316, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for values_by_with_ids should succeed");
    let actual = load_window()
        .values_by_with_ids("rank")
        .expect("session values_by_with_ids(rank) should succeed");

    assert_eq!(
        actual,
        expected_values_by_rank_with_ids(&expected),
        "session values_by_with_ids(rank) parity failed"
    );
}

#[test]
fn session_load_distinct_values_by_matches_execute_projection() {
    seed_pushdown_entities(&[
        (8_341, 7, 10),
        (8_342, 7, 10),
        (8_343, 7, 20),
        (8_344, 7, 30),
        (8_345, 7, 20),
        (8_346, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for distinct_values_by should succeed");
    let actual = load_window()
        .distinct_values_by("rank")
        .expect("session distinct_values_by(rank) should succeed");

    assert_eq!(
        actual,
        expected_distinct_values_by_rank(&expected),
        "session distinct_values_by(rank) parity failed"
    );
}

#[test]
fn session_load_distinct_values_by_matches_values_by_first_observed_dedup() {
    seed_pushdown_entities(&[
        (8_341, 7, 10),
        (8_342, 7, 10),
        (8_343, 7, 20),
        (8_344, 7, 30),
        (8_345, 7, 20),
        (8_346, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    let values = load_window()
        .values_by("rank")
        .expect("session values_by(rank) should succeed");
    let distinct_values = load_window()
        .distinct_values_by("rank")
        .expect("session distinct_values_by(rank) should succeed");

    let mut expected_distinct_from_values = Vec::new();
    for value in &values {
        if expected_distinct_from_values
            .iter()
            .any(|existing| existing == value)
        {
            continue;
        }
        expected_distinct_from_values.push(value.clone());
    }

    assert!(
        values.len() >= distinct_values.len(),
        "session values_by(field).len() must be >= distinct_values_by(field).len()"
    );
    assert_eq!(
        distinct_values, expected_distinct_from_values,
        "session distinct_values_by(field) must equal values_by(field) deduped by first occurrence"
    );
}

#[test]
fn session_load_terminal_value_projection_matches_execute_projection() {
    seed_pushdown_entities(&[
        (8_3511, 7, 10),
        (8_3512, 7, 10),
        (8_3513, 7, 20),
        (8_3514, 7, 30),
        (8_3515, 7, 40),
        (8_3516, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for terminal value projection should succeed");
    let actual_first = load_window()
        .first_value_by("rank")
        .expect("session first_value_by(rank) should succeed");
    let actual_last = load_window()
        .last_value_by("rank")
        .expect("session last_value_by(rank) should succeed");

    assert_eq!(
        actual_first,
        expected_first_value_by_rank(&expected),
        "session first_value_by(rank) parity failed"
    );
    assert_eq!(
        actual_last,
        expected_last_value_by_rank(&expected),
        "session last_value_by(rank) parity failed"
    );
}

#[test]
fn aggregate_field_target_values_by_preserves_scan_budget_parity_with_execute() {
    seed_pushdown_entities(&[
        (8_331, 7, 10),
        (8_332, 7, 10),
        (8_333, 7, 20),
        (8_334, 7, 30),
        (8_335, 7, 40),
        (8_336, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
            .plan()
            .expect("values_by scan-budget parity plan should build")
    };

    let (_, scanned_execute) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.execute(build_plan())
            .expect("execute baseline for values_by scan-budget parity should succeed")
    });
    let (_, scanned_values_by) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.values_by(build_plan(), "rank")
                .expect("values_by(rank) should succeed")
        });

    assert_eq!(
        scanned_values_by, scanned_execute,
        "values_by must preserve scan-budget consumption parity with execute()"
    );
}

#[test]
fn aggregate_field_target_distinct_values_by_preserves_scan_budget_parity_with_execute() {
    seed_pushdown_entities(&[
        (8_351, 7, 10),
        (8_352, 7, 10),
        (8_353, 7, 20),
        (8_354, 7, 30),
        (8_355, 7, 20),
        (8_356, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
            .plan()
            .expect("distinct_values_by scan-budget parity plan should build")
    };

    let (_, scanned_execute) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.execute(build_plan())
            .expect("execute baseline for distinct_values_by scan-budget parity should succeed")
    });
    let (_, scanned_distinct_values_by) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.distinct_values_by(build_plan(), "rank")
                .expect("distinct_values_by(rank) should succeed")
        });

    assert_eq!(
        scanned_distinct_values_by, scanned_execute,
        "distinct_values_by must preserve scan-budget consumption parity with execute()"
    );
}

#[test]
fn aggregate_field_target_values_by_with_ids_preserves_scan_budget_parity_with_execute() {
    seed_pushdown_entities(&[
        (8_361, 7, 10),
        (8_362, 7, 10),
        (8_363, 7, 20),
        (8_364, 7, 30),
        (8_365, 7, 20),
        (8_366, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
            .plan()
            .expect("values_by_with_ids scan-budget parity plan should build")
    };

    let (_, scanned_execute) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.execute(build_plan())
            .expect("execute baseline for values_by_with_ids scan-budget parity should succeed")
    });
    let (_, scanned_values_by_with_ids) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.values_by_with_ids(build_plan(), "rank")
                .expect("values_by_with_ids(rank) should succeed")
        });

    assert_eq!(
        scanned_values_by_with_ids, scanned_execute,
        "values_by_with_ids must preserve scan-budget consumption parity with execute()"
    );
}

#[test]
fn aggregate_field_target_top_k_by_uses_bounded_execute_window_and_scan_budget_parity() {
    seed_pushdown_entities(&[
        (8_3811, 7, 10),
        (8_3812, 7, 20),
        (8_3813, 7, 30),
        (8_3814, 7, 100),
        (8_3815, 7, 90),
        (8_3816, 7, 80),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_bounded_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by("id")
            .limit(3)
            .plan()
            .expect("top_k_by bounded-window scan-budget parity plan should build")
    };
    let build_unbounded_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by("id")
            .plan()
            .expect("top_k_by unbounded-window plan should build")
    };

    let (bounded_execute, scanned_execute) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.execute(build_bounded_plan())
                .expect("top_k_by bounded-window execute baseline should succeed")
        });
    let (bounded_top_k, scanned_top_k) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by(build_bounded_plan(), "rank", 2)
                .expect("top_k_by(rank, 2) bounded-window query should succeed")
        });
    let unbounded_top_k = load
        .top_k_by(build_unbounded_plan(), "rank", 2)
        .expect("top_k_by(rank, 2) unbounded-window query should succeed");
    let mut expected_rank_order = bounded_execute
        .0
        .iter()
        .map(|(id, entity)| (entity.rank, *id))
        .collect::<Vec<_>>();
    expected_rank_order.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        right_rank
            .cmp(left_rank)
            .then_with(|| left_id.key().cmp(&right_id.key()))
    });
    let expected_bounded_top_ids: Vec<Id<PushdownParityEntity>> = expected_rank_order
        .into_iter()
        .take(2)
        .map(|(_, id)| id)
        .collect();

    assert_eq!(
        bounded_top_k.ids(),
        expected_bounded_top_ids,
        "top_k_by(rank, 2) should rank only within the bounded effective execute() window"
    );
    assert_eq!(
        scanned_top_k, scanned_execute,
        "top_k_by must preserve scan-budget consumption parity with execute() for bounded windows"
    );
    assert_ne!(
        bounded_top_k.ids(),
        unbounded_top_k.ids(),
        "top_k_by bounded-window behavior should differ from unbounded query behavior on the same dataset"
    );
}

#[test]
fn aggregate_field_target_bottom_k_by_uses_bounded_execute_window_and_scan_budget_parity() {
    seed_pushdown_entities(&[
        (8_3821, 7, 100),
        (8_3822, 7, 90),
        (8_3823, 7, 80),
        (8_3824, 7, 10),
        (8_3825, 7, 20),
        (8_3826, 7, 30),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_bounded_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by("id")
            .limit(3)
            .plan()
            .expect("bottom_k_by bounded-window scan-budget parity plan should build")
    };
    let build_unbounded_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by("id")
            .plan()
            .expect("bottom_k_by unbounded-window plan should build")
    };

    let (bounded_execute, scanned_execute) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.execute(build_bounded_plan())
                .expect("bottom_k_by bounded-window execute baseline should succeed")
        });
    let (bounded_bottom_k, scanned_bottom_k) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by(build_bounded_plan(), "rank", 2)
                .expect("bottom_k_by(rank, 2) bounded-window query should succeed")
        });
    let unbounded_bottom_k = load
        .bottom_k_by(build_unbounded_plan(), "rank", 2)
        .expect("bottom_k_by(rank, 2) unbounded-window query should succeed");
    let mut expected_rank_order = bounded_execute
        .0
        .iter()
        .map(|(id, entity)| (entity.rank, *id))
        .collect::<Vec<_>>();
    expected_rank_order.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        left_rank
            .cmp(right_rank)
            .then_with(|| left_id.key().cmp(&right_id.key()))
    });
    let expected_bounded_bottom_ids: Vec<Id<PushdownParityEntity>> = expected_rank_order
        .into_iter()
        .take(2)
        .map(|(_, id)| id)
        .collect();

    assert_eq!(
        bounded_bottom_k.ids(),
        expected_bounded_bottom_ids,
        "bottom_k_by(rank, 2) should rank only within the bounded effective execute() window"
    );
    assert_eq!(
        scanned_bottom_k, scanned_execute,
        "bottom_k_by must preserve scan-budget consumption parity with execute() for bounded windows"
    );
    assert_ne!(
        bounded_bottom_k.ids(),
        unbounded_bottom_k.ids(),
        "bottom_k_by bounded-window behavior should differ from unbounded query behavior on the same dataset"
    );
}

#[test]
fn aggregate_field_target_top_k_by_forced_full_scan_and_index_range_match_execute_oracle() {
    seed_simple_entities(&[8_3901, 8_3902, 8_3903, 8_3904, 8_3905, 8_3906]);
    let simple_load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let build_full_scan_plan = || {
        Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .offset(1)
            .limit(4)
            .plan()
            .expect("top_k_by full-scan plan should build")
    };
    let full_scan_plan = build_full_scan_plan();
    assert!(
        matches!(full_scan_plan.explain().access, ExplainAccessPath::FullScan),
        "top_k_by full-scan test must force a FullScan access shape"
    );

    let full_scan_execute = simple_load
        .execute(build_full_scan_plan())
        .expect("top_k_by full-scan execute baseline should succeed");
    let full_scan_top = simple_load
        .top_k_by(build_full_scan_plan(), "id", 2)
        .expect("top_k_by(id, 2) should succeed for full-scan shape");
    let mut expected_full_scan_top_ids = full_scan_execute.ids();
    expected_full_scan_top_ids.sort_unstable_by_key(|right| std::cmp::Reverse(right.key()));
    expected_full_scan_top_ids.truncate(2);
    assert_eq!(
        full_scan_top.ids(),
        expected_full_scan_top_ids,
        "top_k_by(id, 2) should match execute() oracle under forced FullScan"
    );

    seed_unique_index_range_entities(&[
        (8_3911, 100),
        (8_3912, 101),
        (8_3913, 102),
        (8_3914, 103),
        (8_3915, 104),
        (8_3916, 105),
    ]);
    let range_load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let code_range = u32_range_predicate("code", 101, 106);
    let build_index_range_plan = || {
        Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
            .filter(code_range.clone())
            .order_by_desc("code")
            .offset(1)
            .limit(3)
            .plan()
            .expect("top_k_by index-range plan should build")
    };
    let index_range_plan = build_index_range_plan();
    assert!(
        matches!(
            index_range_plan.explain().access,
            ExplainAccessPath::IndexRange { .. }
        ),
        "top_k_by index-range test must force an IndexRange access shape"
    );

    let index_range_execute = range_load
        .execute(build_index_range_plan())
        .expect("top_k_by index-range execute baseline should succeed");
    let index_range_top = range_load
        .top_k_by(build_index_range_plan(), "code", 2)
        .expect("top_k_by(code, 2) should succeed for index-range shape");
    let mut expected_index_range_top_ids = index_range_execute
        .0
        .iter()
        .map(|(id, entity)| (entity.code, *id))
        .collect::<Vec<_>>();
    expected_index_range_top_ids.sort_unstable_by(
        |(left_code, left_id), (right_code, right_id)| {
            right_code
                .cmp(left_code)
                .then_with(|| left_id.key().cmp(&right_id.key()))
        },
    );
    let expected_index_range_top_ids: Vec<Id<UniqueIndexRangeEntity>> =
        expected_index_range_top_ids
            .into_iter()
            .take(2)
            .map(|(_, id)| id)
            .collect();
    assert_eq!(
        index_range_top.ids(),
        expected_index_range_top_ids,
        "top_k_by(code, 2) should match execute() oracle under forced IndexRange"
    );
}

#[test]
fn aggregate_field_target_bottom_k_by_forced_full_scan_and_index_range_match_execute_oracle() {
    seed_simple_entities(&[8_3921, 8_3922, 8_3923, 8_3924, 8_3925, 8_3926]);
    let simple_load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let build_full_scan_plan = || {
        Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .offset(1)
            .limit(4)
            .plan()
            .expect("bottom_k_by full-scan plan should build")
    };
    let full_scan_plan = build_full_scan_plan();
    assert!(
        matches!(full_scan_plan.explain().access, ExplainAccessPath::FullScan),
        "bottom_k_by full-scan test must force a FullScan access shape"
    );

    let full_scan_execute = simple_load
        .execute(build_full_scan_plan())
        .expect("bottom_k_by full-scan execute baseline should succeed");
    let full_scan_bottom = simple_load
        .bottom_k_by(build_full_scan_plan(), "id", 2)
        .expect("bottom_k_by(id, 2) should succeed for full-scan shape");
    let mut expected_full_scan_bottom_ids = full_scan_execute.ids();
    expected_full_scan_bottom_ids.sort_unstable_by_key(Id::key);
    expected_full_scan_bottom_ids.truncate(2);
    assert_eq!(
        full_scan_bottom.ids(),
        expected_full_scan_bottom_ids,
        "bottom_k_by(id, 2) should match execute() oracle under forced FullScan"
    );

    seed_unique_index_range_entities(&[
        (8_3931, 100),
        (8_3932, 101),
        (8_3933, 102),
        (8_3934, 103),
        (8_3935, 104),
        (8_3936, 105),
    ]);
    let range_load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let code_range = u32_range_predicate("code", 101, 106);
    let build_index_range_plan = || {
        Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
            .filter(code_range.clone())
            .order_by_desc("code")
            .offset(1)
            .limit(3)
            .plan()
            .expect("bottom_k_by index-range plan should build")
    };
    let index_range_plan = build_index_range_plan();
    assert!(
        matches!(
            index_range_plan.explain().access,
            ExplainAccessPath::IndexRange { .. }
        ),
        "bottom_k_by index-range test must force an IndexRange access shape"
    );

    let index_range_execute = range_load
        .execute(build_index_range_plan())
        .expect("bottom_k_by index-range execute baseline should succeed");
    let index_range_bottom = range_load
        .bottom_k_by(build_index_range_plan(), "code", 2)
        .expect("bottom_k_by(code, 2) should succeed for index-range shape");
    let mut expected_index_range_bottom_ids = index_range_execute
        .0
        .iter()
        .map(|(id, entity)| (entity.code, *id))
        .collect::<Vec<_>>();
    expected_index_range_bottom_ids.sort_unstable_by(
        |(left_code, left_id), (right_code, right_id)| {
            left_code
                .cmp(right_code)
                .then_with(|| left_id.key().cmp(&right_id.key()))
        },
    );
    let expected_index_range_bottom_ids: Vec<Id<UniqueIndexRangeEntity>> =
        expected_index_range_bottom_ids
            .into_iter()
            .take(2)
            .map(|(_, id)| id)
            .collect();
    assert_eq!(
        index_range_bottom.ids(),
        expected_index_range_bottom_ids,
        "bottom_k_by(code, 2) should match execute() oracle under forced IndexRange"
    );
}

#[test]
fn aggregate_exists_desc_early_stop_matches_asc_scan_budget() {
    seed_simple_entities(&[9201, 9202, 9203, 9204, 9205, 9206]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (exists_asc, scanned_asc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_exists(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("exists ASC plan should build"),
        )
        .expect("exists ASC should succeed")
    });
    let (exists_desc, scanned_desc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_exists(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by_desc("id")
                .plan()
                .expect("exists DESC plan should build"),
        )
        .expect("exists DESC should succeed")
    });

    assert!(exists_asc, "exists ASC should find at least one row");
    assert!(exists_desc, "exists DESC should find at least one row");
    assert_eq!(
        scanned_asc, 1,
        "exists ASC should early-stop after first key"
    );
    assert_eq!(
        scanned_desc, 1,
        "exists DESC should early-stop after first key"
    );
}

#[test]
fn aggregate_extrema_first_row_short_circuit_is_direction_symmetric() {
    seed_simple_entities(&[9301, 9302, 9303, 9304, 9305, 9306]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (min_asc, scanned_min_asc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_min(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("min ASC plan should build"),
        )
        .expect("min ASC should succeed")
    });
    let (max_desc, scanned_max_desc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_max(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by_desc("id")
                .plan()
                .expect("max DESC plan should build"),
        )
        .expect("max DESC should succeed")
    });

    assert_eq!(min_asc.map(|id| id.key()), Some(Ulid::from_u128(9301)));
    assert_eq!(max_desc.map(|id| id.key()), Some(Ulid::from_u128(9306)));
    assert_eq!(
        scanned_min_asc, 1,
        "min ASC should early-stop after first in-window key"
    );
    assert_eq!(
        scanned_max_desc, 1,
        "max DESC should early-stop after first in-window key"
    );
}

#[test]
fn aggregate_extrema_offset_short_circuit_scans_offset_plus_one() {
    seed_simple_entities(&[9401, 9402, 9403, 9404, 9405, 9406, 9407]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (min_asc, scanned_min_asc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_min(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .offset(3)
                .plan()
                .expect("min ASC with offset plan should build"),
        )
        .expect("min ASC with offset should succeed")
    });
    let (max_desc, scanned_max_desc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_max(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by_desc("id")
                .offset(3)
                .plan()
                .expect("max DESC with offset plan should build"),
        )
        .expect("max DESC with offset should succeed")
    });

    assert_eq!(min_asc.map(|id| id.key()), Some(Ulid::from_u128(9404)));
    assert_eq!(max_desc.map(|id| id.key()), Some(Ulid::from_u128(9404)));
    assert_eq!(
        scanned_min_asc, 4,
        "min ASC should scan exactly offset + 1 keys"
    );
    assert_eq!(
        scanned_max_desc, 4,
        "max DESC should scan exactly offset + 1 keys"
    );
}

#[test]
fn aggregate_first_offset_short_circuit_scans_offset_plus_one() {
    seed_simple_entities(&[9451, 9452, 9453, 9454, 9455, 9456, 9457]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (first_asc, scanned_first_asc) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
            load.aggregate_first(
                Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                    .order_by("id")
                    .offset(3)
                    .plan()
                    .expect("first ASC with offset plan should build"),
            )
            .expect("first ASC with offset should succeed")
        });
    let (first_desc, scanned_first_desc) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
            load.aggregate_first(
                Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                    .order_by_desc("id")
                    .offset(3)
                    .plan()
                    .expect("first DESC with offset plan should build"),
            )
            .expect("first DESC with offset should succeed")
        });

    assert_eq!(first_asc.map(|id| id.key()), Some(Ulid::from_u128(9454)));
    assert_eq!(first_desc.map(|id| id.key()), Some(Ulid::from_u128(9454)));
    assert_eq!(
        scanned_first_asc, 4,
        "first ASC should scan exactly offset + 1 keys"
    );
    assert_eq!(
        scanned_first_desc, 4,
        "first DESC should scan exactly offset + 1 keys"
    );
}

#[test]
fn aggregate_last_limited_window_scans_offset_plus_limit() {
    seed_simple_entities(&[9461, 9462, 9463, 9464, 9465, 9466, 9467]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (last_asc, scanned_last_asc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_last(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .offset(2)
                .limit(3)
                .plan()
                .expect("last ASC with window plan should build"),
        )
        .expect("last ASC with window should succeed")
    });
    let (last_desc, scanned_last_desc) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
            load.aggregate_last(
                Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                    .order_by_desc("id")
                    .offset(2)
                    .limit(3)
                    .plan()
                    .expect("last DESC with window plan should build"),
            )
            .expect("last DESC with window should succeed")
        });

    assert_eq!(last_asc.map(|id| id.key()), Some(Ulid::from_u128(9465)));
    assert_eq!(last_desc.map(|id| id.key()), Some(Ulid::from_u128(9463)));
    assert_eq!(
        scanned_last_asc, 5,
        "last ASC should scan exactly offset + limit keys"
    );
    assert_eq!(
        scanned_last_desc, 5,
        "last DESC should scan exactly offset + limit keys"
    );
}

#[test]
fn aggregate_last_unbounded_window_scans_full_stream() {
    seed_simple_entities(&[9471, 9472, 9473, 9474, 9475, 9476, 9477]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (last_asc, scanned_last_asc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_last(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .offset(2)
                .plan()
                .expect("last ASC unbounded plan should build"),
        )
        .expect("last ASC unbounded should succeed")
    });
    let (last_desc, scanned_last_desc) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
            load.aggregate_last(
                Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                    .order_by_desc("id")
                    .offset(2)
                    .plan()
                    .expect("last DESC unbounded plan should build"),
            )
            .expect("last DESC unbounded should succeed")
        });

    assert_eq!(last_asc.map(|id| id.key()), Some(Ulid::from_u128(9477)));
    assert_eq!(last_desc.map(|id| id.key()), Some(Ulid::from_u128(9471)));
    assert_eq!(
        scanned_last_asc, 7,
        "last ASC without limit should scan the full stream"
    );
    assert_eq!(
        scanned_last_desc, 7,
        "last DESC without limit should scan the full stream"
    );
}

#[test]
fn aggregate_last_unbounded_desc_large_dataset_scans_full_stream() {
    let ids: Vec<u128> = (0u128..128u128)
        .map(|i| 9701u128.saturating_add(i))
        .collect();
    seed_simple_entities(&ids);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (last_desc, scanned_last_desc) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
            load.aggregate_last(
                Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                    .order_by_desc("id")
                    .plan()
                    .expect("last DESC large unbounded plan should build"),
            )
            .expect("last DESC large unbounded should succeed")
        });

    assert_eq!(
        last_desc.map(|id| id.key()),
        Some(Ulid::from_u128(9701)),
        "last DESC should return the last id in descending response order"
    );
    assert_eq!(
        scanned_last_desc, 128,
        "last DESC without limit should scan the full stream for large datasets"
    );
}

#[test]
fn aggregate_last_secondary_index_desc_mixed_direction_falls_back_safely() {
    let mut rows = Vec::new();
    for i in 0u32..64u32 {
        rows.push((
            9801u128.saturating_add(u128::from(i)),
            if i % 2 == 0 { 7 } else { 8 },
            i,
        ));
    }
    seed_pushdown_entities(rows.as_slice());
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    let (last_desc, scanned_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_last(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(group_seven.clone())
                    .order_by_desc("rank")
                    .plan()
                    .expect("secondary last DESC unbounded plan should build"),
            )
            .expect("secondary last DESC unbounded should succeed")
        });

    assert_eq!(
        last_desc.map(|id| id.key()),
        Some(Ulid::from_u128(9801)),
        "secondary last DESC should return the final row in descending rank order"
    );
    assert_eq!(
        scanned_desc, 64,
        "mixed-direction secondary order should reject pushdown and fall back without under-scanning"
    );
}

#[test]
fn aggregate_last_index_range_ineligible_pushdown_shape_preserves_parity() {
    seed_unique_index_range_entities(&[
        (9811, 200),
        (9812, 201),
        (9813, 202),
        (9814, 203),
        (9815, 204),
        (9816, 205),
    ]);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let range_predicate = u32_range_predicate("code", 201, 206);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
                .filter(range_predicate.clone())
                .order_by("label")
                .offset(1)
                .limit(2)
        },
        "index-range ineligible pushdown shape",
    );
}

#[test]
fn aggregate_first_and_last_respect_requested_direction() {
    seed_simple_entities(&[9481, 9482, 9483, 9484, 9485]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let first_asc = load
        .aggregate_first(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("first ASC plan should build"),
        )
        .expect("first ASC should succeed");
    let first_desc = load
        .aggregate_first(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by_desc("id")
                .plan()
                .expect("first DESC plan should build"),
        )
        .expect("first DESC should succeed");
    let last_asc = load
        .aggregate_last(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("last ASC plan should build"),
        )
        .expect("last ASC should succeed");
    let last_desc = load
        .aggregate_last(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by_desc("id")
                .plan()
                .expect("last DESC plan should build"),
        )
        .expect("last DESC should succeed");

    assert_eq!(first_asc.map(|id| id.key()), Some(Ulid::from_u128(9481)));
    assert_eq!(first_desc.map(|id| id.key()), Some(Ulid::from_u128(9485)));
    assert_eq!(last_asc.map(|id| id.key()), Some(Ulid::from_u128(9485)));
    assert_eq!(last_desc.map(|id| id.key()), Some(Ulid::from_u128(9481)));
}

#[test]
fn aggregate_distinct_offset_probe_hint_suppression_preserves_parity() {
    seed_simple_entities(&[9501, 9502]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let duplicate_front_predicate = Predicate::Or(vec![
        id_in_predicate(&[9501]),
        id_in_predicate(&[9501, 9502]),
    ]);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(duplicate_front_predicate.clone())
                .distinct()
                .order_by("id")
                .offset(1)
        },
        "distinct + offset probe-hint suppression",
    );
}

#[test]
fn aggregate_count_distinct_offset_window_disables_bounded_probe_hint() {
    seed_simple_entities(&[9511, 9512, 9513, 9514, 9515, 9516, 9517]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count_asc, scanned_asc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .distinct()
                .order_by("id")
                .offset(2)
                .limit(2)
                .plan()
                .expect("count distinct+offset ASC plan should build"),
        )
        .expect("count distinct+offset ASC should succeed")
    });
    let (count_desc, scanned_desc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .distinct()
                .order_by_desc("id")
                .offset(2)
                .limit(2)
                .plan()
                .expect("count distinct+offset DESC plan should build"),
        )
        .expect("count distinct+offset DESC should succeed")
    });

    assert_eq!(
        count_asc, 2,
        "ASC distinct+offset count should respect window"
    );
    assert_eq!(
        count_desc, 2,
        "DESC distinct+offset count should respect window"
    );
    assert_eq!(
        scanned_asc, 7,
        "ASC distinct+offset count should stay unbounded at access phase"
    );
    assert_eq!(
        scanned_desc, 7,
        "DESC distinct+offset count should stay unbounded at access phase"
    );
}

#[test]
fn aggregate_secondary_index_strict_prefilter_preserves_parity_across_window_shapes() {
    let mut rows = Vec::new();
    for rank in 0u32..48u32 {
        rows.push((10_101u128.saturating_add(u128::from(rank)), 7, rank));
    }
    for rank in 0u32..24u32 {
        rows.push((10_301u128.saturating_add(u128::from(rank)), 8, rank));
    }
    seed_pushdown_entities(rows.as_slice());
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let strict_filter = Predicate::And(vec![
        u32_eq_predicate_strict("group", 7),
        u32_in_predicate_strict("rank", &[3, 7, 19, 23, 41]),
    ]);

    for (direction_desc, distinct) in [(false, false), (false, true), (true, false), (true, true)] {
        assert_aggregate_parity_for_query(
            &load,
            || {
                let mut query = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(strict_filter.clone());
                if distinct {
                    query = query.distinct();
                }
                if direction_desc {
                    query.order_by_desc("rank").offset(1).limit(3)
                } else {
                    query.order_by("rank").offset(1).limit(3)
                }
            },
            "secondary strict index-predicate prefilter parity",
        );
    }
}

#[test]
fn aggregate_secondary_index_strict_prefilter_reduces_scan_vs_uncertain_fallback() {
    let mut rows = Vec::new();
    for rank in 0u32..160u32 {
        rows.push((10_601u128.saturating_add(u128::from(rank)), 7, rank));
    }
    for rank in 0u32..40u32 {
        rows.push((10_901u128.saturating_add(u128::from(rank)), 8, rank));
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

    let (strict_exists, strict_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_exists(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(strict_filter.clone())
                    .order_by("rank")
                    .plan()
                    .expect("strict prefilter exists plan should build"),
            )
            .expect("strict prefilter exists should succeed")
        });
    let (fallback_exists, fallback_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_exists(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(widen_filter.clone())
                    .order_by("rank")
                    .plan()
                    .expect("uncertain fallback exists plan should build"),
            )
            .expect("uncertain fallback exists should succeed")
        });

    assert_eq!(
        strict_exists, fallback_exists,
        "strict prefilter and uncertain fallback should preserve EXISTS parity"
    );
    assert!(
        strict_scanned < fallback_scanned,
        "strict aggregate prefilter should scan fewer rows than uncertain materialized fallback"
    );
    assert!(
        strict_scanned <= 3,
        "strict aggregate prefilter should bound scans to matching index candidates"
    );
}

#[test]
fn aggregate_field_extrema_strict_prefilter_reduces_scan_vs_uncertain_fallback() {
    let mut rows = Vec::new();
    for rank in 0u32..160u32 {
        rows.push((11_001u128.saturating_add(u128::from(rank)), 7, rank));
    }
    for rank in 0u32..40u32 {
        rows.push((11_401u128.saturating_add(u128::from(rank)), 8, rank));
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

    let (strict_min_by, strict_min_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min_by(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(strict_filter.clone())
                    .order_by("rank")
                    .plan()
                    .expect("strict prefilter min_by plan should build"),
                "rank",
            )
            .expect("strict prefilter min_by should succeed")
        });
    let (fallback_min_by, fallback_min_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min_by(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(widen_filter.clone())
                    .order_by("rank")
                    .plan()
                    .expect("uncertain fallback min_by plan should build"),
                "rank",
            )
            .expect("uncertain fallback min_by should succeed")
        });
    assert_eq!(
        strict_min_by, fallback_min_by,
        "strict prefilter and uncertain fallback should preserve min_by parity"
    );
    assert!(
        strict_min_scanned < fallback_min_scanned,
        "strict field-extrema prefilter should scan fewer rows than uncertain materialized fallback"
    );

    let (strict_max_by, strict_max_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max_by(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(strict_filter.clone())
                    .order_by_desc("rank")
                    .plan()
                    .expect("strict prefilter max_by plan should build"),
                "rank",
            )
            .expect("strict prefilter max_by should succeed")
        });
    let (fallback_max_by, fallback_max_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max_by(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(widen_filter.clone())
                    .order_by_desc("rank")
                    .plan()
                    .expect("uncertain fallback max_by plan should build"),
                "rank",
            )
            .expect("uncertain fallback max_by should succeed")
        });
    assert_eq!(
        strict_max_by, fallback_max_by,
        "strict prefilter and uncertain fallback should preserve max_by parity"
    );
    assert!(
        strict_max_scanned < fallback_max_scanned,
        "strict field-extrema prefilter should scan fewer rows than uncertain materialized fallback"
    );
}

#[test]
fn aggregate_first_last_strict_prefilter_reduces_scan_vs_uncertain_fallback() {
    let mut rows = Vec::new();
    for rank in 0u32..160u32 {
        rows.push((11_801u128.saturating_add(u128::from(rank)), 7, rank));
    }
    for rank in 0u32..40u32 {
        rows.push((12_201u128.saturating_add(u128::from(rank)), 8, rank));
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

    let (strict_first, strict_first_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_first(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(strict_filter.clone())
                    .order_by("rank")
                    .plan()
                    .expect("strict prefilter first plan should build"),
            )
            .expect("strict prefilter first should succeed")
        });
    let (fallback_first, fallback_first_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_first(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(widen_filter.clone())
                    .order_by("rank")
                    .plan()
                    .expect("uncertain fallback first plan should build"),
            )
            .expect("uncertain fallback first should succeed")
        });
    assert_eq!(
        strict_first, fallback_first,
        "strict prefilter and uncertain fallback should preserve first parity"
    );
    assert!(
        strict_first_scanned < fallback_first_scanned,
        "strict first prefilter should scan fewer rows than uncertain materialized fallback"
    );

    let (strict_last, strict_last_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_last(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(strict_filter.clone())
                    .order_by("rank")
                    .plan()
                    .expect("strict prefilter last plan should build"),
            )
            .expect("strict prefilter last should succeed")
        });
    let (fallback_last, fallback_last_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_last(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(widen_filter.clone())
                    .order_by("rank")
                    .plan()
                    .expect("uncertain fallback last plan should build"),
            )
            .expect("uncertain fallback last should succeed")
        });
    assert_eq!(
        strict_last, fallback_last,
        "strict prefilter and uncertain fallback should preserve last parity"
    );
    assert!(
        strict_last_scanned < fallback_last_scanned,
        "strict last prefilter should scan fewer rows than uncertain materialized fallback"
    );
}

#[test]
fn aggregate_missing_ok_skips_leading_stale_secondary_keys_for_exists_min_max() {
    seed_pushdown_entities(&[
        (9601, 7, 10),
        (9602, 7, 20),
        (9603, 7, 30),
        (9604, 7, 40),
        (9605, 8, 50),
    ]);
    // Remove edge rows from primary data only, preserving index entries to
    // simulate stale leading secondary keys.
    remove_pushdown_row_data(9601);
    remove_pushdown_row_data(9604);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(group_seven.clone())
                .order_by("rank")
        },
        "MissingOk stale-leading ASC secondary path",
    );
    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(group_seven.clone())
                .order_by_desc("rank")
        },
        "MissingOk stale-leading DESC secondary path",
    );

    let (exists_asc, scanned_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_exists(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(group_seven.clone())
                    .order_by("rank")
                    .plan()
                    .expect("exists ASC stale-leading plan should build"),
            )
            .expect("exists ASC stale-leading should succeed")
        });
    let (exists_desc, scanned_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_exists(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(group_seven.clone())
                    .order_by_desc("rank")
                    .plan()
                    .expect("exists DESC stale-leading plan should build"),
            )
            .expect("exists DESC stale-leading should succeed")
        });

    assert!(
        exists_asc,
        "exists ASC should continue past stale leading key and find a row"
    );
    assert!(
        exists_desc,
        "exists DESC should continue past stale leading key and find a row"
    );
    assert!(
        scanned_asc >= 2,
        "exists ASC should scan beyond the first stale key"
    );
    assert!(
        scanned_desc >= 2,
        "exists DESC should scan beyond the first stale key"
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn aggregate_count_pushdown_contract_matrix_preserves_parity() {
    // Case A: full-scan ordered shape should be count-pushdown eligible.
    seed_simple_entities(&[9701, 9702, 9703, 9704, 9705]);
    let simple_load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let full_scan_query = || {
        Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .offset(1)
            .limit(2)
    };
    let full_scan_plan = full_scan_query()
        .plan()
        .expect("full-scan count matrix plan should build");
    assert!(
        ExecutionKernel::is_streaming_access_shape_safe::<SimpleEntity, _>(
            full_scan_plan.as_inner(),
        ),
        "full-scan matrix shape should be streaming-safe"
    );
    assert!(
        count_pushdown_contract_eligible(&full_scan_plan),
        "full-scan matrix shape should be count-pushdown eligible by contract"
    );
    assert_count_parity_for_query(&simple_load, full_scan_query, "count matrix full-scan");

    // Case B: residual-filter full-scan is access-supported but not streaming-safe.
    seed_phase_entities(&[(9801, 1), (9802, 2), (9803, 2), (9804, 3)]);
    let phase_load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let residual_filter_query = || {
        Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("rank", 2))
            .order_by("id")
    };
    let residual_filter_plan = residual_filter_query()
        .plan()
        .expect("residual-filter count matrix plan should build");
    assert!(
        !ExecutionKernel::is_streaming_access_shape_safe::<PhaseEntity, _>(
            residual_filter_plan.as_inner(),
        ),
        "residual-filter matrix shape should be streaming-unsafe"
    );
    assert!(
        explain_access_supports_count_pushdown(&residual_filter_plan.explain().access),
        "residual-filter matrix shape should still be access-supported for pushdown paths"
    );
    assert!(
        !count_pushdown_contract_eligible(&residual_filter_plan),
        "residual-filter matrix shape must not be count-pushdown eligible"
    );
    assert_count_parity_for_query(
        &phase_load,
        residual_filter_query,
        "count matrix residual-filter full-scan",
    );

    // Case C: secondary-order query with stale leading keys must remain ineligible
    // for count pushdown and preserve materialized count parity.
    seed_pushdown_entities(&[(9901, 7, 10), (9902, 7, 20), (9903, 7, 30), (9904, 7, 40)]);
    remove_pushdown_row_data(9901);
    let pushdown_load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let secondary_index_query = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
    };
    let secondary_index_plan = secondary_index_query()
        .plan()
        .expect("secondary-index count matrix plan should build");
    assert!(
        !count_pushdown_contract_eligible(&secondary_index_plan),
        "secondary-index matrix shape must not be count-pushdown eligible"
    );
    assert_count_parity_for_query(
        &pushdown_load,
        secondary_index_query,
        "count matrix secondary-index",
    );

    // Case D: composite (OR) shape must remain ineligible for count pushdown.
    seed_simple_entities(&[9951, 9952, 9953, 9954, 9955, 9956]);
    let composite_load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let composite_predicate = Predicate::Or(vec![
        id_in_predicate(&[9951, 9952, 9953, 9954]),
        id_in_predicate(&[9953, 9954, 9955, 9956]),
    ]);
    let composite_query = || {
        Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
            .filter(composite_predicate.clone())
            .order_by("id")
    };
    let composite_plan = composite_query()
        .plan()
        .expect("composite count matrix plan should build");
    assert!(
        matches!(
            composite_plan.explain().access,
            ExplainAccessPath::Union(_) | ExplainAccessPath::Intersection(_)
        ),
        "composite count matrix shape should compile to a composite access plan"
    );
    assert!(
        !count_pushdown_contract_eligible(&composite_plan),
        "composite count matrix shape must not be count-pushdown eligible"
    );
    assert_count_parity_for_query(
        &composite_load,
        composite_query,
        "count matrix composite OR",
    );
}

#[test]
fn desc_cursor_resume_sequence_matches_unbounded_execution() {
    seed_simple_entities(&[9971, 9972, 9973, 9974, 9975, 9976, 9977, 9978, 9979, 9980]);
    let session = DbSession::new(DB);

    let expected_desc_ids: Vec<Ulid> = session
        .load::<SimpleEntity>()
        .order_by_desc("id")
        .execute()
        .expect("unbounded DESC execute should succeed")
        .ids()
        .into_iter()
        .map(|id| id.key())
        .collect();

    let mut resumed_desc_ids = Vec::new();
    let mut cursor_token = None::<String>;
    loop {
        let mut paged_query = session.load::<SimpleEntity>().order_by_desc("id").limit(3);
        if let Some(token) = cursor_token.as_deref() {
            paged_query = paged_query.cursor(token);
        }

        let execution = paged_query
            .execute_paged()
            .expect("paged DESC execute should succeed");
        resumed_desc_ids.extend(execution.response().ids().into_iter().map(|id| id.key()));

        match execution.continuation_cursor() {
            Some(bytes) => {
                cursor_token = Some(crate::db::encode_cursor(bytes));
            }
            None => break,
        }
    }

    assert_eq!(
        resumed_desc_ids, expected_desc_ids,
        "DESC cursor resume sequence should match unbounded DESC execution exactly"
    );
    assert!(
        resumed_desc_ids
            .windows(2)
            .all(|window| window[0] > window[1]),
        "DESC cursor resume sequence should stay strictly descending without duplicates"
    );
}

#[test]
fn desc_cursor_resume_secondary_index_sequence_matches_unbounded_execution() {
    seed_pushdown_entities(&[
        (9981, 7, 40),
        (9982, 7, 30),
        (9983, 7, 30),
        (9984, 7, 20),
        (9985, 7, 20),
        (9986, 7, 10),
        (9987, 8, 50),
    ]);
    let session = DbSession::new(DB);
    let group_seven = u32_eq_predicate("group", 7);

    let expected_desc_ids: Vec<Ulid> = session
        .load::<PushdownParityEntity>()
        .filter(group_seven.clone())
        .order_by_desc("rank")
        .order_by_desc("id")
        .execute()
        .expect("unbounded DESC secondary-index execute should succeed")
        .ids()
        .into_iter()
        .map(|id| id.key())
        .collect();

    let mut resumed_desc_ids = Vec::new();
    let mut cursor_token = None::<String>;
    loop {
        let mut paged_query = session
            .load::<PushdownParityEntity>()
            .filter(group_seven.clone())
            .order_by_desc("rank")
            .order_by_desc("id")
            .limit(2);
        if let Some(token) = cursor_token.as_deref() {
            paged_query = paged_query.cursor(token);
        }

        let execution = paged_query
            .execute_paged()
            .expect("paged DESC secondary-index execute should succeed");
        resumed_desc_ids.extend(execution.response().ids().into_iter().map(|id| id.key()));

        match execution.continuation_cursor() {
            Some(bytes) => {
                cursor_token = Some(crate::db::encode_cursor(bytes));
            }
            None => break,
        }
    }

    assert_eq!(
        resumed_desc_ids, expected_desc_ids,
        "DESC secondary-index cursor resume sequence should match unbounded DESC execution"
    );
}

#[test]
fn desc_cursor_resume_index_range_sequence_matches_unbounded_execution() {
    seed_unique_index_range_entities(&[
        (9991, 200),
        (9992, 201),
        (9993, 202),
        (9994, 203),
        (9995, 204),
        (9996, 205),
    ]);
    let session = DbSession::new(DB);
    let range_predicate = u32_range_predicate("code", 201, 206);

    let expected_desc_ids: Vec<Ulid> = session
        .load::<UniqueIndexRangeEntity>()
        .filter(range_predicate.clone())
        .order_by_desc("code")
        .order_by_desc("id")
        .execute()
        .expect("unbounded DESC index-range execute should succeed")
        .ids()
        .into_iter()
        .map(|id| id.key())
        .collect();

    let mut resumed_desc_ids = Vec::new();
    let mut cursor_token = None::<String>;
    loop {
        let mut paged_query = session
            .load::<UniqueIndexRangeEntity>()
            .filter(range_predicate.clone())
            .order_by_desc("code")
            .order_by_desc("id")
            .limit(2);
        if let Some(token) = cursor_token.as_deref() {
            paged_query = paged_query.cursor(token);
        }

        let execution = paged_query
            .execute_paged()
            .expect("paged DESC index-range execute should succeed");
        resumed_desc_ids.extend(execution.response().ids().into_iter().map(|id| id.key()));

        match execution.continuation_cursor() {
            Some(bytes) => {
                cursor_token = Some(crate::db::encode_cursor(bytes));
            }
            None => break,
        }
    }

    assert_eq!(
        resumed_desc_ids, expected_desc_ids,
        "DESC index-range cursor resume sequence should match unbounded DESC execution"
    );
}
