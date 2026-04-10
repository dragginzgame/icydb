//! Module: db::executor::tests::aggregate_tail
//! Responsibility: live aggregate tail-terminal semantics and scan-budget contracts.
//! Does not own: aggregate path parity helpers or ranked field-target matrices.
//! Boundary: keeps terminal short-circuit and tail-window behavior local to the revived executor harness.

use super::*;
use crate::{
    db::{
        data::DataKey,
        executor::{ExecutablePlan, ScalarTerminalBoundaryRequest, aggregate::AggregateKind},
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::{
            explain::ExplainExecutionNodeType,
            intent::Query,
            plan::{FieldSlot as PlannedFieldSlot, OrderDirection},
        },
        response::EntityResponse,
    },
    error::InternalError,
    metrics::sink::{MetricsEvent, MetricsSink, with_metrics_sink},
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
    types::{Id, Ulid},
    value::Value,
};
use std::cell::RefCell;

///
/// AggregateTailCaptureSink
///
/// Small metrics sink used to keep tail-terminal scan-budget assertions live
/// while the old aggregate tail matrix is drained into owner-local tests.
///

#[derive(Default)]
struct AggregateTailCaptureSink {
    events: RefCell<Vec<MetricsEvent>>,
}

impl AggregateTailCaptureSink {
    fn into_events(self) -> Vec<MetricsEvent> {
        self.events.into_inner()
    }
}

impl MetricsSink for AggregateTailCaptureSink {
    fn record(&self, event: MetricsEvent) {
        self.events.borrow_mut().push(event);
    }
}

///
/// SimpleTerminalProbeKind
///
/// Declares one simple-entity aggregate terminal for short-circuit probe rows.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SimpleTerminalProbeKind {
    Exists,
    Min,
    Max,
    First,
    Last,
}

///
/// SimpleTerminalExpected
///
/// Canonical expected payload for simple-entity short-circuit probe rows.
/// IDs are represented as raw `Ulid` values for table readability.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SimpleTerminalExpected {
    Exists(bool),
    Id(Option<Ulid>),
}

///
/// SimpleTerminalProbeCase
///
/// One short-circuit matrix row for simple-entity terminal semantics.
/// Each row binds terminal kind, direction/window controls, and expected behavior.
///

#[derive(Clone, Copy)]
struct SimpleTerminalProbeCase {
    label: &'static str,
    ids: &'static [u128],
    terminal: SimpleTerminalProbeKind,
    direction: OrderDirection,
    offset: u32,
    limit: Option<u32>,
    expected: SimpleTerminalExpected,
    expected_scanned: Option<usize>,
}

///
/// RankOrderTerminal
///
/// Selects the rank orientation shared by the live top-k and bottom-k tail
/// terminal matrices.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RankOrderTerminal {
    Top,
    Bottom,
}

///
/// BoundedRankWindowCase
///
/// One bounded-window rank matrix row. Each row verifies execute-oracle parity,
/// bounded scan-budget parity, and divergence from the unbounded dataset view.
///

struct BoundedRankWindowCase {
    label: &'static str,
    rows: &'static [(u128, u32, u32)],
    terminal: RankOrderTerminal,
}

///
/// ForcedShapeRankCase
///
/// One forced-shape rank matrix row that binds a full-scan fixture and an
/// index-range fixture to the same top-k or bottom-k terminal contract.
///

struct ForcedShapeRankCase {
    label: &'static str,
    full_scan_rows: &'static [u128],
    index_range_rows: &'static [(u128, u32)],
    terminal: RankOrderTerminal,
}

const SIMPLE_PROBE_EXISTS_IDS: [u128; 6] = [9201, 9202, 9203, 9204, 9205, 9206];
const SIMPLE_PROBE_FIRST_ROW_EXTREMA_IDS: [u128; 6] = [9301, 9302, 9303, 9304, 9305, 9306];
const SIMPLE_PROBE_OFFSET_EXTREMA_IDS: [u128; 7] = [9401, 9402, 9403, 9404, 9405, 9406, 9407];
const SIMPLE_PROBE_OFFSET_FIRST_IDS: [u128; 7] = [9451, 9452, 9453, 9454, 9455, 9456, 9457];
const SIMPLE_PROBE_LIMITED_LAST_IDS: [u128; 7] = [9461, 9462, 9463, 9464, 9465, 9466, 9467];
const SIMPLE_PROBE_UNBOUNDED_LAST_IDS: [u128; 7] = [9471, 9472, 9473, 9474, 9475, 9476, 9477];
const SIMPLE_PROBE_DIRECTION_IDS: [u128; 5] = [9481, 9482, 9483, 9484, 9485];
const BOUNDED_RANK_WINDOW_TOP_ROWS: [(u128, u32, u32); 6] = [
    (8_3811, 7, 10),
    (8_3812, 7, 20),
    (8_3813, 7, 30),
    (8_3814, 7, 100),
    (8_3815, 7, 90),
    (8_3816, 7, 80),
];
const BOUNDED_RANK_WINDOW_BOTTOM_ROWS: [(u128, u32, u32); 6] = [
    (8_3821, 7, 100),
    (8_3822, 7, 90),
    (8_3823, 7, 80),
    (8_3824, 7, 10),
    (8_3825, 7, 20),
    (8_3826, 7, 30),
];
const FORCED_SHAPE_FULL_SCAN_TOP_ROWS: [u128; 6] = [8_3901, 8_3902, 8_3903, 8_3904, 8_3905, 8_3906];
const FORCED_SHAPE_FULL_SCAN_BOTTOM_ROWS: [u128; 6] =
    [8_3921, 8_3922, 8_3923, 8_3924, 8_3925, 8_3926];
const FORCED_SHAPE_INDEX_RANGE_TOP_ROWS: [(u128, u32); 6] = [
    (8_3911, 100),
    (8_3912, 101),
    (8_3913, 102),
    (8_3914, 103),
    (8_3915, 104),
    (8_3916, 105),
];
const FORCED_SHAPE_INDEX_RANGE_BOTTOM_ROWS: [(u128, u32); 6] = [
    (8_3931, 100),
    (8_3932, 101),
    (8_3933, 102),
    (8_3934, 103),
    (8_3935, 104),
    (8_3936, 105),
];

///
/// StrictPrefilterAggregate
///
/// Declares the aggregate terminal families used by the strict-prefilter
/// scan-reduction matrix.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StrictPrefilterAggregate {
    Count,
    Exists,
    MinBy,
    MaxBy,
    First,
    Last,
}

///
/// StrictPrefilterOutput
///
/// Typed output wrapper for the strict-prefilter parity matrix so the strict
/// and fallback lanes can be compared without separate assertion code per terminal.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum StrictPrefilterOutput {
    Count(u32),
    Exists(bool),
    Id(Option<Id<PushdownParityEntity>>),
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
    let sink = AggregateTailCaptureSink::default();
    let output = with_metrics_sink(&sink, run);
    let rows_scanned = rows_scanned_for_entity(&sink.into_events(), entity_path);

    (output, rows_scanned)
}

fn seed_simple_entities(rows: &[u128]) {
    reset_store();
    let save = SaveExecutor::<SimpleEntity>::new(DB, false);

    for id in rows {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(*id),
        })
        .expect("aggregate tail simple seed save should succeed");
    }
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
        .expect("aggregate tail pushdown seed save should succeed");
    }
}

fn seed_unique_index_range_entities(rows: &[(u128, u32)]) {
    reset_store();
    let save = SaveExecutor::<UniqueIndexRangeEntity>::new(DB, false);

    for (id, code) in rows {
        save.insert(UniqueIndexRangeEntity {
            id: Ulid::from_u128(*id),
            code: *code,
            label: format!("code-{code}"),
        })
        .expect("aggregate tail unique-range seed save should succeed");
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
            "expected pushdown row to exist before data-only removal",
        );
    });
}

fn planned_slot<E>(field: &str) -> PlannedFieldSlot
where
    E: EntityKind,
{
    let resolved_index = resolve_field_slot(E::MODEL, field);
    let index = resolved_index.unwrap_or(0);

    PlannedFieldSlot {
        index,
        field: field.to_string(),
        kind: resolved_index.and_then(|index| E::MODEL.fields.get(index).map(|field| field.kind)),
    }
}

fn execution_root_node_type<E>(plan: &ExecutablePlan<E>) -> ExplainExecutionNodeType
where
    E: EntityKind + EntityValue,
{
    plan.explain_load_execution_node_descriptor()
        .expect("aggregate tail execution descriptor should build")
        .node_type()
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

fn execute_exists_terminal<E>(
    load: &LoadExecutor<E>,
    plan: crate::db::executor::ExecutablePlan<E>,
) -> Result<bool, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(plan, ScalarTerminalBoundaryRequest::Exists)?
        .into_exists()
}

fn execute_id_terminal<E>(
    load: &LoadExecutor<E>,
    plan: crate::db::executor::ExecutablePlan<E>,
    kind: AggregateKind,
) -> Result<Option<crate::types::Id<E>>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(plan, ScalarTerminalBoundaryRequest::IdTerminal { kind })?
        .into_id()
}

fn execute_min_by_slot_terminal<E>(
    load: &LoadExecutor<E>,
    plan: crate::db::executor::ExecutablePlan<E>,
    target_field: PlannedFieldSlot,
) -> Result<Option<Id<E>>, InternalError>
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
    .into_id()
}

fn execute_max_by_slot_terminal<E>(
    load: &LoadExecutor<E>,
    plan: crate::db::executor::ExecutablePlan<E>,
    target_field: PlannedFieldSlot,
) -> Result<Option<Id<E>>, InternalError>
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

fn u32_eq_predicate_strict(field: &str, value: u32) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::Eq,
        Value::Uint(u64::from(value)),
        CoercionId::Strict,
    ))
}

fn strict_compare_predicate(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        value,
        CoercionId::Strict,
    ))
}

fn u32_in_predicate(field: &str, values: &'static [u32]) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::In,
        Value::List(
            values
                .iter()
                .copied()
                .map(|value| Value::Uint(u64::from(value)))
                .collect(),
        ),
        CoercionId::NumericWiden,
    ))
}

fn u32_in_predicate_strict(field: &str, values: &'static [u32]) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::In,
        Value::List(
            values
                .iter()
                .copied()
                .map(|value| Value::Uint(u64::from(value)))
                .collect(),
        ),
        CoercionId::Strict,
    ))
}

fn u32_range_predicate(field: &str, lower_inclusive: u32, upper_inclusive: u32) -> Predicate {
    Predicate::And(vec![
        strict_compare_predicate(
            field,
            CompareOp::Gte,
            Value::Uint(u64::from(lower_inclusive)),
        ),
        strict_compare_predicate(
            field,
            CompareOp::Lte,
            Value::Uint(u64::from(upper_inclusive)),
        ),
    ])
}

fn seed_pushdown_group_rank_fixture(
    group_seven_base: u128,
    group_seven_count: u32,
    group_eight_base: u128,
    group_eight_count: u32,
) {
    let mut rows = Vec::with_capacity(
        usize::try_from(group_seven_count.saturating_add(group_eight_count)).unwrap_or(0),
    );

    for rank in 0u32..group_seven_count {
        rows.push((group_seven_base.saturating_add(u128::from(rank)), 7, rank));
    }
    for rank in 0u32..group_eight_count {
        rows.push((group_eight_base.saturating_add(u128::from(rank)), 8, rank));
    }

    seed_pushdown_entities(rows.as_slice());
}

fn strict_group_rank_subset_filter(ranks: &'static [u32]) -> Predicate {
    Predicate::And(vec![
        u32_eq_predicate_strict("group", 7),
        u32_in_predicate_strict("rank", ranks),
    ])
}

fn uncertain_group_rank_subset_filter(ranks: &'static [u32]) -> Predicate {
    Predicate::And(vec![
        u32_eq_predicate("group", 7),
        u32_in_predicate("rank", ranks),
    ])
}

fn run_strict_prefilter_aggregate(
    load: &LoadExecutor<PushdownParityEntity>,
    aggregate: StrictPrefilterAggregate,
    filter: Predicate,
) -> Result<StrictPrefilterOutput, InternalError> {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore).filter(filter);
    let plan = match aggregate {
        StrictPrefilterAggregate::MaxBy => query
            .order_by_desc("rank")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("strict prefilter DESC aggregate plan should build"),
        StrictPrefilterAggregate::Count
        | StrictPrefilterAggregate::Exists
        | StrictPrefilterAggregate::MinBy
        | StrictPrefilterAggregate::First
        | StrictPrefilterAggregate::Last => query
            .order_by("rank")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("strict prefilter ASC aggregate plan should build"),
    };

    match aggregate {
        StrictPrefilterAggregate::Count => {
            execute_count_terminal(load, plan).map(StrictPrefilterOutput::Count)
        }
        StrictPrefilterAggregate::Exists => {
            execute_exists_terminal(load, plan).map(StrictPrefilterOutput::Exists)
        }
        StrictPrefilterAggregate::MinBy => {
            execute_min_by_slot_terminal(load, plan, planned_slot::<PushdownParityEntity>("rank"))
                .map(StrictPrefilterOutput::Id)
        }
        StrictPrefilterAggregate::MaxBy => {
            execute_max_by_slot_terminal(load, plan, planned_slot::<PushdownParityEntity>("rank"))
                .map(StrictPrefilterOutput::Id)
        }
        StrictPrefilterAggregate::First => {
            execute_id_terminal(load, plan, AggregateKind::First).map(StrictPrefilterOutput::Id)
        }
        StrictPrefilterAggregate::Last => {
            execute_id_terminal(load, plan, AggregateKind::Last).map(StrictPrefilterOutput::Id)
        }
    }
}

fn assert_strict_prefilter_scan_reduction(
    load: &LoadExecutor<PushdownParityEntity>,
    aggregate: StrictPrefilterAggregate,
    label: &'static str,
) -> usize {
    const TARGET_RANKS: &[u32] = &[151, 152, 153];
    let strict_filter = strict_group_rank_subset_filter(TARGET_RANKS);
    let uncertain_filter = uncertain_group_rank_subset_filter(TARGET_RANKS);
    let (strict_output, strict_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            run_strict_prefilter_aggregate(load, aggregate, strict_filter.clone())
                .expect("strict prefilter aggregate should succeed")
        });
    let (fallback_output, fallback_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            run_strict_prefilter_aggregate(load, aggregate, uncertain_filter.clone())
                .expect("uncertain fallback aggregate should succeed")
        });

    assert_eq!(
        strict_output, fallback_output,
        "strict prefilter and uncertain fallback should preserve parity for terminal={label}",
    );
    assert!(
        strict_scanned < fallback_scanned,
        "strict prefilter should scan fewer rows than uncertain fallback for terminal={label}",
    );

    strict_scanned
}

fn run_pushdown_rank_terminal(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
    terminal: RankOrderTerminal,
    k: u32,
) -> Result<EntityResponse<PushdownParityEntity>, InternalError> {
    match terminal {
        RankOrderTerminal::Top => {
            load.top_k_by_slot(plan, planned_slot::<PushdownParityEntity>("rank"), k)
        }
        RankOrderTerminal::Bottom => {
            load.bottom_k_by_slot(plan, planned_slot::<PushdownParityEntity>("rank"), k)
        }
    }
}

fn expected_pushdown_rank_ids(
    response: &EntityResponse<PushdownParityEntity>,
    terminal: RankOrderTerminal,
    k: usize,
) -> Vec<Id<PushdownParityEntity>> {
    let mut expected_rank_order = response
        .iter()
        .map(|row| (row.entity_ref().rank, row.id()))
        .collect::<Vec<_>>();
    expected_rank_order.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        match terminal {
            RankOrderTerminal::Top => right_rank
                .cmp(left_rank)
                .then_with(|| left_id.key().cmp(&right_id.key())),
            RankOrderTerminal::Bottom => left_rank
                .cmp(right_rank)
                .then_with(|| left_id.key().cmp(&right_id.key())),
        }
    });

    expected_rank_order
        .into_iter()
        .take(k)
        .map(|(_, id)| id)
        .collect()
}

fn bounded_rank_window_cases() -> [BoundedRankWindowCase; 2] {
    [
        BoundedRankWindowCase {
            label: "top_k_by",
            rows: &BOUNDED_RANK_WINDOW_TOP_ROWS,
            terminal: RankOrderTerminal::Top,
        },
        BoundedRankWindowCase {
            label: "bottom_k_by",
            rows: &BOUNDED_RANK_WINDOW_BOTTOM_ROWS,
            terminal: RankOrderTerminal::Bottom,
        },
    ]
}

fn run_simple_rank_terminal(
    load: &LoadExecutor<SimpleEntity>,
    plan: ExecutablePlan<SimpleEntity>,
    terminal: RankOrderTerminal,
    k: u32,
) -> Result<EntityResponse<SimpleEntity>, InternalError> {
    match terminal {
        RankOrderTerminal::Top => load.top_k_by_slot(plan, planned_slot::<SimpleEntity>("id"), k),
        RankOrderTerminal::Bottom => {
            load.bottom_k_by_slot(plan, planned_slot::<SimpleEntity>("id"), k)
        }
    }
}

fn run_unique_index_rank_terminal(
    load: &LoadExecutor<UniqueIndexRangeEntity>,
    plan: ExecutablePlan<UniqueIndexRangeEntity>,
    terminal: RankOrderTerminal,
    k: u32,
) -> Result<EntityResponse<UniqueIndexRangeEntity>, InternalError> {
    match terminal {
        RankOrderTerminal::Top => {
            load.top_k_by_slot(plan, planned_slot::<UniqueIndexRangeEntity>("code"), k)
        }
        RankOrderTerminal::Bottom => {
            load.bottom_k_by_slot(plan, planned_slot::<UniqueIndexRangeEntity>("code"), k)
        }
    }
}

fn expected_simple_rank_ids(
    response: &EntityResponse<SimpleEntity>,
    terminal: RankOrderTerminal,
    k: usize,
) -> Vec<Id<SimpleEntity>> {
    let mut expected: Vec<_> = response.ids().collect();
    match terminal {
        RankOrderTerminal::Top => {
            expected.sort_unstable_by_key(|id| std::cmp::Reverse(id.key()));
        }
        RankOrderTerminal::Bottom => expected.sort_unstable_by_key(Id::key),
    }
    expected.truncate(k);
    expected
}

fn expected_unique_index_rank_ids(
    response: &EntityResponse<UniqueIndexRangeEntity>,
    terminal: RankOrderTerminal,
    k: usize,
) -> Vec<Id<UniqueIndexRangeEntity>> {
    let mut ranked = response
        .iter()
        .map(|row| (row.entity_ref().code, row.id()))
        .collect::<Vec<_>>();
    ranked.sort_unstable_by(
        |(left_code, left_id), (right_code, right_id)| match terminal {
            RankOrderTerminal::Top => right_code
                .cmp(left_code)
                .then_with(|| left_id.key().cmp(&right_id.key())),
            RankOrderTerminal::Bottom => left_code
                .cmp(right_code)
                .then_with(|| left_id.key().cmp(&right_id.key())),
        },
    );

    ranked.into_iter().take(k).map(|(_, id)| id).collect()
}

fn forced_shape_rank_cases() -> [ForcedShapeRankCase; 2] {
    [
        ForcedShapeRankCase {
            label: "top_k_by",
            full_scan_rows: &FORCED_SHAPE_FULL_SCAN_TOP_ROWS,
            index_range_rows: &FORCED_SHAPE_INDEX_RANGE_TOP_ROWS,
            terminal: RankOrderTerminal::Top,
        },
        ForcedShapeRankCase {
            label: "bottom_k_by",
            full_scan_rows: &FORCED_SHAPE_FULL_SCAN_BOTTOM_ROWS,
            index_range_rows: &FORCED_SHAPE_INDEX_RANGE_BOTTOM_ROWS,
            terminal: RankOrderTerminal::Bottom,
        },
    ]
}

fn build_simple_terminal_probe_plan(
    case: SimpleTerminalProbeCase,
) -> crate::db::executor::ExecutablePlan<SimpleEntity> {
    let mut query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore);
    query = match case.direction {
        OrderDirection::Asc => query.order_by("id"),
        OrderDirection::Desc => query.order_by_desc("id"),
    };
    if case.offset > 0 {
        query = query.offset(case.offset);
    }
    if let Some(limit) = case.limit {
        query = query.limit(limit);
    }

    query
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("simple terminal probe plan should build")
}

fn run_simple_terminal_probe(
    load: &LoadExecutor<SimpleEntity>,
    case: SimpleTerminalProbeCase,
) -> Result<SimpleTerminalExpected, InternalError> {
    let plan = build_simple_terminal_probe_plan(case);

    let output = match case.terminal {
        SimpleTerminalProbeKind::Exists => {
            SimpleTerminalExpected::Exists(execute_exists_terminal(load, plan)?)
        }
        SimpleTerminalProbeKind::Min => SimpleTerminalExpected::Id(
            execute_id_terminal(load, plan, AggregateKind::Min)?.map(|id| id.key()),
        ),
        SimpleTerminalProbeKind::Max => SimpleTerminalExpected::Id(
            execute_id_terminal(load, plan, AggregateKind::Max)?.map(|id| id.key()),
        ),
        SimpleTerminalProbeKind::First => SimpleTerminalExpected::Id(
            execute_id_terminal(load, plan, AggregateKind::First)?.map(|id| id.key()),
        ),
        SimpleTerminalProbeKind::Last => SimpleTerminalExpected::Id(
            execute_id_terminal(load, plan, AggregateKind::Last)?.map(|id| id.key()),
        ),
    };

    Ok(output)
}

#[expect(clippy::too_many_arguments)]
fn simple_terminal_probe_case(
    label: &'static str,
    ids: &'static [u128],
    terminal: SimpleTerminalProbeKind,
    direction: OrderDirection,
    offset: u32,
    limit: Option<u32>,
    expected: SimpleTerminalExpected,
    expected_scanned: Option<usize>,
) -> SimpleTerminalProbeCase {
    SimpleTerminalProbeCase {
        label,
        ids,
        terminal,
        direction,
        offset,
        limit,
        expected,
        expected_scanned,
    }
}

#[expect(clippy::too_many_lines)]
fn simple_terminal_probe_cases() -> [SimpleTerminalProbeCase; 16] {
    [
        simple_terminal_probe_case(
            "exists_asc_early_stop",
            &SIMPLE_PROBE_EXISTS_IDS,
            SimpleTerminalProbeKind::Exists,
            OrderDirection::Asc,
            0,
            None,
            SimpleTerminalExpected::Exists(true),
            Some(1),
        ),
        simple_terminal_probe_case(
            "exists_desc_early_stop",
            &SIMPLE_PROBE_EXISTS_IDS,
            SimpleTerminalProbeKind::Exists,
            OrderDirection::Desc,
            0,
            None,
            SimpleTerminalExpected::Exists(true),
            Some(1),
        ),
        simple_terminal_probe_case(
            "min_asc_first_row_short_circuit",
            &SIMPLE_PROBE_FIRST_ROW_EXTREMA_IDS,
            SimpleTerminalProbeKind::Min,
            OrderDirection::Asc,
            0,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9301))),
            Some(1),
        ),
        simple_terminal_probe_case(
            "max_desc_first_row_short_circuit",
            &SIMPLE_PROBE_FIRST_ROW_EXTREMA_IDS,
            SimpleTerminalProbeKind::Max,
            OrderDirection::Desc,
            0,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9306))),
            Some(1),
        ),
        simple_terminal_probe_case(
            "min_asc_offset_plus_one",
            &SIMPLE_PROBE_OFFSET_EXTREMA_IDS,
            SimpleTerminalProbeKind::Min,
            OrderDirection::Asc,
            3,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9404))),
            Some(4),
        ),
        simple_terminal_probe_case(
            "max_desc_offset_plus_one",
            &SIMPLE_PROBE_OFFSET_EXTREMA_IDS,
            SimpleTerminalProbeKind::Max,
            OrderDirection::Desc,
            3,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9404))),
            Some(4),
        ),
        simple_terminal_probe_case(
            "first_asc_offset_plus_one",
            &SIMPLE_PROBE_OFFSET_FIRST_IDS,
            SimpleTerminalProbeKind::First,
            OrderDirection::Asc,
            3,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9454))),
            Some(4),
        ),
        simple_terminal_probe_case(
            "first_desc_offset_plus_one",
            &SIMPLE_PROBE_OFFSET_FIRST_IDS,
            SimpleTerminalProbeKind::First,
            OrderDirection::Desc,
            3,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9454))),
            Some(4),
        ),
        simple_terminal_probe_case(
            "last_asc_limited_window_offset_plus_limit",
            &SIMPLE_PROBE_LIMITED_LAST_IDS,
            SimpleTerminalProbeKind::Last,
            OrderDirection::Asc,
            2,
            Some(3),
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9465))),
            Some(5),
        ),
        simple_terminal_probe_case(
            "last_desc_limited_window_offset_plus_limit",
            &SIMPLE_PROBE_LIMITED_LAST_IDS,
            SimpleTerminalProbeKind::Last,
            OrderDirection::Desc,
            2,
            Some(3),
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9463))),
            Some(5),
        ),
        simple_terminal_probe_case(
            "last_asc_unbounded_window_scans_full_stream",
            &SIMPLE_PROBE_UNBOUNDED_LAST_IDS,
            SimpleTerminalProbeKind::Last,
            OrderDirection::Asc,
            2,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9477))),
            Some(7),
        ),
        simple_terminal_probe_case(
            "last_desc_unbounded_window_scans_full_stream",
            &SIMPLE_PROBE_UNBOUNDED_LAST_IDS,
            SimpleTerminalProbeKind::Last,
            OrderDirection::Desc,
            2,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9471))),
            Some(7),
        ),
        simple_terminal_probe_case(
            "first_asc_respects_direction",
            &SIMPLE_PROBE_DIRECTION_IDS,
            SimpleTerminalProbeKind::First,
            OrderDirection::Asc,
            0,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9481))),
            None,
        ),
        simple_terminal_probe_case(
            "first_desc_respects_direction",
            &SIMPLE_PROBE_DIRECTION_IDS,
            SimpleTerminalProbeKind::First,
            OrderDirection::Desc,
            0,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9485))),
            None,
        ),
        simple_terminal_probe_case(
            "last_asc_respects_direction",
            &SIMPLE_PROBE_DIRECTION_IDS,
            SimpleTerminalProbeKind::Last,
            OrderDirection::Asc,
            0,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9485))),
            None,
        ),
        simple_terminal_probe_case(
            "last_desc_respects_direction",
            &SIMPLE_PROBE_DIRECTION_IDS,
            SimpleTerminalProbeKind::Last,
            OrderDirection::Desc,
            0,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9481))),
            None,
        ),
    ]
}

#[test]
fn aggregate_tail_simple_terminal_short_circuit_and_direction_matrix() {
    for case in simple_terminal_probe_cases() {
        seed_simple_entities(case.ids);
        let load = LoadExecutor::<SimpleEntity>::new(DB, false);

        let (actual, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
            run_simple_terminal_probe(&load, case)
                .expect("simple terminal probe execution should succeed")
        });

        assert_eq!(
            actual, case.expected,
            "terminal output mismatch for case={}",
            case.label
        );

        if let Some(expected_scanned) = case.expected_scanned {
            assert_eq!(
                scanned, expected_scanned,
                "terminal scan budget mismatch for case={}",
                case.label
            );
        }
    }
}

#[test]
fn aggregate_tail_last_unbounded_desc_large_dataset_scans_full_stream() {
    let ids: Vec<u128> = (0u128..128u128)
        .map(|i| 9701u128.saturating_add(i))
        .collect();
    seed_simple_entities(&ids);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let (last_desc, scanned_last_desc) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
            execute_id_terminal(
                &load,
                Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                    .order_by_desc("id")
                    .plan()
                    .map(crate::db::executor::ExecutablePlan::from)
                    .expect("last DESC large unbounded plan should build"),
                AggregateKind::Last,
            )
            .expect("last DESC large unbounded should succeed")
        });

    assert_eq!(last_desc.map(|id| id.key()), Some(Ulid::from_u128(9701)));
    assert_eq!(scanned_last_desc, 128);
}

#[test]
fn aggregate_tail_last_secondary_index_desc_mixed_direction_falls_back_safely() {
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
            execute_id_terminal(
                &load,
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(group_seven.clone())
                    .order_by_desc("rank")
                    .plan()
                    .map(crate::db::executor::ExecutablePlan::from)
                    .expect("secondary last DESC unbounded plan should build"),
                AggregateKind::Last,
            )
            .expect("secondary last DESC unbounded should succeed")
        });

    assert_eq!(last_desc.map(|id| id.key()), Some(Ulid::from_u128(9801)));
    assert_eq!(scanned_desc, 32);
}

#[test]
fn aggregate_tail_count_distinct_offset_window_stays_unbounded() {
    seed_simple_entities(&[9511, 9512, 9513, 9514, 9515, 9516, 9517]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let (count_asc, scanned_asc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        execute_count_terminal(
            &load,
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .distinct()
                .order_by("id")
                .offset(2)
                .limit(2)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("count distinct+offset ASC plan should build"),
        )
        .expect("count distinct+offset ASC should succeed")
    });
    let (count_desc, scanned_desc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        execute_count_terminal(
            &load,
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .distinct()
                .order_by_desc("id")
                .offset(2)
                .limit(2)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("count distinct+offset DESC plan should build"),
        )
        .expect("count distinct+offset DESC should succeed")
    });

    assert_eq!(count_asc, 2);
    assert_eq!(count_desc, 2);
    assert_eq!(scanned_asc, 7);
    assert_eq!(scanned_desc, 7);
}

#[test]
fn aggregate_tail_strict_prefilter_reduces_scan_vs_uncertain_fallback() {
    seed_pushdown_group_rank_fixture(10_601, 160, 10_901, 40);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let strict_count_scanned =
        assert_strict_prefilter_scan_reduction(&load, StrictPrefilterAggregate::Count, "count");
    assert!(strict_count_scanned <= 3);

    let strict_exists_scanned =
        assert_strict_prefilter_scan_reduction(&load, StrictPrefilterAggregate::Exists, "exists");
    assert!(strict_exists_scanned <= 3);

    for (aggregate, label) in [
        (StrictPrefilterAggregate::MinBy, "min_by"),
        (StrictPrefilterAggregate::MaxBy, "max_by"),
        (StrictPrefilterAggregate::First, "first"),
        (StrictPrefilterAggregate::Last, "last"),
    ] {
        assert_strict_prefilter_scan_reduction(&load, aggregate, label);
    }
}

#[test]
fn aggregate_tail_rank_terminals_bounded_window_scan_budget_and_oracle_matrix() {
    for case in bounded_rank_window_cases() {
        seed_pushdown_entities(case.rows);
        let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
        let build_bounded_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("id")
                .limit(3)
                .plan()
                .map(ExecutablePlan::from)
                .expect("bounded rank-window matrix plan should build")
        };
        let build_unbounded_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("id")
                .plan()
                .map(ExecutablePlan::from)
                .expect("unbounded rank-window matrix plan should build")
        };

        // Phase 1: establish execute oracle and bounded terminal scan budget.
        let (bounded_execute, scanned_execute) =
            capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
                load.execute(build_bounded_plan())
                    .expect("bounded rank-window matrix execute baseline should succeed")
            });
        let (bounded_ranked, scanned_ranked) =
            capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
                run_pushdown_rank_terminal(&load, build_bounded_plan(), case.terminal, 2)
                    .expect("bounded rank-window matrix terminal should succeed")
            });

        // Phase 2: keep ranked output aligned with the bounded execute window.
        let expected_bounded_ids = expected_pushdown_rank_ids(&bounded_execute, case.terminal, 2);
        assert_eq!(
            bounded_ranked.ids().collect::<Vec<_>>(),
            expected_bounded_ids,
            "bounded rank-window execute oracle mismatch for case={}",
            case.label
        );
        assert_eq!(
            scanned_ranked, scanned_execute,
            "bounded rank-window scan-budget parity failed for case={}",
            case.label
        );

        // Phase 3: prove the bounded window differs from the unbounded dataset.
        let unbounded_ranked =
            run_pushdown_rank_terminal(&load, build_unbounded_plan(), case.terminal, 2)
                .expect("unbounded rank-window matrix terminal should succeed");
        assert_ne!(
            bounded_ranked.ids().collect::<Vec<_>>(),
            unbounded_ranked.ids().collect::<Vec<_>>(),
            "bounded rank-window behavior should differ from unbounded behavior for case={}",
            case.label
        );
    }
}

#[test]
fn aggregate_tail_rank_terminals_forced_shape_execute_oracle_matrix() {
    for case in forced_shape_rank_cases() {
        // Phase 1: force a full-scan shape and keep top/bottom-k parity with execute().
        seed_simple_entities(case.full_scan_rows);
        let simple_load = LoadExecutor::<SimpleEntity>::new(DB, false);
        let build_full_scan_plan = || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .offset(1)
                .limit(4)
                .plan()
                .map(ExecutablePlan::from)
                .expect("forced-shape full-scan matrix plan should build")
        };
        let full_scan_plan = build_full_scan_plan();
        assert!(
            matches!(
                execution_root_node_type(&full_scan_plan),
                ExplainExecutionNodeType::FullScan
            ),
            "forced-shape FullScan matrix must force FullScan for case={}",
            case.label
        );
        let full_scan_execute = simple_load
            .execute(build_full_scan_plan())
            .expect("forced-shape FullScan execute baseline should succeed");
        let full_scan_ranked =
            run_simple_rank_terminal(&simple_load, build_full_scan_plan(), case.terminal, 2)
                .expect("forced-shape FullScan terminal should succeed");
        assert_eq!(
            full_scan_ranked.ids().collect::<Vec<_>>(),
            expected_simple_rank_ids(&full_scan_execute, case.terminal, 2),
            "forced-shape FullScan execute oracle mismatch for case={}",
            case.label
        );

        // Phase 2: force an index-range shape and keep code-ranking aligned.
        seed_unique_index_range_entities(case.index_range_rows);
        let range_load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
        let code_range = u32_range_predicate("code", 101, 106);
        let build_index_range_plan = || {
            Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
                .filter(code_range.clone())
                .order_by_desc("code")
                .offset(1)
                .limit(3)
                .plan()
                .map(ExecutablePlan::from)
                .expect("forced-shape index-range matrix plan should build")
        };
        let index_range_plan = build_index_range_plan();
        assert!(
            matches!(
                execution_root_node_type(&index_range_plan),
                ExplainExecutionNodeType::IndexRangeScan
            ),
            "forced-shape IndexRange matrix must force IndexRange for case={}",
            case.label
        );
        let index_range_execute = range_load
            .execute(build_index_range_plan())
            .expect("forced-shape IndexRange execute baseline should succeed");
        let index_range_ranked =
            run_unique_index_rank_terminal(&range_load, build_index_range_plan(), case.terminal, 2)
                .expect("forced-shape IndexRange terminal should succeed");
        assert_eq!(
            index_range_ranked.ids().collect::<Vec<_>>(),
            expected_unique_index_rank_ids(&index_range_execute, case.terminal, 2),
            "forced-shape IndexRange execute oracle mismatch for case={}",
            case.label
        );
    }
}

#[test]
fn aggregate_tail_missing_ok_skips_leading_stale_secondary_keys_for_exists_min_max() {
    seed_pushdown_entities(&[
        (9601, 7, 10),
        (9602, 7, 20),
        (9603, 7, 30),
        (9604, 7, 40),
        (9605, 8, 50),
    ]);
    remove_pushdown_row_data(9601);
    remove_pushdown_row_data(9604);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);
    let build_asc_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(group_seven.clone())
            .order_by("rank")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("missing-ok stale-leading ASC plan should build")
    };
    let build_desc_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(group_seven.clone())
            .order_by_desc("rank")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("missing-ok stale-leading DESC plan should build")
    };

    let (exists_asc, scanned_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_exists_terminal(&load, build_asc_plan()).expect("exists ASC should succeed")
        });
    let (exists_desc, scanned_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_exists_terminal(&load, build_desc_plan()).expect("exists DESC should succeed")
        });
    let min_id = execute_id_terminal(&load, build_asc_plan(), AggregateKind::Min)
        .expect("min should succeed");
    let max_id = execute_id_terminal(&load, build_asc_plan(), AggregateKind::Max)
        .expect("max should succeed");
    let first_asc = execute_id_terminal(&load, build_asc_plan(), AggregateKind::First)
        .expect("first ASC should succeed");
    let last_asc = execute_id_terminal(&load, build_asc_plan(), AggregateKind::Last)
        .expect("last ASC should succeed");
    let first_desc = execute_id_terminal(&load, build_desc_plan(), AggregateKind::First)
        .expect("first DESC should succeed");
    let last_desc = execute_id_terminal(&load, build_desc_plan(), AggregateKind::Last)
        .expect("last DESC should succeed");

    assert!(exists_asc);
    assert!(exists_desc);
    assert!(scanned_asc >= 2);
    assert!(scanned_desc >= 2);
    assert_eq!(min_id.map(|id| id.key()), Some(Ulid::from_u128(9602)));
    assert_eq!(max_id.map(|id| id.key()), Some(Ulid::from_u128(9603)));
    assert_eq!(first_asc.map(|id| id.key()), Some(Ulid::from_u128(9602)));
    assert_eq!(last_asc.map(|id| id.key()), Some(Ulid::from_u128(9603)));
    assert_eq!(first_desc.map(|id| id.key()), Some(Ulid::from_u128(9603)));
    assert_eq!(last_desc.map(|id| id.key()), Some(Ulid::from_u128(9602)));
}
