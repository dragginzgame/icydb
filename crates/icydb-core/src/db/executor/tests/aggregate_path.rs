//! Module: db::executor::tests::aggregate_path
//! Responsibility: live aggregate path-parity and scan-budget contracts.
//! Does not own: optimization-hit marker counters or unrelated projection/ranked behavior.
//! Boundary: keeps aggregate path semantics local to the live executor test harness.

use super::*;
use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        data::{DataKey, PersistedRow},
        executor::{ExecutablePlan, ScalarTerminalBoundaryRequest},
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::{
            explain::ExplainExecutionNodeType,
            intent::Query,
            plan::{AccessPlannedQuery, OrderDirection, OrderSpec, PageSpec},
        },
        response::EntityResponse,
    },
    error::{ErrorClass, InternalError},
    metrics::sink::{MetricsEvent, MetricsSink, with_metrics_sink},
    traits::{EntityKind, EntityValue},
    types::Ulid,
    value::Value,
};
use std::cell::RefCell;
use std::ops::Bound;

const COMPOSITE_COUNT_BASE: u128 = 8_751;
const COMPOSITE_EXISTS_BASE: u128 = 8_761;

///
/// AggregatePathCaptureSink
///
/// Small metrics sink used to keep path-level scan-budget assertions live
/// while aggregate path contracts move out of the stale matrix family.
///

#[derive(Default)]
struct AggregatePathCaptureSink {
    events: RefCell<Vec<MetricsEvent>>,
}

impl AggregatePathCaptureSink {
    fn into_events(self) -> Vec<MetricsEvent> {
        self.events.into_inner()
    }
}

impl MetricsSink for AggregatePathCaptureSink {
    fn record(&self, event: MetricsEvent) {
        self.events.borrow_mut().push(event);
    }
}

///
/// CompositeTerminal
///
/// Minimal terminal family used to preserve the composite direct-vs-fallback
/// scan-accounting contracts from the stale aggregate path matrix.
///

#[derive(Clone, Copy)]
enum CompositeTerminal {
    Count,
    Exists,
}

///
/// CompositeTerminalResult
///
/// Typed result wrapper used by the composite direct/fallback parity helpers.
///

#[derive(Debug, PartialEq)]
enum CompositeTerminalResult {
    Count(u32),
    Exists(bool),
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
    let sink = AggregatePathCaptureSink::default();
    let output = with_metrics_sink(&sink, run);
    let rows_scanned = rows_scanned_for_entity(&sink.into_events(), entity_path);

    (output, rows_scanned)
}

fn execute_count_terminal<E>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
) -> Result<u32, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(plan, ScalarTerminalBoundaryRequest::Count)?
        .into_count()
}

fn execute_exists_terminal<E>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
) -> Result<bool, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(plan, ScalarTerminalBoundaryRequest::Exists)?
        .into_exists()
}

fn execution_root_node_type<E>(plan: &ExecutablePlan<E>) -> ExplainExecutionNodeType
where
    E: EntityKind + EntityValue,
{
    plan.explain_load_execution_node_descriptor()
        .expect("aggregate path execution descriptor should build")
        .node_type()
}

fn strict_compare_predicate(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        value,
        CoercionId::Strict,
    ))
}

fn u32_eq_predicate(field: &str, value: u32) -> Predicate {
    strict_compare_predicate(field, CompareOp::Eq, Value::Uint(u64::from(value)))
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

fn seed_simple_entities(rows: &[u128]) {
    reset_store();
    let save = SaveExecutor::<SimpleEntity>::new(DB, false);

    for id in rows {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(*id),
        })
        .expect("aggregate path simple seed save should succeed");
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
        .expect("aggregate path phase seed save should succeed");
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
        .expect("aggregate path pushdown seed save should succeed");
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
        .expect("aggregate path unique-range seed save should succeed");
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

fn seed_stale_secondary_rows(rows: &[(u128, u32, u32)], stale_ids: &[u128]) {
    seed_pushdown_entities(rows);
    for stale_id in stale_ids {
        remove_pushdown_row_data(*stale_id);
    }
}

fn plan_from_query<E>(query: Query<E>, label: &str) -> ExecutablePlan<E>
where
    E: EntityKind + EntityValue,
{
    query.plan().map_or_else(
        |err| panic!("{label} plan should build: {err}"),
        ExecutablePlan::from,
    )
}

fn secondary_group_rank_order_plan(
    consistency: MissingRowPolicy,
    direction: OrderDirection,
    offset: u32,
) -> ExecutablePlan<PushdownParityEntity> {
    let mut logical_plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_PARITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        consistency,
    );
    logical_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), direction),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical_plan.scalar_plan_mut().page = Some(PageSpec {
        limit: None,
        offset,
    });

    ExecutablePlan::<PushdownParityEntity>::new(logical_plan)
}

fn secondary_group_rank_index_range_count_plan(
    consistency: MissingRowPolicy,
    offset: u32,
    limit: u32,
) -> ExecutablePlan<PushdownParityEntity> {
    let mut logical_plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            PUSHDOWN_PARITY_INDEX_MODELS[0],
            vec![Value::Uint(7)],
            Bound::Included(Value::Uint(10)),
            Bound::Included(Value::Uint(40)),
        ),
        consistency,
    );
    logical_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical_plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(limit),
        offset,
    });

    ExecutablePlan::<PushdownParityEntity>::new(logical_plan)
}

fn execute_count_exists_window_parity<E>(
    load: &LoadExecutor<E>,
    build_query: impl Fn() -> Query<E>,
    label: &str,
) where
    E: PersistedRow + EntityKind + EntityValue,
{
    // Phase 1: materialize the canonical execute window and derive the
    // expected count/exists terminal outputs from the same ordered rows.
    let expected_response: EntityResponse<E> = load
        .execute(plan_from_query(build_query(), label))
        .unwrap_or_else(|err| panic!("{label} execute baseline should succeed: {err}"));
    let expected_count = expected_response.count();
    let expected_exists = !expected_response.is_empty();

    // Phase 2: assert both scalar path terminals preserve the canonical
    // execute window semantics for the same logical query.
    let count = execute_count_terminal(load, plan_from_query(build_query(), label))
        .unwrap_or_else(|err| panic!("{label} COUNT terminal should succeed: {err}"));
    let exists = execute_exists_terminal(load, plan_from_query(build_query(), label))
        .unwrap_or_else(|err| panic!("{label} EXISTS terminal should succeed: {err}"));

    assert_eq!(
        count, expected_count,
        "{label} COUNT must preserve canonical execute-window cardinality",
    );
    assert_eq!(
        exists, expected_exists,
        "{label} EXISTS must preserve canonical execute-window emptiness",
    );
}

fn phase_rows_with_base(base: u128) -> [(u128, u32); 6] {
    [
        (base, 10),
        (base.saturating_add(1), 20),
        (base.saturating_add(2), 30),
        (base.saturating_add(3), 40),
        (base.saturating_add(4), 50),
        (base.saturating_add(5), 60),
    ]
}

fn composite_key_sets_with_base(base: u128) -> (Vec<Ulid>, Vec<Ulid>) {
    let first = [0u128, 1, 2, 3]
        .into_iter()
        .map(|offset| Ulid::from_u128(base.saturating_add(offset)))
        .collect();
    let second = [2u128, 3, 4, 5]
        .into_iter()
        .map(|offset| Ulid::from_u128(base.saturating_add(offset)))
        .collect();

    (first, second)
}

fn build_phase_composite_plan(
    order_field: &str,
    first: Vec<Ulid>,
    second: Vec<Ulid>,
) -> ExecutablePlan<PhaseEntity> {
    let access = AccessPlan::Union(vec![
        AccessPlan::path(AccessPath::ByKeys(first)),
        AccessPlan::path(AccessPath::ByKeys(second)),
    ]);
    let mut logical_plan = AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore);
    logical_plan.access = access.into_value_plan();
    logical_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![(order_field.to_string(), OrderDirection::Asc)],
    });

    ExecutablePlan::<PhaseEntity>::new(logical_plan)
}

fn run_composite_terminal(
    load: &LoadExecutor<PhaseEntity>,
    plan: ExecutablePlan<PhaseEntity>,
    terminal: CompositeTerminal,
) -> Result<CompositeTerminalResult, InternalError> {
    match terminal {
        CompositeTerminal::Count => {
            execute_count_terminal(load, plan).map(CompositeTerminalResult::Count)
        }
        CompositeTerminal::Exists => {
            execute_exists_terminal(load, plan).map(CompositeTerminalResult::Exists)
        }
    }
}

fn assert_composite_terminal_direct_path_scan_does_not_exceed_fallback(
    rows: &[(u128, u32)],
    first: Vec<Ulid>,
    second: Vec<Ulid>,
    terminal: CompositeTerminal,
    label: &str,
) {
    seed_phase_entities(rows);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    // Phase 1: build one direct composite path and one fallback shape that
    // preserves the same row set while forcing materialized order handling.
    let direct_plan = build_phase_composite_plan("id", first.clone(), second.clone());
    let fallback_plan = build_phase_composite_plan("label", first, second);

    assert!(
        matches!(
            execution_root_node_type(&direct_plan),
            ExplainExecutionNodeType::Union | ExplainExecutionNodeType::Intersection
        ),
        "direct composite {label} shape should remain on a composite access root",
    );
    assert!(
        matches!(
            execution_root_node_type(&fallback_plan),
            ExplainExecutionNodeType::Union | ExplainExecutionNodeType::Intersection
        ),
        "fallback composite {label} shape should remain on a composite access root",
    );

    // Phase 2: keep the stale matrix contract live on the real executor path:
    // the direct composite stream must not scan more rows than the fallback
    // materialized order lane for the same logical filter.
    let (direct_result, direct_scanned) =
        capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
            run_composite_terminal(&load, direct_plan, terminal)
                .expect("direct composite terminal should succeed")
        });
    let (fallback_result, fallback_scanned) =
        capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
            run_composite_terminal(&load, fallback_plan, terminal)
                .expect("fallback composite terminal should succeed")
        });

    assert_eq!(
        direct_result, fallback_result,
        "composite direct/fallback {label} must preserve terminal parity",
    );
    assert!(
        direct_scanned <= fallback_scanned,
        "composite direct {label} must not scan more rows than fallback",
    );
}

#[test]
fn aggregate_path_ordered_desc_window_count_and_exists_match_execute() {
    seed_simple_entities(&[8_201, 8_202, 8_203, 8_204, 8_205, 8_206, 8_207, 8_208]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    execute_count_exists_window_parity(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by_desc("id")
                .offset(1)
                .limit(4)
        },
        "ordered DESC page window",
    );
}

#[test]
fn aggregate_path_by_id_and_by_ids_count_and_exists_match_execute() {
    seed_simple_entities(&[8_601, 8_602, 8_603, 8_604]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    execute_count_exists_window_parity(
        &load,
        || Query::<SimpleEntity>::new(MissingRowPolicy::Ignore).by_id(Ulid::from_u128(8_602)),
        "by_id path",
    );
    execute_count_exists_window_parity(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore).by_ids([
                Ulid::from_u128(8_604),
                Ulid::from_u128(8_601),
                Ulid::from_u128(8_604),
            ])
        },
        "by_ids path",
    );
}

#[test]
fn aggregate_path_by_id_window_shape_count_and_exists_match_execute() {
    seed_simple_entities(&[8_611]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    execute_count_exists_window_parity(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_id(Ulid::from_u128(8_611))
                .order_by("id")
                .offset(1)
                .limit(1)
        },
        "by_id windowed shape",
    );
}

#[test]
fn aggregate_path_by_id_count_ignore_missing_returns_zero() {
    seed_simple_entities(&[8_626]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        execute_count_terminal(
            &load,
            plan_from_query(
                Query::<SimpleEntity>::new(MissingRowPolicy::Ignore).by_id(Ulid::from_u128(8_627)),
                "ignore by_id COUNT",
            ),
        )
        .expect("ignore by_id COUNT should succeed")
    });

    assert_eq!(
        count, 0,
        "missing by_id COUNT should return zero under ignore mode"
    );
    assert_eq!(
        scanned, 1,
        "missing by_id COUNT should evaluate exactly one candidate key"
    );
}

#[test]
fn aggregate_path_by_id_count_strict_missing_surfaces_corruption_error() {
    seed_simple_entities(&[8_628]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let err = execute_count_terminal(
        &load,
        plan_from_query(
            Query::<SimpleEntity>::new(MissingRowPolicy::Error).by_id(Ulid::from_u128(8_629)),
            "strict by_id COUNT",
        ),
    )
    .expect_err("strict by_id COUNT should fail when row is missing");

    assert_eq!(
        err.class,
        ErrorClass::Corruption,
        "strict by_id COUNT missing row should classify as corruption",
    );
}

#[test]
fn aggregate_path_by_id_exists_strict_missing_surfaces_corruption_error() {
    seed_simple_entities(&[8_631]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let err = execute_exists_terminal(
        &load,
        plan_from_query(
            Query::<SimpleEntity>::new(MissingRowPolicy::Error).by_id(Ulid::from_u128(8_632)),
            "strict by_id EXISTS",
        ),
    )
    .expect_err("strict by_id EXISTS should fail when row is missing");

    assert_eq!(
        err.class,
        ErrorClass::Corruption,
        "strict by_id EXISTS missing row should classify as corruption",
    );
}

#[test]
fn aggregate_path_by_ids_window_shape_with_duplicates_count_and_exists_match_execute() {
    seed_simple_entities(&[8_641, 8_642, 8_643, 8_644, 8_645]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    execute_count_exists_window_parity(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_ids([
                    Ulid::from_u128(8_645),
                    Ulid::from_u128(8_642),
                    Ulid::from_u128(8_642),
                    Ulid::from_u128(8_644),
                    Ulid::from_u128(8_641),
                ])
                .order_by("id")
                .offset(1)
                .limit(2)
        },
        "by_ids windowed + duplicates shape",
    );
}

#[test]
fn aggregate_path_by_ids_strict_missing_surfaces_corruption_error() {
    seed_simple_entities(&[8_661]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let err = execute_count_terminal(
        &load,
        plan_from_query(
            Query::<SimpleEntity>::new(MissingRowPolicy::Error)
                .by_ids([Ulid::from_u128(8_662)])
                .order_by("id"),
            "strict by_ids COUNT",
        ),
    )
    .expect_err("strict by_ids COUNT should fail when row is missing");

    assert_eq!(
        err.class,
        ErrorClass::Corruption,
        "strict by_ids COUNT missing row should classify as corruption",
    );
}

#[test]
fn aggregate_path_count_full_scan_window_scans_offset_plus_limit() {
    seed_simple_entities(&[8_671, 8_672, 8_673, 8_674, 8_675, 8_676, 8_677]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        execute_count_terminal(
            &load,
            plan_from_query(
                Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                    .order_by("id")
                    .offset(2)
                    .limit(2),
                "full-scan COUNT",
            ),
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
fn aggregate_path_count_key_range_window_scans_offset_plus_limit() {
    seed_simple_entities(&[8_681, 8_682, 8_683, 8_684, 8_685, 8_686, 8_687]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let mut logical_plan = AccessPlannedQuery::new(
        AccessPath::KeyRange {
            start: Value::Ulid(Ulid::from_u128(8_682)),
            end: Value::Ulid(Ulid::from_u128(8_686)),
        },
        MissingRowPolicy::Ignore,
    );
    logical_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    logical_plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        execute_count_terminal(&load, ExecutablePlan::<SimpleEntity>::new(logical_plan))
            .expect("key-range COUNT should succeed")
    });

    assert_eq!(count, 2, "key-range COUNT should honor the page window");
    assert_eq!(
        scanned, 3,
        "key-range COUNT should scan exactly offset + limit keys"
    );
}

#[test]
fn aggregate_path_exists_full_scan_window_scans_offset_plus_one() {
    seed_simple_entities(&[8_691, 8_692, 8_693, 8_694, 8_695, 8_696, 8_697]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (exists, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        execute_exists_terminal(
            &load,
            plan_from_query(
                Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                    .order_by("id")
                    .offset(2),
                "full-scan EXISTS",
            ),
        )
        .expect("full-scan EXISTS should succeed")
    });

    assert!(exists, "full-scan EXISTS window should find a matching row");
    assert_eq!(
        scanned, 3,
        "full-scan EXISTS should scan exactly offset + 1 keys"
    );
}

#[test]
fn aggregate_path_exists_index_range_window_scans_offset_plus_one() {
    seed_unique_index_range_entities(&[
        (8_701, 100),
        (8_702, 101),
        (8_703, 102),
        (8_704, 103),
        (8_705, 104),
        (8_706, 105),
    ]);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);

    let mut logical_plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            UNIQUE_INDEX_RANGE_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(101)),
            Bound::Excluded(Value::Uint(106)),
        ),
        MissingRowPolicy::Ignore,
    );
    logical_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("code".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical_plan.scalar_plan_mut().page = Some(PageSpec {
        limit: None,
        offset: 2,
    });

    let (exists, scanned) = capture_rows_scanned_for_entity(UniqueIndexRangeEntity::PATH, || {
        execute_exists_terminal(
            &load,
            ExecutablePlan::<UniqueIndexRangeEntity>::new(logical_plan),
        )
        .expect("index-range EXISTS should succeed")
    });

    assert!(
        exists,
        "index-range EXISTS window should find a matching row"
    );
    assert_eq!(
        scanned, 3,
        "index-range EXISTS should scan exactly offset + 1 keys"
    );
}

#[test]
fn aggregate_path_union_and_intersection_count_and_exists_match_execute() {
    seed_simple_entities(&[8_711, 8_712, 8_713, 8_714, 8_715, 8_716]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let union_predicate = Predicate::Or(vec![
        id_in_predicate(&[8_711, 8_712, 8_713, 8_714]),
        id_in_predicate(&[8_713, 8_714, 8_715, 8_716]),
    ]);
    execute_count_exists_window_parity(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .filter(union_predicate.clone())
                .order_by("id")
                .offset(1)
                .limit(4)
        },
        "union path",
    );

    let intersection_predicate = Predicate::And(vec![
        id_in_predicate(&[8_711, 8_712, 8_713, 8_714]),
        id_in_predicate(&[8_713, 8_714, 8_715, 8_716]),
    ]);
    execute_count_exists_window_parity(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .filter(intersection_predicate.clone())
                .order_by_desc("id")
                .limit(2)
        },
        "intersection path",
    );
}

#[test]
fn aggregate_path_composite_count_direct_path_scan_does_not_exceed_fallback() {
    let (first, second) = composite_key_sets_with_base(COMPOSITE_COUNT_BASE);
    assert_composite_terminal_direct_path_scan_does_not_exceed_fallback(
        &phase_rows_with_base(COMPOSITE_COUNT_BASE),
        first,
        second,
        CompositeTerminal::Count,
        "COUNT",
    );
}

#[test]
fn aggregate_path_composite_exists_direct_path_scan_does_not_exceed_fallback() {
    let (first, second) = composite_key_sets_with_base(COMPOSITE_EXISTS_BASE);
    assert_composite_terminal_direct_path_scan_does_not_exceed_fallback(
        &phase_rows_with_base(COMPOSITE_EXISTS_BASE),
        first,
        second,
        CompositeTerminal::Exists,
        "EXISTS",
    );
}

#[test]
fn aggregate_path_index_range_shape_count_and_exists_match_execute() {
    seed_unique_index_range_entities(&[
        (8_901, 100),
        (8_902, 101),
        (8_903, 102),
        (8_904, 103),
        (8_905, 104),
        (8_906, 105),
    ]);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let range_predicate = u32_range_predicate("code", 101, 105);

    execute_count_exists_window_parity(
        &load,
        || {
            Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
                .filter(range_predicate.clone())
                .order_by_desc("code")
                .offset(1)
                .limit(2)
        },
        "index-range shape",
    );
}

#[test]
fn aggregate_path_strict_consistency_count_and_exists_match_execute() {
    seed_simple_entities(&[9_001, 9_002, 9_003, 9_004, 9_005]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    execute_count_exists_window_parity(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Error)
                .order_by_desc("id")
                .offset(1)
                .limit(3)
        },
        "strict consistency",
    );
}

#[test]
fn aggregate_path_limit_zero_window_count_and_exists_match_execute() {
    seed_simple_entities(&[9_101, 9_102, 9_103, 9_104]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    execute_count_exists_window_parity(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .offset(2)
                .limit(0)
        },
        "limit zero window",
    );
}

#[test]
fn aggregate_path_secondary_exists_window_preserves_missing_ok_scan_safety() {
    seed_pushdown_entities(&[
        (8_811, 7, 10),
        (8_812, 7, 20),
        (8_813, 7, 30),
        (8_814, 7, 40),
        (8_815, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    let (exists, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        execute_exists_terminal(
            &load,
            plan_from_query(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(group_seven.clone())
                    .order_by("rank")
                    .offset(2),
                "secondary-index EXISTS window",
            ),
        )
        .expect("secondary-index EXISTS window should succeed")
    });

    assert!(
        exists,
        "secondary-index EXISTS window should find a matching row"
    );
    assert_eq!(
        scanned, 4,
        "secondary-index EXISTS window should scan one stale candidate plus offset + 1 live ordered rows under ignore safety",
    );
}

#[test]
fn aggregate_path_secondary_exists_strict_missing_surfaces_corruption_error() {
    seed_pushdown_entities(&[(8_821, 7, 10), (8_822, 7, 20), (8_823, 7, 30)]);
    remove_pushdown_row_data(8_821);

    let err = execute_exists_terminal(
        &LoadExecutor::<PushdownParityEntity>::new(DB, false),
        secondary_group_rank_order_plan(MissingRowPolicy::Error, OrderDirection::Asc, 0),
    )
    .expect_err("strict secondary-index EXISTS should fail when row is missing");

    assert_eq!(
        err.class,
        ErrorClass::Corruption,
        "strict secondary-index EXISTS missing row should classify as corruption",
    );
}

#[test]
fn aggregate_path_secondary_covering_exists_matches_materialized_parity_with_stale_keys() {
    seed_stale_secondary_rows(
        &[
            (8_851, 7, 10),
            (8_852, 7, 20),
            (8_853, 7, 30),
            (8_854, 7, 40),
            (8_855, 8, 50),
        ],
        &[8_851],
    );
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let fast_path_exists = execute_exists_terminal(
        &load,
        plan_from_query(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7)),
            "secondary-index covering EXISTS fast-path plan",
        ),
    )
    .expect("secondary-index covering EXISTS fast path should succeed");
    let forced_materialized_exists = execute_exists_terminal(
        &load,
        plan_from_query(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("rank"),
            "forced materialized EXISTS",
        ),
    )
    .expect("secondary-index forced materialized EXISTS should succeed");
    let canonical_materialized_exists = !load
        .execute(plan_from_query(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("rank"),
            "materialized EXISTS baseline",
        ))
        .expect("secondary-index materialized EXISTS baseline should succeed")
        .is_empty();

    assert_eq!(
        fast_path_exists, forced_materialized_exists,
        "secondary-index covering EXISTS must match forced materialized EXISTS under stale keys",
    );
    assert_eq!(
        fast_path_exists, canonical_materialized_exists,
        "secondary-index covering EXISTS must match canonical row-materialized EXISTS under stale keys",
    );
}

#[test]
fn aggregate_path_secondary_count_strict_missing_surfaces_corruption_error() {
    seed_pushdown_entities(&[(8_921, 7, 10), (8_922, 7, 20), (8_923, 7, 30)]);
    remove_pushdown_row_data(8_921);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let err = execute_count_terminal(
        &load,
        plan_from_query(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Error)
                .filter(u32_eq_predicate("group", 7))
                .order_by("rank"),
            "strict secondary-index COUNT",
        ),
    )
    .expect_err("strict secondary-index COUNT should fail when row is missing");

    assert_eq!(
        err.class,
        ErrorClass::Corruption,
        "strict secondary-index COUNT missing row should classify as corruption",
    );
    assert!(
        err.message.contains("missing row"),
        "strict secondary-index COUNT should preserve missing-row error context",
    );
}

#[test]
fn aggregate_path_secondary_covering_count_matches_materialized_parity_with_stale_keys() {
    seed_stale_secondary_rows(
        &[
            (8_931, 7, 10),
            (8_932, 7, 20),
            (8_933, 7, 30),
            (8_934, 7, 40),
            (8_935, 8, 50),
        ],
        &[8_931],
    );
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let fast_path_count = execute_count_terminal(
        &load,
        plan_from_query(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7)),
            "secondary-index covering COUNT fast-path plan",
        ),
    )
    .expect("secondary-index covering COUNT fast path should succeed");
    let forced_materialized_count = execute_count_terminal(
        &load,
        plan_from_query(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("rank"),
            "forced materialized COUNT",
        ),
    )
    .expect("secondary-index forced materialized COUNT should succeed");
    let canonical_materialized_count = load
        .execute(plan_from_query(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("rank"),
            "materialized COUNT baseline",
        ))
        .expect("secondary-index materialized COUNT baseline should succeed")
        .count();

    assert_eq!(
        fast_path_count, forced_materialized_count,
        "secondary-index covering COUNT must match forced materialized COUNT under stale keys",
    );
    assert_eq!(
        fast_path_count, canonical_materialized_count,
        "secondary-index covering COUNT must match canonical row-materialized COUNT under stale keys",
    );
}

#[test]
fn aggregate_path_secondary_index_range_count_missing_ok_stale_preserves_parity() {
    seed_stale_secondary_rows(
        &[
            (8_951, 7, 10),
            (8_952, 7, 20),
            (8_953, 7, 30),
            (8_954, 7, 40),
        ],
        &[8_951],
    );
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let (count_from_pushdown, rows_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_count_terminal(
                &load,
                secondary_group_rank_index_range_count_plan(MissingRowPolicy::Ignore, 1, 2),
            )
            .expect("index-range COUNT pushdown path should succeed")
        });
    let expected_count = load
        .execute(secondary_group_rank_index_range_count_plan(
            MissingRowPolicy::Ignore,
            1,
            2,
        ))
        .expect("canonical execute baseline should succeed")
        .count();

    assert_eq!(
        count_from_pushdown, expected_count,
        "bounded index-range COUNT pushdown must preserve canonical window parity under stale-leading ignore mode",
    );
    assert_eq!(
        rows_scanned, 3,
        "bounded index-range COUNT should preserve configured fetch-window scan accounting",
    );
}
