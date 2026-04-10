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
    model::entity::resolve_field_slot,
    serialize::serialized_len,
    traits::{EntityKind, EntityValue},
    types::{Id, Ulid},
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

// Sum persisted row payload lengths for the exact effective execute window.
fn persisted_payload_bytes_for_simple_ids(ids: impl IntoIterator<Item = Id<SimpleEntity>>) -> u64 {
    ids.into_iter().fold(0u64, |acc, id| {
        let raw_key = DataKey::try_new::<SimpleEntity>(id.key())
            .expect("simple data key should build")
            .to_raw()
            .expect("simple data key should encode");
        let row = DATA_STORE.with(|store| {
            store
                .borrow()
                .get(&raw_key)
                .expect("simple row should exist for bytes parity")
        });

        acc.saturating_add(u64::try_from(row.len()).unwrap_or(u64::MAX))
    })
}

fn persisted_payload_bytes_for_pushdown_ids(
    ids: impl IntoIterator<Item = Id<PushdownParityEntity>>,
) -> u64 {
    ids.into_iter().fold(0u64, |acc, id| {
        let raw_key = DataKey::try_new::<PushdownParityEntity>(id.key())
            .expect("pushdown data key should build")
            .to_raw()
            .expect("pushdown data key should encode");
        let row = DATA_STORE.with(|store| {
            store
                .borrow()
                .get(&raw_key)
                .expect("pushdown row should exist for bytes parity")
        });

        acc.saturating_add(u64::try_from(row.len()).unwrap_or(u64::MAX))
    })
}

fn serialized_field_payload_bytes_for_pushdown_rows(
    response: &EntityResponse<PushdownParityEntity>,
    field: &str,
) -> u64 {
    response.iter().fold(0u64, |acc, row| {
        let value = match field {
            "group" => Value::Uint(u64::from(row.entity_ref().group)),
            "rank" => Value::Uint(u64::from(row.entity_ref().rank)),
            "label" => Value::Text(row.entity_ref().label.clone()),
            other => panic!("pushdown field should resolve for bytes parity: {other}"),
        };
        let value_len = serialized_len(&value).expect("pushdown field value should encode");

        acc.saturating_add(u64::try_from(value_len).unwrap_or(u64::MAX))
    })
}

fn planned_slot<E>(field: &str) -> crate::db::query::plan::FieldSlot
where
    E: EntityKind,
{
    let resolved_index = resolve_field_slot(E::MODEL, field);
    let index = resolved_index.unwrap_or(0);

    crate::db::query::plan::FieldSlot {
        index,
        field: field.to_string(),
        kind: resolved_index.and_then(|index| E::MODEL.fields.get(index).map(|field| field.kind)),
    }
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

fn execute_count_window_parity<E>(
    load: &LoadExecutor<E>,
    build_query: impl Fn() -> Query<E>,
    label: &str,
) where
    E: PersistedRow + EntityKind + EntityValue,
{
    // Phase 1: materialize the canonical execute window and derive the
    // expected COUNT from the same ordered response rows.
    let expected_count = load
        .execute(plan_from_query(build_query(), label))
        .unwrap_or_else(|err| panic!("{label} execute baseline should succeed: {err}"))
        .count();

    // Phase 2: keep the scalar COUNT terminal aligned with that same window.
    let count = execute_count_terminal(load, plan_from_query(build_query(), label))
        .unwrap_or_else(|err| panic!("{label} COUNT terminal should succeed: {err}"));

    assert_eq!(
        count, expected_count,
        "{label} COUNT must preserve canonical execute-window cardinality",
    );
}

fn execute_simple_count_exists_bytes_window_parity(
    load: &LoadExecutor<SimpleEntity>,
    build_query: impl Fn() -> Query<SimpleEntity>,
    label: &str,
) {
    // Phase 1: materialize the canonical ordered response and derive the
    // scalar/count window plus persisted-row bytes from those exact ids.
    let expected_response = load
        .execute(plan_from_query(build_query(), label))
        .unwrap_or_else(|err| panic!("{label} execute baseline should succeed: {err}"));
    let expected_count = expected_response.count();
    let expected_exists = !expected_response.is_empty();
    let expected_bytes = persisted_payload_bytes_for_simple_ids(expected_response.ids());

    // Phase 2: keep COUNT, EXISTS, and bytes aligned with that same ordered
    // response window for the identical logical query.
    let count = execute_count_terminal(load, plan_from_query(build_query(), label))
        .unwrap_or_else(|err| panic!("{label} COUNT terminal should succeed: {err}"));
    let exists = execute_exists_terminal(load, plan_from_query(build_query(), label))
        .unwrap_or_else(|err| panic!("{label} EXISTS terminal should succeed: {err}"));
    let bytes = load
        .bytes(plan_from_query(build_query(), label))
        .unwrap_or_else(|err| panic!("{label} bytes terminal should succeed: {err}"));

    assert_eq!(
        count, expected_count,
        "{label} COUNT must preserve canonical execute-window cardinality",
    );
    assert_eq!(
        exists, expected_exists,
        "{label} EXISTS must preserve canonical execute-window emptiness",
    );
    assert_eq!(
        bytes, expected_bytes,
        "{label} bytes terminal must preserve canonical execute-window payload parity",
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
fn aggregate_path_bytes_parity_ordered_page_window_asc() {
    seed_simple_entities(&[8_981, 8_982, 8_983, 8_984, 8_985, 8_986, 8_987, 8_988]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let build_query = || {
        Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .offset(2)
            .limit(3)
    };

    let expected_bytes = persisted_payload_bytes_for_simple_ids(
        load.execute(plan_from_query(build_query(), "ordered ASC bytes parity"))
            .expect("ordered ASC bytes parity execute should succeed")
            .ids(),
    );
    let bytes = load
        .bytes(plan_from_query(build_query(), "ordered ASC bytes parity"))
        .expect("ordered ASC bytes terminal should succeed");

    assert_eq!(
        bytes, expected_bytes,
        "ordered ASC bytes window should match canonical execute parity",
    );
}

#[test]
fn aggregate_path_bytes_parity_ordered_page_window_desc() {
    seed_simple_entities(&[8_901, 8_902, 8_903, 8_904, 8_905, 8_906, 8_907, 8_908]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let build_query = || {
        Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    let expected_bytes = persisted_payload_bytes_for_simple_ids(
        load.execute(plan_from_query(build_query(), "ordered DESC bytes parity"))
            .expect("ordered DESC bytes parity execute should succeed")
            .ids(),
    );
    let bytes = load
        .bytes(plan_from_query(build_query(), "ordered DESC bytes parity"))
        .expect("ordered DESC bytes terminal should succeed");

    assert_eq!(
        bytes, expected_bytes,
        "ordered DESC bytes window should match canonical execute parity",
    );
}

#[test]
fn aggregate_path_bytes_key_range_window_parity_desc() {
    seed_simple_entities(&[8_989, 8_990, 8_991, 8_992, 8_993, 8_994, 8_995]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let build_plan = || {
        let mut logical_plan = AccessPlannedQuery::new(
            AccessPath::KeyRange {
                start: Value::Ulid(Ulid::from_u128(8_990)),
                end: Value::Ulid(Ulid::from_u128(8_994)),
            },
            MissingRowPolicy::Ignore,
        );
        logical_plan.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        logical_plan.scalar_plan_mut().page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });

        ExecutablePlan::<SimpleEntity>::new(logical_plan)
    };

    let expected_bytes = persisted_payload_bytes_for_simple_ids(
        load.execute(build_plan())
            .expect("key-range bytes parity execute should succeed")
            .ids(),
    );
    let bytes = load
        .bytes(build_plan())
        .expect("key-range DESC bytes terminal should succeed");

    assert_eq!(
        bytes, expected_bytes,
        "key-range DESC bytes window should match canonical execute parity",
    );
}

#[test]
fn aggregate_path_bytes_path_parity_index_prefix_and_full_scan_equivalent_rows() {
    seed_pushdown_entities(&[
        (8_971, 7, 5),
        (8_972, 7, 10),
        (8_973, 7, 20),
        (8_974, 8, 40),
        (8_975, 7, 30),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let mut index_logical = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_PARITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    index_logical.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let index_plan = ExecutablePlan::<PushdownParityEntity>::new(index_logical);

    let mut full_scan_logical =
        AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore);
    full_scan_logical.scalar_plan_mut().predicate = Some(u32_eq_predicate("group", 7));
    full_scan_logical.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let full_scan_plan = ExecutablePlan::<PushdownParityEntity>::new(full_scan_logical);

    assert_eq!(
        execution_root_node_type(&index_plan),
        ExplainExecutionNodeType::IndexPrefixScan,
        "group equality filter should route through index-prefix access",
    );
    assert_eq!(
        execution_root_node_type(&full_scan_plan),
        ExplainExecutionNodeType::FullScan,
        "group equality filter under residual full scan should route through full scan",
    );

    let index_bytes = load
        .bytes(index_plan)
        .expect("index-prefix bytes terminal should succeed");
    let full_scan_bytes = load
        .bytes(full_scan_plan)
        .expect("full-scan bytes terminal should succeed");

    assert_eq!(
        index_bytes, full_scan_bytes,
        "equivalent index-prefix/full-scan row sets should yield identical bytes totals",
    );

    let expected_bytes = persisted_payload_bytes_for_pushdown_ids(
        load.execute(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("rank")
                .plan()
                .map(ExecutablePlan::from)
                .expect("bytes expected-baseline plan should build"),
        )
        .expect("bytes expected-baseline execute should succeed")
        .ids(),
    );
    assert_eq!(index_bytes, expected_bytes);
    assert_eq!(full_scan_bytes, expected_bytes);
}

#[test]
fn aggregate_path_bytes_by_path_parity_index_prefix_and_full_scan_equivalent_rows() {
    seed_pushdown_entities(&[
        (8_981, 7, 5),
        (8_982, 7, 10),
        (8_983, 7, 20),
        (8_984, 8, 40),
        (8_985, 7, 30),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let mut index_logical = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_PARITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    index_logical.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let index_plan = ExecutablePlan::<PushdownParityEntity>::new(index_logical);

    let mut full_scan_logical =
        AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore);
    full_scan_logical.scalar_plan_mut().predicate = Some(u32_eq_predicate("group", 7));
    full_scan_logical.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let full_scan_plan = ExecutablePlan::<PushdownParityEntity>::new(full_scan_logical);

    assert_eq!(
        execution_root_node_type(&index_plan),
        ExplainExecutionNodeType::IndexPrefixScan,
        "group equality filter should route through index-prefix access",
    );
    assert_eq!(
        execution_root_node_type(&full_scan_plan),
        ExplainExecutionNodeType::FullScan,
        "group equality filter under residual full scan should route through full scan",
    );

    let index_bytes = load
        .bytes_by_slot(index_plan, planned_slot::<PushdownParityEntity>("rank"))
        .expect("index-prefix bytes_by(rank) terminal should succeed");
    let full_scan_bytes = load
        .bytes_by_slot(full_scan_plan, planned_slot::<PushdownParityEntity>("rank"))
        .expect("full-scan bytes_by(rank) terminal should succeed");

    assert_eq!(
        index_bytes, full_scan_bytes,
        "equivalent index-prefix/full-scan row sets should yield identical bytes_by(rank) totals",
    );

    let expected_bytes = serialized_field_payload_bytes_for_pushdown_rows(
        &load
            .execute(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(u32_eq_predicate("group", 7))
                    .order_by("rank")
                    .plan()
                    .map(ExecutablePlan::from)
                    .expect("bytes_by expected-baseline plan should build"),
            )
            .expect("bytes_by expected-baseline execute should succeed"),
        "rank",
    );
    assert_eq!(index_bytes, expected_bytes);
    assert_eq!(full_scan_bytes, expected_bytes);
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
fn aggregate_path_by_id_windowed_count_scans_one_candidate_key() {
    seed_simple_entities(&[8_621]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        execute_count_terminal(
            &load,
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_id(Ulid::from_u128(8_621))
                .order_by("id")
                .offset(1)
                .limit(1)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("by-id windowed COUNT plan should build"),
        )
        .expect("by-id windowed COUNT should succeed")
    });

    assert_eq!(count, 0, "offset window should exclude the only row");
    assert_eq!(
        scanned, 1,
        "single-key windowed COUNT should scan only one candidate key",
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
fn aggregate_path_index_range_ineligible_pushdown_shape_count_and_exists_match_execute() {
    seed_unique_index_range_entities(&[
        (9_811, 200),
        (9_812, 201),
        (9_813, 202),
        (9_814, 203),
        (9_815, 204),
        (9_816, 205),
    ]);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let range_predicate = u32_range_predicate("code", 201, 206);

    execute_count_exists_window_parity(
        &load,
        || {
            Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
                .filter(range_predicate.clone())
                .order_by("label")
                .offset(1)
                .limit(2)
        },
        "index-range ineligible pushdown shape",
    );
}

#[test]
fn aggregate_path_distinct_offset_probe_hint_suppression_count_and_exists_match_execute() {
    seed_simple_entities(&[9_501, 9_502]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let duplicate_front_predicate = Predicate::Or(vec![
        id_in_predicate(&[9_501]),
        id_in_predicate(&[9_501, 9_502]),
    ]);

    execute_count_exists_window_parity(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .filter(duplicate_front_predicate.clone())
                .distinct()
                .order_by("id")
                .offset(1)
        },
        "distinct + offset probe-hint suppression",
    );
}

#[test]
fn aggregate_path_secondary_index_strict_prefilter_count_and_exists_match_execute() {
    seed_stale_secondary_rows(
        &[
            (10_101, 7, 3),
            (10_102, 7, 7),
            (10_103, 7, 11),
            (10_104, 7, 19),
            (10_105, 7, 23),
            (10_106, 7, 41),
            (10_301, 8, 3),
            (10_302, 8, 7),
            (10_303, 8, 19),
        ],
        &[],
    );
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let strict_filter = Predicate::And(vec![
        u32_eq_predicate("group", 7),
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::In,
            Value::List(
                [3_u32, 7, 19, 23, 41]
                    .into_iter()
                    .map(|value| Value::Uint(u64::from(value)))
                    .collect(),
            ),
            CoercionId::Strict,
        )),
    ]);

    for (direction_desc, distinct) in [(false, false), (false, true), (true, false), (true, true)] {
        execute_count_exists_window_parity(
            &load,
            || {
                let mut query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
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
fn aggregate_path_count_pushdown_contract_matrix_preserves_parity() {
    // Phase 1: full-scan ordered windows preserve canonical COUNT parity.
    seed_simple_entities(&[9_701, 9_702, 9_703, 9_704, 9_705]);
    let simple_load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let full_scan_query = || {
        Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .offset(1)
            .limit(2)
    };
    let full_scan_plan = plan_from_query(full_scan_query(), "full-scan count matrix");
    assert!(matches!(
        execution_root_node_type(&full_scan_plan),
        ExplainExecutionNodeType::FullScan
    ));
    execute_count_window_parity(&simple_load, full_scan_query, "count matrix full-scan");

    // Phase 2: residual-filter full-scan remains materialized but keeps COUNT parity.
    seed_phase_entities(&[(9_801, 1), (9_802, 2), (9_803, 2), (9_804, 3)]);
    let phase_load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let residual_filter_query = || {
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("rank", 2))
            .order_by("id")
    };
    let residual_filter_plan = plan_from_query(residual_filter_query(), "residual-filter count");
    assert!(matches!(
        execution_root_node_type(&residual_filter_plan),
        ExplainExecutionNodeType::FullScan
    ));
    execute_count_window_parity(
        &phase_load,
        residual_filter_query,
        "count matrix residual-filter full-scan",
    );

    // Phase 3: stale-leading secondary-order windows preserve parity on the index path.
    seed_pushdown_entities(&[
        (9_901, 7, 10),
        (9_902, 7, 20),
        (9_903, 7, 30),
        (9_904, 7, 40),
    ]);
    remove_pushdown_row_data(9_901);
    let pushdown_load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let secondary_index_query = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
    };
    let secondary_index_plan =
        plan_from_query(secondary_index_query(), "secondary-index count matrix");
    assert!(matches!(
        execution_root_node_type(&secondary_index_plan),
        ExplainExecutionNodeType::IndexPrefixScan
    ));
    execute_count_window_parity(
        &pushdown_load,
        secondary_index_query,
        "count matrix secondary-index",
    );

    // Phase 4: composite OR shapes preserve canonical count parity.
    seed_simple_entities(&[9_951, 9_952, 9_953, 9_954, 9_955, 9_956]);
    let composite_load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let composite_predicate = Predicate::Or(vec![
        id_in_predicate(&[9_951, 9_952, 9_953, 9_954]),
        id_in_predicate(&[9_953, 9_954, 9_955, 9_956]),
    ]);
    let composite_query = || {
        Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .filter(composite_predicate.clone())
            .order_by("id")
    };
    let composite_plan = plan_from_query(composite_query(), "composite count matrix");
    assert!(matches!(
        execution_root_node_type(&composite_plan),
        ExplainExecutionNodeType::Union | ExplainExecutionNodeType::Intersection
    ));
    execute_count_window_parity(
        &composite_load,
        composite_query,
        "count matrix composite OR",
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
fn aggregate_path_secondary_index_order_shape_count_and_exists_match_execute() {
    seed_pushdown_entities(&[
        (8_801, 7, 40),
        (8_802, 7, 10),
        (8_803, 7, 30),
        (8_804, 7, 20),
        (8_805, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    execute_count_exists_window_parity(
        &load,
        || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("rank")
                .offset(1)
                .limit(2)
        },
        "secondary-index order shape",
    );
}

#[test]
fn aggregate_path_secondary_index_order_shape_desc_with_explicit_pk_tie_break_count_and_exists_match_execute()
 {
    seed_pushdown_entities(&[
        (8_811, 7, 40),
        (8_812, 7, 10),
        (8_813, 7, 30),
        (8_814, 7, 20),
        (8_815, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    execute_count_exists_window_parity(
        &load,
        || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("rank")
                .order_by_desc("id")
                .offset(1)
                .limit(2)
        },
        "secondary-index order shape DESC with explicit PK tie-break",
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

#[test]
fn aggregate_path_distinct_asc_count_exists_and_bytes_match_execute() {
    seed_simple_entities(&[8_301, 8_302, 8_303, 8_304, 8_305, 8_306]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    execute_simple_count_exists_bytes_window_parity(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .filter(Predicate::Or(vec![
                    id_in_predicate(&[8_301, 8_302, 8_303, 8_304]),
                    id_in_predicate(&[8_303, 8_304, 8_305, 8_306]),
                ]))
                .distinct()
                .order_by("id")
                .offset(1)
                .limit(3)
        },
        "distinct ASC",
    );
}

#[test]
fn aggregate_path_distinct_desc_count_exists_and_bytes_match_execute() {
    seed_simple_entities(&[8_401, 8_402, 8_403, 8_404, 8_405, 8_406]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    execute_simple_count_exists_bytes_window_parity(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .filter(Predicate::Or(vec![
                    id_in_predicate(&[8_401, 8_402, 8_403, 8_404]),
                    id_in_predicate(&[8_403, 8_404, 8_405, 8_406]),
                ]))
                .distinct()
                .order_by_desc("id")
                .offset(1)
                .limit(3)
        },
        "distinct DESC",
    );
}
