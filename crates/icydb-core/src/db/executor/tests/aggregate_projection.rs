//! Module: db::executor::tests::aggregate_projection
//! Covers aggregate projection behavior and grouped output materialization.
//! Does not own: production aggregate behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use super::*;
use crate::{
    db::{
        access::AccessPath,
        data::DataKey,
        executor::{
            ExecutablePlan,
            aggregate::{AggregateKind, ScalarProjectionBoundaryRequest},
        },
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::{
            intent::Query,
            plan::{
                AccessPlannedQuery, FieldSlot as PlannedFieldSlot, OrderDirection, OrderSpec,
                PageSpec,
            },
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
use std::ops::Bound;

#[derive(Default)]
struct AggregateProjectionCaptureSink {
    events: RefCell<Vec<MetricsEvent>>,
}

impl AggregateProjectionCaptureSink {
    fn into_events(self) -> Vec<MetricsEvent> {
        self.events.into_inner()
    }
}

impl MetricsSink for AggregateProjectionCaptureSink {
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
    let sink = AggregateProjectionCaptureSink::default();
    let output = with_metrics_sink(&sink, run);
    let rows_scanned = rows_scanned_for_entity(&sink.into_events(), entity_path);

    (output, rows_scanned)
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

fn execute_projection_count_distinct_boundary<E>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
    target_field: PlannedFieldSlot,
) -> Result<u32, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_projection_boundary(
        plan,
        target_field,
        ScalarProjectionBoundaryRequest::CountDistinct,
    )?
    .into_count()
}

fn execute_projection_values_boundary<E>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
    target_field: PlannedFieldSlot,
) -> Result<Vec<Value>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_projection_boundary(
        plan,
        target_field,
        ScalarProjectionBoundaryRequest::Values,
    )?
    .into_values()
}

fn execute_projection_distinct_values_boundary<E>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
    target_field: PlannedFieldSlot,
) -> Result<Vec<Value>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_projection_boundary(
        plan,
        target_field,
        ScalarProjectionBoundaryRequest::DistinctValues,
    )?
    .into_values()
}

fn execute_projection_values_with_ids_boundary<E>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
    target_field: PlannedFieldSlot,
) -> Result<Vec<(Id<E>, Value)>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_projection_boundary(
        plan,
        target_field,
        ScalarProjectionBoundaryRequest::ValuesWithIds,
    )?
    .into_values_with_ids::<E>()
}

fn execute_projection_terminal_value_boundary<E>(
    load: &LoadExecutor<E>,
    plan: ExecutablePlan<E>,
    target_field: PlannedFieldSlot,
    terminal_kind: AggregateKind,
) -> Result<Option<Value>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_projection_boundary(
        plan,
        target_field,
        ScalarProjectionBoundaryRequest::TerminalValue { terminal_kind },
    )?
    .into_terminal_value()
}

fn u32_eq_predicate(field: &str, value: u32) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::Eq,
        Value::Uint(u64::from(value)),
        CoercionId::Strict,
    ))
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

fn seed_phase_entities_custom(rows: Vec<PhaseEntity>) {
    reset_store();
    let save = SaveExecutor::<PhaseEntity>::new(DB, false);

    for row in rows {
        save.insert(row)
            .expect("aggregate phase custom seed save should succeed");
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

fn expected_values_by_rank(response: &EntityResponse<PushdownParityEntity>) -> Vec<Value> {
    response
        .iter()
        .map(|row| Value::Uint(u64::from(row.entity_ref().rank)))
        .collect()
}

fn expected_values_by_rank_with_ids(
    response: &EntityResponse<PushdownParityEntity>,
) -> Vec<(Id<PushdownParityEntity>, Value)> {
    response
        .iter()
        .map(|row| (row.id(), Value::Uint(u64::from(row.entity_ref().rank))))
        .collect()
}

fn expected_distinct_values_by_rank(response: &EntityResponse<PushdownParityEntity>) -> Vec<Value> {
    let mut distinct = Vec::new();

    for value in expected_values_by_rank(response) {
        if distinct.iter().any(|existing| existing == &value) {
            continue;
        }

        distinct.push(value);
    }

    distinct
}

fn expected_first_value_by_rank(response: &EntityResponse<PushdownParityEntity>) -> Option<Value> {
    response
        .iter()
        .next()
        .map(|row| Value::Uint(u64::from(row.entity_ref().rank)))
}

fn expected_last_value_by_rank(response: &EntityResponse<PushdownParityEntity>) -> Option<Value> {
    response
        .iter()
        .last()
        .map(|row| Value::Uint(u64::from(row.entity_ref().rank)))
}

fn expected_count_distinct_by_rank(response: &EntityResponse<PushdownParityEntity>) -> u32 {
    u32::try_from(expected_distinct_values_by_rank(response).len()).unwrap_or(u32::MAX)
}

#[derive(Clone, Copy)]
enum OptionalFieldNullTerminal {
    TopKBy,
    BottomKBy,
    TopKByValues,
    BottomKByValues,
    TopKByWithIds,
    BottomKByWithIds,
}

#[derive(Clone, Copy)]
enum MissingFieldTerminal {
    TopKBy,
    TopKByValues,
    BottomKBy,
    BottomKByValues,
    TopKByWithIds,
    BottomKByWithIds,
}

fn seed_optional_field_null_values_fixture() {
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
}

fn optional_field_null_plan() -> ExecutablePlan<PhaseEntity> {
    Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .plan()
        .map(ExecutablePlan::from)
        .expect("optional-field null-semantics plan should build")
}

fn optional_field_null_baseline_error(
    load: &LoadExecutor<PhaseEntity>,
    terminal: OptionalFieldNullTerminal,
) -> InternalError {
    match terminal {
        OptionalFieldNullTerminal::TopKByWithIds | OptionalFieldNullTerminal::BottomKByWithIds => {
            execute_projection_values_with_ids_boundary(
                load,
                optional_field_null_plan(),
                planned_slot::<PhaseEntity>("opt_rank"),
            )
            .expect_err("values_by_with_ids(opt_rank) should reject null field values")
        }
        _ => execute_projection_values_boundary(
            load,
            optional_field_null_plan(),
            planned_slot::<PhaseEntity>("opt_rank"),
        )
        .expect_err("values_by(opt_rank) should reject null field values"),
    }
}

fn optional_field_null_terminal_error(
    load: &LoadExecutor<PhaseEntity>,
    terminal: OptionalFieldNullTerminal,
) -> InternalError {
    match terminal {
        OptionalFieldNullTerminal::TopKBy => load
            .top_k_by_slot(
                optional_field_null_plan(),
                planned_slot::<PhaseEntity>("opt_rank"),
                2,
            )
            .expect_err("top_k_by(opt_rank, 2) should reject null field values"),
        OptionalFieldNullTerminal::BottomKBy => load
            .bottom_k_by_slot(
                optional_field_null_plan(),
                planned_slot::<PhaseEntity>("opt_rank"),
                2,
            )
            .expect_err("bottom_k_by(opt_rank, 2) should reject null field values"),
        OptionalFieldNullTerminal::TopKByValues => load
            .top_k_by_values_slot(
                optional_field_null_plan(),
                planned_slot::<PhaseEntity>("opt_rank"),
                2,
            )
            .expect_err("top_k_by_values(opt_rank, 2) should reject null field values"),
        OptionalFieldNullTerminal::BottomKByValues => load
            .bottom_k_by_values_slot(
                optional_field_null_plan(),
                planned_slot::<PhaseEntity>("opt_rank"),
                2,
            )
            .expect_err("bottom_k_by_values(opt_rank, 2) should reject null field values"),
        OptionalFieldNullTerminal::TopKByWithIds => load
            .top_k_by_with_ids_slot(
                optional_field_null_plan(),
                planned_slot::<PhaseEntity>("opt_rank"),
                2,
            )
            .expect_err("top_k_by_with_ids(opt_rank, 2) should reject null field values"),
        OptionalFieldNullTerminal::BottomKByWithIds => load
            .bottom_k_by_with_ids_slot(
                optional_field_null_plan(),
                planned_slot::<PhaseEntity>("opt_rank"),
                2,
            )
            .expect_err("bottom_k_by_with_ids(opt_rank, 2) should reject null field values"),
    }
}

fn assert_optional_field_null_parity(terminal: OptionalFieldNullTerminal, label: &str) {
    seed_optional_field_null_values_fixture();
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let baseline_err = optional_field_null_baseline_error(&load, terminal);
    let terminal_err = optional_field_null_terminal_error(&load, terminal);

    assert_eq!(
        baseline_err.class,
        crate::error::ErrorClass::InvariantViolation,
        "{label} baseline projection should classify null-value mismatch as invariant violation",
    );
    assert_eq!(
        terminal_err.class,
        crate::error::ErrorClass::InvariantViolation,
        "{label} should classify null-value mismatch as invariant violation",
    );
    assert!(
        baseline_err
            .message
            .contains("aggregate target field value type mismatch"),
        "{label} baseline projection should expose type-mismatch reason for null values",
    );
    assert!(
        terminal_err
            .message
            .contains("aggregate target field value type mismatch"),
        "{label} should expose type-mismatch reason for null values",
    );
    assert!(
        baseline_err.message.contains("value=Null") && terminal_err.message.contains("value=Null"),
        "{label} should report null payload mismatch consistently with baseline projection",
    );
}

fn seed_missing_field_parity_fixture() {
    seed_pushdown_entities(&[(8_3381, 7, 10), (8_3382, 7, 20), (8_3383, 7, 30)]);
}

fn missing_field_parity_plan() -> ExecutablePlan<PushdownParityEntity> {
    Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(ExecutablePlan::from)
        .expect("missing-field parity plan should build")
}

fn missing_field_baseline_error(
    load: &LoadExecutor<PushdownParityEntity>,
    terminal: MissingFieldTerminal,
) -> InternalError {
    match terminal {
        MissingFieldTerminal::TopKByWithIds | MissingFieldTerminal::BottomKByWithIds => {
            execute_projection_values_with_ids_boundary(
                load,
                missing_field_parity_plan(),
                planned_slot::<PushdownParityEntity>("missing_field"),
            )
            .expect_err("values_by_with_ids(missing_field) should be rejected")
        }
        _ => execute_projection_values_boundary(
            load,
            missing_field_parity_plan(),
            planned_slot::<PushdownParityEntity>("missing_field"),
        )
        .expect_err("values_by(missing_field) should be rejected"),
    }
}

fn missing_field_terminal_error(
    load: &LoadExecutor<PushdownParityEntity>,
    terminal: MissingFieldTerminal,
) -> InternalError {
    match terminal {
        MissingFieldTerminal::TopKBy => load
            .top_k_by_slot(
                missing_field_parity_plan(),
                planned_slot::<PushdownParityEntity>("missing_field"),
                2,
            )
            .expect_err("top_k_by(missing_field, 2) should be rejected"),
        MissingFieldTerminal::TopKByValues => load
            .top_k_by_values_slot(
                missing_field_parity_plan(),
                planned_slot::<PushdownParityEntity>("missing_field"),
                2,
            )
            .expect_err("top_k_by_values(missing_field, 2) should be rejected"),
        MissingFieldTerminal::BottomKBy => load
            .bottom_k_by_slot(
                missing_field_parity_plan(),
                planned_slot::<PushdownParityEntity>("missing_field"),
                2,
            )
            .expect_err("bottom_k_by(missing_field, 2) should be rejected"),
        MissingFieldTerminal::BottomKByValues => load
            .bottom_k_by_values_slot(
                missing_field_parity_plan(),
                planned_slot::<PushdownParityEntity>("missing_field"),
                2,
            )
            .expect_err("bottom_k_by_values(missing_field, 2) should be rejected"),
        MissingFieldTerminal::TopKByWithIds => load
            .top_k_by_with_ids_slot(
                missing_field_parity_plan(),
                planned_slot::<PushdownParityEntity>("missing_field"),
                2,
            )
            .expect_err("top_k_by_with_ids(missing_field, 2) should be rejected"),
        MissingFieldTerminal::BottomKByWithIds => load
            .bottom_k_by_with_ids_slot(
                missing_field_parity_plan(),
                planned_slot::<PushdownParityEntity>("missing_field"),
                2,
            )
            .expect_err("bottom_k_by_with_ids(missing_field, 2) should be rejected"),
    }
}

fn assert_missing_field_terminal_parity(terminal: MissingFieldTerminal, label: &str) {
    seed_missing_field_parity_fixture();
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let baseline_err = missing_field_baseline_error(&load, terminal);
    let terminal_err = missing_field_terminal_error(&load, terminal);

    assert_eq!(
        terminal_err.class, baseline_err.class,
        "{label} should classify unknown-field failures the same way as baseline projection",
    );
    assert_eq!(
        terminal_err.origin, baseline_err.origin,
        "{label} should preserve unknown-field origin parity with baseline projection",
    );
    assert!(
        terminal_err
            .message
            .contains("unknown aggregate target field"),
        "{label} should surface the same unknown-field reason",
    );
}

#[test]
fn aggregate_projection_count_distinct_counts_window_values() {
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
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .limit(5)
            .plan()
            .map(ExecutablePlan::from)
            .expect("field-target count-distinct plan should build")
    };

    let expected_response = load
        .execute(build_plan())
        .expect("field-target count-distinct baseline execute should succeed");
    let distinct_count = execute_projection_count_distinct_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("count_distinct_by(rank) should succeed");
    let empty_window_count = execute_projection_count_distinct_boundary(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(50)
            .limit(5)
            .plan()
            .map(ExecutablePlan::from)
            .expect("empty-window count-distinct plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("empty-window count_distinct_by(rank) should succeed");

    assert_eq!(
        distinct_count,
        expected_count_distinct_by_rank(&expected_response),
        "count_distinct_by(rank) should match distinct values in the effective window",
    );
    assert_eq!(
        empty_window_count, 0,
        "count_distinct_by(rank) should return zero for empty windows",
    );
}

#[test]
fn aggregate_projection_count_distinct_supports_non_orderable_fields() {
    seed_phase_entities(&[(8_197, 10), (8_198, 20), (8_199, 10)]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let distinct_count = execute_projection_count_distinct_boundary(
        &load,
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(ExecutablePlan::from)
            .expect("non-orderable count-distinct plan should build"),
        planned_slot::<PhaseEntity>("tags"),
    )
    .expect("count_distinct_by(tags) should succeed");

    assert_eq!(
        distinct_count, 2,
        "count_distinct_by(tags) should support structured field equality",
    );
}

#[test]
fn aggregate_projection_count_distinct_list_order_semantics_are_stable() {
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

    let distinct_count = execute_projection_count_distinct_boundary(
        &load,
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(ExecutablePlan::from)
            .expect("list-order count-distinct plan should build"),
        planned_slot::<PhaseEntity>("tags"),
    )
    .expect("count_distinct_by(tags) should succeed");

    assert_eq!(
        distinct_count, 3,
        "count_distinct_by(tags) should preserve list-order equality semantics",
    );
}

#[test]
fn aggregate_projection_count_distinct_residual_retry_parity_and_scan_budget_match_execute() {
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
            MissingRowPolicy::Ignore,
        );
        logical.scalar_plan_mut().predicate = Some(Predicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        });
        logical.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![
                ("tag".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        logical.scalar_plan_mut().page = Some(PageSpec {
            limit: Some(2),
            offset: 0,
        });

        ExecutablePlan::<IndexedMetricsEntity>::new(logical)
    };

    let (distinct_count, scanned_count_distinct) =
        capture_rows_scanned_for_entity(IndexedMetricsEntity::PATH, || {
            execute_projection_count_distinct_boundary(
                &load,
                build_plan(),
                planned_slot::<IndexedMetricsEntity>("tag"),
            )
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
        for row in &response {
            let value = Value::Uint(u64::from(row.entity_ref().tag));
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
        "count_distinct_by(tag) should preserve canonical fallback parity for residual-retry index-range shapes",
    );
    assert_eq!(
        scanned_count_distinct, scanned_execute,
        "count_distinct_by(tag) should preserve scan-budget parity with execute() on residual-retry index-range shapes",
    );
}

#[test]
fn aggregate_projection_count_distinct_is_direction_invariant() {
    seed_pushdown_entities(&[
        (8_3201, 7, 10),
        (8_3202, 7, 20),
        (8_3203, 7, 20),
        (8_3204, 7, 30),
        (8_3205, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let asc_count = execute_projection_count_distinct_boundary(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
            .plan()
            .map(ExecutablePlan::from)
            .expect("direction-invariant ASC plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("direction-invariant ASC count_distinct_by(rank) should succeed");
    let desc_count = execute_projection_count_distinct_boundary(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("rank")
            .order_by_desc("id")
            .plan()
            .map(ExecutablePlan::from)
            .expect("direction-invariant DESC plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("direction-invariant DESC count_distinct_by(rank) should succeed");

    assert_eq!(
        asc_count, desc_count,
        "count_distinct_by(rank) should be invariant to traversal direction over the same effective window",
    );
}

#[test]
fn aggregate_projection_count_distinct_distinct_modifier_tracks_effective_window_rows() {
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
        let mut query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
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
                .map(ExecutablePlan::from)
                .expect("non-distinct count-distinct baseline plan should build"),
        )
        .expect("non-distinct count-distinct baseline execute should succeed");
    let distinct_response = load
        .execute(
            build_query(true)
                .plan()
                .map(ExecutablePlan::from)
                .expect("distinct count-distinct baseline plan should build"),
        )
        .expect("distinct count-distinct baseline execute should succeed");

    let non_distinct_count = execute_projection_count_distinct_boundary(
        &load,
        build_query(false)
            .plan()
            .map(ExecutablePlan::from)
            .expect("non-distinct count-distinct plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("non-distinct count_distinct_by(rank) should succeed");
    let distinct_count = execute_projection_count_distinct_boundary(
        &load,
        build_query(true)
            .plan()
            .map(ExecutablePlan::from)
            .expect("distinct count-distinct plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("distinct count_distinct_by(rank) should succeed");

    assert_eq!(
        non_distinct_count,
        expected_count_distinct_by_rank(&non_distinct_response),
        "non-distinct count_distinct_by(rank) should match effective-window field distinct count",
    );
    assert_eq!(
        distinct_count,
        expected_count_distinct_by_rank(&distinct_response),
        "distinct count_distinct_by(rank) should match effective-window field distinct count",
    );
}

#[test]
fn aggregate_projection_values_by_distinct_remains_row_level() {
    seed_pushdown_entities(&[
        (8_1971, 7, 10),
        (8_1972, 7, 10),
        (8_1973, 7, 20),
        (8_1974, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let values = execute_projection_values_boundary(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .distinct()
            .order_by("id")
            .plan()
            .map(ExecutablePlan::from)
            .expect("values_by distinct plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("values_by(rank) should succeed");

    assert_eq!(
        values,
        vec![Value::Uint(10), Value::Uint(10), Value::Uint(20)],
        "query-level DISTINCT must remain row-level; equal projected values may repeat",
    );
}

#[test]
fn aggregate_projection_covering_constant_projection_terminals_match_effective_window() {
    seed_pushdown_entities(&[
        (8_4011, 7, 10),
        (8_4012, 7, 20),
        (8_4013, 7, 30),
        (8_4014, 7, 40),
        (8_4015, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
            .offset(1)
            .limit(3)
            .plan()
            .map(ExecutablePlan::from)
            .expect("covering-constant projection plan should build")
    };

    let expected_rows = load
        .execute(build_plan())
        .expect("covering-constant baseline execute should succeed");
    let expected_value = Value::Uint(7);
    let expected_values = vec![expected_value.clone(); expected_rows.len()];
    let expected_values_with_ids = expected_rows
        .iter()
        .map(|row| (row.id(), expected_value.clone()))
        .collect::<Vec<_>>();
    let expected_first_or_last = if expected_rows.is_empty() {
        None
    } else {
        Some(expected_value.clone())
    };

    let values = execute_projection_values_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("group"),
    )
    .expect("values_by(group) should succeed on covering index-prefix window");
    let distinct_values = execute_projection_distinct_values_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("group"),
    )
    .expect("distinct_values_by(group) should succeed on covering index-prefix window");
    let values_with_ids = execute_projection_values_with_ids_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("group"),
    )
    .expect("values_by_with_ids(group) should succeed on covering index-prefix window");
    let first_value = execute_projection_terminal_value_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("group"),
        AggregateKind::First,
    )
    .expect("first_value_by(group) should succeed on covering index-prefix window");
    let last_value = execute_projection_terminal_value_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("group"),
        AggregateKind::Last,
    )
    .expect("last_value_by(group) should succeed on covering index-prefix window");

    assert_eq!(
        values, expected_values,
        "values_by(group) should preserve effective-window cardinality for covering constant projections",
    );
    assert_eq!(
        distinct_values,
        expected_first_or_last
            .clone()
            .into_iter()
            .collect::<Vec<_>>(),
        "distinct_values_by(group) should return one value when the effective window is non-empty",
    );
    assert_eq!(
        values_with_ids, expected_values_with_ids,
        "values_by_with_ids(group) should preserve id/value alignment for covering constant projections",
    );
    assert_eq!(
        first_value, expected_first_or_last,
        "first_value_by(group) should match the constant covering projection value",
    );
    assert_eq!(
        last_value,
        if expected_rows.is_empty() {
            None
        } else {
            Some(expected_value)
        },
        "last_value_by(group) should match the constant covering projection value",
    );
}

#[test]
fn aggregate_projection_covering_projection_matches_row_materialized_projection() {
    seed_pushdown_entities(&[
        (8_4031, 7, 10),
        (8_4032, 7, 20),
        (8_4033, 7, 20),
        (8_4034, 7, 30),
        (8_4035, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
            .offset(1)
            .limit(3)
            .plan()
            .map(ExecutablePlan::from)
            .expect("covering-index projection plan should build")
    };

    let expected_response = load
        .execute(build_plan())
        .expect("covering-index baseline execute should succeed");
    let expected_values = expected_values_by_rank(&expected_response);
    let expected_values_with_ids = expected_values_by_rank_with_ids(&expected_response);
    let expected_distinct = expected_distinct_values_by_rank(&expected_response);
    let expected_first = expected_first_value_by_rank(&expected_response);
    let expected_last = expected_last_value_by_rank(&expected_response);

    let values = execute_projection_values_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("values_by(rank) should succeed on covering index projection");
    let values_with_ids = execute_projection_values_with_ids_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("values_by_with_ids(rank) should succeed on covering index projection");
    let distinct_values = execute_projection_distinct_values_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("distinct_values_by(rank) should succeed on covering index projection");
    let first_value = execute_projection_terminal_value_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
        AggregateKind::First,
    )
    .expect("first_value_by(rank) should succeed on covering index projection");
    let last_value = execute_projection_terminal_value_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
        AggregateKind::Last,
    )
    .expect("last_value_by(rank) should succeed on covering index projection");

    assert_eq!(
        values, expected_values,
        "values_by(rank) should match effective-window projection under covering index paths",
    );
    assert_eq!(
        values_with_ids, expected_values_with_ids,
        "values_by_with_ids(rank) should match effective-window id/value projection under covering index paths",
    );
    assert_eq!(
        distinct_values, expected_distinct,
        "distinct_values_by(rank) should match first-observed distinct projection under covering index paths",
    );
    assert_eq!(
        first_value, expected_first,
        "first_value_by(rank) should match effective-window first projection under covering index paths",
    );
    assert_eq!(
        last_value, expected_last,
        "last_value_by(rank) should match effective-window last projection under covering index paths",
    );
}

#[test]
fn aggregate_projection_covering_index_distinct_non_leading_component_preserves_first_observed_dedup()
 {
    seed_pushdown_entities(&[
        (8_4039, 7, 10),
        (8_4040, 7, 20),
        (8_4041, 8, 10),
        (8_4042, 8, 30),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_by("group")
            .order_by("rank")
            .order_by("id")
            .plan()
            .map(ExecutablePlan::from)
            .expect("covering non-leading distinct plan should build")
    };

    let values = execute_projection_values_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("values_by(rank) should succeed for covering non-leading distinct shape");
    let distinct_values = execute_projection_distinct_values_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("distinct_values_by(rank) should succeed for covering non-leading distinct shape");

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

    assert_eq!(
        values,
        vec![
            Value::Uint(10),
            Value::Uint(20),
            Value::Uint(10),
            Value::Uint(30),
        ],
        "covering non-leading distinct fixture should keep duplicate rank values non-adjacent in index order",
    );
    assert_eq!(
        distinct_values, expected_distinct_from_values,
        "distinct_values_by(rank) must preserve first-observed semantics when duplicates are non-adjacent in covering order",
    );
}

#[test]
fn aggregate_projection_bytes_by_projection_mode_classifier_matches_bounded_route_shapes() {
    let covering_index_plan = {
        let mut logical_plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: PUSHDOWN_PARITY_INDEX_MODELS[0],
                values: vec![Value::Uint(7)],
            },
            MissingRowPolicy::Ignore,
        );
        logical_plan.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        ExecutablePlan::<PushdownParityEntity>::new(logical_plan)
    };
    let covering_index_mode = covering_index_plan.bytes_by_projection_mode("rank");
    assert_eq!(
        covering_index_mode,
        crate::db::executor::BytesByProjectionMode::CoveringIndex,
        "bytes-by classifier should mark eligible ordered index-prefix shapes as covering-index",
    );
    assert_eq!(
        ExecutablePlan::<PushdownParityEntity>::bytes_by_projection_mode_label(covering_index_mode),
        "field_covering_index",
        "bytes-by classifier labels should remain stable for covering-index mode",
    );

    let constant_covering_plan =
        ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: PUSHDOWN_PARITY_INDEX_MODELS[0],
                values: vec![Value::Uint(7), Value::Uint(20)],
            },
            MissingRowPolicy::Ignore,
        ));
    let constant_mode = constant_covering_plan.bytes_by_projection_mode("rank");
    assert_eq!(
        constant_mode,
        crate::db::executor::BytesByProjectionMode::CoveringConstant,
        "bytes-by classifier should mark prefix-bound fields as covering-constant",
    );
    assert_eq!(
        ExecutablePlan::<PushdownParityEntity>::bytes_by_projection_mode_label(constant_mode),
        "field_covering_constant",
        "bytes-by classifier labels should remain stable for covering-constant mode",
    );

    let strict_plan = ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_PARITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7), Value::Uint(20)],
        },
        MissingRowPolicy::Error,
    ));
    let strict_mode = strict_plan.bytes_by_projection_mode("rank");
    assert_eq!(
        strict_mode,
        crate::db::executor::BytesByProjectionMode::Materialized,
        "strict bytes-by classifier should fail closed to materialized mode",
    );
    assert_eq!(
        ExecutablePlan::<PushdownParityEntity>::bytes_by_projection_mode_label(strict_mode),
        "field_materialized",
        "bytes-by classifier labels should remain stable for strict materialized mode",
    );
}

#[test]
fn aggregate_projection_covering_index_projection_strict_missing_row_preserves_error_surface() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for (id, group, rank) in [(8_4041u128, 7u32, 10u32), (8_4042, 7, 20), (8_4043, 7, 30)] {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(id),
            group,
            rank,
            label: format!("g{group}-r{rank}"),
        })
        .expect("strict covering-index projection seed row save should succeed");
    }

    remove_pushdown_row_data(8_4042);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let err = execute_projection_values_boundary(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Error)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
            .plan()
            .map(ExecutablePlan::from)
            .expect("strict covering-index projection plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect_err("strict covering-index projection should fail on missing primary rows");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "strict covering-index projection must preserve missing-row corruption classification",
    );
    assert!(
        err.message.contains("missing row"),
        "strict covering-index projection must preserve missing-row error context",
    );

    let with_ids_err = execute_projection_values_with_ids_boundary(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Error)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
            .plan()
            .map(ExecutablePlan::from)
            .expect("strict covering-index projection with-ids plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect_err("strict covering-index projection with ids should fail on missing primary rows");

    assert_eq!(
        with_ids_err.class,
        crate::error::ErrorClass::Corruption,
        "strict covering-index projection with ids must preserve missing-row corruption classification",
    );
    assert!(
        with_ids_err.message.contains("missing row"),
        "strict covering-index projection with ids must preserve missing-row error context",
    );
}

#[test]
fn aggregate_projection_distinct_values_by_matches_effective_window_projection() {
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
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
            .plan()
            .map(ExecutablePlan::from)
            .expect("distinct_values_by plan should build")
    };

    let expected = load
        .execute(build_plan())
        .expect("baseline execute for distinct_values_by should succeed");
    let actual = execute_projection_distinct_values_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("distinct_values_by(rank) should succeed");

    assert_eq!(
        actual,
        expected_distinct_values_by_rank(&expected),
        "distinct_values_by(rank) should match effective-window first-observed distinct projection",
    );
}

#[test]
fn aggregate_projection_distinct_values_by_matches_values_by_first_observed_dedup() {
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
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
            .plan()
            .map(ExecutablePlan::from)
            .expect("distinct-values invariant plan should build")
    };

    let values = execute_projection_values_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("values_by(rank) should succeed");
    let distinct_values = execute_projection_distinct_values_boundary(
        &load,
        build_plan(),
        planned_slot::<PushdownParityEntity>("rank"),
    )
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
        "values_by(field).len() must be >= distinct_values_by(field).len()",
    );
    assert_eq!(
        distinct_values, expected_distinct_from_values,
        "distinct_values_by(field) must equal values_by(field) deduped by first occurrence",
    );
}

#[test]
fn aggregate_projection_optional_field_null_value_terminal_parity_matrix() {
    for terminal in [
        OptionalFieldNullTerminal::TopKBy,
        OptionalFieldNullTerminal::BottomKBy,
        OptionalFieldNullTerminal::TopKByValues,
        OptionalFieldNullTerminal::BottomKByValues,
        OptionalFieldNullTerminal::TopKByWithIds,
        OptionalFieldNullTerminal::BottomKByWithIds,
    ] {
        assert_optional_field_null_parity(terminal, "optional-field null-value parity");
    }
}

#[test]
fn aggregate_projection_count_distinct_optional_field_null_values_are_rejected_consistently() {
    seed_optional_field_null_values_fixture();
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let build_plan_asc = || {
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .order_by("rank")
            .plan()
            .map(ExecutablePlan::from)
            .expect("optional-field null-semantics ASC plan should build")
    };
    let build_plan_desc = || {
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .order_by_desc("rank")
            .order_by_desc("id")
            .plan()
            .map(ExecutablePlan::from)
            .expect("optional-field null-semantics DESC plan should build")
    };
    let asc_err = execute_projection_count_distinct_boundary(
        &load,
        build_plan_asc(),
        planned_slot::<PhaseEntity>("opt_rank"),
    )
    .expect_err("count_distinct_by(opt_rank) ASC should reject null field values");
    let desc_err = execute_projection_count_distinct_boundary(
        &load,
        build_plan_desc(),
        planned_slot::<PhaseEntity>("opt_rank"),
    )
    .expect_err("count_distinct_by(opt_rank) DESC should reject null field values");

    assert_eq!(
        asc_err.class,
        crate::error::ErrorClass::InvariantViolation,
        "count_distinct_by(opt_rank) should classify null-value mismatch as invariant violation",
    );
    assert_eq!(
        desc_err.class,
        crate::error::ErrorClass::InvariantViolation,
        "descending count_distinct_by(opt_rank) should classify null-value mismatch as invariant violation",
    );
    assert!(
        asc_err
            .message
            .contains("aggregate target field value type mismatch"),
        "count_distinct_by(opt_rank) should expose type-mismatch reason for null values",
    );
    assert!(
        desc_err
            .message
            .contains("aggregate target field value type mismatch"),
        "descending count_distinct_by(opt_rank) should expose type-mismatch reason for null values",
    );
    assert!(
        asc_err.message.contains("value=Null") && desc_err.message.contains("value=Null"),
        "count_distinct_by(opt_rank) should report null payload mismatch consistently across directions",
    );
}

#[test]
fn aggregate_projection_missing_field_ranked_projection_parity_matrix() {
    for terminal in [
        MissingFieldTerminal::TopKBy,
        MissingFieldTerminal::TopKByValues,
        MissingFieldTerminal::BottomKBy,
        MissingFieldTerminal::BottomKByValues,
        MissingFieldTerminal::TopKByWithIds,
        MissingFieldTerminal::BottomKByWithIds,
    ] {
        assert_missing_field_terminal_parity(terminal, "missing-field ranked projection parity");
    }
}

#[test]
fn aggregate_projection_missing_field_projection_terminals_fail_without_scan() {
    seed_missing_field_parity_fixture();
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let values_err = execute_projection_values_boundary(
        &load,
        missing_field_parity_plan(),
        planned_slot::<PushdownParityEntity>("missing_field"),
    )
    .expect_err("values_by(missing_field) should be rejected");
    let distinct_values_err = execute_projection_distinct_values_boundary(
        &load,
        missing_field_parity_plan(),
        planned_slot::<PushdownParityEntity>("missing_field"),
    )
    .expect_err("distinct_values_by(missing_field) should be rejected");
    let values_with_ids_err = execute_projection_values_with_ids_boundary(
        &load,
        missing_field_parity_plan(),
        planned_slot::<PushdownParityEntity>("missing_field"),
    )
    .expect_err("values_by_with_ids(missing_field) should be rejected");
    let first_value_err = execute_projection_terminal_value_boundary(
        &load,
        missing_field_parity_plan(),
        planned_slot::<PushdownParityEntity>("missing_field"),
        AggregateKind::First,
    )
    .expect_err("first_value_by(missing_field) should be rejected");
    let last_value_err = execute_projection_terminal_value_boundary(
        &load,
        missing_field_parity_plan(),
        planned_slot::<PushdownParityEntity>("missing_field"),
        AggregateKind::Last,
    )
    .expect_err("last_value_by(missing_field) should be rejected");
    let count_distinct_err = execute_projection_count_distinct_boundary(
        &load,
        missing_field_parity_plan(),
        planned_slot::<PushdownParityEntity>("missing_field"),
    )
    .expect_err("count_distinct_by(missing_field) should be rejected");

    for (label, err) in [
        ("values_by", &values_err),
        ("distinct_values_by", &distinct_values_err),
        ("values_by_with_ids", &values_with_ids_err),
        ("first_value_by", &first_value_err),
        ("last_value_by", &last_value_err),
        ("count_distinct_by", &count_distinct_err),
    ] {
        assert_eq!(
            err.class, values_err.class,
            "{label} should keep the baseline unknown-field error class",
        );
        assert_eq!(
            err.origin, values_err.origin,
            "{label} should keep the baseline unknown-field error origin",
        );
        assert!(
            err.message.contains("unknown aggregate target field"),
            "{label} should preserve explicit unknown-field taxonomy: {err:?}",
        );
    }
}

#[test]
fn aggregate_projection_covering_constant_projection_strict_missing_row_preserves_error_surface() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for (id, group, rank) in [(8_4021u128, 7u32, 10u32), (8_4022, 7, 20), (8_4023, 7, 30)] {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(id),
            group,
            rank,
            label: format!("g{group}-r{rank}"),
        })
        .expect("strict covering-projection seed row save should succeed");
    }

    remove_pushdown_row_data(8_4021);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let err = execute_projection_values_boundary(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Error)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
            .plan()
            .map(ExecutablePlan::from)
            .expect("strict covering-projection plan should build"),
        planned_slot::<PushdownParityEntity>("group"),
    )
    .expect_err("strict covering projection should fail on missing primary rows");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "strict covering projection must preserve missing-row corruption classification",
    );
    assert!(
        err.message.contains("missing row"),
        "strict covering projection must preserve missing-row error context",
    );

    let with_ids_err = execute_projection_values_with_ids_boundary(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Error)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
            .plan()
            .map(ExecutablePlan::from)
            .expect("strict covering-projection with-ids plan should build"),
        planned_slot::<PushdownParityEntity>("group"),
    )
    .expect_err("strict covering projection with ids should fail on missing primary rows");

    assert_eq!(
        with_ids_err.class,
        crate::error::ErrorClass::Corruption,
        "strict covering projection with ids must preserve missing-row corruption classification",
    );
    assert!(
        with_ids_err.message.contains("missing row"),
        "strict covering projection with ids must preserve missing-row error context",
    );
}

///
/// ProjectionScanBudgetTerminal
///
/// Declares one projection terminal that must preserve execute() scan budget.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectionScanBudgetTerminal {
    ValuesBy,
    DistinctValuesBy,
    ValuesByWithIds,
}

///
/// ProjectionScanBudgetCase
///
/// One matrix row for projection-terminal scan-budget parity against execute().
/// Each row binds a fixture, terminal kind, and behavior-cell coordinates.
///

struct ProjectionScanBudgetCase {
    label: &'static str,
    rows: &'static [(u128, u32, u32)],
    terminal: ProjectionScanBudgetTerminal,
}

const PROJECTION_SCAN_BUDGET_VALUES_BY_ROWS: [(u128, u32, u32); 6] = [
    (8_331, 7, 10),
    (8_332, 7, 10),
    (8_333, 7, 20),
    (8_334, 7, 30),
    (8_335, 7, 40),
    (8_336, 8, 99),
];
const PROJECTION_SCAN_BUDGET_DISTINCT_VALUES_BY_ROWS: [(u128, u32, u32); 6] = [
    (8_351, 7, 10),
    (8_352, 7, 10),
    (8_353, 7, 20),
    (8_354, 7, 30),
    (8_355, 7, 20),
    (8_356, 8, 99),
];
const PROJECTION_SCAN_BUDGET_VALUES_BY_WITH_IDS_ROWS: [(u128, u32, u32); 6] = [
    (8_361, 7, 10),
    (8_362, 7, 10),
    (8_363, 7, 20),
    (8_364, 7, 30),
    (8_365, 7, 20),
    (8_366, 8, 99),
];

fn run_projection_scan_budget_terminal(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
    terminal: ProjectionScanBudgetTerminal,
) -> Result<(), InternalError> {
    match terminal {
        ProjectionScanBudgetTerminal::ValuesBy => {
            execute_projection_values_boundary(
                load,
                plan,
                planned_slot::<PushdownParityEntity>("rank"),
            )?;
        }
        ProjectionScanBudgetTerminal::DistinctValuesBy => {
            execute_projection_distinct_values_boundary(
                load,
                plan,
                planned_slot::<PushdownParityEntity>("rank"),
            )?;
        }
        ProjectionScanBudgetTerminal::ValuesByWithIds => {
            execute_projection_values_with_ids_boundary(
                load,
                plan,
                planned_slot::<PushdownParityEntity>("rank"),
            )?;
        }
    }

    Ok(())
}

fn projection_scan_budget_cases() -> [ProjectionScanBudgetCase; 3] {
    [
        ProjectionScanBudgetCase {
            label: "values_by",
            rows: &PROJECTION_SCAN_BUDGET_VALUES_BY_ROWS,
            terminal: ProjectionScanBudgetTerminal::ValuesBy,
        },
        ProjectionScanBudgetCase {
            label: "distinct_values_by",
            rows: &PROJECTION_SCAN_BUDGET_DISTINCT_VALUES_BY_ROWS,
            terminal: ProjectionScanBudgetTerminal::DistinctValuesBy,
        },
        ProjectionScanBudgetCase {
            label: "values_by_with_ids",
            rows: &PROJECTION_SCAN_BUDGET_VALUES_BY_WITH_IDS_ROWS,
            terminal: ProjectionScanBudgetTerminal::ValuesByWithIds,
        },
    ]
}

#[test]
fn aggregate_projection_terminals_preserve_scan_budget_parity_with_execute_matrix() {
    for case in projection_scan_budget_cases() {
        seed_pushdown_entities(case.rows);
        let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
        let build_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("id")
                .offset(1)
                .limit(4)
                .plan()
                .map(ExecutablePlan::from)
                .expect("projection scan-budget matrix plan should build")
        };

        // Phase 1: establish execute() baseline scan budget for the shared matrix shape.
        let (_, scanned_execute) =
            capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
                load.execute(build_plan())
                    .expect("projection scan-budget execute baseline should succeed")
            });

        // Phase 2: execute the matrix terminal and assert scan-budget parity.
        let ((), scanned_terminal) =
            capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
                run_projection_scan_budget_terminal(&load, build_plan(), case.terminal)
                    .expect("projection scan-budget matrix terminal should succeed");
            });
        assert_eq!(
            scanned_terminal, scanned_execute,
            "projection terminal scan-budget parity failed for case={}",
            case.label
        );
    }
}

#[test]
fn aggregate_projection_terminals_scan_budget_matrix_covers_all_forms() {
    let labels = projection_scan_budget_cases().map(|case| case.label);
    assert_eq!(
        labels,
        ["values_by", "distinct_values_by", "values_by_with_ids"],
        "projection scan-budget matrix must enumerate all projection terminal forms"
    );
}
