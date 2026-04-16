//! Module: db::executor::tests::aggregate_core
//! Covers core aggregate execution behavior across scalar and grouped routes.
//! Does not own: production aggregate behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use super::support::*;
use crate::{
    db::{
        access::AccessPath,
        data::DataKey,
        executor::{
            PreparedExecutionPlan,
            aggregate::{
                AggregateKind, ScalarNumericFieldBoundaryRequest, ScalarTerminalBoundaryRequest,
                field::{
                    AggregateFieldValueError, resolve_orderable_aggregate_target_slot_from_fields,
                },
            },
        },
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::{
            builder::AggregateExpr,
            explain::ExplainExecutionNodeType,
            intent::Query,
            plan::{
                AccessPlannedQuery, FieldSlot as PlannedFieldSlot, OrderDirection, OrderSpec,
                PageSpec,
            },
        },
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
    types::{Id, Ulid},
    value::Value,
};

type AggregateId<E> = crate::types::Id<E>;
type AggregateIdPair<E> = Option<(AggregateId<E>, AggregateId<E>)>;

const SECONDARY_SINGLE_STEP_STRICT_ROWS: [(u128, u32, u32); 5] = [
    (8_831, 7, 10),
    (8_832, 7, 20),
    (8_833, 7, 30),
    (8_834, 7, 40),
    (8_835, 8, 50),
];
const SECONDARY_SINGLE_STEP_MISSING_OK_ROWS: [(u128, u32, u32); 5] = [
    (8_841, 7, 10),
    (8_842, 7, 20),
    (8_843, 7, 30),
    (8_844, 7, 40),
    (8_845, 8, 50),
];
const SECONDARY_STALE_ID_ROWS: [(u128, u32, u32); 5] = [
    (8_851, 7, 10),
    (8_852, 7, 20),
    (8_853, 7, 30),
    (8_854, 7, 40),
    (8_855, 8, 50),
];
const SECONDARY_STALE_FIELD_ROWS: [(u128, u32, u32); 5] = [
    (8_261, 7, 10),
    (8_262, 7, 20),
    (8_263, 7, 30),
    (8_264, 7, 40),
    (8_265, 8, 50),
];

fn expected_nth_by_rank_id(
    response: &crate::db::response::EntityResponse<PushdownParityEntity>,
    nth: usize,
) -> Option<crate::types::Id<PushdownParityEntity>> {
    let mut rows: Vec<_> = response
        .iter()
        .map(|row| (row.entity_ref().rank, row.id()))
        .collect();
    rows.sort_unstable_by_key(|(rank, id)| (*rank, id.key()));

    rows.get(nth).map(|(_, id)| *id)
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

fn seed_simple_entities(rows: &[u128]) {
    reset_store();
    let save = SaveExecutor::<SimpleEntity>::new(DB, false);

    for id in rows {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(*id),
        })
        .expect("aggregate simple seed save should succeed");
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

fn seed_indexed_metrics_rows(rows: &[(u128, u32, &str)]) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<IndexedMetricsEntity>::new(DB, false);
    for (id, tag, label) in rows {
        save.insert(IndexedMetricsEntity {
            id: Ulid::from_u128(*id),
            tag: *tag,
            label: (*label).to_string(),
        })
        .expect("seed indexed-metrics row save should succeed");
    }
}

fn remove_indexed_metrics_row_data(id: u128) {
    let raw_key = DataKey::try_new::<IndexedMetricsEntity>(Ulid::from_u128(id))
        .expect("indexed-metrics data key should build")
        .to_raw()
        .expect("indexed-metrics data key should encode");

    DATA_STORE.with(|store| {
        let removed = store.borrow_mut().remove(&raw_key);
        assert!(
            removed.is_some(),
            "expected indexed-metrics row to exist before data-only removal",
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

fn strict_compare_predicate(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        value,
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

fn execution_root_node_type<E>(plan: &PreparedExecutionPlan<E>) -> ExplainExecutionNodeType
where
    E: EntityKind + EntityValue,
{
    plan.explain_load_execution_node_descriptor()
        .expect("aggregate execution descriptor should build")
        .node_type()
}

// Keep the secondary-index ordered aggregate cases owner-local to the live
// aggregate-core suite instead of depending on the stale aggregate path helper.
fn secondary_group_rank_order_plan(
    consistency: MissingRowPolicy,
    direction: OrderDirection,
    offset: u32,
) -> PreparedExecutionPlan<PushdownParityEntity> {
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

    PreparedExecutionPlan::<PushdownParityEntity>::new(logical_plan)
}

fn indexed_metrics_tag_index_range_plan(
    consistency: MissingRowPolicy,
) -> PreparedExecutionPlan<IndexedMetricsEntity> {
    let mut logical_plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            vec![],
            std::ops::Bound::Included(Value::Uint(0)),
            std::ops::Bound::Excluded(Value::Uint(1_000)),
        ),
        consistency,
    );
    logical_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });

    PreparedExecutionPlan::<IndexedMetricsEntity>::new(logical_plan)
}

fn assert_secondary_id_extrema_single_step(
    rows: &[(u128, u32, u32)],
    consistency: MissingRowPolicy,
    expected_min: u128,
    expected_max: u128,
    label: &str,
) {
    seed_pushdown_entities(rows);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let (min_asc, scanned_min_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_min_terminal(
                &load,
                secondary_group_rank_order_plan(consistency, OrderDirection::Asc, 2),
            )
            .expect("secondary single-step MIN ASC should succeed")
        });
    let (max_desc, scanned_max_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_max_terminal(
                &load,
                secondary_group_rank_order_plan(consistency, OrderDirection::Desc, 2),
            )
            .expect("secondary single-step MAX DESC should succeed")
        });

    assert_eq!(
        min_asc.map(|id| id.key()),
        Some(Ulid::from_u128(expected_min))
    );
    assert_eq!(
        max_desc.map(|id| id.key()),
        Some(Ulid::from_u128(expected_max))
    );
    assert_eq!(
        scanned_min_asc, 4,
        "{label} MIN ASC should scan the full secondary group window under the current contract",
    );
    assert_eq!(
        scanned_max_desc, 4,
        "{label} MAX DESC should scan the full secondary group window under the current contract",
    );
}

fn assert_secondary_id_extrema_missing_ok_stale_fallback(
    rows: &[(u128, u32, u32)],
    stale_ids: &[u128],
) {
    seed_stale_secondary_rows(rows, stale_ids);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let expected_min_asc = load
        .execute(secondary_group_rank_order_plan(
            MissingRowPolicy::Ignore,
            OrderDirection::Asc,
            0,
        ))
        .expect("stale-leading MIN ASC baseline execute should succeed")
        .ids()
        .next();
    let expected_max_desc = load
        .execute(secondary_group_rank_order_plan(
            MissingRowPolicy::Ignore,
            OrderDirection::Desc,
            0,
        ))
        .expect("stale-leading MAX DESC baseline execute should succeed")
        .ids()
        .next();
    let (min_asc, scanned_min_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_min_terminal(
                &load,
                secondary_group_rank_order_plan(MissingRowPolicy::Ignore, OrderDirection::Asc, 0),
            )
            .expect("stale-leading secondary MIN ASC should succeed")
        });
    let (max_desc, scanned_max_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_max_terminal(
                &load,
                secondary_group_rank_order_plan(MissingRowPolicy::Ignore, OrderDirection::Desc, 0),
            )
            .expect("stale-leading secondary MAX DESC should succeed")
        });

    assert_eq!(
        min_asc, expected_min_asc,
        "stale-leading MIN ASC should preserve materialized parity",
    );
    assert_eq!(
        max_desc, expected_max_desc,
        "stale-leading MAX DESC should preserve materialized parity",
    );
    assert!(
        scanned_min_asc >= 2,
        "stale-leading MIN ASC should scan past bounded probe and retry unbounded",
    );
    assert!(
        scanned_max_desc >= 2,
        "stale-leading MAX DESC should scan past bounded probe and retry unbounded",
    );
}

fn assert_secondary_id_extrema_strict_stale_corruption(
    rows: &[(u128, u32, u32)],
    stale_ids: &[u128],
) {
    seed_stale_secondary_rows(rows, stale_ids);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let min_err = execute_min_terminal(
        &load,
        secondary_group_rank_order_plan(MissingRowPolicy::Error, OrderDirection::Asc, 0),
    )
    .expect_err("strict secondary MIN should fail when leading key is stale");
    let max_err = execute_max_terminal(
        &load,
        secondary_group_rank_order_plan(MissingRowPolicy::Error, OrderDirection::Desc, 0),
    )
    .expect_err("strict secondary MAX should fail when leading key is stale");

    assert_eq!(
        min_err.class,
        ErrorClass::Corruption,
        "strict secondary MIN stale-leading key should classify as corruption",
    );
    assert_eq!(
        max_err.class,
        ErrorClass::Corruption,
        "strict secondary MAX stale-leading key should classify as corruption",
    );
}

fn assert_secondary_field_extrema_missing_ok_stale_fallback(
    rows: &[(u128, u32, u32)],
    stale_ids: &[u128],
    target_field: &str,
) {
    seed_stale_secondary_rows(rows, stale_ids);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let expected_min_by = load
        .execute(secondary_group_rank_order_plan(
            MissingRowPolicy::Ignore,
            OrderDirection::Asc,
            0,
        ))
        .expect("missing-ok field MIN baseline execute should succeed")
        .ids()
        .next();
    let expected_max_by = load
        .execute(secondary_group_rank_order_plan(
            MissingRowPolicy::Ignore,
            OrderDirection::Desc,
            0,
        ))
        .expect("missing-ok field MAX baseline execute should succeed")
        .ids()
        .next();
    let (min_by, scanned_min_by) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_min_by_slot_terminal(
                &load,
                secondary_group_rank_order_plan(MissingRowPolicy::Ignore, OrderDirection::Asc, 0),
                planned_slot::<PushdownParityEntity>(target_field),
            )
            .expect("missing-ok field MIN should succeed")
        });
    let (max_by, scanned_max_by) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_max_by_slot_terminal(
                &load,
                secondary_group_rank_order_plan(MissingRowPolicy::Ignore, OrderDirection::Desc, 0),
                planned_slot::<PushdownParityEntity>(target_field),
            )
            .expect("missing-ok field MAX should succeed")
        });

    assert_eq!(
        min_by, expected_min_by,
        "missing-ok field MIN should preserve materialized parity under stale-leading keys",
    );
    assert_eq!(
        max_by, expected_max_by,
        "missing-ok field MAX should preserve materialized parity under stale-leading keys",
    );
    assert!(
        scanned_min_by >= 2,
        "missing-ok field MIN should scan past bounded probe and retry unbounded",
    );
    assert!(
        scanned_max_by >= 2,
        "missing-ok field MAX should scan past bounded probe and retry unbounded",
    );
}

fn assert_secondary_field_extrema_strict_stale_corruption(
    rows: &[(u128, u32, u32)],
    stale_ids: &[u128],
    target_field: &str,
) {
    seed_stale_secondary_rows(rows, stale_ids);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let min_err = execute_min_by_slot_terminal(
        &load,
        secondary_group_rank_order_plan(MissingRowPolicy::Error, OrderDirection::Asc, 0),
        planned_slot::<PushdownParityEntity>(target_field),
    )
    .expect_err("strict field MIN should fail when leading key is stale");
    let max_err = execute_max_by_slot_terminal(
        &load,
        secondary_group_rank_order_plan(MissingRowPolicy::Error, OrderDirection::Desc, 0),
        planned_slot::<PushdownParityEntity>(target_field),
    )
    .expect_err("strict field MAX should fail when leading key is stale");

    assert_eq!(
        min_err.class,
        ErrorClass::Corruption,
        "strict field MIN stale-leading key should classify as corruption",
    );
    assert_eq!(
        max_err.class,
        ErrorClass::Corruption,
        "strict field MAX stale-leading key should classify as corruption",
    );
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
        .expect("aggregate unique-range seed save should succeed");
    }
}

fn execute_min_by_slot_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
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

fn execute_projection_count_distinct_boundary<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
    target_field: PlannedFieldSlot,
) -> Result<u32, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_projection_boundary(
        plan,
        target_field,
        crate::db::executor::aggregate::ScalarProjectionBoundaryRequest::CountDistinct,
    )?
    .into_count()
}

fn execute_min_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
) -> Result<Option<crate::types::Id<E>>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(
        plan,
        ScalarTerminalBoundaryRequest::IdTerminal {
            kind: AggregateKind::Min,
        },
    )?
    .into_id::<E>()
}

fn execute_max_by_slot_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
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

fn execute_max_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
) -> Result<Option<crate::types::Id<E>>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(
        plan,
        ScalarTerminalBoundaryRequest::IdTerminal {
            kind: AggregateKind::Max,
        },
    )?
    .into_id::<E>()
}

fn execute_nth_by_slot_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
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
    plan: PreparedExecutionPlan<E>,
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
    plan: PreparedExecutionPlan<E>,
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

fn execute_numeric_field_boundary<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
    target_field: PlannedFieldSlot,
    request: ScalarNumericFieldBoundaryRequest,
) -> Result<Option<crate::types::Decimal>, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_numeric_field_boundary(plan, target_field, request)
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
        resolve_orderable_aggregate_target_slot_from_fields(E::MODEL.fields(), target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;
    }

    Ok(())
}

///
/// RankedKOneTerminal
///
/// Declares which extrema terminal anchors one k=1 ranked parity row.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RankedKOneTerminal {
    Top,
    Bottom,
}

///
/// RankedKOneProjection
///
/// Declares the projection shape asserted for one k=1 ranked parity row.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RankedKOneProjection {
    Ids,
    Values,
    ValuesWithIds,
}

///
/// RankedDirectionResult
///
/// Captures the concrete output shape of one ranked terminal so parity can be
/// asserted across ids, values, and value-with-id projections uniformly.
///

#[derive(Debug, PartialEq)]
enum RankedDirectionResult {
    Ids(Vec<Id<PushdownParityEntity>>),
    Values(Vec<Value>),
    ValuesWithIds(Vec<(Id<PushdownParityEntity>, Value)>),
}

///
/// RankedKOneCase
///
/// One matrix row for k=1 ranked/extrema parity. Each row fixes the seed data,
/// terminal family, and output projection that must stay aligned.
///

struct RankedKOneCase {
    label: &'static str,
    capability: &'static str,
    rows: &'static [(u128, u32, u32)],
    terminal: RankedKOneTerminal,
    projection: RankedKOneProjection,
    expected_first_id_tie_break: Option<u128>,
}

const RANKED_K_ONE_TOP_IDS_ROWS: [(u128, u32, u32); 5] = [
    (8_3741, 7, 90),
    (8_3742, 7, 40),
    (8_3743, 7, 90),
    (8_3744, 7, 20),
    (8_3745, 8, 99),
];
const RANKED_K_ONE_TOP_VALUES_ROWS: [(u128, u32, u32); 5] = [
    (8_3811, 7, 90),
    (8_3812, 7, 40),
    (8_3813, 7, 90),
    (8_3814, 7, 20),
    (8_3815, 8, 99),
];
const RANKED_K_ONE_BOTTOM_IDS_ROWS: [(u128, u32, u32); 5] = [
    (8_3751, 7, 10),
    (8_3752, 7, 30),
    (8_3753, 7, 10),
    (8_3754, 7, 40),
    (8_3755, 8, 99),
];
const RANKED_K_ONE_BOTTOM_VALUES_ROWS: [(u128, u32, u32); 5] = [
    (8_3821, 7, 10),
    (8_3822, 7, 30),
    (8_3823, 7, 10),
    (8_3824, 7, 40),
    (8_3825, 8, 99),
];
const RANKED_K_ONE_TOP_VALUES_WITH_IDS_ROWS: [(u128, u32, u32); 5] = [
    (8_3831, 7, 90),
    (8_3832, 7, 40),
    (8_3833, 7, 90),
    (8_3834, 7, 20),
    (8_3835, 8, 99),
];
const RANKED_K_ONE_BOTTOM_VALUES_WITH_IDS_ROWS: [(u128, u32, u32); 5] = [
    (8_3836, 7, 10),
    (8_3837, 7, 30),
    (8_3838, 7, 10),
    (8_3839, 7, 40),
    (8_3840, 8, 99),
];

fn run_ranked_k_one_terminal(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: PreparedExecutionPlan<PushdownParityEntity>,
    terminal: RankedKOneTerminal,
    projection: RankedKOneProjection,
) -> Result<RankedDirectionResult, InternalError> {
    let rank_slot = planned_slot::<PushdownParityEntity>("rank");

    match (terminal, projection) {
        (RankedKOneTerminal::Top, RankedKOneProjection::Ids) => Ok(RankedDirectionResult::Ids(
            load.top_k_by_slot(plan, rank_slot, 1)?.ids().collect(),
        )),
        (RankedKOneTerminal::Top, RankedKOneProjection::Values) => Ok(
            RankedDirectionResult::Values(load.top_k_by_values_slot(plan, rank_slot, 1)?),
        ),
        (RankedKOneTerminal::Top, RankedKOneProjection::ValuesWithIds) => Ok(
            RankedDirectionResult::ValuesWithIds(load.top_k_by_with_ids_slot(plan, rank_slot, 1)?),
        ),
        (RankedKOneTerminal::Bottom, RankedKOneProjection::Ids) => Ok(RankedDirectionResult::Ids(
            load.bottom_k_by_slot(plan, rank_slot, 1)?.ids().collect(),
        )),
        (RankedKOneTerminal::Bottom, RankedKOneProjection::Values) => Ok(
            RankedDirectionResult::Values(load.bottom_k_by_values_slot(plan, rank_slot, 1)?),
        ),
        (RankedKOneTerminal::Bottom, RankedKOneProjection::ValuesWithIds) => {
            Ok(RankedDirectionResult::ValuesWithIds(
                load.bottom_k_by_with_ids_slot(plan, rank_slot, 1)?,
            ))
        }
    }
}

fn run_ranked_k_one_extrema(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: PreparedExecutionPlan<PushdownParityEntity>,
    terminal: RankedKOneTerminal,
) -> Result<Option<Id<PushdownParityEntity>>, InternalError> {
    let rank_slot = planned_slot::<PushdownParityEntity>("rank");

    match terminal {
        RankedKOneTerminal::Top => execute_max_by_slot_terminal(load, plan, rank_slot),
        RankedKOneTerminal::Bottom => execute_min_by_slot_terminal(load, plan, rank_slot),
    }
}

fn ranked_k_one_projection_from_extrema(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: PreparedExecutionPlan<PushdownParityEntity>,
    extrema_id: Option<Id<PushdownParityEntity>>,
    projection: RankedKOneProjection,
) -> Result<RankedDirectionResult, InternalError> {
    match projection {
        RankedKOneProjection::Ids => {
            Ok(RankedDirectionResult::Ids(extrema_id.into_iter().collect()))
        }
        RankedKOneProjection::Values => {
            let projected = if let Some(target_id) = extrema_id {
                load.execute(plan)?
                    .into_iter()
                    .find(|row| row.id() == target_id)
                    .map(|row| Value::Uint(u64::from(row.entity().rank)))
                    .into_iter()
                    .collect()
            } else {
                Vec::new()
            };

            Ok(RankedDirectionResult::Values(projected))
        }
        RankedKOneProjection::ValuesWithIds => {
            let projected = if let Some(target_id) = extrema_id {
                load.execute(plan)?
                    .into_iter()
                    .find(|row| row.id() == target_id)
                    .map(|row| (target_id, Value::Uint(u64::from(row.entity().rank))))
                    .into_iter()
                    .collect()
            } else {
                Vec::new()
            };

            Ok(RankedDirectionResult::ValuesWithIds(projected))
        }
    }
}

fn first_ranked_result_id(result: &RankedDirectionResult) -> Option<Id<PushdownParityEntity>> {
    match result {
        RankedDirectionResult::Ids(ids) => ids.first().copied(),
        RankedDirectionResult::Values(_) => None,
        RankedDirectionResult::ValuesWithIds(values_with_ids) => {
            values_with_ids.first().map(|(id, _)| *id)
        }
    }
}

fn ranked_k_one_cases() -> [RankedKOneCase; 6] {
    [
        RankedKOneCase {
            label: "top_k_by_ids",
            capability: "ranked_ids",
            rows: &RANKED_K_ONE_TOP_IDS_ROWS,
            terminal: RankedKOneTerminal::Top,
            projection: RankedKOneProjection::Ids,
            expected_first_id_tie_break: Some(8_3741),
        },
        RankedKOneCase {
            label: "top_k_by_values",
            capability: "ranked_values",
            rows: &RANKED_K_ONE_TOP_VALUES_ROWS,
            terminal: RankedKOneTerminal::Top,
            projection: RankedKOneProjection::Values,
            expected_first_id_tie_break: None,
        },
        RankedKOneCase {
            label: "bottom_k_by_ids",
            capability: "ranked_ids",
            rows: &RANKED_K_ONE_BOTTOM_IDS_ROWS,
            terminal: RankedKOneTerminal::Bottom,
            projection: RankedKOneProjection::Ids,
            expected_first_id_tie_break: Some(8_3751),
        },
        RankedKOneCase {
            label: "bottom_k_by_values",
            capability: "ranked_values",
            rows: &RANKED_K_ONE_BOTTOM_VALUES_ROWS,
            terminal: RankedKOneTerminal::Bottom,
            projection: RankedKOneProjection::Values,
            expected_first_id_tie_break: None,
        },
        RankedKOneCase {
            label: "top_k_by_with_ids",
            capability: "ranked_values_with_ids",
            rows: &RANKED_K_ONE_TOP_VALUES_WITH_IDS_ROWS,
            terminal: RankedKOneTerminal::Top,
            projection: RankedKOneProjection::ValuesWithIds,
            expected_first_id_tie_break: None,
        },
        RankedKOneCase {
            label: "bottom_k_by_with_ids",
            capability: "ranked_values_with_ids",
            rows: &RANKED_K_ONE_BOTTOM_VALUES_WITH_IDS_ROWS,
            terminal: RankedKOneTerminal::Bottom,
            projection: RankedKOneProjection::ValuesWithIds,
            expected_first_id_tie_break: None,
        },
    ]
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
fn aggregate_core_unknown_field_target_fails_without_scan_when_planner_is_bypassed() {
    seed_pushdown_entities(&[(8_041, 7, 10), (8_042, 7, 20), (8_043, 7, 30)]);

    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        execute_bypassed_field_target_validation::<PushdownParityEntity>(
            crate::db::query::builder::aggregate::min_by("missing_field"),
        )
    });
    let Err(err) = result else {
        panic!("field-target unknown field should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "field-target unknown-field MIN should fail before any scan-budget consumption",
    );
    assert!(
        err.message.contains("unknown aggregate target field"),
        "unknown field taxonomy should remain explicit: {err:?}",
    );
}

#[test]
fn aggregate_core_non_orderable_field_target_fails_without_scan_when_planner_is_bypassed() {
    seed_phase_entities(&[(8_051, 10), (8_052, 20), (8_053, 30)]);

    let (result, scanned) = capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
        execute_bypassed_field_target_validation::<PhaseEntity>(
            crate::db::query::builder::aggregate::min_by("tags"),
        )
    });
    let Err(err) = result else {
        panic!("field-target MIN on list field should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "field-target non-orderable MIN should fail before any scan-budget consumption",
    );
    assert!(
        err.message.contains("does not support ordering"),
        "non-orderable field taxonomy should remain explicit: {err:?}",
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
            .map(crate::db::executor::PreparedExecutionPlan::from)
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
            .map(crate::db::executor::PreparedExecutionPlan::from)
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
            .map(crate::db::executor::PreparedExecutionPlan::from)
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("field-target MIN tie-break plan should build");
    let max_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
fn aggregate_core_secondary_index_min_uses_index_leading_order() {
    seed_pushdown_entities(&[
        (8_071, 7, 30),
        (8_072, 7, 10),
        (8_073, 7, 20),
        (8_074, 8, 5),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = secondary_group_rank_order_plan(MissingRowPolicy::Ignore, OrderDirection::Asc, 0);

    let min_id =
        execute_min_by_slot_terminal(&load, plan, planned_slot::<PushdownParityEntity>("rank"))
            .expect("secondary-index field-target MIN should succeed");

    assert_eq!(
        min_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_072)),
        "secondary-index field-target MIN should return the lowest rank id"
    );
}

#[test]
fn aggregate_core_secondary_index_max_tie_breaks_primary_key_ascending() {
    seed_pushdown_entities(&[
        (8_081, 7, 20),
        (8_082, 7, 40),
        (8_083, 7, 40),
        (8_084, 7, 10),
        (8_085, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = secondary_group_rank_order_plan(MissingRowPolicy::Ignore, OrderDirection::Desc, 0);

    let max_id =
        execute_max_by_slot_terminal(&load, plan, planned_slot::<PushdownParityEntity>("rank"))
            .expect("secondary-index field-target MAX should succeed");

    assert_eq!(
        max_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_082)),
        "secondary-index field-target MAX should pick primary key ascending within max-value ties"
    );
}

#[test]
fn aggregate_core_secondary_index_extrema_strict_single_step_scans_offset_plus_one() {
    assert_secondary_id_extrema_single_step(
        &SECONDARY_SINGLE_STEP_STRICT_ROWS,
        MissingRowPolicy::Error,
        8_833,
        8_832,
        "strict secondary",
    );
}

#[test]
fn aggregate_core_secondary_index_extrema_missing_ok_clean_single_step_scans_offset_plus_one() {
    assert_secondary_id_extrema_single_step(
        &SECONDARY_SINGLE_STEP_MISSING_OK_ROWS,
        MissingRowPolicy::Ignore,
        8_843,
        8_842,
        "missing-ok secondary",
    );
}

#[test]
fn aggregate_core_secondary_index_extrema_missing_ok_stale_leading_probe_falls_back() {
    assert_secondary_id_extrema_missing_ok_stale_fallback(
        &SECONDARY_STALE_ID_ROWS,
        &[8_851, 8_854],
    );
}

#[test]
fn aggregate_core_secondary_index_extrema_strict_stale_leading_surfaces_corruption_error() {
    assert_secondary_id_extrema_strict_stale_corruption(&SECONDARY_STALE_ID_ROWS, &[8_851, 8_854]);
}

#[test]
fn aggregate_core_field_extrema_missing_ok_stale_leading_probe_falls_back() {
    assert_secondary_field_extrema_missing_ok_stale_fallback(
        &SECONDARY_STALE_FIELD_ROWS,
        &[8_261, 8_264],
        "rank",
    );
}

#[test]
fn aggregate_core_field_extrema_strict_stale_leading_surfaces_corruption_error() {
    assert_secondary_field_extrema_strict_stale_corruption(
        &SECONDARY_STALE_FIELD_ROWS,
        &[8_261, 8_264],
        "rank",
    );
}

#[test]
fn aggregate_core_field_extrema_secondary_index_eligible_shape_locks_scan_budget() {
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
            execute_min_by_slot_terminal(
                &load,
                secondary_group_rank_order_plan(MissingRowPolicy::Ignore, OrderDirection::Asc, 0),
                planned_slot::<PushdownParityEntity>("rank"),
            )
            .expect("missing-ok secondary MIN(field) eligible shape should succeed")
        });
    let (max_by_desc, scanned_max_by_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_max_by_slot_terminal(
                &load,
                secondary_group_rank_order_plan(MissingRowPolicy::Ignore, OrderDirection::Desc, 0),
                planned_slot::<PushdownParityEntity>("rank"),
            )
            .expect("missing-ok secondary MAX(field) eligible shape should succeed")
        });

    assert_eq!(
        min_by_asc.map(|id| id.key()),
        Some(Ulid::from_u128(8_281)),
        "missing-ok secondary MIN(field) eligible shape should return the first ordered candidate",
    );
    assert_eq!(
        max_by_desc.map(|id| id.key()),
        Some(Ulid::from_u128(8_284)),
        "missing-ok secondary MAX(field) eligible shape should return the first ordered DESC candidate",
    );
    assert_eq!(
        scanned_min_by_asc, 4,
        "missing-ok secondary MIN(field) eligible shape should scan the full group window under current contract",
    );
    assert_eq!(
        scanned_max_by_desc, 4,
        "missing-ok secondary MAX(field) eligible shape should scan the full group window under current contract",
    );
}

#[test]
fn aggregate_core_field_extrema_index_leading_min_uses_one_key_probe_hint() {
    seed_indexed_metrics_rows(&[(8_511, 10, "a"), (8_512, 10, "b"), (8_513, 30, "c")]);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    let (min_by_tag, scanned_min_by_tag) =
        capture_rows_scanned_for_entity(IndexedMetricsEntity::PATH, || {
            execute_min_by_slot_terminal(
                &load,
                indexed_metrics_tag_index_range_plan(MissingRowPolicy::Ignore),
                planned_slot::<IndexedMetricsEntity>("tag"),
            )
            .expect("index-leading MIN(field) should succeed")
        });

    assert_eq!(
        min_by_tag.map(|id| id.key()),
        Some(Ulid::from_u128(8_511)),
        "index-leading MIN(field) should use primary-key ascending tie-break inside the first field-value group",
    );
    assert_eq!(
        scanned_min_by_tag, 1,
        "index-leading MIN(field) should resolve through one-key bounded probe",
    );
}

#[test]
fn aggregate_core_field_extrema_unique_index_leading_max_uses_one_key_probe_hint() {
    seed_unique_index_range_entities(&[(8_531, 10), (8_532, 20), (8_533, 30)]);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let mut logical_plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            UNIQUE_INDEX_RANGE_INDEX_MODELS[0],
            vec![],
            std::ops::Bound::Included(Value::Uint(0)),
            std::ops::Bound::Excluded(Value::Uint(100)),
        ),
        MissingRowPolicy::Ignore,
    );
    logical_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("code".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    let plan = PreparedExecutionPlan::<UniqueIndexRangeEntity>::new(logical_plan);

    let (max_by_code, scanned_max_by_code) =
        capture_rows_scanned_for_entity(UniqueIndexRangeEntity::PATH, || {
            execute_max_by_slot_terminal(
                &load,
                plan,
                planned_slot::<UniqueIndexRangeEntity>("code"),
            )
            .expect("unique-index MAX(field) should succeed")
        });

    assert_eq!(
        max_by_code.map(|id| id.key()),
        Some(Ulid::from_u128(8_533)),
        "unique-index MAX(field) should resolve to the highest ordered code",
    );
    assert_eq!(
        scanned_max_by_code, 1,
        "unique-index MAX(field) should resolve through one-key bounded probe",
    );
}

#[test]
fn aggregate_core_field_extrema_index_leading_min_ignore_stale_probe_retries_unbounded() {
    seed_indexed_metrics_rows(&[(8_521, 10, "a"), (8_522, 20, "b"), (8_523, 30, "c")]);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    remove_indexed_metrics_row_data(8_521);

    let (min_by_tag, scanned_min_by_tag) =
        capture_rows_scanned_for_entity(IndexedMetricsEntity::PATH, || {
            execute_min_by_slot_terminal(
                &load,
                indexed_metrics_tag_index_range_plan(MissingRowPolicy::Ignore),
                planned_slot::<IndexedMetricsEntity>("tag"),
            )
            .expect("stale-leading index-leading MIN(field) should succeed in ignore mode")
        });

    assert_eq!(
        min_by_tag.map(|id| id.key()),
        Some(Ulid::from_u128(8_522)),
        "ignore-mode index-leading MIN(field) should retry unbounded and skip stale leading keys",
    );
    assert!(
        scanned_min_by_tag >= 2,
        "ignore-mode stale-leading MIN(field) should scan beyond one-key probe due to fallback retry",
    );
}

#[test]
fn aggregate_core_field_extrema_negative_lock_distinct_and_offset_shapes_avoid_single_step_probe() {
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
            execute_min_by_slot_terminal(
                &load,
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(u32_eq_predicate("group", 7))
                    .distinct()
                    .order_by("rank")
                    .plan()
                    .map(crate::db::executor::PreparedExecutionPlan::from)
                    .expect("distinct MIN(field) plan should build"),
                planned_slot::<PushdownParityEntity>("rank"),
            )
            .expect("distinct MIN(field) should succeed")
        });
    let (offset_max, scanned_offset_max) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            execute_max_by_slot_terminal(
                &load,
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(u32_eq_predicate("group", 7))
                    .order_by("rank")
                    .offset(2)
                    .plan()
                    .map(crate::db::executor::PreparedExecutionPlan::from)
                    .expect("offset MAX(field) plan should build"),
                planned_slot::<PushdownParityEntity>("rank"),
            )
            .expect("offset MAX(field) should succeed")
        });

    assert_eq!(
        distinct_min.map(|id| id.key()),
        Some(Ulid::from_u128(8_301)),
        "distinct MIN(field) should preserve canonical parity",
    );
    assert_eq!(
        offset_max.map(|id| id.key()),
        Some(Ulid::from_u128(8_305)),
        "offset MAX(field) should preserve canonical parity",
    );
    assert!(
        scanned_distinct_min >= 2,
        "distinct MIN(field) should not collapse to single-step probe",
    );
    assert!(
        scanned_offset_max >= 3,
        "offset MAX(field) should remain bounded by window traversal, not single-step probe",
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn aggregate_core_field_terminal_error_classification_matrix() {
    seed_pushdown_entities(&[(8_291, 7, 10), (8_292, 7, 20), (8_293, 7, 30)]);
    let pushdown_load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let unknown_field_min_error = execute_min_by_slot_terminal(
        &pushdown_load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("unknown-field MIN(field) plan should build"),
        planned_slot::<PushdownParityEntity>("missing_field"),
    )
    .expect_err("unknown field MIN(field) should fail");
    let unknown_field_median_error = execute_median_by_slot_terminal(
        &pushdown_load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("unknown-field MEDIAN(field) plan should build"),
        planned_slot::<PushdownParityEntity>("missing_field"),
    )
    .expect_err("unknown field MEDIAN(field) should fail");
    let unknown_field_count_distinct_error = execute_projection_count_distinct_boundary(
        &pushdown_load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("unknown-field COUNT_DISTINCT(field) plan should build"),
        planned_slot::<PushdownParityEntity>("missing_field"),
    )
    .expect_err("unknown field COUNT_DISTINCT(field) should fail");
    let unknown_field_min_max_error = execute_min_max_by_slot_terminal(
        &pushdown_load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("unknown-field MIN_MAX(field) plan should build"),
        planned_slot::<PushdownParityEntity>("missing_field"),
    )
    .expect_err("unknown field MIN_MAX(field) should fail");
    let non_numeric_error = execute_numeric_field_boundary(
        &pushdown_load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("non-numeric SUM(field) plan should build"),
        planned_slot::<PushdownParityEntity>("label"),
        ScalarNumericFieldBoundaryRequest::Sum,
    )
    .expect_err("non-numeric SUM(field) should fail");
    remove_pushdown_row_data(8_291);
    let strict_stale_error = execute_min_by_slot_terminal(
        &pushdown_load,
        secondary_group_rank_order_plan(MissingRowPolicy::Error, OrderDirection::Asc, 0),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect_err("strict stale-leading MIN(field) should fail");
    let strict_stale_median_error = execute_median_by_slot_terminal(
        &pushdown_load,
        secondary_group_rank_order_plan(MissingRowPolicy::Error, OrderDirection::Asc, 0),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect_err("strict stale-leading MEDIAN(field) should fail");
    let strict_stale_count_distinct_error = execute_projection_count_distinct_boundary(
        &pushdown_load,
        secondary_group_rank_order_plan(MissingRowPolicy::Error, OrderDirection::Asc, 0),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect_err("strict stale-leading COUNT_DISTINCT(field) should fail");
    let strict_stale_min_max_error = execute_min_max_by_slot_terminal(
        &pushdown_load,
        secondary_group_rank_order_plan(MissingRowPolicy::Error, OrderDirection::Asc, 0),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect_err("strict stale-leading MIN_MAX(field) should fail");

    seed_phase_entities(&[(8_294, 10), (8_295, 20)]);
    let phase_load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let non_orderable_min_error = execute_min_by_slot_terminal(
        &phase_load,
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("non-orderable MIN(field) plan should build"),
        planned_slot::<PhaseEntity>("tags"),
    )
    .expect_err("non-orderable MIN(field) should fail");
    let non_orderable_median_error = execute_median_by_slot_terminal(
        &phase_load,
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("non-orderable MEDIAN(field) plan should build"),
        planned_slot::<PhaseEntity>("tags"),
    )
    .expect_err("non-orderable MEDIAN(field) should fail");
    let non_orderable_min_max_error = execute_min_max_by_slot_terminal(
        &phase_load,
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("non-orderable MIN_MAX(field) plan should build"),
        planned_slot::<PhaseEntity>("tags"),
    )
    .expect_err("non-orderable MIN_MAX(field) should fail");

    assert_eq!(
        unknown_field_min_error.class,
        ErrorClass::Unsupported,
        "unknown field MIN(field) should classify as Unsupported",
    );
    assert_eq!(
        unknown_field_median_error.class,
        ErrorClass::Unsupported,
        "unknown field MEDIAN(field) should classify as Unsupported",
    );
    assert_eq!(
        unknown_field_count_distinct_error.class,
        ErrorClass::Unsupported,
        "unknown field COUNT_DISTINCT(field) should classify as Unsupported",
    );
    assert_eq!(
        unknown_field_min_max_error.class,
        ErrorClass::Unsupported,
        "unknown field MIN_MAX(field) should classify as Unsupported",
    );
    assert_eq!(
        non_orderable_min_error.class,
        ErrorClass::Unsupported,
        "non-orderable MIN(field) should classify as Unsupported",
    );
    assert_eq!(
        non_orderable_median_error.class,
        ErrorClass::Unsupported,
        "non-orderable MEDIAN(field) should classify as Unsupported",
    );
    assert_eq!(
        non_orderable_min_max_error.class,
        ErrorClass::Unsupported,
        "non-orderable MIN_MAX(field) should classify as Unsupported",
    );
    assert_eq!(
        non_numeric_error.class,
        ErrorClass::Unsupported,
        "non-numeric SUM(field) should classify as Unsupported",
    );
    assert_eq!(
        strict_stale_error.class,
        ErrorClass::Corruption,
        "strict stale-leading MIN(field) should classify as Corruption",
    );
    assert_eq!(
        strict_stale_median_error.class,
        ErrorClass::Corruption,
        "strict stale-leading MEDIAN(field) should classify as Corruption",
    );
    assert_eq!(
        strict_stale_count_distinct_error.class,
        ErrorClass::Corruption,
        "strict stale-leading COUNT_DISTINCT(field) should classify as Corruption",
    );
    assert_eq!(
        strict_stale_min_max_error.class,
        ErrorClass::Corruption,
        "strict stale-leading MIN_MAX(field) should classify as Corruption",
    );
}

#[test]
fn aggregate_core_sum_distinct_uses_grouped_global_distinct_path() {
    seed_pushdown_entities(&[
        (8_0991, 7, 10),
        (8_0992, 7, 20),
        (8_0993, 7, 20),
        (8_0994, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("sum_distinct_by(rank) plan should build");

    let sum_distinct = load
        .execute_numeric_field_boundary(
            plan,
            planned_slot::<PushdownParityEntity>("rank"),
            ScalarNumericFieldBoundaryRequest::SumDistinct,
        )
        .expect("sum_distinct_by(rank) should succeed");

    assert_eq!(
        sum_distinct,
        crate::types::Decimal::from_num(30_u64),
        "sum_distinct_by(rank) should sum unique rank values only",
    );
}

#[test]
fn aggregate_core_avg_distinct_uses_grouped_global_distinct_path() {
    seed_pushdown_entities(&[
        (8_1091, 7, 10),
        (8_1092, 7, 20),
        (8_1093, 7, 20),
        (8_1094, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("avg_distinct_by(rank) plan should build");

    let avg_distinct = load
        .execute_numeric_field_boundary(
            plan,
            planned_slot::<PushdownParityEntity>("rank"),
            ScalarNumericFieldBoundaryRequest::AvgDistinct,
        )
        .expect("avg_distinct_by(rank) should succeed");

    assert_eq!(
        avg_distinct,
        crate::types::Decimal::from_num(15_u64),
        "avg_distinct_by(rank) should average unique rank values only",
    );
}

#[test]
fn aggregate_core_sum_distinct_is_insertion_order_invariant() {
    seed_pushdown_entities(&[
        (809_911, 7, 10),
        (809_912, 7, 20),
        (809_913, 7, 20),
        (809_914, 7, 30),
        (809_915, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan_asc = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(u32_eq_predicate("group", 7))
        .order_by("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("sum_distinct_by(rank) ASC plan should build");
    let plan_desc = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(u32_eq_predicate("group", 7))
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("sum_distinct_by(rank) DESC plan should build");

    let sum_distinct_asc = load
        .execute_numeric_field_boundary(
            plan_asc,
            planned_slot::<PushdownParityEntity>("rank"),
            ScalarNumericFieldBoundaryRequest::SumDistinct,
        )
        .expect("sum_distinct_by(rank) ASC should succeed");
    let sum_distinct_desc = load
        .execute_numeric_field_boundary(
            plan_desc,
            planned_slot::<PushdownParityEntity>("rank"),
            ScalarNumericFieldBoundaryRequest::SumDistinct,
        )
        .expect("sum_distinct_by(rank) DESC should succeed");

    assert_eq!(
        sum_distinct_asc, sum_distinct_desc,
        "sum_distinct_by(rank) should be invariant to insertion/traversal order",
    );
}

#[test]
fn aggregate_core_sum_distinct_handles_large_values_without_wrap() {
    seed_pushdown_entities(&[
        (809_921, 7, u32::MAX),
        (809_922, 7, u32::MAX - 1),
        (809_923, 7, u32::MAX),
        (809_924, 8, 42),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(u32_eq_predicate("group", 7))
        .order_by("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("sum_distinct_by(rank) large-value plan should build");

    let sum_distinct = load
        .execute_numeric_field_boundary(
            plan,
            planned_slot::<PushdownParityEntity>("rank"),
            ScalarNumericFieldBoundaryRequest::SumDistinct,
        )
        .expect("sum_distinct_by(rank) large values should succeed")
        .expect("sum_distinct_by(rank) should return a value");
    let expected = crate::types::Decimal::from_num(u64::from(u32::MAX) + u64::from(u32::MAX - 1))
        .expect("large expected decimal should convert");

    assert_eq!(
        sum_distinct, expected,
        "sum_distinct_by(rank) should preserve large-value accumulation without wraparound",
    );
}

#[test]
fn aggregate_core_sum_distinct_preserves_decimal_integer_canonical_scale() {
    seed_pushdown_entities(&[
        (809_931, 7, 10),
        (809_932, 7, 20),
        (809_933, 7, 20),
        (809_934, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("sum_distinct_by(rank) canonical-scale plan should build");

    let sum_distinct = load
        .execute_numeric_field_boundary(
            plan,
            planned_slot::<PushdownParityEntity>("rank"),
            ScalarNumericFieldBoundaryRequest::SumDistinct,
        )
        .expect("sum_distinct_by(rank) canonical-scale should succeed")
        .expect("sum_distinct_by(rank) should return a value");

    assert_eq!(
        sum_distinct.scale(),
        0,
        "sum_distinct_by(rank) should preserve canonical integer decimal scale",
    );
}

#[test]
fn aggregate_core_grouped_having_supported_operator_executes_through_planner_shape() {
    seed_pushdown_entities(&[(8_1201, 7, 10), (8_1202, 7, 20), (8_1203, 7, 30)]);
    let session = crate::db::DbSession::new(DB);

    let grouped = session
        .load::<PushdownParityEntity>()
        .group_by("group")
        .expect("group_by(group) should resolve")
        .aggregate(crate::db::count())
        .having_aggregate(0, CompareOp::Gt, Value::Uint(0))
        .expect("having aggregate should build")
        .execute()
        .and_then(crate::db::LoadQueryResult::into_grouped)
        .expect("planner-validated grouped HAVING should execute");

    assert_eq!(
        grouped.rows().len(),
        1,
        "supported grouped HAVING shape should execute with one grouped row for one seeded group",
    );
}

#[test]
fn aggregate_core_grouped_having_unsupported_operator_fails_closed_when_planner_is_bypassed() {
    seed_pushdown_entities(&[(8_1211, 7, 10), (8_1212, 7, 20), (8_1213, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let grouped = AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore)
        .into_grouped_with_having(
            crate::db::query::plan::GroupSpec {
                group_fields: vec![
                    crate::db::query::plan::FieldSlot::resolve(
                        <PushdownParityEntity as crate::traits::EntitySchema>::MODEL,
                        "group",
                    )
                    .expect("group field should resolve for bypass fixture"),
                ],
                aggregates: vec![crate::db::query::plan::GroupAggregateSpec {
                    kind: crate::db::query::plan::AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: crate::db::query::plan::GroupedExecutionConfig::unbounded(),
            },
            Some(crate::db::query::plan::GroupHavingExpr::compare_symbol(
                crate::db::query::plan::GroupHavingSymbol::AggregateIndex(0),
                CompareOp::In,
                Value::List(vec![Value::Uint(1)]),
            )),
        );
    let plan = crate::db::executor::PreparedExecutionPlan::<PushdownParityEntity>::new(grouped);

    let err = load
        .execute_grouped_paged_with_cursor_traced(
            plan,
            crate::db::cursor::GroupedPlannedCursor::none(),
        )
        .expect_err("bypassed planner shape should fail with executor invariant");

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
    assert!(
        err.message
            .contains("unsupported grouped HAVING operator reached executor"),
        "bypassed grouped HAVING operator should fail with executor invariant taxonomy: {err:?}",
    );
}

#[test]
fn aggregate_core_grouped_global_distinct_unsupported_kind_fails_without_scan() {
    seed_pushdown_entities(&[(8_1221, 7, 10), (8_1222, 7, 20), (8_1223, 7, 30)]);
    let mut grouped = AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(crate::db::query::plan::GroupSpec {
            group_fields: Vec::new(),
            aggregates: vec![crate::db::query::plan::GroupAggregateSpec {
                kind: crate::db::query::plan::AggregateKind::Exists,
                target_field: Some("rank".to_string()),
                distinct: true,
            }],
            execution: crate::db::query::plan::GroupedExecutionConfig::unbounded(),
        });
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        grouped.finalize_static_planning_shape_for_model(
            <PushdownParityEntity as crate::traits::EntitySchema>::MODEL,
        )
    });
    let err = result.expect_err("unsupported global DISTINCT grouped aggregate kind should fail during static planning freeze");

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Planner);
    assert_eq!(
        scanned, 0,
        "unsupported global DISTINCT grouped aggregate kind should fail before scan-budget consumption",
    );
    assert!(
        err.message
            .contains("global DISTINCT grouped aggregate shape supports COUNT/SUM/AVG only"),
        "unsupported global DISTINCT grouped aggregate kind should fail with planner-policy invariant text: {err:?}",
    );
}

#[test]
fn aggregate_core_grouped_scalar_distinct_policy_violation_fails_without_scan() {
    seed_pushdown_entities(&[(8_1231, 7, 10), (8_1232, 7, 20), (8_1233, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let mut grouped = AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(crate::db::query::plan::GroupSpec {
            group_fields: vec![
                crate::db::query::plan::FieldSlot::resolve(
                    <PushdownParityEntity as crate::traits::EntitySchema>::MODEL,
                    "group",
                )
                .expect("group field should resolve for bypass fixture"),
            ],
            aggregates: vec![crate::db::query::plan::GroupAggregateSpec {
                kind: crate::db::query::plan::AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: crate::db::query::plan::GroupedExecutionConfig::unbounded(),
        });
    grouped.scalar_plan_mut().distinct = true;
    let plan = crate::db::executor::PreparedExecutionPlan::<PushdownParityEntity>::new(grouped);

    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.execute_grouped_paged_with_cursor_traced(
            plan,
            crate::db::cursor::GroupedPlannedCursor::none(),
        )
    });
    let err = result.expect_err(
        "bypassed grouped scalar DISTINCT policy violation should fail with executor invariant",
    );

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
    assert_eq!(
        scanned, 0,
        "bypassed grouped scalar DISTINCT policy violation should fail before scan-budget consumption",
    );
    assert!(
        err.message
            .contains("grouped DISTINCT requires ordered-group adjacency proof"),
        "bypassed grouped scalar DISTINCT policy violation should fail with planner-policy invariant text: {err:?}",
    );
}

#[test]
fn aggregate_core_grouped_field_target_aggregate_fails_without_scan() {
    seed_pushdown_entities(&[(8_1241, 7, 10), (8_1242, 7, 20), (8_1243, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let grouped = AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(crate::db::query::plan::GroupSpec {
            group_fields: vec![
                crate::db::query::plan::FieldSlot::resolve(
                    <PushdownParityEntity as crate::traits::EntitySchema>::MODEL,
                    "group",
                )
                .expect("group field should resolve for bypass fixture"),
            ],
            aggregates: vec![crate::db::query::plan::GroupAggregateSpec {
                kind: crate::db::query::plan::AggregateKind::First,
                target_field: Some("rank".to_string()),
                distinct: false,
            }],
            execution: crate::db::query::plan::GroupedExecutionConfig::unbounded(),
        });
    let plan = crate::db::executor::PreparedExecutionPlan::<PushdownParityEntity>::new(grouped);

    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.execute_grouped_paged_with_cursor_traced(
            plan,
            crate::db::cursor::GroupedPlannedCursor::none(),
        )
    });
    let err = result
        .expect_err("bypassed grouped field-target aggregate should fail with executor invariant");

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
    assert_eq!(
        scanned, 0,
        "bypassed grouped field-target aggregate should fail before scan-budget consumption",
    );
    assert!(
        err.message
            .contains("grouped field-target aggregate reached executor after planning"),
        "bypassed grouped field-target aggregate should fail with executor invariant taxonomy: {err:?}",
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
            .map(crate::db::executor::PreparedExecutionPlan::from)
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
fn aggregate_core_nth_boundary_matrix_respects_window_and_out_of_range() {
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
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(3)
    };
    let expected_response = load
        .execute(
            base_query()
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("nth boundary baseline plan should build"),
        )
        .expect("nth boundary baseline execute should succeed");

    for nth in [0usize, 1, 2, 3, usize::MAX] {
        let actual = execute_nth_by_slot_terminal(
            &load,
            base_query()
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("nth boundary plan should build"),
            planned_slot::<PushdownParityEntity>("rank"),
            nth,
        )
        .expect("nth boundary aggregate should succeed");

        assert_eq!(
            actual,
            expected_nth_by_rank_id(&expected_response, nth),
            "nth boundary parity failed for n={nth}",
        );
    }

    let empty_window_nth_zero = execute_nth_by_slot_terminal(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(50)
            .limit(3)
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("empty-window nth plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
        0,
    )
    .expect("empty-window nth should succeed");

    assert_eq!(expected_response.len(), 3);
    assert_eq!(empty_window_nth_zero, None);
}

#[test]
fn aggregate_core_nth_unknown_and_non_orderable_targets_fail_without_scan() {
    seed_pushdown_entities(&[(8_151, 7, 10), (8_152, 7, 20)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let unknown_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
            .map(crate::db::executor::PreparedExecutionPlan::from)
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
fn aggregate_core_median_order_direction_invariant_on_same_window() {
    seed_pushdown_entities(&[
        (8_2051, 7, 10),
        (8_2052, 7, 20),
        (8_2053, 7, 20),
        (8_2054, 7, 40),
        (8_2055, 7, 50),
        (8_2056, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let asc_median = execute_median_by_slot_terminal(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by("id")
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("median ASC plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("median_by(rank) ASC should succeed");
    let desc_median = execute_median_by_slot_terminal(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("median DESC plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("median_by(rank) DESC should succeed");

    assert_eq!(
        asc_median, desc_median,
        "median_by(rank) should be invariant to query order direction on the same row window",
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
            .map(crate::db::executor::PreparedExecutionPlan::from)
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
            .map(crate::db::executor::PreparedExecutionPlan::from)
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

#[test]
fn aggregate_core_min_max_metamorphic_matrix_matches_individual_extrema() {
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
            let mut query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
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

        let min_max = execute_min_max_by_slot_terminal(
            &load,
            build_query()
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("metamorphic min_max plan should build"),
            planned_slot::<PushdownParityEntity>("rank"),
        )
        .expect("metamorphic min_max_by(rank) should succeed");
        let min_by = execute_min_by_slot_terminal(
            &load,
            build_query()
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("metamorphic min plan should build"),
            planned_slot::<PushdownParityEntity>("rank"),
        )
        .expect("metamorphic min_by(rank) should succeed");
        let max_by = execute_max_by_slot_terminal(
            &load,
            build_query()
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("metamorphic max plan should build"),
            planned_slot::<PushdownParityEntity>("rank"),
        )
        .expect("metamorphic max_by(rank) should succeed");

        assert_eq!(
            min_max,
            min_by.zip(max_by),
            "metamorphic min_max parity failed for case={label}",
        );
    }
}

#[test]
fn aggregate_core_min_max_empty_window_returns_none() {
    seed_pushdown_entities(&[(8_2031, 7, 10), (8_2032, 7, 20), (8_2033, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let min_max = execute_min_max_by_slot_terminal(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by("id")
            .offset(50)
            .limit(2)
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("empty-window min_max plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("empty-window min_max_by(rank) should succeed");

    assert_eq!(min_max, None, "empty-window min_max_by should return None");
}

#[test]
fn aggregate_core_min_max_single_row_returns_same_id_pair() {
    seed_pushdown_entities(&[(8_2041, 7, 10), (8_2042, 7, 20), (8_2043, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let min_max = execute_min_max_by_slot_terminal(
        &load,
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by("id")
            .offset(1)
            .limit(1)
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("single-row min_max plan should build"),
        planned_slot::<PushdownParityEntity>("rank"),
    )
    .expect("single-row min_max_by(rank) should succeed");

    assert_eq!(
        min_max.map(|(min_id, max_id)| (min_id.key(), max_id.key())),
        Some((Ulid::from_u128(8_2042), Ulid::from_u128(8_2042))),
        "single-row min_max_by should return the same id for both extrema",
    );
}

#[test]
fn aggregate_core_numeric_field_unknown_target_fails_without_scan() {
    seed_pushdown_entities(&[(8_101, 7, 10), (8_102, 7, 20)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(PreparedExecutionPlan::from)
        .expect("numeric field unknown-target plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.execute_numeric_field_boundary(
            plan,
            planned_slot::<PushdownParityEntity>("missing_field"),
            ScalarNumericFieldBoundaryRequest::Sum,
        )
    });
    let Err(err) = result else {
        panic!("sum_by(missing_field) should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(scanned, 0);
}

#[test]
fn aggregate_core_numeric_field_non_numeric_target_fails_without_scan() {
    seed_pushdown_entities(&[(8_111, 7, 10), (8_112, 7, 20)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(PreparedExecutionPlan::from)
        .expect("numeric field non-numeric target plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.execute_numeric_field_boundary(
            plan,
            planned_slot::<PushdownParityEntity>("label"),
            ScalarNumericFieldBoundaryRequest::Avg,
        )
    });
    let Err(err) = result else {
        panic!("avg_by(label) should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(scanned, 0);
}

#[test]
fn aggregate_core_top_k_by_direction_invariance_across_forced_access_shapes() {
    // Phase 1: force a full-scan shape and assert ASC/DESC base-order invariance.
    seed_simple_entities(&[8_3941, 8_3942, 8_3943, 8_3944, 8_3945, 8_3946]);
    let simple_load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let full_scan_top_ids_for = |direction: OrderDirection| {
        let query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore);
        let query = match direction {
            OrderDirection::Asc => query.order_by("id"),
            OrderDirection::Desc => query.order_by_desc("id"),
        };
        let plan = query
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("top_k_by full-scan direction-invariance plan should build");
        assert!(
            matches!(
                execution_root_node_type(&plan),
                ExplainExecutionNodeType::FullScan
            ),
            "top_k_by full-scan direction invariance test must force FullScan",
        );

        simple_load
            .top_k_by_slot(plan, planned_slot::<SimpleEntity>("id"), 3)
            .expect("top_k_by(id, 3) should succeed for full-scan direction matrix")
            .ids()
            .collect::<Vec<_>>()
    };
    let full_scan_asc = full_scan_top_ids_for(OrderDirection::Asc);
    let full_scan_desc = full_scan_top_ids_for(OrderDirection::Desc);
    assert_eq!(
        full_scan_asc, full_scan_desc,
        "top_k_by(id, k) should be invariant to ASC/DESC base order under forced FullScan",
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
        let query = Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
            .filter(code_range.clone());
        let query = match direction {
            OrderDirection::Asc => query.order_by("code"),
            OrderDirection::Desc => query.order_by_desc("code"),
        };
        let plan = query
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("top_k_by index-range direction-invariance plan should build");
        assert!(
            matches!(
                execution_root_node_type(&plan),
                ExplainExecutionNodeType::IndexRangeScan
            ),
            "top_k_by index-range direction invariance test must force IndexRange",
        );

        range_load
            .top_k_by_slot(plan, planned_slot::<UniqueIndexRangeEntity>("code"), 3)
            .expect("top_k_by(code, 3) should succeed for index-range direction matrix")
            .ids()
            .collect::<Vec<_>>()
    };
    let index_range_asc = index_range_top_ids_for(OrderDirection::Asc);
    let index_range_desc = index_range_top_ids_for(OrderDirection::Desc);
    assert_eq!(
        index_range_asc, index_range_desc,
        "top_k_by(code, k) should be invariant to ASC/DESC base order under forced IndexRange",
    );
}

#[test]
fn aggregate_core_rank_k_one_extrema_equivalence_matrix() {
    for case in ranked_k_one_cases() {
        seed_pushdown_entities(case.rows);
        let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
        let build_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("id")
                .limit(4)
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("ranked k-one equivalence matrix plan should build")
        };

        // Phase 1: execute ranked terminal and extrema anchor while capturing scan budgets.
        let (actual, scanned_terminal) =
            capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
                run_ranked_k_one_terminal(&load, build_plan(), case.terminal, case.projection)
                    .expect("ranked k-one terminal matrix execution should succeed")
            });
        let (extrema_id, scanned_extrema) =
            capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
                run_ranked_k_one_extrema(&load, build_plan(), case.terminal)
                    .expect("ranked k-one extrema matrix execution should succeed")
            });

        // Phase 2: project extrema output into terminal shape and assert parity.
        let expected =
            ranked_k_one_projection_from_extrema(&load, build_plan(), extrema_id, case.projection)
                .expect("ranked k-one expected projection should succeed");
        assert_eq!(
            actual, expected,
            "ranked k-one extrema equivalence failed for case={} capability={}",
            case.label, case.capability
        );

        // Phase 3: assert deterministic tie-break and scan-budget dominance contracts.
        if let Some(expected_first_id_tie_break) = case.expected_first_id_tie_break {
            assert_eq!(
                first_ranked_result_id(&actual).map(|id| id.key()),
                Some(Ulid::from_u128(expected_first_id_tie_break)),
                "ranked k-one tie-break contract failed for case={}",
                case.label
            );
        }
        assert!(
            scanned_terminal >= scanned_extrema,
            "ranked k-one terminal scan budget must dominate extrema for case={} capability={}",
            case.label,
            case.capability
        );
    }
}

#[test]
fn aggregate_core_rank_k_one_extrema_equivalence_matrix_covers_all_projection_forms() {
    let labels = ranked_k_one_cases().map(|case| case.label);
    assert_eq!(
        labels,
        [
            "top_k_by_ids",
            "top_k_by_values",
            "bottom_k_by_ids",
            "bottom_k_by_values",
            "top_k_by_with_ids",
            "bottom_k_by_with_ids",
        ],
        "ranked k-one extrema equivalence matrix must enumerate all rank projection forms",
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn aggregate_core_take_and_rank_terminals_k_zero_return_empty_with_execute_scan_parity() {
    seed_pushdown_entities(&[
        (8_3761, 7, 10),
        (8_3762, 7, 20),
        (8_3763, 7, 30),
        (8_3764, 7, 40),
        (8_3765, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let rank_slot = planned_slot::<PushdownParityEntity>("rank");
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(3)
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("k-zero terminal plan should build")
    };

    // Phase 1: execute the non-truncated baseline once so every k=0 terminal
    // can assert that it preserves the same scan-budget contract.
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
            load.top_k_by_slot(build_plan(), rank_slot.clone(), 0)
                .expect("top_k_by(rank, 0) should succeed and return an empty response")
        });
    let (bottom_k_zero, scanned_bottom_k_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_slot(build_plan(), rank_slot.clone(), 0)
                .expect("bottom_k_by(rank, 0) should succeed and return an empty response")
        });
    let (top_k_values_zero, scanned_top_k_values_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_values_slot(build_plan(), rank_slot.clone(), 0)
                .expect("top_k_by_values(rank, 0) should succeed and return an empty response")
        });
    let (bottom_k_values_zero, scanned_bottom_k_values_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_values_slot(build_plan(), rank_slot.clone(), 0)
                .expect("bottom_k_by_values(rank, 0) should succeed and return an empty response")
        });
    let (top_k_with_ids_zero, scanned_top_k_with_ids_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_with_ids_slot(build_plan(), rank_slot.clone(), 0)
                .expect("top_k_by_with_ids(rank, 0) should succeed and return an empty response")
        });
    let (bottom_k_with_ids_zero, scanned_bottom_k_with_ids_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_with_ids_slot(build_plan(), rank_slot, 0)
                .expect("bottom_k_by_with_ids(rank, 0) should succeed and return an empty response")
        });

    // Phase 2: assert empty outputs first so the scan-budget checks below stay
    // focused on continuation semantics rather than terminal shape.
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

    // Phase 3: preserve the old matrix contract that k=0 truncation does not
    // alter the underlying execute() scan budget for any ranked terminal shape.
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
