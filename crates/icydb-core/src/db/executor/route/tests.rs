//! Module: db::executor::route::tests
//! Responsibility: module-local ownership and contracts for db::executor::route::tests.
//! Does not own: production route behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use super::{
    AGGREGATE_FAST_PATH_ORDER, ExecutionModeRouteCase, FastPathOrder, GroupedExecutionStrategy,
    GroupedRouteDecisionOutcome, LOAD_FAST_PATH_ORDER, LoadOrderRouteContract,
    LoadTerminalFastPathContract, RouteExecutionMode, TopNSeekSpec,
    build_execution_route_plan_for_aggregate_spec_with_model,
    build_execution_route_plan_for_grouped_plan, build_execution_route_plan_for_load_with_model,
    build_execution_route_plan_for_mutation_with_model,
    build_initial_execution_route_plan_for_load_with_model_store_witness,
    derive_load_terminal_fast_path_contract_for_model,
    grouped_ordered_runtime_revalidation_flag_count_guard,
    grouped_plan_metrics_strategy_for_execution_strategy, route_capability_flag_count_guard,
    route_execution_mode_case_count_guard, route_shape_kind_count_guard,
};
use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        cursor::CursorBoundary,
        data::DataStore,
        direction::Direction,
        executor::{
            ExecutionPlan, ExecutionPreparation,
            aggregate::AggregateFoldMode,
            aggregate::capability::AggregateFieldExtremaIneligibilityReason,
            continuation::{ContinuationMode, ScalarContinuationContext},
            plan_metrics::GroupedPlanMetricsStrategy,
            preparation::slot_map_for_model_plan,
        },
        index::{IndexCompilePolicy, IndexStore, compile_index_program},
        predicate::{CompareOp, MissingRowPolicy, Predicate},
        query::builder::aggregate,
        query::explain::{
            ExplainGroupAggregate, ExplainGroupField, ExplainGroupHaving, ExplainGroupHavingClause,
            ExplainGroupHavingSymbol, ExplainGroupedStrategy, ExplainGrouping,
        },
        query::plan::{
            AccessPlannedQuery, AggregateKind, CoveringExistingRowMode, CoveringReadFieldSource,
            DeleteSpec, FieldSlot, GroupAggregateSpec, GroupDistinctPolicyReason,
            GroupHavingClause, GroupHavingSpec, GroupHavingSymbol, GroupSpec,
            GroupedExecutionConfig, GroupedPlanStrategyHint, OrderDirection, OrderSpec, PageSpec,
            QueryMode,
            expr::{FieldId, ProjectionSelection},
            grouped_executor_handoff, grouped_plan_strategy_hint,
        },
        registry::StoreHandle,
    },
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
    testing::test_memory,
    traits::{EntitySchema, Path},
    types::Ulid,
    value::Value,
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, fs, ops::Bound};

const ROUTE_FEATURE_SOFT_BUDGET_DELTA: usize = 1;
const ROUTE_CAPABILITY_FLAG_BASELINE_0247: usize = 9;
const ROUTE_EXECUTION_MODE_CASE_BASELINE_0246: usize = 3;
const ROUTE_SHAPE_KIND_BASELINE_0256: usize = 4;
const ROUTE_GROUPED_RUNTIME_REVALIDATION_FLAG_BASELINE_0251: usize = 3;

crate::test_canister! {
    ident = RouteCapabilityTestCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = RouteCapabilityTestStore,
    canister = RouteCapabilityTestCanister,
}

///
/// RouteCapabilityEntity
///
/// Small route-owner test fixture that exposes one primary key, one indexed
/// scalar field, and one non-indexed scalar field for capability checks.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct RouteCapabilityEntity {
    id: Ulid,
    rank: u32,
    label: String,
    scores: Vec<u32>,
}

static ROUTE_CAPABILITY_SCORE_KIND: FieldKind = FieldKind::Uint;
static ROUTE_CAPABILITY_INDEX_FIELDS: [&str; 1] = ["rank"];
static ROUTE_CAPABILITY_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
    "rank_idx",
    RouteCapabilityTestStore::PATH,
    &ROUTE_CAPABILITY_INDEX_FIELDS,
    false,
)];

crate::test_entity_schema! {
    ident = RouteCapabilityEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RouteCapabilityEntity",
    entity_tag = crate::testing::ROUTE_MATRIX_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("rank", FieldKind::Uint),
        ("label", FieldKind::Text),
        ("scores", FieldKind::List(&ROUTE_CAPABILITY_SCORE_KIND)),
    ],
    indexes = [&ROUTE_CAPABILITY_INDEX_MODELS[0]],
    store = RouteCapabilityTestStore,
    canister = RouteCapabilityTestCanister,
}

///
/// UniqueRouteCapabilityEntity
///
/// Route-owner fixture for unique secondary-prefix load planning. This keeps
/// the unique-index budget/hint checks local to the route module.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct UniqueRouteCapabilityEntity {
    id: Ulid,
    code: u32,
}

static UNIQUE_ROUTE_CAPABILITY_INDEX_FIELDS: [&str; 1] = ["code"];
static UNIQUE_ROUTE_CAPABILITY_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
    "code_idx",
    RouteCapabilityTestStore::PATH,
    &UNIQUE_ROUTE_CAPABILITY_INDEX_FIELDS,
    true,
)];

thread_local! {
    static ROUTE_AUTHORITY_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(180)));
    static ROUTE_AUTHORITY_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(181)));
}

crate::test_entity_schema! {
    ident = UniqueRouteCapabilityEntity,
    id = Ulid,
    id_field = id,
    entity_name = "UniqueRouteCapabilityEntity",
    entity_tag = crate::testing::ROUTE_MATRIX_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("code", FieldKind::Uint),
    ],
    indexes = [&UNIQUE_ROUTE_CAPABILITY_INDEX_MODELS[0]],
    store = RouteCapabilityTestStore,
    canister = RouteCapabilityTestCanister,
}

fn initial_scalar_continuation_context() -> ScalarContinuationContext {
    ScalarContinuationContext::initial()
}

fn build_load_route_plan_for_model(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    probe_fetch_hint: Option<usize>,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    build_execution_route_plan_for_load_with_model(model, plan, continuation, probe_fetch_hint)
}

fn build_load_route_plan(
    plan: &AccessPlannedQuery,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    build_load_route_plan_for_model(
        RouteCapabilityEntity::MODEL,
        plan,
        &initial_scalar_continuation_context(),
        None,
    )
}

fn build_load_route_plan_with_continuation(
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    build_load_route_plan_for_model(RouteCapabilityEntity::MODEL, plan, continuation, None)
}

fn build_load_route_plan_with_probe_hint(
    plan: &AccessPlannedQuery,
    probe_fetch_hint: Option<usize>,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    build_load_route_plan_for_model(
        RouteCapabilityEntity::MODEL,
        plan,
        &initial_scalar_continuation_context(),
        probe_fetch_hint,
    )
}

fn build_unique_load_route_plan(
    plan: &AccessPlannedQuery,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    build_load_route_plan_for_model(
        UniqueRouteCapabilityEntity::MODEL,
        plan,
        &initial_scalar_continuation_context(),
        None,
    )
}

fn build_mutation_route_plan(
    plan: &AccessPlannedQuery,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    build_execution_route_plan_for_mutation_with_model(RouteCapabilityEntity::MODEL, plan)
}

fn build_aggregate_route(plan: &AccessPlannedQuery, kind: AggregateKind) -> ExecutionPlan {
    let aggregate_expr = match kind {
        AggregateKind::Count => aggregate::count(),
        AggregateKind::Exists => aggregate::exists(),
        AggregateKind::Min => aggregate::min(),
        AggregateKind::Max => aggregate::max(),
        AggregateKind::First => aggregate::first(),
        AggregateKind::Last => aggregate::last(),
        _ => panic!("unsupported terminal aggregate kind for route test helper: {kind:?}"),
    };

    build_aggregate_spec_route(plan, aggregate_expr)
}

fn build_aggregate_spec_route(
    plan: &AccessPlannedQuery,
    aggregate_expr: crate::db::query::builder::AggregateExpr,
) -> ExecutionPlan {
    let execution_preparation = ExecutionPreparation::from_plan(
        RouteCapabilityEntity::MODEL,
        plan,
        slot_map_for_model_plan(RouteCapabilityEntity::MODEL, plan),
    );

    build_execution_route_plan_for_aggregate_spec_with_model(
        RouteCapabilityEntity::MODEL,
        plan,
        aggregate_expr,
        &execution_preparation,
    )
}

// Build one direct store handle for route-level authority promotion tests.
fn route_authority_store_handle() -> StoreHandle {
    StoreHandle::new(&ROUTE_AUTHORITY_DATA_STORE, &ROUTE_AUTHORITY_INDEX_STORE)
}

// Reset the route-level authority fixture so covering promotion tests start
// from one empty `Valid` index with no synchronized authority bits restored.
fn reset_route_authority_store() {
    ROUTE_AUTHORITY_DATA_STORE.with(|store| store.borrow_mut().clear());
    ROUTE_AUTHORITY_INDEX_STORE.with(|store| {
        let mut store = store.borrow_mut();
        store.clear();
        store.mark_valid();
    });
}

// Build one narrow order-only covering plan used to prove that route-level
// witness promotion now depends on index validity in addition to the older
// synchronized authority bits.
fn secondary_order_covering_plan() -> AccessPlannedQuery {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.projection_selection =
        ProjectionSelection::Fields(vec![FieldId::new("id"), FieldId::new("rank")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    plan
}

fn field_extrema_index_range_plan(
    direction: OrderDirection,
    offset: u32,
    distinct: bool,
) -> AccessPlannedQuery {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().distinct = distinct;
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), direction),
            ("id".to_string(), direction),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: None,
        offset,
    });

    plan
}

fn grouped_field_slots(fields: &[&str]) -> Vec<FieldSlot> {
    fields
        .iter()
        .map(|field| {
            FieldSlot::resolve(RouteCapabilityEntity::MODEL, field)
                .unwrap_or_else(|| panic!("group field should resolve: {field}"))
        })
        .collect()
}

fn grouped_field_slot(field: &str) -> FieldSlot {
    FieldSlot::resolve(RouteCapabilityEntity::MODEL, field)
        .unwrap_or_else(|| panic!("group field should resolve: {field}"))
}

fn build_grouped_route_plan(plan: &AccessPlannedQuery) -> ExecutionPlan {
    let grouped_handoff =
        grouped_executor_handoff(plan).expect("grouped logical plans should build handoff");

    build_execution_route_plan_for_grouped_plan(
        RouteCapabilityEntity::MODEL,
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy_hint(),
    )
}

fn scalar_aggregate_route_snapshot(
    plan: &AccessPlannedQuery,
    aggregate_expr: crate::db::query::builder::AggregateExpr,
) -> String {
    let route_plan = build_aggregate_spec_route(plan, aggregate_expr.clone());

    [
        format!("aggregate_kind={:?}", aggregate_expr.kind()),
        "grouped=false".to_string(),
        format!("distinct_mode={}", aggregate_expr.is_distinct()),
        format!("target_field={:?}", aggregate_expr.target_field()),
        format!("route_strategy={:?}", route_plan.execution_mode_case),
        format!("execution_mode={:?}", route_plan.execution_mode),
        format!("fold_mode={:?}", route_plan.aggregate_fold_mode),
    ]
    .join("\n")
}

fn grouped_aggregate_route_snapshot(plan: &AccessPlannedQuery) -> String {
    let planner_hint =
        grouped_plan_strategy_hint(plan).expect("grouped route snapshot requires grouped hint");
    let handoff = grouped_executor_handoff(plan).expect("grouped route snapshot requires handoff");
    let aggregate_contracts = handoff
        .aggregate_exprs()
        .iter()
        .map(|aggregate_expr| {
            format!(
                "{:?}:{:?}:{}",
                aggregate_expr.kind(),
                aggregate_expr.target_field(),
                aggregate_expr.is_distinct()
            )
        })
        .collect::<Vec<_>>();
    let route_plan = build_execution_route_plan_for_grouped_plan(
        RouteCapabilityEntity::MODEL,
        handoff.base(),
        handoff.grouped_plan_strategy_hint(),
    );
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route snapshot requires grouped observability payload");

    [
        "grouped=true".to_string(),
        format!("planner_hint={planner_hint:?}"),
        format!("aggregate_contracts={aggregate_contracts:?}"),
        format!("route_strategy={:?}", route_plan.execution_mode_case),
        format!("execution_mode={:?}", route_plan.execution_mode),
        format!(
            "grouped_execution_strategy={:?}",
            grouped_observability.grouped_execution_strategy()
        ),
        format!("fold_mode={:?}", route_plan.aggregate_fold_mode),
    ]
    .join("\n")
}

fn grouped_policy_snapshot(
    plan: &AccessPlannedQuery,
) -> (
    GroupedPlanStrategyHint,
    Option<crate::db::query::plan::GroupDistinctPolicyReason>,
    GroupedExecutionStrategy,
    bool,
) {
    let planner_hint =
        grouped_plan_strategy_hint(plan).expect("grouped plans should project planner hints");
    let handoff = grouped_executor_handoff(plan).expect("grouped plans should project handoff");
    let distinct_violation = handoff.distinct_policy_violation_for_executor();
    let route_plan = build_execution_route_plan_for_grouped_plan(
        RouteCapabilityEntity::MODEL,
        handoff.base(),
        handoff.grouped_plan_strategy_hint(),
    );
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped plans should always project grouped route observability");

    (
        planner_hint,
        distinct_violation,
        grouped_observability.grouped_execution_strategy(),
        grouped_observability.eligible(),
    )
}

#[test]
fn route_feature_budget_capability_flags_stay_within_soft_delta() {
    let capability_flags = route_capability_flag_count_guard();
    assert!(
        capability_flags <= ROUTE_CAPABILITY_FLAG_BASELINE_0247 + ROUTE_FEATURE_SOFT_BUDGET_DELTA,
        "route capability flags exceeded soft feature budget; consolidate before adding more flags"
    );
}

#[test]
fn route_feature_budget_execution_mode_cases_stay_within_soft_delta() {
    let execution_mode_cases = route_execution_mode_case_count_guard();
    assert!(
        execution_mode_cases
            <= ROUTE_EXECUTION_MODE_CASE_BASELINE_0246 + ROUTE_FEATURE_SOFT_BUDGET_DELTA,
        "route execution-mode branching exceeded soft feature budget; consolidate before adding more cases"
    );
}

#[test]
fn route_feature_budget_shape_kinds_stay_within_soft_delta() {
    let route_shape_kinds = route_shape_kind_count_guard();
    assert!(
        route_shape_kinds <= ROUTE_SHAPE_KIND_BASELINE_0256 + ROUTE_FEATURE_SOFT_BUDGET_DELTA,
        "route shape-kind partitioning exceeded soft feature budget; consolidate before adding more shape variants",
    );
}

#[test]
fn route_grouped_runtime_revalidation_flags_match_baseline() {
    let flags = grouped_ordered_runtime_revalidation_flag_count_guard();
    assert_eq!(
        flags, ROUTE_GROUPED_RUNTIME_REVALIDATION_FLAG_BASELINE_0251,
        "grouped ordered-route runtime revalidation flags changed; keep grouped semantics planner-owned and runtime revalidation capability-focused",
    );
}

#[test]
fn load_fast_path_order_matches_expected_precedence() {
    assert_eq!(
        LOAD_FAST_PATH_ORDER,
        [
            FastPathOrder::PrimaryKey,
            FastPathOrder::SecondaryPrefix,
            FastPathOrder::IndexRange,
        ],
        "load fast-path precedence must stay stable"
    );
}

#[test]
fn aggregate_fast_path_order_matches_expected_precedence() {
    assert_eq!(
        AGGREGATE_FAST_PATH_ORDER,
        [
            FastPathOrder::PrimaryKey,
            FastPathOrder::SecondaryPrefix,
            FastPathOrder::PrimaryScan,
            FastPathOrder::IndexRange,
            FastPathOrder::Composite,
        ],
        "aggregate fast-path precedence must stay stable"
    );
}

#[test]
fn aggregate_fast_path_order_starts_with_load_contract_prefix() {
    assert!(
        AGGREGATE_FAST_PATH_ORDER
            .starts_with(&[FastPathOrder::PrimaryKey, FastPathOrder::SecondaryPrefix]),
        "aggregate precedence must preserve load-first prefix to avoid subtle route drift"
    );
}

#[test]
fn route_capabilities_full_scan_desc_pk_order_reflect_expected_flags() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 2,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        route_plan
            .load_order_route_contract()
            .allows_streaming_load()
    );
    assert!(route_plan.desc_physical_reverse_supported());
    assert!(route_plan.capabilities.count_pushdown_shape_supported);
    assert!(
        !route_plan
            .capabilities
            .count_pushdown_existing_rows_shape_supported
    );
    assert!(
        !route_plan
            .capabilities
            .index_range_limit_pushdown_shape_supported
    );
    assert!(!route_plan.composite_aggregate_fast_path_eligible());
    assert!(route_plan.capabilities.bounded_probe_hint_safe);
    assert!(!route_plan.field_min_fast_path_eligible());
    assert!(!route_plan.field_max_fast_path_eligible());
}

#[test]
fn route_capabilities_by_keys_desc_distinct_offset_disable_probe_hint() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(7303)),
            Value::Ulid(Ulid::from_u128(7301)),
            Value::Ulid(Ulid::from_u128(7302)),
        ]),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.scalar_plan_mut().distinct = true;
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        route_plan
            .load_order_route_contract()
            .allows_streaming_load()
    );
    assert!(!route_plan.desc_physical_reverse_supported());
    assert!(!route_plan.capabilities.count_pushdown_shape_supported);
    assert!(
        !route_plan
            .capabilities
            .count_pushdown_existing_rows_shape_supported
    );
    assert!(
        !route_plan
            .capabilities
            .index_range_limit_pushdown_shape_supported
    );
    assert!(!route_plan.composite_aggregate_fast_path_eligible());
    assert!(!route_plan.capabilities.bounded_probe_hint_safe);
    assert!(!route_plan.field_min_fast_path_eligible());
    assert!(!route_plan.field_max_fast_path_eligible());
}

#[test]
fn route_capabilities_index_range_order_compatible_shape_is_streaming_safe() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        route_plan
            .load_order_route_contract()
            .allows_streaming_load()
    );
    assert!(
        route_plan
            .capabilities
            .count_pushdown_existing_rows_shape_supported
    );
    assert!(
        route_plan
            .capabilities
            .index_range_limit_pushdown_shape_supported
    );
}

#[test]
fn route_capabilities_index_range_without_order_remains_limit_pushdown_eligible() {
    let plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        route_plan
            .capabilities
            .index_range_limit_pushdown_shape_supported,
        "no-order index-range shapes remain eligible for limit pushdown",
    );
}

#[test]
fn route_capabilities_index_range_with_empty_order_rejects_limit_pushdown_shape() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec { fields: Vec::new() });

    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        !route_plan
            .capabilities
            .index_range_limit_pushdown_shape_supported,
        "empty-order planner-bypass shapes must not be treated as limit-pushdown eligible",
    );
}

#[test]
fn route_capabilities_non_unique_index_prefix_order_requires_post_access_sort() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });

    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        !route_plan
            .load_order_route_contract()
            .allows_streaming_load(),
        "non-unique index-prefix ordering must preserve post-access sorting",
    );
}

#[test]
fn route_capabilities_bound_non_unique_index_prefix_order_is_streaming_safe() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![Value::Uint(10)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        route_plan
            .load_order_route_contract()
            .allows_streaming_load(),
        "bound non-unique index-prefix ordering should stream when the equality prefix collapses traversal to one suffix window",
    );
}

#[test]
fn route_plan_load_uses_route_owned_fast_path_order() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert_eq!(route_plan.fast_path_order(), &LOAD_FAST_PATH_ORDER);
    assert_eq!(route_plan.direction(), Direction::Asc);
    assert_eq!(
        route_plan.continuation().capabilities().mode(),
        ContinuationMode::Initial
    );
}

#[test]
fn route_plan_shape_descriptor_matches_route_axes() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    let shape = route_plan.shape();
    assert_eq!(shape.execution_mode(), RouteExecutionMode::Streaming);
    assert!(shape.is_streaming());
}

#[test]
fn route_plan_load_terminal_covering_read_contract_requires_coverable_projection() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("rank")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let contract = derive_load_terminal_fast_path_contract_for_model(
        RouteCapabilityEntity::MODEL,
        &projected,
        true,
    )
    .expect("direct projected indexed field should derive one covering-read route contract");

    let LoadTerminalFastPathContract::CoveringRead(covering) = contract;
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "rank");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::Constant(Value::Uint(7)),
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::RequiresRowPresenceCheck,
    );

    let materialized = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    assert!(
        derive_load_terminal_fast_path_contract_for_model(
            RouteCapabilityEntity::MODEL,
            &materialized,
            true,
        )
        .is_none(),
        "all-field entity projection should stay on the materialized load route",
    );
}

#[test]
fn route_plan_execution_route_plan_retains_covering_read_contract() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("rank")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let route_plan = build_execution_route_plan_for_load_with_model(
        RouteCapabilityEntity::MODEL,
        &projected,
        &ScalarContinuationContext::initial(),
        None,
    )
    .expect("execution route plan should build for coverable projected load");
    let covering = route_plan
        .load_terminal_fast_path()
        .expect("execution route plan should retain the route-owned covering-read contract");
    let LoadTerminalFastPathContract::CoveringRead(covering) = covering;

    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "rank");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::Constant(Value::Uint(7)),
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::RequiresRowPresenceCheck,
    );
}

#[test]
fn route_plan_store_witness_requires_valid_index_for_witness_validated_promotion() {
    reset_route_authority_store();
    let plan = secondary_order_covering_plan();
    let store = route_authority_store_handle();

    store.mark_secondary_covering_authoritative();
    let promoted_route = build_initial_execution_route_plan_for_load_with_model_store_witness(
        RouteCapabilityEntity::MODEL,
        &plan,
        None,
        store,
    )
    .expect("witness-valid route plan should build");
    let covering = promoted_route
        .load_terminal_fast_path()
        .expect("witness-valid route should retain a covering-read contract");
    let LoadTerminalFastPathContract::CoveringRead(covering) = covering;
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::WitnessValidated,
        "Valid indexes may promote one eligible covering cohort onto witness-backed authority",
    );

    store.mark_index_building();
    let invalid_route = build_initial_execution_route_plan_for_load_with_model_store_witness(
        RouteCapabilityEntity::MODEL,
        &plan,
        None,
        store,
    )
    .expect("invalid-index route plan should still build");
    let covering = invalid_route
        .load_terminal_fast_path()
        .expect("invalid-index route should retain a covering-read contract");
    let LoadTerminalFastPathContract::CoveringRead(covering) = covering;
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::RequiresRowPresenceCheck,
        "Building indexes must fail closed back to row_check_required even for one otherwise eligible witness-backed covering cohort",
    );
}

#[test]
fn route_plan_store_witness_requires_valid_index_for_storage_existence_witness_promotion() {
    reset_route_authority_store();
    let plan = secondary_order_covering_plan();
    let store = route_authority_store_handle();

    store.mark_secondary_existence_witness_authoritative();
    let promoted_route = build_initial_execution_route_plan_for_load_with_model_store_witness(
        RouteCapabilityEntity::MODEL,
        &plan,
        None,
        store,
    )
    .expect("storage-witness-valid route plan should build");
    let covering = promoted_route
        .load_terminal_fast_path()
        .expect("storage-witness-valid route should retain a covering-read contract");
    let LoadTerminalFastPathContract::CoveringRead(covering) = covering;
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::StorageExistenceWitness,
        "Valid indexes may promote one eligible stale covering cohort onto the storage existence witness route",
    );

    store.with_index_mut(IndexStore::mark_dropping);
    let invalid_route = build_initial_execution_route_plan_for_load_with_model_store_witness(
        RouteCapabilityEntity::MODEL,
        &plan,
        None,
        store,
    )
    .expect("dropping-index route plan should still build");
    let covering = invalid_route
        .load_terminal_fast_path()
        .expect("dropping-index route should retain a covering-read contract");
    let LoadTerminalFastPathContract::CoveringRead(covering) = covering;
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::RequiresRowPresenceCheck,
        "Dropping indexes must fail closed back to row_check_required even when the explicit stale storage witness was previously authoritative",
    );
}

#[test]
fn route_plan_load_terminal_covering_read_contract_marks_pk_only_full_scan_as_planner_proven() {
    let mut projected =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let contract = derive_load_terminal_fast_path_contract_for_model(
        RouteCapabilityEntity::MODEL,
        &projected,
        true,
    )
    .expect("PK-only full scan should derive one planner-proven covering-read route contract");

    let LoadTerminalFastPathContract::CoveringRead(covering) = contract;
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner,
    );
}

#[test]
fn route_plan_load_terminal_covering_read_contract_marks_pk_only_key_range_as_planner_proven() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::<Value>::KeyRange {
            start: Value::Ulid(Ulid::from_u128(9_511)),
            end: Value::Ulid(Ulid::from_u128(9_512)),
        },
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let contract = derive_load_terminal_fast_path_contract_for_model(
        RouteCapabilityEntity::MODEL,
        &projected,
        true,
    )
    .expect("PK-only key range should derive one planner-proven covering-read route contract");

    let LoadTerminalFastPathContract::CoveringRead(covering) = contract;
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner,
    );
}

#[test]
fn route_plan_execution_route_plan_retains_pk_only_planner_proven_covering_contract() {
    let mut projected =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let route_plan = build_execution_route_plan_for_load_with_model(
        RouteCapabilityEntity::MODEL,
        &projected,
        &ScalarContinuationContext::initial(),
        None,
    )
    .expect("execution route plan should build for PK-only planner-proven covering load");
    let covering = route_plan
        .load_terminal_fast_path()
        .expect("execution route plan should retain the planner-proven covering-read contract");
    let LoadTerminalFastPathContract::CoveringRead(covering) = covering;

    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner,
    );
}

#[test]
fn route_plan_execution_route_plan_retains_pk_only_key_range_covering_contract() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::<Value>::KeyRange {
            start: Value::Ulid(Ulid::from_u128(9_511)),
            end: Value::Ulid(Ulid::from_u128(9_512)),
        },
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let route_plan = build_execution_route_plan_for_load_with_model(
        RouteCapabilityEntity::MODEL,
        &projected,
        &ScalarContinuationContext::initial(),
        None,
    )
    .expect("execution route plan should build for PK-only planner-proven covering key range");
    let covering = route_plan
        .load_terminal_fast_path()
        .expect("execution route plan should retain the planner-proven covering-read contract");
    let LoadTerminalFastPathContract::CoveringRead(covering) = covering;

    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner,
    );
}

#[test]
fn route_plan_load_terminal_covering_read_contract_marks_pk_only_by_key_as_row_check_required() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::<Value>::ByKey(Value::Ulid(Ulid::from_u128(9_511))),
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let contract = derive_load_terminal_fast_path_contract_for_model(
        RouteCapabilityEntity::MODEL,
        &projected,
        true,
    )
    .expect("PK-only by-key lookup should derive one row-check covering-read route contract");

    let LoadTerminalFastPathContract::CoveringRead(covering) = contract;
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::RequiresRowPresenceCheck,
    );
}

#[test]
fn route_plan_load_terminal_covering_read_contract_marks_pk_only_by_keys_as_row_check_required() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::<Value>::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(9_511)),
            Value::Ulid(Ulid::from_u128(9_513)),
        ]),
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let contract = derive_load_terminal_fast_path_contract_for_model(
        RouteCapabilityEntity::MODEL,
        &projected,
        true,
    )
    .expect("PK-only by-keys lookup should derive one row-check covering-read route contract");

    let LoadTerminalFastPathContract::CoveringRead(covering) = contract;
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::RequiresRowPresenceCheck,
    );
}

#[test]
fn route_plan_load_terminal_covering_read_contract_rejects_pk_only_by_keys_desc_for_now() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::<Value>::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(9_511)),
            Value::Ulid(Ulid::from_u128(9_513)),
        ]),
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });

    assert!(
        derive_load_terminal_fast_path_contract_for_model(
            RouteCapabilityEntity::MODEL,
            &projected,
            true,
        )
        .is_none(),
        "phase-1 multi-key PK covering should stay fail-closed on descending order until exact-key reorder is explicit",
    );
}

#[test]
fn runtime_route_consumers_avoid_direct_execution_mode_field_reads() {
    let runtime_consumers = [
        "src/db/executor/pipeline/runtime/mod.rs",
        "src/db/executor/pipeline/runtime/fast_path/mod.rs",
        "src/db/executor/pipeline/runtime/fast_path/strategy.rs",
        "src/db/executor/pipeline/entrypoints/scalar/mod.rs",
        "src/db/executor/pipeline/entrypoints/scalar/surface.rs",
        "src/db/executor/aggregate/mod.rs",
        "src/db/executor/explain/descriptor/mod.rs",
    ];

    for relative_path in runtime_consumers {
        let absolute_path = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), relative_path);
        let source = fs::read_to_string(&absolute_path)
            .unwrap_or_else(|err| panic!("failed to read {absolute_path}: {err}"));
        assert!(
            !source.contains("route_plan.execution_mode"),
            "runtime route consumer should use ExecutionRouteShape accessors instead of direct execution_mode field reads: {relative_path}",
        );
    }
}

#[test]
fn route_matrix_load_pk_desc_with_page_uses_streaming_budget_and_reverse() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 2,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Streaming
    );
    assert_eq!(route_plan.direction(), Direction::Desc);
    assert_eq!(
        route_plan.continuation().capabilities().mode(),
        ContinuationMode::Initial
    );
    assert_eq!(route_plan.continuation().effective_offset(), 2);
    assert!(route_plan.desc_physical_reverse_supported());
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, Some(6));
    assert_eq!(
        route_plan.top_n_seek_spec().map(TopNSeekSpec::fetch),
        Some(6)
    );
    assert!(route_plan.index_range_limit_spec.is_none());
}

#[test]
fn route_matrix_load_index_range_cursor_without_anchor_disables_pushdown() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(20)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let continuation =
        ScalarContinuationContext::new(Some(CursorBoundary { slots: Vec::new() }).into());
    let route_plan = build_load_route_plan_with_continuation(&plan, &continuation)
        .expect("load route plan should build");

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Streaming
    );
    assert_eq!(
        route_plan.continuation().capabilities().mode(),
        ContinuationMode::CursorBoundary
    );
    assert_eq!(route_plan.continuation().effective_offset(), 0);
    assert!(route_plan.desc_physical_reverse_supported());
    assert!(route_plan.index_range_limit_spec.is_none());
    assert!(route_plan.top_n_seek_spec().is_none());
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
}

#[test]
fn route_matrix_load_index_range_residual_predicate_allows_small_window_pushdown() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(20)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::eq(
        "label".to_string(),
        Value::Text("keep".to_string()),
    ));
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert_eq!(
        route_plan.index_range_limit_spec.map(|spec| spec.fetch),
        Some(3),
        "small residual-filter windows should retain index-range limit pushdown",
    );
}

#[test]
fn route_matrix_load_index_range_residual_predicate_large_window_disables_pushdown() {
    let limit = 256_u32;
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(20)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::eq(
        "label".to_string(),
        Value::Text("keep".to_string()),
    ));
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(limit),
        offset: 0,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        route_plan.index_range_limit_spec.is_none(),
        "residual-filter windows above the fetch cap must disable index-range limit pushdown",
    );
}

#[test]
fn route_matrix_load_index_range_incompatible_order_disables_limit_pushdown() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(20)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("label".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        !route_plan
            .capabilities
            .index_range_limit_pushdown_shape_supported,
        "index-range LIMIT pushdown shape must be rejected when ORDER BY is not planner-compatible",
    );
    assert!(
        route_plan.index_range_limit_spec.is_none(),
        "incompatible ordered index-range shapes must not derive index-range limit pushdown specs",
    );
    assert!(
        route_plan.top_n_seek_spec().is_none(),
        "incompatible ordered shapes must not derive Top-N seek hints",
    );
}

#[test]
fn route_matrix_load_index_range_missing_pk_tie_break_disables_limit_pushdown() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(20)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("rank".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        !route_plan
            .capabilities
            .index_range_limit_pushdown_shape_supported,
        "index-range LIMIT pushdown shape must be rejected when ORDER BY omits PK tie-break",
    );
    assert!(
        route_plan.index_range_limit_spec.is_none(),
        "missing PK tie-break must disable index-range limit pushdown specs",
    );
    assert!(
        route_plan.top_n_seek_spec().is_none(),
        "missing PK tie-break must disable Top-N seek hints",
    );
}

#[test]
fn route_matrix_load_index_range_mixed_direction_disables_limit_pushdown() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(20)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        !route_plan
            .capabilities
            .index_range_limit_pushdown_shape_supported,
        "index-range LIMIT pushdown shape must be rejected for mixed ORDER BY directions",
    );
    assert!(
        route_plan.index_range_limit_spec.is_none(),
        "mixed ORDER BY directions must disable index-range limit pushdown specs",
    );
    assert!(
        route_plan.top_n_seek_spec().is_none(),
        "mixed ORDER BY directions must disable Top-N seek hints",
    );
}

#[test]
fn route_matrix_load_non_pk_order_disables_scan_budget_hint() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("rank".to_string(), OrderDirection::Desc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 2,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
}

#[test]
fn route_matrix_load_unique_secondary_order_limit_one_uses_bounded_scan_budget_hint() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: UNIQUE_ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("code".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(1),
        offset: 0,
    });
    let route_plan = build_unique_load_route_plan(&plan)
        .expect("secondary-order limit-one route plan should build");

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Streaming
    );
    assert_eq!(route_plan.direction(), Direction::Desc);
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(
        route_plan.scan_hints.load_scan_budget_hint,
        Some(2),
        "secondary ORDER BY DESC LIMIT 1 should bound access scanning to keep+continuation fetch",
    );
    assert_eq!(
        route_plan.top_n_seek_spec().map(TopNSeekSpec::fetch),
        Some(2)
    );
}

#[test]
fn route_matrix_load_non_unique_secondary_order_desc_limit_one_fails_closed_before_top_n() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(1),
        offset: 0,
    });

    let route_plan = build_load_route_plan(&plan)
        .expect("non-unique descending secondary order route plan should build");

    assert_eq!(
        route_plan.load_order_route_contract(),
        LoadOrderRouteContract::MaterializedFallback,
        "non-unique descending secondary order must fail closed to the fallback materialized contract",
    );
    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Materialized,
        "non-unique descending secondary order must fail closed to materialized execution",
    );
    assert_eq!(
        route_plan.top_n_seek_spec(),
        None,
        "non-unique descending secondary order must not derive top-n seek",
    );
    assert_eq!(
        route_plan.scan_hints.load_scan_budget_hint, None,
        "non-unique descending secondary order must not derive bounded scan-budget hints",
    );
}

#[test]
fn route_matrix_load_non_unique_secondary_order_desc_offset_fails_closed_before_top_n() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(1),
        offset: 1,
    });

    let route_plan = build_load_route_plan(&plan)
        .expect("offset-sensitive descending secondary order route plan should build");

    assert_eq!(
        route_plan.load_order_route_contract(),
        LoadOrderRouteContract::MaterializedFallback,
        "offset-sensitive descending secondary order must fail closed to the fallback materialized contract",
    );
    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Materialized,
        "offset-sensitive descending secondary order must fail closed to materialized execution",
    );
    assert_eq!(
        route_plan.top_n_seek_spec(),
        None,
        "offset-sensitive descending secondary order must not derive top-n seek",
    );
}

#[test]
fn route_matrix_load_unique_secondary_order_desc_offset_stays_on_materialized_boundary() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: UNIQUE_ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("code".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(1),
        offset: 1,
    });

    let route_plan = build_unique_load_route_plan(&plan)
        .expect("offset-sensitive unique descending secondary order route plan should build");

    assert_eq!(
        route_plan.load_order_route_contract(),
        LoadOrderRouteContract::MaterializedBoundary,
        "offset-sensitive unique descending secondary order should keep the ordered materialized boundary contract",
    );
    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Materialized,
        "offset-sensitive unique descending secondary order must stay materialized",
    );
    assert_eq!(
        route_plan.top_n_seek_spec(),
        None,
        "offset-sensitive unique descending secondary order must not derive top-n seek",
    );
}

#[test]
fn route_matrix_load_non_unique_secondary_order_desc_distinct_fails_closed_before_top_n() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().distinct = true;
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(1),
        offset: 0,
    });

    let route_plan = build_load_route_plan(&plan)
        .expect("distinct descending secondary order route plan should build");

    assert_eq!(
        route_plan.load_order_route_contract(),
        LoadOrderRouteContract::MaterializedFallback,
        "distinct descending secondary order must fail closed to the fallback materialized contract",
    );
    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Materialized,
        "distinct descending secondary order must fail closed to materialized execution",
    );
    assert_eq!(
        route_plan.top_n_seek_spec(),
        None,
        "distinct descending secondary order must not derive top-n seek",
    );
}

#[test]
fn route_matrix_load_secondary_order_with_residual_filter_fails_closed_before_top_n() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::eq(
        "label".to_string(),
        Value::Text("keep".to_string()),
    ));
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(1),
        offset: 0,
    });

    let route_plan = build_load_route_plan(&plan)
        .expect("residual descending secondary order route should build");

    assert_eq!(
        route_plan.load_order_route_contract(),
        LoadOrderRouteContract::MaterializedFallback,
        "residual descending secondary order must fail closed to the fallback materialized contract",
    );
    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Materialized,
        "residual descending secondary order must fail closed to materialized execution",
    );
    assert_eq!(
        route_plan.top_n_seek_spec(),
        None,
        "residual descending secondary order must not derive top-n seek",
    );
}

#[test]
fn route_matrix_load_by_keys_desc_disables_fallback_fetch_hint_without_reverse_support() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(7203)),
            Value::Ulid(Ulid::from_u128(7201)),
            Value::Ulid(Ulid::from_u128(7202)),
        ]),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    let route_plan = build_load_route_plan_with_probe_hint(&plan, Some(4))
        .expect("load route plan should build");

    assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(4));
    assert_eq!(
        route_plan.fallback_physical_fetch_hint(Direction::Desc),
        None
    );
    assert_eq!(
        route_plan.fallback_physical_fetch_hint(Direction::Asc),
        Some(4)
    );
}

#[test]
fn route_matrix_load_desc_reverse_support_gate_allows_and_blocks_fetch_hint() {
    let mut reverse_capable =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    reverse_capable.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    let reverse_capable_route = build_load_route_plan_with_probe_hint(&reverse_capable, Some(5))
        .expect("reverse-capable load route should build");
    assert!(reverse_capable_route.desc_physical_reverse_supported());
    assert_eq!(
        reverse_capable_route.scan_hints.physical_fetch_hint,
        Some(5)
    );
    assert_eq!(
        reverse_capable_route.fallback_physical_fetch_hint(Direction::Desc),
        Some(5)
    );

    let mut reverse_blocked = AccessPlannedQuery::new(
        AccessPath::<Value>::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(7_203)),
            Value::Ulid(Ulid::from_u128(7_201)),
            Value::Ulid(Ulid::from_u128(7_202)),
        ]),
        MissingRowPolicy::Ignore,
    );
    reverse_blocked.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    let reverse_blocked_route = build_load_route_plan_with_probe_hint(&reverse_blocked, Some(5))
        .expect("reverse-blocked load route should build");
    assert!(!reverse_blocked_route.desc_physical_reverse_supported());
    assert_eq!(
        reverse_blocked_route.scan_hints.physical_fetch_hint,
        Some(5)
    );
    assert_eq!(
        reverse_blocked_route.fallback_physical_fetch_hint(Direction::Desc),
        None
    );
}

#[test]
fn route_plan_mutation_is_materialized_with_no_fast_paths_or_hints() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
    let route_plan = build_mutation_route_plan(&plan).expect("mutation route plan should build");

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert!(
        route_plan.fast_path_order().is_empty(),
        "mutation routes must not advertise load or aggregate fast paths",
    );
    assert_eq!(route_plan.direction(), Direction::Asc);
    assert_eq!(
        route_plan.continuation().capabilities().mode(),
        ContinuationMode::Initial
    );
    assert_eq!(route_plan.continuation().effective_offset(), 0);
    assert!(
        route_plan.scan_hints.physical_fetch_hint.is_none(),
        "mutation route should not emit physical fetch hints"
    );
    assert!(
        route_plan.scan_hints.load_scan_budget_hint.is_none(),
        "mutation route should not emit load scan-budget hints"
    );
}

#[test]
fn route_plan_mutation_rejects_non_delete_mode() {
    let plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let result = build_mutation_route_plan(&plan);
    let Err(err) = result else {
        panic!("mutation route must reject non-delete plans")
    };

    assert_eq!(err.class, crate::error::ErrorClass::InvariantViolation);
    assert!(
        err.message
            .contains("mutation route planning requires delete plans"),
        "mutation route rejection should return clear invariant message"
    );
}

#[test]
fn route_plan_aggregate_uses_route_owned_fast_path_order() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let route_plan = build_aggregate_route(&plan, AggregateKind::Exists);

    assert_eq!(route_plan.fast_path_order(), &AGGREGATE_FAST_PATH_ORDER);
    assert_eq!(route_plan.grouped_observability(), None);
}

#[test]
fn route_plan_grouped_wrapper_maps_to_grouped_case_materialized_without_fast_paths() {
    let mut base = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    base.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let grouped = base.into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        route_plan.execution_mode_case,
        ExecutionModeRouteCase::AggregateGrouped
    );
    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Materialized);
    assert_eq!(
        route_plan.continuation().capabilities().mode(),
        ContinuationMode::Initial
    );
    assert_eq!(route_plan.index_range_limit_spec, None);
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
    assert_eq!(route_plan.fast_path_order(), &[]);
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
    assert_eq!(grouped_observability.rejection_reason(), None);
    assert!(grouped_observability.eligible());
    assert_eq!(
        grouped_observability.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(
        grouped_observability.grouped_execution_strategy(),
        GroupedExecutionStrategy::HashMaterialized
    );
}

#[test]
fn route_plan_grouped_wrapper_keeps_blocking_shape_under_tight_budget_config() {
    let mut base = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    base.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let grouped = base.into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::with_hard_limits(1, 1),
    });
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        route_plan.execution_mode_case,
        ExecutionModeRouteCase::AggregateGrouped
    );
    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Materialized);
    assert_eq!(
        route_plan.continuation().capabilities().mode(),
        ContinuationMode::Initial
    );
    assert_eq!(route_plan.index_range_limit_spec, None);
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
    assert_eq!(route_plan.fast_path_order(), &[]);
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
    assert_eq!(grouped_observability.rejection_reason(), None);
    assert!(grouped_observability.eligible());
    assert_eq!(
        grouped_observability.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(
        grouped_observability.grouped_execution_strategy(),
        GroupedExecutionStrategy::HashMaterialized
    );
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_index_prefix_shape() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        grouped_observability.grouped_execution_strategy(),
        GroupedExecutionStrategy::OrderedMaterialized
    );
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
}

#[test]
fn route_plan_grouped_wrapper_downgrades_ordered_strategy_when_residual_predicate_exists() {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    grouped.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(7)));
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        grouped_observability.grouped_execution_strategy(),
        GroupedExecutionStrategy::HashMaterialized
    );
}

#[test]
fn route_plan_grouped_wrapper_downgrades_ordered_strategy_for_unsupported_having_operator() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having(
        GroupSpec {
            group_fields: grouped_field_slots(&["rank"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        },
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(0),
                op: CompareOp::In,
                value: Value::List(vec![Value::Uint(1)]),
            }],
        }),
    );
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");
    let planner_hint =
        grouped_plan_strategy_hint(&grouped).expect("grouped plans should project strategy hints");

    assert_eq!(
        planner_hint,
        GroupedPlanStrategyHint::HashGroup,
        "unsupported grouped HAVING operators should be planner-policy rejected from ordered-group hints",
    );
    assert_eq!(
        grouped_observability.grouped_execution_strategy(),
        GroupedExecutionStrategy::HashMaterialized
    );
}

#[test]
fn route_plan_grouped_wrapper_preserves_kind_matrix_in_query_handoff() {
    let kind_cases = [
        AggregateKind::Count,
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ];
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: grouped_field_slots(&["rank"]),
            aggregates: kind_cases
                .iter()
                .map(|kind| GroupAggregateSpec {
                    kind: *kind,
                    target_field: None,
                    distinct: false,
                })
                .collect(),
            execution: GroupedExecutionConfig::unbounded(),
        });
    let grouped_handoff =
        grouped_executor_handoff(&grouped).expect("grouped logical plans should build handoff");

    assert_eq!(grouped_handoff.group_fields().len(), 1);
    assert_eq!(grouped_handoff.group_fields()[0].field(), "rank");
    assert_eq!(grouped_handoff.aggregate_exprs().len(), kind_cases.len());
    for (index, expected_kind) in kind_cases.iter().enumerate() {
        assert_eq!(
            grouped_handoff.aggregate_exprs()[index].kind(),
            *expected_kind
        );
        assert_eq!(
            grouped_handoff.aggregate_exprs()[index].target_field(),
            None
        );
    }
}

#[test]
fn route_plan_grouped_wrapper_preserves_target_field_in_query_handoff() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: grouped_field_slots(&["rank", "label"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Max,
                target_field: Some("rank".to_string()),
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        });
    let grouped_handoff =
        grouped_executor_handoff(&grouped).expect("grouped logical plans should build handoff");

    assert_eq!(grouped_handoff.group_fields().len(), 2);
    assert_eq!(grouped_handoff.group_fields()[0].field(), "rank");
    assert_eq!(grouped_handoff.group_fields()[1].field(), "label");
    assert_eq!(grouped_handoff.aggregate_exprs().len(), 1);
    assert_eq!(
        grouped_handoff.aggregate_exprs()[0].kind(),
        AggregateKind::Max
    );
    assert_eq!(
        grouped_handoff.aggregate_exprs()[0].target_field(),
        Some("rank")
    );
}

#[test]
fn route_plan_grouped_wrapper_preserves_supported_target_field_matrix_in_query_handoff() {
    let grouped_cases = [
        (AggregateKind::Count, None),
        (AggregateKind::Exists, None),
        (AggregateKind::Min, None),
        (AggregateKind::Min, Some("rank")),
        (AggregateKind::Sum, Some("rank")),
        (AggregateKind::Avg, Some("rank")),
        (AggregateKind::Max, None),
        (AggregateKind::Max, Some("label")),
        (AggregateKind::First, None),
        (AggregateKind::Last, None),
    ];
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: grouped_field_slots(&["rank", "label"]),
            aggregates: grouped_cases
                .iter()
                .map(|(kind, target_field)| GroupAggregateSpec {
                    kind: *kind,
                    target_field: target_field.map(str::to_string),
                    distinct: false,
                })
                .collect(),
            execution: GroupedExecutionConfig::unbounded(),
        });
    let grouped_handoff =
        grouped_executor_handoff(&grouped).expect("grouped logical plans should build handoff");

    assert_eq!(grouped_handoff.group_fields().len(), 2);
    assert_eq!(grouped_handoff.group_fields()[0].field(), "rank");
    assert_eq!(grouped_handoff.group_fields()[1].field(), "label");
    assert_eq!(grouped_handoff.aggregate_exprs().len(), grouped_cases.len());
    for (index, (expected_kind, expected_target)) in grouped_cases.iter().enumerate() {
        let aggregate = &grouped_handoff.aggregate_exprs()[index];
        assert_eq!(aggregate.kind(), *expected_kind);
        assert_eq!(aggregate.target_field(), *expected_target);
    }
}

#[test]
fn route_plan_grouped_wrapper_observability_vector_is_frozen() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: grouped_field_slots(&["rank"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(11, 2048),
        });
    let route_plan = build_grouped_route_plan(&grouped);
    let observability = route_plan
        .grouped_observability()
        .expect("grouped route should always project grouped observability for grouped intents");
    let actual = (
        observability.outcome(),
        observability.rejection_reason(),
        observability.eligible(),
        observability.execution_mode(),
        observability.grouped_execution_strategy(),
    );
    let expected = (
        GroupedRouteDecisionOutcome::MaterializedFallback,
        None,
        true,
        RouteExecutionMode::Materialized,
        GroupedExecutionStrategy::HashMaterialized,
    );

    assert_eq!(actual, expected);
}

#[test]
fn grouped_policy_snapshot_matrix_remains_consistent_across_planner_handoff_and_route() {
    let ordered_grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    assert_eq!(
        grouped_policy_snapshot(&ordered_grouped),
        (
            GroupedPlanStrategyHint::OrderedGroup,
            None,
            GroupedExecutionStrategy::OrderedMaterialized,
            true,
        )
    );

    let having_rejected_grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having(
        GroupSpec {
            group_fields: grouped_field_slots(&["rank"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        },
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(0),
                op: CompareOp::In,
                value: Value::List(vec![Value::Uint(1)]),
            }],
        }),
    );
    assert_eq!(
        grouped_policy_snapshot(&having_rejected_grouped),
        (
            GroupedPlanStrategyHint::HashGroup,
            None,
            GroupedExecutionStrategy::HashMaterialized,
            true,
        )
    );

    let mut scalar_distinct_grouped =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank"]),
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::unbounded(),
            });
    scalar_distinct_grouped.scalar_plan_mut().distinct = true;
    assert_eq!(
        grouped_policy_snapshot(&scalar_distinct_grouped),
        (
            GroupedPlanStrategyHint::HashGroup,
            Some(GroupDistinctPolicyReason::DistinctAdjacencyEligibilityRequired),
            GroupedExecutionStrategy::HashMaterialized,
            true,
        )
    );
}

#[test]
fn grouped_policy_snapshot_global_distinct_field_target_kind_matrix_includes_avg() {
    for kind in [AggregateKind::Count, AggregateKind::Sum, AggregateKind::Avg] {
        let grouped =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
                .into_grouped(GroupSpec {
                    group_fields: Vec::new(),
                    aggregates: vec![GroupAggregateSpec {
                        kind,
                        target_field: Some("rank".to_string()),
                        distinct: true,
                    }],
                    execution: GroupedExecutionConfig::unbounded(),
                });

        assert_eq!(
            grouped_policy_snapshot(&grouped),
            (
                GroupedPlanStrategyHint::HashGroup,
                None,
                GroupedExecutionStrategy::HashMaterialized,
                true,
            ),
            "global DISTINCT grouped strategy snapshot should stay stable for {kind:?}",
        );
    }
}

#[test]
fn route_plan_grouped_explain_projection_and_execution_contract_is_frozen() {
    let group_field = grouped_field_slot("rank");
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having(
        GroupSpec {
            group_fields: vec![group_field.clone()],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(17, 8192),
        },
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(0),
                op: CompareOp::Gt,
                value: Value::Uint(1),
            }],
        }),
    );

    assert_eq!(
        grouped.explain().grouping(),
        &ExplainGrouping::Grouped {
            strategy: ExplainGroupedStrategy::OrderedGroup,
            group_fields: vec![ExplainGroupField {
                slot_index: group_field.index(),
                field: group_field.field().to_string(),
            }],
            aggregates: vec![ExplainGroupAggregate {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            having: Some(ExplainGroupHaving {
                clauses: vec![ExplainGroupHavingClause {
                    symbol: ExplainGroupHavingSymbol::AggregateIndex { index: 0 },
                    op: CompareOp::Gt,
                    value: Value::Uint(1),
                }],
            }),
            max_groups: 17,
            max_group_bytes: 8192,
        },
        "grouped explain projection must preserve strategy, fields, aggregates, having, and hard limits",
    );

    let grouped_handoff =
        grouped_executor_handoff(&grouped).expect("grouped logical plans should build handoff");
    assert_eq!(grouped_handoff.execution().max_groups(), 17);
    assert_eq!(grouped_handoff.execution().max_group_bytes(), 8192);
    let route_plan = build_execution_route_plan_for_grouped_plan(
        RouteCapabilityEntity::MODEL,
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy_hint(),
    );
    assert_eq!(
        route_plan.execution_mode_case,
        ExecutionModeRouteCase::AggregateGrouped
    );
    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Materialized);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should always project grouped observability");
    assert_eq!(
        grouped_observability.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(
        grouped_observability.grouped_execution_strategy(),
        GroupedExecutionStrategy::OrderedMaterialized
    );
}

#[test]
fn grouped_route_strategy_to_metrics_strategy_mapping_is_stable() {
    for (route_strategy, expected_metrics_strategy) in [
        (
            GroupedExecutionStrategy::HashMaterialized,
            GroupedPlanMetricsStrategy::HashMaterialized,
        ),
        (
            GroupedExecutionStrategy::OrderedMaterialized,
            GroupedPlanMetricsStrategy::OrderedMaterialized,
        ),
    ] {
        assert_eq!(
            grouped_plan_metrics_strategy_for_execution_strategy(route_strategy),
            expected_metrics_strategy,
            "grouped route strategy must map to stable grouped metrics strategy labels",
        );
    }
}

#[test]
fn aggregate_route_snapshot_for_scalar_count_is_stable() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let actual = scalar_aggregate_route_snapshot(&plan, crate::db::count());
    let expected = [
        "aggregate_kind=Count".to_string(),
        "grouped=false".to_string(),
        "distinct_mode=false".to_string(),
        "target_field=None".to_string(),
        "route_strategy=AggregateCount".to_string(),
        "execution_mode=Streaming".to_string(),
        "fold_mode=KeysOnly".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "scalar COUNT aggregate route snapshot drifted; route strategy/fold mode are stabilized",
    );
}

#[test]
fn aggregate_route_snapshot_for_scalar_sum_field_is_stable() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let actual = scalar_aggregate_route_snapshot(&plan, crate::db::sum("rank"));
    let expected = [
        "aggregate_kind=Sum".to_string(),
        "grouped=false".to_string(),
        "distinct_mode=false".to_string(),
        "target_field=Some(\"rank\")".to_string(),
        "route_strategy=AggregateNonCount".to_string(),
        "execution_mode=Materialized".to_string(),
        "fold_mode=ExistingRows".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "scalar SUM(field) aggregate route snapshot drifted; route strategy/fold mode are stabilized",
    );
}

#[test]
fn aggregate_route_snapshot_for_scalar_avg_field_is_stable() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let actual = scalar_aggregate_route_snapshot(&plan, crate::db::avg("rank"));
    let expected = [
        "aggregate_kind=Avg".to_string(),
        "grouped=false".to_string(),
        "distinct_mode=false".to_string(),
        "target_field=Some(\"rank\")".to_string(),
        "route_strategy=AggregateNonCount".to_string(),
        "execution_mode=Materialized".to_string(),
        "fold_mode=ExistingRows".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "scalar AVG(field) aggregate route snapshot drifted; route strategy/fold mode are stabilized",
    );
}

#[test]
fn aggregate_route_snapshot_for_grouped_field_aggregates_is_stable() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: grouped_field_slots(&["rank"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Avg,
                target_field: Some("rank".to_string()),
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        });

    let actual = grouped_aggregate_route_snapshot(&grouped);
    let expected = [
        "grouped=true".to_string(),
        "planner_hint=HashGroup".to_string(),
        "aggregate_contracts=[\"Avg:Some(\\\"rank\\\"):false\"]".to_string(),
        "route_strategy=AggregateGrouped".to_string(),
        "execution_mode=Materialized".to_string(),
        "grouped_execution_strategy=HashMaterialized".to_string(),
        "fold_mode=ExistingRows".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "grouped field-aggregate route snapshot drifted; grouped planner/route/executor strategy is stabilized",
    );
}

#[test]
fn aggregate_route_strategy_parity_for_scalar_avg_matches_sum_field() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let sum_route = build_aggregate_spec_route(&plan, crate::db::sum("rank"));
    let avg_route = build_aggregate_spec_route(&plan, crate::db::avg("rank"));

    assert_eq!(avg_route.execution_mode_case, sum_route.execution_mode_case);
    assert_eq!(avg_route.execution_mode, sum_route.execution_mode);
    assert_eq!(avg_route.aggregate_fold_mode, sum_route.aggregate_fold_mode);
}

#[test]
fn route_matrix_aggregate_count_pk_order_is_streaming_keys_only() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(4),
        offset: 2,
    });
    let route_plan = build_aggregate_route(&plan, AggregateKind::Count);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Streaming);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::KeysOnly
    ));
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(6));
}

#[test]
fn route_matrix_aggregate_fold_mode_contract_maps_non_count_to_existing_rows() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    for kind in [
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        let route_plan = build_aggregate_route(&plan, kind);

        assert!(matches!(
            route_plan.aggregate_fold_mode,
            AggregateFoldMode::ExistingRows
        ));
    }
}

#[test]
fn route_matrix_numeric_field_aggregate_fold_mode_contract_maps_sum_avg_to_existing_rows() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    for aggregate_expr in [crate::db::sum("rank"), crate::db::avg("rank")] {
        let route_plan = build_aggregate_spec_route(&plan, aggregate_expr);

        assert!(matches!(
            route_plan.aggregate_fold_mode,
            AggregateFoldMode::ExistingRows
        ));
    }
}

#[test]
fn route_matrix_aggregate_count_secondary_shape_streams_with_existing_rows() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let route_plan = build_aggregate_route(&plan, AggregateKind::Count);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Streaming);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
}

#[test]
fn route_matrix_aggregate_count_secondary_shape_with_strict_predicate_streams() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(7)));
    let route_plan = build_aggregate_route(&plan, AggregateKind::Count);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Streaming);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
}

#[test]
fn route_matrix_aggregate_count_secondary_shape_with_strict_uncertainty_materializes() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(7)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    let route_plan = build_aggregate_route(&plan, AggregateKind::Count);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Materialized);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
}

#[test]
fn route_matrix_aggregate_distinct_offset_last_disables_probe_hint() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.scalar_plan_mut().distinct = true;
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 1,
    });
    let route_plan = build_aggregate_route(&plan, AggregateKind::Last);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Streaming);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
}

#[test]
fn route_matrix_aggregate_distinct_offset_disables_bounded_probe_hints_for_terminals() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().distinct = true;
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 1,
    });

    for kind in [
        AggregateKind::Count,
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        let route_plan = build_aggregate_route(&plan, kind);

        assert_eq!(
            route_plan.scan_hints.physical_fetch_hint, None,
            "DISTINCT+offset must disable bounded aggregate hints for {kind:?}"
        );
        assert_eq!(
            route_plan.aggregate_seek_fetch_hint(),
            None,
            "DISTINCT+offset must disable secondary extrema probe hints for {kind:?}"
        );
    }
}

#[test]
fn route_matrix_aggregate_by_keys_desc_disables_probe_hint_without_reverse_support() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(7103)),
            Value::Ulid(Ulid::from_u128(7101)),
            Value::Ulid(Ulid::from_u128(7102)),
        ]),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = build_aggregate_route(&plan, AggregateKind::First);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Streaming);
    assert!(!route_plan.desc_physical_reverse_supported());
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
}

#[test]
fn route_matrix_aggregate_secondary_extrema_probe_hints_lock_offset_plus_one() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: None,
        offset: 2,
    });

    let min_asc = build_aggregate_route(&plan, AggregateKind::Min);
    let max_asc = build_aggregate_route(&plan, AggregateKind::Max);
    assert_eq!(min_asc.scan_hints.physical_fetch_hint, Some(3));
    assert_eq!(max_asc.scan_hints.physical_fetch_hint, None);
    assert_eq!(min_asc.aggregate_seek_fetch_hint(), Some(3));
    assert_eq!(max_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(
        min_asc.aggregate_seek_spec(),
        Some(super::AggregateSeekSpec::First { fetch: 3 })
    );
    assert_eq!(max_asc.aggregate_seek_spec(), None);

    let first_asc = build_aggregate_route(&plan, AggregateKind::First);
    assert_eq!(
        first_asc.aggregate_seek_fetch_hint(),
        None,
        "secondary extrema probe hints must stay route-owned and Min/Max-only"
    );

    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    let max_desc = build_aggregate_route(&plan, AggregateKind::Max);
    let min_desc = build_aggregate_route(&plan, AggregateKind::Min);
    assert_eq!(max_desc.scan_hints.physical_fetch_hint, Some(3));
    assert_eq!(min_desc.scan_hints.physical_fetch_hint, None);
    assert_eq!(max_desc.aggregate_seek_fetch_hint(), Some(3));
    assert_eq!(min_desc.aggregate_seek_fetch_hint(), None);
    assert_eq!(
        max_desc.aggregate_seek_spec(),
        Some(super::AggregateSeekSpec::Last { fetch: 3 })
    );
    assert_eq!(min_desc.aggregate_seek_spec(), None);
}

#[test]
fn route_matrix_aggregate_index_range_desc_with_window_enables_pushdown_hint() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = build_aggregate_route(&plan, AggregateKind::Last);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Streaming);
    assert!(route_plan.desc_physical_reverse_supported());
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(3));
    assert_eq!(
        route_plan.index_range_limit_spec.map(|spec| spec.fetch),
        Some(3)
    );
}

#[test]
fn route_matrix_aggregate_count_pushdown_boundary_matrix() {
    let assert_count_route = |plan: &AccessPlannedQuery, expected_fold_mode: AggregateFoldMode| {
        let route_plan = build_aggregate_route(plan, AggregateKind::Count);
        assert_eq!(
            route_plan.execution_mode,
            RouteExecutionMode::Streaming,
            "COUNT pushdown matrix should stay on streaming execution mode",
        );
        assert_eq!(
            route_plan.aggregate_fold_mode, expected_fold_mode,
            "COUNT pushdown matrix should preserve fold-mode contract",
        );

        route_plan
    };

    let mut full_scan =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    full_scan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let _full_scan_route = assert_count_route(&full_scan, AggregateFoldMode::KeysOnly);

    let mut key_range = AccessPlannedQuery::new(
        AccessPath::<Value>::KeyRange {
            start: Value::Ulid(Ulid::from_u128(1)),
            end: Value::Ulid(Ulid::from_u128(9)),
        },
        MissingRowPolicy::Ignore,
    );
    key_range.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let _key_range_route = assert_count_route(&key_range, AggregateFoldMode::KeysOnly);

    let mut secondary = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    secondary.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let _secondary_route = assert_count_route(&secondary, AggregateFoldMode::ExistingRows);

    let mut index_range = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    index_range.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    index_range.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let index_range_route = assert_count_route(&index_range, AggregateFoldMode::ExistingRows);
    assert_eq!(
        index_range_route
            .index_range_limit_spec
            .map(|spec| spec.fetch),
        Some(3),
        "index-range COUNT with page window should inherit bounded pushdown fetch",
    );
}

#[test]
fn route_matrix_secondary_extrema_probe_eligibility_is_min_max_only() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: None,
        offset: 2,
    });

    let min_asc = build_aggregate_route(&plan, AggregateKind::Min);
    let max_asc = build_aggregate_route(&plan, AggregateKind::Max);
    let first_asc = build_aggregate_route(&plan, AggregateKind::First);
    let exists_asc = build_aggregate_route(&plan, AggregateKind::Exists);
    let last_asc = build_aggregate_route(&plan, AggregateKind::Last);
    assert_eq!(min_asc.aggregate_seek_fetch_hint(), Some(3));
    assert_eq!(max_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(first_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(exists_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(last_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(
        min_asc.aggregate_seek_spec(),
        Some(super::AggregateSeekSpec::First { fetch: 3 })
    );
    assert_eq!(max_asc.aggregate_seek_spec(), None);
    assert_eq!(first_asc.aggregate_seek_spec(), None);
    assert_eq!(exists_asc.aggregate_seek_spec(), None);
    assert_eq!(last_asc.aggregate_seek_spec(), None);

    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    let min_desc = build_aggregate_route(&plan, AggregateKind::Min);
    let max_desc = build_aggregate_route(&plan, AggregateKind::Max);
    assert_eq!(min_desc.aggregate_seek_fetch_hint(), None);
    assert_eq!(max_desc.aggregate_seek_fetch_hint(), Some(3));
    assert_eq!(min_desc.aggregate_seek_spec(), None);
    assert_eq!(
        max_desc.aggregate_seek_spec(),
        Some(super::AggregateSeekSpec::Last { fetch: 3 })
    );
}

#[test]
fn route_matrix_index_predicate_compile_mode_subset_vs_strict_boundary_is_explicit() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(12)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let execution_preparation = ExecutionPreparation::from_plan(
        RouteCapabilityEntity::MODEL,
        &plan,
        slot_map_for_model_plan(RouteCapabilityEntity::MODEL, &plan),
    );
    let predicate_slots = execution_preparation
        .compiled_predicate()
        .expect("predicate slots should compile for mixed strict/residual predicate");
    let index_slots = execution_preparation
        .slot_map()
        .expect("index-range plan should expose one resolvable index slot");
    let subset_program = compile_index_program(
        predicate_slots.executable(),
        index_slots,
        IndexCompilePolicy::ConservativeSubset,
    );
    let strict_program = compile_index_program(
        predicate_slots.executable(),
        index_slots,
        IndexCompilePolicy::StrictAllOrNone,
    );

    assert!(
        subset_program.is_some(),
        "subset compile mode should keep the strict index-covered rank clause as a safe AND subset",
    );
    assert!(
        strict_program.is_none(),
        "strict compile mode must fail closed when any predicate child is not index-only-safe",
    );
}

#[test]
fn route_matrix_aggregate_strict_compile_uncertainty_forces_materialized_execution_mode() {
    let mut strict_compatible = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    strict_compatible.scalar_plan_mut().predicate =
        Some(Predicate::eq("rank".to_string(), Value::Uint(12)));
    strict_compatible.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    strict_compatible.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let strict_compatible_route = build_aggregate_route(&strict_compatible, AggregateKind::Exists);
    assert_eq!(
        strict_compatible_route.execution_mode,
        RouteExecutionMode::Streaming,
        "strict-compilable secondary predicate shapes should keep aggregate streaming eligibility",
    );

    let mut strict_uncertain = strict_compatible.clone();
    strict_uncertain.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(12)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    let strict_uncertain_route = build_aggregate_route(&strict_uncertain, AggregateKind::Exists);
    assert_eq!(
        strict_uncertain_route.execution_mode,
        RouteExecutionMode::Materialized,
        "aggregate route planning must force materialized execution when strict index compile fails",
    );

    let load_route = build_load_route_plan(&strict_uncertain)
        .expect("load route plan should build for strict/subset parity boundary shape");
    assert_eq!(
        load_route.execution_mode,
        RouteExecutionMode::Streaming,
        "load routing should remain streaming for the same shape via conservative subset policy",
    );
}

#[test]
fn route_matrix_aggregate_exists_secondary_order_prefix_shape_stays_materialized() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });

    let route_plan = build_aggregate_route(&plan, AggregateKind::Exists);

    assert_eq!(
        route_plan.execution_mode,
        RouteExecutionMode::Materialized,
        "ordered secondary-prefix EXISTS must stay on the canonical materialized lane",
    );
}

#[test]
fn route_matrix_strict_vs_subset_decision_logs_are_stable() {
    let mut strict_compatible = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    strict_compatible.scalar_plan_mut().predicate =
        Some(Predicate::eq("rank".to_string(), Value::Uint(12)));
    strict_compatible.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    strict_compatible.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let strict_compatible_route = build_aggregate_route(&strict_compatible, AggregateKind::Exists);
    let mut strict_uncertain = strict_compatible.clone();
    strict_uncertain.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(12)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    let strict_uncertain_route = build_aggregate_route(&strict_uncertain, AggregateKind::Exists);
    let load_route = build_load_route_plan(&strict_uncertain)
        .expect("load route plan should build for strict/subset log shape");

    let strict_compatible_log = format!(
        "aggregate:mode={:?};fold={:?};fetch={:?};secondary_probe={:?};index_range_limit={};continuation={:?}",
        strict_compatible_route.execution_mode,
        strict_compatible_route.aggregate_fold_mode,
        strict_compatible_route.scan_hints.physical_fetch_hint,
        strict_compatible_route.aggregate_seek_fetch_hint(),
        strict_compatible_route.index_range_limit_fast_path_enabled(),
        strict_compatible_route.continuation().capabilities().mode(),
    );
    let strict_uncertain_log = format!(
        "aggregate:mode={:?};fold={:?};fetch={:?};secondary_probe={:?};index_range_limit={};continuation={:?}",
        strict_uncertain_route.execution_mode,
        strict_uncertain_route.aggregate_fold_mode,
        strict_uncertain_route.scan_hints.physical_fetch_hint,
        strict_uncertain_route.aggregate_seek_fetch_hint(),
        strict_uncertain_route.index_range_limit_fast_path_enabled(),
        strict_uncertain_route.continuation().capabilities().mode(),
    );
    let load_log = format!(
        "load:mode={:?};fetch={:?};scan_budget={:?};index_range_limit={};continuation={:?}",
        load_route.execution_mode,
        load_route.scan_hints.physical_fetch_hint,
        load_route.scan_hints.load_scan_budget_hint,
        load_route.index_range_limit_fast_path_enabled(),
        load_route.continuation().capabilities().mode(),
    );

    assert_eq!(
        strict_compatible_log,
        "aggregate:mode=Streaming;fold=ExistingRows;fetch=Some(1);secondary_probe=None;index_range_limit=true;continuation=Initial",
        "strict-compilable aggregate route decision log should remain stable",
    );
    assert_eq!(
        strict_uncertain_log,
        "aggregate:mode=Materialized;fold=ExistingRows;fetch=Some(1);secondary_probe=None;index_range_limit=false;continuation=Initial",
        "strict-uncertain aggregate route decision log should remain stable",
    );
    assert_eq!(
        load_log,
        "load:mode=Streaming;fetch=None;scan_budget=None;index_range_limit=true;continuation=Initial",
        "subset load route decision log should remain stable for the same shape",
    );
}

#[test]
fn route_matrix_field_extrema_capability_flags_enable_for_eligible_shapes() {
    let mut min_plan =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    min_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let mut max_plan =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    max_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });

    let min_route = build_aggregate_spec_route(&min_plan, aggregate::min_by("id"));
    let max_route = build_aggregate_spec_route(&max_plan, aggregate::max_by("id"));

    assert!(min_route.field_min_fast_path_eligible());
    assert!(!min_route.field_max_fast_path_eligible());
    assert!(!max_route.field_min_fast_path_eligible());
    assert!(max_route.field_max_fast_path_eligible());
    assert_eq!(
        min_route
            .capabilities
            .field_min_fast_path_ineligibility_reason,
        None
    );
    assert_eq!(
        max_route
            .capabilities
            .field_max_fast_path_ineligibility_reason,
        None
    );
}

#[test]
fn route_matrix_field_target_max_pk_shape_enables_single_step_probe_hint() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });

    let route = build_aggregate_spec_route(&plan, aggregate::max_by("id"));

    assert_eq!(route.execution_mode, RouteExecutionMode::Streaming);
    assert!(route.field_max_fast_path_eligible());
    assert_eq!(route.scan_hints.physical_fetch_hint, Some(1));
    assert_eq!(route.aggregate_seek_fetch_hint(), Some(1));
}

#[test]
fn route_matrix_field_extrema_capability_rejects_unknown_target_field() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, false);
    let route = build_aggregate_spec_route(&plan, aggregate::min_by("missing_field"));

    assert!(!route.field_min_fast_path_eligible());
    assert!(!route.field_max_fast_path_eligible());
    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::UnknownTargetField)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_unsupported_field_type() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, false);
    let route = build_aggregate_spec_route(&plan, aggregate::min_by("scores"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::UnsupportedFieldType)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_distinct_shape() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, true);
    let route = build_aggregate_spec_route(&plan, aggregate::min_by("rank"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::DistinctNotSupported)
    );
}

#[test]
fn route_matrix_field_extrema_capability_allows_index_predicate_covered_shape() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(12)));
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: None,
        offset: 0,
    });

    let route = build_aggregate_spec_route(&plan, aggregate::min_by("rank"));

    assert!(
        route.field_min_fast_path_eligible(),
        "strict index-covered predicate shapes should remain eligible for field-extrema streaming",
    );
    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        None
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_offset_shape() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 1, false);
    let route = build_aggregate_spec_route(&plan, aggregate::min_by("rank"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::OffsetNotSupported)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_composite_access_shape() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let child_path = AccessPath::<Value>::index_range(
        ROUTE_CAPABILITY_INDEX_MODELS[0],
        vec![],
        Bound::Included(Value::Uint(10)),
        Bound::Excluded(Value::Uint(30)),
    );
    plan.access = AccessPlan::Union(vec![
        AccessPlan::path(child_path.clone()),
        AccessPlan::path(child_path),
    ])
    .into_value_plan();
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(4),
        offset: 0,
    });

    let route = build_aggregate_spec_route(&plan, aggregate::min_by("rank"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::CompositePathNotSupported)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_no_matching_index() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(4),
        offset: 0,
    });

    let route = build_aggregate_spec_route(&plan, aggregate::min_by("rank"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::NoMatchingIndex)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_page_limit_shape() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(4),
        offset: 0,
    });

    let route = build_aggregate_spec_route(&plan, aggregate::min_by("id"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::PageLimitNotSupported)
    );
}

#[test]
fn route_matrix_field_target_min_fallback_route_matches_terminal_min() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let terminal_route = build_aggregate_route(&plan, AggregateKind::Min);
    let field_route = build_aggregate_spec_route(&plan, aggregate::min_by("rank"));

    assert_eq!(terminal_route.execution_mode, RouteExecutionMode::Streaming);
    assert_eq!(field_route.execution_mode, RouteExecutionMode::Materialized);
    assert_eq!(field_route.scan_hints.physical_fetch_hint, None);
    assert_eq!(field_route.scan_hints.load_scan_budget_hint, None);
    assert!(field_route.index_range_limit_spec.is_none());
    assert_eq!(
        field_route.aggregate_fold_mode,
        terminal_route.aggregate_fold_mode
    );
    assert!(!field_route.field_min_fast_path_eligible());
    assert!(!field_route.field_max_fast_path_eligible());
}

#[test]
fn route_matrix_field_target_unknown_field_fallback_route_matches_terminal_min() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let terminal_route = build_aggregate_route(&plan, AggregateKind::Min);
    let unknown_field_route = build_aggregate_spec_route(&plan, aggregate::min_by("missing_field"));

    assert_eq!(terminal_route.execution_mode, RouteExecutionMode::Streaming);
    assert_eq!(
        unknown_field_route.execution_mode,
        RouteExecutionMode::Materialized
    );
    assert_eq!(unknown_field_route.scan_hints.physical_fetch_hint, None);
    assert_eq!(unknown_field_route.scan_hints.load_scan_budget_hint, None);
    assert!(unknown_field_route.index_range_limit_spec.is_none());
    assert_eq!(
        unknown_field_route.aggregate_fold_mode,
        terminal_route.aggregate_fold_mode
    );
    assert!(!unknown_field_route.field_min_fast_path_eligible());
    assert!(!unknown_field_route.field_max_fast_path_eligible());
}

#[test]
fn route_matrix_field_target_max_fallback_route_matches_terminal_max_desc() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let terminal_route = build_aggregate_route(&plan, AggregateKind::Max);
    let field_route = build_aggregate_spec_route(&plan, aggregate::max_by("rank"));

    assert_eq!(terminal_route.execution_mode, RouteExecutionMode::Streaming);
    assert_eq!(field_route.execution_mode, RouteExecutionMode::Materialized);
    assert_eq!(field_route.scan_hints.physical_fetch_hint, None);
    assert_eq!(field_route.scan_hints.load_scan_budget_hint, None);
    assert!(field_route.index_range_limit_spec.is_none());
    assert_eq!(
        field_route.aggregate_fold_mode,
        terminal_route.aggregate_fold_mode
    );
    assert!(!field_route.field_min_fast_path_eligible());
    assert!(!field_route.field_max_fast_path_eligible());
}

#[test]
fn route_matrix_field_target_non_extrema_fallback_route_matches_terminal_count() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 2,
    });

    let terminal_route = build_aggregate_route(&plan, AggregateKind::Count);
    let field_route = build_aggregate_spec_route(&plan, aggregate::count_by("rank"));

    assert_eq!(field_route.execution_mode, terminal_route.execution_mode);
    assert_eq!(
        field_route.scan_hints.physical_fetch_hint,
        terminal_route.scan_hints.physical_fetch_hint
    );
    assert_eq!(
        field_route.scan_hints.load_scan_budget_hint,
        terminal_route.scan_hints.load_scan_budget_hint
    );
    assert_eq!(
        field_route.index_range_limit_spec,
        terminal_route.index_range_limit_spec
    );
    assert_eq!(
        field_route.aggregate_fold_mode,
        terminal_route.aggregate_fold_mode
    );
    assert!(!field_route.field_min_fast_path_eligible());
    assert!(!field_route.field_max_fast_path_eligible());
}
