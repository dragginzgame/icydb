//! Module: db::executor::planning::route::tests
//! Covers route planning, route diagnostics, and route-shape invariants.
//! Does not own: production route behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

mod aggregate;
mod aggregate_matrix;
mod covering;
mod fast_path_guards;
mod field_extrema;
mod grouped;

use super::terminal::derive_load_terminal_fast_path_contract;
use super::{
    AGGREGATE_FAST_PATH_ORDER, AggregateRouteShape, FastPathOrder, GroupedExecutionMode,
    GroupedExecutionModeProjection, GroupedRouteDecisionOutcome, LOAD_FAST_PATH_ORDER,
    LoadOrderRouteContract, LoadOrderRouteReason, LoadTerminalFastPathContract, RouteCapabilities,
    RouteExecutionMode, RouteShapeKind, TopNSeekSpec,
    build_execution_route_plan_for_aggregate_spec, build_execution_route_plan_for_grouped_plan,
    build_execution_route_plan_for_load, build_execution_route_plan_for_mutation,
    build_initial_execution_route_plan_for_load,
    capability::{
        count_pushdown_existing_rows_shape_supported,
        index_range_limit_pushdown_shape_supported_for_model,
    },
    grouped_ordered_runtime_revalidation_flag_count_guard, route_capability_flag_count_guard,
    route_shape_kind_count_guard,
};
use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        cursor::CursorBoundary,
        direction::Direction,
        executor::{
            EntityAuthority, ExecutionPlan, ExecutionPreparation,
            aggregate::AggregateFoldMode,
            aggregate::capability::AggregateFieldExtremaIneligibilityReason,
            planning::{
                continuation::{ContinuationMode, ScalarContinuationContext},
                preparation::slot_map_for_model_plan,
            },
        },
        index::{IndexCompilePolicy, compile_index_program},
        predicate::{CompareOp, MissingRowPolicy, Predicate},
        query::builder::aggregate as aggregate_builder,
        query::explain::{
            ExplainGroupAggregate, ExplainGroupField, ExplainGroupHaving, ExplainGrouping,
        },
        query::plan::{
            AccessPlannedQuery, AggregateKind, CoveringExistingRowMode, CoveringReadFieldSource,
            DeleteSpec, FieldSlot, GroupAggregateSpec, GroupDistinctPolicyReason, GroupSpec,
            GroupedExecutionConfig, GroupedPlanAggregateFamily, GroupedPlanFallbackReason,
            GroupedPlanStrategy, OrderDirection, OrderSpec, PageSpec, QueryMode,
            expr::{FieldId, ProjectionSelection},
            group_aggregate_spec_expr, grouped_executor_handoff, grouped_plan_strategy,
        },
    },
    model::{field::FieldKind, index::IndexModel},
    traits::{EntitySchema, Path},
    types::Ulid,
    value::Value,
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::{fs, ops::Bound};

fn aggregate_having_expr(
    group: &GroupSpec,
    index: usize,
    op: CompareOp,
    value: Value,
) -> crate::db::query::plan::expr::Expr {
    having_compare_expr(
        crate::db::query::plan::expr::Expr::Aggregate(group_aggregate_spec_expr(
            group
                .aggregates
                .get(index)
                .expect("grouped HAVING aggregate should exist"),
        )),
        op,
        value,
    )
}

fn having_compare_expr(
    left: crate::db::query::plan::expr::Expr,
    op: CompareOp,
    value: Value,
) -> crate::db::query::plan::expr::Expr {
    if matches!(value, Value::Null) {
        let function = match op {
            CompareOp::Eq => Some(crate::db::query::plan::expr::Function::IsNull),
            CompareOp::Ne => Some(crate::db::query::plan::expr::Function::IsNotNull),
            CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::Gt
            | CompareOp::Gte
            | CompareOp::In
            | CompareOp::NotIn
            | CompareOp::Contains
            | CompareOp::StartsWith
            | CompareOp::EndsWith => None,
        };

        if let Some(function) = function {
            return crate::db::query::plan::expr::Expr::FunctionCall {
                function,
                args: vec![left],
            };
        }
    }

    crate::db::query::plan::expr::Expr::Binary {
        op: match op {
            CompareOp::Eq
            | CompareOp::In
            | CompareOp::NotIn
            | CompareOp::Contains
            | CompareOp::StartsWith
            | CompareOp::EndsWith => crate::db::query::plan::expr::BinaryOp::Eq,
            CompareOp::Ne => crate::db::query::plan::expr::BinaryOp::Ne,
            CompareOp::Lt => crate::db::query::plan::expr::BinaryOp::Lt,
            CompareOp::Lte => crate::db::query::plan::expr::BinaryOp::Lte,
            CompareOp::Gt => crate::db::query::plan::expr::BinaryOp::Gt,
            CompareOp::Gte => crate::db::query::plan::expr::BinaryOp::Gte,
        },
        left: Box::new(left),
        right: Box::new(crate::db::query::plan::expr::Expr::Literal(value)),
    }
}

const ROUTE_FEATURE_SOFT_BUDGET_DELTA: usize = 1;
const ROUTE_CAPABILITY_FLAG_BASELINE_0247: usize = 9;
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

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RouteCapabilityEntity {
    id: Ulid,
    rank: u32,
    label: String,
    scores: Vec<u32>,
}

static ROUTE_CAPABILITY_SCORE_KIND: FieldKind = FieldKind::Uint;
static ROUTE_CAPABILITY_INDEX_FIELDS: [&str; 1] = ["rank"];
static ROUTE_CAPABILITY_COMPOSITE_INDEX_FIELDS: [&str; 2] = ["rank", "label"];
static ROUTE_CAPABILITY_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "rank_idx",
    RouteCapabilityTestStore::PATH,
    &ROUTE_CAPABILITY_INDEX_FIELDS,
    false,
)];
static ROUTE_CAPABILITY_COMPOSITE_INDEX_MODEL: IndexModel = IndexModel::generated(
    "rank_label_idx",
    RouteCapabilityTestStore::PATH,
    &ROUTE_CAPABILITY_COMPOSITE_INDEX_FIELDS,
    false,
);

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

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct UniqueRouteCapabilityEntity {
    id: Ulid,
    code: u32,
}

static UNIQUE_ROUTE_CAPABILITY_INDEX_FIELDS: [&str; 1] = ["code"];
static UNIQUE_ROUTE_CAPABILITY_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "code_idx",
    RouteCapabilityTestStore::PATH,
    &UNIQUE_ROUTE_CAPABILITY_INDEX_FIELDS,
    true,
)];

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

fn route_capability_authority() -> EntityAuthority {
    EntityAuthority::for_type::<RouteCapabilityEntity>()
}

fn unique_route_capability_authority() -> EntityAuthority {
    EntityAuthority::for_type::<UniqueRouteCapabilityEntity>()
}

fn finalized_plan_for_authority(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> AccessPlannedQuery {
    let mut finalized = plan.clone();
    authority.finalize_static_planning_shape(&mut finalized);
    authority.finalize_planner_route_profile(&mut finalized);

    finalized
}

fn build_load_route_plan_for_authority(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    probe_fetch_hint: Option<usize>,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    let finalized = finalized_plan_for_authority(authority, plan);

    build_execution_route_plan_for_load(authority, &finalized, continuation, probe_fetch_hint)
}

fn build_load_route_plan(
    plan: &AccessPlannedQuery,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    build_load_route_plan_for_authority(
        route_capability_authority(),
        plan,
        &initial_scalar_continuation_context(),
        None,
    )
}

fn build_load_route_plan_with_continuation(
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    build_load_route_plan_for_authority(route_capability_authority(), plan, continuation, None)
}

fn build_load_route_plan_with_probe_hint(
    plan: &AccessPlannedQuery,
    probe_fetch_hint: Option<usize>,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    build_load_route_plan_for_authority(
        route_capability_authority(),
        plan,
        &initial_scalar_continuation_context(),
        probe_fetch_hint,
    )
}

fn build_unique_load_route_plan(
    plan: &AccessPlannedQuery,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    build_load_route_plan_for_authority(
        unique_route_capability_authority(),
        plan,
        &initial_scalar_continuation_context(),
        None,
    )
}

fn load_count_pushdown_existing_rows_shape_supported(plan: &AccessPlannedQuery) -> bool {
    let finalized = finalized_plan_for_authority(route_capability_authority(), plan);

    count_pushdown_existing_rows_shape_supported(&finalized.access_strategy().class())
}

fn load_index_range_limit_pushdown_shape_supported(plan: &AccessPlannedQuery) -> bool {
    let finalized = finalized_plan_for_authority(route_capability_authority(), plan);

    index_range_limit_pushdown_shape_supported_for_model(
        &finalized,
        finalized.planner_route_profile(),
    )
}

fn build_mutation_route_plan(
    plan: &AccessPlannedQuery,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    let authority = route_capability_authority();
    let finalized = finalized_plan_for_authority(authority, plan);

    build_execution_route_plan_for_mutation(authority, &finalized)
}

fn build_initial_load_route_plan(
    plan: &AccessPlannedQuery,
) -> Result<ExecutionPlan, crate::error::InternalError> {
    let authority = route_capability_authority();
    let finalized = finalized_plan_for_authority(authority, plan);

    build_initial_execution_route_plan_for_load(authority, &finalized, None)
}

fn derive_load_terminal_fast_path_contract_for_test(
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> Option<LoadTerminalFastPathContract> {
    let authority = route_capability_authority();
    let finalized = finalized_plan_for_authority(authority, plan);

    derive_load_terminal_fast_path_contract(authority, &finalized, strict_predicate_compatible)
}

fn build_aggregate_route(plan: &AccessPlannedQuery, kind: AggregateKind) -> ExecutionPlan {
    let aggregate_expr = match kind {
        AggregateKind::Count => aggregate_builder::count(),
        AggregateKind::Exists => aggregate_builder::exists(),
        AggregateKind::Min => aggregate_builder::min(),
        AggregateKind::Max => aggregate_builder::max(),
        AggregateKind::First => aggregate_builder::first(),
        AggregateKind::Last => aggregate_builder::last(),
        _ => panic!("unsupported terminal aggregate kind for route test helper: {kind:?}"),
    };

    build_aggregate_spec_route(plan, aggregate_expr)
}

fn build_aggregate_spec_route(
    plan: &AccessPlannedQuery,
    aggregate_expr: crate::db::query::builder::AggregateExpr,
) -> ExecutionPlan {
    let authority = route_capability_authority();
    let finalized = finalized_plan_for_authority(authority, plan);
    let execution_preparation =
        ExecutionPreparation::from_plan(&finalized, slot_map_for_model_plan(&finalized));

    build_execution_route_plan_for_aggregate_spec(
        &finalized,
        AggregateRouteShape::new_from_fields(
            aggregate_expr.kind(),
            aggregate_expr.target_field(),
            authority.fields(),
            authority.primary_key_name(),
        ),
        &execution_preparation,
    )
}

// Build one narrow order-only covering plan used to prove that route-level
// planner-owned covering mode applies directly to admitted secondary routes.
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    plan
}

// Build one explicit composite order-only covering plan used to prove that the
// same planner-owned covering mode applies to admitted composite routes too.
fn composite_secondary_order_covering_plan(direction: OrderDirection) -> AccessPlannedQuery {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_COMPOSITE_INDEX_MODEL,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.projection_selection = ProjectionSelection::Fields(vec![
        FieldId::new("id"),
        FieldId::new("rank"),
        FieldId::new("label"),
    ]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", direction),
            crate::db::query::plan::OrderTerm::field("label", direction),
            crate::db::query::plan::OrderTerm::field("id", direction),
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
            crate::db::query::plan::OrderTerm::field("rank", direction),
            crate::db::query::plan::OrderTerm::field("id", direction),
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
    let finalized = finalized_plan_for_authority(route_capability_authority(), plan);
    let grouped_handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");

    build_execution_route_plan_for_grouped_plan(
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy(),
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
        format!("route_strategy={:?}", route_plan.route_shape_kind()),
        format!("execution_mode={:?}", route_plan.execution_mode()),
        format!("fold_mode={:?}", route_plan.aggregate_fold_mode),
    ]
    .join("\n")
}

fn grouped_aggregate_route_snapshot(plan: &AccessPlannedQuery) -> String {
    let finalized = finalized_plan_for_authority(route_capability_authority(), plan);
    let planner_strategy = grouped_plan_strategy(&finalized)
        .expect("grouped route snapshot requires grouped strategy");
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped route snapshot requires handoff");
    let aggregate_contracts = handoff
        .aggregate_specs()
        .iter()
        .map(|aggregate_spec| {
            format!(
                "{:?}:{:?}:{}",
                aggregate_spec.kind(),
                aggregate_spec.target_field(),
                aggregate_spec.distinct()
            )
        })
        .collect::<Vec<_>>();
    let route_plan = build_execution_route_plan_for_grouped_plan(
        handoff.base(),
        handoff.grouped_plan_strategy(),
    );
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route snapshot requires grouped observability payload");

    [
        "grouped=true".to_string(),
        format!("planner_strategy={planner_strategy:?}"),
        format!("aggregate_contracts={aggregate_contracts:?}"),
        format!("route_strategy={:?}", route_plan.route_shape_kind()),
        format!("execution_mode={:?}", route_plan.execution_mode()),
        format!(
            "planner_fallback_reason={:?}",
            grouped_observability.planner_fallback_reason()
        ),
        format!(
            "grouped_execution_mode={:?}",
            grouped_observability.grouped_execution_mode()
        ),
        format!("fold_mode={:?}", route_plan.aggregate_fold_mode),
    ]
    .join("\n")
}

fn grouped_policy_snapshot(
    plan: &AccessPlannedQuery,
) -> (
    GroupedPlanStrategy,
    Option<crate::db::query::plan::GroupDistinctPolicyReason>,
    GroupedExecutionMode,
    bool,
) {
    let finalized = finalized_plan_for_authority(route_capability_authority(), plan);
    let planner_strategy =
        grouped_plan_strategy(&finalized).expect("grouped plans should project planner strategy");
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped plans should project handoff");
    let distinct_violation = handoff.distinct_policy_violation_for_executor();
    let route_plan = build_execution_route_plan_for_grouped_plan(
        handoff.base(),
        handoff.grouped_plan_strategy(),
    );
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped plans should always project grouped route observability");

    (
        planner_strategy,
        distinct_violation,
        grouped_observability.grouped_execution_mode(),
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
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
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
    assert!(!load_count_pushdown_existing_rows_shape_supported(&plan));
    assert!(!load_index_range_limit_pushdown_shape_supported(&plan));
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
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
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
    assert!(!load_count_pushdown_existing_rows_shape_supported(&plan));
    assert!(!load_index_range_limit_pushdown_shape_supported(&plan));
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        route_plan
            .load_order_route_contract()
            .allows_streaming_load()
    );
    assert!(load_count_pushdown_existing_rows_shape_supported(&plan));
    assert!(load_index_range_limit_pushdown_shape_supported(&plan));
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
    let _route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        load_index_range_limit_pushdown_shape_supported(&plan),
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

    let _route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        !load_index_range_limit_pushdown_shape_supported(&plan),
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
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
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
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
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert_eq!(route_plan.fast_path_order(), &LOAD_FAST_PATH_ORDER);
    assert_eq!(route_plan.direction(), Direction::Asc);
    assert_eq!(route_plan.continuation().mode(), ContinuationMode::Initial);
}

#[test]
fn route_plan_shape_descriptor_matches_route_axes() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert_eq!(route_plan.execution_mode(), RouteExecutionMode::Streaming);
    assert!(route_plan.is_streaming());
}

#[test]
fn runtime_route_consumers_avoid_direct_execution_mode_field_reads() {
    let runtime_consumers = [
        "src/db/executor/pipeline/runtime/mod.rs",
        "src/db/executor/pipeline/runtime/fast_path/mod.rs",
        "src/db/executor/pipeline/runtime/fast_path/strategy.rs",
        "src/db/executor/pipeline/entrypoints/scalar/mod.rs",
        "src/db/executor/pipeline/entrypoints/scalar/hints.rs",
        "src/db/executor/aggregate/mod.rs",
        "src/db/executor/explain/descriptor/mod.rs",
    ];

    for relative_path in runtime_consumers {
        let absolute_path = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), relative_path);
        let source = fs::read_to_string(&absolute_path)
            .unwrap_or_else(|err| panic!("failed to read {absolute_path}: {err}"));
        assert!(
            !source.contains("route_plan.execution_mode,")
                && !source.contains("route_plan.execution_mode;")
                && !source.contains("route_plan.execution_mode "),
            "runtime route consumer should use ExecutionRoutePlan accessors instead of direct execution_mode field reads: {relative_path}",
        );
    }
}

#[test]
fn route_matrix_load_pk_desc_with_page_uses_streaming_budget_and_reverse() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 2,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert_eq!(route_plan.execution_mode(), RouteExecutionMode::Streaming);
    assert_eq!(route_plan.direction(), Direction::Desc);
    assert_eq!(route_plan.continuation().mode(), ContinuationMode::Initial);
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
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

    assert_eq!(route_plan.execution_mode(), RouteExecutionMode::Streaming);
    assert_eq!(
        route_plan.continuation().mode(),
        ContinuationMode::CursorBoundary
    );
    assert_eq!(route_plan.continuation().effective_offset(), 0);
    assert!(route_plan.desc_physical_reverse_supported());
    assert!(route_plan.index_range_limit_spec.is_none());
    assert!(route_plan.top_n_seek_spec().is_none());
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
}

#[test]
fn route_matrix_load_index_range_residual_filter_predicate_allows_small_window_pushdown() {
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
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
fn route_matrix_load_index_range_offset_uses_bounded_limit_pushdown() {
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Streaming,
        "offset-aware ordered index-range shapes should stay streaming when pushdown is admitted",
    );
    assert_eq!(
        route_plan.index_range_limit_spec.map(|spec| spec.fetch),
        Some(4),
        "offset-aware ordered index-range shapes should derive offset+limit+1 fetch contracts",
    );
    assert_eq!(
        route_plan.scan_hints.load_scan_budget_hint,
        Some(4),
        "offset-aware ordered index-range shapes should keep scan-budget hints aligned with limit pushdown",
    );
    assert_eq!(
        route_plan.top_n_seek_spec().map(TopNSeekSpec::fetch),
        Some(4),
        "offset-aware ordered index-range shapes should retain one generic bounded window hint alongside limit pushdown",
    );
}

#[test]
fn route_matrix_load_index_range_desc_offset_uses_bounded_limit_pushdown() {
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Streaming,
        "descending offset-aware ordered index-range shapes should stay streaming when pushdown is admitted",
    );
    assert_eq!(
        route_plan.index_range_limit_spec.map(|spec| spec.fetch),
        Some(4),
        "descending offset-aware ordered index-range shapes should derive offset+limit+1 fetch contracts",
    );
    assert_eq!(
        route_plan.scan_hints.load_scan_budget_hint,
        Some(4),
        "descending offset-aware ordered index-range shapes should keep scan-budget hints aligned with limit pushdown",
    );
    assert_eq!(
        route_plan.top_n_seek_spec().map(TopNSeekSpec::fetch),
        Some(4),
        "descending offset-aware ordered index-range shapes should retain one generic bounded window hint alongside limit pushdown",
    );
}

#[test]
fn route_matrix_load_composite_order_only_offset_uses_bounded_limit_pushdown() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_COMPOSITE_INDEX_MODEL,
            vec![],
            Bound::Unbounded,
            Bound::Unbounded,
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("label", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Streaming,
        "offset-aware composite order-only index-range shapes should stay streaming when pushdown is admitted",
    );
    assert_eq!(
        route_plan.index_range_limit_spec.map(|spec| spec.fetch),
        Some(4),
        "offset-aware composite order-only index-range shapes should derive offset+limit+1 fetch contracts",
    );
    assert_eq!(
        route_plan.scan_hints.load_scan_budget_hint,
        Some(4),
        "offset-aware composite order-only index-range shapes should keep scan-budget hints aligned with limit pushdown",
    );
    assert_eq!(
        route_plan.top_n_seek_spec().map(TopNSeekSpec::fetch),
        Some(4),
        "offset-aware composite order-only index-range shapes should retain one generic bounded window hint alongside limit pushdown",
    );
}

#[test]
fn route_matrix_load_composite_order_only_desc_offset_uses_bounded_limit_pushdown() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_COMPOSITE_INDEX_MODEL,
            vec![],
            Bound::Unbounded,
            Bound::Unbounded,
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("label", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Streaming,
        "descending offset-aware composite order-only index-range shapes should stay streaming when pushdown is admitted",
    );
    assert_eq!(
        route_plan.index_range_limit_spec.map(|spec| spec.fetch),
        Some(4),
        "descending offset-aware composite order-only index-range shapes should derive offset+limit+1 fetch contracts",
    );
    assert_eq!(
        route_plan.scan_hints.load_scan_budget_hint,
        Some(4),
        "descending offset-aware composite order-only index-range shapes should keep scan-budget hints aligned with limit pushdown",
    );
    assert_eq!(
        route_plan.top_n_seek_spec().map(TopNSeekSpec::fetch),
        Some(4),
        "descending offset-aware composite order-only index-range shapes should retain one generic bounded window hint alongside limit pushdown",
    );
}

#[test]
fn route_matrix_load_index_range_residual_filter_predicate_large_window_disables_pushdown() {
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
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
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "label",
            OrderDirection::Asc,
        )],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        !load_index_range_limit_pushdown_shape_supported(&plan),
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
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "rank",
            OrderDirection::Asc,
        )],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        !load_index_range_limit_pushdown_shape_supported(&plan),
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert!(
        !load_index_range_limit_pushdown_shape_supported(&plan),
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
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "rank",
            OrderDirection::Desc,
        )],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 2,
    });
    let route_plan = build_load_route_plan(&plan).expect("load route plan should build");

    assert_eq!(
        route_plan.execution_mode(),
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
            crate::db::query::plan::OrderTerm::field("code", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(1),
        offset: 0,
    });
    let route_plan = build_unique_load_route_plan(&plan)
        .expect("secondary-order limit-one route plan should build");

    assert_eq!(route_plan.execution_mode(), RouteExecutionMode::Streaming);
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
fn route_matrix_load_unique_secondary_order_offset_uses_bounded_top_n_seek() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: UNIQUE_ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("code", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let route_plan = build_unique_load_route_plan(&plan)
        .expect("offset-sensitive unique secondary order route plan should build");

    assert_eq!(
        route_plan.load_order_route_contract(),
        LoadOrderRouteContract::DirectStreaming,
        "offset-sensitive unique secondary order should stay on the direct streaming contract",
    );
    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Streaming,
        "offset-sensitive unique secondary order should stay streaming",
    );
    assert_eq!(
        route_plan.top_n_seek_spec().map(TopNSeekSpec::fetch),
        Some(4),
        "offset-sensitive unique secondary order should derive one offset-aware Top-N seek fetch window",
    );
    assert_eq!(
        route_plan.scan_hints.load_scan_budget_hint,
        Some(4),
        "offset-sensitive unique secondary order should derive one offset-aware scan-budget hint",
    );
}

#[test]
fn route_matrix_load_bound_non_unique_secondary_order_offset_uses_bounded_top_n_seek() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_COMPOSITE_INDEX_MODEL,
            values: vec![Value::Uint(10)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("label", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let route_plan = build_load_route_plan(&plan)
        .expect("offset-sensitive bound non-unique secondary order route plan should build");

    assert_eq!(
        route_plan.load_order_route_contract(),
        LoadOrderRouteContract::DirectStreaming,
        "offset-sensitive bound non-unique secondary order should stay on the direct streaming contract",
    );
    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Streaming,
        "offset-sensitive bound non-unique secondary order should stay streaming",
    );
    assert_eq!(
        route_plan.top_n_seek_spec().map(TopNSeekSpec::fetch),
        Some(4),
        "offset-sensitive bound non-unique secondary order should derive one offset-aware Top-N seek fetch window",
    );
    assert_eq!(
        route_plan.scan_hints.load_scan_budget_hint,
        Some(4),
        "offset-sensitive bound non-unique secondary order should derive one offset-aware scan-budget hint",
    );
}

#[test]
fn route_matrix_load_bound_non_unique_secondary_order_desc_offset_fails_closed_before_top_n() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_COMPOSITE_INDEX_MODEL,
            values: vec![Value::Uint(10)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("label", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let route_plan = build_load_route_plan(&plan).expect(
        "offset-sensitive descending bound non-unique secondary order route plan should build",
    );

    assert_eq!(
        route_plan.load_order_route_contract(),
        LoadOrderRouteContract::MaterializedBoundary,
        "offset-sensitive descending bound non-unique secondary order must fail closed to the materialized boundary contract",
    );
    assert_eq!(
        route_plan.load_order_route_reason(),
        LoadOrderRouteReason::DescendingNonUniqueSecondaryPrefixNotAdmitted,
        "offset-sensitive descending bound non-unique secondary order must expose the planner-owned boundary reason",
    );
    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Materialized,
        "offset-sensitive descending bound non-unique secondary order must fail closed to materialized execution",
    );
    assert_eq!(
        route_plan.top_n_seek_spec(),
        None,
        "offset-sensitive descending bound non-unique secondary order must not derive Top-N seek",
    );
    assert_eq!(
        route_plan.scan_hints.load_scan_budget_hint, None,
        "offset-sensitive descending bound non-unique secondary order must not derive bounded scan-budget hints",
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
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
        route_plan.load_order_route_reason(),
        LoadOrderRouteReason::RequiresMaterializedSort,
        "non-unique descending secondary order must expose the planner-owned fallback reason",
    );
    assert_eq!(
        route_plan.execution_mode(),
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
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
        route_plan.load_order_route_reason(),
        LoadOrderRouteReason::RequiresMaterializedSort,
        "offset-sensitive descending secondary order must expose the planner-owned fallback reason",
    );
    assert_eq!(
        route_plan.execution_mode(),
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
fn route_matrix_load_unique_secondary_order_desc_offset_uses_bounded_top_n_seek() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: UNIQUE_ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("code", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
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
        LoadOrderRouteContract::DirectStreaming,
        "offset-sensitive unique descending secondary order should stay on the direct streaming contract",
    );
    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Streaming,
        "offset-sensitive unique descending secondary order should stay streaming",
    );
    assert_eq!(
        route_plan.top_n_seek_spec().map(TopNSeekSpec::fetch),
        Some(3),
        "offset-sensitive unique descending secondary order should derive one offset-aware Top-N seek fetch window",
    );
    assert_eq!(
        route_plan.scan_hints.load_scan_budget_hint,
        Some(3),
        "offset-sensitive unique descending secondary order should derive one offset-aware scan-budget hint",
    );
}

#[test]
fn route_matrix_load_bound_non_unique_secondary_order_distinct_requires_materialized_boundary() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_COMPOSITE_INDEX_MODEL,
            values: vec![Value::Uint(10)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("label", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().distinct = true;
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(1),
        offset: 0,
    });

    let route_plan = build_load_route_plan(&plan)
        .expect("distinct bound non-unique secondary order route plan should build");

    assert_eq!(
        route_plan.load_order_route_contract(),
        LoadOrderRouteContract::MaterializedBoundary,
        "distinct bound non-unique secondary order must stay on the materialized boundary contract",
    );
    assert_eq!(
        route_plan.load_order_route_reason(),
        LoadOrderRouteReason::DistinctRequiresMaterialization,
        "distinct bound non-unique secondary order must expose the planner-owned materialized-boundary reason",
    );
    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Materialized,
        "distinct bound non-unique secondary order must stay on materialized execution",
    );
    assert_eq!(
        route_plan.top_n_seek_spec(),
        None,
        "distinct bound non-unique secondary order must not derive top-n seek",
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
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
        route_plan.load_order_route_reason(),
        LoadOrderRouteReason::RequiresMaterializedSort,
        "distinct descending secondary order must expose the planner-owned fallback reason",
    );
    assert_eq!(
        route_plan.execution_mode(),
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
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
        route_plan.load_order_route_reason(),
        LoadOrderRouteReason::ResidualFilterBlocksDirectStreaming,
        "residual descending secondary order must expose the planner-owned fallback reason",
    );
    assert_eq!(
        route_plan.execution_mode(),
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
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
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
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
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
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
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
        route_plan.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert!(
        route_plan.fast_path_order().is_empty(),
        "mutation routes must not advertise load or aggregate fast paths",
    );
    assert_eq!(route_plan.direction(), Direction::Asc);
    assert_eq!(route_plan.continuation().mode(), ContinuationMode::Initial);
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
