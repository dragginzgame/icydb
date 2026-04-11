//! Module: db::executor::route::tests
//! Covers route planning, route diagnostics, and route-shape invariants.
//! Does not own: production route behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

mod aggregate;
mod covering;
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
            ExplainGroupAggregate, ExplainGroupField, ExplainGroupHaving, ExplainGroupHavingClause,
            ExplainGroupHavingSymbol, ExplainGrouping,
        },
        query::plan::{
            AccessPlannedQuery, AggregateKind, CoveringExistingRowMode, CoveringReadFieldSource,
            DeleteSpec, FieldSlot, GroupAggregateSpec, GroupDistinctPolicyReason,
            GroupHavingClause, GroupHavingSpec, GroupHavingSymbol, GroupSpec,
            GroupedExecutionConfig, GroupedPlanAggregateFamily, GroupedPlanFallbackReason,
            GroupedPlanStrategy, OrderDirection, OrderSpec, PageSpec, QueryMode,
            expr::{FieldId, ProjectionSelection},
            grouped_executor_handoff, grouped_plan_strategy,
        },
    },
    model::{field::FieldKind, index::IndexModel},
    traits::{EntitySchema, Path},
    types::Ulid,
    value::Value,
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::{Deserialize, Serialize};
use std::{fs, ops::Bound};

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

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
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
            ("rank".to_string(), direction),
            ("label".to_string(), direction),
            ("id".to_string(), direction),
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
        .aggregate_projection_specs()
        .iter()
        .map(|aggregate_projection_spec| {
            format!(
                "{:?}:{:?}:{}",
                aggregate_projection_spec.kind(),
                aggregate_projection_spec.target_field(),
                aggregate_projection_spec.distinct()
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
    assert_eq!(route_plan.continuation().mode(), ContinuationMode::Initial);
}

#[test]
fn route_plan_shape_descriptor_matches_route_axes() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
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
        fields: vec![("id".to_string(), OrderDirection::Desc)],
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
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
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
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
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
            ("rank".to_string(), OrderDirection::Asc),
            ("label".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
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
            ("rank".to_string(), OrderDirection::Desc),
            ("label".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
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
        fields: vec![("rank".to_string(), OrderDirection::Asc)],
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
        fields: vec![("rank".to_string(), OrderDirection::Desc)],
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
            ("code".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
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
            ("label".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
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
            ("label".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
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
            ("label".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
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
        route_plan.load_order_route_reason(),
        LoadOrderRouteReason::ResidualPredicateBlocksDirectStreaming,
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
    let finalized = finalized_plan_for_authority(route_capability_authority(), &plan);

    let execution_preparation =
        ExecutionPreparation::from_plan(&finalized, slot_map_for_model_plan(&finalized));
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
        strict_compatible_route.continuation().mode(),
    );
    let strict_uncertain_log = format!(
        "aggregate:mode={:?};fold={:?};fetch={:?};secondary_probe={:?};index_range_limit={};continuation={:?}",
        strict_uncertain_route.execution_mode,
        strict_uncertain_route.aggregate_fold_mode,
        strict_uncertain_route.scan_hints.physical_fetch_hint,
        strict_uncertain_route.aggregate_seek_fetch_hint(),
        strict_uncertain_route.index_range_limit_fast_path_enabled(),
        strict_uncertain_route.continuation().mode(),
    );
    let load_log = format!(
        "load:mode={:?};fetch={:?};scan_budget={:?};index_range_limit={};continuation={:?}",
        load_route.execution_mode,
        load_route.scan_hints.physical_fetch_hint,
        load_route.scan_hints.load_scan_budget_hint,
        load_route.index_range_limit_fast_path_enabled(),
        load_route.continuation().mode(),
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

    let min_route = build_aggregate_spec_route(&min_plan, aggregate_builder::min_by("id"));
    let max_route = build_aggregate_spec_route(&max_plan, aggregate_builder::max_by("id"));

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

    let route = build_aggregate_spec_route(&plan, aggregate_builder::max_by("id"));

    assert_eq!(route.execution_mode, RouteExecutionMode::Streaming);
    assert!(route.field_max_fast_path_eligible());
    assert_eq!(route.scan_hints.physical_fetch_hint, Some(1));
    assert_eq!(route.aggregate_seek_fetch_hint(), Some(1));
}

#[test]
fn route_matrix_field_extrema_capability_rejects_unknown_target_field() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, false);
    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("missing_field"));

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
    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("scores"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::UnsupportedFieldType)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_distinct_shape() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, true);
    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("rank"));

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

    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("rank"));

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
    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("rank"));

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

    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("rank"));

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

    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("rank"));

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

    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("id"));

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
    let field_route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("rank"));

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
    let unknown_field_route =
        build_aggregate_spec_route(&plan, aggregate_builder::min_by("missing_field"));

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
    let field_route = build_aggregate_spec_route(&plan, aggregate_builder::max_by("rank"));

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
    let field_route = build_aggregate_spec_route(&plan, aggregate_builder::count_by("rank"));

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
