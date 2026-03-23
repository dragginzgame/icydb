//! Module: db::executor::tests::route
//! Responsibility: module-local ownership and contracts for db::executor::tests::route.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        cursor::CursorBoundary,
        direction::Direction,
        executor::{
            aggregate::AggregateFoldMode,
            continuation::ScalarContinuationContext,
            pipeline::contracts::LoadExecutor,
            route::{
                AGGREGATE_FAST_PATH_ORDER, ContinuationMode, ExecutionModeRouteCase, FastPathOrder,
                FieldExtremaIneligibilityReason, GroupedRouteDecisionOutcome, LOAD_FAST_PATH_ORDER,
                MUTATION_FAST_PATH_ORDER, RouteExecutionMode,
                build_execution_route_plan_for_load_with_model,
                build_execution_route_plan_for_mutation_with_model,
                grouped_ordered_runtime_revalidation_flag_count_guard,
                route_capability_flag_count_guard, route_execution_mode_case_count_guard,
                route_shape_kind_count_guard,
            },
        },
        predicate::{CompareOp, MissingRowPolicy, Predicate},
        query::plan::{
            AccessPlannedQuery, AggregateKind, DeleteSpec, FieldSlot, GroupAggregateSpec,
            GroupHavingClause, GroupHavingSpec, GroupHavingSymbol, GroupSpec,
            GroupedExecutionConfig, OrderDirection, OrderSpec, PageSpec, QueryMode,
            grouped_executor_handoff,
        },
    },
    model::{field::FieldKind, index::IndexModel},
    traits::{EntitySchema, Path},
    types::Ulid,
    value::Value,
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::{Deserialize, Serialize};
use std::ops::Bound;

const ROUTE_FEATURE_SOFT_BUDGET_DELTA: usize = 1;
const ROUTE_CAPABILITY_FLAG_BASELINE_0247: usize = 9;
const ROUTE_EXECUTION_MODE_CASE_BASELINE_0246: usize = 3;
const ROUTE_SHAPE_KIND_BASELINE_0256: usize = 4;
const ROUTE_GROUPED_RUNTIME_REVALIDATION_FLAG_BASELINE_0251: usize = 3;

crate::test_canister! {
    ident = RouteMatrixCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = RouteMatrixStore,
    canister = RouteMatrixCanister,
}

static ROUTE_MATRIX_SCORE_KIND: FieldKind = FieldKind::Uint;
static ROUTE_MATRIX_INDEX_FIELDS: [&str; 1] = ["rank"];
static ROUTE_MATRIX_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
    "rank_idx",
    RouteMatrixStore::PATH,
    &ROUTE_MATRIX_INDEX_FIELDS,
    false,
)];

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize)]
struct RouteMatrixEntity {
    id: Ulid,
    rank: u32,
    label: String,
    scores: Vec<u32>,
}

crate::test_entity_schema! {
    ident = RouteMatrixEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RouteMatrixEntity",
    entity_tag = crate::testing::ROUTE_MATRIX_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("rank", FieldKind::Uint),
        ("label", FieldKind::Text),
        ("scores", FieldKind::List(&ROUTE_MATRIX_SCORE_KIND)),
    ],
    indexes = [&ROUTE_MATRIX_INDEX_MODELS[0]],
    store = RouteMatrixStore,
    canister = RouteMatrixCanister,
}

fn field_extrema_index_range_plan(
    direction: OrderDirection,
    offset: u32,
    distinct: bool,
) -> AccessPlannedQuery {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), direction),
            ("id".to_string(), direction),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(4),
        offset,
    });
    plan.scalar_plan_mut().distinct = distinct;

    plan
}

fn grouped_field_slot(field: &str) -> FieldSlot {
    FieldSlot::resolve(<RouteMatrixEntity as EntitySchema>::MODEL, field)
        .expect("grouped field must resolve in route matrix entity model")
}

fn grouped_field_slots(fields: &[&str]) -> Vec<FieldSlot> {
    fields
        .iter()
        .map(|field| grouped_field_slot(field))
        .collect()
}

fn initial_scalar_continuation_context() -> ScalarContinuationContext {
    ScalarContinuationContext::initial()
}

fn build_load_route_plan_for_entity<E>(
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
) -> Result<crate::db::executor::ExecutionPlan, crate::error::InternalError>
where
    E: EntitySchema,
{
    build_load_route_plan_for_entity_with_probe_hint::<E>(plan, continuation, None)
}

fn build_load_route_plan_for_entity_with_probe_hint<E>(
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    probe_fetch_hint: Option<usize>,
) -> Result<crate::db::executor::ExecutionPlan, crate::error::InternalError>
where
    E: EntitySchema,
{
    build_execution_route_plan_for_load_with_model(E::MODEL, plan, continuation, probe_fetch_hint)
}

fn build_mutation_route_plan_for_entity<E>(
    plan: &AccessPlannedQuery,
) -> Result<crate::db::executor::ExecutionPlan, crate::error::InternalError>
where
    E: EntitySchema,
{
    build_execution_route_plan_for_mutation_with_model(E::MODEL, plan)
}

mod aggregate_matrix;
mod budget_matrix;
mod capability_matrix;
mod field_extrema_matrix;
mod load_matrix;
mod mutation_matrix;
mod precedence_matrix;
