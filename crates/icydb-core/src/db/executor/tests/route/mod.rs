use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        cursor::CursorBoundary,
        executor::{
            aggregate_model::{AggregateFoldMode, AggregateKind, AggregateSpec},
            load::LoadExecutor,
            route::{
                AGGREGATE_FAST_PATH_ORDER, ContinuationMode, ExecutionMode, FastPathOrder,
                FieldExtremaIneligibilityReason, LOAD_FAST_PATH_ORDER, MUTATION_FAST_PATH_ORDER,
                route_capability_flag_count_guard, route_execution_mode_case_count_guard,
            },
        },
        query::{
            ReadConsistency,
            intent::{DeleteSpec, QueryMode},
            plan::{AccessPlannedQuery, Direction, OrderDirection, OrderSpec, PageSpec},
            predicate::Predicate,
        },
    },
    model::{field::FieldKind, index::IndexModel},
    traits::Path,
    types::Ulid,
    value::Value,
};
use icydb_derive::FieldProjection;
use serde::{Deserialize, Serialize};
use std::ops::Bound;

const ROUTE_FEATURE_SOFT_BUDGET_DELTA: usize = 1;
const ROUTE_CAPABILITY_FLAG_BASELINE_0247: usize = 9;
const ROUTE_EXECUTION_MODE_CASE_BASELINE_0246: usize = 3;

fn assert_no_eligibility_helper_defs(file_label: &str, source: &str) {
    for line in source.lines() {
        let trimmed = line.trim_start();
        let defines_eligibility_helper = (trimmed.starts_with("fn is_")
            || trimmed.starts_with("const fn is_"))
            && trimmed.contains("eligible");
        assert!(
            !defines_eligibility_helper,
            "{file_label} must keep eligibility helpers route-owned (found: {trimmed})"
        );
    }
}

fn source_uses_direct_context_stream_construction(source: &str) -> bool {
    source.contains(".ordered_key_stream_from_access(")
        || source.contains(".ordered_key_stream_from_access_plan_with_index_range_anchor(")
}

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

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
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
    primary_key = "id",
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
) -> AccessPlannedQuery<Ulid> {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        ReadConsistency::MissingOk,
    );
    plan.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), direction),
            ("id".to_string(), direction),
        ],
    });
    plan.page = Some(PageSpec {
        limit: Some(4),
        offset,
    });
    plan.distinct = distinct;

    plan
}

mod aggregate;
mod budget;
mod capability;
mod field_extrema;
mod load;
mod mutation;
mod planner_capability;
mod planner_fast_path;
mod planner_mode;
mod precedence;
