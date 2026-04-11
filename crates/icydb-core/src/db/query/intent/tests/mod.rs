//! Module: db::query::intent::tests
//! Covers query-intent builder, planning, and explain-facing invariants.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod explain;
mod grouped;
mod verbose;

use super::*;
#[cfg(feature = "sql")]
use crate::db::query::plan::{
    AggregateKind,
    expr::{Expr, ProjectionField},
};
use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        cursor::GroupedContinuationToken,
        direction::Direction,
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::{
            builder::{FieldRef, count, count_by, exists, first, last, max, max_by, min, sum},
            explain::{
                ExplainAccessPath, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
            },
            expr::FilterExpr,
            intent::model::QueryModel,
            plan::{AccessPlannedQuery, LogicalPlan, OrderDirection, OrderSpec, ScalarPlan},
        },
    },
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel},
        index::{IndexExpression, IndexKeyItem, IndexModel},
    },
    testing::entity_model_from_static,
    traits::{EntitySchema, FieldProjection, FieldValue, Path},
    types::{Date, Duration, Timestamp, Ulid, Unit},
    value::{Value, ValueEnum},
};
use icydb_derive::FieldProjection;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// Helper for intent tests that need the typed model snapshot.
fn basic_model() -> &'static EntityModel {
    <PlanEntity as EntitySchema>::MODEL
}

fn verbose_diagnostics_lines(verbose: &str) -> Vec<String> {
    verbose
        .lines()
        .filter(|line| line.starts_with("diag."))
        .map(ToOwned::to_owned)
        .collect()
}

fn verbose_diagnostics_map(verbose: &str) -> BTreeMap<String, String> {
    let mut diagnostics = BTreeMap::new();
    for line in verbose_diagnostics_lines(verbose) {
        let Some((key, value)) = line.split_once('=') else {
            panic!("diagnostic line must contain '=': {line}");
        };
        diagnostics.insert(key.to_string(), value.to_string());
    }

    diagnostics
}

fn explain_execution_contains_node_type(
    descriptor: &ExplainExecutionNodeDescriptor,
    node_type: ExplainExecutionNodeType,
) -> bool {
    if descriptor.node_type() == node_type {
        return true;
    }

    descriptor
        .children()
        .iter()
        .any(|child| explain_execution_contains_node_type(child, node_type))
}

fn assert_expression_access_choice_selected(
    diagnostics: &BTreeMap<String, String>,
    expected_choice: &str,
) {
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&expected_choice.to_string()),
        "access-choice must select the same expression index chosen by planner access lowering",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"single_candidate".to_string()),
        "expression lookup parity expects deterministic single-candidate selection",
    );
}

fn query_error_is_group_plan_error(
    err: &QueryError,
    predicate: impl FnOnce(&crate::db::query::plan::validate::GroupPlanError) -> bool,
) -> bool {
    let QueryError::Plan(plan_err) = err else {
        return false;
    };

    match plan_err.as_ref() {
        crate::db::query::plan::PlanError::User(inner) => match inner.as_ref() {
            crate::db::query::plan::PlanUserError::Group(inner) => predicate(inner.as_ref()),
            _ => false,
        },
        crate::db::query::plan::PlanError::Policy(inner) => match inner.as_ref() {
            crate::db::query::plan::PlanPolicyError::Group(inner) => predicate(inner.as_ref()),
            crate::db::query::plan::PlanPolicyError::Policy(_) => false,
        },
        crate::db::query::plan::PlanError::Cursor(_) => false,
    }
}

fn query_error_is_policy_plan_error(
    err: &QueryError,
    predicate: impl FnOnce(&crate::db::query::plan::validate::PolicyPlanError) -> bool,
) -> bool {
    let QueryError::Plan(plan_err) = err else {
        return false;
    };

    match plan_err.as_ref() {
        crate::db::query::plan::PlanError::Policy(inner) => match inner.as_ref() {
            crate::db::query::plan::PlanPolicyError::Policy(inner) => predicate(inner.as_ref()),
            crate::db::query::plan::PlanPolicyError::Group(_) => false,
        },
        crate::db::query::plan::PlanError::User(_)
        | crate::db::query::plan::PlanError::Cursor(_) => false,
    }
}

fn query_error_is_order_plan_error(
    err: &QueryError,
    predicate: impl FnOnce(&crate::db::query::plan::validate::OrderPlanError) -> bool,
) -> bool {
    let QueryError::Plan(plan_err) = err else {
        return false;
    };

    match plan_err.as_ref() {
        crate::db::query::plan::PlanError::User(inner) => match inner.as_ref() {
            crate::db::query::plan::PlanUserError::Order(inner) => predicate(inner.as_ref()),
            _ => false,
        },
        crate::db::query::plan::PlanError::Policy(_)
        | crate::db::query::plan::PlanError::Cursor(_) => false,
    }
}

fn query_error_is_predicate_validation_error(
    err: &QueryError,
    predicate: impl FnOnce(&crate::db::schema::ValidateError) -> bool,
) -> bool {
    let QueryError::Plan(plan_err) = err else {
        return false;
    };

    match plan_err.as_ref() {
        crate::db::query::plan::PlanError::User(inner) => match inner.as_ref() {
            crate::db::query::plan::PlanUserError::PredicateInvalid(inner) => {
                predicate(inner.as_ref())
            }
            _ => false,
        },
        crate::db::query::plan::PlanError::Policy(_)
        | crate::db::query::plan::PlanError::Cursor(_) => false,
    }
}

// Test-only entity to compare typed vs model planning without schema macros.
#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PlanEntity {
    id: Ulid,
    name: String,
}

static MAP_PLAN_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated(
        "attributes",
        FieldKind::Map {
            key: &FieldKind::Text,
            value: &FieldKind::Uint,
        },
    ),
];
static MAP_PLAN_INDEXES: [&IndexModel; 0] = [];
static MAP_PLAN_MODEL: EntityModel = entity_model_from_static(
    "intent_tests::MapPlanEntity",
    "MapPlanEntity",
    &MAP_PLAN_FIELDS[0],
    0,
    &MAP_PLAN_FIELDS,
    &MAP_PLAN_INDEXES,
);

static ENUM_PLAN_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated(
        "stage",
        FieldKind::Enum {
            path: "intent_tests::Stage",
            variants: &[],
        },
    ),
];
static ENUM_PLAN_INDEXES: [&IndexModel; 0] = [];
static ENUM_PLAN_MODEL: EntityModel = entity_model_from_static(
    "intent_tests::EnumPlanEntity",
    "EnumPlanEntity",
    &ENUM_PLAN_FIELDS[0],
    0,
    &ENUM_PLAN_FIELDS,
    &ENUM_PLAN_INDEXES,
);

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct PlanSingleton {
    id: Unit,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PlanSimpleEntity {
    id: Ulid,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PlanNumericEntity {
    id: Ulid,
    rank: i32,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PlanPushdownEntity {
    id: Ulid,
    group: u32,
    rank: u32,
    label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PlanUniqueRangeEntity {
    id: Ulid,
    code: u32,
    label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PlanTextPrefixEntity {
    id: Ulid,
    label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PlanPhaseEntity {
    id: Ulid,
    tags: Vec<u32>,
    label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PlanExpressionCasefoldEntity {
    id: Ulid,
    email: String,
    label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PlanDeterministicChoiceEntity {
    id: Ulid,
    tier: String,
    handle: String,
    label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PlanDeterministicRangeEntity {
    id: Ulid,
    tier: String,
    score: u32,
    handle: String,
    label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PlanOrderOnlyChoiceEntity {
    id: Ulid,
    alpha: String,
    beta: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PlanTemporalBoundaryEntity {
    id: Ulid,
    occurred_on: Date,
    occurred_at: Timestamp,
    elapsed: Duration,
}

impl FieldProjection for PlanSingleton {
    fn get_value_by_index(&self, index: usize) -> Option<Value> {
        match index {
            0 => Some(self.id.to_value()),
            _ => None,
        }
    }
}

crate::test_canister! {
    ident = PlanCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanSimpleEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PlanSimpleEntity",
    entity_tag = crate::testing::SIMPLE_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Ulid)],
    indexes = [],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PlanEntity",
    entity_tag = crate::testing::PLAN_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
    ],
    indexes = [],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanSingleton,
    id = Unit,
    id_field = id,
    singleton = true,
    entity_name = "PlanSingleton",
    entity_tag = crate::testing::PLAN_SINGLETON_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Unit),
    ],
    indexes = [],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanNumericEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PlanNumericEntity",
    entity_tag = crate::testing::PLAN_NUMERIC_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("rank", FieldKind::Int),
    ],
    indexes = [],
    store = PlanDataStore,
    canister = PlanCanister,
}

static PLAN_PUSHDOWN_INDEX_FIELDS: [&str; 2] = ["group", "rank"];
static PLAN_PUSHDOWN_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "group_rank",
    PlanDataStore::PATH,
    &PLAN_PUSHDOWN_INDEX_FIELDS,
    false,
)];

static PLAN_UNIQUE_RANGE_INDEX_FIELDS: [&str; 1] = ["code"];
static PLAN_UNIQUE_RANGE_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "code_unique",
    PlanDataStore::PATH,
    &PLAN_UNIQUE_RANGE_INDEX_FIELDS,
    true,
)];

static PLAN_TEXT_PREFIX_INDEX_FIELDS: [&str; 1] = ["label"];
static PLAN_TEXT_PREFIX_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "label",
    PlanDataStore::PATH,
    &PLAN_TEXT_PREFIX_INDEX_FIELDS,
    false,
)];

static PLAN_EXPRESSION_CASEFOLD_INDEX_FIELDS: [&str; 1] = ["email"];
static PLAN_EXPRESSION_CASEFOLD_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
static PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_key_items(
        "email_expr",
        PlanDataStore::PATH,
        &PLAN_EXPRESSION_CASEFOLD_INDEX_FIELDS,
        &PLAN_EXPRESSION_CASEFOLD_KEY_ITEMS,
        false,
    )];
static PLAN_DETERMINISTIC_CHOICE_LABEL_INDEX_FIELDS: [&str; 2] = ["tier", "label"];
static PLAN_DETERMINISTIC_CHOICE_HANDLE_INDEX_FIELDS: [&str; 2] = ["tier", "handle"];
static PLAN_DETERMINISTIC_CHOICE_INDEX_MODELS: [IndexModel; 2] = [
    IndexModel::generated(
        "a_tier_label_idx",
        PlanDataStore::PATH,
        &PLAN_DETERMINISTIC_CHOICE_LABEL_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated(
        "z_tier_handle_idx",
        PlanDataStore::PATH,
        &PLAN_DETERMINISTIC_CHOICE_HANDLE_INDEX_FIELDS,
        false,
    ),
];
static PLAN_DETERMINISTIC_RANGE_HANDLE_INDEX_FIELDS: [&str; 3] = ["tier", "score", "handle"];
static PLAN_DETERMINISTIC_RANGE_LABEL_INDEX_FIELDS: [&str; 3] = ["tier", "score", "label"];
static PLAN_DETERMINISTIC_RANGE_INDEX_MODELS: [IndexModel; 2] = [
    IndexModel::generated(
        "a_tier_score_handle_idx",
        PlanDataStore::PATH,
        &PLAN_DETERMINISTIC_RANGE_HANDLE_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated(
        "z_tier_score_label_idx",
        PlanDataStore::PATH,
        &PLAN_DETERMINISTIC_RANGE_LABEL_INDEX_FIELDS,
        false,
    ),
];
static PLAN_ORDER_ONLY_CHOICE_BETA_INDEX_FIELDS: [&str; 1] = ["beta"];
static PLAN_ORDER_ONLY_CHOICE_ALPHA_INDEX_FIELDS: [&str; 1] = ["alpha"];
static PLAN_ORDER_ONLY_CHOICE_INDEX_MODELS: [IndexModel; 2] = [
    IndexModel::generated(
        "a_beta_idx",
        PlanDataStore::PATH,
        &PLAN_ORDER_ONLY_CHOICE_BETA_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated(
        "z_alpha_idx",
        PlanDataStore::PATH,
        &PLAN_ORDER_ONLY_CHOICE_ALPHA_INDEX_FIELDS,
        false,
    ),
];

static PLAN_PHASE_TAG_KIND: FieldKind = FieldKind::Uint;

crate::test_entity_schema! {
    ident = PlanPushdownEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PlanPushdownEntity",
    entity_tag = crate::testing::PUSHDOWN_PARITY_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("group", FieldKind::Uint),
        ("rank", FieldKind::Uint),
        ("label", FieldKind::Text),
    ],
    indexes = [&PLAN_PUSHDOWN_INDEX_MODELS[0]],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanUniqueRangeEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PlanUniqueRangeEntity",
    entity_tag = crate::testing::UNIQUE_INDEX_RANGE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("code", FieldKind::Uint),
        ("label", FieldKind::Text),
    ],
    indexes = [&PLAN_UNIQUE_RANGE_INDEX_MODELS[0]],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanTextPrefixEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PlanTextPrefixEntity",
    entity_tag = crate::testing::TEXT_PREFIX_PARITY_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("label", FieldKind::Text),
    ],
    indexes = [&PLAN_TEXT_PREFIX_INDEX_MODELS[0]],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanPhaseEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PlanPhaseEntity",
    entity_tag = crate::testing::PHASE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tags", FieldKind::List(&PLAN_PHASE_TAG_KIND)),
        ("label", FieldKind::Text),
    ],
    indexes = [],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanExpressionCasefoldEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PlanExpressionCasefoldEntity",
    entity_tag = crate::testing::PLAN_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("email", FieldKind::Text),
        ("label", FieldKind::Text),
    ],
    indexes = [&PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS[0]],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanDeterministicChoiceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PlanDeterministicChoiceEntity",
    entity_tag = crate::testing::PLAN_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tier", FieldKind::Text),
        ("handle", FieldKind::Text),
        ("label", FieldKind::Text),
    ],
    indexes = [
        &PLAN_DETERMINISTIC_CHOICE_INDEX_MODELS[0],
        &PLAN_DETERMINISTIC_CHOICE_INDEX_MODELS[1],
    ],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanDeterministicRangeEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PlanDeterministicRangeEntity",
    entity_tag = crate::testing::PLAN_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tier", FieldKind::Text),
        ("score", FieldKind::Uint),
        ("handle", FieldKind::Text),
        ("label", FieldKind::Text),
    ],
    indexes = [
        &PLAN_DETERMINISTIC_RANGE_INDEX_MODELS[0],
        &PLAN_DETERMINISTIC_RANGE_INDEX_MODELS[1],
    ],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanOrderOnlyChoiceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PlanOrderOnlyChoiceEntity",
    entity_tag = crate::testing::PLAN_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("alpha", FieldKind::Text),
        ("beta", FieldKind::Text),
    ],
    indexes = [
        &PLAN_ORDER_ONLY_CHOICE_INDEX_MODELS[0],
        &PLAN_ORDER_ONLY_CHOICE_INDEX_MODELS[1],
    ],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanTemporalBoundaryEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PlanTemporalBoundaryEntity",
    entity_tag = crate::testing::TEMPORAL_BOUNDARY_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("occurred_on", FieldKind::Date),
        ("occurred_at", FieldKind::Timestamp),
        ("elapsed", FieldKind::Duration),
    ],
    indexes = [],
    store = PlanDataStore,
    canister = PlanCanister,
}

#[test]
fn intent_rejects_by_ids_with_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .by_ids([Ulid::generate()])
        .filter(Predicate::True);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::ByIdsWithPredicate))
    ));
}

#[test]
fn intent_rejects_only_with_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .only(Ulid::generate())
        .filter(Predicate::True);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::OnlyWithPredicate))
    ));
}

#[test]
fn intent_rejects_delete_limit_without_order() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .delete()
        .limit(1);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::PlanShape(
            crate::db::query::plan::validate::PolicyPlanError::DeleteLimitRequiresOrder
        )))
    ));
}

#[test]
fn intent_rejects_delete_offset_modifier() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .delete()
        .offset(10);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::PlanShape(
            crate::db::query::plan::validate::PolicyPlanError::DeletePlanWithOffset
        )))
    ));
}

#[test]
fn intent_rejects_offset_then_delete_shape() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .offset(10)
        .delete();

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::PlanShape(
            crate::db::query::plan::validate::PolicyPlanError::DeletePlanWithOffset
        )))
    ));
}

#[test]
fn delete_query_rejects_grouped_shape_during_intent_validation() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .group_by("name")
        .expect("group field should resolve")
        .plan()
        .expect_err("delete queries must reject grouped logical shape during intent validation");

    assert!(matches!(
        err,
        QueryError::Intent(IntentError::PlanShape(
            crate::db::query::plan::validate::PolicyPlanError::DeletePlanWithGrouping
        ))
    ));
}

#[test]
fn load_limit_without_order_rejects_unordered_pagination() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .limit(1)
        .plan()
        .expect_err("limit without order must fail");

    assert!(query_error_is_policy_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::PolicyPlanError::UnorderedPagination
        )
    }));
}

#[test]
fn load_rejects_duplicate_non_primary_order_field() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .order_by_desc("name")
        .limit(1)
        .plan()
        .expect_err("duplicate non-primary order field must fail");

    assert!(query_error_is_order_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::OrderPlanError::DuplicateOrderField { field }
                if field == "name"
        )
    }));
}

#[test]
fn load_offset_without_order_rejects_unordered_pagination() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .offset(1)
        .plan()
        .expect_err("offset without order must fail");

    assert!(query_error_is_policy_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::PolicyPlanError::UnorderedPagination
        )
    }));
}

#[test]
fn load_limit_and_offset_without_order_rejects_unordered_pagination() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .limit(10)
        .offset(2)
        .plan()
        .expect_err("limit+offset without order must fail");

    assert!(query_error_is_policy_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::PolicyPlanError::UnorderedPagination
        )
    }));
}

#[test]
fn load_ordered_pagination_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .limit(10)
        .offset(2)
        .plan()
        .expect("ordered pagination should plan");
}

#[test]
fn ordered_plan_appends_primary_key_tie_break() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .plan()
        .expect("ordered plan should build")
        .into_inner();
    let order = plan
        .scalar_plan()
        .order
        .as_ref()
        .expect("ordered query should carry order spec");

    assert_eq!(
        order.fields,
        vec![
            ("name".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
        "canonical order should append primary key as terminal tie-break"
    );
}

#[test]
fn ordered_plan_moves_primary_key_to_terminal_position() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("id")
        .order_by("name")
        .plan()
        .expect("ordered plan should build")
        .into_inner();
    let order = plan
        .scalar_plan()
        .order
        .as_ref()
        .expect("ordered query should carry order spec");

    assert_eq!(
        order.fields,
        vec![
            ("name".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Desc),
        ],
        "canonical order must keep exactly one terminal PK tie-break with requested direction"
    );
}

#[test]
fn intent_rejects_empty_order_spec() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .order_spec(OrderSpec { fields: Vec::new() });

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::PlanShape(
            crate::db::query::plan::validate::PolicyPlanError::EmptyOrderSpec
        )))
    ));
}

#[test]
fn intent_rejects_conflicting_key_access() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .by_id(Ulid::generate())
        .by_ids([Ulid::generate()]);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::KeyAccessConflict))
    ));
}

#[test]
fn typed_by_ids_matches_by_id_access() {
    let key = Ulid::generate();

    let by_id = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .by_id(key)
        .plan()
        .expect("by_id plan")
        .into_inner();
    let by_ids = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .by_ids([key])
        .plan()
        .expect("by_ids plan")
        .into_inner();

    assert_eq!(by_id, by_ids);
}

#[test]
fn by_id_limit_one_without_order_simplifies_paging_shape() {
    let key = Ulid::generate();
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .by_id(key)
        .limit(1)
        .plan()
        .expect("by_id + limit(1) plan should build")
        .into_inner();

    assert!(
        plan.scalar_plan().page.is_none(),
        "by_id + limit(1) with no offset should remove redundant page metadata"
    );
    assert!(
        matches!(
            plan.access,
            AccessPlan::Path(path)
                if matches!(path.as_ref(), AccessPath::ByKey(by_key) if *by_key == Value::Ulid(key))
        ),
        "by_id + limit(1) should keep exact ByKey access",
    );
}

#[test]
fn by_key_access_strips_redundant_primary_key_equality_predicate() {
    let key = Ulid::generate();
    let model_plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .by_id(key)
        .filter(FieldRef::new("id").eq(key))
        .build_plan_model()
        .expect("model by_id + id == literal plan should build");
    let AccessPlannedQuery {
        logical,
        access,
        projection_selection: _projection_selection,
        ..
    } = model_plan;
    let typed_plan = AccessPlannedQuery::from_parts(logical, access);

    assert!(
        typed_plan.scalar_plan().predicate.is_none(),
        "by_id + id == literal should strip redundant scalar predicate"
    );
    assert!(
        matches!(
            typed_plan.access,
            AccessPlan::Path(path)
                if matches!(path.as_ref(), AccessPath::ByKey(by_key) if *by_key == Value::Ulid(key))
        ),
        "redundant predicate stripping must keep the exact ByKey path"
    );
}

#[test]
fn by_keys_access_strips_redundant_primary_key_in_predicate() {
    let key1 = Ulid::from_u128(9_811);
    let key2 = Ulid::from_u128(9_813);
    let model_plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![
                Value::Ulid(key2),
                Value::Ulid(key1),
                Value::Ulid(key2),
            ]),
            CoercionId::Strict,
        )))
        .build_plan_model()
        .expect("model id IN literal-set plan should build");
    let AccessPlannedQuery {
        logical,
        access,
        projection_selection: _projection_selection,
        ..
    } = model_plan;
    let typed_plan = AccessPlannedQuery::from_parts(logical, access);

    assert!(
        typed_plan.scalar_plan().predicate.is_none(),
        "exact primary-key IN sets should strip redundant scalar predicates",
    );
    assert!(
        matches!(
            typed_plan.access,
            AccessPlan::Path(path)
                if matches!(
                    path.as_ref(),
                    AccessPath::ByKeys(keys)
                        if keys == &vec![Value::Ulid(key1), Value::Ulid(key2)]
                )
        ),
        "redundant predicate stripping must keep the canonical ByKeys path",
    );
}

#[test]
fn key_range_access_strips_redundant_primary_key_half_open_bounds() {
    let lower = Ulid::from_u128(9_811);
    let upper = Ulid::from_u128(9_813);
    let model_plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "id",
                CompareOp::Gte,
                Value::Ulid(lower),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "id",
                CompareOp::Lt,
                Value::Ulid(upper),
                CoercionId::Strict,
            )),
        ]))
        .build_plan_model()
        .expect("model id half-open range plan should build");
    let AccessPlannedQuery {
        logical,
        access,
        projection_selection: _projection_selection,
        ..
    } = model_plan;
    let typed_plan = AccessPlannedQuery::from_parts(logical, access);

    assert!(
        typed_plan.scalar_plan().predicate.is_none(),
        "exact primary-key half-open ranges should strip redundant scalar predicates",
    );
    assert!(
        matches!(
            typed_plan.access,
            AccessPlan::Path(path)
                if matches!(
                    path.as_ref(),
                    AccessPath::KeyRange { start, end }
                        if *start == Value::Ulid(lower) && *end == Value::Ulid(upper)
                )
        ),
        "redundant predicate stripping must keep the exact KeyRange path",
    );
}

#[test]
fn singleton_only_uses_default_key() {
    let plan = Query::<PlanSingleton>::new(MissingRowPolicy::Ignore)
        .only()
        .plan()
        .expect("singleton plan")
        .into_inner();

    assert!(matches!(
        plan.access,
        AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::ByKey(Value::Unit))
    ));
}

#[test]
fn build_plan_model_full_scan_without_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore);
    let plan = intent.build_plan_model().expect("model plan should build");

    assert!(matches!(
        plan.access,
        AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::FullScan)
    ));
}

#[test]
fn build_plan_model_limit_zero_lowers_to_empty_by_keys() {
    let plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .order_by("id")
        .limit(0)
        .build_plan_model()
        .expect("ordered limit(0) plan should build");

    assert!(matches!(
        &plan.access,
        AccessPlan::Path(path)
            if matches!(path.as_ref(), AccessPath::ByKeys(keys) if keys.is_empty())
    ));
}

#[test]
fn build_plan_model_constant_false_lowers_to_empty_by_keys() {
    let plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter(Predicate::False)
        .build_plan_model()
        .expect("constant false plan should build");

    assert!(
        matches!(
            &plan.access,
            AccessPlan::Path(path)
                if matches!(path.as_ref(), AccessPath::ByKeys(keys) if keys.is_empty())
        ),
        "constant-false filter should lower to empty by-keys access"
    );
    assert!(
        matches!(plan.scalar_plan().predicate, Some(Predicate::False)),
        "constant-false filter should remain visible in logical predicate for explain stability"
    );
}

#[test]
fn build_plan_model_constant_true_elides_logical_predicate() {
    let plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter(Predicate::True)
        .build_plan_model()
        .expect("constant true plan should build");

    assert!(
        plan.scalar_plan().predicate.is_none(),
        "constant-true filter should be folded away before logical planning"
    );
    assert!(
        matches!(
            &plan.access,
            AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::FullScan)
        ),
        "constant-true filter should not force access routing changes",
    );
}

#[test]
fn typed_plan_matches_model_plan_for_same_intent() {
    let predicate = FieldRef::new("id").eq(Ulid::default());

    let model_intent = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("name")
        .limit(10)
        .offset(2);

    let model_plan = model_intent.build_plan_model().expect("model plan");
    let AccessPlannedQuery {
        logical: model_logical,
        access: model_access,
        projection_selection: _projection_selection,
        ..
    } = model_plan;
    let LogicalPlan::Scalar(ScalarPlan {
        mode,
        predicate: plan_predicate,
        order,
        distinct,
        delete_limit,
        page,
        consistency,
    }) = model_logical
    else {
        panic!("typed/model intent parity test expects scalar logical plan");
    };

    let mut model_as_typed = AccessPlannedQuery::from_parts(
        LogicalPlan::Scalar(ScalarPlan {
            mode,
            predicate: plan_predicate,
            order,
            distinct,
            delete_limit,
            page,
            consistency,
        }),
        model_access,
    );
    model_as_typed.finalize_planner_route_profile_for_model(PlanEntity::MODEL);
    model_as_typed
        .finalize_static_planning_shape_for_model(PlanEntity::MODEL)
        .expect("model-backed parity plan should freeze static planning shape");

    let typed_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("name")
        .limit(10)
        .offset(2)
        .plan()
        .expect("typed plan")
        .into_inner();

    assert_eq!(model_as_typed, typed_plan);
}

#[test]
fn query_distinct_defaults_to_false() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .plan()
        .expect("typed plan")
        .into_inner();

    assert!(
        !plan.scalar_plan().distinct,
        "distinct should default to false for new query intents"
    );
}

#[test]
fn query_distinct_sets_logical_plan_flag() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .distinct()
        .plan()
        .expect("typed plan")
        .into_inner();

    assert!(
        plan.scalar_plan().distinct,
        "distinct should be true when query intent enables distinct"
    );
}

#[cfg(feature = "sql")]
#[test]
fn compiled_query_projection_spec_lowers_scalar_fields_in_model_order() {
    let compiled = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .plan()
        .expect("plan should build");
    let field_names = compiled
        .projection_spec()
        .fields()
        .map(|field| match field {
            ProjectionField::Scalar {
                expr: Expr::Field(field),
                alias: None,
            } => field.as_str().to_string(),
            other @ ProjectionField::Scalar { .. } => {
                panic!("scalar projection should lower to plain field exprs: {other:?}")
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(field_names, vec!["id".to_string(), "name".to_string()]);
}

#[cfg(feature = "sql")]
#[test]
fn compiled_query_projection_spec_lowers_grouped_shape_in_declaration_order() {
    let compiled = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group by should resolve")
        .aggregate(count())
        .plan()
        .expect("grouped plan should build");
    let projection = compiled.projection_spec();
    let fields = projection.fields().collect::<Vec<_>>();
    assert_eq!(
        fields.len(),
        2,
        "grouped projection should include key + aggregate"
    );

    match fields[0] {
        ProjectionField::Scalar {
            expr: Expr::Field(field),
            alias: None,
        } => assert_eq!(field.as_str(), "name"),
        other @ ProjectionField::Scalar { .. } => {
            panic!("first grouped projection field should be grouped key expr: {other:?}")
        }
    }
    match fields[1] {
        ProjectionField::Scalar {
            expr: Expr::Aggregate(aggregate),
            alias: None,
        } => {
            assert_eq!(aggregate.kind(), AggregateKind::Count);
            assert_eq!(aggregate.target_field(), None);
            assert!(!aggregate.is_distinct());
        }
        other @ ProjectionField::Scalar { .. } => {
            panic!("second grouped projection field should be grouped aggregate expr: {other:?}")
        }
    }
}

#[cfg(feature = "sql")]
#[test]
fn compiled_query_projection_spec_preserves_global_distinct_aggregate_semantics() {
    let compiled = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(count_by("name").distinct())
        .plan()
        .expect("global distinct grouped plan should build");
    let projection = compiled.projection_spec();
    let fields = projection.fields().collect::<Vec<_>>();
    assert_eq!(
        fields.len(),
        1,
        "global distinct grouped projection should only include one aggregate"
    );

    match fields[0] {
        ProjectionField::Scalar {
            expr: Expr::Aggregate(aggregate),
            alias: None,
        } => {
            assert_eq!(aggregate.kind(), AggregateKind::Count);
            assert_eq!(aggregate.target_field(), Some("name"));
            assert!(aggregate.is_distinct());
        }
        other @ ProjectionField::Scalar { .. } => {
            panic!("global distinct projection should lower to aggregate expr: {other:?}")
        }
    }
}

#[test]
fn build_plan_model_rejects_map_field_predicates_before_planning() {
    let intent = QueryModel::<Ulid>::new(&MAP_PLAN_MODEL, MissingRowPolicy::Ignore).filter(
        Predicate::Compare(ComparePredicate::with_coercion(
            "attributes",
            CompareOp::Eq,
            Value::Map(Vec::new()),
            crate::db::predicate::CoercionId::Strict,
        )),
    );

    let err = intent
        .build_plan_model()
        .expect_err("map field predicates must be rejected before planning");
    assert!(query_error_is_predicate_validation_error(&err, |inner| {
        matches!(
            inner,
            crate::db::schema::ValidateError::UnsupportedQueryFeature(
                crate::db::predicate::UnsupportedQueryFeature::MapPredicate { field }
            ) if field == "attributes"
        )
    }));
}

#[test]
fn filter_expr_resolves_loose_enum_stage_filters() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::Eq,
        Value::Enum(ValueEnum::loose("Active")),
        crate::db::predicate::CoercionId::Strict,
    ));

    let intent = QueryModel::<Ulid>::new(&ENUM_PLAN_MODEL, MissingRowPolicy::Ignore)
        .filter_expr(FilterExpr(predicate))
        .expect("filter expr should lower");
    let plan = intent.build_plan_model().expect("plan should build");

    let Some(Predicate::Compare(cmp)) = plan.scalar_plan().predicate.as_ref() else {
        panic!("expected compare predicate");
    };
    let Value::Enum(stage) = &cmp.value else {
        panic!("expected enum literal");
    };
    assert_eq!(stage.path(), Some("intent_tests::Stage"));
}

#[test]
fn filter_expr_rejects_wrong_strict_enum_path() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::Eq,
        Value::Enum(ValueEnum::new("Active", Some("wrong::Stage"))),
        crate::db::predicate::CoercionId::Strict,
    ));

    let err = QueryModel::<Ulid>::new(&ENUM_PLAN_MODEL, MissingRowPolicy::Ignore)
        .filter_expr(FilterExpr(predicate))
        .expect_err("strict enum with wrong path should fail");
    assert!(matches!(
        err,
        QueryError::Validate(err)
            if matches!(
                err.as_ref(),
                crate::db::schema::ValidateError::InvalidLiteral {
            field,
            ..
                } if field == "stage"
            )
    ));
}

#[test]
fn direct_stage_filter_resolves_loose_enum_path() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::Eq,
        Value::Enum(ValueEnum::loose("Draft")),
        crate::db::predicate::CoercionId::Strict,
    ));

    let plan = QueryModel::<Ulid>::new(&ENUM_PLAN_MODEL, MissingRowPolicy::Ignore)
        .filter(predicate)
        .build_plan_model()
        .expect("direct filter should build");
    let Some(Predicate::Compare(cmp)) = plan.scalar_plan().predicate.as_ref() else {
        panic!("expected compare predicate");
    };
    let Value::Enum(stage) = &cmp.value else {
        panic!("expected enum literal");
    };
    assert_eq!(stage.path(), Some("intent_tests::Stage"));
}
