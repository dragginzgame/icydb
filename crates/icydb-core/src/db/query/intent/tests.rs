//! Module: db::query::intent::tests
//! Responsibility: module-local ownership and contracts for db::query::intent::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

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
    FieldModel::new("id", FieldKind::Ulid),
    FieldModel::new(
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
    &MAP_PLAN_FIELDS,
    &MAP_PLAN_INDEXES,
);

static ENUM_PLAN_FIELDS: [FieldModel; 2] = [
    FieldModel::new("id", FieldKind::Ulid),
    FieldModel::new(
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
static PLAN_PUSHDOWN_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
    "group_rank",
    PlanDataStore::PATH,
    &PLAN_PUSHDOWN_INDEX_FIELDS,
    false,
)];

static PLAN_UNIQUE_RANGE_INDEX_FIELDS: [&str; 1] = ["code"];
static PLAN_UNIQUE_RANGE_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
    "code_unique",
    PlanDataStore::PATH,
    &PLAN_UNIQUE_RANGE_INDEX_FIELDS,
    true,
)];

static PLAN_TEXT_PREFIX_INDEX_FIELDS: [&str; 1] = ["label"];
static PLAN_TEXT_PREFIX_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
    "label",
    PlanDataStore::PATH,
    &PLAN_TEXT_PREFIX_INDEX_FIELDS,
    false,
)];

static PLAN_EXPRESSION_CASEFOLD_INDEX_FIELDS: [&str; 1] = ["email"];
static PLAN_EXPRESSION_CASEFOLD_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
static PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new_with_key_items(
    "email_expr",
    PlanDataStore::PATH,
    &PLAN_EXPRESSION_CASEFOLD_INDEX_FIELDS,
    &PLAN_EXPRESSION_CASEFOLD_KEY_ITEMS,
    false,
)];
static PLAN_DETERMINISTIC_CHOICE_LABEL_INDEX_FIELDS: [&str; 2] = ["tier", "label"];
static PLAN_DETERMINISTIC_CHOICE_HANDLE_INDEX_FIELDS: [&str; 2] = ["tier", "handle"];
static PLAN_DETERMINISTIC_CHOICE_INDEX_MODELS: [IndexModel; 2] = [
    IndexModel::new(
        "a_tier_label_idx",
        PlanDataStore::PATH,
        &PLAN_DETERMINISTIC_CHOICE_LABEL_INDEX_FIELDS,
        false,
    ),
    IndexModel::new(
        "z_tier_handle_idx",
        PlanDataStore::PATH,
        &PLAN_DETERMINISTIC_CHOICE_HANDLE_INDEX_FIELDS,
        false,
    ),
];
static PLAN_DETERMINISTIC_RANGE_HANDLE_INDEX_FIELDS: [&str; 3] = ["tier", "score", "handle"];
static PLAN_DETERMINISTIC_RANGE_LABEL_INDEX_FIELDS: [&str; 3] = ["tier", "score", "label"];
static PLAN_DETERMINISTIC_RANGE_INDEX_MODELS: [IndexModel; 2] = [
    IndexModel::new(
        "a_tier_score_handle_idx",
        PlanDataStore::PATH,
        &PLAN_DETERMINISTIC_RANGE_HANDLE_INDEX_FIELDS,
        false,
    ),
    IndexModel::new(
        "z_tier_score_label_idx",
        PlanDataStore::PATH,
        &PLAN_DETERMINISTIC_RANGE_LABEL_INDEX_FIELDS,
        false,
    ),
];
static PLAN_ORDER_ONLY_CHOICE_BETA_INDEX_FIELDS: [&str; 1] = ["beta"];
static PLAN_ORDER_ONLY_CHOICE_ALPHA_INDEX_FIELDS: [&str; 1] = ["alpha"];
static PLAN_ORDER_ONLY_CHOICE_INDEX_MODELS: [IndexModel; 2] = [
    IndexModel::new(
        "a_beta_idx",
        PlanDataStore::PATH,
        &PLAN_ORDER_ONLY_CHOICE_BETA_INDEX_FIELDS,
        false,
    ),
    IndexModel::new(
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
fn grouped_load_limit_without_order_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("grouped pagination should use canonical grouped-key order");
}

#[test]
fn grouped_load_distinct_is_rejected_without_adjacency_eligibility() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .distinct()
        .plan()
        .expect_err("grouped distinct should be rejected until adjacency eligibility exists");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctAdjacencyEligibilityRequired
        )
    }));
}

#[test]
fn grouped_load_order_prefix_mismatch_is_rejected() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect_err("grouped order should be rejected when group keys are not the order prefix");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::OrderPrefixNotAlignedWithGroupKeys
        )
    }));
}

#[test]
fn grouped_load_order_prefix_alignment_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("grouped order should be accepted when grouped keys lead ORDER BY and LIMIT is explicit");
}

#[test]
fn grouped_load_order_without_limit_is_rejected() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .plan()
        .expect_err("grouped order should reject missing LIMIT");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::OrderRequiresLimit
        )
    }));
}

#[test]
fn grouped_load_distinct_count_terminal_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count().distinct())
        .plan()
        .expect("grouped distinct count terminal should plan in grouped v1");
}

#[test]
fn grouped_aggregate_builder_count_shape_matches_helper_terminal() {
    let helper_explain = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .plan()
        .expect("helper grouped count should plan")
        .into_inner()
        .explain();
    let builder_explain = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .plan()
        .expect("builder grouped count should plan")
        .into_inner()
        .explain();

    assert_eq!(
        helper_explain, builder_explain,
        "aggregate(count()) should preserve grouped count logical shape",
    );
}

#[test]
fn grouped_global_distinct_count_field_without_group_by_is_allowed() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .plan()
        .expect("global grouped count(distinct field) should plan");

    let Some(grouped) = plan.into_inner().grouped_plan().cloned() else {
        panic!("global grouped distinct field aggregate must compile to grouped logical plan");
    };
    assert!(
        grouped.group.group_fields.is_empty(),
        "global grouped distinct aggregate should use zero group keys"
    );
    assert_eq!(
        grouped.group.aggregates.len(),
        1,
        "global grouped distinct aggregate should declare exactly one terminal"
    );
    assert_eq!(
        grouped.group.aggregates[0].target_field(),
        Some("name"),
        "global grouped distinct count should preserve target field"
    );
    assert!(
        grouped.group.aggregates[0].distinct(),
        "global grouped distinct count should preserve DISTINCT modifier"
    );
}

#[test]
fn grouped_aggregate_builder_global_distinct_count_shape_matches_helper_terminal() {
    let helper_explain = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .plan()
        .expect("helper global count(distinct field) should plan")
        .into_inner()
        .explain();
    let builder_explain = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(count_by("name").distinct())
        .plan()
        .expect("builder global count(distinct field) should plan")
        .into_inner()
        .explain();

    assert_eq!(
        helper_explain, builder_explain,
        "aggregate(count_by(field).distinct()) should preserve global distinct-count logical shape",
    );
}

#[test]
fn grouped_aggregate_builder_global_distinct_sum_shape_matches_helper_terminal() {
    let helper_explain = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("rank").distinct())
        .plan()
        .expect("helper global sum(distinct field) should plan")
        .into_inner()
        .explain();
    let builder_explain = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(sum("rank").distinct())
        .plan()
        .expect("builder global sum(distinct field) should plan")
        .into_inner()
        .explain();

    assert_eq!(
        helper_explain, builder_explain,
        "aggregate(sum(field).distinct()) should preserve global distinct-sum logical shape",
    );
}

#[test]
fn grouped_aggregate_builder_fingerprint_parity_preserves_grouping_and_order_shape() {
    let helper_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("helper grouped count plan should build")
        .into_inner();
    let builder_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .limit(1)
        .plan()
        .expect("builder grouped count plan should build")
        .into_inner();

    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "helper and builder grouped count plans must have identical fingerprints",
    );
    assert_eq!(
        helper_plan.continuation_signature("intent::tests::PlanEntity"),
        builder_plan.continuation_signature("intent::tests::PlanEntity"),
        "helper and builder grouped count plans must have identical continuation signatures",
    );
}

#[test]
fn grouped_aggregate_builder_terminal_matrix_matches_helper_fingerprints() {
    for terminal in ["exists", "first", "last", "min", "max"] {
        let helper_plan = match terminal {
            "exists" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::exists())
                .limit(1)
                .plan()
                .expect("helper grouped exists plan should build")
                .into_inner(),
            "first" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::first())
                .limit(1)
                .plan()
                .expect("helper grouped first plan should build")
                .into_inner(),
            "last" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::last())
                .limit(1)
                .plan()
                .expect("helper grouped last plan should build")
                .into_inner(),
            "min" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::min())
                .limit(1)
                .plan()
                .expect("helper grouped min plan should build")
                .into_inner(),
            "max" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::max())
                .limit(1)
                .plan()
                .expect("helper grouped max plan should build")
                .into_inner(),
            _ => unreachable!("terminal matrix is fixed"),
        };
        let builder_plan = match terminal {
            "exists" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(exists())
                .limit(1)
                .plan()
                .expect("builder grouped exists plan should build")
                .into_inner(),
            "first" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(first())
                .limit(1)
                .plan()
                .expect("builder grouped first plan should build")
                .into_inner(),
            "last" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(last())
                .limit(1)
                .plan()
                .expect("builder grouped last plan should build")
                .into_inner(),
            "min" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(min())
                .limit(1)
                .plan()
                .expect("builder grouped min plan should build")
                .into_inner(),
            "max" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(max())
                .limit(1)
                .plan()
                .expect("builder grouped max plan should build")
                .into_inner(),
            _ => unreachable!("terminal matrix is fixed"),
        };

        assert_eq!(
            helper_plan.fingerprint(),
            builder_plan.fingerprint(),
            "terminal `{terminal}` helper/builder fingerprints must match",
        );
        assert_eq!(
            helper_plan.continuation_signature("intent::tests::PlanEntity"),
            builder_plan.continuation_signature("intent::tests::PlanEntity"),
            "terminal `{terminal}` helper/builder continuation signatures must match",
        );
    }
}

#[test]
fn grouped_aggregate_builder_fingerprint_parity_preserves_distinct_flag_shape() {
    let helper_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .limit(1)
        .plan()
        .expect("helper grouped global distinct count plan should build")
        .into_inner();
    let builder_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(count_by("name").distinct())
        .limit(1)
        .plan()
        .expect("builder grouped global distinct count plan should build")
        .into_inner();

    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "helper and builder global distinct-count plans must have identical fingerprints",
    );
    assert_eq!(
        helper_plan.continuation_signature("intent::tests::PlanEntity"),
        builder_plan.continuation_signature("intent::tests::PlanEntity"),
        "helper and builder global distinct-count plans must have identical continuation signatures",
    );
}

#[test]
fn grouped_aggregate_builder_fingerprint_parity_preserves_distinct_sum_shape() {
    let helper_plan = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("rank").distinct())
        .limit(1)
        .plan()
        .expect("helper grouped global distinct sum plan should build")
        .into_inner();
    let builder_plan = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(sum("rank").distinct())
        .limit(1)
        .plan()
        .expect("builder grouped global distinct sum plan should build")
        .into_inner();

    assert_eq!(
        helper_plan.explain().grouping,
        builder_plan.explain().grouping,
        "helper and builder global distinct-sum plans must have identical grouped projection shapes",
    );
    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "helper and builder global distinct-sum plans must have identical fingerprints",
    );
    assert_eq!(
        helper_plan.continuation_signature("intent::tests::PlanNumericEntity"),
        builder_plan.continuation_signature("intent::tests::PlanNumericEntity"),
        "helper and builder global distinct-sum plans must have identical continuation signatures",
    );
}

#[test]
fn grouped_aggregate_builder_fingerprint_parity_preserves_projection_order_shape() {
    let helper_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .aggregate(crate::db::max())
        .limit(1)
        .plan()
        .expect("helper grouped multi-aggregate plan should build")
        .into_inner();
    let builder_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .aggregate(max())
        .limit(1)
        .plan()
        .expect("builder grouped multi-aggregate plan should build")
        .into_inner();

    assert_eq!(
        helper_plan.explain().grouping,
        builder_plan.explain().grouping,
        "helper and builder grouped multi-aggregate projection shapes must match",
    );
    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "helper and builder grouped multi-aggregate plans must have identical fingerprints",
    );
}

#[test]
fn plan_hash_snapshot_is_stable_across_explain_surfaces() {
    // Phase 1: build one deterministic scalar query shape and capture baseline hash surfaces.
    let query = Query::<PlanSingleton>::new(MissingRowPolicy::Ignore).by_id(Unit);

    let baseline_hash = query
        .plan_hash_hex()
        .expect("baseline plan hash should build");
    let planned_hash = query
        .planned()
        .expect("planned query should build for hash parity")
        .plan_hash_hex();
    let compiled_hash = query
        .plan()
        .expect("compiled query should build for hash parity")
        .plan_hash_hex();

    // Phase 2: force logical + execution explain surfaces for the same query shape.
    let _logical_explain = query
        .explain()
        .expect("logical explain should build for plan-hash parity lock");
    let _execution_text = query
        .explain_execution_text()
        .expect("execution text explain should build for plan-hash parity lock");
    let _execution_json = query
        .explain_execution_json()
        .expect("execution json explain should build for plan-hash parity lock");
    let _execution_verbose = query
        .explain_execution_verbose()
        .expect("execution verbose explain should build for plan-hash parity lock");

    // Phase 3: re-read hash after explain rendering and lock deterministic parity.
    let hash_after_explain = query
        .plan_hash_hex()
        .expect("plan hash should still build after explain rendering");
    assert_eq!(
        baseline_hash, planned_hash,
        "planned-query plan hash must match query plan-hash surface",
    );
    assert_eq!(
        baseline_hash, compiled_hash,
        "compiled-query plan hash must match query plan-hash surface",
    );
    assert_eq!(
        baseline_hash, hash_after_explain,
        "explain rendering surfaces must not change semantic plan-hash identity",
    );
    assert_eq!(
        baseline_hash, "70679eb4a9281ecf55aced9a30b47110fdc4bd64160c76a4350b97a58594cab1",
        "plan-hash snapshot drifted; update only for intentional semantic identity changes",
    );
}

#[test]
fn explain_execution_verbose_reports_top_n_seek_hints() {
    let verbose = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("id")
        .offset(2)
        .limit(3)
        .explain_execution_verbose()
        .expect("top-n verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_eq!(
        diagnostics.get("diag.r.top_n_seek"),
        Some(&"fetch(6)".to_string()),
        "verbose execution explain should freeze top-n seek fetch diagnostics",
    );
    assert_eq!(
        diagnostics.get("diag.d.has_top_n_seek"),
        Some(&"true".to_string()),
        "descriptor diagnostics should report TopNSeek node presence",
    );
}

#[test]
fn expression_casefold_eq_access_and_execution_route_stay_in_parity() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::Eq,
        Value::Text("ALICE@EXAMPLE.COM".to_string()),
        CoercionId::TextCasefold,
    ));

    let explain = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain()
        .expect("expression eq explain should build");
    let ExplainAccessPath::IndexPrefix {
        name,
        fields,
        prefix_len,
        values,
    } = explain.access()
    else {
        panic!("expression eq should lower to index-prefix access");
    };
    assert_eq!(name, &PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS[0].name());
    assert_eq!(fields.as_slice(), ["email"]);
    assert_eq!(*prefix_len, 1);
    assert_eq!(
        values.as_slice(),
        [Value::Text("alice@example.com".to_string())]
    );

    let verbose = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain_execution_verbose()
        .expect("expression eq verbose explain should build");
    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_expression_access_choice_selected(&diagnostics, "IndexPrefix(email_expr)");

    let execution = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .explain_execution()
        .expect("expression eq execution explain should build");
    assert!(
        explain_execution_contains_node_type(&execution, ExplainExecutionNodeType::IndexPrefixScan),
        "execution route must preserve expression eq index-prefix route selection",
    );
}

#[test]
fn expression_casefold_in_access_and_execution_route_stay_in_parity() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::In,
        Value::List(vec![
            Value::Text("BOB@EXAMPLE.COM".to_string()),
            Value::Text("alice@example.com".to_string()),
            Value::Text("bob@example.com".to_string()),
        ]),
        CoercionId::TextCasefold,
    ));

    let explain = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain()
        .expect("expression IN explain should build");
    let ExplainAccessPath::IndexMultiLookup {
        name,
        fields,
        values,
    } = explain.access()
    else {
        panic!("expression IN should lower to index-multi-lookup access");
    };
    assert_eq!(name, &PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS[0].name());
    assert_eq!(fields.as_slice(), ["email"]);
    assert_eq!(
        values.as_slice(),
        [
            Value::Text("alice@example.com".to_string()),
            Value::Text("bob@example.com".to_string())
        ],
    );

    let verbose = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain_execution_verbose()
        .expect("expression IN verbose explain should build");
    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_expression_access_choice_selected(&diagnostics, "IndexMultiLookup(email_expr)");

    let execution = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .explain_execution()
        .expect("expression IN execution explain should build");
    assert!(
        explain_execution_contains_node_type(
            &execution,
            ExplainExecutionNodeType::IndexMultiLookup
        ),
        "execution route must preserve expression IN index-multi-lookup route selection",
    );
}

#[test]
fn expression_casefold_starts_with_access_and_execution_route_stay_in_parity() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::StartsWith,
        Value::Text("ALI".to_string()),
        CoercionId::TextCasefold,
    ));

    let explain = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain()
        .expect("expression starts-with explain should build");
    let ExplainAccessPath::IndexRange {
        name,
        fields,
        prefix_len,
        prefix,
        lower,
        upper,
    } = explain.access()
    else {
        panic!("expression starts-with should lower to index-range access");
    };
    assert_eq!(name, &PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS[0].name());
    assert_eq!(fields.as_slice(), ["email"]);
    assert_eq!(*prefix_len, 0);
    assert!(
        prefix.is_empty(),
        "expression starts-with range should not carry equality prefix values",
    );
    assert!(matches!(
        lower,
        std::ops::Bound::Included(Value::Text(value)) if value == "ali"
    ));
    assert!(matches!(upper, std::ops::Bound::Unbounded));

    let verbose = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain_execution_verbose()
        .expect("expression starts-with verbose explain should build");
    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_expression_access_choice_selected(&diagnostics, "IndexRange(email_expr)");
    assert_eq!(
        diagnostics.get("diag.r.predicate_stage"),
        Some(&"index_prefilter(strict_all_or_none)".to_string()),
        "text-casefold expression starts-with should keep the shared strict prefilter stage",
    );
    assert_eq!(
        diagnostics.get("diag.d.has_index_predicate_prefilter"),
        Some(&"true".to_string()),
        "text-casefold expression starts-with should compile the shared strict index prefilter",
    );
    assert_eq!(
        diagnostics.get("diag.d.has_residual_predicate_filter"),
        Some(&"false".to_string()),
        "text-casefold expression starts-with should no longer require a residual predicate filter",
    );

    let execution = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .explain_execution()
        .expect("expression starts-with execution explain should build");
    assert!(
        explain_execution_contains_node_type(&execution, ExplainExecutionNodeType::IndexRangeScan),
        "execution route must preserve expression starts-with index-range route selection",
    );
}

#[test]
fn expression_casefold_starts_with_single_char_prefix_keeps_index_range_route() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::StartsWith,
        Value::Text("A".to_string()),
        CoercionId::TextCasefold,
    ));

    let explain = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain()
        .expect("single-char expression starts-with explain should build");
    let ExplainAccessPath::IndexRange {
        name, lower, upper, ..
    } = explain.access()
    else {
        panic!("single-char expression starts-with should lower to index-range access");
    };
    assert_eq!(name, &PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS[0].name());
    assert!(matches!(
        lower,
        std::ops::Bound::Included(Value::Text(value)) if value == "a"
    ));
    assert!(matches!(upper, std::ops::Bound::Unbounded));

    let execution = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .explain_execution()
        .expect("single-char expression starts-with execution explain should build");
    assert!(
        explain_execution_contains_node_type(&execution, ExplainExecutionNodeType::IndexRangeScan),
        "single-char expression starts-with must keep index-range route selection",
    );
}

#[test]
fn explain_execution_text_and_json_surfaces_are_stable() {
    let id = Ulid::from_u128(9_101);
    let query = Query::<PlanSimpleEntity>::new(MissingRowPolicy::Ignore).by_id(id);
    let descriptor = query
        .explain_execution()
        .expect("execution descriptor explain should build");

    let text = query
        .explain_execution_text()
        .expect("execution text explain should build");
    assert!(
        text.contains("ByKeyLookup"),
        "execution text surface should expose access-root node type"
    );
    assert_eq!(
        text,
        descriptor.render_text_tree(),
        "execution text surface should be canonical descriptor text rendering",
    );

    let json = query
        .explain_execution_json()
        .expect("execution json explain should build");
    assert!(
        json.contains("\"node_type\":\"ByKeyLookup\""),
        "execution json surface should expose canonical root node type"
    );
    assert_eq!(
        json,
        descriptor.render_json_canonical(),
        "execution json surface should be canonical descriptor json rendering",
    );
}

#[test]
fn secondary_in_explain_uses_index_multi_lookup_access_shape() {
    let explain = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::In,
            Value::List(vec![Value::Uint(7), Value::Uint(8), Value::Uint(9)]),
            CoercionId::Strict,
        )))
        .explain()
        .expect("secondary IN explain should build");

    assert!(
        matches!(explain.access(), ExplainAccessPath::IndexMultiLookup { .. }),
        "secondary IN predicates should lower to the dedicated index-multi-lookup access shape",
    );
}

#[test]
fn secondary_or_eq_explain_uses_index_multi_lookup_access_shape() {
    let explain = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "group",
                CompareOp::Eq,
                Value::Uint(8),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "group",
                CompareOp::Eq,
                Value::Uint(7),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "group",
                CompareOp::Eq,
                Value::Uint(8),
                CoercionId::Strict,
            )),
        ]))
        .explain()
        .expect("secondary OR equality explain should build");

    assert!(
        matches!(explain.access(), ExplainAccessPath::IndexMultiLookup { .. }),
        "same-field strict OR equality should lower to index-multi-lookup access shape",
    );
}

#[test]
fn explain_execution_verbose_top_n_seek_shape_snapshot_is_stable() {
    let verbose = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("id")
        .offset(2)
        .limit(3)
        .explain_execution_verbose()
        .expect("top-n verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diag.r.execution_mode=Streaming",
        "diag.r.continuation_applied=false",
        "diag.r.limit=Some(3)",
        "diag.r.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diag.r.secondary_order_pushdown=not_applicable",
        "diag.r.top_n_seek=fetch(6)",
        "diag.r.index_range_limit_pushdown=disabled",
        "diag.r.predicate_stage=none",
        "diag.r.projected_fields=[\"id\", \"rank\"]",
        "diag.r.projection_pushdown=false",
        "diag.r.covering_read=order_mat",
        "diag.r.access_choice_chosen=FullScan",
        "diag.r.access_choice_chosen_reason=non_index_access",
        "diag.r.access_choice_alternatives=[]",
        "diag.r.access_choice_rejections=[]",
        "diag.d.has_top_n_seek=true",
        "diag.d.has_index_range_limit_pushdown=false",
        "diag.d.has_index_predicate_prefilter=false",
        "diag.d.has_residual_predicate_filter=false",
        "diag.p.mode=Load(LoadSpec { limit: Some(3), offset: 2 })",
        "diag.p.order_pushdown=missing_model_context",
        "diag.p.predicate_pushdown=none",
        "diag.p.distinct=false",
        "diag.p.page=Page { limit: Some(3), offset: 2 }",
        "diag.p.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "top-n verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn explain_execution_verbose_reports_secondary_order_pushdown_rejection_reason() {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_by("label")
        .explain_execution_verbose()
        .expect("execution verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_eq!(
        diagnostics.get("diag.r.secondary_order_pushdown"),
        Some(&"rejected(OrderFieldsDoNotMatchIndex(index=group_rank,prefix_len=1,expected_suffix=[\"rank\"],expected_full=[\"group\", \"rank\"],actual=[\"label\"]))".to_string()),
        "verbose execution explain should expose explicit route rejection reason",
    );
    assert_eq!(
        diagnostics.get("diag.p.mode"),
        Some(&"Load(LoadSpec { limit: None, offset: 0 })".to_string()),
        "verbose execution explain should include logical plan mode diagnostics",
    );
}

#[test]
fn explain_execution_verbose_reports_temporal_ranked_order_shape_parity() {
    let top_like_verbose = Query::<PlanTemporalBoundaryEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("occurred_on")
        .order_by("id")
        .limit(2)
        .explain_execution_verbose()
        .expect("temporal top-like verbose explain should build");
    let bottom_like_verbose = Query::<PlanTemporalBoundaryEntity>::new(MissingRowPolicy::Ignore)
        .order_by("occurred_on")
        .order_by("id")
        .limit(2)
        .explain_execution_verbose()
        .expect("temporal bottom-like verbose explain should build");

    let top_like = verbose_diagnostics_map(&top_like_verbose);
    let bottom_like = verbose_diagnostics_map(&bottom_like_verbose);
    let parity_keys = [
        "diag.r.execution_mode",
        "diag.r.continuation_applied",
        "diag.r.limit",
        "diag.r.fast_path_order",
        "diag.r.secondary_order_pushdown",
        "diag.r.top_n_seek",
        "diag.r.index_range_limit_pushdown",
        "diag.r.predicate_stage",
        "diag.r.projected_fields",
        "diag.r.projection_pushdown",
        "diag.r.covering_read",
        "diag.r.access_choice_chosen",
        "diag.r.access_choice_chosen_reason",
        "diag.r.access_choice_alternatives",
        "diag.r.access_choice_rejections",
        "diag.d.has_top_n_seek",
        "diag.d.has_index_range_limit_pushdown",
        "diag.d.has_index_predicate_prefilter",
        "diag.d.has_residual_predicate_filter",
        "diag.p.mode",
        "diag.p.order_pushdown",
        "diag.p.predicate_pushdown",
        "diag.p.distinct",
        "diag.p.page",
        "diag.p.consistency",
    ];
    for key in parity_keys {
        assert_eq!(
            top_like.get(key),
            bottom_like.get(key),
            "temporal top-like vs bottom-like ranked query shapes should keep verbose diagnostic parity for key {key}",
        );
    }
}

#[test]
fn explain_execution_verbose_temporal_ranked_shape_snapshot_is_stable() {
    let verbose = Query::<PlanTemporalBoundaryEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("occurred_on")
        .order_by("id")
        .limit(2)
        .explain_execution_verbose()
        .expect("temporal ranked verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diag.r.execution_mode=Materialized",
        "diag.r.continuation_applied=false",
        "diag.r.limit=Some(2)",
        "diag.r.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diag.r.secondary_order_pushdown=not_applicable",
        "diag.r.top_n_seek=disabled",
        "diag.r.index_range_limit_pushdown=disabled",
        "diag.r.predicate_stage=none",
        "diag.r.projected_fields=[\"id\", \"occurred_on\", \"occurred_at\", \"elapsed\"]",
        "diag.r.projection_pushdown=false",
        "diag.r.covering_read=order_mat",
        "diag.r.access_choice_chosen=FullScan",
        "diag.r.access_choice_chosen_reason=non_index_access",
        "diag.r.access_choice_alternatives=[]",
        "diag.r.access_choice_rejections=[]",
        "diag.d.has_top_n_seek=false",
        "diag.d.has_index_range_limit_pushdown=false",
        "diag.d.has_index_predicate_prefilter=false",
        "diag.d.has_residual_predicate_filter=false",
        "diag.p.mode=Load(LoadSpec { limit: Some(2), offset: 0 })",
        "diag.p.order_pushdown=missing_model_context",
        "diag.p.predicate_pushdown=none",
        "diag.p.distinct=false",
        "diag.p.page=Page { limit: Some(2), offset: 0 }",
        "diag.p.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "temporal ranked verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn explain_execution_verbose_reports_index_range_limit_pushdown_hints() {
    let range_predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Gte,
            Value::Uint(100),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Lt,
            Value::Uint(200),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::Eq,
            Value::Text("keep".to_string()),
            CoercionId::Strict,
        )),
    ]);

    let verbose = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(range_predicate)
        .order_by("code")
        .order_by("id")
        .limit(2)
        .explain_execution_verbose()
        .expect("index-range verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_eq!(
        diagnostics.get("diag.r.index_range_limit_pushdown"),
        Some(&"fetch(3)".to_string()),
        "verbose execution explain should freeze index-range pushdown fetch diagnostics",
    );
    assert_eq!(
        diagnostics.get("diag.d.has_index_range_limit_pushdown"),
        Some(&"true".to_string()),
        "descriptor diagnostics should report index-range pushdown node presence",
    );
    assert_eq!(
        diagnostics.get("diag.r.predicate_stage"),
        Some(&"residual_post_access".to_string()),
        "verbose execution explain should freeze predicate-stage diagnostics",
    );
}

#[test]
fn explain_execution_verbose_rejection_shape_snapshot_is_stable() {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_by("label")
        .explain_execution_verbose()
        .expect("execution verbose explain should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diag.r.execution_mode=Materialized",
        "diag.r.continuation_applied=false",
        "diag.r.limit=None",
        "diag.r.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diag.r.secondary_order_pushdown=rejected(OrderFieldsDoNotMatchIndex(index=group_rank,prefix_len=1,expected_suffix=[\"rank\"],expected_full=[\"group\", \"rank\"],actual=[\"label\"]))",
        "diag.r.top_n_seek=disabled",
        "diag.r.index_range_limit_pushdown=disabled",
        "diag.r.predicate_stage=index_prefilter(strict_all_or_none)",
        "diag.r.projected_fields=[\"id\", \"group\", \"rank\", \"label\"]",
        "diag.r.projection_pushdown=false",
        "diag.r.covering_read=order_mat",
        "diag.r.access_choice_chosen=IndexPrefix(group_rank)",
        "diag.r.access_choice_chosen_reason=single_candidate",
        "diag.r.access_choice_alternatives=[]",
        "diag.r.access_choice_rejections=[]",
        "diag.r.predicate_index_capability=fully_indexable",
        "diag.d.has_top_n_seek=false",
        "diag.d.has_index_range_limit_pushdown=false",
        "diag.d.has_index_predicate_prefilter=true",
        "diag.d.has_residual_predicate_filter=false",
        "diag.p.mode=Load(LoadSpec { limit: None, offset: 0 })",
        "diag.p.order_pushdown=missing_model_context",
        "diag.p.predicate_pushdown=applied(index_prefix)",
        "diag.p.distinct=false",
        "diag.p.page=None",
        "diag.p.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "verbose diagnostics snapshot drifted; output ordering and values are part of the explain contract",
    );
}

#[test]
fn explain_execution_verbose_index_range_pushdown_shape_snapshot_is_stable() {
    let range_predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Gte,
            Value::Uint(100),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Lt,
            Value::Uint(200),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::Eq,
            Value::Text("keep".to_string()),
            CoercionId::Strict,
        )),
    ]);

    let verbose = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(range_predicate)
        .order_by("code")
        .order_by("id")
        .limit(2)
        .explain_execution_verbose()
        .expect("index-range verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diag.r.execution_mode=Streaming",
        "diag.r.continuation_applied=false",
        "diag.r.limit=Some(2)",
        "diag.r.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diag.r.secondary_order_pushdown=eligible(index=code_unique,prefix_len=0)",
        "diag.r.top_n_seek=disabled",
        "diag.r.index_range_limit_pushdown=fetch(3)",
        "diag.r.predicate_stage=residual_post_access",
        "diag.r.projected_fields=[\"id\", \"code\", \"label\"]",
        "diag.r.projection_pushdown=false",
        "diag.r.covering_read=order_mat",
        "diag.r.access_choice_chosen=IndexRange(code_unique)",
        "diag.r.access_choice_chosen_reason=single_candidate",
        "diag.r.access_choice_alternatives=[]",
        "diag.r.access_choice_rejections=[]",
        "diag.r.predicate_index_capability=partially_indexable",
        "diag.d.has_top_n_seek=false",
        "diag.d.has_index_range_limit_pushdown=true",
        "diag.d.has_index_predicate_prefilter=false",
        "diag.d.has_residual_predicate_filter=true",
        "diag.p.mode=Load(LoadSpec { limit: Some(2), offset: 0 })",
        "diag.p.order_pushdown=missing_model_context",
        "diag.p.predicate_pushdown=applied(index_range)",
        "diag.p.distinct=false",
        "diag.p.page=Page { limit: Some(2), offset: 0 }",
        "diag.p.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "index-range verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn explain_execution_verbose_prefix_choice_prefers_order_compatible_index_when_rank_ties() {
    let verbose = Query::<PlanDeterministicChoiceEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )))
        .order_by("handle")
        .order_by("id")
        .explain_execution_verbose()
        .expect("deterministic prefix explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexPrefix(z_tier_handle_idx)".to_string()),
        "verbose explain must project the planner-selected order-compatible prefix index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "planner-choice explain must report the canonical order-compatibility tie-break when predicate rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_label_idx=order_compatible_preferred")
            }),
        "verbose explain must report the lexicographically earlier but order-incompatible index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_range_choice_prefers_order_compatible_index_when_rank_ties() {
    let verbose = Query::<PlanDeterministicRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_by("score")
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("deterministic range explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_tier_score_label_idx)".to_string()),
        "verbose explain must project the planner-selected order-compatible range index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "planner-choice explain must report the canonical order-compatibility tie-break when range rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_score_handle_idx=order_compatible_preferred")
            }),
        "verbose explain must report the lexicographically earlier but order-incompatible range index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_range_choice_desc_prefers_order_compatible_index_when_rank_ties() {
    let verbose = Query::<PlanDeterministicRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_by_desc("score")
        .order_by_desc("label")
        .order_by_desc("id")
        .explain_execution_verbose()
        .expect("descending deterministic range explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_tier_score_label_idx)".to_string()),
        "descending verbose explain must project the planner-selected order-compatible range index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "descending planner-choice explain must report the canonical order-compatibility tie-break when range rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_score_handle_idx=order_compatible_preferred")
            }),
        "descending verbose explain must report the lexicographically earlier but order-incompatible range index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_equality_prefix_suffix_order_prefers_order_compatible_index_when_rank_ties()
 {
    let verbose = Query::<PlanDeterministicRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("deterministic equality-prefix suffix-order explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexPrefix(z_tier_score_label_idx)".to_string()),
        "verbose explain must project the planner-selected order-compatible equality-prefix suffix-order index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "planner-choice explain must report the canonical order-compatibility tie-break when equality-prefix suffix-order rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_score_handle_idx=order_compatible_preferred")
            }),
        "verbose explain must report the lexicographically earlier but order-incompatible equality-prefix suffix-order index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_equality_prefix_suffix_order_desc_prefers_order_compatible_index_when_rank_ties()
 {
    let verbose = Query::<PlanDeterministicRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_by_desc("label")
        .order_by_desc("id")
        .explain_execution_verbose()
        .expect("descending deterministic equality-prefix suffix-order explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexPrefix(z_tier_score_label_idx)".to_string()),
        "descending verbose explain must project the planner-selected order-compatible equality-prefix suffix-order index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "descending planner-choice explain must report the canonical order-compatibility tie-break when equality-prefix suffix-order rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_score_handle_idx=order_compatible_preferred")
            }),
        "descending verbose explain must report the lexicographically earlier but order-incompatible equality-prefix suffix-order index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_order_only_choice_prefers_order_compatible_index_when_rank_ties() {
    let verbose = Query::<PlanOrderOnlyChoiceEntity>::new(MissingRowPolicy::Ignore)
        .order_by("alpha")
        .order_by("id")
        .explain_execution_verbose()
        .expect("deterministic order-only explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_alpha_idx)".to_string()),
        "verbose explain must project the planner-selected order-compatible fallback index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "planner-choice explain must report the canonical order-compatibility tie-break when order-only ranking ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_beta_idx=order_compatible_preferred")
            }),
        "verbose explain must report the lexicographically earlier but order-incompatible fallback index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_composite_order_only_choice_prefers_order_compatible_index_when_rank_ties()
 {
    let verbose = Query::<PlanDeterministicChoiceEntity>::new(MissingRowPolicy::Ignore)
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .explain_execution_verbose()
        .expect("deterministic composite order-only explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_tier_handle_idx)".to_string()),
        "verbose explain must project the planner-selected order-compatible composite fallback index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "planner-choice explain must report the canonical order-compatibility tie-break when composite order-only ranking ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_label_idx=order_compatible_preferred")
            }),
        "verbose explain must report the lexicographically earlier but order-incompatible composite fallback index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_composite_order_only_choice_desc_prefers_order_compatible_index_when_rank_ties()
 {
    let verbose = Query::<PlanDeterministicChoiceEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("tier")
        .order_by_desc("handle")
        .order_by_desc("id")
        .explain_execution_verbose()
        .expect("descending deterministic composite order-only explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_tier_handle_idx)".to_string()),
        "descending verbose explain must project the planner-selected order-compatible composite fallback index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "descending planner-choice explain must report the canonical order-compatibility tie-break when composite order-only ranking ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_label_idx=order_compatible_preferred")
            }),
        "descending verbose explain must report the lexicographically earlier but order-incompatible composite fallback index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_scalar_surface_defers_projection_and_grouped_node_families() {
    let by_key = Query::<PlanSimpleEntity>::new(MissingRowPolicy::Ignore)
        .by_id(Ulid::from_u128(9_301))
        .explain_execution()
        .expect("by-key execution descriptor should build");
    let pushdown_rejected = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_by("label")
        .explain_execution()
        .expect("pushdown-rejected descriptor should build");
    let index_range = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Gte,
                Value::Uint(100),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Lt,
                Value::Uint(200),
                CoercionId::Strict,
            )),
        ]))
        .order_by("code")
        .order_by("id")
        .limit(2)
        .explain_execution()
        .expect("index-range descriptor should build");

    for descriptor in [&by_key, &pushdown_rejected, &index_range] {
        for deferred in [
            ExplainExecutionNodeType::ProjectionMaterialized,
            ExplainExecutionNodeType::GroupedAggregateHashMaterialized,
            ExplainExecutionNodeType::GroupedAggregateOrderedMaterialized,
        ] {
            assert!(
                !explain_execution_contains_node_type(descriptor, deferred),
                "scalar execution descriptors intentionally defer materialized projection/grouped node family {} in this owner-local surface",
                deferred.as_str(),
            );
        }
    }
}

#[test]
fn explain_execution_verbose_reports_equivalent_empty_contract_reason_paths() {
    let is_null_verbose = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").is_null())
        .explain_execution_verbose()
        .expect("primary-key is-null verbose explain should build");
    let strict_in_empty_verbose = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").in_list(std::iter::empty::<Ulid>()))
        .explain_execution_verbose()
        .expect("strict IN [] verbose explain should build");

    let is_null_diagnostics = verbose_diagnostics_map(&is_null_verbose);
    let strict_in_empty_diagnostics = verbose_diagnostics_map(&strict_in_empty_verbose);
    assert_eq!(
        is_null_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"applied(empty_access_contract)".to_string()),
        "primary-key is-null should surface empty-contract predicate diagnostics",
    );
    assert_eq!(
        strict_in_empty_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"applied(empty_access_contract)".to_string()),
        "strict IN [] should surface empty-contract predicate diagnostics",
    );
}

#[test]
fn explain_execution_verbose_reports_equivalent_empty_contract_route_stage_parity() {
    let is_null_verbose = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").is_null())
        .explain_execution_verbose()
        .expect("primary-key is-null verbose explain should build");
    let strict_in_empty_verbose = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").in_list(std::iter::empty::<Ulid>()))
        .explain_execution_verbose()
        .expect("strict IN [] verbose explain should build");

    let is_null_diagnostics = verbose_diagnostics_map(&is_null_verbose);
    let strict_in_empty_diagnostics = verbose_diagnostics_map(&strict_in_empty_verbose);
    assert_eq!(
        is_null_diagnostics.get("diag.r.predicate_stage"),
        strict_in_empty_diagnostics.get("diag.r.predicate_stage"),
        "equivalent empty-contract predicates should keep route predicate-stage diagnostics in parity",
    );
}

#[test]
fn explain_execution_verbose_reports_non_strict_predicate_fallback_reason_path() {
    let non_strict_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::NumericWiden,
        )))
        .explain_execution_verbose()
        .expect("non-strict predicate verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&non_strict_verbose);
    assert_eq!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(non_strict_compare_coercion)".to_string()),
        "non-strict indexed compare should surface full-scan fallback predicate diagnostics",
    );
    assert_eq!(
        diagnostics.get("diag.r.predicate_stage"),
        Some(&"residual_post_access".to_string()),
        "non-strict indexed compare should execute as residual post-access predicate stage",
    );
}

#[test]
fn explain_execution_verbose_reports_is_null_predicate_pushdown_reason_paths() {
    let primary_key_is_null_verbose = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").is_null())
        .explain_execution_verbose()
        .expect("primary-key is-null verbose explain should build");
    let secondary_is_null_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("group").is_null())
        .explain_execution_verbose()
        .expect("secondary is-null verbose explain should build");

    let primary_key_diagnostics = verbose_diagnostics_map(&primary_key_is_null_verbose);
    let secondary_diagnostics = verbose_diagnostics_map(&secondary_is_null_verbose);

    assert_eq!(
        primary_key_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"applied(empty_access_contract)".to_string()),
        "impossible primary-key IS NULL should surface empty-contract predicate pushdown diagnostics",
    );
    assert_eq!(
        secondary_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(is_null_full_scan)".to_string()),
        "non-primary IS NULL should surface full-scan fallback predicate diagnostics",
    );
}

#[test]
fn explain_execution_verbose_non_strict_fallback_shape_snapshot_is_stable() {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::NumericWiden,
        )))
        .explain_execution_verbose()
        .expect("non-strict fallback verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diag.r.execution_mode=Materialized",
        "diag.r.continuation_applied=false",
        "diag.r.limit=None",
        "diag.r.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diag.r.secondary_order_pushdown=not_applicable",
        "diag.r.top_n_seek=disabled",
        "diag.r.index_range_limit_pushdown=disabled",
        "diag.r.predicate_stage=residual_post_access",
        "diag.r.projected_fields=[\"id\", \"group\", \"rank\", \"label\"]",
        "diag.r.projection_pushdown=false",
        "diag.r.covering_read=access_not_cov",
        "diag.r.access_choice_chosen=FullScan",
        "diag.r.access_choice_chosen_reason=non_index_access",
        "diag.r.access_choice_alternatives=[]",
        "diag.r.access_choice_rejections=[]",
        "diag.d.has_top_n_seek=false",
        "diag.d.has_index_range_limit_pushdown=false",
        "diag.d.has_index_predicate_prefilter=false",
        "diag.d.has_residual_predicate_filter=true",
        "diag.p.mode=Load(LoadSpec { limit: None, offset: 0 })",
        "diag.p.order_pushdown=missing_model_context",
        "diag.p.predicate_pushdown=fallback(non_strict_compare_coercion)",
        "diag.p.distinct=false",
        "diag.p.page=None",
        "diag.p.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "non-strict fallback verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn explain_execution_verbose_reports_empty_prefix_starts_with_fallback_reason_path() {
    let empty_prefix_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("label").text_starts_with(""))
        .explain_execution_verbose()
        .expect("empty-prefix starts-with verbose explain should build");
    let non_empty_prefix_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("label").text_starts_with("label"))
        .explain_execution_verbose()
        .expect("non-empty starts-with verbose explain should build");

    let empty_prefix_diagnostics = verbose_diagnostics_map(&empty_prefix_verbose);
    let non_empty_prefix_diagnostics = verbose_diagnostics_map(&non_empty_prefix_verbose);
    assert_eq!(
        empty_prefix_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(starts_with_empty_prefix)".to_string()),
        "empty-prefix starts-with should surface the explicit empty-prefix fallback reason",
    );
    assert_eq!(
        non_empty_prefix_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(full_scan)".to_string()),
        "non-empty starts-with over a non-indexed field should remain generic full-scan fallback",
    );
    assert_eq!(
        empty_prefix_diagnostics.get("diag.r.predicate_stage"),
        Some(&"residual_post_access".to_string()),
        "empty-prefix starts-with fallback should preserve residual predicate stage diagnostics",
    );
}

#[test]
fn explain_execution_verbose_reports_text_operator_fallback_reason_path() {
    let text_contains_ci_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("label").text_contains_ci("label"))
        .explain_execution_verbose()
        .expect("text-contains-ci verbose explain should build");
    let ends_with_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::EndsWith,
            Value::Text("fix".to_string()),
            CoercionId::Strict,
        )))
        .explain_execution_verbose()
        .expect("ends-with verbose explain should build");

    let text_contains_ci_diagnostics = verbose_diagnostics_map(&text_contains_ci_verbose);
    let ends_with_diagnostics = verbose_diagnostics_map(&ends_with_verbose);
    assert_eq!(
        text_contains_ci_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(text_operator_full_scan)".to_string()),
        "text contains-ci should surface dedicated text-operator full-scan fallback reason",
    );
    assert_eq!(
        ends_with_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(text_operator_full_scan)".to_string()),
        "ends-with compare should surface dedicated text-operator full-scan fallback reason",
    );
    assert_eq!(
        text_contains_ci_diagnostics.get("diag.r.predicate_stage"),
        Some(&"residual_post_access".to_string()),
        "text-operator fallback should preserve residual predicate-stage diagnostics",
    );
}

#[test]
fn explain_execution_verbose_reports_equivalent_in_set_route_stage_parity() {
    let in_permuted_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("group").in_list([8_u32, 7_u32, 8_u32]))
        .explain_execution_verbose()
        .expect("permuted IN verbose explain should build");
    let in_canonical_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("group").in_list([7_u32, 8_u32]))
        .explain_execution_verbose()
        .expect("canonical IN verbose explain should build");

    let in_permuted_diagnostics = verbose_diagnostics_map(&in_permuted_verbose);
    let in_canonical_diagnostics = verbose_diagnostics_map(&in_canonical_verbose);
    assert_eq!(
        in_permuted_diagnostics.get("diag.r.predicate_stage"),
        in_canonical_diagnostics.get("diag.r.predicate_stage"),
        "equivalent canonical IN sets should keep route predicate-stage diagnostics in parity",
    );
}

#[test]
fn explain_execution_verbose_reports_equivalent_between_and_eq_parity() {
    let equivalent_between_verbose = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Gte,
                Value::Uint(100),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Lte,
                Value::Uint(100),
                CoercionId::Strict,
            )),
        ]))
        .order_by("code")
        .order_by("id")
        .explain_execution_verbose()
        .expect("equivalent-between verbose explain should build");
    let strict_eq_verbose = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Eq,
            Value::Uint(100),
            CoercionId::Strict,
        )))
        .order_by("code")
        .order_by("id")
        .explain_execution_verbose()
        .expect("strict-eq verbose explain should build");

    let between_diagnostics = verbose_diagnostics_map(&equivalent_between_verbose);
    let eq_diagnostics = verbose_diagnostics_map(&strict_eq_verbose);
    assert_eq!(
        between_diagnostics.get("diag.p.predicate_pushdown"),
        eq_diagnostics.get("diag.p.predicate_pushdown"),
        "equivalent BETWEEN-style bounds and strict equality should report identical pushdown reason labels",
    );
    assert_eq!(
        between_diagnostics.get("diag.r.predicate_stage"),
        eq_diagnostics.get("diag.r.predicate_stage"),
        "equivalent BETWEEN-style bounds and strict equality should preserve route predicate-stage parity",
    );
}

#[test]
fn explain_execution_verbose_reports_equivalent_prefix_like_route_stage_parity() {
    let starts_with_verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text("foo".to_string()),
            CoercionId::Strict,
        )))
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("starts-with verbose explain should build");
    let equivalent_range_verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "label",
                CompareOp::Gte,
                Value::Text("foo".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "label",
                CompareOp::Lt,
                Value::Text("fop".to_string()),
                CoercionId::Strict,
            )),
        ]))
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("equivalent-range verbose explain should build");

    let starts_with_diagnostics = verbose_diagnostics_map(&starts_with_verbose);
    let equivalent_range_diagnostics = verbose_diagnostics_map(&equivalent_range_verbose);
    assert_eq!(
        starts_with_diagnostics.get("diag.p.predicate_pushdown"),
        equivalent_range_diagnostics.get("diag.p.predicate_pushdown"),
        "equivalent prefix-like and bounded-range forms should report identical predicate pushdown reason labels",
    );
    assert_eq!(
        starts_with_diagnostics.get("diag.r.predicate_stage"),
        equivalent_range_diagnostics.get("diag.r.predicate_stage"),
        "equivalent prefix-like and bounded-range forms should preserve route predicate-stage parity",
    );
}

#[test]
fn explain_execution_verbose_reports_strict_text_prefix_like_index_range_pushdown_stage() {
    let starts_with_verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text("foo".to_string()),
            CoercionId::Strict,
        )))
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("starts-with verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&starts_with_verbose);
    assert_eq!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"applied(index_range)".to_string()),
        "strict field-key text starts-with should surface the bounded index-range pushdown reason",
    );
    assert_eq!(
        diagnostics.get("diag.r.predicate_stage"),
        Some(&"index_prefilter(strict_all_or_none)".to_string()),
        "strict field-key text starts-with should compile to one strict index prefilter stage",
    );
    assert_eq!(
        diagnostics.get("diag.d.has_index_predicate_prefilter"),
        Some(&"true".to_string()),
        "strict field-key text starts-with should emit the strict index prefilter flag",
    );
    assert_eq!(
        diagnostics.get("diag.d.has_residual_predicate_filter"),
        Some(&"false".to_string()),
        "strict field-key text starts-with should not keep residual filtering once the bounded range is exact",
    );
}

#[test]
fn explain_execution_verbose_reports_max_unicode_prefix_like_parity() {
    let prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();
    let starts_with_verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text(prefix.clone()),
            CoercionId::Strict,
        )))
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("max-unicode starts-with verbose explain should build");
    let equivalent_lower_bound_verbose =
        Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "label",
                CompareOp::Gte,
                Value::Text(prefix),
                CoercionId::Strict,
            )))
            .order_by("label")
            .order_by("id")
            .explain_execution_verbose()
            .expect("equivalent lower-bound verbose explain should build");

    let starts_with_diagnostics = verbose_diagnostics_map(&starts_with_verbose);
    let lower_bound_diagnostics = verbose_diagnostics_map(&equivalent_lower_bound_verbose);
    assert_eq!(
        starts_with_diagnostics.get("diag.p.predicate_pushdown"),
        lower_bound_diagnostics.get("diag.p.predicate_pushdown"),
        "max-unicode prefix-like and equivalent lower-bound forms should report identical predicate pushdown reason labels",
    );
    assert_eq!(
        starts_with_diagnostics.get("diag.r.predicate_stage"),
        lower_bound_diagnostics.get("diag.r.predicate_stage"),
        "max-unicode prefix-like and equivalent lower-bound forms should preserve route predicate-stage parity",
    );
}

#[test]
fn explain_execution_verbose_non_strict_ends_with_uses_non_strict_fallback_precedence() {
    let non_strict_ends_with_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::EndsWith,
            Value::Text("fix".to_string()),
            CoercionId::TextCasefold,
        )))
        .explain_execution_verbose()
        .expect("non-strict ends-with verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&non_strict_ends_with_verbose);
    assert_eq!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(non_strict_compare_coercion)".to_string()),
        "non-strict ends-with should report non-strict compare fallback reason",
    );
    assert_ne!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(text_operator_full_scan)".to_string()),
        "non-strict ends-with should not be classified as text-operator fallback",
    );
}

#[test]
fn explain_execution_verbose_keeps_collection_contains_on_generic_full_scan_fallback() {
    let collection_contains_verbose = Query::<PlanPhaseEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tags",
            CompareOp::Contains,
            Value::Uint(7),
            CoercionId::CollectionElement,
        )))
        .explain_execution_verbose()
        .expect("collection contains verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&collection_contains_verbose);
    assert_eq!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(non_strict_compare_coercion)".to_string()),
        "collection-element contains should continue to report non-strict compare fallback",
    );
    assert_ne!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(text_operator_full_scan)".to_string()),
        "collection-element contains should not be classified as text-operator fallback",
    );
}

#[test]
fn explain_execution_verbose_is_null_fallback_shape_snapshot_is_stable() {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("group").is_null())
        .explain_execution_verbose()
        .expect("is-null fallback verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diag.r.execution_mode=Materialized",
        "diag.r.continuation_applied=false",
        "diag.r.limit=None",
        "diag.r.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diag.r.secondary_order_pushdown=not_applicable",
        "diag.r.top_n_seek=disabled",
        "diag.r.index_range_limit_pushdown=disabled",
        "diag.r.predicate_stage=residual_post_access",
        "diag.r.projected_fields=[\"id\", \"group\", \"rank\", \"label\"]",
        "diag.r.projection_pushdown=false",
        "diag.r.covering_read=access_not_cov",
        "diag.r.access_choice_chosen=FullScan",
        "diag.r.access_choice_chosen_reason=non_index_access",
        "diag.r.access_choice_alternatives=[]",
        "diag.r.access_choice_rejections=[]",
        "diag.d.has_top_n_seek=false",
        "diag.d.has_index_range_limit_pushdown=false",
        "diag.d.has_index_predicate_prefilter=false",
        "diag.d.has_residual_predicate_filter=true",
        "diag.p.mode=Load(LoadSpec { limit: None, offset: 0 })",
        "diag.p.order_pushdown=missing_model_context",
        "diag.p.predicate_pushdown=fallback(is_null_full_scan)",
        "diag.p.distinct=false",
        "diag.p.page=None",
        "diag.p.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "is-null fallback verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn grouped_aggregate_builder_continuation_token_bytes_match_helper_shape() {
    let helper_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("helper grouped continuation plan should build")
        .into_inner();
    let builder_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .limit(1)
        .plan()
        .expect("builder grouped continuation plan should build")
        .into_inner();
    let helper_signature = helper_plan.continuation_signature("intent::tests::PlanEntity");
    let builder_signature = builder_plan.continuation_signature("intent::tests::PlanEntity");
    assert_eq!(
        helper_signature, builder_signature,
        "helper and builder grouped continuation signatures must match",
    );

    let helper_token = GroupedContinuationToken::new_with_direction(
        helper_signature,
        vec![Value::Text("alpha".to_string())],
        Direction::Asc,
        0,
    )
    .encode()
    .expect("helper grouped continuation token should encode");
    let builder_token = GroupedContinuationToken::new_with_direction(
        builder_signature,
        vec![Value::Text("alpha".to_string())],
        Direction::Asc,
        0,
    )
    .encode()
    .expect("builder grouped continuation token should encode");
    assert_eq!(
        helper_token, builder_token,
        "helper and builder grouped continuation token bytes must match for equivalent shapes",
    );
}

#[test]
fn grouped_global_distinct_mixed_terminal_shape_without_group_by_is_rejected() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .aggregate(crate::db::count())
        .plan()
        .expect_err(
            "global grouped distinct without group keys should reject mixed aggregate shape",
        );

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::GlobalDistinctAggregateShapeUnsupported
        )
    }));
}

#[test]
fn grouped_aggregate_builder_rejects_distinct_for_unsupported_kind() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(exists().distinct())
        .plan()
        .expect_err("grouped distinct exists should remain rejected");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctAggregateKindUnsupported { index, kind }
                if *index == 0 && kind == "Exists"
        )
    }));
}

#[test]
fn grouped_aggregate_builder_rejects_field_target_terminal_in_grouped_v1() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(max_by("name"))
        .plan()
        .expect_err("grouped max(field) should remain unsupported in grouped v1");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::FieldTargetAggregatesUnsupported { index, kind, field }
                if *index == 0 && kind == "Max" && field == "name"
        )
    }));
}

#[test]
fn grouped_aggregate_builder_rejects_global_distinct_sum_on_non_numeric_target() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(sum("name").distinct())
        .plan()
        .expect_err("global sum(distinct non-numeric field) should fail");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::GlobalDistinctSumTargetNotNumeric { index, field }
                if *index == 0 && field == "name"
        )
    }));
}

#[test]
fn grouped_having_requires_group_by() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .having_group("name", CompareOp::Eq, Value::Text("alpha".to_string()))
        .expect_err("having should fail when group_by is missing");

    assert!(matches!(
        err,
        QueryError::Intent(IntentError::HavingRequiresGroupBy)
    ));
}

#[test]
fn grouped_having_with_distinct_is_rejected() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .having_aggregate(0, CompareOp::Gt, Value::Uint(0))
        .expect("having aggregate clause should append on grouped query")
        .distinct()
        .plan()
        .expect_err("grouped having with distinct should be rejected in this release");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctHavingUnsupported
        )
    }));
}

#[test]
fn grouped_having_with_distinct_is_rejected_for_ordered_eligible_shape() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .having_aggregate(0, CompareOp::Gt, Value::Uint(0))
        .expect("having aggregate clause should append on grouped query")
        .distinct()
        .plan()
        .expect_err(
            "grouped having with distinct should be rejected even when grouped order prefix is aligned",
        );

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctHavingUnsupported
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

    let model_as_typed = AccessPlannedQuery::from_parts(
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
        QueryError::Validate(crate::db::schema::ValidateError::InvalidLiteral {
            field,
            ..
        }) if field == "stage"
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
