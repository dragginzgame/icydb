//! Module: db::query::intent::tests::support
//! Owns shared fixtures, helper predicates, and model contracts for the
//! topical query-intent owner suites.
//! Does not own: the topical assertions themselves.
//! Boundary: keeps reusable test support out of the owner `mod.rs` wiring file.

#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) use crate::db::query::plan::{
    AggregateKind,
    expr::{Expr, ProjectionField},
};
pub(in crate::db::query::intent::tests) use crate::{
    db::{
        IntentError, Query, QueryError,
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
pub(in crate::db::query::intent::tests) use std::collections::BTreeMap;

// Helper for intent tests that need the typed model snapshot.
pub(in crate::db::query::intent::tests) fn basic_model() -> &'static EntityModel {
    <PlanEntity as EntitySchema>::MODEL
}

pub(in crate::db::query::intent::tests) fn verbose_diagnostics_lines(verbose: &str) -> Vec<String> {
    verbose
        .lines()
        .filter(|line| line.starts_with("diag."))
        .map(ToOwned::to_owned)
        .collect()
}

pub(in crate::db::query::intent::tests) fn verbose_diagnostics_map(
    verbose: &str,
) -> BTreeMap<String, String> {
    let mut diagnostics = BTreeMap::new();
    for line in verbose_diagnostics_lines(verbose) {
        let Some((key, value)) = line.split_once('=') else {
            panic!("diagnostic line must contain '=': {line}");
        };
        diagnostics.insert(key.to_string(), value.to_string());
    }

    diagnostics
}

pub(in crate::db::query::intent::tests) fn explain_execution_contains_node_type(
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

pub(in crate::db::query::intent::tests) fn assert_expression_access_choice_selected(
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

pub(in crate::db::query::intent::tests) fn query_error_is_group_plan_error(
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

pub(in crate::db::query::intent::tests) fn query_error_is_policy_plan_error(
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

pub(in crate::db::query::intent::tests) fn query_error_is_order_plan_error(
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

pub(in crate::db::query::intent::tests) fn query_error_is_predicate_validation_error(
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
pub(in crate::db::query::intent::tests) struct PlanEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) name: String,
}

pub(in crate::db::query::intent::tests) static MAP_PLAN_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated(
        "attributes",
        FieldKind::Map {
            key: &FieldKind::Text,
            value: &FieldKind::Uint,
        },
    ),
];
pub(in crate::db::query::intent::tests) static MAP_PLAN_INDEXES: [&IndexModel; 0] = [];
pub(in crate::db::query::intent::tests) static MAP_PLAN_MODEL: EntityModel =
    entity_model_from_static(
        "intent_tests::MapPlanEntity",
        "MapPlanEntity",
        &MAP_PLAN_FIELDS[0],
        0,
        &MAP_PLAN_FIELDS,
        &MAP_PLAN_INDEXES,
    );

pub(in crate::db::query::intent::tests) static ENUM_PLAN_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated(
        "stage",
        FieldKind::Enum {
            path: "intent_tests::Stage",
            variants: &[],
        },
    ),
];
pub(in crate::db::query::intent::tests) static ENUM_PLAN_INDEXES: [&IndexModel; 0] = [];
pub(in crate::db::query::intent::tests) static ENUM_PLAN_MODEL: EntityModel =
    entity_model_from_static(
        "intent_tests::EnumPlanEntity",
        "EnumPlanEntity",
        &ENUM_PLAN_FIELDS[0],
        0,
        &ENUM_PLAN_FIELDS,
        &ENUM_PLAN_INDEXES,
    );

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub(in crate::db::query::intent::tests) struct PlanSingleton {
    pub(in crate::db::query::intent::tests) id: Unit,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
pub(in crate::db::query::intent::tests) struct PlanSimpleEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
pub(in crate::db::query::intent::tests) struct PlanNumericEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) rank: i32,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
pub(in crate::db::query::intent::tests) struct PlanPushdownEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) group: u32,
    pub(in crate::db::query::intent::tests) rank: u32,
    pub(in crate::db::query::intent::tests) label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
pub(in crate::db::query::intent::tests) struct PlanUniqueRangeEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) code: u32,
    pub(in crate::db::query::intent::tests) label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
pub(in crate::db::query::intent::tests) struct PlanTextPrefixEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
pub(in crate::db::query::intent::tests) struct PlanPhaseEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) tags: Vec<u32>,
    pub(in crate::db::query::intent::tests) label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
pub(in crate::db::query::intent::tests) struct PlanExpressionCasefoldEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) email: String,
    pub(in crate::db::query::intent::tests) label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
pub(in crate::db::query::intent::tests) struct PlanDeterministicChoiceEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) tier: String,
    pub(in crate::db::query::intent::tests) handle: String,
    pub(in crate::db::query::intent::tests) label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
pub(in crate::db::query::intent::tests) struct PlanDeterministicRangeEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) tier: String,
    pub(in crate::db::query::intent::tests) score: u32,
    pub(in crate::db::query::intent::tests) handle: String,
    pub(in crate::db::query::intent::tests) label: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
pub(in crate::db::query::intent::tests) struct PlanOrderOnlyChoiceEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) alpha: String,
    pub(in crate::db::query::intent::tests) beta: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
pub(in crate::db::query::intent::tests) struct PlanTemporalBoundaryEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) occurred_on: Date,
    pub(in crate::db::query::intent::tests) occurred_at: Timestamp,
    pub(in crate::db::query::intent::tests) elapsed: Duration,
}

impl FieldProjection for PlanSingleton {
    fn get_value_by_index(&self, index: usize) -> Option<Value> {
        match index {
            0 => Some(self.id.to_value()),
            _ => None,
        }
    }
}

pub(in crate::db::query::intent::tests) struct PlanCanister;

impl Path for PlanCanister {
    const PATH: &'static str = concat!(module_path!(), "::PlanCanister");
}

impl crate::traits::CanisterKind for PlanCanister {
    const COMMIT_MEMORY_ID: u8 = crate::testing::test_commit_memory_id();
}

pub(in crate::db::query::intent::tests) struct PlanDataStore;

impl Path for PlanDataStore {
    const PATH: &'static str = concat!(module_path!(), "::PlanDataStore");
}

impl crate::traits::StoreKind for PlanDataStore {
    type Canister = PlanCanister;
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

pub(in crate::db::query::intent::tests) static PLAN_PUSHDOWN_INDEX_FIELDS: [&str; 2] =
    ["group", "rank"];
pub(in crate::db::query::intent::tests) static PLAN_PUSHDOWN_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated(
        "group_rank",
        PlanDataStore::PATH,
        &PLAN_PUSHDOWN_INDEX_FIELDS,
        false,
    )];

pub(in crate::db::query::intent::tests) static PLAN_UNIQUE_RANGE_INDEX_FIELDS: [&str; 1] = ["code"];
pub(in crate::db::query::intent::tests) static PLAN_UNIQUE_RANGE_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated(
        "code_unique",
        PlanDataStore::PATH,
        &PLAN_UNIQUE_RANGE_INDEX_FIELDS,
        true,
    )];

pub(in crate::db::query::intent::tests) static PLAN_TEXT_PREFIX_INDEX_FIELDS: [&str; 1] = ["label"];
pub(in crate::db::query::intent::tests) static PLAN_TEXT_PREFIX_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated(
        "label",
        PlanDataStore::PATH,
        &PLAN_TEXT_PREFIX_INDEX_FIELDS,
        false,
    )];

pub(in crate::db::query::intent::tests) static PLAN_EXPRESSION_CASEFOLD_INDEX_FIELDS: [&str; 1] =
    ["email"];
pub(in crate::db::query::intent::tests) static PLAN_EXPRESSION_CASEFOLD_KEY_ITEMS: [IndexKeyItem;
    1] = [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
pub(in crate::db::query::intent::tests) static PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS: [IndexModel;
    1] = [IndexModel::generated_with_key_items(
    "email_expr",
    PlanDataStore::PATH,
    &PLAN_EXPRESSION_CASEFOLD_INDEX_FIELDS,
    &PLAN_EXPRESSION_CASEFOLD_KEY_ITEMS,
    false,
)];
pub(in crate::db::query::intent::tests) static PLAN_DETERMINISTIC_CHOICE_LABEL_INDEX_FIELDS:
    [&str; 2] = ["tier", "label"];
pub(in crate::db::query::intent::tests) static PLAN_DETERMINISTIC_CHOICE_HANDLE_INDEX_FIELDS:
    [&str; 2] = ["tier", "handle"];
pub(in crate::db::query::intent::tests) static PLAN_DETERMINISTIC_CHOICE_INDEX_MODELS:
    [IndexModel; 2] = [
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
pub(in crate::db::query::intent::tests) static PLAN_DETERMINISTIC_RANGE_HANDLE_INDEX_FIELDS:
    [&str; 3] = ["tier", "score", "handle"];
pub(in crate::db::query::intent::tests) static PLAN_DETERMINISTIC_RANGE_LABEL_INDEX_FIELDS: [&str;
    3] = ["tier", "score", "label"];
pub(in crate::db::query::intent::tests) static PLAN_DETERMINISTIC_RANGE_INDEX_MODELS: [IndexModel;
    2] = [
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
pub(in crate::db::query::intent::tests) static PLAN_ORDER_ONLY_CHOICE_BETA_INDEX_FIELDS: [&str; 1] =
    ["beta"];
pub(in crate::db::query::intent::tests) static PLAN_ORDER_ONLY_CHOICE_ALPHA_INDEX_FIELDS: [&str;
    1] = ["alpha"];
pub(in crate::db::query::intent::tests) static PLAN_ORDER_ONLY_CHOICE_INDEX_MODELS: [IndexModel;
    2] = [
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

pub(in crate::db::query::intent::tests) static PLAN_PHASE_TAG_KIND: FieldKind = FieldKind::Uint;

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
