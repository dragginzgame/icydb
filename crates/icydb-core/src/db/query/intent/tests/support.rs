//! Module: db::query::intent::tests::support
//! Owns shared fixtures, helper predicates, and model contracts for the
//! topical query-intent owner suites.
//! Does not own: the topical assertions themselves.
//! Boundary: keeps reusable test support out of the owner `mod.rs` wiring file.

#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) use crate::db::query::plan::{
    AggregateKind,
    expr::{BinaryOp, Expr, FieldId, ProjectionField},
};
#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) use crate::testing::entity_model_from_static;
pub(in crate::db::query::intent::tests) use crate::{
    db::{
        IntentError, Query, QueryError,
        access::{AccessPath, AccessPlan},
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::{
            builder::FieldRef,
            explain::{ExplainExecutionNodeDescriptor, ExplainExecutionNodeType},
            intent::model::QueryModel,
            plan::{AccessPlannedQuery, OrderDirection, OrderSpec},
        },
    },
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
    traits::{EntitySchema, FieldProjection, Path},
    types::{Date, Duration, Timestamp, Ulid, Unit},
    value::{RuntimeValueEncode, Value},
};
#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) use crate::{
    db::{
        cursor::GroupedContinuationToken,
        direction::Direction,
        query::{
            builder::{count, count_by, exists, first, last, max, max_by, min, sum},
            explain::{ExplainAccessDecisionKind, ExplainAccessPath, ExplainPlan},
            intent::{AccessRequirementViolation, RequiredAccessPath},
        },
    },
    model::{
        field::FieldModel,
        index::{IndexExpression, IndexKeyItem},
    },
};
use icydb_derive::FieldProjection;
use serde::Deserialize;
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

#[cfg(feature = "sql")]
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

#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) fn assert_plan(plan: &ExplainPlan) -> PlanAssertion<'_> {
    PlanAssertion { plan }
}

#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) struct PlanAssertion<'a> {
    plan: &'a ExplainPlan,
}

#[cfg(feature = "sql")]
impl PlanAssertion<'_> {
    pub(in crate::db::query::intent::tests) fn uses_index(self, expected: &str) -> Self {
        assert_eq!(
            self.plan.access_decision().selected.index_name.as_deref(),
            Some(expected),
            "expected selected semantic index '{expected}'",
        );

        self
    }

    pub(in crate::db::query::intent::tests) fn access_kind(
        self,
        expected: RequiredAccessPath,
    ) -> Self {
        assert!(
            required_access_path_matches(expected, self.plan.access_decision().selected.kind),
            "expected selected access kind '{}', got {:?}",
            expected.code(),
            self.plan.access_decision().selected.kind,
        );

        self
    }

    pub(in crate::db::query::intent::tests) fn bound_prefix_len(self, expected: usize) -> Self {
        let actual = match self.plan.access() {
            ExplainAccessPath::IndexPrefix { prefix_len, .. }
            | ExplainAccessPath::IndexRange { prefix_len, .. } => Some(*prefix_len),
            ExplainAccessPath::IndexBranchSet {
                fixed_values,
                branch_values,
                ..
            } => (!branch_values.is_empty()).then_some(fixed_values.len().saturating_add(1)),
            ExplainAccessPath::ByKey { .. }
            | ExplainAccessPath::ByKeys { .. }
            | ExplainAccessPath::KeyRange { .. }
            | ExplainAccessPath::IndexMultiLookup { .. }
            | ExplainAccessPath::FullScan
            | ExplainAccessPath::Union(_)
            | ExplainAccessPath::Intersection(_) => None,
        };

        assert_eq!(
            actual,
            Some(expected),
            "expected selected index access prefix length {expected}",
        );

        self
    }

    pub(in crate::db::query::intent::tests) fn has_no_residual_filter(self) -> Self {
        assert_eq!(
            self.plan.access_decision().residual.burden_class,
            "none",
            "expected selected access decision to carry no residual burden",
        );
        assert!(
            !self.plan.access_decision().residual.has_residual_filter,
            "expected selected access decision to carry no residual scalar filter",
        );
        assert!(
            !self.plan.access_decision().residual.has_residual_predicate,
            "expected selected access decision to carry no residual predicate",
        );
        assert_eq!(
            self.plan
                .access_decision()
                .residual
                .residual_predicate_count,
            0,
            "expected selected access decision to carry no residual predicate terms",
        );

        self
    }
}

#[cfg(feature = "sql")]
fn required_access_path_matches(
    expected: RequiredAccessPath,
    actual: ExplainAccessDecisionKind,
) -> bool {
    matches!(
        (expected, actual),
        (RequiredAccessPath::ByKey, ExplainAccessDecisionKind::ByKey)
            | (
                RequiredAccessPath::ByKeys,
                ExplainAccessDecisionKind::ByKeys
            )
            | (
                RequiredAccessPath::KeyRange,
                ExplainAccessDecisionKind::KeyRange
            )
            | (
                RequiredAccessPath::IndexPrefix,
                ExplainAccessDecisionKind::IndexPrefix
            )
            | (
                RequiredAccessPath::IndexMultiLookup,
                ExplainAccessDecisionKind::IndexMultiLookup
            )
            | (
                RequiredAccessPath::IndexBranchSet,
                ExplainAccessDecisionKind::IndexBranchSet
            )
            | (
                RequiredAccessPath::IndexRange,
                ExplainAccessDecisionKind::IndexRange
            )
            | (
                RequiredAccessPath::FullScan,
                ExplainAccessDecisionKind::FullScan
            )
            | (RequiredAccessPath::Union, ExplainAccessDecisionKind::Union)
            | (
                RequiredAccessPath::Intersection,
                ExplainAccessDecisionKind::Intersection
            )
    )
}

#[cfg(feature = "sql")]
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

#[cfg(feature = "sql")]
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
#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) name: String,
}

#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) static MAP_PLAN_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated(
        "attributes",
        FieldKind::Map {
            key: &FieldKind::Text { max_len: None },
            value: &FieldKind::Nat64,
        },
    ),
];
#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) static MAP_PLAN_INDEXES: [&IndexModel; 0] = [];
#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) static MAP_PLAN_MODEL: EntityModel =
    entity_model_from_static(
        "intent_tests::MapPlanEntity",
        "MapPlanEntity",
        &MAP_PLAN_FIELDS[0],
        0,
        &MAP_PLAN_FIELDS,
        &MAP_PLAN_INDEXES,
    );

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanSingleton {
    pub(in crate::db::query::intent::tests) id: Unit,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanSimpleEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanNumericEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) rank: i32,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanPushdownEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) group: u32,
    pub(in crate::db::query::intent::tests) rank: u32,
    pub(in crate::db::query::intent::tests) label: String,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanBranchSetEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) collection_id: String,
    pub(in crate::db::query::intent::tests) stage: String,
    pub(in crate::db::query::intent::tests) title: String,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanUniqueRangeEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) code: u32,
    pub(in crate::db::query::intent::tests) label: String,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanTextPrefixEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) label: String,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanPhaseEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) tags: Vec<u32>,
    pub(in crate::db::query::intent::tests) label: String,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanExpressionCasefoldEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) email: String,
    pub(in crate::db::query::intent::tests) label: String,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanDeterministicChoiceEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) tier: String,
    pub(in crate::db::query::intent::tests) handle: String,
    pub(in crate::db::query::intent::tests) label: String,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanDeterministicRangeEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) tier: String,
    pub(in crate::db::query::intent::tests) score: u32,
    pub(in crate::db::query::intent::tests) handle: String,
    pub(in crate::db::query::intent::tests) label: String,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
pub(in crate::db::query::intent::tests) struct PlanOrderOnlyChoiceEntity {
    pub(in crate::db::query::intent::tests) id: Ulid,
    pub(in crate::db::query::intent::tests) alpha: String,
    pub(in crate::db::query::intent::tests) beta: String,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq)]
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

crate::test_canister! {
    vis = pub(in crate::db::query::intent::tests),
    ident = PlanCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    vis = pub(in crate::db::query::intent::tests),
    ident = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity! {
    ident = PlanSimpleEntity,
    entity_name = "PlanSimpleEntity",
    tag = crate::testing::SIMPLE_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
    ],
    indexes = [],
}

crate::test_entity! {
    ident = PlanEntity,
    entity_name = "PlanEntity",
    tag = crate::testing::PLAN_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [],
}

crate::test_singleton_entity! {
    ident = PlanSingleton,
    entity_name = "PlanSingleton",
    tag = crate::testing::PLAN_SINGLETON_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Unit,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Unit => FieldKind::Unit },
    ],
    indexes = [],
}

crate::test_entity! {
    ident = PlanNumericEntity,
    entity_name = "PlanNumericEntity",
    tag = crate::testing::PLAN_NUMERIC_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { rank: i32 => FieldKind::Int64 },
    ],
    indexes = [],
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

#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) static PLAN_BRANCH_SET_INDEX_FIELDS: [&str; 3] =
    ["collection_id", "stage", "id"];
#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) static PLAN_BRANCH_SET_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated(
        "collection_stage_id",
        PlanDataStore::PATH,
        &PLAN_BRANCH_SET_INDEX_FIELDS,
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

#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) static PLAN_EXPRESSION_CASEFOLD_INDEX_FIELDS: [&str; 1] =
    ["email"];
#[cfg(feature = "sql")]
pub(in crate::db::query::intent::tests) static PLAN_EXPRESSION_CASEFOLD_KEY_ITEMS: [IndexKeyItem;
    1] = [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
#[cfg(feature = "sql")]
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

pub(in crate::db::query::intent::tests) static PLAN_PHASE_TAG_KIND: FieldKind = FieldKind::Nat64;

crate::test_entity! {
    ident = PlanPushdownEntity,
    entity_name = "PlanPushdownEntity",
    tag = crate::testing::PUSHDOWN_PARITY_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { group: u32 => FieldKind::Nat64 },
        crate::test_field! { rank: u32 => FieldKind::Nat64 },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&PLAN_PUSHDOWN_INDEX_MODELS[0]],
}

#[cfg(feature = "sql")]
crate::test_entity! {
    ident = PlanBranchSetEntity,
    entity_name = "PlanBranchSetEntity",
    tag = crate::testing::PLAN_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { collection_id: String => FieldKind::Text { max_len: None } },
        crate::test_field! { stage: String => FieldKind::Text { max_len: None } },
        crate::test_field! { title: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&PLAN_BRANCH_SET_INDEX_MODELS[0]],
}

crate::test_entity! {
    ident = PlanUniqueRangeEntity,
    entity_name = "PlanUniqueRangeEntity",
    tag = crate::testing::UNIQUE_INDEX_RANGE_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { code: u32 => FieldKind::Nat64 },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&PLAN_UNIQUE_RANGE_INDEX_MODELS[0]],
}

crate::test_entity! {
    ident = PlanTextPrefixEntity,
    entity_name = "PlanTextPrefixEntity",
    tag = crate::testing::TEXT_PREFIX_PARITY_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&PLAN_TEXT_PREFIX_INDEX_MODELS[0]],
}

crate::test_entity! {
    ident = PlanPhaseEntity,
    entity_name = "PlanPhaseEntity",
    tag = crate::testing::PHASE_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { tags: Vec<u32> => FieldKind::List(&PLAN_PHASE_TAG_KIND) },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [],
}

#[cfg(feature = "sql")]
crate::test_entity! {
    ident = PlanExpressionCasefoldEntity,
    entity_name = "PlanExpressionCasefoldEntity",
    tag = crate::testing::PLAN_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { email: String => FieldKind::Text { max_len: None } },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS[0]],
}

crate::test_entity! {
    ident = PlanDeterministicChoiceEntity,
    entity_name = "PlanDeterministicChoiceEntity",
    tag = crate::testing::PLAN_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { tier: String => FieldKind::Text { max_len: None } },
        crate::test_field! { handle: String => FieldKind::Text { max_len: None } },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [
        &PLAN_DETERMINISTIC_CHOICE_INDEX_MODELS[0],
        &PLAN_DETERMINISTIC_CHOICE_INDEX_MODELS[1],
    ],
}

crate::test_entity! {
    ident = PlanDeterministicRangeEntity,
    entity_name = "PlanDeterministicRangeEntity",
    tag = crate::testing::PLAN_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { tier: String => FieldKind::Text { max_len: None } },
        crate::test_field! { score: u32 => FieldKind::Nat64 },
        crate::test_field! { handle: String => FieldKind::Text { max_len: None } },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [
        &PLAN_DETERMINISTIC_RANGE_INDEX_MODELS[0],
        &PLAN_DETERMINISTIC_RANGE_INDEX_MODELS[1],
    ],
}

crate::test_entity! {
    ident = PlanOrderOnlyChoiceEntity,
    entity_name = "PlanOrderOnlyChoiceEntity",
    tag = crate::testing::PLAN_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { alpha: String => FieldKind::Text { max_len: None } },
        crate::test_field! { beta: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [
        &PLAN_ORDER_ONLY_CHOICE_INDEX_MODELS[0],
        &PLAN_ORDER_ONLY_CHOICE_INDEX_MODELS[1],
    ],
}

crate::test_entity! {
    ident = PlanTemporalBoundaryEntity,
    entity_name = "PlanTemporalBoundaryEntity",
    tag = crate::testing::TEMPORAL_BOUNDARY_ENTITY_TAG,
    store = PlanDataStore,
    canister = PlanCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { occurred_on: Date => FieldKind::Date },
        crate::test_field! { occurred_at: Timestamp => FieldKind::Timestamp },
        crate::test_field! { elapsed: Duration => FieldKind::Duration },
    ],
    indexes = [],
}
