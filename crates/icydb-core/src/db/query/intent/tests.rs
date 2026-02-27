use super::*;
use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        contracts::{CompareOp, ComparePredicate},
        query::{
            builder::field::FieldRef,
            expr::FilterExpr,
            plan::{AccessPlannedQuery, LogicalPlan},
        },
    },
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel},
        index::IndexModel,
    },
    testing::entity_model_from_static,
    traits::{EntitySchema, FieldProjection, FieldValue},
    types::{Ulid, Unit},
    value::{Value, ValueEnum},
};
use serde::{Deserialize, Serialize};

// Helper for intent tests that need the typed model snapshot.
fn basic_model() -> &'static EntityModel {
    <PlanEntity as EntitySchema>::MODEL
}

// Test-only entity to compare typed vs model planning without schema macros.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct PlanEntity {
    id: Ulid,
    name: String,
}

static MAP_PLAN_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "attributes",
        kind: FieldKind::Map {
            key: &FieldKind::Text,
            value: &FieldKind::Uint,
        },
    },
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
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "stage",
        kind: FieldKind::Enum {
            path: "intent_tests::Stage",
        },
    },
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
    ident = PlanEntity,
    id = Ulid,
    entity_name = "PlanEntity",
    primary_key = "id",
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
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Unit),
    ],
    indexes = [],
    store = PlanDataStore,
    canister = PlanCanister,
}

#[test]
fn intent_rejects_by_ids_with_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, ReadConsistency::MissingOk)
        .by_ids([Ulid::generate()])
        .filter(Predicate::True);

    assert!(matches!(
        intent.validate_intent(),
        Err(IntentError::ByIdsWithPredicate)
    ));
}

#[test]
fn intent_rejects_only_with_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, ReadConsistency::MissingOk)
        .only(Ulid::generate())
        .filter(Predicate::True);

    assert!(matches!(
        intent.validate_intent(),
        Err(IntentError::OnlyWithPredicate)
    ));
}

#[test]
fn intent_rejects_delete_limit_without_order() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, ReadConsistency::MissingOk)
        .delete()
        .limit(1);

    assert!(matches!(
        intent.validate_intent(),
        Err(IntentError::PlanShape(
            crate::db::policy::PlanPolicyError::DeleteLimitRequiresOrder
        ))
    ));
}

#[test]
fn load_limit_without_order_rejects_unordered_pagination() {
    let err = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .limit(1)
        .plan()
        .expect_err("limit without order must fail");

    assert!(matches!(
        err,
        QueryError::Plan(ref plan_err)
            if matches!(
                **plan_err,
                crate::db::query::plan::PlanError::Policy(ref inner)
                    if matches!(
                        inner.as_ref(),
                        crate::db::query::plan_validate::PolicyPlanError::UnorderedPagination
                    )
            )
    ));
}

#[test]
fn load_rejects_duplicate_non_primary_order_field() {
    let err = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .order_by("name")
        .order_by_desc("name")
        .limit(1)
        .plan()
        .expect_err("duplicate non-primary order field must fail");

    assert!(matches!(
        err,
        QueryError::Plan(ref plan_err)
            if matches!(
                **plan_err,
                crate::db::query::plan::PlanError::Order(ref inner)
                    if matches!(
                        inner.as_ref(),
                        crate::db::query::plan_validate::OrderPlanError::DuplicateOrderField { field }
                            if field == "name"
                    )
            )
    ));
}

#[test]
fn load_offset_without_order_rejects_unordered_pagination() {
    let err = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .offset(1)
        .plan()
        .expect_err("offset without order must fail");

    assert!(matches!(
        err,
        QueryError::Plan(ref plan_err)
            if matches!(
                **plan_err,
                crate::db::query::plan::PlanError::Policy(ref inner)
                    if matches!(
                        inner.as_ref(),
                        crate::db::query::plan_validate::PolicyPlanError::UnorderedPagination
                    )
            )
    ));
}

#[test]
fn load_limit_and_offset_without_order_rejects_unordered_pagination() {
    let err = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .limit(10)
        .offset(2)
        .plan()
        .expect_err("limit+offset without order must fail");

    assert!(matches!(
        err,
        QueryError::Plan(ref plan_err)
            if matches!(
                **plan_err,
                crate::db::query::plan::PlanError::Policy(ref inner)
                    if matches!(
                        inner.as_ref(),
                        crate::db::query::plan_validate::PolicyPlanError::UnorderedPagination
                    )
            )
    ));
}

#[test]
fn load_ordered_pagination_is_allowed() {
    Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .order_by("name")
        .limit(10)
        .offset(2)
        .plan()
        .expect("ordered pagination should plan");
}

#[test]
fn ordered_plan_appends_primary_key_tie_break() {
    let plan = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .order_by("name")
        .plan()
        .expect("ordered plan should build")
        .into_inner();
    let order = plan
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
    let plan = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("id")
        .order_by("name")
        .plan()
        .expect("ordered plan should build")
        .into_inner();
    let order = plan
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
    let intent = QueryModel::<Ulid>::new(model, ReadConsistency::MissingOk)
        .order_spec(OrderSpec { fields: Vec::new() });

    assert!(matches!(
        intent.validate_intent(),
        Err(IntentError::PlanShape(
            crate::db::policy::PlanPolicyError::EmptyOrderSpec
        ))
    ));
}

#[test]
fn intent_rejects_conflicting_key_access() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, ReadConsistency::MissingOk)
        .by_id(Ulid::generate())
        .by_ids([Ulid::generate()]);

    assert!(matches!(
        intent.validate_intent(),
        Err(IntentError::KeyAccessConflict)
    ));
}

#[test]
fn typed_by_ids_matches_by_id_access() {
    let key = Ulid::generate();

    let by_id = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .by_id(key)
        .plan()
        .expect("by_id plan")
        .into_inner();
    let by_ids = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .by_ids([key])
        .plan()
        .expect("by_ids plan")
        .into_inner();

    assert_eq!(by_id, by_ids);
}

#[test]
fn singleton_only_uses_default_key() {
    let plan = Query::<PlanSingleton>::new(ReadConsistency::MissingOk)
        .only()
        .plan()
        .expect("singleton plan")
        .into_inner();

    assert!(matches!(
        plan.access,
        AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::ByKey(Unit))
    ));
}

#[test]
fn build_plan_model_full_scan_without_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, ReadConsistency::MissingOk);
    let plan = intent.build_plan_model().expect("model plan should build");

    assert!(matches!(
        plan.access,
        AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::FullScan)
    ));
}

#[test]
fn typed_plan_matches_model_plan_for_same_intent() {
    let predicate = FieldRef::new("id").eq(Ulid::default());

    let model_intent = QueryModel::<Ulid>::new(PlanEntity::MODEL, ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("name")
        .limit(10)
        .offset(2);

    let model_plan = model_intent.build_plan_model().expect("model plan");
    let (model_logical, model_access) = model_plan.into_parts();
    let LogicalPlan {
        mode,
        predicate: plan_predicate,
        order,
        distinct,
        delete_limit,
        page,
        consistency,
    } = model_logical;

    let access = access_plan_to_entity_keys::<PlanEntity>(PlanEntity::MODEL, model_access)
        .expect("convert access plan");
    let model_as_typed = AccessPlannedQuery::from_parts(
        LogicalPlan {
            mode,
            predicate: plan_predicate,
            order,
            distinct,
            delete_limit,
            page,
            consistency,
        },
        access,
    );

    let typed_plan = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
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
    let plan = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .plan()
        .expect("typed plan")
        .into_inner();

    assert!(
        !plan.distinct,
        "distinct should default to false for new query intents"
    );
}

#[test]
fn query_distinct_sets_logical_plan_flag() {
    let plan = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .distinct()
        .plan()
        .expect("typed plan")
        .into_inner();

    assert!(
        plan.distinct,
        "distinct should be true when query intent enables distinct"
    );
}

#[test]
fn build_plan_model_rejects_map_field_predicates_before_planning() {
    let intent = QueryModel::<Ulid>::new(&MAP_PLAN_MODEL, ReadConsistency::MissingOk).filter(
        Predicate::Compare(ComparePredicate::with_coercion(
            "attributes",
            CompareOp::Eq,
            Value::Map(Vec::new()),
            crate::db::contracts::CoercionId::Strict,
        )),
    );

    let err = intent
        .build_plan_model()
        .expect_err("map field predicates must be rejected before planning");
    assert!(matches!(
        err,
        QueryError::Plan(ref plan_err)
            if matches!(
                **plan_err,
                crate::db::query::plan::PlanError::PredicateInvalid(ref inner)
                    if matches!(
                        inner.as_ref(),
                        crate::db::contracts::ValidateError::UnsupportedQueryFeature(
                            crate::db::contracts::UnsupportedQueryFeature::MapPredicate {
                                field
                            }
                        ) if field == "attributes"
                    )
            )
    ));
}

#[test]
fn filter_expr_resolves_loose_enum_stage_filters() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::Eq,
        Value::Enum(ValueEnum::loose("Active")),
        crate::db::contracts::CoercionId::Strict,
    ));

    let intent = QueryModel::<Ulid>::new(&ENUM_PLAN_MODEL, ReadConsistency::MissingOk)
        .filter_expr(FilterExpr(predicate))
        .expect("filter expr should lower");
    let plan = intent.build_plan_model().expect("plan should build");

    let Some(Predicate::Compare(cmp)) = plan.predicate.as_ref() else {
        panic!("expected compare predicate");
    };
    let Value::Enum(stage) = &cmp.value else {
        panic!("expected enum literal");
    };
    assert_eq!(stage.path.as_deref(), Some("intent_tests::Stage"));
}

#[test]
fn filter_expr_rejects_wrong_strict_enum_path() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::Eq,
        Value::Enum(ValueEnum::new("Active", Some("wrong::Stage"))),
        crate::db::contracts::CoercionId::Strict,
    ));

    let err = QueryModel::<Ulid>::new(&ENUM_PLAN_MODEL, ReadConsistency::MissingOk)
        .filter_expr(FilterExpr(predicate))
        .expect_err("strict enum with wrong path should fail");
    assert!(matches!(
        err,
        QueryError::Validate(crate::db::contracts::ValidateError::InvalidLiteral {
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
        crate::db::contracts::CoercionId::Strict,
    ));

    let plan = QueryModel::<Ulid>::new(&ENUM_PLAN_MODEL, ReadConsistency::MissingOk)
        .filter(predicate)
        .build_plan_model()
        .expect("direct filter should build");
    let Some(Predicate::Compare(cmp)) = plan.predicate.as_ref() else {
        panic!("expected compare predicate");
    };
    let Value::Enum(stage) = &cmp.value else {
        panic!("expected enum literal");
    };
    assert_eq!(stage.path.as_deref(), Some("intent_tests::Stage"));
}
