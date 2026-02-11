use super::*;
use crate::{
    db::query::{
        FieldRef,
        plan::{AccessPath, AccessPlan, LogicalPlan},
        predicate::{CompareOp, ComparePredicate},
    },
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
        index::IndexModel,
    },
    test_fixtures::entity_model_from_static,
    traits::{
        AsView, CanisterKind, DataStoreKind, EntityIdentity, EntityKey, EntityKind,
        EntityPlacement, EntitySchema, EntityValue, FieldValue, FieldValues, Path, SanitizeAuto,
        SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    types::{Id, Ulid, Unit},
    value::Value,
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

impl AsView for PlanEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for PlanEntity {}
impl SanitizeCustom for PlanEntity {}
impl ValidateAuto for PlanEntity {}
impl ValidateCustom for PlanEntity {}
impl Visitable for PlanEntity {}

impl Path for PlanEntity {
    const PATH: &'static str = "intent_tests::PlanEntity";
}

impl EntityKey for PlanEntity {
    type Key = Ulid;
}

impl EntityIdentity for PlanEntity {
    const ENTITY_NAME: &'static str = "PlanEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static PLAN_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "name",
        kind: EntityFieldKind::Text,
    },
];
static PLAN_FIELD_NAMES: [&str; 2] = ["id", "name"];
static PLAN_INDEXES: [&IndexModel; 0] = [];

// Manual models keep typed-vs-model planning parity tests independent of schema macros.
static PLAN_MODEL: EntityModel = entity_model_from_static(
    "intent_tests::PlanEntity",
    "PlanEntity",
    &PLAN_FIELDS[0],
    &PLAN_FIELDS,
    &PLAN_INDEXES,
);

static MAP_PLAN_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "attributes",
        kind: EntityFieldKind::Map {
            key: &EntityFieldKind::Text,
            value: &EntityFieldKind::Uint,
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

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct PlanSingleton {
    id: Unit,
}

impl AsView for PlanSingleton {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for PlanSingleton {}
impl SanitizeCustom for PlanSingleton {}
impl ValidateAuto for PlanSingleton {}
impl ValidateCustom for PlanSingleton {}
impl Visitable for PlanSingleton {}

impl Path for PlanSingleton {
    const PATH: &'static str = "intent_tests::PlanSingleton";
}

impl EntityKey for PlanSingleton {
    type Key = Unit;
}

impl EntityIdentity for PlanSingleton {
    const ENTITY_NAME: &'static str = "PlanSingleton";
    const PRIMARY_KEY: &'static str = "id";
}

impl FieldValues for PlanSingleton {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(self.id.to_value()),
            _ => None,
        }
    }
}

impl EntityValue for PlanSingleton {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

static SINGLETON_FIELDS: [EntityFieldModel; 1] = [EntityFieldModel {
    name: "id",
    kind: EntityFieldKind::Unit,
}];
static SINGLETON_FIELD_NAMES: [&str; 1] = ["id"];
static SINGLETON_INDEXES: [&IndexModel; 0] = [];

// Singleton model is hand-built to exercise model-only planning.
static SINGLETON_MODEL: EntityModel = entity_model_from_static(
    "intent_tests::PlanSingleton",
    "PlanSingleton",
    &SINGLETON_FIELDS[0],
    &SINGLETON_FIELDS,
    &SINGLETON_INDEXES,
);

struct PlanCanister;
struct PlanDataStore;

impl Path for PlanCanister {
    const PATH: &'static str = "intent_tests::PlanCanister";
}

impl CanisterKind for PlanCanister {}

impl Path for PlanDataStore {
    const PATH: &'static str = "intent_tests::PlanDataStore";
}

impl DataStoreKind for PlanDataStore {
    type Canister = PlanCanister;
}

impl EntitySchema for PlanEntity {
    const MODEL: &'static EntityModel = &PLAN_MODEL;
    const FIELDS: &'static [&'static str] = &PLAN_FIELD_NAMES;
    const INDEXES: &'static [&'static IndexModel] = &PLAN_INDEXES;
}

impl EntityPlacement for PlanEntity {
    type DataStore = PlanDataStore;
    type Canister = PlanCanister;
}

impl EntityKind for PlanEntity {}

impl EntitySchema for PlanSingleton {
    const MODEL: &'static EntityModel = &SINGLETON_MODEL;
    const FIELDS: &'static [&'static str] = &SINGLETON_FIELD_NAMES;
    const INDEXES: &'static [&'static IndexModel] = &SINGLETON_INDEXES;
}

impl EntityPlacement for PlanSingleton {
    type DataStore = PlanDataStore;
    type Canister = PlanCanister;
}

impl EntityKind for PlanSingleton {}
impl SingletonEntity for PlanSingleton {}

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
        Err(IntentError::DeleteLimitRequiresOrder)
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
        QueryError::Plan(crate::db::query::plan::PlanError::UnorderedPagination)
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
        QueryError::Plan(crate::db::query::plan::PlanError::UnorderedPagination)
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
        QueryError::Plan(crate::db::query::plan::PlanError::UnorderedPagination)
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
fn intent_rejects_empty_order_spec() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, ReadConsistency::MissingOk)
        .order_spec(OrderSpec { fields: Vec::new() });

    assert!(matches!(
        intent.validate_intent(),
        Err(IntentError::EmptyOrderSpec)
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
        AccessPlan::Path(AccessPath::ByKey(Unit))
    ));
}

#[test]
fn build_plan_model_full_scan_without_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, ReadConsistency::MissingOk);
    let plan = intent.build_plan_model().expect("model plan should build");

    assert!(matches!(
        plan.access,
        AccessPlan::Path(AccessPath::FullScan)
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
    let LogicalPlan {
        mode,
        access,
        predicate: plan_predicate,
        order,
        delete_limit,
        page,
        consistency,
    } = model_plan;

    let access = access_plan_to_entity_keys::<PlanEntity>(PlanEntity::MODEL, access)
        .expect("convert access plan");
    let model_as_typed = LogicalPlan {
        mode,
        access,
        predicate: plan_predicate,
        order,
        delete_limit,
        page,
        consistency,
    };

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
fn build_plan_model_rejects_map_field_predicates_before_planning() {
    let intent = QueryModel::<Ulid>::new(&MAP_PLAN_MODEL, ReadConsistency::MissingOk).filter(
        Predicate::Compare(ComparePredicate::with_coercion(
            "attributes",
            CompareOp::Eq,
            Value::Map(Vec::new()),
            crate::db::query::predicate::CoercionId::Strict,
        )),
    );

    let err = intent
        .build_plan_model()
        .expect_err("map field predicates must be rejected before planning");
    assert!(matches!(
        err,
        QueryError::Plan(crate::db::query::plan::PlanError::PredicateInvalid(
            crate::db::query::predicate::ValidateError::UnsupportedQueryFeature(
                crate::db::query::predicate::UnsupportedQueryFeature::MapPredicate { field }
            )
        )) if field == "attributes"
    ));
}
