use super::*;
use crate::{
    db::query::{
        FieldRef,
        plan::{AccessPath, AccessPlan, LogicalPlan},
    },
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
        index::IndexModel,
    },
    test_fixtures::LegacyTestEntityModel,
    traits::{
        CanisterKind, DataStoreKind, EntityIdentity, EntityKind, EntityPlacement, EntitySchema,
        EntityValue, FieldValue, FieldValues, Path, SanitizeAuto, SanitizeCustom, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    types::{Ref, Ulid, Unit},
    value::Value,
};
use serde::{Deserialize, Serialize};

fn field(name: &'static str, kind: EntityFieldKind) -> EntityFieldModel {
    EntityFieldModel { name, kind }
}

// NOTE: Intent tests use legacy manual models to exercise model-only planning logic.
fn basic_model() -> EntityModel {
    LegacyTestEntityModel::from_fields(
        vec![
            field("id", EntityFieldKind::Ulid),
            field("name", EntityFieldKind::Text),
        ],
        0,
    )
}

// Test-only entity to compare typed vs model planning without schema macros.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct PlanEntity {
    id: Ulid,
    name: String,
}

impl View for PlanEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
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

impl EntityIdentity for PlanEntity {
    type Id = Ulid;

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

// Legacy test scaffolding: manual models keep typed-vs-model planning parity
// tests independent of schema macros.
static PLAN_MODEL: EntityModel = LegacyTestEntityModel::from_static(
    "intent_tests::PlanEntity",
    "PlanEntity",
    &PLAN_FIELDS[0],
    &PLAN_FIELDS,
    &PLAN_INDEXES,
);

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct PlanSingleton {
    id: Unit,
}

impl View for PlanSingleton {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
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

impl EntityIdentity for PlanSingleton {
    type Id = Unit;

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
    fn id(&self) -> Self::Id {
        self.id
    }

    fn set_id(&mut self, id: Self::Id) {
        self.id = id;
    }
}

static SINGLETON_FIELDS: [EntityFieldModel; 1] = [EntityFieldModel {
    name: "id",
    kind: EntityFieldKind::Unit,
}];
static SINGLETON_FIELD_NAMES: [&str; 1] = ["id"];
static SINGLETON_INDEXES: [&IndexModel; 0] = [];

// Legacy test scaffolding: singleton model is hand-built to exercise model-only planning.
static SINGLETON_MODEL: EntityModel = LegacyTestEntityModel::from_static(
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
fn intent_rejects_many_with_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(&model, ReadConsistency::MissingOk)
        .by_keys([Ulid::generate()])
        .filter(Predicate::True);

    assert!(matches!(
        intent.validate_intent(),
        Err(IntentError::ManyWithPredicate)
    ));
}

#[test]
fn intent_rejects_only_with_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(&model, ReadConsistency::MissingOk)
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
    let intent = QueryModel::<Ulid>::new(&model, ReadConsistency::MissingOk)
        .delete()
        .limit(1);

    assert!(matches!(
        intent.validate_intent(),
        Err(IntentError::DeleteLimitRequiresOrder)
    ));
}

#[test]
fn intent_rejects_empty_order_spec() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(&model, ReadConsistency::MissingOk)
        .order_spec(OrderSpec { fields: Vec::new() });

    assert!(matches!(
        intent.validate_intent(),
        Err(IntentError::EmptyOrderSpec)
    ));
}

#[test]
fn intent_rejects_conflicting_key_access() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(&model, ReadConsistency::MissingOk)
        .by_key(Ulid::generate())
        .by_keys([Ulid::generate()]);

    assert!(matches!(
        intent.validate_intent(),
        Err(IntentError::KeyAccessConflict)
    ));
}

#[test]
fn typed_by_ref_matches_by_key_access() {
    let key = Ulid::generate();

    let by_key = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .by_key(key)
        .plan()
        .expect("by_key plan")
        .into_inner();
    let by_ref = Query::<PlanEntity>::new(ReadConsistency::MissingOk)
        .by_ref(Ref::new(key))
        .plan()
        .expect("by_ref plan")
        .into_inner();

    assert_eq!(by_key, by_ref);
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
    let intent = QueryModel::<Ulid>::new(&model, ReadConsistency::MissingOk);
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
