use super::*;
use crate::{
    db::query::plan::{AccessPath, AccessPlan},
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
    },
    types::Ulid,
};

fn field(name: &'static str, kind: EntityFieldKind) -> EntityFieldModel {
    EntityFieldModel { name, kind }
}

fn model_with_fields(fields: Vec<EntityFieldModel>, pk_index: usize) -> EntityModel {
    // Leak the field list to satisfy the model's static lifetime contract in tests.
    let fields: &'static [EntityFieldModel] = Box::leak(fields.into_boxed_slice());
    let primary_key = &fields[pk_index];
    let indexes: &'static [&'static crate::model::index::IndexModel] = &[];

    EntityModel {
        path: "intent_tests::Entity",
        entity_name: "IntentEntity",
        primary_key,
        fields,
        indexes,
    }
}

fn basic_model() -> EntityModel {
    model_with_fields(
        vec![
            field("id", EntityFieldKind::Ulid),
            field("name", EntityFieldKind::Text),
        ],
        0,
    )
}

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
fn build_plan_model_full_scan_without_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(&model, ReadConsistency::MissingOk);
    let plan = intent.build_plan_model().expect("model plan should build");

    assert!(matches!(
        plan.access,
        AccessPlan::Path(AccessPath::FullScan)
    ));
}
