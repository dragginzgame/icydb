use super::*;
use crate::{
    db::query::{
        plan::planner::plan_access,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo},
    },
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
        index::IndexModel,
    },
    types::Ulid,
    value::Value,
};

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel = IndexModel::new(
    "plan_tests::idx_tag",
    "plan_tests::IndexStore",
    &INDEX_FIELDS,
    false,
);
const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

fn field(name: &'static str, kind: EntityFieldKind) -> EntityFieldModel {
    EntityFieldModel { name, kind }
}

fn model_with_index() -> EntityModel {
    // Leak the field list to satisfy the model's static lifetime contract in tests.
    let fields: &'static [EntityFieldModel] = Box::leak(
        vec![
            field("id", EntityFieldKind::Ulid),
            field("tag", EntityFieldKind::Text),
        ]
        .into_boxed_slice(),
    );
    let primary_key = &fields[0];

    EntityModel {
        path: "plan_tests::Entity",
        entity_name: "PlanEntity",
        primary_key,
        fields,
        indexes: &INDEXES,
    }
}

#[test]
fn plan_access_full_scan_without_predicate() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(&model).expect("schema should validate");
    let plan = plan_access(&model, &schema, None).expect("plan should build");

    assert_eq!(plan, AccessPlan::full_scan());
}

#[test]
fn plan_access_uses_primary_key_lookup() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(&model).expect("schema should validate");
    let key = Ulid::generate();
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::Eq,
        Value::Ulid(key),
        CoercionId::Strict,
    ));

    let plan = plan_access(&model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(plan, AccessPlan::Path(AccessPath::ByKey(Value::Ulid(key))));
}

#[test]
fn plan_access_uses_index_prefix_for_exact_match() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(&model).expect("schema should validate");
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::Eq,
        Value::Text("alpha".to_string()),
        CoercionId::Strict,
    ));

    let plan = plan_access(&model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::Path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![Value::Text("alpha".to_string())],
        })
    );
}

#[test]
fn plan_access_ignores_non_strict_predicates() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(&model).expect("schema should validate");
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::Eq,
        Value::Text("alpha".to_string()),
        CoercionId::TextCasefold,
    ));

    let plan = plan_access(&model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(plan, AccessPlan::full_scan());
}
