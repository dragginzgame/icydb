use super::*;
use crate::{
    db::query::{
        plan::planner::plan_access,
        predicate::{CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate, SchemaInfo},
    },
    model::{entity::EntityModel, field::EntityFieldKind, index::IndexModel},
    traits::EntitySchema,
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

crate::test_entity_schema! {
    PlanModelEntity,
    id = Ulid,
    path = "plan_tests::Entity",
    entity_name = "PlanEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", EntityFieldKind::Ulid),
        ("tag", EntityFieldKind::Text),
    ],
    indexes = [&INDEX_MODEL],
}

// Helper for tests that need the indexed model derived from a typed schema.
fn model_with_index() -> &'static EntityModel {
    <PlanModelEntity as EntitySchema>::MODEL
}

#[test]
fn plan_access_full_scan_without_predicate() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let plan = plan_access(model, &schema, None).expect("plan should build");

    assert_eq!(plan, AccessPlan::full_scan());
}

#[test]
fn plan_access_uses_primary_key_lookup() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let key = Ulid::generate();
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::Eq,
        Value::Ulid(key),
        CoercionId::Strict,
    ));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(plan, AccessPlan::Path(AccessPath::ByKey(Value::Ulid(key))));
}

#[test]
fn plan_access_uses_index_prefix_for_exact_match() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::Eq,
        Value::Text("alpha".to_string()),
        CoercionId::Strict,
    ));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

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
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::Eq,
        Value::Text("alpha".to_string()),
        CoercionId::TextCasefold,
    ));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(plan, AccessPlan::full_scan());
}

#[test]
fn plan_access_rejects_map_predicates() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::MapContainsEntry {
        field: "tag".to_string(),
        key: Value::Text("k".to_string()),
        value: Value::Uint(1u64),
        coercion: CoercionSpec::new(CoercionId::Strict),
    };

    let err = plan_access(model, &schema, Some(&predicate))
        .expect_err("map predicates must be rejected during planning");
    assert!(format!("{err}").contains("map predicates must be rejected before planning"));
}
