use super::*;
use crate::{
    db::query::{
        plan::planner::plan_access,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo},
    },
    model::{entity::EntityModel, field::EntityFieldKind, index::IndexModel},
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};
use std::ops::Bound;

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel = IndexModel::new(
    "plan_tests::idx_tag",
    "plan_tests::IndexStore",
    &INDEX_FIELDS,
    false,
);
const RANGE_INDEX_FIELDS: [&str; 3] = ["a", "b", "c"];
const RANGE_INDEX_MODEL: IndexModel = IndexModel::new(
    "plan_tests::idx_abc",
    "plan_tests::RangeIndexStore",
    &RANGE_INDEX_FIELDS,
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

crate::test_entity_schema! {
    PlanRangeEntity,
    id = Ulid,
    path = "plan_tests::RangeEntity",
    entity_name = "PlanRangeEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", EntityFieldKind::Ulid),
        ("a", EntityFieldKind::Uint),
        ("b", EntityFieldKind::Uint),
        ("c", EntityFieldKind::Uint),
    ],
    indexes = [&RANGE_INDEX_MODEL],
}

// Helper for tests that need the indexed model derived from a typed schema.
fn model_with_index() -> &'static EntityModel {
    <PlanModelEntity as EntitySchema>::MODEL
}

fn model_with_range_index() -> &'static EntityModel {
    <PlanRangeEntity as EntitySchema>::MODEL
}

fn compare_strict(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        value,
        CoercionId::Strict,
    ))
}

fn compare_numeric_widen(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        value,
        CoercionId::NumericWiden,
    ))
}

type IndexRangeView<'a> = (
    &'a IndexModel,
    &'a [Value],
    &'a Bound<Value>,
    &'a Bound<Value>,
);

fn find_index_range(plan: &'_ AccessPlan<Value>) -> Option<IndexRangeView<'_>> {
    match plan {
        AccessPlan::Path(path) => match path.as_ref() {
            AccessPath::IndexRange {
                index,
                prefix,
                lower,
                upper,
            } => Some((index, prefix, lower, upper)),
            _ => None,
        },
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            children.iter().find_map(find_index_range)
        }
    }
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

    assert_eq!(plan, AccessPlan::path(AccessPath::ByKey(Value::Ulid(key))));
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
        AccessPlan::path(AccessPath::IndexPrefix {
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
fn plan_access_emits_index_range_for_prefix_plus_range() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gte, Value::Uint(100)),
        compare_strict("b", CompareOp::Lt, Value::Uint(200)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (index, prefix, lower, upper) =
        find_index_range(&plan).expect("plan should include index range");

    assert_eq!(index.name, RANGE_INDEX_MODEL.name);
    assert_eq!(prefix, [Value::Uint(7)].as_slice());
    assert_eq!(lower, &Bound::Included(Value::Uint(100)));
    assert_eq!(upper, &Bound::Excluded(Value::Uint(200)));
}

#[test]
fn plan_access_rejects_trailing_equality_after_range() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gte, Value::Uint(100)),
        compare_strict("c", CompareOp::Eq, Value::Uint(3)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    assert!(
        find_index_range(&plan).is_none(),
        "range path should be rejected when equality appears after range field"
    );
}

#[test]
fn plan_access_rejects_range_with_missing_prefix_component() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("c", CompareOp::Gte, Value::Uint(100)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    assert!(
        find_index_range(&plan).is_none(),
        "range path should be rejected when first non-equality component is skipped"
    );
}

#[test]
fn plan_access_rejects_range_before_prefix_equality() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Gte, Value::Uint(7)),
        compare_strict("b", CompareOp::Eq, Value::Uint(3)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    assert!(
        find_index_range(&plan).is_none(),
        "range path should be rejected when equality appears after a leading range field"
    );
}

#[test]
fn plan_access_merges_duplicate_lower_bounds_to_stricter_value() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gte, Value::Uint(50)),
        compare_strict("b", CompareOp::Gt, Value::Uint(80)),
        compare_strict("b", CompareOp::Lte, Value::Uint(200)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (_, _, lower, upper) = find_index_range(&plan).expect("plan should include index range");
    assert_eq!(lower, &Bound::Excluded(Value::Uint(80)));
    assert_eq!(upper, &Bound::Included(Value::Uint(200)));
}

#[test]
fn plan_access_rejects_empty_exclusive_interval() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gt, Value::Uint(100)),
        compare_strict("b", CompareOp::Lt, Value::Uint(100)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    assert!(
        find_index_range(&plan).is_none(),
        "exclusive equal bounds should be rejected as empty interval"
    );
}

#[test]
fn plan_access_rejects_mixed_numeric_variants_for_range_bounds() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_numeric_widen("b", CompareOp::Gte, Value::Int(100)),
        compare_numeric_widen("b", CompareOp::Lte, Value::Uint(200)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    assert!(
        find_index_range(&plan).is_none(),
        "mixed numeric variants should fall back until canonical coercion is implemented"
    );
}
