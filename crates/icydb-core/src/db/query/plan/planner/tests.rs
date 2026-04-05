//! Module: db::query::plan::planner::tests
//! Responsibility: module-local ownership and contracts for db::query::plan::planner::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;
use crate::{
    db::{
        access::{SemanticIndexRangeSpec, normalize_access_plan_value},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate, normalize},
        query::{
            intent::{KeyAccess, build_access_plan_from_keys},
            plan::{OrderDirection, OrderSpec},
        },
    },
    model::{
        field::{FieldKind, FieldModel},
        index::{IndexExpression, IndexKeyItem, IndexModel},
    },
    testing::entity_model_from_static,
    types::Ulid,
};
use std::ops::Bound;

static PLANNER_CANONICAL_FIELDS: [FieldModel; 1] = [FieldModel::new("id", FieldKind::Ulid)];
static PLANNER_CANONICAL_INDEXES: [&IndexModel; 0] = [];
static PLANNER_CANONICAL_MODEL: EntityModel = entity_model_from_static(
    "planner::canonical_test_entity",
    "PlannerCanonicalTestEntity",
    &PLANNER_CANONICAL_FIELDS[0],
    &PLANNER_CANONICAL_FIELDS,
    &PLANNER_CANONICAL_INDEXES,
);

static PLANNER_IN_EMPTY_FIELDS: [FieldModel; 2] = [
    FieldModel::new("id", FieldKind::Ulid),
    FieldModel::new("email", FieldKind::Text),
];
static PLANNER_IN_EMPTY_INDEX_FIELDS: [&str; 1] = ["email"];
static PLANNER_IN_EMPTY_INDEXES: [IndexModel; 1] = [IndexModel::new(
    "email_idx",
    "planner::in_empty_test_entity",
    &PLANNER_IN_EMPTY_INDEX_FIELDS,
    false,
)];
static PLANNER_IN_EMPTY_INDEX_REFS: [&IndexModel; 1] = [&PLANNER_IN_EMPTY_INDEXES[0]];
static PLANNER_IN_EMPTY_MODEL: EntityModel = entity_model_from_static(
    "planner::in_empty_test_entity",
    "PlannerInEmptyTestEntity",
    &PLANNER_IN_EMPTY_FIELDS[0],
    &PLANNER_IN_EMPTY_FIELDS,
    &PLANNER_IN_EMPTY_INDEX_REFS,
);
static PLANNER_ORDER_FIELDS: [FieldModel; 3] = [
    FieldModel::new("id", FieldKind::Ulid),
    FieldModel::new("name", FieldKind::Text),
    FieldModel::new("active", FieldKind::Bool),
];
static PLANNER_ORDER_INDEX_FIELDS: [&str; 1] = ["name"];
static PLANNER_ORDER_INDEXES: [IndexModel; 1] = [IndexModel::new(
    "name_idx",
    "planner::order_test_entity",
    &PLANNER_ORDER_INDEX_FIELDS,
    false,
)];
static PLANNER_ORDER_INDEX_REFS: [&IndexModel; 1] = [&PLANNER_ORDER_INDEXES[0]];
static PLANNER_ORDER_MODEL: EntityModel = entity_model_from_static(
    "planner::order_test_entity",
    "PlannerOrderTestEntity",
    &PLANNER_ORDER_FIELDS[0],
    &PLANNER_ORDER_FIELDS,
    &PLANNER_ORDER_INDEX_REFS,
);
static PLANNER_ORDER_FILTERED_INDEXES: [IndexModel; 1] = [IndexModel::new_with_predicate(
    "name_idx_active_only",
    "planner::order_filtered_test_entity",
    &PLANNER_ORDER_INDEX_FIELDS,
    false,
    Some("active = true"),
)];
static PLANNER_ORDER_FILTERED_INDEX_REFS: [&IndexModel; 1] = [&PLANNER_ORDER_FILTERED_INDEXES[0]];
static PLANNER_ORDER_FILTERED_MODEL: EntityModel = entity_model_from_static(
    "planner::order_filtered_test_entity",
    "PlannerOrderFilteredTestEntity",
    &PLANNER_ORDER_FIELDS[0],
    &PLANNER_ORDER_FIELDS,
    &PLANNER_ORDER_FILTERED_INDEX_REFS,
);
static PLANNER_ORDER_COMPOSITE_FIELDS: [FieldModel; 4] = [
    FieldModel::new("id", FieldKind::Ulid),
    FieldModel::new("code", FieldKind::Text),
    FieldModel::new("serial", FieldKind::Uint),
    FieldModel::new("note", FieldKind::Text),
];
static PLANNER_ORDER_COMPOSITE_INDEX_FIELDS: [&str; 2] = ["code", "serial"];
static PLANNER_ORDER_COMPOSITE_INDEXES: [IndexModel; 1] = [IndexModel::new(
    "code_serial_idx",
    "planner::order_composite_test_entity",
    &PLANNER_ORDER_COMPOSITE_INDEX_FIELDS,
    false,
)];
static PLANNER_ORDER_COMPOSITE_INDEX_REFS: [&IndexModel; 1] = [&PLANNER_ORDER_COMPOSITE_INDEXES[0]];
static PLANNER_ORDER_COMPOSITE_MODEL: EntityModel = entity_model_from_static(
    "planner::order_composite_test_entity",
    "PlannerOrderCompositeTestEntity",
    &PLANNER_ORDER_COMPOSITE_FIELDS[0],
    &PLANNER_ORDER_COMPOSITE_FIELDS,
    &PLANNER_ORDER_COMPOSITE_INDEX_REFS,
);
static PLANNER_ORDER_EXPRESSION_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("name"))];
static PLANNER_ORDER_EXPRESSION_INDEXES: [IndexModel; 1] = [IndexModel::new_with_key_items(
    "name_lower_idx",
    "planner::order_expression_test_entity",
    &PLANNER_ORDER_INDEX_FIELDS,
    &PLANNER_ORDER_EXPRESSION_KEY_ITEMS,
    false,
)];
static PLANNER_ORDER_EXPRESSION_INDEX_REFS: [&IndexModel; 1] =
    [&PLANNER_ORDER_EXPRESSION_INDEXES[0]];
static PLANNER_ORDER_EXPRESSION_MODEL: EntityModel = entity_model_from_static(
    "planner::order_expression_test_entity",
    "PlannerOrderExpressionTestEntity",
    &PLANNER_ORDER_FIELDS[0],
    &PLANNER_ORDER_FIELDS,
    &PLANNER_ORDER_EXPRESSION_INDEX_REFS,
);

fn plan_access_for_test(
    model: &EntityModel,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<AccessPlan<Value>, PlannerError> {
    let normalized = predicate.map(normalize);

    plan_access(model, schema, normalized.as_ref())
}

fn plan_access_for_test_with_order(
    model: &EntityModel,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<OrderSpec>,
) -> Result<AccessPlan<Value>, PlannerError> {
    let normalized = predicate.map(normalize);

    plan_access_with_order(model, schema, normalized.as_ref(), order.as_ref())
}

fn canonical_order(fields: &[(&str, OrderDirection)]) -> OrderSpec {
    OrderSpec {
        fields: fields
            .iter()
            .map(|(field, direction)| ((*field).to_string(), *direction))
            .collect(),
    }
}

#[test]
fn normalize_union_dedups_identical_paths() {
    let key = Value::Ulid(Ulid::from_u128(1));
    let plan = AccessPlan::Union(vec![
        AccessPlan::by_key(key.clone()),
        AccessPlan::by_key(key),
    ]);

    let normalized = normalize_access_plan_value(plan);

    assert_eq!(
        normalized,
        AccessPlan::by_key(Value::Ulid(Ulid::from_u128(1)))
    );
}

#[test]
fn normalize_union_sorts_by_key() {
    let a = Value::Ulid(Ulid::from_u128(1));
    let b = Value::Ulid(Ulid::from_u128(2));
    let plan = AccessPlan::Union(vec![
        AccessPlan::by_key(b.clone()),
        AccessPlan::by_key(a.clone()),
    ]);

    let normalized = normalize_access_plan_value(plan);
    let AccessPlan::Union(children) = normalized else {
        panic!("expected union");
    };

    assert_eq!(children.len(), 2);
    assert_eq!(children[0], AccessPlan::by_key(a));
    assert_eq!(children[1], AccessPlan::by_key(b));
}

#[test]
fn normalize_intersection_removes_full_scan() {
    let key = Value::Ulid(Ulid::from_u128(7));
    let plan = AccessPlan::Intersection(vec![AccessPlan::full_scan(), AccessPlan::by_key(key)]);

    let normalized = normalize_access_plan_value(plan);

    assert_eq!(
        normalized,
        AccessPlan::by_key(Value::Ulid(Ulid::from_u128(7)))
    );
}

#[test]
fn planner_and_intent_access_canonicalization_match_for_single_key_set() {
    let key = Ulid::from_u128(42);
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::In,
        Value::List(vec![Value::Ulid(key)]),
        CoercionId::Strict,
    ));
    let schema = SchemaInfo::from_entity_model(&PLANNER_CANONICAL_MODEL)
        .expect("planner canonicalization test model should produce schema info");

    let planner_shape = plan_access_for_test(&PLANNER_CANONICAL_MODEL, &schema, Some(&predicate))
        .expect("planner access shape should build for strict single-key IN predicate");
    let intent_shape = build_access_plan_from_keys(&KeyAccess::Many(vec![key]));

    assert_eq!(
        planner_shape, intent_shape,
        "planner and intent canonical access shape should agree for one-key sets",
    );
    assert_eq!(
        planner_shape,
        AccessPlan::by_key(Value::Ulid(key)),
        "one-key set canonicalization should collapse to ByKey",
    );
}

#[test]
fn planner_non_pk_in_empty_lowers_to_empty_by_keys() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::In,
        Value::List(Vec::new()),
        CoercionId::Strict,
    ));
    let schema = SchemaInfo::from_entity_model(&PLANNER_IN_EMPTY_MODEL)
        .expect("IN-empty planner test model should produce schema info");

    let planner_shape = plan_access_for_test(&PLANNER_IN_EMPTY_MODEL, &schema, Some(&predicate))
        .expect("planner access shape should build for strict IN-empty predicate");

    assert_eq!(
        planner_shape,
        AccessPlan::by_keys(Vec::new()),
        "IN-empty predicates must lower to an empty access shape instead of full scan",
    );
}

#[test]
fn planner_order_only_single_field_index_falls_back_to_index_range() {
    let schema = SchemaInfo::from_entity_model(&PLANNER_ORDER_MODEL)
        .expect("planner order-only test model should produce schema info");
    let order = canonical_order(&[("name", OrderDirection::Asc), ("id", OrderDirection::Asc)]);

    let planner_shape =
        plan_access_for_test_with_order(&PLANNER_ORDER_MODEL, &schema, None, Some(order))
            .expect("order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            PLANNER_ORDER_INDEXES[0],
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        )),
        "canonical order-only secondary scans should fall back to one whole-index range",
    );
}

#[test]
fn planner_order_only_single_field_desc_index_falls_back_to_index_range() {
    let schema = SchemaInfo::from_entity_model(&PLANNER_ORDER_MODEL)
        .expect("planner descending order-only test model should produce schema info");
    let order = canonical_order(&[("name", OrderDirection::Desc), ("id", OrderDirection::Desc)]);

    let planner_shape =
        plan_access_for_test_with_order(&PLANNER_ORDER_MODEL, &schema, None, Some(order))
            .expect("descending order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            PLANNER_ORDER_INDEXES[0],
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        )),
        "canonical descending order-only secondary scans should fall back to one whole-index range",
    );
}

#[test]
fn planner_order_only_composite_index_falls_back_to_index_range() {
    let schema = SchemaInfo::from_entity_model(&PLANNER_ORDER_COMPOSITE_MODEL)
        .expect("planner composite order-only test model should produce schema info");
    let order = canonical_order(&[
        ("code", OrderDirection::Asc),
        ("serial", OrderDirection::Asc),
        ("id", OrderDirection::Asc),
    ]);

    let planner_shape =
        plan_access_for_test_with_order(&PLANNER_ORDER_COMPOSITE_MODEL, &schema, None, Some(order))
            .expect("composite order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            PLANNER_ORDER_COMPOSITE_INDEXES[0],
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        )),
        "canonical composite order-only scans should use one whole-index range fallback",
    );
}

#[test]
fn planner_order_only_composite_desc_index_falls_back_to_index_range() {
    let schema = SchemaInfo::from_entity_model(&PLANNER_ORDER_COMPOSITE_MODEL)
        .expect("planner descending composite order-only test model should produce schema info");
    let order = canonical_order(&[
        ("code", OrderDirection::Desc),
        ("serial", OrderDirection::Desc),
        ("id", OrderDirection::Desc),
    ]);

    let planner_shape =
        plan_access_for_test_with_order(&PLANNER_ORDER_COMPOSITE_MODEL, &schema, None, Some(order))
            .expect("descending composite order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            PLANNER_ORDER_COMPOSITE_INDEXES[0],
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        )),
        "canonical descending composite order-only scans should use one whole-index range fallback",
    );
}

#[test]
fn planner_order_only_filtered_index_fails_closed_without_guard_predicate() {
    let schema = SchemaInfo::from_entity_model(&PLANNER_ORDER_FILTERED_MODEL)
        .expect("planner filtered order-only test model should produce schema info");
    let order = canonical_order(&[("name", OrderDirection::Asc), ("id", OrderDirection::Asc)]);

    let planner_shape =
        plan_access_for_test_with_order(&PLANNER_ORDER_FILTERED_MODEL, &schema, None, Some(order))
            .expect("filtered order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::full_scan(),
        "filtered indexes must not satisfy order-only access when the query does not imply the guard",
    );
}

#[test]
fn planner_order_only_single_field_index_fails_closed_for_strict_text_prefix_predicate() {
    let schema = SchemaInfo::from_entity_model(&PLANNER_ORDER_MODEL)
        .expect("planner strict text-prefix order-only test model should produce schema info");
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("sam".to_string()),
        CoercionId::Strict,
    ));
    let order = canonical_order(&[("name", OrderDirection::Asc), ("id", OrderDirection::Asc)]);

    let planner_shape = plan_access_for_test_with_order(
        &PLANNER_ORDER_MODEL,
        &schema,
        Some(&predicate),
        Some(order),
    )
    .expect("strict text-prefix order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::full_scan(),
        "strict raw-field text-prefix predicates must keep the fail-closed full-scan route even when ORDER BY matches the secondary index",
    );
}

#[test]
fn planner_order_only_expression_index_falls_back_to_index_range() {
    let schema = SchemaInfo::from_entity_model(&PLANNER_ORDER_EXPRESSION_MODEL)
        .expect("planner expression order-only test model should produce schema info");
    let order = canonical_order(&[
        ("LOWER(name)", OrderDirection::Asc),
        ("id", OrderDirection::Asc),
    ]);

    let planner_shape = plan_access_for_test_with_order(
        &PLANNER_ORDER_EXPRESSION_MODEL,
        &schema,
        None,
        Some(order),
    )
    .expect("expression order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            PLANNER_ORDER_EXPRESSION_INDEXES[0],
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        )),
        "canonical LOWER(field) order-only scans should use the matching expression index range",
    );
}

#[test]
fn planner_order_only_expression_desc_index_falls_back_to_index_range() {
    let schema = SchemaInfo::from_entity_model(&PLANNER_ORDER_EXPRESSION_MODEL)
        .expect("planner descending expression order-only test model should produce schema info");
    let order = canonical_order(&[
        ("LOWER(name)", OrderDirection::Desc),
        ("id", OrderDirection::Desc),
    ]);

    let planner_shape = plan_access_for_test_with_order(
        &PLANNER_ORDER_EXPRESSION_MODEL,
        &schema,
        None,
        Some(order),
    )
    .expect("descending expression order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            PLANNER_ORDER_EXPRESSION_INDEXES[0],
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        )),
        "canonical descending LOWER(field) order-only scans should use the matching expression index range",
    );
}

#[test]
fn planner_order_only_expression_index_fails_closed_for_raw_field_order() {
    let schema = SchemaInfo::from_entity_model(&PLANNER_ORDER_EXPRESSION_MODEL)
        .expect("planner expression order-only test model should produce schema info");
    let order = canonical_order(&[("name", OrderDirection::Asc), ("id", OrderDirection::Asc)]);

    let planner_shape = plan_access_for_test_with_order(
        &PLANNER_ORDER_EXPRESSION_MODEL,
        &schema,
        None,
        Some(order),
    )
    .expect("expression order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::full_scan(),
        "raw field ORDER BY must not silently treat expression-key indexes as field-order-compatible",
    );
}
