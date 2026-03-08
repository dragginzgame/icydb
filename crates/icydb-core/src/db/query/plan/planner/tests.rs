use super::*;
use crate::{
    db::{
        predicate::{CoercionId, CompareOp, ComparePredicate},
        query::intent::{KeyAccess, build_access_plan_from_keys},
    },
    model::{
        field::{FieldKind, FieldModel},
        index::IndexModel,
    },
    testing::entity_model_from_static,
    types::Ulid,
};

static PLANNER_CANONICAL_FIELDS: [FieldModel; 1] = [FieldModel {
    name: "id",
    kind: FieldKind::Ulid,
}];
static PLANNER_CANONICAL_INDEXES: [&IndexModel; 0] = [];
static PLANNER_CANONICAL_MODEL: EntityModel = entity_model_from_static(
    "planner::canonical_test_entity",
    "PlannerCanonicalTestEntity",
    &PLANNER_CANONICAL_FIELDS[0],
    &PLANNER_CANONICAL_FIELDS,
    &PLANNER_CANONICAL_INDEXES,
);

static PLANNER_IN_EMPTY_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "email",
        kind: FieldKind::Text,
    },
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

#[test]
fn normalize_union_dedups_identical_paths() {
    let key = Value::Ulid(Ulid::from_u128(1));
    let plan = AccessPlan::Union(vec![
        AccessPlan::by_key(key.clone()),
        AccessPlan::by_key(key),
    ]);

    let normalized = normalize_planned_access_plan_for_stability(plan);

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

    let normalized = normalize_planned_access_plan_for_stability(plan);
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

    let normalized = normalize_planned_access_plan_for_stability(plan);

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

    let planner_shape = plan_access(&PLANNER_CANONICAL_MODEL, &schema, Some(&predicate))
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

    let planner_shape = plan_access(&PLANNER_IN_EMPTY_MODEL, &schema, Some(&predicate))
        .expect("planner access shape should build for strict IN-empty predicate");

    assert_eq!(
        planner_shape,
        AccessPlan::by_keys(Vec::new()),
        "IN-empty predicates must lower to an empty access shape instead of full scan",
    );
}
