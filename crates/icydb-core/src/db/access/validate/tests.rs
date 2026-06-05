use super::*;
use crate::{
    model::{EntityModel, FieldKind, FieldModel, IndexModel, PrimaryKeyModel},
    testing::entity_model_from_static,
};

static SCALAR_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Nat64),
    FieldModel::generated("name", FieldKind::Text { max_len: None }),
];
static EMPTY_INDEXES: [&IndexModel; 0] = [];
static SCALAR_MODEL: EntityModel = entity_model_from_static(
    "access::validate::tests::ScalarEntity",
    "ScalarEntity",
    &SCALAR_FIELDS[0],
    0,
    &SCALAR_FIELDS,
    &EMPTY_INDEXES,
);

static COMPOSITE_FIELDS: [FieldModel; 3] = [
    FieldModel::generated("tenant_id", FieldKind::Nat64),
    FieldModel::generated("local_id", FieldKind::Nat64),
    FieldModel::generated("label", FieldKind::Text { max_len: None }),
];
static COMPOSITE_PK_FIELDS: [&FieldModel; 2] = [&COMPOSITE_FIELDS[0], &COMPOSITE_FIELDS[1]];
static COMPOSITE_MODEL: EntityModel = EntityModel::generated_with_primary_key_model(
    "access::validate::tests::CompositeEntity",
    "CompositeEntity",
    1,
    PrimaryKeyModel::ordered(&COMPOSITE_PK_FIELDS),
    0,
    &COMPOSITE_FIELDS,
    &EMPTY_INDEXES,
);

#[test]
fn validate_pk_literal_keeps_scalar_key_validation() {
    let schema = SchemaInfo::cached_for_generated_entity_model(&SCALAR_MODEL);

    validate_access_structure_model(schema, &SCALAR_MODEL, &AccessPlan::by_key(Value::Nat64(7)))
        .expect("scalar primary-key literal should validate");

    let err = validate_access_structure_model(
        schema,
        &SCALAR_MODEL,
        &AccessPlan::by_key(Value::List(vec![Value::Nat64(7)])),
    )
    .expect_err("scalar primary-key validation should reject list literals");

    std::assert_matches!(err, AccessPlanError::PrimaryKeyMismatch { .. });
}

#[test]
fn validate_pk_literal_accepts_ordered_composite_key_value_list() {
    let schema = SchemaInfo::cached_for_generated_entity_model(&COMPOSITE_MODEL);
    let key = Value::List(vec![Value::Nat64(7), Value::Nat64(11)]);

    validate_access_structure_model(schema, &COMPOSITE_MODEL, &AccessPlan::by_key(key))
        .expect("ordered composite primary-key literal should validate");
}

#[test]
fn validate_pk_literal_accepts_composite_by_keys() {
    let schema = SchemaInfo::cached_for_generated_entity_model(&COMPOSITE_MODEL);
    let keys = vec![
        Value::List(vec![Value::Nat64(7), Value::Nat64(11)]),
        Value::List(vec![Value::Nat64(7), Value::Nat64(12)]),
    ];

    validate_access_structure_model(schema, &COMPOSITE_MODEL, &AccessPlan::by_keys(keys))
        .expect("ordered composite primary-key list should validate");
}

#[test]
fn runtime_invariants_validate_composite_by_key_shape() {
    let schema = SchemaInfo::cached_for_generated_entity_model(&COMPOSITE_MODEL);

    validate_access_runtime_invariants_with_schema(
        schema,
        &AccessPlan::by_key(Value::List(vec![Value::Nat64(7), Value::Nat64(11)])),
    )
    .expect("runtime access validation should accept ordered composite keys");

    let err = validate_access_runtime_invariants_with_schema(
        schema,
        &AccessPlan::by_key(Value::Nat64(7)),
    )
    .expect_err("runtime access validation should reject scalar value for composite key");

    std::assert_matches!(err, AccessPlanError::PrimaryKeyMismatch { .. });
}

#[test]
fn runtime_invariants_validate_composite_by_keys_shape() {
    let schema = SchemaInfo::cached_for_generated_entity_model(&COMPOSITE_MODEL);
    let valid_keys = vec![
        Value::List(vec![Value::Nat64(7), Value::Nat64(11)]),
        Value::List(vec![Value::Nat64(7), Value::Nat64(12)]),
    ];
    validate_access_runtime_invariants_with_schema(schema, &AccessPlan::by_keys(valid_keys))
        .expect("runtime access validation should accept ordered composite key lists");

    let invalid_keys = vec![
        Value::List(vec![Value::Nat64(7), Value::Nat64(11)]),
        Value::List(vec![Value::Nat64(7)]),
    ];
    let err =
        validate_access_runtime_invariants_with_schema(schema, &AccessPlan::by_keys(invalid_keys))
            .expect_err("runtime access validation should reject malformed composite key lists");

    std::assert_matches!(err, AccessPlanError::PrimaryKeyMismatch { .. });
}

#[test]
fn validate_pk_literal_rejects_composite_key_with_wrong_arity() {
    let schema = SchemaInfo::cached_for_generated_entity_model(&COMPOSITE_MODEL);
    let key = Value::List(vec![Value::Nat64(7)]);

    let err = validate_access_structure_model(schema, &COMPOSITE_MODEL, &AccessPlan::by_key(key))
        .expect_err("composite primary-key validation should reject wrong arity");

    std::assert_matches!(err, AccessPlanError::PrimaryKeyMismatch { .. });
}

#[test]
fn validate_pk_literal_rejects_composite_key_component_type_mismatch() {
    let schema = SchemaInfo::cached_for_generated_entity_model(&COMPOSITE_MODEL);
    let key = Value::List(vec![Value::Nat64(7), Value::Text("wrong".to_string())]);

    let err = validate_access_structure_model(schema, &COMPOSITE_MODEL, &AccessPlan::by_key(key))
        .expect_err("composite primary-key validation should reject component mismatch");

    std::assert_matches!(err, AccessPlanError::PrimaryKeyMismatch { .. });
}

#[test]
fn validate_pk_range_rejects_composite_primary_key_ranges() {
    let schema = SchemaInfo::cached_for_generated_entity_model(&COMPOSITE_MODEL);
    let range = AccessPlan::key_range(
        Value::List(vec![Value::Nat64(7), Value::Nat64(11)]),
        Value::List(vec![Value::Nat64(7), Value::Nat64(12)]),
    );

    let err = validate_access_structure_model(schema, &COMPOSITE_MODEL, &range)
        .expect_err("composite primary-key range access is deferred and should reject");

    std::assert_matches!(
        err,
        AccessPlanError::CompositePrimaryKeyRangeUnsupported { .. }
    );
}

#[test]
fn runtime_invariants_reject_composite_primary_key_ranges() {
    let schema = SchemaInfo::cached_for_generated_entity_model(&COMPOSITE_MODEL);
    let range = AccessPlan::key_range(
        Value::List(vec![Value::Nat64(7), Value::Nat64(11)]),
        Value::List(vec![Value::Nat64(7), Value::Nat64(12)]),
    );

    let err = validate_access_runtime_invariants_with_schema(schema, &range)
        .expect_err("runtime access validation should also reject composite key ranges");

    std::assert_matches!(
        err,
        AccessPlanError::CompositePrimaryKeyRangeUnsupported { .. }
    );
}
