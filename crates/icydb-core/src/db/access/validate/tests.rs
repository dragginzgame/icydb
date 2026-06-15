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

static BRANCH_SET_FIELDS: [FieldModel; 4] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("collection_id", FieldKind::Text { max_len: None }),
    FieldModel::generated("stage", FieldKind::Text { max_len: None }),
    FieldModel::generated("title", FieldKind::Text { max_len: None }),
];
static BRANCH_SET_INDEX_FIELDS: [&str; 3] = ["collection_id", "stage", "id"];
static BRANCH_SET_INDEXES: [IndexModel; 1] = [IndexModel::generated(
    "collection_stage_id_idx",
    "access::validate::tests::BranchSetEntity",
    &BRANCH_SET_INDEX_FIELDS,
    false,
)];
static BRANCH_SET_INDEX_REFS: [&IndexModel; 1] = [&BRANCH_SET_INDEXES[0]];
static BRANCH_SET_MODEL: EntityModel = entity_model_from_static(
    "access::validate::tests::BranchSetEntity",
    "BranchSetEntity",
    &BRANCH_SET_FIELDS[0],
    0,
    &BRANCH_SET_FIELDS,
    &BRANCH_SET_INDEX_REFS,
);

fn branch_set_access_plan(branch_values: Vec<Value>) -> AccessPlan<Value> {
    AccessPlan::index_branch_set_from_contract(
        SemanticIndexAccessContract::model_only_from_generated_index(BRANCH_SET_INDEXES[0]),
        vec![Value::Text("01KV5N439P0000000000000000".to_string())],
        branch_values,
    )
}

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
fn validate_index_branch_set_accepts_canonical_primary_key_suffix_route() {
    let schema = SchemaInfo::cached_for_generated_entity_model(&BRANCH_SET_MODEL);
    let access = branch_set_access_plan(vec![
        Value::Text("Draft".to_string()),
        Value::Text("Review".to_string()),
    ]);

    validate_access_structure_model(schema, &BRANCH_SET_MODEL, &access)
        .expect("canonical branch-set route should validate");
    validate_access_runtime_invariants_with_schema(schema, &access)
        .expect("runtime branch-set invariants should validate");
}

#[test]
fn validate_index_branch_set_rejects_over_cap_branch_values() {
    let schema = SchemaInfo::cached_for_generated_entity_model(&BRANCH_SET_MODEL);
    let access = branch_set_access_plan(
        (0..=MAX_INDEX_BRANCH_SET_VALUES)
            .map(|index| Value::Text(format!("Stage{index:02}")))
            .collect(),
    );

    let err = validate_access_structure_model(schema, &BRANCH_SET_MODEL, &access)
        .expect_err("branch-set route above the cap should reject");

    std::assert_matches!(err, AccessPlanError::IndexBranchSetTooLarge { .. });
}

#[test]
fn validate_index_branch_set_rejects_uncanonical_branch_values() {
    let schema = SchemaInfo::cached_for_generated_entity_model(&BRANCH_SET_MODEL);
    let access = branch_set_access_plan(vec![
        Value::Text("Review".to_string()),
        Value::Text("Draft".to_string()),
    ]);

    let err = validate_access_structure_model(schema, &BRANCH_SET_MODEL, &access)
        .expect_err("branch-set route should require canonical branch values");

    std::assert_matches!(err, AccessPlanError::IndexBranchSetNotCanonical);
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
