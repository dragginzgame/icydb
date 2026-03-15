//! Module: db::contracts::tests::schema_validate
//! Responsibility: module-local ownership and contracts for db::contracts::tests::schema_validate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::schema::{SchemaInfo, ValidateError},
    model::{
        field::{FieldKind, FieldModel},
        index::{IndexExpression, IndexKeyItem, IndexModel},
    },
    testing::InvalidEntityModelBuilder,
};

fn field(name: &'static str, kind: FieldKind) -> FieldModel {
    FieldModel { name, kind }
}

#[test]
fn model_rejects_missing_primary_key() {
    // Invalid test scaffolding: models are hand-built to exercise
    // validation failures that helpers intentionally prevent.
    let fields: &'static [FieldModel] =
        Box::leak(vec![field("id", FieldKind::Ulid)].into_boxed_slice());
    let missing_pk = Box::leak(Box::new(field("missing", FieldKind::Ulid)));

    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        missing_pk,
        fields,
        &[],
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::InvalidPrimaryKey { .. })
    ));
}

#[test]
fn model_rejects_duplicate_fields() {
    let model = InvalidEntityModelBuilder::from_fields(
        vec![field("dup", FieldKind::Text), field("dup", FieldKind::Text)],
        0,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::DuplicateField { .. })
    ));
}

#[test]
fn model_rejects_invalid_primary_key_type() {
    let model = InvalidEntityModelBuilder::from_fields(
        vec![field("pk", FieldKind::List(&FieldKind::Text))],
        0,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::InvalidPrimaryKeyType { .. })
    ));
}

#[test]
fn model_rejects_index_unknown_field() {
    const INDEX_FIELDS: [&str; 1] = ["missing"];
    const INDEX_MODEL: IndexModel = IndexModel::new(
        "test::idx_missing",
        "test::IndexStore",
        &INDEX_FIELDS,
        false,
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [FieldModel] =
        Box::leak(vec![field("id", FieldKind::Ulid)].into_boxed_slice());
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::IndexFieldUnknown { .. })
    ));
}

#[test]
fn model_rejects_index_non_queryable_field() {
    const INDEX_FIELDS: [&str; 1] = ["broken"];
    const INDEX_MODEL: IndexModel =
        IndexModel::new("test::idx_broken", "test::IndexStore", &INDEX_FIELDS, false);
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [FieldModel] = Box::leak(
        vec![
            field("id", FieldKind::Ulid),
            field("broken", FieldKind::Structured { queryable: false }),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::IndexFieldNotQueryable { .. })
    ));
}

#[test]
fn model_rejects_index_map_field_in_0_7_x() {
    const INDEX_FIELDS: [&str; 1] = ["attributes"];
    const INDEX_MODEL: IndexModel = IndexModel::new(
        "test::idx_attributes",
        "test::IndexStore",
        &INDEX_FIELDS,
        false,
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [FieldModel] = Box::leak(
        vec![
            field("id", FieldKind::Ulid),
            field(
                "attributes",
                FieldKind::Map {
                    key: &FieldKind::Text,
                    value: &FieldKind::Uint,
                },
            ),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::IndexFieldMapNotQueryable { .. })
    ));
}

#[test]
fn model_accepts_supported_expression_index_key_items() {
    const INDEX_FIELDS: [&str; 1] = ["email"];
    const INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
        [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
    const INDEX_MODEL: IndexModel = IndexModel::new_with_key_items(
        "test::idx_email_lower",
        "test::IndexStore",
        &INDEX_FIELDS,
        &INDEX_KEY_ITEMS,
        false,
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [FieldModel] = Box::leak(
        vec![
            field("id", FieldKind::Ulid),
            field("email", FieldKind::Text),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    SchemaInfo::from_entity_model(&model)
        .expect("supported expression key metadata should pass schema validation");
}

#[test]
fn model_rejects_expression_index_key_items_with_invalid_field_type() {
    const INDEX_FIELDS: [&str; 1] = ["age"];
    const INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
        [IndexKeyItem::Expression(IndexExpression::Lower("age"))];
    const INDEX_MODEL: IndexModel = IndexModel::new_with_key_items(
        "test::idx_age_lower",
        "test::IndexStore",
        &INDEX_FIELDS,
        &INDEX_KEY_ITEMS,
        false,
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [FieldModel] = Box::leak(
        vec![field("id", FieldKind::Ulid), field("age", FieldKind::Uint)].into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::IndexExpressionFieldTypeInvalid { .. })
    ));
}

#[test]
fn model_rejects_duplicate_index_names() {
    const INDEX_FIELDS_A: [&str; 1] = ["id"];
    const INDEX_FIELDS_B: [&str; 1] = ["other"];
    const INDEX_A: IndexModel = IndexModel::new(
        "test::dup_index",
        "test::IndexStore",
        &INDEX_FIELDS_A,
        false,
    );
    const INDEX_B: IndexModel = IndexModel::new(
        "test::dup_index",
        "test::IndexStore",
        &INDEX_FIELDS_B,
        false,
    );
    const INDEXES: [&IndexModel; 2] = [&INDEX_A, &INDEX_B];

    let fields: &'static [FieldModel] = Box::leak(
        vec![
            field("id", FieldKind::Ulid),
            field("other", FieldKind::Text),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::DuplicateIndexName { .. })
    ));
}

#[test]
fn model_accepts_schema_valid_index_predicate() {
    const INDEX_FIELDS: [&str; 1] = ["active"];
    const INDEX_MODEL: IndexModel = IndexModel::new_with_predicate(
        "test::idx_active_true",
        "test::IndexStore",
        &INDEX_FIELDS,
        false,
        Some("active = true"),
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [FieldModel] = Box::leak(
        vec![
            field("id", FieldKind::Ulid),
            field("active", FieldKind::Bool),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    SchemaInfo::from_entity_model(&model).expect("schema-valid index predicate should pass");
}

#[test]
fn model_rejects_index_predicate_with_invalid_sql_syntax() {
    const INDEX_FIELDS: [&str; 1] = ["active"];
    const INDEX_MODEL: IndexModel = IndexModel::new_with_predicate(
        "test::idx_active_bad_syntax",
        "test::IndexStore",
        &INDEX_FIELDS,
        false,
        Some("active ="),
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [FieldModel] = Box::leak(
        vec![
            field("id", FieldKind::Ulid),
            field("active", FieldKind::Bool),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::InvalidIndexPredicateSyntax { .. })
    ));
}

#[test]
fn model_rejects_index_predicate_with_schema_invalid_field_reference() {
    const INDEX_FIELDS: [&str; 1] = ["active"];
    const INDEX_MODEL: IndexModel = IndexModel::new_with_predicate(
        "test::idx_active_missing_field",
        "test::IndexStore",
        &INDEX_FIELDS,
        false,
        Some("missing = true"),
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [FieldModel] = Box::leak(
        vec![
            field("id", FieldKind::Ulid),
            field("active", FieldKind::Bool),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::InvalidIndexPredicateSchema { .. })
    ));
}
