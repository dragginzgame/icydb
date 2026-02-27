use crate::{
    db::contracts::{SchemaInfo, ValidateError},
    model::{
        field::{FieldKind, FieldModel},
        index::IndexModel,
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
