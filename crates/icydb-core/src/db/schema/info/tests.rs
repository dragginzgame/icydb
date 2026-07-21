use crate::{
    db::predicate::{CompareOp, ComparePredicate, Predicate, normalize_enum_literals},
    db::schema::{
        AcceptedFieldKind, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
        AcceptedValueCatalogHandle, FieldId, PersistedFieldSnapshot, PersistedIndexExpressionOp,
        PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
        PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
        PersistedNestedLeafSnapshot, PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot,
        SchemaFieldSlot, SchemaInfo, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
        enum_catalog::build_initial_accepted_enum_catalog_from_kinds_for_tests,
        literal_matches_type,
    },
    model::{
        entity::EntityModel,
        field::{
            CompositeCodec, CompositeFieldModel, CompositeShapeModel, EnumVariantModel, FieldKind,
            FieldModel, FieldStorageDecode, LeafCodec,
        },
        index::IndexModel,
    },
    testing::entity_model_from_static,
    types::EntityTag,
    value::Value,
};

static FIELDS: [FieldModel; 2] = [
    FieldModel::generated("name", FieldKind::Text { max_len: None }),
    FieldModel::generated("id", FieldKind::Ulid),
];
static PROFILE_NESTED_FIELDS: [FieldModel; 1] = [FieldModel::generated("rank", FieldKind::Nat64)];
static PROFILE_COMPOSITE_FIELDS: [CompositeFieldModel; 1] = [CompositeFieldModel::generated(
    "rank",
    FieldKind::Nat64,
    false,
)];
static PROFILE_COMPOSITE_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Record(&PROFILE_COMPOSITE_FIELDS);
static PROFILE_COMPOSITE_KIND: FieldKind = FieldKind::Composite {
    path: "schema::info::tests::Profile",
    codec: CompositeCodec::StructuralV1,
    shape: &PROFILE_COMPOSITE_SHAPE,
};
static PROFILE_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated_with_storage_decode_nullability_write_policies_and_nested_fields(
        "profile",
        PROFILE_COMPOSITE_KIND,
        FieldStorageDecode::CatalogValue,
        false,
        None,
        None,
        &PROFILE_NESTED_FIELDS,
    ),
];
static INDEXES: [&IndexModel; 0] = [];
static NAME_INDEX_FIELDS: [&str; 1] = ["name"];
static NAME_INDEX: IndexModel = IndexModel::generated(
    "schema_info_name",
    "schema::info::tests::name",
    &NAME_INDEX_FIELDS,
    false,
);
static INDEXED_INDEXES: [&IndexModel; 1] = [&NAME_INDEX];
static MODEL: EntityModel = entity_model_from_static(
    "schema::info::tests::Entity",
    "Entity",
    &FIELDS[1],
    1,
    &FIELDS,
    &INDEXES,
);
static PROFILE_MODEL: EntityModel = entity_model_from_static(
    "schema::info::tests::ProfileEntity",
    "ProfileEntity",
    &PROFILE_FIELDS[0],
    0,
    &PROFILE_FIELDS,
    &INDEXES,
);
static INDEXED_MODEL: EntityModel = entity_model_from_static(
    "schema::info::tests::IndexedEntity",
    "IndexedEntity",
    &FIELDS[1],
    1,
    &FIELDS,
    &INDEXED_INDEXES,
);
static ACCEPTED_STATUS_VARIANTS: [EnumVariantModel; 2] = [
    EnumVariantModel::new("Active", None, FieldStorageDecode::ByKind),
    EnumVariantModel::new("Draft", None, FieldStorageDecode::ByKind),
];
static ACCEPTED_STATUS_KIND: FieldKind = FieldKind::Enum {
    path: "schema::info::tests::Status",
    variants: &ACCEPTED_STATUS_VARIANTS,
};

fn accepted_status_schema_info() -> SchemaInfo {
    let snapshot =
        accepted_schema_with_name_kind(AcceptedFieldKind::from_model_kind(ACCEPTED_STATUS_KIND));
    let catalog = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[ACCEPTED_STATUS_KIND])
        .expect("status catalog should build");

    SchemaInfo::from_accepted_snapshot_and_catalog_for_model(
        &MODEL,
        &snapshot,
        AcceptedValueCatalogHandle::new_for_tests(
            catalog,
            crate::db::schema::AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        ),
        false,
    )
}

// Build one accepted schema whose second field deliberately differs from
// generated metadata so tests can prove `SchemaInfo` follows the persisted
// top-level authority.
fn accepted_schema_with_name_kind(kind: AcceptedFieldKind) -> AcceptedSchemaSnapshot {
    accepted_schema_with_name_kind_and_slots(kind, SchemaFieldSlot::new(1), SchemaFieldSlot::new(1))
}

// Build one accepted schema fixture with independently selected layout and
// field-snapshot slots. Owner-local tests use this to prove `SchemaInfo`
// reads slot facts from accepted row layout, not duplicated field data.
fn accepted_schema_with_name_kind_and_slots(
    kind: AcceptedFieldKind,
    layout_slot: SchemaFieldSlot,
    field_slot: SchemaFieldSlot,
) -> AcceptedSchemaSnapshot {
    AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::info::tests::Entity".to_string(),
        "Entity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), layout_slot),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "name".to_string(),
                field_slot,
                kind,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
        ],
    ))
}

#[test]
fn accepted_enum_predicate_normalization_uses_catalog_instead_of_generated_kind() {
    let schema = accepted_status_schema_info();
    let expected_type_id = schema
        .enum_catalog()
        .and_then(|catalog| catalog.type_id("schema::info::tests::Status"))
        .expect("accepted status type ID");
    assert!(matches!(
        schema
            .accepted_field_contract("name")
            .map(|contract| contract.kind().clone()),
        Some(AcceptedFieldKind::Enum { type_id }) if type_id == expected_type_id
    ));
    let predicate = Predicate::Compare(ComparePredicate::eq(
        "name".to_string(),
        Value::Text("Active".to_string()),
    ));

    let normalized = normalize_enum_literals(&schema, &predicate)
        .expect("accepted enum literal should normalize through the catalog");
    let Predicate::Compare(compare) = normalized else {
        panic!("normalized predicate should remain a comparison");
    };
    let Value::Enum(value) = compare.value else {
        panic!("normalized value should remain an enum");
    };

    let selection = schema
        .enum_catalog()
        .expect("accepted enum catalog")
        .resolve_value(value.canonical())
        .expect("canonical accepted enum value");
    assert_eq!(selection.path(), "schema::info::tests::Status");
    assert_eq!(selection.variant_name(), "Active");
}

#[test]
fn accepted_enum_membership_normalization_is_catalog_checked_and_canonical() {
    let schema = accepted_status_schema_info();
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::In,
        Value::List(vec![
            Value::Text("Draft".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("Draft".to_string()),
        ]),
        crate::db::predicate::CoercionId::Strict,
    ));

    let normalized = normalize_enum_literals(&schema, &predicate)
        .expect("accepted enum membership should normalize through the catalog");
    let Predicate::Compare(compare) = normalized else {
        panic!("normalized predicate should remain a comparison");
    };
    let Value::List(values) = compare.value else {
        panic!("normalized membership should remain a list");
    };

    let catalog = schema.enum_catalog().expect("accepted enum catalog");
    let names = values
        .iter()
        .map(|value| {
            let Value::Enum(value) = value else {
                panic!("normalized membership values must be canonical enums");
            };
            catalog
                .resolve_value(value.canonical())
                .expect("accepted enum value")
                .variant_name()
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["Active".to_string(), "Draft".to_string()]);
}

#[test]
fn accepted_enum_predicate_normalization_rejects_unknown_variant() {
    let schema = accepted_status_schema_info();
    let predicate = Predicate::Compare(ComparePredicate::eq(
        "name".to_string(),
        Value::Text("Missing".to_string()),
    ));

    let error = normalize_enum_literals(&schema, &predicate)
        .expect_err("unknown accepted enum variant must reject before planning");

    assert!(matches!(
        error,
        crate::db::schema::ValidateError::InvalidLiteral {
            field,
            reason: crate::db::schema::SchemaLiteralValidationReason::UnknownEnumVariant,
        } if field == "name"
    ));
}

#[cfg(feature = "sql")]
#[test]
fn accepted_enum_sql_literal_canonicalization_uses_catalog() {
    let schema = accepted_status_schema_info();

    let canonical = schema
        .canonicalize_strict_sql_literal("name", &Value::Text("Active".to_string()))
        .expect("accepted SQL enum literal");
    let Value::Enum(value) = canonical else {
        panic!("accepted SQL enum literal must be canonical");
    };
    let selection = schema
        .enum_catalog()
        .expect("accepted enum catalog")
        .resolve_value(value.canonical())
        .expect("accepted enum value");
    assert_eq!(selection.variant_name(), "Active");
    assert_eq!(
        schema.canonicalize_strict_sql_literal("name", &Value::Text("Missing".to_string())),
        None,
    );
}

fn accepted_schema_with_name_index() -> AcceptedSchemaSnapshot {
    AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "schema::info::tests::Entity".to_string(),
        "Entity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "name".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            1,
            "schema_info_name".to_string(),
            "schema::info::tests::name".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["name".to_string()],
                AcceptedFieldKind::Text { max_len: None },
                false,
            )]),
            None,
        )],
    ))
}

fn accepted_schema_with_composite_primary_key() -> AcceptedSchemaSnapshot {
    AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::info::tests::Entity".to_string(),
        "Entity".to_string(),
        vec![FieldId::new(1), FieldId::new(3)],
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
            (FieldId::new(3), SchemaFieldSlot::new(2)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "name".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(3),
                "age".to_string(),
                SchemaFieldSlot::new(2),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
        ],
    ))
}

fn accepted_schema_with_lower_name_index() -> AcceptedSchemaSnapshot {
    let source = PersistedIndexFieldPathSnapshot::new(
        FieldId::new(2),
        SchemaFieldSlot::new(1),
        vec!["name".to_string()],
        AcceptedFieldKind::Text { max_len: None },
        false,
    );

    AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "schema::info::tests::Entity".to_string(),
        "Entity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "name".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            2,
            "schema_info_lower_name".to_string(),
            "schema::info::tests::lower_name".to_string(),
            true,
            PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(
                Box::new(PersistedIndexExpressionSnapshot::new(
                    PersistedIndexExpressionOp::Lower,
                    source,
                    AcceptedFieldKind::Text { max_len: None },
                    AcceptedFieldKind::Text { max_len: None },
                    "expr:v1:LOWER(name)".to_string(),
                )),
            )]),
            Some("name IS NOT NULL".to_string()),
        )],
    ))
}

#[test]
fn cached_for_generated_entity_model_reuses_one_schema_instance() {
    let first = SchemaInfo::cached_for_generated_entity_model(&MODEL);
    let second = SchemaInfo::cached_for_generated_entity_model(&MODEL);

    assert!(std::ptr::eq(first, second));
    assert!(first.field("id").is_some());
    assert!(first.field("name").is_some());
}

#[test]
fn accepted_snapshot_schema_info_uses_persisted_top_level_field_type() {
    let snapshot = accepted_schema_with_name_kind(AcceptedFieldKind::Blob { max_len: None });

    let schema = SchemaInfo::from_snapshot_with_generated_model_for_test(&MODEL, &snapshot);
    let name_type = schema.field("name").expect("accepted field should exist");

    assert!(literal_matches_type(&Value::Blob(vec![1, 2, 3]), name_type));
    assert!(!literal_matches_type(
        &Value::Text("name".into()),
        name_type
    ));
}

#[cfg(feature = "sql")]
#[test]
fn accepted_snapshot_schema_info_canonicalizes_sql_literals_from_persisted_kind() {
    let generated = SchemaInfo::cached_for_generated_entity_model(&MODEL);
    let snapshot = accepted_schema_with_name_kind(AcceptedFieldKind::Nat64);
    let accepted = SchemaInfo::from_snapshot_with_generated_model_for_test(&MODEL, &snapshot);

    assert_eq!(
        generated.canonicalize_strict_sql_literal("name", &Value::Int64(7)),
        None
    );
    assert_eq!(
        accepted.canonicalize_strict_sql_literal("name", &Value::Int64(7)),
        Some(Value::Nat64(7))
    );
}

#[cfg(feature = "sql")]
#[test]
fn accepted_snapshot_schema_info_uses_persisted_sql_capabilities() {
    let generated = SchemaInfo::cached_for_generated_entity_model(&MODEL);
    let snapshot = accepted_schema_with_name_kind(AcceptedFieldKind::Blob { max_len: None });
    let accepted = SchemaInfo::from_snapshot_with_generated_model_for_test(&MODEL, &snapshot);

    let generated_name = generated
        .sql_capabilities("name")
        .expect("generated field capability should exist");
    let accepted_name = accepted
        .sql_capabilities("name")
        .expect("accepted field capability should exist");

    assert!(generated_name.orderable());
    assert!(accepted_name.selectable());
    assert!(accepted_name.comparable());
    assert!(!accepted_name.orderable());
}

#[test]
fn accepted_snapshot_schema_info_uses_row_layout_slot_authority() {
    let generated = SchemaInfo::cached_for_generated_entity_model(&MODEL);
    let snapshot = accepted_schema_with_name_kind_and_slots(
        AcceptedFieldKind::Text { max_len: None },
        SchemaFieldSlot::new(9),
        SchemaFieldSlot::new(1),
    );
    let accepted = SchemaInfo::from_snapshot_with_generated_model_for_test(&MODEL, &snapshot);

    assert_eq!(generated.field_slot_index("name"), Some(0));
    assert_eq!(accepted.field_slot_index("name"), Some(9));
    assert_eq!(generated.entity_name(), Some("Entity"));
    assert_eq!(accepted.entity_name(), Some("Entity"));
    assert_eq!(generated.scalar_primary_key_name(), Some("id"));
    assert_eq!(accepted.scalar_primary_key_name(), Some("id"));
    assert_eq!(generated.primary_key_names(), ["id"]);
    assert_eq!(accepted.primary_key_names(), ["id"]);
}

#[test]
fn accepted_snapshot_schema_info_exposes_ordered_primary_key_names() {
    let snapshot = accepted_schema_with_composite_primary_key();
    let accepted = SchemaInfo::from_snapshot_with_generated_model_for_test(&MODEL, &snapshot);

    assert_eq!(accepted.scalar_primary_key_name(), None);
    assert_eq!(accepted.primary_key_names(), ["id", "age"]);
}

#[test]
fn accepted_snapshot_schema_info_uses_persisted_index_membership() {
    let generated = SchemaInfo::cached_for_generated_entity_model(&INDEXED_MODEL);
    let unindexed_snapshot =
        accepted_schema_with_name_kind(AcceptedFieldKind::Text { max_len: None });
    let indexed_snapshot = accepted_schema_with_name_index();
    let accepted_unindexed = SchemaInfo::from_snapshot_with_generated_model_for_test(
        &INDEXED_MODEL,
        &unindexed_snapshot,
    );
    let accepted_indexed =
        SchemaInfo::from_snapshot_with_generated_model_for_test(&MODEL, &indexed_snapshot);

    assert!(generated.field_is_indexed("name"));
    assert!(!generated.field_is_indexed("id"));
    assert!(
        !accepted_unindexed.field_is_indexed("name"),
        "accepted SchemaInfo must not inherit generated index membership when the accepted snapshot has no index contract",
    );
    assert!(accepted_indexed.field_is_indexed("name"));
    assert!(!accepted_indexed.field_is_indexed("id"));
    assert!(accepted_unindexed.field_path_indexes().is_empty());
}

#[test]
fn accepted_index_field_contract_rejects_foreign_component_metadata() {
    let snapshot = accepted_schema_with_name_index();
    let catalog = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[])
        .expect("empty accepted enum catalog should build");
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        catalog,
        crate::db::schema::AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let accepted = SchemaInfo::from_accepted_snapshot_and_catalog_for_model(
        &MODEL,
        &snapshot,
        catalog.clone(),
        false,
    );
    let foreign =
        SchemaInfo::from_accepted_snapshot_and_catalog_for_model(&MODEL, &snapshot, catalog, false);
    let index = &accepted.field_path_indexes()[0];
    let own_field = &index.fields()[0];
    let foreign_field = &foreign.field_path_indexes()[0].fields()[0];

    assert!(index.accepted_field_contract(own_field).is_some());
    assert!(
        index.accepted_field_contract(foreign_field).is_none(),
        "index authority must not bind field metadata from another projection",
    );
}

#[cfg(feature = "sql")]
#[test]
fn accepted_snapshot_schema_info_exposes_persisted_field_path_indexes() {
    let snapshot = accepted_schema_with_name_index();
    let accepted = SchemaInfo::from_snapshot_with_generated_model_for_test(&MODEL, &snapshot);
    let indexes = accepted.field_path_indexes();

    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].ordinal(), 1);
    assert_eq!(indexes[0].name(), "schema_info_name");
    assert_eq!(indexes[0].store(), "schema::info::tests::name");
    assert!(!indexes[0].unique());
    assert_eq!(indexes[0].predicate_sql(), None);

    let fields = indexes[0].fields();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].field_id(), Some(FieldId::new(2)));
    assert_eq!(fields[0].field_name(), "name");
    assert_eq!(fields[0].slot(), 1);
    assert_eq!(fields[0].path(), &["name".to_string()]);
    assert_eq!(
        fields[0].persisted_kind(),
        Some(&AcceptedFieldKind::Text { max_len: None })
    );
    assert!(fields[0].ty().is_text());
    assert!(!fields[0].nullable());
}

#[test]
fn accepted_snapshot_schema_info_exposes_persisted_expression_indexes() {
    let snapshot = accepted_schema_with_lower_name_index();
    let accepted = SchemaInfo::from_snapshot_with_generated_model_and_expression_indexes_for_test(
        &MODEL, &snapshot,
    );

    assert!(
        accepted.field_path_indexes().is_empty(),
        "field-path visibility should stay field-path-only until expression planner routing moves over",
    );
    assert!(
        accepted.field_is_indexed("name"),
        "accepted expression indexes should still count as index membership for their source field",
    );

    let indexes = accepted.expression_indexes();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].ordinal(), 2);
    assert_eq!(indexes[0].name(), "schema_info_lower_name");
    assert_eq!(indexes[0].store(), "schema::info::tests::lower_name");
    assert!(indexes[0].unique());
    assert_eq!(indexes[0].predicate_sql(), Some("name IS NOT NULL"));

    let key_items = indexes[0].key_items();
    assert_eq!(key_items.len(), 1);
    let Some(expression) = key_items[0].expression() else {
        panic!("accepted expression index should expose an expression key item");
    };
    assert_eq!(expression.op(), PersistedIndexExpressionOp::Lower);
    assert_eq!(expression.canonical_text(), "expr:v1:LOWER(name)");
    assert_eq!(
        expression.input_kind(),
        &AcceptedFieldKind::Text { max_len: None }
    );
    assert_eq!(
        expression.output_kind(),
        &AcceptedFieldKind::Text { max_len: None }
    );

    let source = expression.source();
    assert_eq!(source.field_id(), Some(FieldId::new(2)));
    assert_eq!(source.field_name(), "name");
    assert_eq!(source.slot(), 1);
    assert_eq!(source.path(), &["name".to_string()]);

    std::assert_matches!(
        &key_items[0],
        super::SchemaExpressionIndexKeyItemInfo::Expression(_)
    );
}

#[test]
fn accepted_snapshot_schema_info_uses_persisted_relation_authority() {
    let generated = SchemaInfo::cached_for_generated_entity_model(&MODEL);
    let accepted_relation = accepted_schema_with_name_kind(AcceptedFieldKind::Relation {
        target_path: "schema::info::tests::Target".to_string(),
        target_entity_name: "Target".to_string(),
        target_entity_tag: EntityTag::new(7),
        target_store_path: "schema::info::tests::target_store".to_string(),
        key_kind: Box::new(AcceptedFieldKind::Ulid),
    })
    .persisted_snapshot()
    .clone()
    .with_relations(vec![PersistedRelationEdgeSnapshot::new(
        "name".to_string(),
        "schema::info::tests::Target".to_string(),
        vec![FieldId::new(2)],
    )]);
    let accepted_relation = AcceptedSchemaSnapshot::new(accepted_relation);
    let accepted =
        SchemaInfo::from_snapshot_with_generated_model_for_test(&MODEL, &accepted_relation);

    assert!(!generated.has_any_relations());
    assert!(accepted.has_any_relations());
}

#[test]
fn accepted_snapshot_schema_info_uses_persisted_nested_leaf_type() {
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::info::tests::ProfileEntity".to_string(),
        "ProfileEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "profile".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::test_composite(),
                vec![PersistedNestedLeafSnapshot::new(
                    vec!["rank".to_string()],
                    AcceptedFieldKind::Blob { max_len: None },
                    false,
                )],
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::CatalogValue,
                LeafCodec::Structural,
            ),
        ],
    ));
    let schema = SchemaInfo::from_snapshot_with_generated_model_for_test(&PROFILE_MODEL, &accepted);
    let path = vec!["rank".to_string()];
    let nested_type = schema
        .nested_field_type("profile", path.as_slice())
        .expect("accepted nested leaf should resolve");

    assert!(literal_matches_type(&Value::Blob(vec![1]), &nested_type));
    assert!(!literal_matches_type(&Value::Nat64(1), &nested_type));
}
