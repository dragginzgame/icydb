use crate::{
    db::{
        EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
        EntityRelationDescription, EntitySchemaDescription,
        relation::{RelationFieldCardinality, relation_field_metadata_for_model_iter},
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
            AcceptedValueCatalogHandle, FieldId, PersistedFieldSnapshot, PersistedSchemaSnapshot,
            RowLayoutVersion, SchemaFieldSlot, SchemaFieldWritePolicy, SchemaHistoricalFill,
            SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
            build_initial_accepted_catalogs_for_tests, compiled_schema_proposal_for_model,
            describe::{
                bounded_schema_value_rendering, describe_entity_fields_with_persisted_schema,
                describe_entity_model, describe_entity_model_with_persisted_schema,
                short_default_payload_fingerprint,
            },
        },
    },
    model::{
        entity::{EntityModel, PrimaryKeyModel},
        field::{
            CompositeCodec, CompositeElementModel, CompositeFieldModel, CompositeShapeModel,
            FieldDatabaseDefault, FieldKind, FieldModel, FieldStorageDecode, LeafCodec,
            ScalarCodec,
        },
    },
    types::EntityTag,
    value::OutputValue,
};
use candid::types::{CandidType, Label, Type, TypeInner};

static DESCRIBE_SINGLE_RELATION_KIND: FieldKind = FieldKind::Relation {
    target_path: "entities::Target",
    target_entity_name: "Target",
    target_entity_tag: EntityTag::new(0xD001),
    target_store_path: "stores::Target",
    key_kind: &FieldKind::Ulid,
};
static DESCRIBE_LIST_RELATION_INNER_KIND: FieldKind = FieldKind::Relation {
    target_path: "entities::Account",
    target_entity_name: "Account",
    target_entity_tag: EntityTag::new(0xD002),
    target_store_path: "stores::Account",
    key_kind: &FieldKind::Nat64,
};
static DESCRIBE_SET_RELATION_INNER_KIND: FieldKind = FieldKind::Relation {
    target_path: "entities::Team",
    target_entity_name: "Team",
    target_entity_tag: EntityTag::new(0xD003),
    target_store_path: "stores::Team",
    key_kind: &FieldKind::Text { max_len: None },
};
static DESCRIBE_RELATION_FIELDS: [FieldModel; 4] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("target", DESCRIBE_SINGLE_RELATION_KIND),
    FieldModel::generated(
        "accounts",
        FieldKind::List(&DESCRIBE_LIST_RELATION_INNER_KIND),
    ),
    FieldModel::generated("teams", FieldKind::Set(&DESCRIBE_SET_RELATION_INNER_KIND)),
];
static DESCRIBE_RELATION_INDEXES: [&crate::model::index::IndexModel; 0] = [];
static DESCRIBE_RELATION_MODEL: EntityModel = EntityModel::generated(
    "entities::Source",
    "Source",
    1,
    &DESCRIBE_RELATION_FIELDS[0],
    0,
    &DESCRIBE_RELATION_FIELDS,
    &DESCRIBE_RELATION_INDEXES,
);
static DESCRIBE_COMPOSITE_PK_FIELDS: [FieldModel; 3] = [
    FieldModel::generated("tenant_id", FieldKind::Nat64),
    FieldModel::generated("local_id", FieldKind::Nat64),
    FieldModel::generated("label", FieldKind::Text { max_len: None }),
];
static DESCRIBE_COMPOSITE_PK_FIELD_REFS: [&FieldModel; 2] = [
    &DESCRIBE_COMPOSITE_PK_FIELDS[0],
    &DESCRIBE_COMPOSITE_PK_FIELDS[1],
];
static DESCRIBE_COMPOSITE_PK_MODEL: EntityModel = EntityModel::generated_with_primary_key_model(
    "entities::Composite",
    "Composite",
    1,
    PrimaryKeyModel::ordered(&DESCRIBE_COMPOSITE_PK_FIELD_REFS),
    0,
    &DESCRIBE_COMPOSITE_PK_FIELDS,
    &DESCRIBE_RELATION_INDEXES,
);
static DESCRIBE_EXACT_RECORD_FIELDS: [CompositeFieldModel; 2] = [
    CompositeFieldModel::generated("name", FieldKind::Text { max_len: Some(32) }, false),
    CompositeFieldModel::generated("rank", FieldKind::Nat64, true),
];
static DESCRIBE_EXACT_RECORD_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Record(&DESCRIBE_EXACT_RECORD_FIELDS);
static DESCRIBE_EXACT_TUPLE_ELEMENTS: [CompositeElementModel; 2] = [
    CompositeElementModel::generated(FieldKind::Int64, false),
    CompositeElementModel::generated(FieldKind::Bool, true),
];
static DESCRIBE_EXACT_TUPLE_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Tuple(&DESCRIBE_EXACT_TUPLE_ELEMENTS);
static DESCRIBE_EXACT_NEWTYPE_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Newtype(CompositeElementModel::generated(FieldKind::Nat128, false));
static DESCRIBE_EXACT_FIELDS: [FieldModel; 4] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated(
        "profile",
        FieldKind::Composite {
            path: "schema::describe::tests::Profile",
            codec: CompositeCodec::StructuralV1,
            shape: &DESCRIBE_EXACT_RECORD_SHAPE,
        },
    ),
    FieldModel::generated(
        "coordinates",
        FieldKind::Composite {
            path: "schema::describe::tests::Coordinates",
            codec: CompositeCodec::StructuralV1,
            shape: &DESCRIBE_EXACT_TUPLE_SHAPE,
        },
    ),
    FieldModel::generated(
        "customer_id",
        FieldKind::Composite {
            path: "schema::describe::tests::CustomerId",
            codec: CompositeCodec::StructuralV1,
            shape: &DESCRIBE_EXACT_NEWTYPE_SHAPE,
        },
    ),
];
static DESCRIBE_EXACT_MODEL: EntityModel = EntityModel::generated(
    "entities::DescribeExactComposite",
    "DescribeExactComposite",
    1,
    &DESCRIBE_EXACT_FIELDS[0],
    0,
    &DESCRIBE_EXACT_FIELDS,
    &DESCRIBE_RELATION_INDEXES,
);

fn accepted_value_catalog_for_models(models: &[&EntityModel]) -> AcceptedValueCatalogHandle {
    let (enum_catalog, composite_catalog) = build_initial_accepted_catalogs_for_tests(models)
        .expect("accepted value catalogs should build");
    AcceptedValueCatalogHandle::new_for_tests(
        enum_catalog,
        composite_catalog,
        AcceptedSchemaRevision::INITIAL,
    )
}

#[test]
fn accepted_schema_describe_resolves_exact_composite_catalog_contracts() {
    let value_catalog = accepted_value_catalog_for_models(&[&DESCRIBE_EXACT_MODEL]);
    let snapshot = compiled_schema_proposal_for_model(&DESCRIBE_EXACT_MODEL)
        .initial_persisted_schema_snapshot_with_catalogs(
            value_catalog.enum_catalog(),
            value_catalog.composite_catalog(),
        )
        .expect("exact composite proposal should resolve through accepted catalogs");
    let snapshot = AcceptedSchemaSnapshot::try_new(snapshot)
        .expect("exact composite proposal should produce a valid accepted snapshot");

    let described = describe_entity_fields_with_persisted_schema(&snapshot, &value_catalog)
        .expect("accepted exact composites should describe");
    let top_level = described
        .iter()
        .filter_map(|field| field.slot().map(|_| (field.name(), field.kind())))
        .collect::<Vec<_>>();

    assert_eq!(
        top_level,
        vec![
            ("id", "ulid"),
            (
                "profile",
                "composite(path=schema::describe::tests::Profile, codec=structural_v1, shape=record{name:text(max_len=32), rank:nat64?})",
            ),
            (
                "coordinates",
                "composite(path=schema::describe::tests::Coordinates, codec=structural_v1, shape=tuple<int64, bool?>)",
            ),
            (
                "customer_id",
                "composite(path=schema::describe::tests::CustomerId, codec=structural_v1, shape=newtype<nat128>)",
            ),
        ],
    );
}

fn expect_record_fields(ty: Type) -> Vec<String> {
    match ty.as_ref() {
        TypeInner::Record(fields) => fields
            .iter()
            .map(|field| match field.id.as_ref() {
                Label::Named(name) => name.clone(),
                other => panic!("expected named record field, got {other:?}"),
            })
            .collect(),
        other => panic!("expected candid record, got {other:?}"),
    }
}

fn expect_record_field_type(ty: Type, field_name: &str) -> Type {
    match ty.as_ref() {
        TypeInner::Record(fields) => fields
            .iter()
            .find_map(|field| match field.id.as_ref() {
                Label::Named(name) if name == field_name => Some(field.ty.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("expected record field `{field_name}`")),
        other => panic!("expected candid record, got {other:?}"),
    }
}

fn expect_variant_labels(ty: Type) -> Vec<String> {
    match ty.as_ref() {
        TypeInner::Variant(fields) => fields
            .iter()
            .map(|field| match field.id.as_ref() {
                Label::Named(name) => name.clone(),
                other => panic!("expected named variant label, got {other:?}"),
            })
            .collect(),
        other => panic!("expected candid variant, got {other:?}"),
    }
}

#[test]
fn entity_schema_description_candid_shape_is_stable() {
    let fields = expect_record_fields(EntitySchemaDescription::ty());

    for field in [
        "entity_path",
        "entity_name",
        "primary_key",
        "primary_key_fields",
        "fields",
        "indexes",
        "relations",
        "row_layout_current",
        "row_layout_history_floor",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "EntitySchemaDescription must keep `{field}` field key",
        );
    }
}

#[test]
fn entity_field_description_candid_shape_is_stable() {
    let fields = expect_record_fields(EntityFieldDescription::ty());

    for field in [
        "name",
        "slot",
        "kind",
        "primary_key",
        "queryable",
        "origin",
        "insert_omission",
        "insert_default",
        "insert_default_bytes",
        "insert_default_hash",
        "introduced_in_layout",
        "historical_fill",
        "historical_fill_bytes",
        "historical_fill_hash",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "EntityFieldDescription must keep `{field}` field key",
        );
    }

    assert!(
        matches!(
            expect_record_field_type(EntityFieldDescription::ty(), "slot").as_ref(),
            TypeInner::Nat16
        ),
        "EntityFieldDescription slot must remain plain nat16 for CLI/canister compatibility",
    );
}

#[test]
fn entity_index_description_candid_shape_is_stable() {
    let fields = expect_record_fields(EntityIndexDescription::ty());

    for field in ["name", "unique", "fields", "origin"] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "EntityIndexDescription must keep `{field}` field key",
        );
    }
}

#[test]
fn entity_relation_description_candid_shape_is_stable() {
    let fields = expect_record_fields(EntityRelationDescription::ty());

    for field in [
        "field",
        "target_path",
        "target_entity_name",
        "target_store_path",
        "cardinality",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "EntityRelationDescription must keep `{field}` field key",
        );
    }
}

#[test]
fn relation_cardinality_variant_labels_are_stable() {
    let mut cardinality_labels = expect_variant_labels(EntityRelationCardinality::ty());
    cardinality_labels.sort_unstable();
    assert_eq!(
        cardinality_labels,
        vec!["List".to_string(), "Set".to_string(), "Single".to_string()],
    );
}

#[test]
fn accepted_schema_describe_fails_closed_for_unresolved_composite_identity() {
    let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::UnresolvedComposite".to_string(),
        "UnresolvedComposite".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "value".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::test_composite(),
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        )],
    ));
    let value_catalog = accepted_value_catalog_for_models(&[]);

    let error = describe_entity_fields_with_persisted_schema(&snapshot, &value_catalog)
        .expect_err("unresolved accepted composite identities must fail closed");

    assert_eq!(error.class(), crate::error::ErrorClass::InvariantViolation,);
    assert_eq!(error.origin(), crate::error::ErrorOrigin::Store);
}

#[test]
fn describe_fixture_constructors_stay_usable() {
    let payload = EntitySchemaDescription::new(
        "entities::User".to_string(),
        "User".to_string(),
        "id".to_string(),
        vec!["id".to_string()],
        vec![EntityFieldDescription::new(
            "id".to_string(),
            Some(0),
            "ulid".to_string(),
            false,
            true,
            true,
            "generated".to_string(),
            Some("required".to_string()),
            None,
            None,
            None,
            Some(1),
            Some("reject".to_string()),
            None,
            None,
        )],
        vec![EntityIndexDescription::new(
            "idx_email".to_string(),
            true,
            vec!["email".to_string()],
            "generated".to_string(),
        )],
        vec![EntityRelationDescription::new(
            "account_id".to_string(),
            "entities::Account".to_string(),
            "Account".to_string(),
            "accounts".to_string(),
            EntityRelationCardinality::Single,
        )],
        1,
        1,
    );

    assert_eq!(payload.entity_name(), "User");
    assert_eq!(payload.primary_key(), "id");
    assert_eq!(payload.primary_key_fields(), ["id".to_string()].as_slice());
    assert_eq!(payload.fields().len(), 1);
    assert_eq!(payload.indexes().len(), 1);
    assert_eq!(payload.relations().len(), 1);
    assert_eq!(payload.row_layout_current(), 1);
    assert_eq!(payload.row_layout_history_floor(), 1);
}

#[test]
fn describe_entity_model_marks_all_composite_primary_key_fields() {
    let described = describe_entity_model(&DESCRIBE_COMPOSITE_PK_MODEL);
    let primary_key_fields = described
        .fields()
        .iter()
        .filter(|field| field.primary_key())
        .map(EntityFieldDescription::name)
        .collect::<Vec<_>>();

    assert_eq!(described.primary_key(), "tenant_id, local_id");
    assert_eq!(
        described.primary_key_fields(),
        ["tenant_id".to_string(), "local_id".to_string()].as_slice(),
    );
    assert_eq!(primary_key_fields, ["tenant_id", "local_id"]);
}

#[test]
fn schema_describe_relations_match_relation_field_metadata() {
    let metadata =
        relation_field_metadata_for_model_iter(&DESCRIBE_RELATION_MODEL).collect::<Vec<_>>();
    let described = describe_entity_model(&DESCRIBE_RELATION_MODEL);
    let relations = described.relations();

    assert_eq!(metadata.len(), relations.len());

    for (metadata, relation) in metadata.iter().zip(relations) {
        assert_eq!(relation.field(), metadata.field_name());
        assert_eq!(relation.target_path(), metadata.target_path());
        assert_eq!(relation.target_entity_name(), metadata.target_entity_name());
        assert_eq!(relation.target_store_path(), metadata.target_store_path());
        assert_eq!(
            relation.cardinality(),
            match metadata.cardinality() {
                RelationFieldCardinality::Single => EntityRelationCardinality::Single,
                RelationFieldCardinality::List => EntityRelationCardinality::List,
                RelationFieldCardinality::Set => EntityRelationCardinality::Set,
            }
        );
    }
}

#[test]
fn accepted_schema_describe_relations_use_persisted_relation_authority() {
    let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::AcceptedSource".to_string(),
        "AcceptedSource".to_string(),
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
                "accepted_targets".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Relation {
                    target_path: "accepted::Target".to_string(),
                    target_entity_name: "AcceptedTarget".to_string(),
                    target_entity_tag: EntityTag::new(0xD0A1),
                    target_store_path: "accepted::TargetStore".to_string(),
                    key_kind: Box::new(AcceptedFieldKind::Nat128),
                })),
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
        ],
    ));

    let value_catalog = accepted_value_catalog_for_models(&[&DESCRIBE_RELATION_MODEL]);
    let described = describe_entity_model_with_persisted_schema(
        &DESCRIBE_RELATION_MODEL,
        &snapshot,
        &value_catalog,
    )
    .expect("accepted relation schema should describe");

    assert_eq!(described.entity_path(), "entities::AcceptedSource");
    assert_eq!(described.entity_name(), "AcceptedSource");
    assert_eq!(
        described.primary_key_fields(),
        ["id".to_string()].as_slice()
    );
    assert_eq!(described.relations().len(), 1);

    let relation = &described.relations()[0];
    assert_eq!(relation.field(), "accepted_targets");
    assert_eq!(relation.target_path(), "accepted::Target");
    assert_eq!(relation.target_entity_name(), "AcceptedTarget");
    assert_eq!(relation.target_store_path(), "accepted::TargetStore");
    assert_eq!(relation.cardinality(), EntityRelationCardinality::Set);
}

#[test]
fn schema_describe_includes_text_max_len_contract() {
    static FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("name", FieldKind::Text { max_len: Some(16) }),
    ];
    static INDEXES: [&crate::model::index::IndexModel; 0] = [];
    static MODEL: EntityModel = EntityModel::generated(
        "entities::BoundedName",
        "BoundedName",
        1,
        &FIELDS[0],
        0,
        &FIELDS,
        &INDEXES,
    );

    let described = describe_entity_model(&MODEL);
    let name_field = described
        .fields()
        .iter()
        .find(|field| field.name() == "name")
        .expect("bounded text field should be described");

    assert_eq!(name_field.kind(), "text(max_len=16)");
}

#[test]
fn schema_describe_preserves_fixed_width_numeric_kind_labels() {
    static FIELDS: [FieldModel; 7] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("small_signed", FieldKind::Int8),
        FieldModel::generated("cell_x", FieldKind::Nat16),
        FieldModel::generated("large_signed", FieldKind::Int64),
        FieldModel::generated("large_unsigned", FieldKind::Nat64),
        FieldModel::generated("huge_signed", FieldKind::IntBig { max_bytes: 384 }),
        FieldModel::generated("huge_unsigned", FieldKind::NatBig { max_bytes: 512 }),
    ];
    static INDEXES: [&crate::model::index::IndexModel; 0] = [];
    static MODEL: EntityModel = EntityModel::generated(
        "entities::FixedWidthNumbers",
        "FixedWidthNumbers",
        1,
        &FIELDS[0],
        0,
        &FIELDS,
        &INDEXES,
    );

    let described = describe_entity_model(&MODEL)
        .fields()
        .iter()
        .map(|field| (field.name().to_string(), field.kind().to_string()))
        .collect::<Vec<_>>();

    assert!(described.contains(&("small_signed".to_string(), "int8".to_string())));
    assert!(described.contains(&("cell_x".to_string(), "nat16".to_string())));
    assert!(described.contains(&("large_signed".to_string(), "int64".to_string())));
    assert!(described.contains(&("large_unsigned".to_string(), "nat64".to_string())));
    assert!(described.contains(&(
        "huge_signed".to_string(),
        "int_big(max_bytes=384)".to_string()
    )));
    assert!(described.contains(&(
        "huge_unsigned".to_string(),
        "nat_big(max_bytes=512)".to_string()
    )));
}

#[test]
fn schema_describe_includes_generated_database_default_metadata() {
    static DEFAULT_PAYLOAD: &[u8] = &[0xFF, 0x01, 7, 0, 0, 0, 0, 0, 0, 0];
    static FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated_with_storage_decode_nullability_write_policies_database_default_and_nested_fields(
            "score",
            FieldKind::Nat64,
            FieldStorageDecode::ByKind,
            false,
            None,
            None,
            FieldDatabaseDefault::EncodedSlotPayload(DEFAULT_PAYLOAD),
            &[],
        ),
    ];
    static INDEXES: [&crate::model::index::IndexModel; 0] = [];
    static MODEL: EntityModel = EntityModel::generated(
        "entities::DefaultedScore",
        "DefaultedScore",
        1,
        &FIELDS[0],
        0,
        &FIELDS,
        &INDEXES,
    );

    let described = describe_entity_model(&MODEL);
    let score_field = described
        .fields()
        .iter()
        .find(|field| field.name() == "score")
        .expect("database-defaulted score field should be described");

    assert_eq!(score_field.kind(), "nat64");
    assert_eq!(score_field.insert_omission(), Some("default"));
    assert_eq!(
        score_field.insert_default(),
        Some("slot_payload(bytes=10, sha256=37746b8fe16bb6b4)")
    );
    assert_eq!(score_field.insert_default_bytes(), Some(10));
    assert_eq!(score_field.insert_default_hash(), Some("37746b8fe16bb6b4"));
}

#[test]
fn schema_describe_uses_accepted_top_level_field_metadata() {
    const DEFAULT_PAYLOAD: &[u8] = &[0xFF, 0x01, 7, 0, 0, 0, 0, 0, 0, 0];
    let id_slot = SchemaFieldSlot::new(0);
    let payload_slot = SchemaFieldSlot::new(7);
    // The accepted wrapper below is intentionally inconsistent so this
    // metadata boundary proves row-layout authority owns slot answers.
    let stale_payload_field_slot = SchemaFieldSlot::new(3);
    let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::BlobEvent".to_string(),
        "BlobEvent".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), id_slot),
            (FieldId::new(2), payload_slot),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                id_slot,
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "payload".to_string(),
                stale_payload_field_slot,
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::SlotPayload(DEFAULT_PAYLOAD.to_vec()),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
        ],
    ));

    let value_catalog = accepted_value_catalog_for_models(&[]);
    let described = describe_entity_fields_with_persisted_schema(&snapshot, &value_catalog)
        .expect("accepted field metadata should describe");
    let described_identity = described
        .iter()
        .map(|field| {
            (
                field.name().to_string(),
                field.slot(),
                field.kind().to_string(),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        described_identity,
        vec![
            ("id".to_string(), Some(0), "ulid".to_string()),
            ("payload".to_string(), Some(7), "nat64".to_string()),
        ],
    );
    let payload = described
        .iter()
        .find(|field| field.name() == "payload")
        .expect("defaulted payload field should describe");
    assert_eq!(payload.insert_omission(), Some("default"));
    assert_eq!(payload.insert_default(), Some("7"));
    assert_eq!(payload.insert_default_bytes(), Some(10));
    assert_eq!(payload.insert_default_hash(), Some("37746b8fe16bb6b4"));
    assert_eq!(payload.introduced_in_layout(), Some(1));
    assert_eq!(payload.historical_fill(), Some("reject"));
}

#[test]
fn accepted_schema_describe_keeps_future_default_and_historical_fill_distinct() {
    const CURRENT_DEFAULT: &[u8] = &[0xFF, 0x01, 7, 0, 0, 0, 0, 0, 0, 0];
    const HISTORICAL_FILL: &[u8] = &[0xFF, 0x01, 9, 0, 0, 0, 0, 0, 0, 0];
    let current_layout = RowLayoutVersion::new(2).expect("layout two should be valid");
    let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::new(2),
        "entities::TemporalScore".to_string(),
        "TemporalScore".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            current_layout,
            RowLayoutVersion::INITIAL,
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
            PersistedFieldSnapshot::new_with_write_policy(
                FieldId::new(2),
                "score".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                current_layout,
                SchemaInsertDefault::SlotPayload(CURRENT_DEFAULT.to_vec()),
                SchemaHistoricalFill::SlotPayload(HISTORICAL_FILL.to_vec()),
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
        ],
    ));
    let value_catalog = accepted_value_catalog_for_models(&[]);

    let described = describe_entity_model_with_persisted_schema(
        &DESCRIBE_EXACT_MODEL,
        &snapshot,
        &value_catalog,
    )
    .expect("accepted temporal facts should describe");
    let score = described
        .fields()
        .iter()
        .find(|field| field.name() == "score")
        .expect("temporal score should describe");

    assert_eq!(described.row_layout_current(), 2);
    assert_eq!(described.row_layout_history_floor(), 1);
    assert_eq!(score.insert_omission(), Some("default"));
    assert_eq!(score.insert_default(), Some("7"));
    assert_eq!(score.introduced_in_layout(), Some(2));
    assert_eq!(score.historical_fill(), Some("9"));
    assert_ne!(score.insert_default_hash(), score.historical_fill_hash());
}

#[test]
fn accepted_schema_describe_rejects_malformed_temporal_payload() {
    let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::MalformedDefault".to_string(),
        "MalformedDefault".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "score".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::SlotPayload(vec![0x00]),
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )],
    ));
    let value_catalog = accepted_value_catalog_for_models(&[]);

    let error = describe_entity_fields_with_persisted_schema(&snapshot, &value_catalog)
        .expect_err("malformed accepted default payload must fail introspection closed");

    assert_eq!(error.class(), crate::error::ErrorClass::Corruption);
    assert_eq!(error.origin(), crate::error::ErrorOrigin::Serialize);
}

#[test]
fn schema_temporal_text_rendering_cannot_collide_with_sentinel_labels() {
    assert_eq!(
        bounded_schema_value_rendering(
            &OutputValue::Text("null".to_string()),
            b"null",
            short_default_payload_fingerprint(b"null").as_str(),
        ),
        "'null'",
    );
    assert_eq!(
        bounded_schema_value_rendering(
            &OutputValue::Text("reject\n'value'".to_string()),
            b"reject-value",
            short_default_payload_fingerprint(b"reject-value").as_str(),
        ),
        "'reject\\n\\'value\\''",
    );
}

#[test]
fn schema_describe_preserves_accepted_fixed_width_numeric_kind_labels() {
    let id_slot = SchemaFieldSlot::new(0);
    let x_slot = SchemaFieldSlot::new(1);
    let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::Grid".to_string(),
        "Grid".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), id_slot), (FieldId::new(2), x_slot)]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                id_slot,
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "x".to_string(),
                x_slot,
                AcceptedFieldKind::Nat16,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
        ],
    ));

    let value_catalog = accepted_value_catalog_for_models(&[]);
    let described = describe_entity_fields_with_persisted_schema(&snapshot, &value_catalog)
        .expect("accepted numeric field should describe");
    let x = described
        .iter()
        .find(|field| field.name() == "x")
        .expect("accepted fixed-width field should be described");

    assert_eq!(x.kind(), "nat16");
}

#[test]
fn schema_describe_uses_accepted_nested_leaf_metadata() {
    let value_catalog = accepted_value_catalog_for_models(&[&DESCRIBE_EXACT_MODEL]);
    let snapshot = compiled_schema_proposal_for_model(&DESCRIBE_EXACT_MODEL)
        .initial_persisted_schema_snapshot_with_catalogs(
            value_catalog.enum_catalog(),
            value_catalog.composite_catalog(),
        )
        .expect("exact composite proposal should resolve through accepted catalogs");
    let snapshot = AcceptedSchemaSnapshot::try_new(snapshot)
        .expect("exact composite proposal should produce a valid accepted snapshot");
    let described = describe_entity_fields_with_persisted_schema(&snapshot, &value_catalog)
        .expect("accepted nested leaf metadata should describe");
    let rank = described
        .iter()
        .find(|field| field.name() == "└─ rank")
        .expect("accepted nested leaf should be described");

    assert_eq!(rank.slot(), None);
    assert_eq!(rank.kind(), "nat64");
    assert!(rank.nullable());
    assert!(rank.queryable());
    assert_eq!(rank.insert_omission(), None);
    assert_eq!(rank.introduced_in_layout(), None);
    assert_eq!(rank.historical_fill(), None);
}

#[test]
fn schema_describe_expands_generated_composite_field_leaves() {
    static NESTED_FIELDS: [FieldModel; 3] = [
        FieldModel::generated("name", FieldKind::Text { max_len: None }),
        FieldModel::generated("level", FieldKind::Nat64),
        FieldModel::generated("pid", FieldKind::Principal),
    ];
    static COMPOSITE_FIELDS: [CompositeFieldModel; 3] = [
        CompositeFieldModel::generated("name", FieldKind::Text { max_len: None }, false),
        CompositeFieldModel::generated("level", FieldKind::Nat64, false),
        CompositeFieldModel::generated("pid", FieldKind::Principal, false),
    ];
    static COMPOSITE_SHAPE: CompositeShapeModel = CompositeShapeModel::Record(&COMPOSITE_FIELDS);
    static COMPOSITE_KIND: FieldKind = FieldKind::Composite {
        path: "schema::describe::tests::Mentor",
        codec: CompositeCodec::StructuralV1,
        shape: &COMPOSITE_SHAPE,
    };
    static FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated_with_storage_decode_nullability_write_policies_and_nested_fields(
            "mentor",
            COMPOSITE_KIND,
            FieldStorageDecode::CatalogValue,
            false,
            None,
            None,
            &NESTED_FIELDS,
        ),
    ];
    static INDEXES: [&crate::model::index::IndexModel; 0] = [];
    static MODEL: EntityModel = EntityModel::generated(
        "entities::Character",
        "Character",
        1,
        &FIELDS[0],
        0,
        &FIELDS,
        &INDEXES,
    );

    let described = describe_entity_model(&MODEL);
    let described_fields = described
        .fields()
        .iter()
        .map(|field| (field.name(), field.slot(), field.kind(), field.queryable()))
        .collect::<Vec<_>>();

    assert_eq!(
        described_fields,
        vec![
            ("id", Some(0), "ulid", true),
            (
                "mentor",
                Some(1),
                "composite(path=schema::describe::tests::Mentor, codec=structural_v1, shape=record{name:text(unbounded), level:nat64, pid:principal})",
                false,
            ),
            ("├─ name", None, "text(unbounded)", true),
            ("├─ level", None, "nat64", true),
            ("└─ pid", None, "principal", true),
        ],
    );
}
