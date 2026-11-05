use std::collections::BTreeMap;

use super::{
    AcceptedNamedTypeIdentity, AcceptedSourceBindingCatalog, decode_accepted_source_bindings,
    encode_accepted_source_bindings,
};
use crate::{
    db::schema::{
        AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision,
        AcceptedSchemaRevisionBundle, CandidateSchemaRevision, ConstraintId, FieldId,
        FieldStorageDecode, LeafCodec, PersistedFieldSnapshot, PersistedSchemaSnapshot,
        ScalarCodec, SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
        TestEnumDefinition, TestEnumVariant, build_accepted_enum_catalog_for_tests,
        composite_catalog::{
            AcceptedCompositeElement, AcceptedCompositeField, AcceptedCompositeShape,
            CompositeFieldId, CompositeTypeId,
        },
        empty_accepted_enum_catalog_for_tests,
    },
    types::EntityTag,
};
use icydb_schema::{ConstraintSourceKey, EntitySourceKey, FieldSourceKey};

fn status_enum_definition() -> TestEnumDefinition {
    TestEnumDefinition::new(
        "test::Status",
        vec![
            TestEnumVariant::unit("Active"),
            TestEnumVariant::unit("Disabled"),
        ],
    )
}

#[test]
fn empty_source_binding_catalog_has_one_canonical_current_form() {
    let enums = empty_accepted_enum_catalog_for_tests();
    let composites = AcceptedCompositeCatalog::empty();
    let entities = BTreeMap::new();
    let catalog = AcceptedSourceBindingCatalog::default();

    let encoded = encode_accepted_source_bindings(&catalog, &enums, &composites, &entities)
        .expect("empty source bindings should encode");
    let decoded = decode_accepted_source_bindings(&encoded, &enums, &composites, &entities)
        .expect("empty source bindings should decode");

    assert_eq!(decoded, catalog);
}

#[test]
fn constraint_binding_insertion_enforces_entity_local_identity() {
    let mut catalog = AcceptedSourceBindingCatalog::default();
    let first_entity = EntityTag::new(7);
    let second_entity = EntityTag::new(8);
    let first_source =
        ConstraintSourceKey::try_new("test:first").expect("first constraint source should build");
    let second_source =
        ConstraintSourceKey::try_new("test:second").expect("second constraint source should build");
    let third_source =
        ConstraintSourceKey::try_new("test:third").expect("third constraint source should build");
    let local_id = ConstraintId::new(1).expect("local constraint ID should build");

    catalog
        .insert_constraint(first_entity, first_source.clone(), local_id)
        .expect("first entity should accept its local ID");
    catalog
        .insert_constraint(second_entity, second_source, local_id)
        .expect("another entity may reuse the same local ID");

    assert!(
        catalog
            .insert_constraint(first_entity, third_source, local_id)
            .is_err(),
    );
    assert!(
        catalog
            .insert_constraint(
                first_entity,
                first_source,
                ConstraintId::new(2).expect("second local constraint ID should build"),
            )
            .is_err(),
    );
}

#[test]
fn field_removal_carries_retained_source_identity_through_dense_ids() {
    let entity = EntityTag::new(7);
    let first = FieldSourceKey::try_new("test:first").expect("first field source should build");
    let removed =
        FieldSourceKey::try_new("test:removed").expect("removed field source should build");
    let retained =
        FieldSourceKey::try_new("test:retained").expect("retained field source should build");
    let mut catalog = AcceptedSourceBindingCatalog::default();
    catalog
        .fields
        .insert((entity, first.clone()), FieldId::new(1));
    catalog
        .fields
        .insert((entity, removed.clone()), FieldId::new(2));
    catalog
        .fields
        .insert((entity, retained.clone()), FieldId::new(3));

    catalog
        .remove_field_and_remap(
            entity,
            &removed,
            FieldId::new(2),
            |field_id| match field_id.get() {
                1 => Some(FieldId::new(1)),
                3 => Some(FieldId::new(2)),
                _ => None,
            },
        )
        .expect("exact removal lineage should apply");

    assert_eq!(catalog.field(entity, &first), Some(FieldId::new(1)));
    assert_eq!(catalog.field(entity, &retained), Some(FieldId::new(2)));
    assert_eq!(catalog.field(entity, &removed), None);
}

#[test]
fn field_rekey_uniqueness_is_entity_scoped() {
    let first_entity = EntityTag::new(7);
    let second_entity = EntityTag::new(8);
    let field_id = FieldId::new(1);
    let old = FieldSourceKey::try_new("old").expect("source should admit");
    let new = FieldSourceKey::try_new("new").expect("source should admit");
    let other = FieldSourceKey::try_new("other").expect("source should admit");
    let alias = FieldSourceKey::try_new("alias").expect("source should admit");
    let mut catalog = AcceptedSourceBindingCatalog::default();
    catalog.fields.insert((first_entity, old.clone()), field_id);
    catalog
        .fields
        .insert((second_entity, other.clone()), field_id);

    catalog
        .rekey_field(first_entity, &old, new.clone(), field_id)
        .expect("the same entity-local ID in another entity must not conflict");
    assert_eq!(catalog.field(first_entity, &new), Some(field_id));
    assert_eq!(catalog.field(second_entity, &other), Some(field_id));

    catalog.fields.insert((first_entity, alias), field_id);
    assert!(
        catalog
            .rekey_field(
                first_entity,
                &new,
                FieldSourceKey::try_new("newer").expect("source should admit"),
                field_id,
            )
            .is_err(),
        "two source keys in one entity must not alias one field ID",
    );
    assert_eq!(catalog.field(first_entity, &new), Some(field_id));
}

#[test]
fn entity_rekey_conflict_retains_the_predecessor_binding() {
    let entity = EntityTag::new(7);
    let old = EntitySourceKey::try_new("old").expect("source should admit");
    let alias = EntitySourceKey::try_new("alias").expect("source should admit");
    let mut catalog = AcceptedSourceBindingCatalog::default();
    catalog.entities.insert(old.clone(), entity);
    catalog.entities.insert(alias, entity);

    assert!(
        catalog
            .rekey_entity(
                &old,
                EntitySourceKey::try_new("new").expect("source should admit"),
                entity,
            )
            .is_err(),
        "one accepted entity ID must not acquire two source bindings",
    );
    assert_eq!(catalog.entity(&old), Some(entity));
}

#[test]
fn source_binding_catalog_rejects_missing_structural_owner() {
    let enums = empty_accepted_enum_catalog_for_tests();
    let composites = AcceptedCompositeCatalog::empty();
    let entities = BTreeMap::new();
    let mut catalog = AcceptedSourceBindingCatalog::default();
    catalog.entities.insert(
        icydb_schema::EntitySourceKey::try_new("test:missing").expect("source key should build"),
        EntityTag::new(7),
    );

    assert!(encode_accepted_source_bindings(&catalog, &enums, &composites, &entities).is_err());
}

#[test]
fn source_binding_catalog_round_trips_existing_accepted_identities() {
    let enums = empty_accepted_enum_catalog_for_tests();
    let composites = AcceptedCompositeCatalog::empty();
    let entity = EntityTag::new(7);
    let field = FieldId::new(1);
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "test::Bound".to_string(),
        "Bound".to_string(),
        field,
        SchemaRowLayout::initial(vec![(field, SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            field,
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )],
    );
    let entities = BTreeMap::from([(entity, snapshot)]);
    let mut catalog = AcceptedSourceBindingCatalog::default();
    catalog.entities.insert(
        icydb_schema::EntitySourceKey::try_new("test:bound")
            .expect("entity source key should build"),
        entity,
    );
    catalog.fields.insert(
        (
            entity,
            icydb_schema::FieldSourceKey::try_new("test:bound:id")
                .expect("field source key should build"),
        ),
        field,
    );

    let encoded = encode_accepted_source_bindings(&catalog, &enums, &composites, &entities)
        .expect("closed source bindings should encode");
    let decoded = decode_accepted_source_bindings(&encoded, &enums, &composites, &entities)
        .expect("closed source bindings should decode");

    assert_eq!(decoded, catalog);
}

#[test]
fn source_binding_catalog_closes_enum_variant_source_identities() {
    let enums = build_accepted_enum_catalog_for_tests(&[status_enum_definition()])
        .expect("enum catalog should build");
    let composites = AcceptedCompositeCatalog::empty();
    let entities = BTreeMap::new();
    let enum_type = enums
        .type_id("test::Status")
        .expect("accepted enum type should exist");
    let definition = enums
        .enum_type(enum_type)
        .expect("accepted enum definition should exist");
    let active = definition
        .variant_id("Active")
        .expect("accepted active variant should exist");
    let disabled = definition
        .variant_id("Disabled")
        .expect("accepted disabled variant should exist");
    let mut catalog = AcceptedSourceBindingCatalog::default();
    catalog.types.insert(
        icydb_schema::TypeSourceKey::try_new("test:status").expect("type source key should build"),
        AcceptedNamedTypeIdentity::Enum(enum_type),
    );
    catalog.enum_variants.insert(
        (
            enum_type,
            icydb_schema::TypeSourceKey::try_new("test:status:active")
                .expect("variant source key should build"),
        ),
        active,
    );
    catalog.enum_variants.insert(
        (
            enum_type,
            icydb_schema::TypeSourceKey::try_new("test:status:disabled")
                .expect("variant source key should build"),
        ),
        disabled,
    );

    let encoded = encode_accepted_source_bindings(&catalog, &enums, &composites, &entities)
        .expect("closed enum bindings should encode");
    let decoded = decode_accepted_source_bindings(&encoded, &enums, &composites, &entities)
        .expect("closed enum bindings should decode");

    assert_eq!(decoded, catalog);
    let adapter_names = catalog
        .typed_adapter_names(&enums, &composites)
        .expect("accepted adapter names should resolve");
    assert_eq!(
        adapter_names.named_types,
        vec![("test:status".to_string(), "test::Status".to_string())],
    );
    assert_eq!(
        adapter_names.enum_variants,
        vec![
            (
                "test:status".to_string(),
                "test:status:active".to_string(),
                "Active".to_string(),
            ),
            (
                "test:status".to_string(),
                "test:status:disabled".to_string(),
                "Disabled".to_string(),
            ),
        ],
    );

    let mut incomplete = catalog.clone();
    incomplete
        .enum_variants
        .retain(|(_, source), _| source.as_str() != "test:status:disabled");
    assert!(
        encode_accepted_source_bindings(&incomplete, &enums, &composites, &entities).is_err(),
        "a bound enum type must bind every accepted variant",
    );
}

#[test]
fn source_binding_catalog_closes_record_member_source_identities() {
    let enums = empty_accepted_enum_catalog_for_tests();
    let composite_type = CompositeTypeId::new(1).expect("one is non-zero");
    let alpha = CompositeFieldId::new(1).expect("one is non-zero");
    let zeta = CompositeFieldId::new(2).expect("two is non-zero");
    let composites = AcceptedCompositeCatalog::from_initial_definitions(
        BTreeMap::from([(
            composite_type,
            (
                "test::Record".to_string(),
                AcceptedCompositeShape::Record(vec![
                    AcceptedCompositeField::new(
                        alpha,
                        "alpha".to_string(),
                        AcceptedCompositeElement::new(AcceptedFieldKind::Nat64, false),
                    ),
                    AcceptedCompositeField::new(
                        zeta,
                        "zeta".to_string(),
                        AcceptedCompositeElement::new(AcceptedFieldKind::Bool, false),
                    ),
                ]),
            ),
        )]),
        &enums,
    )
    .expect("record catalog should build");
    let entities = BTreeMap::new();
    let mut catalog = AcceptedSourceBindingCatalog::default();
    catalog.types.insert(
        icydb_schema::TypeSourceKey::try_new("test:record").expect("type source key should build"),
        AcceptedNamedTypeIdentity::Composite(composite_type),
    );
    catalog.composite_fields.insert(
        (
            composite_type,
            icydb_schema::FieldSourceKey::try_new("test:record:alpha")
                .expect("field source key should build"),
        ),
        alpha,
    );
    catalog.composite_fields.insert(
        (
            composite_type,
            icydb_schema::FieldSourceKey::try_new("test:record:zeta")
                .expect("field source key should build"),
        ),
        zeta,
    );

    let encoded = encode_accepted_source_bindings(&catalog, &enums, &composites, &entities)
        .expect("closed record bindings should encode");
    let decoded = decode_accepted_source_bindings(&encoded, &enums, &composites, &entities)
        .expect("closed record bindings should decode");

    assert_eq!(decoded, catalog);
    let adapter_names = catalog
        .typed_adapter_names(&enums, &composites)
        .expect("accepted adapter names should resolve");
    assert_eq!(
        adapter_names.named_types,
        vec![("test:record".to_string(), "test::Record".to_string())],
    );
    assert_eq!(
        adapter_names.composite_fields,
        vec![
            (
                "test:record".to_string(),
                "test:record:alpha".to_string(),
                "alpha".to_string(),
            ),
            (
                "test:record".to_string(),
                "test:record:zeta".to_string(),
                "zeta".to_string(),
            ),
        ],
    );

    let mut incomplete = catalog;
    incomplete
        .composite_fields
        .retain(|(_, source), _| source.as_str() != "test:record:zeta");
    assert!(
        encode_accepted_source_bindings(&incomplete, &enums, &composites, &entities).is_err(),
        "a bound record type must bind every accepted member",
    );
}

#[test]
fn accepted_schema_fingerprint_covers_source_bindings() {
    let enums = empty_accepted_enum_catalog_for_tests();
    let composites = AcceptedCompositeCatalog::empty();
    let entity = EntityTag::new(7);
    let field = FieldId::new(1);
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "test::Bound".to_string(),
        "Bound".to_string(),
        field,
        SchemaRowLayout::initial(vec![(field, SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            field,
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )],
    );
    let entities = BTreeMap::from([(entity, snapshot)]);
    let mut first_bindings = AcceptedSourceBindingCatalog::default();
    first_bindings.entities.insert(
        icydb_schema::EntitySourceKey::try_new("test:first")
            .expect("first source key should build"),
        entity,
    );
    let mut second_bindings = AcceptedSourceBindingCatalog::default();
    second_bindings.entities.insert(
        icydb_schema::EntitySourceKey::try_new("test:second")
            .expect("second source key should build"),
        entity,
    );

    let first = CandidateSchemaRevision::new(
        AcceptedSchemaRevisionBundle::new_with_source_bindings(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            enums.clone(),
            composites.clone(),
            first_bindings,
            entities.clone(),
        )
        .expect("first bundle should close"),
    )
    .expect("first candidate should encode");
    let second = CandidateSchemaRevision::new(
        AcceptedSchemaRevisionBundle::new_with_source_bindings(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            enums,
            composites,
            second_bindings,
            entities,
        )
        .expect("second bundle should close"),
    )
    .expect("second candidate should encode");

    assert_ne!(
        first.root().fingerprint().as_bytes(),
        second.root().fingerprint().as_bytes(),
    );
}
