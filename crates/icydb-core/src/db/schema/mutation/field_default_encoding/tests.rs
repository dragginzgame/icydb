use super::*;
use crate::db::{
    data::validate_default_payload_for_accepted_field_contract,
    schema::{
        AcceptedSchemaRevision, FieldId, SchemaFieldSlot, TestEnumDefinition, TestEnumVariant,
        build_accepted_enum_catalog_for_tests,
    },
};

fn status_definition() -> TestEnumDefinition {
    TestEnumDefinition::new(
        "tests::DefaultStatus",
        vec![TestEnumVariant::unit("Active")],
    )
}

fn enum_field(kind: AcceptedFieldKind) -> PersistedFieldSnapshot {
    PersistedFieldSnapshot::new_initial(
        FieldId::new(1),
        "status".to_string(),
        SchemaFieldSlot::new(0),
        kind,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Structural,
    )
}

#[test]
fn sql_ddl_enum_default_is_catalog_admitted_and_id_backed() {
    let catalog = build_accepted_enum_catalog_for_tests(&[status_definition()])
        .expect("enum catalog should build");
    let type_id = catalog
        .type_id("tests::DefaultStatus")
        .expect("status type should exist");
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        catalog,
        crate::db::schema::AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let field = enum_field(AcceptedFieldKind::Enum { type_id });

    let default = encode_sql_ddl_alter_column_default(
        &field,
        &Value::Text("Active".to_string()),
        Some(&catalog),
    )
    .expect("unit enum default should admit through the accepted catalog");
    let payload = default
        .slot_payload()
        .expect("default should own a payload");

    assert_eq!(payload.first(), Some(&0x84));
    let contract = AcceptedFieldDecodeContract::new(
        field.name(),
        field.kind(),
        field.nullable(),
        field.storage_decode(),
        field.leaf_codec(),
    );
    validate_default_payload_for_accepted_field_contract(
        catalog.enum_catalog(),
        catalog.composite_catalog(),
        contract,
        payload,
    )
    .expect("encoded default should pass bundle validation");
}

#[test]
fn sql_ddl_enum_default_requires_catalog_and_rejects_unknown_variant() {
    let catalog = build_accepted_enum_catalog_for_tests(&[status_definition()])
        .expect("enum catalog should build");
    let type_id = catalog
        .type_id("tests::DefaultStatus")
        .expect("status type should exist");
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        catalog,
        crate::db::schema::AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let field = enum_field(AcceptedFieldKind::Enum { type_id });

    assert_eq!(
        encode_sql_ddl_alter_column_default(&field, &Value::Text("Active".to_string()), None,),
        Err(SchemaDdlFieldDefaultEncodingError::Encoding),
    );
    assert_eq!(
        encode_sql_ddl_alter_column_default(
            &field,
            &Value::Text("Missing".to_string()),
            Some(&catalog),
        ),
        Err(SchemaDdlFieldDefaultEncodingError::Encoding),
    );
}
