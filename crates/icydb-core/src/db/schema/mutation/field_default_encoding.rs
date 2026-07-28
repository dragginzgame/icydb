//! Schema-owned field default encoding helpers for DDL-authored candidates.

use crate::db::schema::{FieldStorageDecode, LeafCodec};
use crate::db::{
    data::encode_input_value_for_accepted_field_contract,
    schema::{
        AcceptedFieldDecodeContract, AcceptedFieldKind, AcceptedFieldPersistenceContract,
        AcceptedValueCatalogHandle, PersistedFieldSnapshot, SchemaInsertDefault,
        enum_catalog::ValueAdmissionBudget, input_value_from_strict_sql_literal_for_persisted_kind,
    },
};
use crate::value::Value;

/// Default payload encoding failures for SQL DDL-authored schema mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlFieldDefaultEncodingError {
    /// Accepted database defaults cannot persist explicit NULL payloads.
    NullDefault,
    /// The accepted field contract rejected the supplied runtime value.
    Encoding,
}

/// Encode an ADD COLUMN default through the accepted field contract selected by
/// schema mutation code.
pub(in crate::db) fn encode_sql_ddl_add_column_default(
    column_name: &str,
    default: Option<&Value>,
    kind: &AcceptedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
    catalog: Option<&AcceptedValueCatalogHandle>,
) -> Result<SchemaInsertDefault, SchemaDdlFieldDefaultEncodingError> {
    let Some(default) = default else {
        return Ok(SchemaInsertDefault::None);
    };
    encode_sql_ddl_field_default_payload(
        column_name,
        default,
        kind,
        nullable,
        storage_decode,
        leaf_codec,
        catalog,
    )
}

/// Encode an ALTER COLUMN SET DEFAULT payload through the current accepted
/// field contract.
pub(in crate::db) fn encode_sql_ddl_alter_column_default(
    field: &PersistedFieldSnapshot,
    default: &Value,
    catalog: Option<&AcceptedValueCatalogHandle>,
) -> Result<SchemaInsertDefault, SchemaDdlFieldDefaultEncodingError> {
    if matches!(default, Value::Null) && field.nullable() {
        return Ok(SchemaInsertDefault::None);
    }

    encode_sql_ddl_field_default_payload(
        field.name(),
        default,
        field.kind(),
        field.nullable(),
        field.storage_decode(),
        field.leaf_codec(),
        catalog,
    )
}

fn encode_sql_ddl_field_default_payload(
    field_name: &str,
    default: &Value,
    kind: &AcceptedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
    catalog: Option<&AcceptedValueCatalogHandle>,
) -> Result<SchemaInsertDefault, SchemaDdlFieldDefaultEncodingError> {
    if matches!(default, Value::Null) {
        return Err(SchemaDdlFieldDefaultEncodingError::NullDefault);
    }

    let field =
        AcceptedFieldDecodeContract::new(field_name, kind, nullable, storage_decode, leaf_codec);
    let input = input_value_from_strict_sql_literal_for_persisted_kind(kind, default)
        .ok_or(SchemaDdlFieldDefaultEncodingError::Encoding)?;
    let catalog = catalog.ok_or(SchemaDdlFieldDefaultEncodingError::Encoding)?;
    let encoding = AcceptedFieldPersistenceContract::new(catalog, field)
        .map_err(|_| SchemaDdlFieldDefaultEncodingError::Encoding)?;
    let mut budget = ValueAdmissionBudget::standard();
    let payload = encode_input_value_for_accepted_field_contract(encoding, input, &mut budget)
        .map_err(|_| SchemaDdlFieldDefaultEncodingError::Encoding)?;

    Ok(SchemaInsertDefault::SlotPayload(payload))
}

#[cfg(test)]
mod tests {
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
}
