//! Schema-owned field default encoding helpers for DDL-authored candidates.

use crate::db::{
    data::{
        encode_admitted_value_for_accepted_field_contract,
        encode_runtime_value_for_accepted_field_contract,
    },
    schema::{
        AcceptedEnumCatalogHandle, AcceptedFieldDecodeContract, AcceptedFieldKind,
        PersistedFieldSnapshot, SchemaFieldDefault,
        canonicalize_strict_sql_literal_for_persisted_kind,
        enum_catalog::{ValueAdmissionBudget, normalize_and_admit_persisted_field_value},
        input_value_from_strict_sql_literal_for_persisted_kind,
    },
};
use crate::model::field::{FieldStorageDecode, LeafCodec};
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
    catalog: Option<&AcceptedEnumCatalogHandle>,
) -> Result<SchemaFieldDefault, SchemaDdlFieldDefaultEncodingError> {
    let Some(default) = default else {
        return Ok(SchemaFieldDefault::None);
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
    catalog: Option<&AcceptedEnumCatalogHandle>,
) -> Result<SchemaFieldDefault, SchemaDdlFieldDefaultEncodingError> {
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
    catalog: Option<&AcceptedEnumCatalogHandle>,
) -> Result<SchemaFieldDefault, SchemaDdlFieldDefaultEncodingError> {
    if matches!(default, Value::Null) {
        return Err(SchemaDdlFieldDefaultEncodingError::NullDefault);
    }

    let contract =
        AcceptedFieldDecodeContract::new(field_name, kind, nullable, storage_decode, leaf_codec);
    let input = input_value_from_strict_sql_literal_for_persisted_kind(kind, default)
        .ok_or(SchemaDdlFieldDefaultEncodingError::Encoding)?;
    let payload = if let Some(catalog) = catalog {
        let mut budget = ValueAdmissionBudget::standard();
        let admitted = normalize_and_admit_persisted_field_value(
            catalog,
            kind,
            storage_decode,
            nullable,
            input,
            &mut budget,
        )
        .map_err(|_| SchemaDdlFieldDefaultEncodingError::Encoding)?;
        encode_admitted_value_for_accepted_field_contract(catalog, contract, &admitted)
            .map_err(|_| SchemaDdlFieldDefaultEncodingError::Encoding)?
    } else {
        if matches!(kind, AcceptedFieldKind::Enum { .. }) {
            return Err(SchemaDdlFieldDefaultEncodingError::Encoding);
        }
        let normalized = canonicalize_strict_sql_literal_for_persisted_kind(kind, default)
            .unwrap_or_else(|| default.clone());
        encode_runtime_value_for_accepted_field_contract(contract, &normalized)
            .map_err(|_| SchemaDdlFieldDefaultEncodingError::Encoding)?
    };

    Ok(SchemaFieldDefault::SlotPayload(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        data::validate_default_payload_for_accepted_field_contract,
        schema::{
            AcceptedSchemaRevision, FieldId, SchemaFieldSlot,
            enum_catalog::build_initial_accepted_enum_catalog_from_kinds_for_tests,
        },
    };
    use crate::model::field::{EnumVariantModel, FieldKind};

    static STATUS_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
        "Active",
        None,
        FieldStorageDecode::ByKind,
    )];
    static STATUS_KIND: FieldKind = FieldKind::Enum {
        path: "tests::DefaultStatus",
        variants: &STATUS_VARIANTS,
    };

    fn enum_field(kind: AcceptedFieldKind) -> PersistedFieldSnapshot {
        PersistedFieldSnapshot::new(
            FieldId::new(1),
            "status".to_string(),
            SchemaFieldSlot::new(0),
            kind,
            Vec::new(),
            false,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::StructuralFallback,
        )
    }

    #[test]
    fn sql_ddl_enum_default_is_catalog_admitted_and_id_backed() {
        let catalog = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[STATUS_KIND])
            .expect("enum catalog should build");
        let catalog =
            AcceptedEnumCatalogHandle::new_for_tests(catalog, AcceptedSchemaRevision::INITIAL);
        let field = enum_field(AcceptedFieldKind::from_model_kind(STATUS_KIND));

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
        validate_default_payload_for_accepted_field_contract(catalog.catalog(), contract, payload)
            .expect("encoded default should pass bundle validation");
    }

    #[test]
    fn sql_ddl_enum_default_requires_catalog_and_unit_variant() {
        let field = enum_field(AcceptedFieldKind::from_model_kind(STATUS_KIND));

        assert_eq!(
            encode_sql_ddl_alter_column_default(&field, &Value::Text("Active".to_string()), None,),
            Err(SchemaDdlFieldDefaultEncodingError::Encoding),
        );
        assert_eq!(
            encode_sql_ddl_alter_column_default(&field, &Value::Text("Missing".to_string()), None,),
            Err(SchemaDdlFieldDefaultEncodingError::Encoding),
        );
    }
}
