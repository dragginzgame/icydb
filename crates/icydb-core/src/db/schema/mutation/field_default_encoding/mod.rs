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
    let payload = encode_input_value_for_accepted_field_contract(&encoding, input, &mut budget)
        .map_err(|_| SchemaDdlFieldDefaultEncodingError::Encoding)?;

    Ok(SchemaInsertDefault::SlotPayload(payload))
}

#[cfg(test)]
mod tests;
