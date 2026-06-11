//! Schema-owned field default encoding helpers for DDL-authored candidates.

use crate::db::{
    data::encode_runtime_value_for_accepted_field_contract,
    schema::{
        AcceptedFieldDecodeContract, PersistedFieldKind, PersistedFieldSnapshot,
        SchemaFieldDefault, canonicalize_strict_sql_literal_for_persisted_kind,
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
    kind: &PersistedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
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
    )
}

/// Encode an ALTER COLUMN SET DEFAULT payload through the current accepted
/// field contract.
pub(in crate::db) fn encode_sql_ddl_alter_column_default(
    field: &PersistedFieldSnapshot,
    default: &Value,
) -> Result<SchemaFieldDefault, SchemaDdlFieldDefaultEncodingError> {
    encode_sql_ddl_field_default_payload(
        field.name(),
        default,
        field.kind(),
        field.nullable(),
        field.storage_decode(),
        field.leaf_codec(),
    )
}

fn encode_sql_ddl_field_default_payload(
    field_name: &str,
    default: &Value,
    kind: &PersistedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
) -> Result<SchemaFieldDefault, SchemaDdlFieldDefaultEncodingError> {
    if matches!(default, Value::Null) {
        return Err(SchemaDdlFieldDefaultEncodingError::NullDefault);
    }

    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(kind, default)
        .unwrap_or_else(|| default.clone());
    let contract =
        AcceptedFieldDecodeContract::new(field_name, kind, nullable, storage_decode, leaf_codec);
    let payload = encode_runtime_value_for_accepted_field_contract(contract, &normalized)
        .map_err(|_| SchemaDdlFieldDefaultEncodingError::Encoding)?;

    Ok(SchemaFieldDefault::SlotPayload(payload))
}
