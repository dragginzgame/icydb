//! Schema-owned field allocation helpers for DDL-authored field candidates.

use crate::db::schema::{
    AcceptedFieldKind, AcceptedSchemaSnapshot, FieldId, PersistedFieldOrigin,
    PersistedFieldSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaFieldWritePolicy,
};
use crate::model::field::{FieldStorageDecode, LeafCodec};

/// Field addition candidate resolution failures for SQL DDL-authored schema
/// mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlFieldAdditionCandidateError {
    /// An accepted field already uses the requested SQL column name.
    Duplicate,
    /// Required ADD COLUMN without a database default cannot backfill existing rows.
    RequiredWithoutDefault,
}

/// Resolve the accepted name boundary for one SQL DDL field addition before
/// SQL type or default decoding performs work for an already-existing column.
pub(in crate::db) fn resolve_sql_ddl_field_addition_name_candidate(
    accepted_before: &AcceptedSchemaSnapshot,
    name: &str,
) -> Result<(), SchemaDdlFieldAdditionCandidateError> {
    if accepted_before
        .persisted_snapshot()
        .fields()
        .iter()
        .any(|field| field.name() == name)
    {
        return Err(SchemaDdlFieldAdditionCandidateError::Duplicate);
    }

    Ok(())
}

/// Build one DDL-owned additive field candidate with schema-owned ID and slot
/// allocation. SQL DDL supplies author intent; schema mutation code assigns
/// durable catalog identity.
pub(in crate::db) fn build_sql_ddl_field_addition_candidate(
    accepted_before: &AcceptedSchemaSnapshot,
    name: String,
    kind: AcceptedFieldKind,
    nullable: bool,
    default: SchemaFieldDefault,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
) -> Result<PersistedFieldSnapshot, SchemaDdlFieldAdditionCandidateError> {
    resolve_sql_ddl_field_addition_name_candidate(accepted_before, name.as_str())?;

    if !nullable && default.is_none() {
        return Err(SchemaDdlFieldAdditionCandidateError::RequiredWithoutDefault);
    }

    Ok(PersistedFieldSnapshot::new_with_write_policy_and_origin(
        next_sql_ddl_field_id(accepted_before),
        name,
        next_sql_ddl_field_slot(accepted_before),
        kind,
        Vec::new(),
        nullable,
        default,
        SchemaFieldWritePolicy::from_model_policies(None, None),
        PersistedFieldOrigin::SqlDdl,
        storage_decode,
        leaf_codec,
    ))
}

fn next_sql_ddl_field_id(accepted_before: &AcceptedSchemaSnapshot) -> FieldId {
    let next = u32::try_from(accepted_before.persisted_snapshot().fields().len())
        .ok()
        .and_then(|count| count.checked_add(1))
        .expect("accepted field IDs should not be exhausted");

    FieldId::new(next)
}

fn next_sql_ddl_field_slot(accepted_before: &AcceptedSchemaSnapshot) -> SchemaFieldSlot {
    accepted_before
        .persisted_snapshot()
        .row_layout()
        .next_unallocated_slot()
}
