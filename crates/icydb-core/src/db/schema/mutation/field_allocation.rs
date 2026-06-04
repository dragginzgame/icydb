//! Schema-owned field allocation helpers for DDL-authored field candidates.

use crate::db::schema::{
    AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedFieldOrigin,
    PersistedFieldSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaFieldWritePolicy,
};
use crate::model::field::{FieldStorageDecode, LeafCodec};

/// Build one DDL-owned additive field candidate with schema-owned ID and slot
/// allocation. SQL DDL supplies author intent; schema mutation code assigns
/// durable catalog identity.
pub(in crate::db) fn build_sql_ddl_field_addition_candidate(
    accepted_before: &AcceptedSchemaSnapshot,
    name: String,
    kind: PersistedFieldKind,
    nullable: bool,
    default: SchemaFieldDefault,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
) -> PersistedFieldSnapshot {
    PersistedFieldSnapshot::new_with_write_policy_and_origin(
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
    )
}

fn next_sql_ddl_field_id(accepted_before: &AcceptedSchemaSnapshot) -> FieldId {
    let snapshot = accepted_before.persisted_snapshot();
    let next = snapshot
        .fields()
        .iter()
        .map(|field| field.id().get())
        .chain(
            snapshot
                .row_layout()
                .retired_field_slots()
                .iter()
                .map(|(field_id, _)| field_id.get()),
        )
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .expect("accepted field IDs should not be exhausted");

    FieldId::new(next)
}

fn next_sql_ddl_field_slot(accepted_before: &AcceptedSchemaSnapshot) -> SchemaFieldSlot {
    accepted_before
        .persisted_snapshot()
        .row_layout()
        .next_unallocated_slot()
}
