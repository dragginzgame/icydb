//! Module: executor::aggregate::runtime::grouped_fold::utils::hashing
//! Responsibility: allocation-free grouped-count stable hash construction.
//! Boundary: mirrors owned `GroupKey` canonical hashing without materializing keys.

use crate::{
    db::{
        executor::{
            group::{StableHash, stable_hash_from_digest},
            pipeline::runtime::RowView,
        },
        query::plan::FieldSlot,
    },
    error::InternalError,
    value::{Value, ValueHashWriter, hash_single_list_identity_canonical_value},
};

// Hash one virtual grouped key list directly from borrowed row slots so the
// grouped `COUNT(*)` fast path does not allocate `Vec<Value>` on lookups.
#[inline]
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn stable_hash_group_values_from_row_view(
    row_view: &RowView,
    group_fields: &[FieldSlot],
) -> Result<StableHash, InternalError> {
    let mut hash_writer = ValueHashWriter::new();
    hash_writer.write_list_prefix(group_fields.len());

    for field in group_fields {
        hash_writer.write_list_value(row_view.require_slot_ref(field.index())?)?;
    }

    Ok(stable_hash_from_digest(hash_writer.finish()))
}

// Hash one canonical single grouped value through the same one-element list
// framing used by grouped-count key materialization.
#[inline]
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn stable_hash_single_group_value(
    group_value: &Value,
) -> Result<StableHash, InternalError> {
    if let Some(digest) = hash_single_list_identity_canonical_value(group_value)? {
        return Ok(stable_hash_from_digest(digest));
    }

    let mut hash_writer = ValueHashWriter::new();
    hash_writer.write_list_prefix(1);
    hash_writer.write_list_value(group_value)?;

    Ok(stable_hash_from_digest(hash_writer.finish()))
}
