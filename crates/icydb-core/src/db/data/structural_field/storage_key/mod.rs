//! Module: data::structural_field::storage_key
//! Responsibility: direct persisted-field decoding into canonical `StorageKey` forms.
//! Does not own: generic runtime `Value` decode, composite `ByKind` recursion, or low-level structural binary walking.
//! Boundary: relation and index integrity paths call into this module when they need keys without rebuilding `Value`.

mod decode;
mod encode;
mod scalar;
#[cfg(test)]
mod tests;

use crate::{
    db::{data::structural_field::FieldDecodeError, schema::PersistedFieldKind},
    error::InternalError,
    model::field::FieldKind,
    value::StorageKey,
};

pub(in crate::db) use crate::db::data::structural_field::storage_key::{
    decode::{decode_storage_key_binary_value_bytes, validate_storage_key_binary_value_bytes},
    encode::encode_storage_key_binary_value_bytes,
};

///
/// RelationKeyDecodeState
///
/// RelationKeyDecodeState carries the output buffer plus the relation key kind
/// while the Structural Binary v1 list walker visits relation items.
///
type RelationKeyDecodeState = (Vec<StorageKey>, FieldKind);
type AcceptedRelationKeyDecodeState<'a> = (Vec<StorageKey>, &'a PersistedFieldKind);

/// Return whether this field kind is owned by the Structural Binary v1
/// storage-key lane.
pub(in crate::db) const fn supports_storage_key_binary_kind(kind: FieldKind) -> bool {
    match kind {
        FieldKind::Account
        | FieldKind::Int
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Ulid
        | FieldKind::Unit => true,
        FieldKind::Relation { key_kind, .. } => supports_storage_key_binary_kind(*key_kind),
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => {
            supports_storage_key_binary_kind(**key_kind)
        }
        _ => false,
    }
}

/// Decode one strong-relation field payload directly into target storage keys.
///
/// This keeps delete validation and reverse-index maintenance on structural
/// key forms without first rebuilding a runtime `Value` or `Value::List`.
#[cfg(test)]
pub(in crate::db) fn decode_relation_target_storage_keys_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Vec<StorageKey>, FieldDecodeError> {
    crate::db::data::structural_field::storage_key::decode::decode_relation_target_storage_keys_binary_bytes(
        raw_bytes, kind,
    )
}

/// Decode one accepted strong-relation field payload directly into target
/// storage keys.
pub(in crate::db) fn decode_accepted_relation_target_storage_keys_bytes(
    raw_bytes: &[u8],
    kind: &PersistedFieldKind,
) -> Result<Vec<StorageKey>, FieldDecodeError> {
    crate::db::data::structural_field::storage_key::decode::decode_accepted_relation_target_storage_keys_binary_bytes(
        raw_bytes, kind,
    )
}

/// Decode one storage-key-compatible field payload directly into its canonical
/// `StorageKey` form.
pub(in crate::db) fn decode_storage_key_field_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<StorageKey, FieldDecodeError> {
    crate::db::data::structural_field::storage_key::decode::decode_storage_key_field_binary_bytes(
        raw_bytes, kind,
    )
}

/// Decode one optional storage-key-compatible field payload directly into its
/// canonical `StorageKey` form.
pub(in crate::db) fn decode_optional_storage_key_field_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<StorageKey>, FieldDecodeError> {
    crate::db::data::structural_field::storage_key::decode::decode_optional_storage_key_field_binary_bytes(
        raw_bytes, kind,
    )
}

/// Encode one storage-key-compatible field payload directly into its
/// canonical Structural Binary v1 bytes.
pub(in crate::db) fn encode_storage_key_field_bytes(
    key: StorageKey,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    crate::db::data::structural_field::storage_key::encode::encode_storage_key_field_binary_bytes(
        key, kind, field_name,
    )
}
