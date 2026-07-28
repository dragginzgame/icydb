//! Module: data::structural_field::primary_key_component
//! Responsibility: direct persisted-field decoding into canonical primary-key component forms.
//! Does not own: generic runtime `Value` decode, composite `ByKind` recursion, or low-level structural binary walking.
//! Boundary: relation and index integrity paths call into this module when they need keys without rebuilding `Value`.

mod decode;
mod encode;
mod scalar;
#[cfg(test)]
mod tests;

use crate::db::{
    data::structural_field::FieldDecodeError, key_taxonomy::PrimaryKeyComponent,
    schema::AcceptedFieldKind,
};

#[cfg(test)]
pub(in crate::db) use crate::db::data::structural_field::primary_key_component::decode::validate_primary_key_component_binary_value_bytes;
pub(in crate::db) use crate::db::data::structural_field::primary_key_component::{
    decode::decode_primary_key_component_binary_value_bytes,
    encode::encode_primary_key_component_binary_value_bytes,
};

/// Return whether this field kind is owned by the Structural Binary v1
/// primary-key-component lane.
pub(in crate::db) fn supports_primary_key_component_binary_kind(kind: &AcceptedFieldKind) -> bool {
    match kind {
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit => true,
        AcceptedFieldKind::Relation { key_kind, .. } => {
            supports_primary_key_component_binary_kind(key_kind)
        }
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner)
            if matches!(inner.as_ref(), AcceptedFieldKind::Relation { .. }) =>
        {
            supports_primary_key_component_binary_kind(inner)
        }
        _ => false,
    }
}

/// Decode one accepted relation field payload directly into target
/// primary-key components.
pub(in crate::db) fn decode_accepted_relation_target_primary_key_components_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<Vec<PrimaryKeyComponent>, FieldDecodeError> {
    crate::db::data::structural_field::primary_key_component::decode::decode_accepted_relation_target_primary_key_components_binary_bytes(
        raw_bytes, kind,
    )
}
