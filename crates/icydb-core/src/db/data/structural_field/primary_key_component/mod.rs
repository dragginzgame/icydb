//! Module: data::structural_field::primary_key_component
//! Responsibility: direct persisted-field decoding into canonical primary-key component forms.
//! Does not own: generic runtime `Value` decode, composite `ByKind` recursion, or low-level structural binary walking.
//! Boundary: relation and index integrity paths call into this module when they need keys without rebuilding `Value`.

mod decode;
mod encode;
mod scalar;
#[cfg(test)]
mod tests;

use crate::{
    db::key_taxonomy::PrimaryKeyComponent,
    db::{data::structural_field::FieldDecodeError, schema::AcceptedFieldKind},
    model::field::FieldKind,
};

pub(in crate::db) use crate::db::data::structural_field::primary_key_component::{
    decode::{
        decode_primary_key_component_binary_value_bytes,
        validate_primary_key_component_binary_value_bytes,
    },
    encode::encode_primary_key_component_binary_value_bytes,
};

/// Return whether this field kind is owned by the Structural Binary v1
/// primary-key-component lane.
pub(in crate::db) const fn supports_primary_key_component_binary_kind(kind: FieldKind) -> bool {
    match kind {
        FieldKind::Account
        | FieldKind::Int8
        | FieldKind::Int16
        | FieldKind::Int32
        | FieldKind::Int64
        | FieldKind::Int128
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Timestamp
        | FieldKind::Nat8
        | FieldKind::Nat16
        | FieldKind::Nat32
        | FieldKind::Nat64
        | FieldKind::Nat128
        | FieldKind::Ulid
        | FieldKind::Unit => true,
        FieldKind::Relation { key_kind, .. } => {
            supports_primary_key_component_binary_kind(*key_kind)
        }
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => {
            supports_primary_key_component_binary_kind(**key_kind)
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
