//! Module: data::structural_field
//! Responsibility: canonical persisted-field structural decode helpers.
//! Does not own: row layout planning, typed entity reconstruction, or query semantics.
//! Boundary: runtime paths use this module when they need one persisted field decoded without `E`.

mod accepted;
mod binary;
mod composite;
mod encode;
mod leaf;
mod primary_key_component;
mod primitive;
mod scalar;
#[cfg(test)]
mod tests;
mod typed;
mod value_storage;

use crate::db::data::structural_field::binary::{
    push_binary_list_len, push_binary_map_len, walk_binary_list_items, walk_binary_map_entries,
};
use crate::{error::InternalError, model::field::FieldKind, value::Value};

use composite::{decode_composite_field_by_kind_bytes, validate_composite_field_by_kind_bytes};
use leaf::{
    decode_date_field_by_kind_bytes as decode_structural_date_field_by_kind_bytes,
    decode_decimal_field_by_kind_bytes as decode_structural_decimal_field_by_kind_bytes,
    decode_duration_field_by_kind_bytes as decode_structural_duration_field_by_kind_bytes,
    decode_int_big_field_by_kind_bytes as decode_structural_int_big_field_by_kind_bytes,
    decode_leaf_field_by_kind_bytes,
    decode_nat_big_field_by_kind_bytes as decode_structural_nat_big_field_by_kind_bytes,
    encode_date_field_by_kind_bytes as encode_structural_date_field_by_kind_bytes,
    encode_decimal_field_by_kind_bytes as encode_structural_decimal_field_by_kind_bytes,
    encode_duration_field_by_kind_bytes as encode_structural_duration_field_by_kind_bytes,
    encode_int_big_field_by_kind_bytes as encode_structural_int_big_field_by_kind_bytes,
    encode_nat_big_field_by_kind_bytes as encode_structural_nat_big_field_by_kind_bytes,
};
use scalar::{
    decode_blob_fast_path_binary_bytes, decode_bool_fast_path_binary_bytes,
    decode_float32_fast_path_binary_bytes, decode_float64_fast_path_binary_bytes,
    decode_int128_fast_path_binary_bytes as decode_scalar_int128_field_by_kind_bytes,
    decode_nat128_fast_path_binary_bytes as decode_scalar_nat128_field_by_kind_bytes,
    decode_scalar_fast_path_bytes, decode_text_fast_path_binary_bytes,
    encode_blob_fast_path_binary_bytes, encode_bool_fast_path_binary_bytes,
    encode_float32_fast_path_binary_bytes, encode_float64_fast_path_binary_bytes,
    encode_int128_fast_path_binary_bytes as encode_scalar_int128_field_by_kind_bytes,
    encode_nat128_fast_path_binary_bytes as encode_scalar_nat128_field_by_kind_bytes,
    encode_text_fast_path_binary_bytes,
};

pub(in crate::db) use accepted::{
    accepted_kind_supports_primary_key_component_binary,
    decode_structural_field_by_accepted_kind_bytes, encode_structural_field_by_accepted_kind_bytes,
    validate_structural_field_by_accepted_kind_bytes,
};
pub(in crate::db) use encode::encode_structural_field_by_kind_bytes;
#[cfg(test)]
pub(in crate::db) use primary_key_component::decode_relation_target_primary_key_components_bytes;
pub(in crate::db) use primary_key_component::{
    decode_accepted_relation_target_primary_key_components_bytes,
    decode_optional_primary_key_component_field_bytes,
    decode_primary_key_component_binary_value_bytes,
    encode_primary_key_component_binary_value_bytes, encode_primary_key_component_field_bytes,
    supports_primary_key_component_binary_kind,
};
pub(in crate::db) use value_storage::{
    ValueStorageView, decode_account, decode_canonical_value_storage_bytes, decode_decimal,
    decode_int, decode_int128, decode_nat, decode_nat128,
    decode_structural_value_storage_blob_bytes, decode_structural_value_storage_bool_bytes,
    decode_structural_value_storage_bytes, decode_structural_value_storage_date_bytes,
    decode_structural_value_storage_duration_bytes, decode_structural_value_storage_float32_bytes,
    decode_structural_value_storage_float64_bytes, decode_structural_value_storage_i64_bytes,
    decode_structural_value_storage_principal_bytes,
    decode_structural_value_storage_subaccount_bytes,
    decode_structural_value_storage_timestamp_bytes, decode_structural_value_storage_u64_bytes,
    decode_structural_value_storage_ulid_bytes, decode_structural_value_storage_unit_bytes,
    decode_value_storage_list_item_slices, decode_value_storage_map_entry_slices,
    decode_value_storage_text, encode_account, encode_canonical_value_storage_bytes,
    encode_decimal, encode_int, encode_int128, encode_nat, encode_nat128,
    encode_structural_value_storage_blob_bytes, encode_structural_value_storage_bool_bytes,
    encode_structural_value_storage_bytes, encode_structural_value_storage_date_bytes,
    encode_structural_value_storage_duration_bytes, encode_structural_value_storage_float32_bytes,
    encode_structural_value_storage_float64_bytes, encode_structural_value_storage_i64_bytes,
    encode_structural_value_storage_null_bytes, encode_structural_value_storage_principal_bytes,
    encode_structural_value_storage_subaccount_bytes,
    encode_structural_value_storage_timestamp_bytes, encode_structural_value_storage_u64_bytes,
    encode_structural_value_storage_ulid_bytes, encode_structural_value_storage_unit_bytes,
    encode_value_storage_list_item_slices, encode_value_storage_map_entry_slices,
    encode_value_storage_owned_list_items, encode_value_storage_owned_map_entries,
    encode_value_storage_text, validate_structural_value_storage_bytes,
    value_storage_bytes_are_null,
};

///
/// FieldDecodeError
///
/// FieldDecodeError captures one persisted-field structural decode
/// failure.
/// It keeps structural decode diagnostics local to the field boundary so row
/// and relation callers can map them into taxonomy-correct higher-level errors.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct FieldDecodeError;

impl FieldDecodeError {
    // Build one compact structural field-decode failure marker. Detailed
    // corruption taxonomy is added by row/store boundaries.
    const fn new() -> Self {
        Self
    }
}

// Carry owned list-item payload bytes while the generic structural walker
// splits one by-kind list or set payload into nested item slices.
type OwnedByKindListItems = Vec<Vec<u8>>;

// Carry owned map-entry payload bytes while the generic structural walker
// splits one by-kind map payload into nested key/value slices.
type OwnedByKindMapEntries = Vec<(Vec<u8>, Vec<u8>)>;

/// Decode one encoded persisted field payload strictly by semantic field kind.
pub(in crate::db) fn decode_structural_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Value, FieldDecodeError> {
    // Keep byte-backed `ByKind` leaves off the generic `ValueWire` bridge
    // whenever their persisted shape is fixed or already owned by the leaf
    // type.
    if let Some(value) = decode_scalar_fast_path_bytes(raw_bytes, kind)? {
        return Ok(value);
    }

    // Keep the root entrypoint as a thin lane router: scalar fast path above,
    // then non-recursive leaves, then the recursive composite authority.
    if let Some(value) = decode_leaf_field_by_kind_bytes(raw_bytes, kind)? {
        return Ok(value);
    }

    decode_composite_field_by_kind_bytes(raw_bytes, kind)
}

/// Validate one encoded persisted field payload strictly by semantic field
/// kind without eagerly building the final runtime `Value`.
pub(in crate::db) fn validate_structural_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<(), FieldDecodeError> {
    // Keep the validate-only entrypoint aligned with the existing decode lane
    // ordering so row-open validation and later materialization still share one
    // field-contract authority.
    if decode_scalar_fast_path_bytes(raw_bytes, kind)?.is_some() {
        return Ok(());
    }

    if decode_leaf_field_by_kind_bytes(raw_bytes, kind)?.is_some() {
        return Ok(());
    }

    validate_composite_field_by_kind_bytes(raw_bytes, kind)
}

/// Encode one direct bool leaf through the canonical scalar fast path.
pub(in crate::db) fn encode_bool_field_by_kind_bytes(
    value: bool,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_bool_fast_path_binary_bytes(value, kind, field_name)
}

/// Decode one direct bool leaf through the canonical scalar fast path.
pub(in crate::db) fn decode_bool_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<bool>, FieldDecodeError> {
    decode_bool_fast_path_binary_bytes(raw_bytes, kind)
}

/// Encode one direct text leaf through the canonical scalar fast path.
pub(in crate::db) fn encode_text_field_by_kind_bytes(
    value: &str,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_text_fast_path_binary_bytes(value, kind, field_name)
}

/// Decode one direct text leaf through the canonical scalar fast path.
pub(in crate::db) fn decode_text_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<String>, FieldDecodeError> {
    decode_text_fast_path_binary_bytes(raw_bytes, kind)
}

/// Encode one direct blob leaf through the canonical scalar fast path.
pub(in crate::db) fn encode_blob_field_by_kind_bytes(
    value: &crate::types::Blob,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_blob_fast_path_binary_bytes(value, kind, field_name)
}

/// Decode one direct blob leaf through the canonical scalar fast path.
pub(in crate::db) fn decode_blob_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<crate::types::Blob>, FieldDecodeError> {
    decode_blob_fast_path_binary_bytes(raw_bytes, kind)
}

/// Encode one direct float32 leaf through the canonical scalar fast path.
pub(in crate::db) fn encode_float32_field_by_kind_bytes(
    value: crate::types::Float32,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_float32_fast_path_binary_bytes(value, kind, field_name)
}

/// Decode one direct float32 leaf through the canonical scalar fast path.
pub(in crate::db) fn decode_float32_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<crate::types::Float32>, FieldDecodeError> {
    decode_float32_fast_path_binary_bytes(raw_bytes, kind)
}

/// Encode one direct float64 leaf through the canonical scalar fast path.
pub(in crate::db) fn encode_float64_field_by_kind_bytes(
    value: crate::types::Float64,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_float64_fast_path_binary_bytes(value, kind, field_name)
}

/// Decode one direct float64 leaf through the canonical scalar fast path.
pub(in crate::db) fn decode_float64_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<crate::types::Float64>, FieldDecodeError> {
    decode_float64_fast_path_binary_bytes(raw_bytes, kind)
}

/// Encode one direct list or set by-kind payload from already encoded nested
/// item payload slices.
#[cfg(test)]
pub(in crate::db) fn encode_list_field_items(
    items: &[&[u8]],
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::List(_) | FieldKind::Set(_)) {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    }

    let mut encoded = Vec::new();
    push_binary_list_len(&mut encoded, items.len());
    for item in items {
        encoded.extend_from_slice(item);
    }

    Ok(encoded)
}

/// Encode one direct list or set by-kind payload from owned nested item payload
/// buffers without staging a second borrowed-slice vector.
pub(in crate::db) fn encode_list_field_owned_items(
    items: &[Vec<u8>],
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::List(_) | FieldKind::Set(_)) {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    }

    let mut encoded = Vec::new();
    push_binary_list_len(&mut encoded, items.len());
    for item in items {
        encoded.extend_from_slice(item);
    }

    Ok(encoded)
}

/// Decode one direct list or set by-kind payload into owned nested item bytes.
pub(in crate::db) fn decode_list_field_items(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<OwnedByKindListItems, FieldDecodeError> {
    if !matches!(kind, FieldKind::List(_) | FieldKind::Set(_)) {
        return Err(FieldDecodeError::new());
    }

    let mut state = Vec::new();
    walk_binary_list_items(raw_bytes, &mut |item_bytes| {
        state.try_reserve(1).map_err(|_| FieldDecodeError::new())?;
        state.push(item_bytes.to_vec());

        Ok(())
    })?;

    Ok(state)
}

/// Encode one direct map by-kind payload from already encoded nested key/value
/// payload slices.
#[cfg(test)]
pub(in crate::db) fn encode_map_field_entries(
    entries: &[(&[u8], &[u8])],
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::Map { .. }) {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    }

    let mut encoded = Vec::new();
    push_binary_map_len(&mut encoded, entries.len());
    for (key_bytes, value_bytes) in entries {
        encoded.extend_from_slice(key_bytes);
        encoded.extend_from_slice(value_bytes);
    }

    Ok(encoded)
}

/// Encode one direct map by-kind payload from owned nested key/value payload
/// buffers without staging a second borrowed-slice vector.
pub(in crate::db) fn encode_map_field_owned_entries(
    entries: &[(Vec<u8>, Vec<u8>)],
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if !matches!(kind, FieldKind::Map { .. }) {
        return Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        ));
    }

    let mut encoded = Vec::new();
    push_binary_map_len(&mut encoded, entries.len());
    for (key_bytes, value_bytes) in entries {
        encoded.extend_from_slice(key_bytes);
        encoded.extend_from_slice(value_bytes);
    }

    Ok(encoded)
}

/// Decode one direct map by-kind payload into owned nested key/value bytes.
pub(in crate::db) fn decode_map_field_entries(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<OwnedByKindMapEntries, FieldDecodeError> {
    if !matches!(kind, FieldKind::Map { .. }) {
        return Err(FieldDecodeError::new());
    }

    let mut state = Vec::new();
    walk_binary_map_entries(raw_bytes, &mut |key_bytes, value_bytes| {
        state.try_reserve(1).map_err(|_| FieldDecodeError::new())?;
        state.push((key_bytes.to_vec(), value_bytes.to_vec()));

        Ok(())
    })?;

    Ok(state)
}

/// Encode one direct int128 leaf through the canonical scalar fast path.
pub(in crate::db) fn encode_int128_field_by_kind_bytes(
    value: i128,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_scalar_int128_field_by_kind_bytes(value, kind, field_name)
}

/// Decode one direct int128 leaf through the canonical scalar fast path.
pub(in crate::db) fn decode_int128_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<i128>, FieldDecodeError> {
    decode_scalar_int128_field_by_kind_bytes(raw_bytes, kind)
}

/// Encode one direct nat128 leaf through the canonical scalar fast path.
pub(in crate::db) fn encode_nat128_field_by_kind_bytes(
    value: u128,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_scalar_nat128_field_by_kind_bytes(value, kind, field_name)
}

/// Decode one direct nat128 leaf through the canonical scalar fast path.
pub(in crate::db) fn decode_nat128_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<u128>, FieldDecodeError> {
    decode_scalar_nat128_field_by_kind_bytes(raw_bytes, kind)
}

/// Encode one direct date leaf through the canonical structural leaf lane.
pub(in crate::db) fn encode_date_field_by_kind_bytes(
    value: crate::types::Date,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_structural_date_field_by_kind_bytes(value, kind, field_name)
}

/// Decode one direct date leaf through the canonical structural leaf lane.
pub(in crate::db) fn decode_date_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<crate::types::Date>, FieldDecodeError> {
    decode_structural_date_field_by_kind_bytes(raw_bytes, kind)
}

/// Encode one direct decimal leaf through the canonical structural leaf lane.
pub(in crate::db) fn encode_decimal_field_by_kind_bytes(
    value: crate::types::Decimal,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_structural_decimal_field_by_kind_bytes(value, kind, field_name)
}

/// Decode one direct decimal leaf through the canonical structural leaf lane.
pub(in crate::db) fn decode_decimal_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<crate::types::Decimal>, FieldDecodeError> {
    decode_structural_decimal_field_by_kind_bytes(raw_bytes, kind)
}

/// Encode one direct duration leaf through the canonical structural leaf lane.
pub(in crate::db) fn encode_duration_field_by_kind_bytes(
    value: crate::types::Duration,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_structural_duration_field_by_kind_bytes(value, kind, field_name)
}

/// Decode one direct duration leaf through the canonical structural leaf lane.
pub(in crate::db) fn decode_duration_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<crate::types::Duration>, FieldDecodeError> {
    decode_structural_duration_field_by_kind_bytes(raw_bytes, kind)
}

/// Encode one direct `int_big` leaf through the canonical structural leaf lane.
pub(in crate::db) fn encode_int_big_field_by_kind_bytes(
    value: &crate::types::IntBig,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_structural_int_big_field_by_kind_bytes(value, kind, field_name)
}

/// Decode one direct `int_big` leaf through the canonical structural leaf lane.
pub(in crate::db) fn decode_int_big_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<crate::types::IntBig>, FieldDecodeError> {
    decode_structural_int_big_field_by_kind_bytes(raw_bytes, kind)
}

/// Encode one direct `nat_big` leaf through the canonical structural leaf lane.
pub(in crate::db) fn encode_nat_big_field_by_kind_bytes(
    value: &crate::types::NatBig,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_structural_nat_big_field_by_kind_bytes(value, kind, field_name)
}

/// Decode one direct `nat_big` leaf through the canonical structural leaf lane.
pub(in crate::db) fn decode_nat_big_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<crate::types::NatBig>, FieldDecodeError> {
    decode_structural_nat_big_field_by_kind_bytes(raw_bytes, kind)
}
