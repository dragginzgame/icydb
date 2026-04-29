//! Module: data::structural_field
//! Responsibility: canonical persisted-field structural decode helpers.
//! Does not own: row layout planning, typed entity reconstruction, or query semantics.
//! Boundary: runtime paths use this module when they need one persisted field decoded without `E`.

mod binary;
mod composite;
mod encode;
mod leaf;
mod primitive;
mod scalar;
mod storage_key;
mod typed;
mod value_storage;

use crate::db::data::structural_field::binary::{
    push_binary_list_len, push_binary_map_len, walk_binary_list_items, walk_binary_map_entries,
};
use crate::{error::InternalError, model::field::FieldKind, value::Value};
use thiserror::Error as ThisError;

use composite::{decode_composite_field_by_kind_bytes, validate_composite_field_by_kind_bytes};
use leaf::{
    decode_date_field_by_kind_bytes as decode_structural_date_field_by_kind_bytes,
    decode_decimal_field_by_kind_bytes as decode_structural_decimal_field_by_kind_bytes,
    decode_duration_field_by_kind_bytes as decode_structural_duration_field_by_kind_bytes,
    decode_int_big_field_by_kind_bytes as decode_structural_int_big_field_by_kind_bytes,
    decode_leaf_field_by_kind_bytes,
    decode_uint_big_field_by_kind_bytes as decode_structural_uint_big_field_by_kind_bytes,
    encode_date_field_by_kind_bytes as encode_structural_date_field_by_kind_bytes,
    encode_decimal_field_by_kind_bytes as encode_structural_decimal_field_by_kind_bytes,
    encode_duration_field_by_kind_bytes as encode_structural_duration_field_by_kind_bytes,
    encode_int_big_field_by_kind_bytes as encode_structural_int_big_field_by_kind_bytes,
    encode_uint_big_field_by_kind_bytes as encode_structural_uint_big_field_by_kind_bytes,
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

pub(in crate::db) use encode::encode_structural_field_by_kind_bytes;
pub(in crate::db) use storage_key::{
    decode_optional_storage_key_field_bytes, decode_relation_target_storage_keys_bytes,
    decode_storage_key_binary_value_bytes, decode_storage_key_field_bytes,
    encode_storage_key_binary_value_bytes, encode_storage_key_field_bytes,
    supports_storage_key_binary_kind, validate_storage_key_binary_value_bytes,
};
pub(in crate::db) use value_storage::{
    BoundedValueStorageScalar, ValueStorageView, decode_account,
    decode_bounded_structural_value_storage_scalar_bytes, decode_decimal, decode_enum, decode_int,
    decode_int128, decode_list_item, decode_map_entry, decode_nat, decode_nat128,
    decode_structural_value_storage_blob_bytes, decode_structural_value_storage_bool_bytes,
    decode_structural_value_storage_bytes, decode_structural_value_storage_date_bytes,
    decode_structural_value_storage_duration_bytes, decode_structural_value_storage_float32_bytes,
    decode_structural_value_storage_float64_bytes, decode_structural_value_storage_i64_bytes,
    decode_structural_value_storage_principal_bytes,
    decode_structural_value_storage_subaccount_bytes,
    decode_structural_value_storage_timestamp_bytes, decode_structural_value_storage_u64_bytes,
    decode_structural_value_storage_ulid_bytes, decode_structural_value_storage_unit_bytes,
    decode_text, encode_account, encode_decimal, encode_enum, encode_int, encode_int128,
    encode_list_item, encode_map_entry, encode_nat, encode_nat128, encode_owned_list_item,
    encode_owned_map_entry, encode_structural_value_storage_blob_bytes,
    encode_structural_value_storage_bool_bytes, encode_structural_value_storage_bytes,
    encode_structural_value_storage_date_bytes, encode_structural_value_storage_duration_bytes,
    encode_structural_value_storage_float32_bytes, encode_structural_value_storage_float64_bytes,
    encode_structural_value_storage_i64_bytes, encode_structural_value_storage_null_bytes,
    encode_structural_value_storage_principal_bytes,
    encode_structural_value_storage_subaccount_bytes,
    encode_structural_value_storage_timestamp_bytes, encode_structural_value_storage_u64_bytes,
    encode_structural_value_storage_ulid_bytes, encode_structural_value_storage_unit_bytes,
    encode_text, structural_value_storage_bytes_are_null, validate_structural_value_storage_bytes,
};

///
/// FieldDecodeError
///
/// FieldDecodeError captures one persisted-field structural decode
/// failure.
/// It keeps structural decode diagnostics local to the field boundary so row
/// and relation callers can map them into taxonomy-correct higher-level errors.
///

#[derive(Clone, Debug, ThisError)]
#[error("{message}")]
pub(in crate::db) struct FieldDecodeError {
    message: String,
}

impl FieldDecodeError {
    // Build one structural field-decode failure message.
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

// Carry owned list-item payload bytes while the generic structural walker
// splits one by-kind list or set payload into nested item slices.
type OwnedByKindListItems = Vec<Vec<u8>>;

// Carry owned map-entry payload bytes while the generic structural walker
// splits one by-kind map payload into nested key/value slices.
type OwnedByKindMapEntries = Vec<(Vec<u8>, Vec<u8>)>;

// Push one owned by-kind list or set item payload into the owned slice state.
//
// Safety:
// `context` must point to `OwnedByKindListItems`.
fn push_owned_by_kind_list_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<OwnedByKindListItems>() };
    state
        .try_reserve(1)
        .map_err(|_| FieldDecodeError::new("structural binary: list item allocation overflow"))?;
    state.push(item_bytes.to_vec());

    Ok(())
}

// Push one owned by-kind map entry payload into the owned slice state.
//
// Safety:
// `context` must point to `OwnedByKindMapEntries`.
fn push_owned_by_kind_map_entry(
    key_bytes: &[u8],
    value_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<OwnedByKindMapEntries>() };
    state
        .try_reserve(1)
        .map_err(|_| FieldDecodeError::new("structural binary: map entry allocation overflow"))?;
    state.push((key_bytes.to_vec(), value_bytes.to_vec()));

    Ok(())
}

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
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept list or set payload items"),
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
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept list or set payload items"),
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
) -> Result<Vec<Vec<u8>>, FieldDecodeError> {
    if !matches!(kind, FieldKind::List(_) | FieldKind::Set(_)) {
        return Err(FieldDecodeError::new(
            "field kind is not owned by the by-kind list/set framing lane",
        ));
    }

    let mut state = Vec::new();
    walk_binary_list_items(
        raw_bytes,
        "expected Structural Binary list for list/set field",
        "structural binary: trailing bytes after list/set field",
        (&raw mut state).cast(),
        push_owned_by_kind_list_item,
    )?;

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
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept map payload entries"),
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
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept map payload entries"),
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
        return Err(FieldDecodeError::new(
            "field kind is not owned by the by-kind map framing lane",
        ));
    }

    let mut state = Vec::new();
    walk_binary_map_entries(
        raw_bytes,
        "expected Structural Binary map for map field",
        "structural binary: trailing bytes after map field",
        (&raw mut state).cast(),
        push_owned_by_kind_map_entry,
    )?;

    Ok(state)
}

/// Encode one direct int128 leaf through the canonical scalar fast path.
pub(in crate::db) fn encode_int128_field_by_kind_bytes(
    value: crate::types::Int128,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_scalar_int128_field_by_kind_bytes(value, kind, field_name)
}

/// Decode one direct int128 leaf through the canonical scalar fast path.
pub(in crate::db) fn decode_int128_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<crate::types::Int128>, FieldDecodeError> {
    decode_scalar_int128_field_by_kind_bytes(raw_bytes, kind)
}

/// Encode one direct nat128 leaf through the canonical scalar fast path.
pub(in crate::db) fn encode_nat128_field_by_kind_bytes(
    value: crate::types::Nat128,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_scalar_nat128_field_by_kind_bytes(value, kind, field_name)
}

/// Decode one direct nat128 leaf through the canonical scalar fast path.
pub(in crate::db) fn decode_nat128_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<crate::types::Nat128>, FieldDecodeError> {
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

/// Encode one direct signed-bigint leaf through the canonical structural leaf
/// lane.
pub(in crate::db) fn encode_int_big_field_by_kind_bytes(
    value: &crate::types::Int,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_structural_int_big_field_by_kind_bytes(value, kind, field_name)
}

/// Decode one direct signed-bigint leaf through the canonical structural leaf
/// lane.
pub(in crate::db) fn decode_int_big_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<crate::types::Int>, FieldDecodeError> {
    decode_structural_int_big_field_by_kind_bytes(raw_bytes, kind)
}

/// Encode one direct unsigned-bigint leaf through the canonical structural
/// leaf lane.
pub(in crate::db) fn encode_uint_big_field_by_kind_bytes(
    value: &crate::types::Nat,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    encode_structural_uint_big_field_by_kind_bytes(value, kind, field_name)
}

/// Decode one direct unsigned-bigint leaf through the canonical structural
/// leaf lane.
pub(in crate::db) fn decode_uint_big_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<crate::types::Nat>, FieldDecodeError> {
    decode_structural_uint_big_field_by_kind_bytes(raw_bytes, kind)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        decode_relation_target_storage_keys_bytes, decode_structural_field_by_kind_bytes,
        decode_structural_value_storage_bytes, encode_storage_key_binary_value_bytes,
        encode_structural_field_by_kind_bytes, encode_structural_value_storage_bytes,
        validate_structural_field_by_kind_bytes, validate_structural_value_storage_bytes,
    };
    use crate::{
        db::data::structural_field::binary::{
            push_binary_bytes, push_binary_list_len, push_binary_text, push_binary_uint64,
        },
        model::field::{FieldKind, RelationStrength},
        types::{
            Account, Decimal, EntityTag, Float32, Float64, Int128, Nat128, Principal, Subaccount,
            Ulid,
        },
        value::{StorageKey, Value, ValueEnum},
    };

    static RELATION_ULID_KEY_KIND: FieldKind = FieldKind::Ulid;
    static STRONG_RELATION_KIND: FieldKind = FieldKind::Relation {
        target_path: "RelationTargetEntity",
        target_entity_name: "RelationTargetEntity",
        target_entity_tag: EntityTag::new(7),
        target_store_path: "RelationTargetStore",
        key_kind: &RELATION_ULID_KEY_KIND,
        strength: RelationStrength::Strong,
    };
    static STRONG_RELATION_LIST_KIND: FieldKind = FieldKind::List(&STRONG_RELATION_KIND);

    #[test]
    fn relation_target_storage_key_decode_handles_single_ulid_and_null() {
        let target = Ulid::from_u128(7);
        let target_bytes =
            encode_storage_key_binary_value_bytes(STRONG_RELATION_KIND, &Value::Ulid(target), "id")
                .expect("storage-key relation bytes should encode")
                .expect("relation kind should use storage-key binary lane");
        let null_bytes =
            encode_storage_key_binary_value_bytes(STRONG_RELATION_KIND, &Value::Null, "id")
                .expect("null relation bytes should encode")
                .expect("relation kind should use storage-key binary lane");

        let decoded =
            decode_relation_target_storage_keys_bytes(&target_bytes, STRONG_RELATION_KIND)
                .expect("single relation should decode");
        let decoded_null =
            decode_relation_target_storage_keys_bytes(&null_bytes, STRONG_RELATION_KIND)
                .expect("null relation should decode");

        assert_eq!(decoded, vec![StorageKey::Ulid(target)]);
        assert!(
            decoded_null.is_empty(),
            "null relation should yield no targets"
        );
    }

    #[test]
    fn relation_target_storage_key_decode_handles_list_and_skips_null_items() {
        let left = Ulid::from_u128(8);
        let right = Ulid::from_u128(9);
        let bytes = encode_storage_key_binary_value_bytes(
            STRONG_RELATION_LIST_KIND,
            &Value::List(vec![Value::Ulid(left), Value::Null, Value::Ulid(right)]),
            "ids",
        )
        .expect("relation list bytes should encode")
        .expect("relation list should use storage-key binary lane");

        let decoded = decode_relation_target_storage_keys_bytes(&bytes, STRONG_RELATION_LIST_KIND)
            .expect("relation list should decode");

        assert_eq!(
            decoded,
            vec![StorageKey::Ulid(left), StorageKey::Ulid(right)],
        );
    }

    #[test]
    fn structural_field_decode_list_bytes_preserves_scalar_items() {
        let bytes = encode_structural_field_by_kind_bytes(
            FieldKind::List(&FieldKind::Text { max_len: None }),
            &Value::List(vec![
                Value::Text("left".to_string()),
                Value::Text("right".to_string()),
            ]),
            "items",
        )
        .expect("list bytes should encode");

        let decoded = decode_structural_field_by_kind_bytes(
            &bytes,
            FieldKind::List(&FieldKind::Text { max_len: None }),
        )
        .expect("scalar list field should decode");

        assert_eq!(
            decoded,
            Value::List(vec![
                Value::Text("left".to_string()),
                Value::Text("right".to_string()),
            ]),
        );
    }

    #[test]
    fn structural_field_decode_map_bytes_preserves_scalar_entries() {
        let bytes = encode_structural_field_by_kind_bytes(
            FieldKind::Map {
                key: &FieldKind::Text { max_len: None },
                value: &FieldKind::Uint,
            },
            &Value::Map(vec![
                (Value::Text("alpha".to_string()), Value::Uint(1)),
                (Value::Text("beta".to_string()), Value::Uint(2)),
            ]),
            "entries",
        )
        .expect("map bytes should encode");

        let decoded = decode_structural_field_by_kind_bytes(
            &bytes,
            FieldKind::Map {
                key: &FieldKind::Text { max_len: None },
                value: &FieldKind::Uint,
            },
        )
        .expect("scalar map field should decode");

        assert_eq!(
            decoded,
            Value::Map(vec![
                (Value::Text("alpha".to_string()), Value::Uint(1)),
                (Value::Text("beta".to_string()), Value::Uint(2)),
            ]),
        );
    }

    #[test]
    fn structural_field_decode_float_scalars_uses_binary_lane() {
        let float32 = Value::Float32(Float32::try_new(3.5).expect("finite f32"));
        let float64 = Value::Float64(Float64::try_new(9.25).expect("finite f64"));

        let float32_bytes =
            encode_structural_field_by_kind_bytes(FieldKind::Float32, &float32, "ratio")
                .expect("float32 bytes should encode");
        let float64_bytes =
            encode_structural_field_by_kind_bytes(FieldKind::Float64, &float64, "score")
                .expect("float64 bytes should encode");

        let decoded_float32 =
            decode_structural_field_by_kind_bytes(&float32_bytes, FieldKind::Float32)
                .expect("float32 payload should decode");
        let decoded_float64 =
            decode_structural_field_by_kind_bytes(&float64_bytes, FieldKind::Float64)
                .expect("float64 payload should decode");

        assert_eq!(decoded_float32, float32);
        assert_eq!(decoded_float64, float64);
    }

    #[test]
    fn structural_field_decode_value_storage_handles_enum_payload() {
        let value = Value::Enum(
            ValueEnum::new("Active", Some("Status")).with_payload(Value::Map(vec![(
                Value::Text("count".into()),
                Value::Uint(7),
            )])),
        );
        let bytes =
            encode_structural_value_storage_bytes(&value).expect("value bytes should encode");

        let decoded = decode_structural_value_storage_bytes(&bytes)
            .expect("value enum payload should decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn structural_field_decode_typed_wrappers_preserves_payloads() {
        let account = Account::from_parts(Principal::dummy(7), Some(Subaccount::from([7_u8; 32])));
        let decimal = Decimal::new(1234, 2);

        let account_bytes = encode_structural_field_by_kind_bytes(
            FieldKind::Account,
            &Value::Account(account),
            "account",
        )
        .expect("account bytes should encode");
        let decimal_bytes = encode_structural_field_by_kind_bytes(
            FieldKind::Decimal { scale: 2 },
            &Value::Decimal(decimal),
            "amount",
        )
        .expect("decimal bytes should encode");

        let decoded_account =
            decode_structural_field_by_kind_bytes(&account_bytes, FieldKind::Account)
                .expect("account payload should decode");
        let decoded_decimal =
            decode_structural_field_by_kind_bytes(&decimal_bytes, FieldKind::Decimal { scale: 2 })
                .expect("decimal payload should decode");

        assert_eq!(decoded_account, Value::Account(account));
        assert_eq!(decoded_decimal, Value::Decimal(decimal));
    }

    #[test]
    fn structural_field_decode_value_storage_roundtrips_nested_bytes_like_variants() {
        let nested = Value::from_map(vec![
            (
                Value::Text("blob".to_string()),
                Value::Blob(vec![0x10, 0x20, 0x30]),
            ),
            (
                Value::Text("i128".to_string()),
                Value::Int128(Int128::from(-123i128)),
            ),
            (
                Value::Text("u128".to_string()),
                Value::Uint128(Nat128::from(456u128)),
            ),
            (
                Value::Text("list".to_string()),
                Value::List(vec![
                    Value::Blob(vec![0xAA, 0xBB]),
                    Value::Int128(Int128::from(7i128)),
                    Value::Uint128(Nat128::from(8u128)),
                ]),
            ),
            (
                Value::Text("enum".to_string()),
                Value::Enum(
                    ValueEnum::new("Loaded", Some("tests::StructuredPayload"))
                        .with_payload(Value::Blob(vec![0xCC, 0xDD])),
                ),
            ),
        ])
        .expect("nested value payload should normalize");
        let bytes = encode_structural_value_storage_bytes(&nested)
            .expect("nested value payload should serialize");

        let decoded = decode_structural_value_storage_bytes(&bytes)
            .expect("nested value payload should decode through value storage");

        assert_eq!(decoded, nested);
    }

    #[test]
    fn structural_field_validate_matches_decode_for_malformed_leaf_payloads() {
        let mut bytes = Vec::new();
        push_binary_list_len(&mut bytes, 2);
        push_binary_bytes(&mut bytes, &1_i128.to_be_bytes());
        push_binary_uint64(&mut bytes, u64::from(Decimal::max_supported_scale() + 1));

        let decode = decode_structural_field_by_kind_bytes(
            bytes.as_slice(),
            FieldKind::Decimal { scale: 2 },
        );
        let validate = validate_structural_field_by_kind_bytes(
            bytes.as_slice(),
            FieldKind::Decimal { scale: 2 },
        );

        assert!(
            decode.is_err(),
            "malformed decimal payload must fail decode"
        );
        assert!(
            validate.is_err(),
            "malformed decimal payload must fail validate"
        );
    }

    #[test]
    fn structural_field_validate_matches_decode_for_malformed_storage_key_payloads() {
        let mut bytes = Vec::new();
        push_binary_text(&mut bytes, "aaaaa-aa");

        let decode = decode_structural_field_by_kind_bytes(bytes.as_slice(), FieldKind::Principal);
        let validate =
            validate_structural_field_by_kind_bytes(bytes.as_slice(), FieldKind::Principal);

        assert!(decode.is_err(), "principal text payload must fail decode");
        assert!(
            validate.is_err(),
            "principal text payload must fail validate"
        );
    }

    #[test]
    fn structural_field_validate_matches_decode_for_malformed_composite_payloads() {
        let mut bytes = encode_structural_field_by_kind_bytes(
            FieldKind::List(&FieldKind::Text { max_len: None }),
            &Value::List(vec![Value::Text("left".to_string())]),
            "items",
        )
        .expect("list bytes should encode");
        bytes.push(0x00);

        let decode = decode_structural_field_by_kind_bytes(
            bytes.as_slice(),
            FieldKind::List(&FieldKind::Text { max_len: None }),
        );
        let validate = validate_structural_field_by_kind_bytes(
            bytes.as_slice(),
            FieldKind::List(&FieldKind::Text { max_len: None }),
        );

        assert!(decode.is_err(), "trailing list bytes must fail decode");
        assert!(validate.is_err(), "trailing list bytes must fail validate");
    }

    #[test]
    fn structural_value_storage_validate_matches_decode_for_malformed_payloads() {
        let bytes = [0xF6];

        let decode = decode_structural_value_storage_bytes(&bytes);
        let validate = validate_structural_value_storage_bytes(&bytes);

        assert!(decode.is_err(), "unknown value tag must fail decode");
        assert!(validate.is_err(), "unknown value tag must fail validate");
    }
}
