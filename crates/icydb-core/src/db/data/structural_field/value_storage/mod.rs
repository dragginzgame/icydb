//! Module: data::structural_field::value_storage
//! Responsibility: owner-local binary `Value` envelope encode and decode.
//! Does not own: top-level `ByKind` dispatch, typed wrapper payload definitions, or storage-key policy.
//! Boundary: `FieldStorageDecode::CatalogValue` routes through this module without widening authority over sibling structural lanes.

mod canonical;
mod decode;
mod encode;
mod primitives;
mod skip;
mod tags;
#[cfg(test)]
mod tests;
mod walk;

use crate::{db::data::structural_field::FieldDecodeError, value::Value};

const MAX_VALUE_STORAGE_DECODE_DEPTH: usize = 64;

const fn next_value_storage_decode_depth(depth: usize) -> Result<usize, FieldDecodeError> {
    if depth >= MAX_VALUE_STORAGE_DECODE_DEPTH {
        return Err(FieldDecodeError::new());
    }

    Ok(depth.saturating_add(1))
}

fn reserve_one_value_storage_item<T>(items: &mut Vec<T>) -> Result<(), FieldDecodeError> {
    items.try_reserve(1).map_err(|_| FieldDecodeError::new())
}

pub(in crate::db) use canonical::{
    decode_canonical_value_storage_bytes, encode_canonical_value_storage_bytes,
};
pub(in crate::db) use decode::{
    ValueStorageView, decode_account, decode_decimal, decode_int, decode_int128, decode_nat,
    decode_nat128, decode_structural_value_storage_blob_bytes,
    decode_structural_value_storage_bool_bytes, decode_structural_value_storage_bytes,
    decode_structural_value_storage_date_bytes, decode_structural_value_storage_duration_bytes,
    decode_structural_value_storage_float32_bytes, decode_structural_value_storage_float64_bytes,
    decode_structural_value_storage_i64_bytes, decode_structural_value_storage_principal_bytes,
    decode_structural_value_storage_subaccount_bytes,
    decode_structural_value_storage_timestamp_bytes, decode_structural_value_storage_u64_bytes,
    decode_structural_value_storage_ulid_bytes, decode_structural_value_storage_unit_bytes,
    decode_value_storage_list_item_slices, decode_value_storage_map_entry_slices,
    decode_value_storage_text, validate_structural_value_storage_bytes,
    value_storage_bytes_are_null,
};
pub(in crate::db) use encode::{
    encode_account, encode_decimal, encode_int, encode_int128, encode_nat, encode_nat128,
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
    encode_value_storage_text,
};

// Normalize decoded map entries in place when they satisfy the runtime map
// invariants, but preserve the original decoded order when validation rejects
// the shape. This keeps current semantics without cloning the whole entry list.
pub(super) fn normalize_map_entries_or_preserve(mut entries: Vec<(Value, Value)>) -> Value {
    if Value::validate_map_entries(&entries).is_err() {
        return Value::Map(entries);
    }

    Value::sort_map_entries_in_place(entries.as_mut_slice());

    for i in 1..entries.len() {
        let (left_key, _) = &entries[i - 1];
        let (right_key, _) = &entries[i];
        if Value::canonical_cmp_key(left_key, right_key) == std::cmp::Ordering::Equal {
            return Value::Map(entries);
        }
    }

    Value::Map(entries)
}
