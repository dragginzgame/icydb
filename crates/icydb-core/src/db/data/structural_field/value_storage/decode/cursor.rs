use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            TAG_BYTES, TAG_FALSE, TAG_INT64, TAG_LIST, TAG_MAP, TAG_NULL, TAG_TEXT, TAG_TRUE,
            TAG_UINT64, TAG_UNIT,
        },
        value_storage::{
            decode::{
                ValueStorageSlice,
                scalar::{
                    decode_binary_blob_value_at, decode_binary_i64_value_at,
                    decode_binary_text_value_at, decode_binary_u64_value_at,
                },
                value::decode_value_storage_slice,
            },
            skip::skip_value_storage_binary_value,
            tags::{
                VALUE_BINARY_TAG_ACCOUNT, VALUE_BINARY_TAG_DATE, VALUE_BINARY_TAG_DECIMAL,
                VALUE_BINARY_TAG_DURATION, VALUE_BINARY_TAG_ENUM, VALUE_BINARY_TAG_FLOAT32,
                VALUE_BINARY_TAG_FLOAT64, VALUE_BINARY_TAG_INT_BIG, VALUE_BINARY_TAG_INT128,
                VALUE_BINARY_TAG_PRINCIPAL, VALUE_BINARY_TAG_SUBACCOUNT,
                VALUE_BINARY_TAG_TIMESTAMP, VALUE_BINARY_TAG_UINT_BIG, VALUE_BINARY_TAG_UINT128,
                VALUE_BINARY_TAG_ULID,
            },
            walk::{
                decode_value_storage_binary_list_items_single_pass,
                decode_value_storage_binary_map_entries_single_pass,
            },
        },
    },
    value::Value,
};

// Decode one nested value-storage payload from `offset` and return the cursor
// immediately after that payload. Collection variants use the single-pass
// decode helpers; local extension tags keep the existing skip-then-decode lane
// so their nested validation behavior stays narrow and familiar.
pub(super) fn decode_value_storage_binary_value_at(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<(Value, usize), FieldDecodeError> {
    let Some(&tag) = raw_bytes.get(offset) else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value payload",
        ));
    };

    match tag {
        TAG_NULL => decode_value_storage_tag_only_at(offset, Value::Null),
        TAG_UNIT => decode_value_storage_tag_only_at(offset, Value::Unit),
        TAG_FALSE => decode_value_storage_tag_only_at(offset, Value::Bool(false)),
        TAG_TRUE => decode_value_storage_tag_only_at(offset, Value::Bool(true)),
        TAG_INT64 => decode_binary_i64_value_at(raw_bytes, offset),
        TAG_UINT64 => decode_binary_u64_value_at(raw_bytes, offset),
        TAG_TEXT => decode_binary_text_value_at(raw_bytes, offset),
        TAG_BYTES => decode_binary_blob_value_at(raw_bytes, offset),
        TAG_LIST => {
            let (items, cursor) = decode_value_storage_binary_list_items_single_pass(
                raw_bytes,
                offset,
                "expected structural binary list for value list payload",
                None,
                decode_value_storage_binary_value_at,
            )?;

            Ok((Value::List(items), cursor))
        }
        TAG_MAP => {
            let (entries, cursor) = decode_value_storage_binary_map_entries_single_pass(
                raw_bytes,
                offset,
                "expected structural binary map for value map payload",
                None,
                decode_value_storage_binary_value_at,
            )?;
            let value = Value::from_map(entries)
                .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))?;

            Ok((value, cursor))
        }
        VALUE_BINARY_TAG_ACCOUNT
        | VALUE_BINARY_TAG_DATE
        | VALUE_BINARY_TAG_DECIMAL
        | VALUE_BINARY_TAG_DURATION
        | VALUE_BINARY_TAG_ENUM
        | VALUE_BINARY_TAG_FLOAT32
        | VALUE_BINARY_TAG_FLOAT64
        | VALUE_BINARY_TAG_INT128
        | VALUE_BINARY_TAG_INT_BIG
        | VALUE_BINARY_TAG_PRINCIPAL
        | VALUE_BINARY_TAG_SUBACCOUNT
        | VALUE_BINARY_TAG_TIMESTAMP
        | VALUE_BINARY_TAG_UINT128
        | VALUE_BINARY_TAG_UINT_BIG
        | VALUE_BINARY_TAG_ULID => {
            let cursor = skip_value_storage_binary_value(raw_bytes, offset)?;
            let slice = ValueStorageSlice::from_skip_bounded_unchecked(&raw_bytes[offset..cursor]);
            let value = decode_value_storage_slice(slice)?;

            Ok((value, cursor))
        }
        other => Err(FieldDecodeError::new(format!(
            "structural binary: unsupported value tag 0x{other:02X}"
        ))),
    }
}

// Decode one tag-only value at a nested cursor position.
fn decode_value_storage_tag_only_at(
    offset: usize,
    value: Value,
) -> Result<(Value, usize), FieldDecodeError> {
    let cursor = offset
        .checked_add(1)
        .ok_or_else(|| FieldDecodeError::new("structural binary: head offset overflow"))?;

    Ok((value, cursor))
}

// Compute the end of a parsed scalar payload without requiring it to be the
// end of the enclosing byte slice. This mirrors the scalar branch of
// `skip_binary_value` for nested collection items.
pub(super) fn parsed_value_payload_end(
    raw_bytes: &[u8],
    len: u32,
    payload_start: usize,
    truncated_label: &'static str,
) -> Result<usize, FieldDecodeError> {
    let payload_len = usize::try_from(len)
        .map_err(|_| FieldDecodeError::new("structural binary: scalar length too large"))?;
    let cursor = payload_start
        .checked_add(payload_len)
        .ok_or_else(|| FieldDecodeError::new("structural binary: length overflow"))?;
    if cursor > raw_bytes.len() {
        return Err(FieldDecodeError::new(truncated_label));
    }

    Ok(cursor)
}

// Enforce root-scalar trailing-byte checks only when the caller is decoding a
// top-level value. Nested collection decode leaves following bytes to the
// owning walker and therefore passes no trailing label.
pub(super) fn enforce_optional_trailing_label(
    cursor: usize,
    raw_len: usize,
    trailing_label: Option<&'static str>,
) -> Result<(), FieldDecodeError> {
    if let Some(trailing_label) = trailing_label
        && cursor != raw_len
    {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok(())
}
