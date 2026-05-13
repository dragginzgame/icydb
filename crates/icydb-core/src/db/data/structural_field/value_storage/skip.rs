use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{
        TAG_BYTES, TAG_FALSE, TAG_INT64, TAG_LIST, TAG_MAP, TAG_NAT64, TAG_NULL, TAG_TEXT,
        TAG_TRUE, TAG_UNIT, parse_binary_head, skip_binary_value,
    },
    value_storage::tags::is_local_value_storage_tag,
};

// Skip one binary `Value` envelope without delegating nested `Value` items
// back to the generic Structural Binary walker.
pub(super) fn skip_value_storage_binary_value(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<usize, FieldDecodeError> {
    let Some(&tag) = raw_bytes.get(offset) else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value payload",
        ));
    };

    match tag {
        TAG_NULL | TAG_UNIT | TAG_FALSE | TAG_TRUE | TAG_INT64 | TAG_NAT64 | TAG_TEXT
        | TAG_BYTES => skip_binary_value(raw_bytes, offset),
        TAG_LIST => skip_value_storage_binary_list(raw_bytes, offset),
        TAG_MAP => skip_value_storage_binary_map(raw_bytes, offset),
        other if is_local_value_storage_tag(other) => {
            skip_value_storage_binary_value(raw_bytes, offset + 1)
        }
        other => Err(FieldDecodeError::new(format!(
            "structural binary: unsupported value tag 0x{other:02X}"
        ))),
    }
}

// Skip one binary value list by recursing through nested `Value` items.
fn skip_value_storage_binary_list(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<usize, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value list payload",
        ));
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new(
            "structural binary: expected value list payload",
        ));
    }

    let mut cursor = payload_start;
    for _ in 0..len {
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
    }

    Ok(cursor)
}

// Skip one binary value map by recursing through nested `Value` keys and
// values.
fn skip_value_storage_binary_map(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<usize, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, offset)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated value map payload",
        ));
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new(
            "structural binary: expected value map payload",
        ));
    }

    let mut cursor = payload_start;
    for _ in 0..len {
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
        cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
    }

    Ok(cursor)
}
