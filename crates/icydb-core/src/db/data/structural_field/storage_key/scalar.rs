use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            TAG_BYTES, TAG_INT64, TAG_LIST, TAG_UINT64, TAG_UNIT,
            parse_binary_head as parse_structural_binary_head,
            payload_bytes as binary_payload_bytes, push_binary_bytes, push_binary_int64,
            push_binary_list_len, push_binary_uint64, push_binary_unit,
            skip_binary_value as skip_structural_binary_value,
        },
    },
    error::InternalError,
    model::field::FieldKind,
    types::Ulid,
    value::StorageKey,
};

pub(in crate::db::data::structural_field::storage_key) fn encode_scalar_storage_key_field_binary_into(
    out: &mut Vec<u8>,
    key: StorageKey,
    kind: FieldKind,
    field_name: &str,
) -> Result<(), InternalError> {
    match (kind, key) {
        (FieldKind::Account, StorageKey::Account(value)) => {
            push_binary_list_len(out, 2);
            push_binary_bytes(out, value.owner().as_slice());
            match value.subaccount() {
                Some(subaccount) => push_binary_bytes(out, subaccount.as_slice()),
                None => crate::db::data::structural_field::binary::push_binary_null(out),
            }
            Ok(())
        }
        (FieldKind::Int, StorageKey::Int(value)) => {
            push_binary_int64(out, value);
            Ok(())
        }
        (FieldKind::Principal, StorageKey::Principal(value)) => {
            push_binary_bytes(out, value.as_slice());
            Ok(())
        }
        (FieldKind::Subaccount, StorageKey::Subaccount(value)) => {
            push_binary_bytes(out, value.as_slice());
            Ok(())
        }
        (FieldKind::Timestamp, StorageKey::Timestamp(value)) => {
            push_binary_int64(out, value.as_millis());
            Ok(())
        }
        (FieldKind::Uint, StorageKey::Uint(value)) => {
            push_binary_uint64(out, value);
            Ok(())
        }
        (FieldKind::Ulid, StorageKey::Ulid(value)) => {
            push_binary_bytes(out, &value.to_bytes());
            Ok(())
        }
        (FieldKind::Unit, StorageKey::Unit) => {
            push_binary_unit(out);
            Ok(())
        }
        (other, key) => Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {other:?} does not accept storage key {key:?}"),
        )),
    }
}

// Decode one account relation-key payload from Structural Binary v1 without
// routing through generic value decode.
pub(in crate::db::data::structural_field::storage_key) fn decode_account_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated account payload",
        ));
    };
    if tag != TAG_LIST || len != 2 {
        return Err(FieldDecodeError::new(
            "structural binary: expected two-item account payload",
        ));
    }

    let owner_start = payload_start;
    let owner_end = skip_structural_binary_value(raw_bytes, owner_start)?;
    let sub_start = owner_end;
    let sub_end = skip_structural_binary_value(raw_bytes, sub_start)?;
    if sub_end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after account payload",
        ));
    }

    let StorageKey::Principal(owner) =
        decode_principal_storage_key_binary_bytes(&raw_bytes[owner_start..owner_end])?
    else {
        unreachable!("principal key decode must return a principal");
    };
    let subaccount = if let Some((tag, _len, _payload_start)) =
        parse_structural_binary_head(&raw_bytes[sub_start..sub_end], 0)?
    {
        if tag == crate::db::data::structural_field::binary::TAG_NULL {
            None
        } else {
            match decode_subaccount_storage_key_binary_bytes(&raw_bytes[sub_start..sub_end])? {
                StorageKey::Subaccount(value) => Some(value),
                _ => unreachable!("subaccount key decode must return a subaccount"),
            }
        }
    } else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated subaccount payload",
        ));
    };

    Ok(StorageKey::Account(crate::types::Account::from_parts(
        owner, subaccount,
    )))
}

// Decode one timestamp relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::storage_key) fn decode_timestamp_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated timestamp payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after timestamp payload",
        ));
    }
    if tag != TAG_INT64 || len != 8 {
        return Err(FieldDecodeError::new(
            "structural binary: expected i64 timestamp payload",
        ));
    }
    let payload: [u8; 8] = binary_payload_bytes(raw_bytes, len, payload_start, "timestamp")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid timestamp payload"))?;

    Ok(StorageKey::Timestamp(crate::types::Timestamp::from_millis(
        i64::from_be_bytes(payload),
    )))
}

// Decode one principal relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::storage_key) fn decode_principal_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated principal payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after principal payload",
        ));
    }
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new(
            "structural binary: expected bytes principal payload",
        ));
    }

    crate::types::Principal::try_from_bytes(binary_payload_bytes(
        raw_bytes,
        len,
        payload_start,
        "principal",
    )?)
    .map(StorageKey::Principal)
    .map_err(|_| FieldDecodeError::new("structural binary: invalid principal payload"))
}

// Decode one subaccount relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::storage_key) fn decode_subaccount_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated subaccount payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after subaccount payload",
        ));
    }
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new(
            "structural binary: expected bytes subaccount payload",
        ));
    }
    let payload = binary_payload_bytes(raw_bytes, len, payload_start, "subaccount")?;
    let bytes: [u8; 32] = payload
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid subaccount payload"))?;

    Ok(StorageKey::Subaccount(
        crate::types::Subaccount::from_array(bytes),
    ))
}

// Decode one ULID relation-key payload directly from its fixed-width Structural
// Binary bytes form.
pub(in crate::db::data::structural_field::storage_key) fn decode_ulid_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated ulid payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after ulid payload",
        ));
    }
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new(
            "structural binary: expected bytes ulid payload",
        ));
    }

    Ulid::try_from_bytes(binary_payload_bytes(raw_bytes, len, payload_start, "ulid")?)
        .map(StorageKey::Ulid)
        .map_err(|_| FieldDecodeError::new("structural binary: invalid ulid payload"))
}

// Decode one unit relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::storage_key) fn decode_unit_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, _len, _payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated unit payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after unit payload",
        ));
    }
    if tag != TAG_UNIT {
        return Err(FieldDecodeError::new(
            "structural binary: expected unit payload",
        ));
    }

    Ok(StorageKey::Unit)
}

// Decode one signed storage-key-compatible integer payload from Structural
// Binary v1.
pub(in crate::db::data::structural_field::storage_key) fn decode_int_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated integer payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after relation field",
        ));
    }
    if tag != TAG_INT64 || len != 8 {
        return Err(FieldDecodeError::new(
            "structural binary: expected i64 integer payload",
        ));
    }
    let payload: [u8; 8] = binary_payload_bytes(raw_bytes, len, payload_start, "integer")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid i64 payload"))?;

    Ok(StorageKey::Int(i64::from_be_bytes(payload)))
}

// Decode one unsigned storage-key-compatible integer payload from Structural
// Binary v1.
pub(in crate::db::data::structural_field::storage_key) fn decode_uint_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated integer payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after relation field",
        ));
    }
    if tag != TAG_UINT64 || len != 8 {
        return Err(FieldDecodeError::new(
            "structural binary: expected u64 integer payload",
        ));
    }
    let payload: [u8; 8] = binary_payload_bytes(raw_bytes, len, payload_start, "integer")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid u64 payload"))?;

    Ok(StorageKey::Uint(u64::from_be_bytes(payload)))
}
