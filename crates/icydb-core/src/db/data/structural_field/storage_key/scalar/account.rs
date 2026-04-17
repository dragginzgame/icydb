use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            TAG_LIST, parse_binary_head as parse_structural_binary_head,
            skip_binary_value as skip_structural_binary_value,
        },
        storage_key::scalar::{
            decode_principal_storage_key_binary_bytes, decode_subaccount_storage_key_binary_bytes,
        },
    },
    value::StorageKey,
};

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
