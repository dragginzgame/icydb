use crate::{
    db::data::structural_field::binary::{
        push_binary_bytes, push_binary_int64, push_binary_list_len, push_binary_uint64,
        push_binary_unit,
    },
    db::data::structural_field::typed::{
        encode_principal_payload_bytes, encode_subaccount_payload_bytes,
        encode_timestamp_payload_millis, encode_ulid_payload_bytes,
    },
    error::InternalError,
    model::field::FieldKind,
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
            push_binary_bytes(out, encode_principal_payload_bytes(value)?.as_slice());
            Ok(())
        }
        (FieldKind::Subaccount, StorageKey::Subaccount(value)) => {
            push_binary_bytes(out, &encode_subaccount_payload_bytes(value));
            Ok(())
        }
        (FieldKind::Timestamp, StorageKey::Timestamp(value)) => {
            push_binary_int64(out, encode_timestamp_payload_millis(value));
            Ok(())
        }
        (FieldKind::Uint, StorageKey::Uint(value)) => {
            push_binary_uint64(out, value);
            Ok(())
        }
        (FieldKind::Ulid, StorageKey::Ulid(value)) => {
            push_binary_bytes(out, &encode_ulid_payload_bytes(value));
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
