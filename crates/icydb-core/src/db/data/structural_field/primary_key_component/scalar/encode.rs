//! Module: data::structural_field::primary_key_component::scalar::encode
//! Responsibility: primary-key-component scalar payload encode dispatch.
//! Does not own: full relation collection framing, accepted-schema routing, or row encode.
//! Boundary: writes one primary-key-component scalar payload for a selected field kind.

use crate::{
    db::data::structural_field::binary::{
        push_binary_bytes, push_binary_int64, push_binary_list_len, push_binary_nat64,
        push_binary_unit,
    },
    db::data::structural_field::typed::{
        encode_int128_payload_bytes, encode_nat128_payload_bytes, encode_principal_payload_bytes,
        encode_subaccount_payload_bytes, encode_timestamp_payload_millis,
        encode_ulid_payload_bytes,
    },
    db::key_taxonomy::PrimaryKeyComponent,
    error::InternalError,
    model::field::FieldKind,
};

pub(in crate::db::data::structural_field::primary_key_component) fn encode_scalar_primary_key_component_field_binary_into(
    out: &mut Vec<u8>,
    key: PrimaryKeyComponent,
    kind: FieldKind,
    field_name: &str,
) -> Result<(), InternalError> {
    match (kind, key) {
        (FieldKind::Account, PrimaryKeyComponent::Account(value)) => {
            push_binary_list_len(out, 2);
            push_binary_bytes(out, value.owner().as_slice());
            match value.subaccount() {
                Some(subaccount) => push_binary_bytes(out, subaccount.as_slice()),
                None => crate::db::data::structural_field::binary::push_binary_null(out),
            }
            Ok(())
        }
        (FieldKind::Int64, PrimaryKeyComponent::Int64(value)) => {
            push_binary_int64(out, value);
            Ok(())
        }
        (FieldKind::Int8, PrimaryKeyComponent::Int64(value)) if i8::try_from(value).is_ok() => {
            push_binary_int64(out, value);
            Ok(())
        }
        (FieldKind::Int16, PrimaryKeyComponent::Int64(value)) if i16::try_from(value).is_ok() => {
            push_binary_int64(out, value);
            Ok(())
        }
        (FieldKind::Int32, PrimaryKeyComponent::Int64(value)) if i32::try_from(value).is_ok() => {
            push_binary_int64(out, value);
            Ok(())
        }
        (FieldKind::Int128, PrimaryKeyComponent::Int128(value)) => {
            push_binary_bytes(out, &encode_int128_payload_bytes(value));
            Ok(())
        }
        (FieldKind::Principal, PrimaryKeyComponent::Principal(value)) => {
            push_binary_bytes(out, encode_principal_payload_bytes(value)?.as_slice());
            Ok(())
        }
        (FieldKind::Subaccount, PrimaryKeyComponent::Subaccount(value)) => {
            push_binary_bytes(out, &encode_subaccount_payload_bytes(value));
            Ok(())
        }
        (FieldKind::Timestamp, PrimaryKeyComponent::Timestamp(value)) => {
            push_binary_int64(out, encode_timestamp_payload_millis(value));
            Ok(())
        }
        (FieldKind::Nat64, PrimaryKeyComponent::Nat64(value)) => {
            push_binary_nat64(out, value);
            Ok(())
        }
        (FieldKind::Nat8, PrimaryKeyComponent::Nat64(value)) if u8::try_from(value).is_ok() => {
            push_binary_nat64(out, value);
            Ok(())
        }
        (FieldKind::Nat16, PrimaryKeyComponent::Nat64(value)) if u16::try_from(value).is_ok() => {
            push_binary_nat64(out, value);
            Ok(())
        }
        (FieldKind::Nat32, PrimaryKeyComponent::Nat64(value)) if u32::try_from(value).is_ok() => {
            push_binary_nat64(out, value);
            Ok(())
        }
        (FieldKind::Nat128, PrimaryKeyComponent::Nat128(value)) => {
            push_binary_bytes(out, &encode_nat128_payload_bytes(value));
            Ok(())
        }
        (FieldKind::Ulid, PrimaryKeyComponent::Ulid(value)) => {
            push_binary_bytes(out, &encode_ulid_payload_bytes(value));
            Ok(())
        }
        (FieldKind::Unit, PrimaryKeyComponent::Unit) => {
            push_binary_unit(out);
            Ok(())
        }
        (_, _) => Err(InternalError::persisted_row_field_encode_internal(
            field_name,
        )),
    }
}
