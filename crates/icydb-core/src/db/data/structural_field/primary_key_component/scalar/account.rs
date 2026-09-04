//! Module: data::structural_field::primary_key_component::scalar::account
//! Responsibility: account primary-key-component scalar decode.
//! Does not own: generic scalar dispatch, relation traversal, or row decode.
//! Boundary: decodes the account-specific primary-key-component payload after callers select this scalar lane.

use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{CompleteBinaryValue, TAG_LIST, TAG_NULL, skip_binary_value},
        primary_key_component::scalar::{
            decode_principal_primary_key_component, decode_subaccount_primary_key_component,
        },
    },
    db::key_taxonomy::PrimaryKeyComponent,
};

// Decode one account relation-key payload from Structural Binary v1 without
// routing through generic value decode.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_account_primary_key_component(
    root: &CompleteBinaryValue<'_>,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    if root.tag() != TAG_LIST || root.len() != 2 {
        return Err(FieldDecodeError::new());
    }

    let raw_bytes = root.bytes();
    let owner_start = root.payload_offset();
    let owner_end = skip_binary_value(raw_bytes, owner_start)?;
    let sub_start = owner_end;
    let sub_end = skip_binary_value(raw_bytes, sub_start)?;
    if sub_end != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    let owner_root = CompleteBinaryValue::from_skip_bounded(&raw_bytes[owner_start..owner_end])?;
    let PrimaryKeyComponent::Principal(owner) =
        decode_principal_primary_key_component(&owner_root)?
    else {
        return Err(FieldDecodeError::new());
    };
    let sub_root = CompleteBinaryValue::from_skip_bounded(&raw_bytes[sub_start..sub_end])?;
    let subaccount = if sub_root.tag() == TAG_NULL {
        None
    } else {
        match decode_subaccount_primary_key_component(&sub_root)? {
            PrimaryKeyComponent::Subaccount(value) => Some(value),
            _ => return Err(FieldDecodeError::new()),
        }
    };

    Ok(PrimaryKeyComponent::Account(
        crate::types::Account::from_owner_and_subaccount(owner, subaccount),
    ))
}
