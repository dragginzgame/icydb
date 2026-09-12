//! Module: data::structural_field::value_storage::tags
//! Responsibility: owner-local value-storage extension tag allocation.
//! Does not own: generic Structural Binary tags, payload encoding, or payload decoding.
//! Boundary: centralizes local tag constants so encode/decode stay ABI-aligned.

use crate::types::{Account, AccountStorageCodec};

pub(super) const VALUE_BINARY_TAG_ACCOUNT: u8 = 0x80;
pub(super) const VALUE_BINARY_TAG_DATE: u8 = 0x81;
pub(super) const VALUE_BINARY_TAG_DECIMAL: u8 = 0x82;
pub(super) const VALUE_BINARY_TAG_DURATION: u8 = 0x83;
pub(super) const VALUE_BINARY_TAG_ENUM: u8 = 0x84;
pub(super) const VALUE_BINARY_TAG_FLOAT32: u8 = 0x85;
pub(super) const VALUE_BINARY_TAG_FLOAT64: u8 = 0x86;
pub(super) const VALUE_BINARY_TAG_INT128: u8 = 0x87;
pub(super) const VALUE_BINARY_TAG_INT_BIG: u8 = 0x88;
pub(super) const VALUE_BINARY_TAG_PRINCIPAL: u8 = 0x89;
pub(super) const VALUE_BINARY_TAG_SUBACCOUNT: u8 = 0x8A;
pub(super) const VALUE_BINARY_TAG_TIMESTAMP: u8 = 0x8B;
pub(super) const VALUE_BINARY_TAG_NAT128: u8 = 0x8C;
pub(super) const VALUE_BINARY_TAG_NAT_BIG: u8 = 0x8D;
pub(super) const VALUE_BINARY_TAG_ULID: u8 = 0x8E;
pub(super) const VALUE_BINARY_TAG_U256: u8 = 0x8F;

// Fixed local tags already identify their payload width; no nested tag or
// length prefix is stored. Skip and exact decode share this one width owner.
pub(super) const fn fixed_value_storage_payload_len(tag: u8) -> Option<usize> {
    match tag {
        VALUE_BINARY_TAG_ACCOUNT => Some(Account::STORED_SIZE as usize),
        VALUE_BINARY_TAG_DATE | VALUE_BINARY_TAG_FLOAT32 => Some(4),
        VALUE_BINARY_TAG_DURATION | VALUE_BINARY_TAG_FLOAT64 | VALUE_BINARY_TAG_TIMESTAMP => {
            Some(8)
        }
        VALUE_BINARY_TAG_INT128 | VALUE_BINARY_TAG_NAT128 | VALUE_BINARY_TAG_ULID => Some(16),
        VALUE_BINARY_TAG_DECIMAL => Some(17),
        VALUE_BINARY_TAG_SUBACCOUNT | VALUE_BINARY_TAG_U256 => Some(32),
        _ => None,
    }
}

// Only variable-width extensions carry a nested structural frame.
pub(super) const fn is_nested_value_storage_tag(tag: u8) -> bool {
    matches!(
        tag,
        VALUE_BINARY_TAG_ENUM
            | VALUE_BINARY_TAG_INT_BIG
            | VALUE_BINARY_TAG_NAT_BIG
            | VALUE_BINARY_TAG_PRINCIPAL
    )
}
