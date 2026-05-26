//! Module: data::structural_field::primary_key_component::scalar
//! Responsibility: primary-key-component scalar codec module wiring.
//! Does not own: relation collection traversal, value-storage envelopes, or row policy.
//! Boundary: re-exports scalar helpers only to the relation primary-key-component owner.

mod account;
mod bytes;
mod encode;
mod primitive;

pub(in crate::db::data::structural_field::primary_key_component) use account::decode_account_primary_key_component_binary_bytes;
pub(in crate::db::data::structural_field::primary_key_component) use bytes::{
    decode_principal_primary_key_component_binary_bytes,
    decode_subaccount_primary_key_component_binary_bytes,
    decode_ulid_primary_key_component_binary_bytes,
};
pub(in crate::db::data::structural_field::primary_key_component) use encode::encode_scalar_primary_key_component_field_binary_into;
pub(in crate::db::data::structural_field::primary_key_component) use primitive::{
    decode_int_primary_key_component_binary_bytes,
    decode_int128_primary_key_component_binary_bytes,
    decode_nat_primary_key_component_binary_bytes,
    decode_nat128_primary_key_component_binary_bytes,
    decode_timestamp_primary_key_component_binary_bytes,
    decode_unit_primary_key_component_binary_bytes,
};
