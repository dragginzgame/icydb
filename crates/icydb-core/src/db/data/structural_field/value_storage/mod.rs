//! Module: data::structural_field::value_storage
//! Responsibility: owner-local binary `Value` envelope encode and decode.
//! Does not own: top-level `ByKind` dispatch, typed wrapper payload definitions, or storage-key policy.
//! Boundary: `FieldStorageDecode::CatalogValue` routes through this module without widening authority over sibling structural lanes.

mod canonical;
mod decode;
mod encode;
mod skip;
mod tags;

pub(in crate::db) use canonical::{
    decode_canonical_value_storage_bytes, encode_canonical_value_storage_bytes,
};
pub(in crate::db) use decode::{
    ValueStorageView, decode_structural_value_storage_bytes,
    validate_structural_value_storage_bytes, value_storage_bytes_are_null,
};
pub(in crate::db) use encode::encode_structural_value_storage_null_bytes;
