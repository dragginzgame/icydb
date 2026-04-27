//! Module: data::storage_key
//! Responsibility: short aliases for storage-key field and binary codecs.
//! Boundary: preserves the original structural-field functions while giving
//! callers a semantic namespace.

pub(in crate::db) mod decode {
    pub(in crate::db) use crate::db::data::structural_field::{
        decode_optional_storage_key_field_bytes as optional_field,
        decode_storage_key_binary_value_bytes as binary_value,
    };
}

pub(in crate::db) mod encode {
    pub(in crate::db) use crate::db::data::structural_field::{
        encode_storage_key_binary_value_bytes as binary_value,
        encode_storage_key_field_bytes as field,
    };
}

pub(in crate::db) use crate::db::data::structural_field::supports_storage_key_binary_kind as supports_binary_kind;
