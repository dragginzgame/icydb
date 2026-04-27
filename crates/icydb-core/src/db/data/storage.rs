//! Module: data::storage
//! Responsibility: short aliases for raw structural value-storage codecs.
//! Boundary: preserves the original structural-field functions while giving
//! callers a semantic namespace.

pub(in crate::db) mod decode {
    pub(in crate::db) use crate::db::data::structural_field::{
        decode_account as account, decode_decimal as decimal, decode_int as int,
        decode_int128 as int128, decode_nat as nat, decode_nat128 as nat128,
        decode_structural_value_storage_blob_bytes as blob,
        decode_structural_value_storage_bool_bytes as bool,
        decode_structural_value_storage_bytes as value,
        decode_structural_value_storage_date_bytes as date,
        decode_structural_value_storage_duration_bytes as duration,
        decode_structural_value_storage_float32_bytes as float32,
        decode_structural_value_storage_float64_bytes as float64,
        decode_structural_value_storage_i64_bytes as i64,
        decode_structural_value_storage_principal_bytes as principal,
        decode_structural_value_storage_subaccount_bytes as subaccount,
        decode_structural_value_storage_timestamp_bytes as timestamp,
        decode_structural_value_storage_u64_bytes as u64,
        decode_structural_value_storage_ulid_bytes as ulid,
        decode_structural_value_storage_unit_bytes as unit, decode_text as text,
        structural_value_storage_bytes_are_null as is_null,
    };
}

pub(in crate::db) mod encode {
    pub(in crate::db) use crate::db::data::structural_field::{
        encode_account as account, encode_decimal as decimal, encode_int as int,
        encode_int128 as int128, encode_nat as nat, encode_nat128 as nat128,
        encode_structural_value_storage_blob_bytes as blob,
        encode_structural_value_storage_bool_bytes as bool,
        encode_structural_value_storage_bytes as value,
        encode_structural_value_storage_date_bytes as date,
        encode_structural_value_storage_duration_bytes as duration,
        encode_structural_value_storage_float32_bytes as float32,
        encode_structural_value_storage_float64_bytes as float64,
        encode_structural_value_storage_i64_bytes as i64,
        encode_structural_value_storage_null_bytes as null,
        encode_structural_value_storage_principal_bytes as principal,
        encode_structural_value_storage_subaccount_bytes as subaccount,
        encode_structural_value_storage_timestamp_bytes as timestamp,
        encode_structural_value_storage_u64_bytes as u64,
        encode_structural_value_storage_ulid_bytes as ulid,
        encode_structural_value_storage_unit_bytes as unit, encode_text as text,
    };
}
