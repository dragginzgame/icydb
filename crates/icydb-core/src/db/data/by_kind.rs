//! Module: data::by_kind
//! Responsibility: short aliases for FieldKind-driven structural codecs.
//! Boundary: preserves the original structural-field functions while giving
//! callers a semantic namespace.

pub(in crate::db) mod decode {
    pub(in crate::db) use crate::db::data::structural_field::{
        decode_blob_field_by_kind_bytes as blob, decode_bool_field_by_kind_bytes as bool,
        decode_date_field_by_kind_bytes as date, decode_decimal_field_by_kind_bytes as decimal,
        decode_duration_field_by_kind_bytes as duration,
        decode_float32_field_by_kind_bytes as float32,
        decode_float64_field_by_kind_bytes as float64,
        decode_int_big_field_by_kind_bytes as int_big, decode_int128_field_by_kind_bytes as int128,
        decode_nat128_field_by_kind_bytes as nat128,
        decode_structural_field_by_kind_bytes as value, decode_text_field_by_kind_bytes as text,
        decode_uint_big_field_by_kind_bytes as uint_big,
    };
}

pub(in crate::db) mod encode {
    pub(in crate::db) use crate::db::data::structural_field::{
        encode_blob_field_by_kind_bytes as blob, encode_bool_field_by_kind_bytes as bool,
        encode_date_field_by_kind_bytes as date, encode_decimal_field_by_kind_bytes as decimal,
        encode_duration_field_by_kind_bytes as duration,
        encode_float32_field_by_kind_bytes as float32,
        encode_float64_field_by_kind_bytes as float64,
        encode_int_big_field_by_kind_bytes as int_big, encode_int128_field_by_kind_bytes as int128,
        encode_nat128_field_by_kind_bytes as nat128,
        encode_structural_field_by_kind_bytes as value, encode_text_field_by_kind_bytes as text,
        encode_uint_big_field_by_kind_bytes as uint_big,
    };
}
