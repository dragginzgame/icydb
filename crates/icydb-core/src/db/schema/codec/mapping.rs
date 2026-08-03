//! Mechanical helpers for direct accepted-schema binary mappings.
//!
//! These macros deliberately generate only length framing and exhaustive
//! numeric-tag matches. They do not create a reflected value model or a
//! second hierarchy of wire DTOs.

macro_rules! encode_sequence {
    ($writer:expr, $values:expr, $max:expr, |$value:ident| $encode:block) => {{
        let values = $values;
        if values.len() > $max {
            return Err(crate::error::InternalError::store_unsupported());
        }
        $writer.push_len(values.len())?;
        for $value in values {
            $encode
        }
    }};
}

macro_rules! decode_sequence {
    ($reader:expr, $max:expr, $decode:expr) => {{
        let count = $reader.read_bounded_count($max)?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push($decode);
        }
        values
    }};
}

macro_rules! direct_unit_enum_codec {
    (
        encode = $encode:ident,
        decode = $decode:ident,
        type = $ty:path,
        writer = $writer:ty,
        { $($tag:literal => $variant:path),+ $(,)? }
    ) => {
        fn $encode(writer: &mut $writer, value: $ty) {
            let tag = match value {
                $($variant => $tag,)+
            };
            writer.push_u8(tag);
        }

        fn $decode(
            reader: &mut crate::db::schema::wire::SchemaWireReader<'_>,
        ) -> Result<$ty, crate::error::InternalError> {
            match reader.read_u8()? {
                $($tag => Ok($variant),)+
                _ => Err(crate::error::InternalError::store_corruption()),
            }
        }
    };
}

pub(super) use decode_sequence;
pub(super) use direct_unit_enum_codec;
pub(super) use encode_sequence;
