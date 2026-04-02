//! Module: data::structural_row
//! Responsibility: canonical structural persisted-row decode helpers.
//! Does not own: typed entity reconstruction, slot layout planning, or query semantics.
//! Boundary: runtime paths use this module when they need persisted-row structure without `E`.

use crate::{
    db::{codec::ROW_FORMAT_VERSION_CURRENT, data::RawRow},
    error::InternalError,
    model::entity::EntityModel,
};
use serde_cbor::Value as CborValue;
use std::borrow::Cow;
use thiserror::Error as ThisError;

type SlotSpan = Option<(usize, usize)>;
type SlotSpans = Vec<SlotSpan>;
type RowFieldSpans<'a> = (Cow<'a, [u8]>, SlotSpans);

///
/// StructuralRowFieldBytes
///
/// StructuralRowFieldBytes is the top-level persisted-row field scanner for
/// slot-driven proof paths.
/// It keeps the original encoded field payload bytes and records one byte span
/// per model slot so callers can decode only the fields they actually need.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct StructuralRowFieldBytes<'a> {
    payload: Cow<'a, [u8]>,
    spans: SlotSpans,
}

impl<'a> StructuralRowFieldBytes<'a> {
    /// Decode one raw row payload into model slot-aligned encoded field spans.
    pub(in crate::db) fn from_row_bytes(
        row_bytes: &'a [u8],
        model: &'static EntityModel,
    ) -> Result<Self, StructuralRowDecodeError> {
        let payload = decode_structural_row_payload_bytes(row_bytes)?;
        let (payload, spans) = decode_row_field_spans(payload, model)?;

        Ok(Self { payload, spans })
    }

    /// Decode one raw row into model slot-aligned encoded field payload spans.
    pub(in crate::db) fn from_raw_row(
        raw_row: &'a RawRow,
        model: &'static EntityModel,
    ) -> Result<Self, StructuralRowDecodeError> {
        Self::from_row_bytes(raw_row.as_bytes(), model)
    }

    /// Borrow one encoded persisted field payload by stable slot index.
    #[must_use]
    pub(in crate::db) fn field(&self, slot: usize) -> Option<&[u8]> {
        let (start, end) = self.spans.get(slot).copied().flatten()?;

        Some(&self.payload[start..end])
    }
}

///
/// StructuralRowDecodeError
///
/// StructuralRowDecodeError captures shape failures after persisted-row bytes
/// have already decoded successfully through the shared structural CBOR path.
///

#[derive(Debug, ThisError)]
pub(in crate::db) enum StructuralRowDecodeError {
    #[error(transparent)]
    Deserialize(#[from] InternalError),
}

impl StructuralRowDecodeError {
    // Collapse the local structural decode wrapper back into the internal taxonomy.
    pub(in crate::db) fn into_internal_error(self) -> InternalError {
        match self {
            Self::Deserialize(err) => err,
        }
    }

    // Build one structural row corruption error at the manual decode boundary.
    fn corruption(message: impl Into<String>) -> Self {
        Self::Deserialize(InternalError::serialize_corruption(message.into()))
    }

    // Build one structural row compatibility error at the manual decode boundary.
    fn incompatible_persisted_format(message: impl Into<String>) -> Self {
        Self::Deserialize(InternalError::serialize_incompatible_persisted_format(
            message.into(),
        ))
    }
}

/// Decode one persisted row through the structural row-envelope validation path.
///
/// The only supported persisted row shape is the slot-framed payload envelope,
/// so this helper returns the validated slot payload bytes as `CborValue::Bytes`.
pub(in crate::db) fn decode_structural_row_cbor(
    raw_row: &RawRow,
) -> Result<CborValue, InternalError> {
    let payload = decode_structural_row_payload_bytes(raw_row.as_bytes())
        .map_err(StructuralRowDecodeError::into_internal_error)?;

    Ok(CborValue::Bytes(payload.into_owned()))
}

// Decode one persisted row envelope into the enclosed slot payload bytes.
fn decode_structural_row_payload_bytes(
    bytes: &[u8],
) -> Result<Cow<'_, [u8]>, StructuralRowDecodeError> {
    let Some((major, argument, mut cursor)) = parse_cbor_head(bytes, 0)? else {
        return Err(StructuralRowDecodeError::corruption(
            "row decode: empty row envelope",
        ));
    };
    if major != 4 || argument != 2 {
        return Err(StructuralRowDecodeError::corruption(
            "row decode: expected row envelope array[2]",
        ));
    }

    let Some((version_major, version_argument, version_end)) = parse_cbor_head(bytes, cursor)?
    else {
        return Err(StructuralRowDecodeError::corruption(
            "row decode: missing row format version",
        ));
    };
    if version_major != 0 {
        return Err(StructuralRowDecodeError::corruption(
            "row decode: row format version is not an unsigned integer",
        ));
    }
    let version = u8::try_from(version_argument).map_err(|_| {
        StructuralRowDecodeError::corruption("row decode: row format version out of range")
    })?;
    validate_structural_row_format_version(version)?;
    cursor = version_end;

    let Some((payload_major, payload_argument, payload_start)) = parse_cbor_head(bytes, cursor)?
    else {
        return Err(StructuralRowDecodeError::corruption(
            "row decode: missing row payload",
        ));
    };
    let payload = match payload_major {
        2 => {
            let payload_len = usize::try_from(payload_argument).map_err(|_| {
                StructuralRowDecodeError::corruption("row decode: payload length out of range")
            })?;
            let payload_end = payload_start.checked_add(payload_len).ok_or_else(|| {
                StructuralRowDecodeError::corruption("row decode: payload length overflow")
            })?;
            if payload_end != bytes.len() {
                return Err(StructuralRowDecodeError::corruption(
                    "row decode: trailing bytes after payload",
                ));
            }

            Cow::Borrowed(&bytes[payload_start..payload_end])
        }
        4 => {
            let payload_len = usize::try_from(payload_argument).map_err(|_| {
                StructuralRowDecodeError::corruption(
                    "row decode: payload array length out of range",
                )
            })?;
            let mut payload = Vec::with_capacity(payload_len);
            let mut payload_cursor = payload_start;

            for _ in 0..payload_len {
                let Some((byte_major, byte_argument, next_cursor)) =
                    parse_cbor_head(bytes, payload_cursor)?
                else {
                    return Err(StructuralRowDecodeError::corruption(
                        "row decode: truncated payload byte array",
                    ));
                };
                if byte_major != 0 {
                    return Err(StructuralRowDecodeError::corruption(
                        "row decode: payload byte array contains non-integer element",
                    ));
                }
                let byte = u8::try_from(byte_argument).map_err(|_| {
                    StructuralRowDecodeError::corruption(
                        "row decode: payload byte array element out of range",
                    )
                })?;
                payload.push(byte);
                payload_cursor = next_cursor;
            }

            if payload_cursor != bytes.len() {
                return Err(StructuralRowDecodeError::corruption(
                    "row decode: trailing bytes after payload byte array",
                ));
            }

            Cow::Owned(payload)
        }
        _ => {
            return Err(StructuralRowDecodeError::corruption(
                "row decode: payload is not a byte string",
            ));
        }
    };

    Ok(payload)
}

// Decode the canonical slot-container header into slot-aligned payload spans.
fn decode_row_field_spans<'a>(
    payload: Cow<'a, [u8]>,
    model: &'static EntityModel,
) -> Result<RowFieldSpans<'a>, StructuralRowDecodeError> {
    let bytes = payload.as_ref();
    let field_count_bytes = bytes
        .get(..2)
        .ok_or_else(|| StructuralRowDecodeError::corruption("row decode: truncated slot header"))?;
    let field_count = usize::from(u16::from_be_bytes([
        field_count_bytes[0],
        field_count_bytes[1],
    ]));
    if field_count != model.fields().len() {
        return Err(StructuralRowDecodeError::corruption(format!(
            "row decode: slot count mismatch: expected {}, found {}",
            model.fields().len(),
            field_count,
        )));
    }
    let table_len = field_count
        .checked_mul(8)
        .ok_or_else(|| StructuralRowDecodeError::corruption("row decode: slot table overflow"))?;
    let data_start = 2usize.checked_add(table_len).ok_or_else(|| {
        StructuralRowDecodeError::corruption("row decode: slot payload header overflow")
    })?;
    let table = bytes
        .get(2..data_start)
        .ok_or_else(|| StructuralRowDecodeError::corruption("row decode: truncated slot table"))?;
    let data_section = bytes
        .get(data_start..)
        .ok_or_else(|| StructuralRowDecodeError::corruption("row decode: missing slot payloads"))?;
    let mut spans: SlotSpans = vec![None; model.fields().len()];

    for (slot, span) in spans.iter_mut().enumerate() {
        let entry_start = slot.checked_mul(8).ok_or_else(|| {
            StructuralRowDecodeError::corruption("row decode: slot index overflow")
        })?;
        let entry = table.get(entry_start..entry_start + 8).ok_or_else(|| {
            StructuralRowDecodeError::corruption("row decode: truncated slot table entry")
        })?;
        let start = usize::try_from(u32::from_be_bytes([entry[0], entry[1], entry[2], entry[3]]))
            .map_err(|_| {
            StructuralRowDecodeError::corruption("row decode: slot start out of range")
        })?;
        let len = usize::try_from(u32::from_be_bytes([entry[4], entry[5], entry[6], entry[7]]))
            .map_err(|_| {
                StructuralRowDecodeError::corruption("row decode: slot length out of range")
            })?;
        if len == 0 {
            return Err(StructuralRowDecodeError::corruption(format!(
                "row decode: missing slot payload: slot={slot}",
            )));
        }
        let end = start.checked_add(len).ok_or_else(|| {
            StructuralRowDecodeError::corruption("row decode: slot span overflow")
        })?;
        if end > data_section.len() {
            return Err(StructuralRowDecodeError::corruption(
                "row decode: slot span exceeds payload length",
            ));
        }
        *span = Some((start, end));
    }

    let payload = match payload {
        Cow::Borrowed(bytes) => Cow::Borrowed(&bytes[data_start..]),
        Cow::Owned(bytes) => Cow::Owned(bytes[data_start..].to_vec()),
    };

    Ok((payload, spans))
}

// Parse one CBOR head into `(major, argument, payload_cursor)` while rejecting
// indefinite-length encodings from persisted rows.
fn parse_cbor_head(
    bytes: &[u8],
    cursor: usize,
) -> Result<Option<(u8, u64, usize)>, StructuralRowDecodeError> {
    let Some(&first) = bytes.get(cursor) else {
        return Ok(None);
    };
    let major = first >> 5;
    let additional = first & 0x1f;
    let mut next_cursor = cursor + 1;

    let argument = match additional {
        value @ 0..=23 => u64::from(value),
        24 => {
            let value = *bytes.get(next_cursor).ok_or_else(|| {
                StructuralRowDecodeError::corruption("row decode: truncated CBOR head")
            })?;
            next_cursor += 1;

            u64::from(value)
        }
        25 => {
            let bytes = bytes.get(next_cursor..next_cursor + 2).ok_or_else(|| {
                StructuralRowDecodeError::corruption("row decode: truncated CBOR head")
            })?;
            next_cursor += 2;

            u64::from(u16::from_be_bytes([bytes[0], bytes[1]]))
        }
        26 => {
            let bytes = bytes.get(next_cursor..next_cursor + 4).ok_or_else(|| {
                StructuralRowDecodeError::corruption("row decode: truncated CBOR head")
            })?;
            next_cursor += 4;

            u64::from(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        }
        27 => {
            let bytes = bytes.get(next_cursor..next_cursor + 8).ok_or_else(|| {
                StructuralRowDecodeError::corruption("row decode: truncated CBOR head")
            })?;
            next_cursor += 8;

            u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
        }
        31 => {
            return Err(StructuralRowDecodeError::corruption(
                "row decode: indefinite-length CBOR is unsupported in persisted rows",
            ));
        }
        _ => {
            return Err(StructuralRowDecodeError::corruption(
                "row decode: invalid CBOR additional info",
            ));
        }
    };

    Ok(Some((major, argument, next_cursor)))
}

// Validate the manually decoded persisted row format version.
fn validate_structural_row_format_version(
    format_version: u8,
) -> Result<(), StructuralRowDecodeError> {
    if format_version == ROW_FORMAT_VERSION_CURRENT {
        return Ok(());
    }

    Err(StructuralRowDecodeError::incompatible_persisted_format(
        format!(
            "row format version {format_version} is unsupported by runtime version {ROW_FORMAT_VERSION_CURRENT}",
        ),
    ))
}
