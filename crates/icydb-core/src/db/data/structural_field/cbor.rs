//! Module: data::structural_field::cbor
//! Responsibility: low-level bounded CBOR parsing and raw-slice walkers for structural field decode.
//! Does not own: field semantics, runtime `Value` reconstruction, or storage-key policy.
//! Boundary: sibling structural-field modules call into this file when they need raw CBOR traversal without serde.

use crate::db::data::structural_field::FieldDecodeError;
use std::str;

const CBOR_MAJOR_TYPE_SHIFT: u8 = 5;
const CBOR_ADDITIONAL_INFO_MASK: u8 = 0x1f;

const CBOR_MAJOR_UNSIGNED_INT: u8 = 0;
const CBOR_MAJOR_NEGATIVE_INT: u8 = 1;
const CBOR_MAJOR_BYTE_STRING: u8 = 2;
const CBOR_MAJOR_TEXT_STRING: u8 = 3;
const CBOR_MAJOR_ARRAY: u8 = 4;
const CBOR_MAJOR_MAP: u8 = 5;
const CBOR_MAJOR_TAG: u8 = 6;
const CBOR_MAJOR_SIMPLE_OR_FLOAT: u8 = 7;

const CBOR_ADDITIONAL_INFO_INLINE_MAX: u8 = 23;
const CBOR_ADDITIONAL_INFO_U8: u8 = 24;
const CBOR_ADDITIONAL_INFO_U16: u8 = 25;
const CBOR_ADDITIONAL_INFO_U32: u8 = 26;
const CBOR_ADDITIONAL_INFO_U64: u8 = 27;
const CBOR_ADDITIONAL_INFO_INDEFINITE: u8 = 31;

const CBOR_U16_WIDTH: usize = 2;
const CBOR_U32_WIDTH: usize = 4;
const CBOR_U64_WIDTH: usize = 8;

const CBOR_FLOAT32_ARGUMENT: u64 = 26;
const CBOR_FLOAT64_ARGUMENT: u64 = 27;

const TAGGED_VARIANT_ENTRY_COUNT: u64 = 1;

// Alias the callback shape for raw CBOR array walkers.
type ArrayItemDecodeFn = unsafe fn(&[u8], *mut ()) -> Result<(), FieldDecodeError>;

// Alias the callback shape for raw CBOR map walkers.
type MapEntryDecodeFn = unsafe fn(&[u8], &[u8], *mut ()) -> Result<(), FieldDecodeError>;

// Walk one CBOR array and yield each raw item slice to the caller.
//
// Safety:
// `context` must point at the state type expected by `on_item` for the full
// duration of this call.
pub(super) fn walk_cbor_array_items(
    raw_bytes: &[u8],
    shape_label: &'static str,
    trailing_label: &'static str,
    context: *mut (),
    on_item: ArrayItemDecodeFn,
) -> Result<(), FieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    if major != CBOR_MAJOR_ARRAY {
        return Err(FieldDecodeError::new(shape_label));
    }

    let item_count = bounded_cbor_len(argument, "expected bounded CBOR array length")?;
    for _ in 0..item_count {
        let item_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        // Safety: the caller pairs `context` with the matching callback, so the
        // callback sees the concrete state type it expects.
        unsafe { on_item(&raw_bytes[item_start..cursor], context)? };
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok(())
}

// Walk one CBOR map and yield each raw key/value slice pair to the caller.
//
// Safety:
// `context` must point at the state type expected by `on_entry` for the full
// duration of this call.
pub(super) fn walk_cbor_map_entries(
    raw_bytes: &[u8],
    shape_label: &'static str,
    trailing_label: &'static str,
    context: *mut (),
    on_entry: MapEntryDecodeFn,
) -> Result<(), FieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    if major != CBOR_MAJOR_MAP {
        return Err(FieldDecodeError::new(shape_label));
    }

    let entry_count = bounded_cbor_len(argument, "expected bounded CBOR map length")?;
    for _ in 0..entry_count {
        let key_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        let value_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        // Safety: the caller pairs `context` with the matching callback, so the
        // callback sees the concrete state type it expects.
        unsafe {
            on_entry(
                &raw_bytes[key_start..value_start],
                &raw_bytes[value_start..cursor],
                context,
            )?;
        };
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(trailing_label));
    }

    Ok(())
}

// Parse one bounded CBOR container length into a host `usize`.
pub(super) fn bounded_cbor_len(
    argument: u64,
    label: &'static str,
) -> Result<usize, FieldDecodeError> {
    usize::try_from(argument).map_err(|_| FieldDecodeError::new(label))
}

// Parse one tagged CBOR head into `(major, argument, payload_start)`.
pub(super) fn parse_tagged_cbor_head(
    bytes: &[u8],
    mut cursor: usize,
) -> Result<Option<(u8, u64, usize)>, FieldDecodeError> {
    let Some((mut major, mut argument, mut next_cursor)) = parse_cbor_head(bytes, cursor)? else {
        return Ok(None);
    };

    while major == CBOR_MAJOR_TAG {
        cursor = next_cursor;
        let Some((inner_major, inner_argument, inner_next_cursor)) =
            parse_cbor_head(bytes, cursor)?
        else {
            return Err(FieldDecodeError::new(
                "typed CBOR decode failed: truncated tagged CBOR value",
            ));
        };
        major = inner_major;
        argument = inner_argument;
        next_cursor = inner_next_cursor;
    }

    Ok(Some((major, argument, next_cursor)))
}

// Parse one definite-length CBOR head.
fn parse_cbor_head(
    bytes: &[u8],
    cursor: usize,
) -> Result<Option<(u8, u64, usize)>, FieldDecodeError> {
    let Some(&first) = bytes.get(cursor) else {
        return Ok(None);
    };
    let major = first >> CBOR_MAJOR_TYPE_SHIFT;
    let additional = first & CBOR_ADDITIONAL_INFO_MASK;
    let mut next_cursor = cursor + 1;

    let argument = match additional {
        value @ 0..=CBOR_ADDITIONAL_INFO_INLINE_MAX => u64::from(value),
        CBOR_ADDITIONAL_INFO_U8 => {
            let value = *bytes.get(next_cursor).ok_or_else(|| {
                FieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 1;

            u64::from(value)
        }
        CBOR_ADDITIONAL_INFO_U16 => {
            let payload = bytes
                .get(next_cursor..next_cursor + CBOR_U16_WIDTH)
                .ok_or_else(|| {
                    FieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
                })?;
            next_cursor += CBOR_U16_WIDTH;

            u64::from(u16::from_be_bytes([payload[0], payload[1]]))
        }
        CBOR_ADDITIONAL_INFO_U32 => {
            let payload = bytes
                .get(next_cursor..next_cursor + CBOR_U32_WIDTH)
                .ok_or_else(|| {
                    FieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
                })?;
            next_cursor += CBOR_U32_WIDTH;

            u64::from(u32::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ]))
        }
        CBOR_ADDITIONAL_INFO_U64 => {
            let payload = bytes
                .get(next_cursor..next_cursor + CBOR_U64_WIDTH)
                .ok_or_else(|| {
                    FieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
                })?;
            next_cursor += CBOR_U64_WIDTH;

            u64::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
                payload[7],
            ])
        }
        CBOR_ADDITIONAL_INFO_INDEFINITE => {
            return Err(FieldDecodeError::new(
                "typed CBOR decode failed: indefinite-length CBOR is unsupported",
            ));
        }
        _ => {
            return Err(FieldDecodeError::new(
                "typed CBOR decode failed: invalid CBOR additional info",
            ));
        }
    };

    Ok(Some((major, argument, next_cursor)))
}

// Skip one tagged CBOR value without rebuilding a `CborValue`.
pub(super) fn skip_cbor_value(bytes: &[u8], cursor: usize) -> Result<usize, FieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(bytes, cursor)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };

    match major {
        CBOR_MAJOR_UNSIGNED_INT | CBOR_MAJOR_NEGATIVE_INT | CBOR_MAJOR_SIMPLE_OR_FLOAT => {
            Ok(cursor)
        }
        CBOR_MAJOR_BYTE_STRING | CBOR_MAJOR_TEXT_STRING => {
            let len = usize::try_from(argument).map_err(|_| {
                FieldDecodeError::new("typed CBOR decode failed: CBOR scalar too large")
            })?;
            cursor = cursor.checked_add(len).ok_or_else(|| {
                FieldDecodeError::new("typed CBOR decode failed: CBOR scalar length overflow")
            })?;
            if cursor > bytes.len() {
                return Err(FieldDecodeError::new(
                    "typed CBOR decode failed: truncated CBOR scalar payload",
                ));
            }

            Ok(cursor)
        }
        CBOR_MAJOR_ARRAY => {
            let len = usize::try_from(argument).map_err(|_| {
                FieldDecodeError::new("typed CBOR decode failed: CBOR array too large")
            })?;
            for _ in 0..len {
                cursor = skip_cbor_value(bytes, cursor)?;
            }

            Ok(cursor)
        }
        CBOR_MAJOR_MAP => {
            let len = usize::try_from(argument).map_err(|_| {
                FieldDecodeError::new("typed CBOR decode failed: CBOR map too large")
            })?;
            for _ in 0..len {
                cursor = skip_cbor_value(bytes, cursor)?;
                cursor = skip_cbor_value(bytes, cursor)?;
            }

            Ok(cursor)
        }
        _ => Err(FieldDecodeError::new(
            "typed CBOR decode failed: unsupported CBOR major type",
        )),
    }
}

// Parse one tagged CBOR text scalar in place.
pub(super) fn parse_text_scalar_at(
    bytes: &[u8],
    cursor: usize,
) -> Result<(&str, usize), FieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(bytes, cursor)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: missing text scalar",
        ));
    };
    if major != CBOR_MAJOR_TEXT_STRING {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: expected a text string",
        ));
    }

    let text = decode_text_scalar_bytes(bytes, argument, payload_start)?;
    let text_len = usize::try_from(argument)
        .map_err(|_| FieldDecodeError::new("typed CBOR decode failed: text too large"))?;
    let next_cursor = payload_start
        .checked_add(text_len)
        .ok_or_else(|| FieldDecodeError::new("typed CBOR decode failed: text length overflow"))?;

    Ok((text, next_cursor))
}

// Check whether one self-contained CBOR text scalar matches a known ASCII
// literal without routing through the general field-name text helper.
pub(super) fn cbor_text_literal_eq(
    raw_bytes: &[u8],
    literal: &[u8],
) -> Result<bool, FieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: truncated text scalar",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after text scalar",
        ));
    }
    if major != CBOR_MAJOR_TEXT_STRING {
        return Ok(false);
    }

    let Ok(text_len) = usize::try_from(argument) else {
        return Ok(false);
    };
    if text_len != literal.len() {
        return Ok(false);
    }

    let payload_end = payload_start
        .checked_add(text_len)
        .ok_or_else(|| FieldDecodeError::new("typed CBOR decode failed: text length overflow"))?;
    let payload = raw_bytes
        .get(payload_start..payload_end)
        .ok_or_else(|| FieldDecodeError::new("typed CBOR decode failed: truncated text payload"))?;

    Ok(payload == literal)
}

// Parse one externally tagged variant envelope as either a unit variant name
// or a single payload-bearing variant entry.
pub(super) fn parse_tagged_variant_payload_bytes<'a>(
    raw_bytes: &'a [u8],
    truncated_label: &'static str,
    unit_or_payload_label: &'static str,
    one_entry_map_label: &'static str,
    trailing_label: &'static str,
) -> Result<(&'a str, Option<&'a [u8]>), FieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(truncated_label));
    };

    match major {
        CBOR_MAJOR_TEXT_STRING => {
            let variant = decode_text_scalar_bytes(raw_bytes, argument, cursor)?;
            let text_len = usize::try_from(argument)
                .map_err(|_| FieldDecodeError::new("typed CBOR decode failed: text too large"))?;
            cursor = cursor.checked_add(text_len).ok_or_else(|| {
                FieldDecodeError::new("typed CBOR decode failed: text length overflow")
            })?;
            if cursor != raw_bytes.len() {
                return Err(FieldDecodeError::new(trailing_label));
            }

            Ok((variant, None))
        }
        CBOR_MAJOR_MAP => {
            if argument != TAGGED_VARIANT_ENTRY_COUNT {
                return Err(FieldDecodeError::new(one_entry_map_label));
            }

            let (variant, next_cursor) = parse_text_scalar_at(raw_bytes, cursor)?;
            cursor = next_cursor;
            let payload_start = cursor;
            cursor = skip_cbor_value(raw_bytes, cursor)?;
            if cursor != raw_bytes.len() {
                return Err(FieldDecodeError::new(trailing_label));
            }

            Ok((variant, Some(&raw_bytes[payload_start..cursor])))
        }
        _ => Err(FieldDecodeError::new(unit_or_payload_label)),
    }
}

// Decode one definite-length CBOR text payload from the enclosing field bytes.
pub(super) fn decode_text_scalar_bytes(
    bytes: &[u8],
    argument: u64,
    payload_start: usize,
) -> Result<&str, FieldDecodeError> {
    let text_len = usize::try_from(argument)
        .map_err(|_| FieldDecodeError::new("typed CBOR decode failed: text too large"))?;
    let payload_end = payload_start
        .checked_add(text_len)
        .ok_or_else(|| FieldDecodeError::new("typed CBOR decode failed: text length overflow"))?;
    let payload = bytes
        .get(payload_start..payload_end)
        .ok_or_else(|| FieldDecodeError::new("typed CBOR decode failed: truncated text payload"))?;

    str::from_utf8(payload)
        .map_err(|_| FieldDecodeError::new("typed CBOR decode failed: non-utf8 text string"))
}

// Decode one raw payload slice from a definite-length CBOR byte string.
pub(super) fn payload_bytes<'a>(
    raw_bytes: &'a [u8],
    argument: u64,
    payload_start: usize,
    expected: &'static str,
) -> Result<&'a [u8], FieldDecodeError> {
    let payload_len = usize::try_from(argument).map_err(|_| {
        FieldDecodeError::new(format!("typed CBOR decode failed: {expected} too large"))
    })?;
    let payload_end = payload_start.checked_add(payload_len).ok_or_else(|| {
        FieldDecodeError::new(format!(
            "typed CBOR decode failed: {expected} length overflow"
        ))
    })?;

    raw_bytes.get(payload_start..payload_end).ok_or_else(|| {
        FieldDecodeError::new(format!(
            "typed CBOR decode failed: truncated {expected} payload"
        ))
    })
}

// Decode one CBOR major-type integer into a signed host integer.
pub(super) fn decode_cbor_integer(major: u8, argument: u64) -> Result<i128, FieldDecodeError> {
    match major {
        CBOR_MAJOR_UNSIGNED_INT => Ok(i128::from(argument)),
        CBOR_MAJOR_NEGATIVE_INT => Ok(-1 - i128::from(argument)),
        _ => Err(FieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected an integer",
        )),
    }
}

// Decode one CBOR float payload from the already-parsed head and payload span.
pub(super) fn decode_cbor_float(
    raw_bytes: &[u8],
    argument: u64,
    payload_start: usize,
) -> Result<f64, FieldDecodeError> {
    match argument {
        CBOR_FLOAT32_ARGUMENT => {
            let payload: [u8; CBOR_U32_WIDTH] =
                payload_bytes(raw_bytes, CBOR_U32_WIDTH as u64, payload_start, "float")?
                    .try_into()
                    .map_err(|_| {
                        FieldDecodeError::new(
                            "typed CBOR decode failed: expected four-byte float payload",
                        )
                    })?;
            Ok(f64::from(f32::from_be_bytes(payload)))
        }
        CBOR_FLOAT64_ARGUMENT => {
            let payload: [u8; CBOR_U64_WIDTH] =
                payload_bytes(raw_bytes, CBOR_U64_WIDTH as u64, payload_start, "float")?
                    .try_into()
                    .map_err(|_| {
                        FieldDecodeError::new(
                            "typed CBOR decode failed: expected eight-byte float payload",
                        )
                    })?;
            Ok(f64::from_be_bytes(payload))
        }
        _ => Err(FieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected a float",
        )),
    }
}
