//! Module: data::structural_field::cbor
//! Responsibility: low-level bounded CBOR parsing and raw-slice walkers for structural field decode.
//! Does not own: field semantics, runtime `Value` reconstruction, or storage-key policy.
//! Boundary: sibling structural-field modules call into this file when they need raw CBOR traversal without serde.

use crate::db::data::structural_field::StructuralFieldDecodeError;
use std::str;

// Alias the callback shape for raw CBOR array walkers.
type ArrayItemDecodeFn = unsafe fn(&[u8], *mut ()) -> Result<(), StructuralFieldDecodeError>;

// Alias the callback shape for raw CBOR map walkers.
type MapEntryDecodeFn = unsafe fn(&[u8], &[u8], *mut ()) -> Result<(), StructuralFieldDecodeError>;

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
) -> Result<(), StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    if major != 4 {
        return Err(StructuralFieldDecodeError::new(shape_label));
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
        return Err(StructuralFieldDecodeError::new(trailing_label));
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
) -> Result<(), StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    if major != 5 {
        return Err(StructuralFieldDecodeError::new(shape_label));
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
        return Err(StructuralFieldDecodeError::new(trailing_label));
    }

    Ok(())
}

// Parse one bounded CBOR container length into a host `usize`.
pub(super) fn bounded_cbor_len(
    argument: u64,
    label: &'static str,
) -> Result<usize, StructuralFieldDecodeError> {
    usize::try_from(argument).map_err(|_| StructuralFieldDecodeError::new(label))
}

// Parse one tagged CBOR head into `(major, argument, payload_start)`.
pub(super) fn parse_tagged_cbor_head(
    bytes: &[u8],
    mut cursor: usize,
) -> Result<Option<(u8, u64, usize)>, StructuralFieldDecodeError> {
    let Some((mut major, mut argument, mut next_cursor)) = parse_cbor_head(bytes, cursor)? else {
        return Ok(None);
    };

    while major == 6 {
        cursor = next_cursor;
        let Some((inner_major, inner_argument, inner_next_cursor)) =
            parse_cbor_head(bytes, cursor)?
        else {
            return Err(StructuralFieldDecodeError::new(
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
) -> Result<Option<(u8, u64, usize)>, StructuralFieldDecodeError> {
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
                StructuralFieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 1;

            u64::from(value)
        }
        25 => {
            let payload = bytes.get(next_cursor..next_cursor + 2).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 2;

            u64::from(u16::from_be_bytes([payload[0], payload[1]]))
        }
        26 => {
            let payload = bytes.get(next_cursor..next_cursor + 4).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 4;

            u64::from(u32::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ]))
        }
        27 => {
            let payload = bytes.get(next_cursor..next_cursor + 8).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 8;

            u64::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
                payload[7],
            ])
        }
        31 => {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: indefinite-length CBOR is unsupported",
            ));
        }
        _ => {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: invalid CBOR additional info",
            ));
        }
    };

    Ok(Some((major, argument, next_cursor)))
}

// Skip one tagged CBOR value without rebuilding a `CborValue`.
pub(super) fn skip_cbor_value(
    bytes: &[u8],
    cursor: usize,
) -> Result<usize, StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(bytes, cursor)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };

    match major {
        0 | 1 | 7 => Ok(cursor),
        2 | 3 => {
            let len = usize::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: CBOR scalar too large")
            })?;
            cursor = cursor.checked_add(len).ok_or_else(|| {
                StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: CBOR scalar length overflow",
                )
            })?;
            if cursor > bytes.len() {
                return Err(StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: truncated CBOR scalar payload",
                ));
            }

            Ok(cursor)
        }
        4 => {
            let len = usize::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: CBOR array too large")
            })?;
            for _ in 0..len {
                cursor = skip_cbor_value(bytes, cursor)?;
            }

            Ok(cursor)
        }
        5 => {
            let len = usize::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: CBOR map too large")
            })?;
            for _ in 0..len {
                cursor = skip_cbor_value(bytes, cursor)?;
                cursor = skip_cbor_value(bytes, cursor)?;
            }

            Ok(cursor)
        }
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: unsupported CBOR major type",
        )),
    }
}

// Parse one tagged CBOR text scalar in place.
pub(super) fn parse_text_scalar_at(
    bytes: &[u8],
    cursor: usize,
) -> Result<(&str, usize), StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(bytes, cursor)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: missing text scalar",
        ));
    };
    if major != 3 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: expected a text string",
        ));
    }

    let text = decode_text_scalar_bytes(bytes, argument, payload_start)?;
    let text_len = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("typed CBOR decode failed: text too large"))?;
    let next_cursor = payload_start.checked_add(text_len).ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: text length overflow")
    })?;

    Ok((text, next_cursor))
}

// Check whether one self-contained CBOR text scalar matches a known ASCII
// literal without routing through the general field-name text helper.
pub(super) fn cbor_text_literal_eq(
    raw_bytes: &[u8],
    literal: &[u8],
) -> Result<bool, StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated text scalar",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after text scalar",
        ));
    }
    if major != 3 {
        return Ok(false);
    }

    let Ok(text_len) = usize::try_from(argument) else {
        return Ok(false);
    };
    if text_len != literal.len() {
        return Ok(false);
    }

    let payload_end = payload_start.checked_add(text_len).ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: text length overflow")
    })?;
    let payload = raw_bytes.get(payload_start..payload_end).ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: truncated text payload")
    })?;

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
) -> Result<(&'a str, Option<&'a [u8]>), StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(truncated_label));
    };

    match major {
        3 => {
            let variant = decode_text_scalar_bytes(raw_bytes, argument, cursor)?;
            let text_len = usize::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: text too large")
            })?;
            cursor = cursor.checked_add(text_len).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: text length overflow")
            })?;
            if cursor != raw_bytes.len() {
                return Err(StructuralFieldDecodeError::new(trailing_label));
            }

            Ok((variant, None))
        }
        5 => {
            if argument != 1 {
                return Err(StructuralFieldDecodeError::new(one_entry_map_label));
            }

            let (variant, next_cursor) = parse_text_scalar_at(raw_bytes, cursor)?;
            cursor = next_cursor;
            let payload_start = cursor;
            cursor = skip_cbor_value(raw_bytes, cursor)?;
            if cursor != raw_bytes.len() {
                return Err(StructuralFieldDecodeError::new(trailing_label));
            }

            Ok((variant, Some(&raw_bytes[payload_start..cursor])))
        }
        _ => Err(StructuralFieldDecodeError::new(unit_or_payload_label)),
    }
}

// Decode one definite-length CBOR text payload from the enclosing field bytes.
pub(super) fn decode_text_scalar_bytes(
    bytes: &[u8],
    argument: u64,
    payload_start: usize,
) -> Result<&str, StructuralFieldDecodeError> {
    let text_len = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("typed CBOR decode failed: text too large"))?;
    let payload_end = payload_start.checked_add(text_len).ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: text length overflow")
    })?;
    let payload = bytes.get(payload_start..payload_end).ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: truncated text payload")
    })?;

    str::from_utf8(payload).map_err(|_| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: non-utf8 text string")
    })
}

// Decode one raw payload slice from a definite-length CBOR byte string.
pub(super) fn payload_bytes<'a>(
    raw_bytes: &'a [u8],
    argument: u64,
    payload_start: usize,
    expected: &'static str,
) -> Result<&'a [u8], StructuralFieldDecodeError> {
    let payload_len = usize::try_from(argument).map_err(|_| {
        StructuralFieldDecodeError::new(format!("typed CBOR decode failed: {expected} too large"))
    })?;
    let payload_end = payload_start.checked_add(payload_len).ok_or_else(|| {
        StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: {expected} length overflow"
        ))
    })?;

    raw_bytes.get(payload_start..payload_end).ok_or_else(|| {
        StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: truncated {expected} payload"
        ))
    })
}

// Decode one CBOR major-type integer into a signed host integer.
pub(super) fn decode_cbor_integer(
    major: u8,
    argument: u64,
) -> Result<i128, StructuralFieldDecodeError> {
    match major {
        0 => Ok(i128::from(argument)),
        1 => Ok(-1 - i128::from(argument)),
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected an integer",
        )),
    }
}

// Decode one CBOR float payload from the already-parsed head and payload span.
pub(super) fn decode_cbor_float(
    raw_bytes: &[u8],
    argument: u64,
    payload_start: usize,
) -> Result<f64, StructuralFieldDecodeError> {
    match argument {
        26 => {
            let payload: [u8; 4] = payload_bytes(raw_bytes, 4, payload_start, "float")?
                .try_into()
                .map_err(|_| {
                    StructuralFieldDecodeError::new(
                        "typed CBOR decode failed: expected four-byte float payload",
                    )
                })?;
            Ok(f64::from(f32::from_be_bytes(payload)))
        }
        27 => {
            let payload: [u8; 8] = payload_bytes(raw_bytes, 8, payload_start, "float")?
                .try_into()
                .map_err(|_| {
                    StructuralFieldDecodeError::new(
                        "typed CBOR decode failed: expected eight-byte float payload",
                    )
                })?;
            Ok(f64::from_be_bytes(payload))
        }
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected a float",
        )),
    }
}
