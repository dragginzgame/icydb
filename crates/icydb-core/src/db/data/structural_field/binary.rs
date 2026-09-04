//! Module: data::structural_field::binary
//! Responsibility: bounded Structural Binary v1 framing, shared payload codecs, and raw-slice walkers.
//! Does not own: field semantics, runtime `Value` reconstruction, or row-level policy.
//! Boundary: structural-field lanes provide admitted semantic parts and consume the
//! canonical version-1 payload shapes owned here.

use crate::db::data::structural_field::{
    FieldDecodeError,
    primitive::{
        decode_i64_payload_bytes, decode_u64_payload_bytes, encode_f32_payload_bytes,
        encode_f64_payload_bytes, encode_i64_payload_bytes, encode_u64_payload_bytes,
    },
};
use num_bigint::{BigInt, BigUint, Sign as BigIntSign};

pub(super) const TAG_NULL: u8 = 0x00;
pub(super) const TAG_UNIT: u8 = 0x01;
pub(super) const TAG_FALSE: u8 = 0x02;
pub(super) const TAG_TRUE: u8 = 0x03;
pub(super) const TAG_NAT64: u8 = 0x10;
pub(super) const TAG_INT64: u8 = 0x11;
pub(super) const TAG_TEXT: u8 = 0x12;
pub(super) const TAG_BYTES: u8 = 0x13;
pub(super) const TAG_FLOAT32: u8 = 0x14;
pub(super) const TAG_FLOAT64: u8 = 0x15;
pub(super) const TAG_LIST: u8 = 0x20;
pub(super) const TAG_MAP: u8 = 0x21;

const WORD32_LEN: usize = 4;
const WORD64_LEN: usize = 8;
const WORD32_LEN_U32: u32 = 4;
const WORD64_LEN_U32: u32 = 8;
const MAX_STRUCTURAL_BINARY_SKIP_DEPTH: usize = 64;

/// Append one tag-only Structural Binary v1 value.
pub(super) fn push_binary_tag(out: &mut Vec<u8>, tag: u8) {
    out.push(tag);
}

/// Append one `null` Structural Binary v1 value.
pub(super) fn push_binary_null(out: &mut Vec<u8>) {
    push_binary_tag(out, TAG_NULL);
}

/// Append one `unit` Structural Binary v1 value.
pub(super) fn push_binary_unit(out: &mut Vec<u8>) {
    push_binary_tag(out, TAG_UNIT);
}

/// Append one `bool` Structural Binary v1 value.
pub(super) fn push_binary_bool(out: &mut Vec<u8>, value: bool) {
    push_binary_tag(out, if value { TAG_TRUE } else { TAG_FALSE });
}

/// Append one fixed-width `u64` Structural Binary v1 value.
pub(super) fn push_binary_nat64(out: &mut Vec<u8>, value: u64) {
    out.push(TAG_NAT64);
    out.extend_from_slice(&encode_u64_payload_bytes(value));
}

/// Append one fixed-width `i64` Structural Binary v1 value.
pub(super) fn push_binary_int64(out: &mut Vec<u8>, value: i64) {
    out.push(TAG_INT64);
    out.extend_from_slice(&encode_i64_payload_bytes(value));
}

/// Append one fixed-width `f32` Structural Binary v1 value.
pub(super) fn push_binary_float32(out: &mut Vec<u8>, value: f32) {
    out.push(TAG_FLOAT32);
    out.extend_from_slice(&encode_f32_payload_bytes(value));
}

/// Append one fixed-width `f64` Structural Binary v1 value.
pub(super) fn push_binary_float64(out: &mut Vec<u8>, value: f64) {
    out.push(TAG_FLOAT64);
    out.extend_from_slice(&encode_f64_payload_bytes(value));
}

/// Append one length-prefixed UTF-8 string Structural Binary v1 value.
pub(super) fn push_binary_text(out: &mut Vec<u8>, value: &str) {
    out.push(TAG_TEXT);
    let len = structural_binary_len(value.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

/// Append one length-prefixed raw-byte Structural Binary v1 value.
pub(super) fn push_binary_bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.push(TAG_BYTES);
    let len = structural_binary_len(value.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
}

/// Append one list header with the given item count.
pub(super) fn push_binary_list_len(out: &mut Vec<u8>, len: usize) {
    out.push(TAG_LIST);
    let len = structural_binary_len(len);
    out.extend_from_slice(&len.to_be_bytes());
}

/// Append one map header with the given entry count.
pub(super) fn push_binary_map_len(out: &mut Vec<u8>, len: usize) {
    out.push(TAG_MAP);
    let len = structural_binary_len(len);
    out.extend_from_slice(&len.to_be_bytes());
}

/// Append the canonical Structural Binary decimal `(mantissa, scale)` payload.
pub(super) fn push_binary_decimal_payload(out: &mut Vec<u8>, mantissa: i128, scale: u32) {
    push_binary_list_len(out, 2);
    push_binary_bytes(out, &mantissa.to_be_bytes());
    push_binary_nat64(out, u64::from(scale));
}

/// Append the canonical Structural Binary signed big-integer payload.
pub(super) fn push_binary_int_big_payload(out: &mut Vec<u8>, is_negative: bool, digits: &[u32]) {
    let sign = if digits.is_empty() {
        0
    } else if is_negative {
        -1
    } else {
        1
    };

    push_binary_list_len(out, 2);
    push_binary_int64(out, sign);
    push_binary_u32_digit_list(out, digits);
}

/// Append the canonical Structural Binary unsigned big-integer payload.
pub(super) fn push_binary_nat_big_payload(out: &mut Vec<u8>, digits: &[u32]) {
    push_binary_u32_digit_list(out, digits);
}

// Emit one canonical big-integer magnitude limb sequence.
fn push_binary_u32_digit_list(out: &mut Vec<u8>, digits: &[u32]) {
    push_binary_list_len(out, digits.len());
    for digit in digits {
        push_binary_nat64(out, u64::from(*digit));
    }
}

// Structural values are admitted under smaller runtime bounds before reaching
// this framing layer. Saturating the host-only impossible overflow keeps this
// mechanical encoder non-panicking without inventing a second admission rule.
fn structural_binary_len(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

type ListItemVisitor<'a> = dyn FnMut(&[u8]) -> Result<(), FieldDecodeError> + 'a;
type MapEntryVisitor<'a> = dyn FnMut(&[u8], &[u8]) -> Result<(), FieldDecodeError> + 'a;

///
/// BinaryHead
///
/// BinaryHead captures one parsed Structural Binary v1 value head.
/// Higher layers use it to distinguish fixed-width scalar forms from
/// length-prefixed or recursively traversable forms without rebuilding a
/// generic tree.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BinaryHead {
    payload_offset: usize,
    tag: u8,
    len: u32,
}

/// One complete Structural Binary v1 root with its parsed head.
pub(super) struct CompleteBinaryValue<'a> {
    bytes: &'a [u8],
    tag: u8,
    len: u32,
}

impl<'a> CompleteBinaryValue<'a> {
    /// Parse and validate exactly one complete Structural Binary v1 root.
    #[inline]
    pub(super) fn parse(bytes: &'a [u8]) -> Result<Self, FieldDecodeError> {
        let value = Self::from_skip_bounded(bytes)?;
        let head = BinaryHead {
            tag: value.tag,
            len: value.len,
            payload_offset: value.payload_offset(),
        };
        if skip_parsed_binary_value(bytes, head, 0)? != bytes.len() {
            return Err(FieldDecodeError::new());
        }

        Ok(value)
    }

    /// Parse bytes whose complete boundary was already proven by skip traversal.
    #[inline]
    pub(super) fn from_skip_bounded(bytes: &'a [u8]) -> Result<Self, FieldDecodeError> {
        let (tag, len, _) = parse_binary_head(bytes, 0)?.ok_or_else(FieldDecodeError::new)?;

        Ok(Self { bytes, tag, len })
    }

    /// Return the complete validated root bytes.
    #[inline]
    pub(super) const fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Return the parsed root tag.
    #[inline]
    pub(super) const fn tag(&self) -> u8 {
        self.tag
    }

    /// Return the parsed root length field.
    #[inline]
    pub(super) const fn len(&self) -> u32 {
        self.len
    }

    /// Return the byte offset where the root payload starts.
    #[inline]
    pub(super) const fn payload_offset(&self) -> usize {
        match self.tag {
            TAG_TEXT | TAG_BYTES | TAG_LIST | TAG_MAP => 1 + WORD32_LEN,
            _ => 1,
        }
    }

    /// Return a scalar root's bounded payload bytes.
    #[inline]
    pub(super) fn scalar_payload(&self) -> Result<&'a [u8], FieldDecodeError> {
        payload_bytes(self.bytes, self.len, self.payload_offset())
    }
}

/// Decode one complete Structural Binary `null` payload.
pub(super) fn decode_binary_required_null(raw_bytes: &[u8]) -> Result<(), FieldDecodeError> {
    parse_required_binary_payload(raw_bytes, TAG_NULL, Some(0)).map(|_| ())
}

/// Decode one complete Structural Binary byte-string payload.
pub(super) fn decode_binary_required_bytes(raw_bytes: &[u8]) -> Result<&[u8], FieldDecodeError> {
    parse_required_binary_payload(raw_bytes, TAG_BYTES, None)?.scalar_payload()
}

/// Decode one complete Structural Binary signed 64-bit payload.
pub(super) fn decode_binary_required_i64(raw_bytes: &[u8]) -> Result<i64, FieldDecodeError> {
    decode_i64_payload_bytes(
        parse_required_binary_payload(raw_bytes, TAG_INT64, Some(8))?.scalar_payload()?,
    )
}

/// Decode one complete Structural Binary unsigned 64-bit payload.
pub(super) fn decode_binary_required_u64(raw_bytes: &[u8]) -> Result<u64, FieldDecodeError> {
    decode_u64_payload_bytes(
        parse_required_binary_payload(raw_bytes, TAG_NAT64, Some(8))?.scalar_payload()?,
    )
}

/// Decode the canonical Structural Binary decimal `(mantissa, scale)` payload.
pub(super) fn decode_binary_decimal_payload(
    raw_bytes: &[u8],
) -> Result<(i128, u32), FieldDecodeError> {
    let [mantissa, scale] = split_binary_tuple_2(raw_bytes)?;
    let mantissa: [u8; 16] = decode_binary_required_bytes(mantissa)?
        .try_into()
        .map_err(|_| FieldDecodeError::new())?;
    let scale =
        u32::try_from(decode_binary_required_u64(scale)?).map_err(|_| FieldDecodeError::new())?;

    Ok((i128::from_be_bytes(mantissa), scale))
}

/// Decode the canonical Structural Binary signed big-integer payload.
pub(super) fn decode_binary_int_big_payload(raw_bytes: &[u8]) -> Result<BigInt, FieldDecodeError> {
    let [sign, magnitude] = split_binary_tuple_2(raw_bytes)?;
    let sign = decode_binary_big_integer_sign(sign)?;
    let magnitude = decode_binary_big_integer_magnitude(magnitude)?;

    Ok(BigInt::from_biguint(sign, magnitude))
}

/// Decode the canonical Structural Binary unsigned big-integer payload.
pub(super) fn decode_binary_nat_big_payload(raw_bytes: &[u8]) -> Result<BigUint, FieldDecodeError> {
    decode_binary_big_integer_magnitude(raw_bytes)
}

// Validate one complete Structural Binary scalar and return its parsed root.
fn parse_required_binary_payload(
    raw_bytes: &[u8],
    expected_tag: u8,
    expected_len: Option<u32>,
) -> Result<CompleteBinaryValue<'_>, FieldDecodeError> {
    let root = CompleteBinaryValue::parse(raw_bytes)?;
    if root.tag() != expected_tag || expected_len.is_some_and(|len| root.len() != len) {
        return Err(FieldDecodeError::new());
    }

    Ok(root)
}

// Split one fixed two-item Structural Binary tuple without allocating.
fn split_binary_tuple_2(raw_bytes: &[u8]) -> Result<[&[u8]; 2], FieldDecodeError> {
    let Some((tag, len, mut cursor)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_LIST || len != 2 {
        return Err(FieldDecodeError::new());
    }

    let first_start = cursor;
    cursor = skip_binary_value(raw_bytes, cursor)?;
    let first = &raw_bytes[first_start..cursor];

    let second_start = cursor;
    cursor = skip_binary_value(raw_bytes, cursor)?;
    let second = &raw_bytes[second_start..cursor];

    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok([first, second])
}

// Decode one signed big-integer sign payload serialized as -1, 0, or 1.
fn decode_binary_big_integer_sign(raw_bytes: &[u8]) -> Result<BigIntSign, FieldDecodeError> {
    match decode_binary_required_i64(raw_bytes)? {
        -1 => Ok(BigIntSign::Minus),
        0 => Ok(BigIntSign::NoSign),
        1 => Ok(BigIntSign::Plus),
        _ => Err(FieldDecodeError::new()),
    }
}

// Decode one canonical sequence of base-2^32 big-integer magnitude limbs.
fn decode_binary_big_integer_magnitude(raw_bytes: &[u8]) -> Result<BigUint, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new());
    }

    let mut cursor = payload_start;
    let mut digits = Vec::new();
    for _ in 0..len {
        digits.try_reserve(1).map_err(|_| FieldDecodeError::new())?;
        let digit_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        let digit = u32::try_from(decode_binary_required_u64(&raw_bytes[digit_start..cursor])?)
            .map_err(|_| FieldDecodeError::new())?;
        digits.push(digit);
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(BigUint::new(digits))
}

// Parse one Structural Binary v1 head from the provided byte offset.
pub(super) fn parse_binary_head(
    bytes: &[u8],
    offset: usize,
) -> Result<Option<(u8, u32, usize)>, FieldDecodeError> {
    let Some(&tag) = bytes.get(offset) else {
        return Ok(None);
    };
    let payload_offset = offset.checked_add(1).ok_or_else(FieldDecodeError::new)?;

    let len = match tag {
        TAG_NULL | TAG_UNIT | TAG_FALSE | TAG_TRUE => 0,
        TAG_NAT64 | TAG_INT64 | TAG_FLOAT64 => WORD64_LEN_U32,
        TAG_FLOAT32 => WORD32_LEN_U32,
        TAG_TEXT | TAG_BYTES | TAG_LIST | TAG_MAP => decode_u32(bytes, payload_offset)?,
        _ => {
            return Err(FieldDecodeError::new());
        }
    };

    let payload_offset = match tag {
        TAG_NULL | TAG_UNIT | TAG_FALSE | TAG_TRUE | TAG_NAT64 | TAG_INT64 | TAG_FLOAT32
        | TAG_FLOAT64 => payload_offset,
        TAG_TEXT | TAG_BYTES | TAG_LIST | TAG_MAP => payload_offset
            .checked_add(WORD32_LEN)
            .ok_or_else(FieldDecodeError::new)?,
        _ => return Err(FieldDecodeError::new()),
    };

    Ok(Some((tag, len, payload_offset)))
}

// Skip one self-contained Structural Binary v1 value without decoding it.
pub(super) fn skip_binary_value(bytes: &[u8], offset: usize) -> Result<usize, FieldDecodeError> {
    skip_binary_value_at_depth(bytes, offset, 0)
}

fn skip_binary_value_at_depth(
    bytes: &[u8],
    offset: usize,
    depth: usize,
) -> Result<usize, FieldDecodeError> {
    let Some((tag, len, payload_offset)) = parse_binary_head(bytes, offset)? else {
        return Err(FieldDecodeError::new());
    };
    let head = BinaryHead {
        payload_offset,
        tag,
        len,
    };

    skip_parsed_binary_value(bytes, head, depth)
}

// Skip one value whose head is already parsed. Complete-root consumers use
// this path so validation never reparses the root before decoding it.
fn skip_parsed_binary_value(
    bytes: &[u8],
    head: BinaryHead,
    depth: usize,
) -> Result<usize, FieldDecodeError> {
    if depth >= MAX_STRUCTURAL_BINARY_SKIP_DEPTH {
        return Err(FieldDecodeError::new());
    }
    let depth = depth.saturating_add(1);

    match head.tag {
        TAG_NULL | TAG_UNIT | TAG_FALSE | TAG_TRUE => Ok(head.payload_offset),
        TAG_FLOAT32 => checked_advance(bytes, head.payload_offset, WORD32_LEN),
        TAG_NAT64 | TAG_INT64 | TAG_FLOAT64 => {
            checked_advance(bytes, head.payload_offset, WORD64_LEN)
        }
        TAG_TEXT | TAG_BYTES => checked_advance(
            bytes,
            head.payload_offset,
            usize::try_from(head.len).map_err(|_| FieldDecodeError::new())?,
        ),
        TAG_LIST => skip_list_payload(bytes, head, depth),
        TAG_MAP => skip_map_payload(bytes, head, depth),
        _ => Err(FieldDecodeError::new()),
    }
}

// Walk one Structural Binary v1 list and yield each raw item slice to the caller.
pub(super) fn walk_binary_list_items(
    raw_bytes: &[u8],
    on_item: &mut ListItemVisitor<'_>,
) -> Result<(), FieldDecodeError> {
    let Some((tag, len, payload_offset)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new());
    }
    let head = BinaryHead {
        payload_offset,
        tag,
        len,
    };

    let mut cursor = head.payload_offset;
    for _ in 0..head.len {
        let item_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        on_item(&raw_bytes[item_start..cursor])?;
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(())
}

// Walk one Structural Binary v1 map and yield each raw key/value slice pair to the caller.
pub(super) fn walk_binary_map_entries(
    raw_bytes: &[u8],
    on_entry: &mut MapEntryVisitor<'_>,
) -> Result<(), FieldDecodeError> {
    let Some((tag, len, payload_offset)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new());
    }
    let head = BinaryHead {
        payload_offset,
        tag,
        len,
    };

    let mut cursor = head.payload_offset;
    for _ in 0..head.len {
        let key_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        let value_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        on_entry(
            &raw_bytes[key_start..value_start],
            &raw_bytes[value_start..cursor],
        )?;
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(())
}

// Decode one big-endian u32 from the requested byte offset.
fn decode_u32(bytes: &[u8], offset: usize) -> Result<u32, FieldDecodeError> {
    let slice = bytes
        .get(offset..offset + WORD32_LEN)
        .ok_or_else(FieldDecodeError::new)?;

    Ok(u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

// Advance one cursor by the requested number of bytes and prove the resulting
// slice still fits inside the provided buffer.
fn checked_advance(bytes: &[u8], offset: usize, len: usize) -> Result<usize, FieldDecodeError> {
    let end = offset.checked_add(len).ok_or_else(FieldDecodeError::new)?;
    if end > bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(end)
}

// Skip one list payload by recursively skipping its declared item count.
fn skip_list_payload(
    bytes: &[u8],
    head: BinaryHead,
    depth: usize,
) -> Result<usize, FieldDecodeError> {
    let mut cursor = head.payload_offset;
    for _ in 0..head.len {
        cursor = skip_binary_value_at_depth(bytes, cursor, depth)?;
    }

    Ok(cursor)
}

// Skip one map payload by recursively skipping its declared key/value entry pairs.
fn skip_map_payload(
    bytes: &[u8],
    head: BinaryHead,
    depth: usize,
) -> Result<usize, FieldDecodeError> {
    let mut cursor = head.payload_offset;
    for _ in 0..head.len {
        cursor = skip_binary_value_at_depth(bytes, cursor, depth)?;
        cursor = skip_binary_value_at_depth(bytes, cursor, depth)?;
    }

    Ok(cursor)
}

// Decode one definite-length Structural Binary text payload from the enclosing field bytes.
pub(super) fn decode_text_scalar_bytes(
    bytes: &[u8],
    len: u32,
    payload_start: usize,
) -> Result<&str, FieldDecodeError> {
    let text_len = usize::try_from(len).map_err(|_| FieldDecodeError::new())?;
    let payload_end = payload_start
        .checked_add(text_len)
        .ok_or_else(FieldDecodeError::new)?;
    let payload = bytes
        .get(payload_start..payload_end)
        .ok_or_else(FieldDecodeError::new)?;

    std::str::from_utf8(payload).map_err(|_| FieldDecodeError::new())
}

// Decode one raw payload slice from a definite-length Structural Binary byte payload.
pub(super) fn payload_bytes(
    raw_bytes: &[u8],
    len: u32,
    payload_start: usize,
) -> Result<&[u8], FieldDecodeError> {
    let payload_len = usize::try_from(len).map_err(|_| FieldDecodeError::new())?;
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or_else(FieldDecodeError::new)?;

    raw_bytes
        .get(payload_start..payload_end)
        .ok_or_else(FieldDecodeError::new)
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
