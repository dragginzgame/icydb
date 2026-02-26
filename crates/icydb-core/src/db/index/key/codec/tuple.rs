use crate::db::index::key::codec::{
    bounds::SEGMENT_LEN_SIZE,
    error::{
        ERR_OVERLONG_SEGMENT, ERR_SEGMENT_OVERFLOW, ERR_TRUNCATED_KEY, ERR_ZERO_LENGTH_SEGMENT,
    },
};
use std::cmp::Ordering;

#[expect(clippy::checked_conversions)]
pub(super) fn push_segment(bytes: &mut Vec<u8>, segment: &[u8]) {
    assert!(
        segment.len() <= u16::MAX as usize,
        "segment length overflowed u16 despite bounded invariants",
    );
    let len_u16 =
        u16::try_from(segment.len()).expect("segment length should fit in a u16 after assert");

    bytes.extend_from_slice(&len_u16.to_be_bytes());
    bytes.extend_from_slice(segment);
}

// Compare one raw segment the same way its length-prefixed bytes compare.
pub(super) fn compare_length_prefixed_segment(left: &[u8], right: &[u8]) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

// Compare encoded component segments under length-prefixed ordering semantics.
pub(super) fn compare_component_segments(left: &[Vec<u8>], right: &[Vec<u8>]) -> Ordering {
    for (left_segment, right_segment) in left.iter().zip(right.iter()) {
        let segment_order = compare_length_prefixed_segment(left_segment, right_segment);
        if segment_order != Ordering::Equal {
            return segment_order;
        }
    }

    Ordering::Equal
}

pub(super) fn read_segment<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    max_len: usize,
    _label: &str,
) -> Result<&'a [u8], &'static str> {
    if *offset + SEGMENT_LEN_SIZE > bytes.len() {
        return Err(ERR_TRUNCATED_KEY);
    }

    let mut len_buf = [0u8; SEGMENT_LEN_SIZE];
    len_buf.copy_from_slice(&bytes[*offset..*offset + SEGMENT_LEN_SIZE]);
    *offset += SEGMENT_LEN_SIZE;

    let len = u16::from_be_bytes(len_buf) as usize;
    if len == 0 {
        return Err(ERR_ZERO_LENGTH_SEGMENT);
    }
    if len > max_len {
        return Err(ERR_OVERLONG_SEGMENT);
    }

    let end = (*offset).checked_add(len).ok_or(ERR_SEGMENT_OVERFLOW)?;
    if end > bytes.len() {
        return Err(ERR_TRUNCATED_KEY);
    }

    let out = &bytes[*offset..end];
    *offset = end;

    Ok(out)
}
