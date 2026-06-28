//! Module: index::key::codec::tuple
//! Responsibility: length-prefixed tuple-segment encode/decode helpers.
//! Does not own: index-key semantic ordering policy.
//! Boundary: internal utility for codec framing.

use crate::db::index::key::codec::{
    bounds::SEGMENT_LEN_SIZE,
    error::{IndexKeyDecodeError, IndexKeyEncodeError},
};
use std::cmp::Ordering;

pub(super) fn push_segment(bytes: &mut Vec<u8>, segment: &[u8]) -> Result<(), IndexKeyEncodeError> {
    if segment.is_empty() {
        return Err(IndexKeyEncodeError::EmptySegment);
    }

    // Segment length is persisted as u16 by codec contract.
    let len_u16 = u16::try_from(segment.len()).map_err(|_| IndexKeyEncodeError::SegmentTooLarge)?;

    bytes.extend_from_slice(&len_u16.to_be_bytes());
    bytes.extend_from_slice(segment);

    Ok(())
}

/// Compare one decoded segment under canonical component ordering semantics.
pub(super) fn compare_segment_bytes(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
}

/// Compare encoded component segments under canonical component ordering semantics.
pub(super) fn compare_component_segments(left: &[Vec<u8>], right: &[Vec<u8>]) -> Ordering {
    for (left_segment, right_segment) in left.iter().zip(right.iter()) {
        let segment_order = compare_segment_bytes(left_segment, right_segment);
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
) -> Result<&'a [u8], IndexKeyDecodeError> {
    // Phase 1: decode segment length and enforce shape bounds.
    if *offset + SEGMENT_LEN_SIZE > bytes.len() {
        return Err(IndexKeyDecodeError::TruncatedKey);
    }

    let mut len_buf = [0u8; SEGMENT_LEN_SIZE];
    len_buf.copy_from_slice(&bytes[*offset..*offset + SEGMENT_LEN_SIZE]);
    *offset += SEGMENT_LEN_SIZE;

    let len = u16::from_be_bytes(len_buf) as usize;
    if len == 0 {
        return Err(IndexKeyDecodeError::ZeroLengthSegment);
    }
    if len > max_len {
        return Err(IndexKeyDecodeError::OverlongSegment);
    }

    let end = (*offset)
        .checked_add(len)
        .ok_or(IndexKeyDecodeError::SegmentOverflow)?;
    if end > bytes.len() {
        return Err(IndexKeyDecodeError::TruncatedKey);
    }

    // Phase 2: return the segment slice and advance decode cursor.
    let out = &bytes[*offset..end];
    *offset = end;

    Ok(out)
}
