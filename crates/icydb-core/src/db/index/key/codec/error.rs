//! Module: index::key::codec::error
//! Responsibility: stable corruption reason strings for raw index-key decode.
//! Does not own: error class mapping.
//! Boundary: referenced by codec decode helpers and tests.

pub(super) const ERR_INVALID_KEY_KIND: &str = "corrupted IndexKey: invalid key kind";
pub(super) const ERR_INVALID_SIZE: &str = "corrupted IndexKey: invalid size";
pub(super) const ERR_INVALID_INDEX_NAME_BYTES: &str = "corrupted IndexKey: invalid IndexName bytes";
pub(super) const ERR_INVALID_INDEX_LENGTH: &str = "corrupted IndexKey: invalid index length";
pub(super) const ERR_TRAILING_BYTES: &str = "corrupted IndexKey: trailing bytes";
pub(super) const ERR_TRUNCATED_KEY: &str = "corrupted IndexKey: truncated key";
pub(super) const ERR_ZERO_LENGTH_SEGMENT: &str = "corrupted IndexKey: zero-length segment";
pub(super) const ERR_OVERLONG_SEGMENT: &str = "corrupted IndexKey: overlong segment";
pub(super) const ERR_SEGMENT_OVERFLOW: &str = "corrupted IndexKey: segment overflow";
