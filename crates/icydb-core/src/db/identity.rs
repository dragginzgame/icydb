#![expect(clippy::cast_possible_truncation)]
//! Identity invariants and construction.
//!
//! Invariants:
//! - Identities are ASCII, non-empty, and bounded by MAX_* limits.
//! - All construction paths validate invariants.
//! - Stored byte representation is canonical and order-preserving.

use crate::MAX_INDEX_FIELDS;
use std::{
    cmp::Ordering,
    fmt::{self, Display},
};
use thiserror::Error as ThisError;

///
/// Constants
///

pub(super) const MAX_ENTITY_NAME_LEN: usize = 64;
pub(super) const MAX_INDEX_FIELD_NAME_LEN: usize = 64;
pub(super) const MAX_INDEX_NAME_LEN: usize =
    MAX_ENTITY_NAME_LEN + (MAX_INDEX_FIELDS * (MAX_INDEX_FIELD_NAME_LEN + 1));

///
/// Decode errors (storage / corruption boundary)
///

#[derive(Debug, ThisError)]
pub enum IdentityDecodeError {
    #[error("invalid size")]
    InvalidSize,
    #[error("invalid length")]
    InvalidLength,
    #[error("non-ascii encoding")]
    NonAscii,
    #[error("non-zero padding")]
    NonZeroPadding,
}

///
/// EntityNameError
///

#[derive(Debug, ThisError)]
pub enum EntityNameError {
    #[error("entity name is empty")]
    Empty,

    #[error("entity name length {len} exceeds max {max}")]
    TooLong { len: usize, max: usize },

    #[error("entity name must be ASCII")]
    NonAscii,
}

///
/// IndexNameError
///

#[derive(Debug, ThisError)]
pub enum IndexNameError {
    #[error("index has {len} fields (max {max})")]
    TooManyFields { len: usize, max: usize },

    #[error("index field name '{field}' exceeds max length {max}")]
    FieldTooLong { field: String, max: usize },

    #[error("index field name '{field}' must be ASCII")]
    FieldNonAscii { field: String },

    #[error("index name length {len} exceeds max {max}")]
    TooLong { len: usize, max: usize },
}

///
/// EntityName
///

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct EntityName {
    len: u8,
    bytes: [u8; MAX_ENTITY_NAME_LEN],
}

impl EntityName {
    /// Fixed on-disk size in bytes (stable, protocol-level)
    pub const STORED_SIZE_BYTES: u64 = 1 + (MAX_ENTITY_NAME_LEN as u64);

    /// Fixed in-memory size (for buffers and arrays)
    pub const STORED_SIZE_USIZE: usize = Self::STORED_SIZE_BYTES as usize;

    pub fn try_from_str(name: &str) -> Result<Self, EntityNameError> {
        let bytes = name.as_bytes();
        let len = bytes.len();

        if len == 0 {
            return Err(EntityNameError::Empty);
        }
        if len > MAX_ENTITY_NAME_LEN {
            return Err(EntityNameError::TooLong {
                len,
                max: MAX_ENTITY_NAME_LEN,
            });
        }
        if !bytes.is_ascii() {
            return Err(EntityNameError::NonAscii);
        }

        let mut out = [0u8; MAX_ENTITY_NAME_LEN];
        out[..len].copy_from_slice(bytes);

        Ok(Self {
            len: len as u8,
            bytes: out,
        })
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len()]
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        // SAFETY:
        // Preconditions:
        // - Constructors (`try_from_str`) and decoders (`from_bytes`) reject
        //   non-ASCII inputs.
        // - Stored slices returned by `as_bytes` are within initialized bounds.
        //
        // Aliasing:
        // - This creates an immutable `&str` view over immutable bytes already
        //   owned by `self`; no mutable aliasing is introduced.
        //
        // What would break this:
        // - Any future constructor/decoder path that permits non-ASCII bytes.
        // - Any mutation of `bytes[..len]` bypassing validation guarantees.
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    #[must_use]
    pub fn to_bytes(self) -> [u8; Self::STORED_SIZE_USIZE] {
        let mut out = [0u8; Self::STORED_SIZE_USIZE];
        out[0] = self.len;
        out[1..].copy_from_slice(&self.bytes);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, IdentityDecodeError> {
        if bytes.len() != Self::STORED_SIZE_USIZE {
            return Err(IdentityDecodeError::InvalidSize);
        }

        let len = bytes[0] as usize;
        if len == 0 || len > MAX_ENTITY_NAME_LEN {
            return Err(IdentityDecodeError::InvalidLength);
        }
        if !bytes[1..=len].is_ascii() {
            return Err(IdentityDecodeError::NonAscii);
        }
        if bytes[1 + len..].iter().any(|&b| b != 0) {
            return Err(IdentityDecodeError::NonZeroPadding);
        }

        let mut name = [0u8; MAX_ENTITY_NAME_LEN];
        name.copy_from_slice(&bytes[1..]);

        Ok(Self {
            len: len as u8,
            bytes: name,
        })
    }

    #[must_use]
    pub const fn max_storable() -> Self {
        Self {
            len: MAX_ENTITY_NAME_LEN as u8,
            bytes: [b'z'; MAX_ENTITY_NAME_LEN],
        }
    }
}

impl Ord for EntityName {
    fn cmp(&self, other: &Self) -> Ordering {
        self.to_bytes().cmp(&other.to_bytes())
    }
}

impl PartialOrd for EntityName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for EntityName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for EntityName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EntityName({})", self.as_str())
    }
}

///
/// IndexName
///

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct IndexName {
    len: u16,
    bytes: [u8; MAX_INDEX_NAME_LEN],
}

impl IndexName {
    pub const STORED_SIZE_BYTES: u64 = 2 + (MAX_INDEX_NAME_LEN as u64);
    pub const STORED_SIZE_USIZE: usize = Self::STORED_SIZE_BYTES as usize;

    pub fn try_from_parts(entity: &EntityName, fields: &[&str]) -> Result<Self, IndexNameError> {
        if fields.len() > MAX_INDEX_FIELDS {
            return Err(IndexNameError::TooManyFields {
                len: fields.len(),
                max: MAX_INDEX_FIELDS,
            });
        }

        let mut total_len = entity.len();
        for field in fields {
            let field_len = field.len();
            if field_len > MAX_INDEX_FIELD_NAME_LEN {
                return Err(IndexNameError::FieldTooLong {
                    field: (*field).to_string(),
                    max: MAX_INDEX_FIELD_NAME_LEN,
                });
            }
            if !field.is_ascii() {
                return Err(IndexNameError::FieldNonAscii {
                    field: (*field).to_string(),
                });
            }
            total_len = total_len.saturating_add(1 + field_len);
        }

        if total_len > MAX_INDEX_NAME_LEN {
            return Err(IndexNameError::TooLong {
                len: total_len,
                max: MAX_INDEX_NAME_LEN,
            });
        }

        let mut out = [0u8; MAX_INDEX_NAME_LEN];
        let mut len = 0usize;

        Self::push_bytes(&mut out, &mut len, entity.as_bytes());
        for field in fields {
            Self::push_bytes(&mut out, &mut len, b"|");
            Self::push_bytes(&mut out, &mut len, field.as_bytes());
        }

        Ok(Self {
            len: len as u16,
            bytes: out,
        })
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        // SAFETY:
        // Preconditions:
        // - `try_from_parts` validates all segments are ASCII.
        // - `from_bytes` rejects non-ASCII payloads and malformed lengths.
        // - `as_bytes` returns only initialized bytes within `len`.
        //
        // Aliasing:
        // - We expose a shared `&str` over immutable storage; no mutable alias
        //   is created while the reference is live.
        //
        // What would break this:
        // - Accepting non-ASCII bytes in any construction/decoding path.
        // - Mutating the underlying `bytes` without re-validating invariants.
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    #[must_use]
    pub fn to_bytes(self) -> [u8; Self::STORED_SIZE_USIZE] {
        let mut out = [0u8; Self::STORED_SIZE_USIZE];
        out[..2].copy_from_slice(&self.len.to_be_bytes());
        out[2..].copy_from_slice(&self.bytes);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, IdentityDecodeError> {
        if bytes.len() != Self::STORED_SIZE_USIZE {
            return Err(IdentityDecodeError::InvalidSize);
        }

        let len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
        if len == 0 || len > MAX_INDEX_NAME_LEN {
            return Err(IdentityDecodeError::InvalidLength);
        }
        if !bytes[2..2 + len].is_ascii() {
            return Err(IdentityDecodeError::NonAscii);
        }
        if bytes[2 + len..].iter().any(|&b| b != 0) {
            return Err(IdentityDecodeError::NonZeroPadding);
        }

        let mut name = [0u8; MAX_INDEX_NAME_LEN];
        name.copy_from_slice(&bytes[2..]);

        Ok(Self {
            len: len as u16,
            bytes: name,
        })
    }

    fn push_bytes(out: &mut [u8; MAX_INDEX_NAME_LEN], len: &mut usize, bytes: &[u8]) {
        let end = *len + bytes.len();
        out[*len..end].copy_from_slice(bytes);
        *len = end;
    }

    #[must_use]
    pub const fn max_storable() -> Self {
        Self {
            len: MAX_INDEX_NAME_LEN as u16,
            bytes: [b'z'; MAX_INDEX_NAME_LEN],
        }
    }
}

impl Ord for IndexName {
    fn cmp(&self, other: &Self) -> Ordering {
        self.to_bytes().cmp(&other.to_bytes())
    }
}

impl PartialOrd for IndexName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for IndexName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IndexName({})", self.as_str())
    }
}

impl Display for IndexName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    const ENTITY_64: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const ENTITY_64_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const FIELD_64_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const FIELD_64_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const FIELD_64_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const FIELD_64_D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

    #[test]
    fn index_name_max_len_matches_limits() {
        let entity = EntityName::try_from_str(ENTITY_64).unwrap();
        let fields = [FIELD_64_A, FIELD_64_B, FIELD_64_C, FIELD_64_D];

        assert_eq!(entity.as_str().len(), MAX_ENTITY_NAME_LEN);
        for field in &fields {
            assert_eq!(field.len(), MAX_INDEX_FIELD_NAME_LEN);
        }
        assert_eq!(fields.len(), MAX_INDEX_FIELDS);

        let name = IndexName::try_from_parts(&entity, &fields).unwrap();
        assert_eq!(name.as_bytes().len(), MAX_INDEX_NAME_LEN);
    }

    #[test]
    fn index_name_max_size_roundtrip_and_ordering() {
        let entity_a = EntityName::try_from_str(ENTITY_64).unwrap();
        let entity_b = EntityName::try_from_str(ENTITY_64_B).unwrap();

        let fields_a = [FIELD_64_A, FIELD_64_A, FIELD_64_A, FIELD_64_A];
        let fields_b = [FIELD_64_B, FIELD_64_B, FIELD_64_B, FIELD_64_B];

        let idx_a = IndexName::try_from_parts(&entity_a, &fields_a).unwrap();
        let idx_b = IndexName::try_from_parts(&entity_b, &fields_b).unwrap();

        let decoded = IndexName::from_bytes(&idx_a.to_bytes()).unwrap();
        assert_eq!(idx_a, decoded);

        assert_eq!(idx_a.cmp(&idx_b), idx_a.to_bytes().cmp(&idx_b.to_bytes()));
    }

    #[test]
    fn rejects_too_many_index_fields() {
        let entity = EntityName::try_from_str("entity").unwrap();
        let fields = ["a", "b", "c", "d", "e"];

        let err = IndexName::try_from_parts(&entity, &fields).unwrap_err();
        assert!(matches!(err, IndexNameError::TooManyFields { .. }));
    }

    #[test]
    fn rejects_index_field_over_len() {
        let entity = EntityName::try_from_str("entity").unwrap();
        let long_field = "a".repeat(MAX_INDEX_FIELD_NAME_LEN + 1);

        let err = IndexName::try_from_parts(&entity, &[long_field.as_str()]).unwrap_err();
        assert!(matches!(err, IndexNameError::FieldTooLong { .. }));
    }

    #[test]
    fn entity_try_from_str_roundtrip() {
        let e = EntityName::try_from_str("user").unwrap();
        assert_eq!(e.len(), 4);
        assert_eq!(e.as_str(), "user");
    }

    #[test]
    fn entity_rejects_empty() {
        let err = EntityName::try_from_str("").unwrap_err();
        assert!(matches!(err, EntityNameError::Empty));
    }

    #[test]
    fn entity_rejects_len_over_max() {
        let s = "a".repeat(MAX_ENTITY_NAME_LEN + 1);
        let err = EntityName::try_from_str(&s).unwrap_err();
        assert!(matches!(err, EntityNameError::TooLong { .. }));
    }

    #[test]
    fn entity_rejects_non_ascii() {
        let err = EntityName::try_from_str("usér").unwrap_err();
        assert!(matches!(err, EntityNameError::NonAscii));
    }

    #[test]
    fn entity_storage_roundtrip() {
        let e = EntityName::try_from_str("entity_name").unwrap();
        let bytes = e.to_bytes();
        let decoded = EntityName::from_bytes(&bytes).unwrap();
        assert_eq!(e, decoded);
    }

    #[test]
    fn entity_max_storable_is_ascii_utf8() {
        let max = EntityName::max_storable();
        assert_eq!(max.len(), MAX_ENTITY_NAME_LEN);
        assert!(max.as_str().is_ascii());
    }

    #[test]
    fn entity_rejects_invalid_size() {
        let buf = vec![0u8; EntityName::STORED_SIZE_USIZE - 1];
        assert!(matches!(
            EntityName::from_bytes(&buf),
            Err(IdentityDecodeError::InvalidSize)
        ));
    }

    #[test]
    fn entity_rejects_len_over_max_from_bytes() {
        let mut buf = [0u8; EntityName::STORED_SIZE_USIZE];
        buf[0] = (MAX_ENTITY_NAME_LEN as u8).saturating_add(1);
        assert!(matches!(
            EntityName::from_bytes(&buf),
            Err(IdentityDecodeError::InvalidLength)
        ));
    }

    #[test]
    fn entity_rejects_non_ascii_from_bytes() {
        let mut buf = [0u8; EntityName::STORED_SIZE_USIZE];
        buf[0] = 1;
        buf[1] = 0xFF;
        assert!(matches!(
            EntityName::from_bytes(&buf),
            Err(IdentityDecodeError::NonAscii)
        ));
    }

    #[test]
    fn entity_rejects_non_zero_padding() {
        let e = EntityName::try_from_str("user").unwrap();
        let mut bytes = e.to_bytes();
        bytes[1 + e.len()] = b'x';

        assert!(matches!(
            EntityName::from_bytes(&bytes),
            Err(IdentityDecodeError::NonZeroPadding)
        ));
    }

    #[test]
    fn entity_ordering_matches_bytes() {
        let a = EntityName::try_from_str("abc").unwrap();
        let b = EntityName::try_from_str("abd").unwrap();
        let c = EntityName::try_from_str("abcx").unwrap();

        assert_eq!(a.cmp(&b), a.to_bytes().cmp(&b.to_bytes()));
        assert_eq!(a.cmp(&c), a.to_bytes().cmp(&c.to_bytes()));
    }

    #[test]
    fn index_single_field_format() {
        let entity = EntityName::try_from_str("user").unwrap();
        let idx = IndexName::try_from_parts(&entity, &["email"]).unwrap();

        assert_eq!(idx.as_str(), "user|email");
    }

    #[test]
    fn index_field_order_is_preserved() {
        let entity = EntityName::try_from_str("user").unwrap();
        let idx = IndexName::try_from_parts(&entity, &["a", "b", "c"]).unwrap();

        assert_eq!(idx.as_str(), "user|a|b|c");
    }

    #[test]
    fn index_storage_roundtrip() {
        let entity = EntityName::try_from_str("user").unwrap();
        let idx = IndexName::try_from_parts(&entity, &["a", "b"]).unwrap();

        let bytes = idx.to_bytes();
        let decoded = IndexName::from_bytes(&bytes).unwrap();
        assert_eq!(idx, decoded);
    }

    #[test]
    fn index_max_storable_is_ascii_utf8() {
        let max = IndexName::max_storable();
        assert_eq!(max.as_bytes().len(), MAX_INDEX_NAME_LEN);
        assert!(max.as_str().is_ascii());
    }

    #[test]
    fn index_rejects_non_ascii_from_bytes() {
        let mut buf = [0u8; IndexName::STORED_SIZE_USIZE];
        buf[..2].copy_from_slice(&1u16.to_be_bytes());
        buf[2] = 0xFF;

        assert!(matches!(
            IndexName::from_bytes(&buf),
            Err(IdentityDecodeError::NonAscii)
        ));
    }

    // ------------------------------------------------------------------
    // FUZZING (deterministic)
    // ------------------------------------------------------------------

    fn gen_ascii(seed: u64, max_len: usize) -> String {
        let len = (seed as usize % max_len).max(1);
        let mut out = String::with_capacity(len);

        let mut x = seed;
        for _ in 0..len {
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let c = b'a' + (x % 26) as u8;
            out.push(c as char);
        }

        out
    }

    #[test]
    fn fuzz_entity_name_roundtrip_and_ordering() {
        let mut prev: Option<EntityName> = None;

        for i in 1..=1_000u64 {
            let s = gen_ascii(i, MAX_ENTITY_NAME_LEN);
            let e = EntityName::try_from_str(&s).unwrap();

            let bytes = e.to_bytes();
            let decoded = EntityName::from_bytes(&bytes).unwrap();
            assert_eq!(e, decoded);

            if let Some(p) = prev {
                assert_eq!(p.cmp(&e), p.to_bytes().cmp(&e.to_bytes()));
            }

            prev = Some(e);
        }
    }

    #[test]
    fn fuzz_index_name_roundtrip_and_ordering() {
        let entity = EntityName::try_from_str("entity").unwrap();
        let mut prev: Option<IndexName> = None;

        for i in 1..=1_000u64 {
            let field_count = (i as usize % MAX_INDEX_FIELDS).max(1);

            let mut fields = Vec::with_capacity(field_count);
            for f in 0..field_count {
                let s = gen_ascii(i * 31 + f as u64, MAX_INDEX_FIELD_NAME_LEN);
                fields.push(s);
            }

            let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
            let idx = IndexName::try_from_parts(&entity, &field_refs).unwrap();

            let bytes = idx.to_bytes();
            let decoded = IndexName::from_bytes(&bytes).unwrap();
            assert_eq!(idx, decoded);

            if let Some(p) = prev {
                assert_eq!(p.cmp(&idx), p.to_bytes().cmp(&idx.to_bytes()));
            }

            prev = Some(idx);
        }
    }
}
