#![expect(clippy::cast_possible_truncation)]
//! Module: identity
//! Responsibility: validated entity/index naming and stable byte ordering contracts.
//! Does not own: schema metadata, relation policy, or storage-layer persistence.
//! Boundary: all identity construction/decoding for db data/index key domains.
//!
//! Invariants:
//! - Identities are ASCII, non-empty, and bounded by MAX_* limits.
//! - All construction paths validate invariants.
//! - Stored byte representation is canonical and order-preserving.
//! - Ordering semantics follow the length-prefixed stored-byte layout, not
//!   lexicographic string ordering.

#[cfg(test)]
mod tests;

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
/// IdentityDecodeError
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

    /// Validate and construct an entity name from one ASCII string.
    pub fn try_from_str(name: &str) -> Result<Self, EntityNameError> {
        // Phase 1: validate user-visible identity constraints.
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

        // Phase 2: write into fixed-size canonical storage.
        let mut out = [0u8; MAX_ENTITY_NAME_LEN];
        out[..len].copy_from_slice(bytes);

        Ok(Self {
            len: len as u8,
            bytes: out,
        })
    }

    #[must_use]
    /// Return the stored entity-name length.
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    #[must_use]
    /// Return whether the stored entity-name length is zero.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    /// Borrow raw identity bytes excluding trailing fixed-buffer padding.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len()]
    }

    #[must_use]
    /// Borrow the entity name as UTF-8 text.
    pub fn as_str(&self) -> &str {
        // Invariant: construction and decoding enforce ASCII-only storage,
        // so UTF-8 decoding cannot fail.
        std::str::from_utf8(self.as_bytes()).expect("EntityName invariant: ASCII-only storage")
    }

    #[must_use]
    /// Encode this identity into its fixed-size persisted representation.
    pub fn to_bytes(self) -> [u8; Self::STORED_SIZE_USIZE] {
        let mut out = [0u8; Self::STORED_SIZE_USIZE];
        out[0] = self.len;
        out[1..].copy_from_slice(&self.bytes);
        out
    }

    /// Decode one fixed-size persisted entity identity payload.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, IdentityDecodeError> {
        // Phase 1: validate layout and payload bounds.
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

        // Phase 2: materialize canonical fixed-buffer identity storage.
        let mut name = [0u8; MAX_ENTITY_NAME_LEN];
        name.copy_from_slice(&bytes[1..]);

        Ok(Self {
            len: len as u8,
            bytes: name,
        })
    }

    #[must_use]
    /// Return a maximal sortable entity identity sentinel value.
    pub const fn max_storable() -> Self {
        Self {
            len: MAX_ENTITY_NAME_LEN as u8,
            bytes: [b'z'; MAX_ENTITY_NAME_LEN],
        }
    }
}

impl Ord for EntityName {
    fn cmp(&self, other: &Self) -> Ordering {
        // Keep ordering consistent with `to_bytes()` (length prefix first).
        // This is deterministic protocol/storage ordering, not lexical string order.
        self.len.cmp(&other.len).then(self.bytes.cmp(&other.bytes))
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
    /// Fixed on-disk size in bytes (stable, protocol-level).
    pub const STORED_SIZE_BYTES: u64 = 2 + (MAX_INDEX_NAME_LEN as u64);
    /// Fixed in-memory size (for buffers and arrays).
    pub const STORED_SIZE_USIZE: usize = Self::STORED_SIZE_BYTES as usize;

    /// Validate and construct one index identity from an entity + field list.
    pub fn try_from_parts(entity: &EntityName, fields: &[&str]) -> Result<Self, IndexNameError> {
        // Phase 1: validate index-field count and per-field identity constraints.
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

        // Phase 2: encode canonical `entity|field...` bytes into fixed storage.
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
    /// Borrow raw index-identity bytes excluding trailing fixed-buffer padding.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    #[must_use]
    /// Borrow the index identity as UTF-8 text.
    pub fn as_str(&self) -> &str {
        // Invariant: construction and decoding enforce ASCII-only storage,
        // so UTF-8 decoding cannot fail.
        std::str::from_utf8(self.as_bytes()).expect("IndexName invariant: ASCII-only storage")
    }

    #[must_use]
    /// Encode this identity into its fixed-size persisted representation.
    pub fn to_bytes(self) -> [u8; Self::STORED_SIZE_USIZE] {
        let mut out = [0u8; Self::STORED_SIZE_USIZE];
        out[..2].copy_from_slice(&self.len.to_be_bytes());
        out[2..].copy_from_slice(&self.bytes);
        out
    }

    /// Decode one fixed-size persisted index identity payload.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, IdentityDecodeError> {
        // Phase 1: validate layout and payload bounds.
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

        // Phase 2: materialize canonical fixed-buffer identity storage.
        let mut name = [0u8; MAX_INDEX_NAME_LEN];
        name.copy_from_slice(&bytes[2..]);

        Ok(Self {
            len: len as u16,
            bytes: name,
        })
    }

    // Append bytes into the fixed-size identity buffer while tracking write offset.
    fn push_bytes(out: &mut [u8; MAX_INDEX_NAME_LEN], len: &mut usize, bytes: &[u8]) {
        let end = *len + bytes.len();
        out[*len..end].copy_from_slice(bytes);
        *len = end;
    }

    #[must_use]
    /// Return a maximal sortable index identity sentinel value.
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
