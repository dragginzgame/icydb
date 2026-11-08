//! Module: db::identity
//! Responsibility: validated entity/index naming and stable byte ordering contracts.
//! Does not own: accepted EntityTag/IndexId allocation, physical row/index keys,
//! schema metadata, relation policy, or storage-layer persistence.
//! Boundary: public naming/lookup values and their bounded byte representations.
//!
//! Invariants:
//! - Identities are ASCII, non-empty, and bounded by MAX_* limits.
//! - All construction paths validate invariants.
//! - Stored byte representation is canonical and order-preserving.
//! - Ordering semantics follow the length-prefixed stored-byte layout, not
//!   lexicographic string ordering.

#![expect(clippy::cast_possible_truncation)]

#[cfg(test)]
mod tests;

use crate::MAX_INDEX_FIELDS;
use icydb_schema::{
    EntityNameError, MAX_ENTITY_NAME_BYTES, SchemaName, canonical_index_name,
    is_engine_reserved_schema_name, validate_entity_name,
};
use std::{
    cmp::Ordering,
    fmt::{self, Display},
};

///
/// Constants
///

const MAX_ENTITY_NAME_LEN: usize = MAX_ENTITY_NAME_BYTES;
const MAX_INDEX_NAME_LEN: usize = icydb_schema::MAX_SCHEMA_NAME_BYTES;
const INDEX_NAME_SEGMENT_DELIMITER: u8 = b'|';

/// Decode error for persisted identity bytes.
///
/// Owned by the identity layer at storage/corruption boundaries.

#[derive(Debug)]
pub enum IdentityDecodeError {
    /// Index label violates the shared schema-name contract.
    InvalidIndexName,

    /// Persisted byte slice has the wrong fixed envelope size.
    InvalidSize,

    /// Persisted length prefix is empty or exceeds the identity limit.
    InvalidLength,

    /// Identity payload contains non-ASCII bytes.
    NonAscii,

    /// Fixed-width padding contains non-zero bytes.
    NonZeroPadding,

    /// Entity identity payload contains the reserved index segment delimiter.
    Delimiter,
}

/// Admission error for generated index names.
///
/// Owned by the identity layer while deriving index names from an entity and
/// field list.

#[derive(Debug)]
pub enum IndexNameError {
    /// Explicit name violates the shared schema-name contract.
    InvalidName(icydb_schema::SchemaContractError),

    /// Explicit name uses the engine-owned namespace.
    ReservedName,

    /// Index field list exceeds the supported key-width limit.
    TooManyFields { len: usize, max: usize },

    /// Index field list is empty.
    NoFields,

    /// One index field segment is empty.
    FieldEmpty,

    /// One index field segment exceeds the stable identity byte limit.
    FieldTooLong { field: String, max: usize },

    /// One index field segment contains non-ASCII bytes.
    FieldNonAscii { field: String },

    /// One index field segment contains the reserved segment delimiter.
    FieldDelimiter { field: String },
}

/// Validated public entity name with a bounded fixed-width representation.
///
/// Used for naming and lookup; accepted EntityTag owns physical row identity.

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
        validate_entity_name(name)?;
        let bytes = name.as_bytes();
        let len = bytes.len();

        // Phase 2: write into fixed-size canonical storage.
        let mut out = [0u8; MAX_ENTITY_NAME_LEN];
        out[..len].copy_from_slice(bytes);

        Ok(Self {
            len: len as u8,
            bytes: out,
        })
    }

    /// Return the stored entity-name length.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    /// Return whether the stored entity-name length is zero.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow raw identity bytes excluding trailing fixed-buffer padding.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len()]
    }

    /// Borrow the entity name as UTF-8 text.
    ///
    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(self.as_bytes()).unwrap_or_default()
    }

    /// Return the bounded ASCII case-folded form used for display-name lookup.
    ///
    /// This does not change persisted entity identity. Callers use the folded
    /// copy only as an unambiguous lookup key after schema admission has
    /// rejected case-insensitive display-name collisions.
    #[must_use]
    pub(in crate::db) fn ascii_case_fold(mut self) -> Self {
        let len = self.len();
        self.bytes[..len].make_ascii_lowercase();
        self
    }

    /// Encode this identity into its fixed-size persisted representation.
    #[must_use]
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
        if bytes[1..=len].contains(&INDEX_NAME_SEGMENT_DELIMITER) {
            return Err(IdentityDecodeError::Delimiter);
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

/// Validated public index name with a bounded fixed-width representation.
///
/// Used for naming and lookup; accepted IndexId owns physical index identity.

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

    /// Validate and construct one non-unique index identity from an entity +
    /// field list.
    pub fn try_from_entity_fields(
        entity: &EntityName,
        fields: &[&str],
    ) -> Result<Self, IndexNameError> {
        Self::try_from_entity_fields_with_prefix("idx", entity, fields)
    }

    /// Validate and construct one unique index identity from an entity + field
    /// list.
    pub fn try_unique_from_entity_fields(
        entity: &EntityName,
        fields: &[&str],
    ) -> Result<Self, IndexNameError> {
        Self::try_from_entity_fields_with_prefix("uniq", entity, fields)
    }

    fn try_from_entity_fields_with_prefix(
        prefix: &str,
        entity: &EntityName,
        fields: &[&str],
    ) -> Result<Self, IndexNameError> {
        // Phase 1: validate index-field count and per-field identity constraints.
        if fields.is_empty() {
            return Err(IndexNameError::NoFields);
        }
        if fields.len() > MAX_INDEX_FIELDS {
            return Err(IndexNameError::TooManyFields {
                len: fields.len(),
                max: MAX_INDEX_FIELDS,
            });
        }

        for field in fields {
            validate_entity_name(field).map_err(|error| match error {
                EntityNameError::Empty => IndexNameError::FieldEmpty,
                EntityNameError::TooLong { max, .. } => IndexNameError::FieldTooLong {
                    field: (*field).to_string(),
                    max,
                },
                EntityNameError::NonAscii => IndexNameError::FieldNonAscii {
                    field: (*field).to_string(),
                },
                EntityNameError::Delimiter => IndexNameError::FieldDelimiter {
                    field: (*field).to_string(),
                },
            })?;
        }
        let name = canonical_index_name(entity.as_str(), fields, prefix == "uniq", None);
        Self::try_from_str(&name)
    }

    /// Construct an explicit public index label using the shared schema contract.
    /// Accepted IndexId, rather than this label, owns physical index identity.
    pub fn try_from_str(name: &str) -> Result<Self, IndexNameError> {
        SchemaName::try_new(name).map_err(IndexNameError::InvalidName)?;
        if is_engine_reserved_schema_name(name) {
            return Err(IndexNameError::ReservedName);
        }
        let mut bytes = [0; MAX_INDEX_NAME_LEN];
        bytes[..name.len()].copy_from_slice(name.as_bytes());
        Ok(Self {
            len: name.len() as u16,
            bytes,
        })
    }

    /// Borrow raw index-identity bytes excluding trailing fixed-buffer padding.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    /// Borrow the index identity as UTF-8 text.
    ///
    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(self.as_bytes()).unwrap_or_default()
    }

    /// Encode this identity into its fixed-size persisted representation.
    #[must_use]
    pub fn to_bytes(self) -> [u8; Self::STORED_SIZE_USIZE] {
        let mut out = [0u8; Self::STORED_SIZE_USIZE];
        out[..2].copy_from_slice(&self.len.to_be_bytes());
        out[2..].copy_from_slice(&self.bytes);
        out
    }

    /// Decode one fixed-size persisted index identity payload.
    ///
    /// This validates the fixed-width envelope and shared name contract without
    /// reconstructing an index definition. This lookup-label envelope does not
    /// replace accepted catalog identity or its independent persisted decoder.
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

        let name =
            std::str::from_utf8(&bytes[2..2 + len]).map_err(|_| IdentityDecodeError::NonAscii)?;
        Self::try_from_str(name).map_err(|_| IdentityDecodeError::InvalidIndexName)
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
