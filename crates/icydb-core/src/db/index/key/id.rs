//! Module: index::key::id
//! Responsibility: stable logical identifier for one index definition.
//! Does not own: key byte encoding or predicate semantics.
//! Boundary: prefix component used by `IndexKey`.

use crate::types::EntityTag;
use std::{fmt, mem::size_of};

///
/// IndexId
///
/// Logical identifier for an index.
/// Combines one entity tag with one stable per-entity index ordinal.
/// Used as the prefix component of all index keys.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct IndexId {
    pub(crate) entity_tag: EntityTag,
    pub(crate) ordinal: u16,
}

impl IndexId {
    /// Fixed on-disk size in bytes for index identity framing.
    pub(crate) const STORED_SIZE_USIZE: usize = size_of::<u64>() + size_of::<u16>();

    /// Build one runtime index identity from data-only identity components.
    #[must_use]
    pub(crate) const fn new(entity_tag: EntityTag, ordinal: u16) -> Self {
        Self {
            entity_tag,
            ordinal,
        }
    }

    /// Encode one fixed-size runtime index identity payload.
    #[must_use]
    pub(crate) fn to_bytes(self) -> [u8; Self::STORED_SIZE_USIZE] {
        let mut out = [0u8; Self::STORED_SIZE_USIZE];
        let entity = self.entity_tag.value().to_be_bytes();
        let ordinal = self.ordinal.to_be_bytes();

        out[..size_of::<u64>()].copy_from_slice(&entity);
        out[size_of::<u64>()..].copy_from_slice(&ordinal);

        out
    }

    /// Decode one fixed-size runtime index identity payload.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::STORED_SIZE_USIZE {
            return None;
        }

        let mut entity = [0u8; size_of::<u64>()];
        entity.copy_from_slice(&bytes[..size_of::<u64>()]);

        let mut ordinal = [0u8; size_of::<u16>()];
        ordinal.copy_from_slice(&bytes[size_of::<u64>()..]);

        Some(Self::new(
            EntityTag::new(u64::from_be_bytes(entity)),
            u16::from_be_bytes(ordinal),
        ))
    }

    /// Maximum sentinel value for test-only stable-memory bound checks.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn max_storable() -> Self {
        Self::new(EntityTag::new(u64::MAX), u16::MAX)
    }
}

impl fmt::Display for IndexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.entity_tag.value(), self.ordinal)
    }
}
