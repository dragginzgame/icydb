//! Module: index::key::codec
//! Responsibility: raw byte framing/parsing for `IndexKey`.
//! Does not own: semantic index key construction policy.
//! Boundary: this module is the storage-key codec authority for index keys.

mod bounds;
mod envelope;
mod error;
mod scalar;
mod tuple;

use crate::{
    MAX_INDEX_FIELDS,
    db::{
        data::{StorageKey, StorageKeyDecodeError},
        identity::IndexName,
        index::key::IndexId,
    },
};
use bounds::{
    COMPONENT_COUNT_SIZE, INDEX_ID_SIZE, KEY_KIND_TAG_SIZE, KEY_PREFIX_SIZE, SEGMENT_LEN_SIZE,
};
use error::{
    ERR_INVALID_INDEX_LENGTH, ERR_INVALID_INDEX_NAME_BYTES, ERR_INVALID_SIZE, ERR_TRAILING_BYTES,
};
pub(crate) use scalar::IndexKeyKind;
use std::cmp::Ordering;
use tuple::{
    compare_component_segments, compare_length_prefixed_segment, push_segment, read_segment,
};

///
/// IndexKey
///
/// Fully-qualified index lookup key.
/// Variable-length, manually encoded structure designed for stable-memory ordering.
/// Ordering of this type must exactly match byte-level ordering.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct IndexKey {
    pub(super) key_kind: IndexKeyKind,
    pub(super) index_id: IndexId,
    pub(super) components: Vec<Vec<u8>>,
    pub(super) primary_key: Vec<u8>,
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key_kind
            .cmp(&other.key_kind)
            .then_with(|| self.index_id.cmp(&other.index_id))
            .then_with(|| self.components.len().cmp(&other.components.len()))
            .then_with(|| compare_component_segments(&self.components, &other.components))
            .then_with(|| compare_length_prefixed_segment(&self.primary_key, &other.primary_key))
    }
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl IndexKey {
    #[must_use]
    pub(crate) fn to_raw(&self) -> RawIndexKey {
        // Phase 1: precompute capacity and validate in-memory invariants.
        let component_count = self.components.len();
        debug_assert!(component_count <= MAX_INDEX_FIELDS);
        debug_assert!(u8::try_from(component_count).is_ok());
        debug_assert!(!self.primary_key.is_empty());
        debug_assert!(self.primary_key.len() <= Self::MAX_PK_SIZE);

        let mut capacity = KEY_PREFIX_SIZE + SEGMENT_LEN_SIZE + self.primary_key.len();
        for component in &self.components {
            debug_assert!(!component.is_empty());
            debug_assert!(component.len() <= Self::MAX_COMPONENT_SIZE);
            capacity += SEGMENT_LEN_SIZE + component.len();
        }

        let mut bytes = Vec::with_capacity(capacity);

        // Phase 2: write key kind, index id, component count, and segments.
        bytes.push(self.key_kind.tag());

        let name_bytes = self.index_id.0.to_bytes();
        bytes.extend_from_slice(&name_bytes);

        let component_count_u8 =
            u8::try_from(component_count).expect("component count should fit in one byte");
        bytes.push(component_count_u8);

        for component in &self.components {
            push_segment(&mut bytes, component);
        }

        push_segment(&mut bytes, &self.primary_key);

        RawIndexKey(bytes)
    }

    pub(crate) fn try_from_raw(raw: &RawIndexKey) -> Result<Self, &'static str> {
        // Phase 1: validate frame size and read fixed prefix fields.
        let bytes = raw.as_bytes();
        if bytes.len() < Self::MIN_STORED_SIZE_USIZE || bytes.len() > Self::STORED_SIZE_USIZE {
            return Err(ERR_INVALID_SIZE);
        }

        let mut offset = 0;

        let key_kind = IndexKeyKind::from_tag(bytes[offset])?;
        offset += KEY_KIND_TAG_SIZE;

        let index_name =
            IndexName::from_bytes(&bytes[offset..offset + IndexName::STORED_SIZE_USIZE])
                .map_err(|_| ERR_INVALID_INDEX_NAME_BYTES)?;
        offset += INDEX_ID_SIZE;

        let component_count = bytes[offset];
        offset += COMPONENT_COUNT_SIZE;

        let component_count_usize = usize::from(component_count);
        if component_count_usize > MAX_INDEX_FIELDS {
            return Err(ERR_INVALID_INDEX_LENGTH);
        }

        // Phase 2: decode length-prefixed components + primary key.
        let mut components = Vec::with_capacity(component_count_usize);
        for _ in 0..component_count_usize {
            let component = read_segment(
                bytes,
                &mut offset,
                Self::MAX_COMPONENT_SIZE,
                "component segment",
            )?;
            components.push(component.to_vec());
        }

        let primary_key = read_segment(bytes, &mut offset, Self::MAX_PK_SIZE, "primary key")?;
        if offset != bytes.len() {
            return Err(ERR_TRAILING_BYTES);
        }

        Ok(Self {
            key_kind,
            index_id: IndexId(index_name),
            components,
            primary_key: primary_key.to_vec(),
        })
    }

    #[must_use]
    pub(crate) fn uses_system_namespace(&self) -> bool {
        self.key_kind == IndexKeyKind::System
    }

    #[must_use]
    pub(in crate::db) const fn key_kind(&self) -> IndexKeyKind {
        self.key_kind
    }

    #[must_use]
    pub(in crate::db) const fn index_id(&self) -> &IndexId {
        &self.index_id
    }

    #[must_use]
    pub(in crate::db) const fn component_count(&self) -> usize {
        self.components.len()
    }

    #[must_use]
    pub(in crate::db) fn component(&self, index: usize) -> Option<&[u8]> {
        self.components.get(index).map(Vec::as_slice)
    }

    pub(in crate::db) fn primary_storage_key(&self) -> Result<StorageKey, StorageKeyDecodeError> {
        StorageKey::try_from_bytes(&self.primary_key)
    }
}

///
/// RawIndexKey
///
/// Variable-length, stable-memory representation of IndexKey.
/// This is the form stored in BTreeMap keys.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RawIndexKey(Vec<u8>);

///
/// TESTS
///

#[cfg(test)]
mod tests;
