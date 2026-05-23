//! Module: index::key::codec
//! Responsibility: raw byte framing/parsing for `IndexKey`.
//! Does not own: semantic index key construction policy.
//! Boundary: this module is the raw-key codec authority for index keys.

mod bounds;
mod envelope;
mod error;
mod scalar;
mod tuple;

use crate::{
    MAX_INDEX_FIELDS,
    db::{
        data::{StorageKey, StorageKeyDecodeError},
        index::key::IndexId,
        key_taxonomy::{
            CompactPrimaryKeyDecodeError, EncodedIndexComponent, EncodedPrimaryKey, IndexStoreKey,
            IndexStoreKeyKind, PrimaryKeyValue,
        },
    },
};
use bounds::{
    COMPONENT_COUNT_SIZE, INDEX_ID_SIZE, KEY_KIND_TAG_SIZE, KEY_PREFIX_SIZE, SEGMENT_LEN_SIZE,
};
use error::{
    ERR_INVALID_INDEX_ID_BYTES, ERR_INVALID_INDEX_LENGTH, ERR_INVALID_SIZE, ERR_TRAILING_BYTES,
};
use std::cmp::Ordering;
use tuple::{compare_component_segments, compare_segment_bytes, push_segment, read_segment};

pub(crate) use crate::db::key_taxonomy::RawIndexStoreKey;
pub(crate) use scalar::IndexKeyKind;

///
/// IndexKey
///
/// Fully-qualified index lookup key.
/// Variable-length, manually encoded structure designed for stable-memory storage.
/// Ordering of this type follows decoded component semantics rather than tuple-frame bytes.
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
            .then_with(|| compare_segment_bytes(&self.primary_key, &other.primary_key))
    }
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl IndexKey {
    fn to_raw_with_primary_key(&self, primary_key: &[u8]) -> RawIndexStoreKey {
        // Phase 1: validate in-memory invariants before crossing into the
        // store-key taxonomy wrapper.
        let component_count = self.components.len();
        debug_assert!(component_count <= MAX_INDEX_FIELDS);
        debug_assert!(u8::try_from(component_count).is_ok());
        debug_assert!(!primary_key.is_empty());
        debug_assert!(primary_key.len() <= Self::MAX_PK_SIZE);

        for component in &self.components {
            debug_assert!(!component.is_empty());
            debug_assert!(component.len() <= Self::MAX_COMPONENT_SIZE);
        }

        // Phase 2: write ordinary row keys through the compact taxonomy
        // contract. Prefix/range sentinels intentionally use wildcard primary
        // segments, so they stay on the raw segment path below.
        let Ok(primary_key) = EncodedPrimaryKey::try_from(primary_key) else {
            return self.to_raw_with_primary_key_segment(primary_key);
        };
        let components = self
            .components
            .iter()
            .cloned()
            .map(EncodedIndexComponent::from_canonical_bytes)
            .collect();
        let raw = IndexStoreKey::new_with_kind(
            index_key_kind_to_store_key_kind(self.key_kind),
            self.index_id,
            components,
            primary_key,
        )
        .to_raw()
        .expect("validated index key should encode");

        RawIndexStoreKey::from_persisted_bytes(raw.as_bytes().to_vec())
    }

    fn to_raw_with_primary_key_segment(&self, primary_key: &[u8]) -> RawIndexStoreKey {
        let component_count = self.components.len();
        let mut capacity = KEY_PREFIX_SIZE + SEGMENT_LEN_SIZE + primary_key.len();
        for component in &self.components {
            capacity += SEGMENT_LEN_SIZE + component.len();
        }

        let mut bytes = Vec::with_capacity(capacity);
        bytes.push(index_key_kind_to_store_key_kind(self.key_kind).tag());
        bytes.extend_from_slice(&self.index_id.to_bytes());
        let component_count_u8 =
            u8::try_from(component_count).expect("component count should fit in one byte");
        bytes.push(component_count_u8);

        for component in &self.components {
            push_segment(&mut bytes, component);
        }
        push_segment(&mut bytes, primary_key);

        RawIndexStoreKey::from_persisted_bytes(bytes)
    }

    #[must_use]
    pub(crate) fn to_raw(&self) -> RawIndexStoreKey {
        self.to_raw_with_primary_key(&self.primary_key)
    }

    #[must_use]
    pub(in crate::db) fn raw_bounds_for_all_components(
        &self,
    ) -> (RawIndexStoreKey, RawIndexStoreKey) {
        (
            self.to_raw_with_primary_key(&Self::wildcard_low_pk()),
            self.to_raw_with_primary_key(&Self::wildcard_high_pk()),
        )
    }

    pub(crate) fn try_from_raw(raw: &RawIndexStoreKey) -> Result<Self, &'static str> {
        let bytes = raw.as_bytes();
        let (key_kind, index_id, component_count_usize, mut offset) =
            parse_index_key_header(bytes)?;

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
            index_id,
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

    pub(in crate::db) fn primary_key_value(&self) -> Result<StorageKey, StorageKeyDecodeError> {
        let encoded = EncodedPrimaryKey::try_from(self.primary_key.as_slice())
            .map_err(primary_key_decode_error_to_storage_key_decode_error)?;

        encoded
            .decode()
            .map(StorageKey::from)
            .map_err(primary_key_decode_error_to_storage_key_decode_error)
    }

    pub(in crate::db) fn compact_primary_key_bytes(primary_key: StorageKey) -> Vec<u8> {
        EncodedPrimaryKey::encode(PrimaryKeyValue::from(primary_key))
            .expect("storage-key primary keys must compact-encode")
            .as_bytes()
            .to_vec()
    }
}

const fn index_key_kind_to_store_key_kind(kind: IndexKeyKind) -> IndexStoreKeyKind {
    match kind {
        IndexKeyKind::User => IndexStoreKeyKind::User,
        IndexKeyKind::System => IndexStoreKeyKind::System,
    }
}

const fn primary_key_decode_error_to_storage_key_decode_error(
    err: CompactPrimaryKeyDecodeError,
) -> StorageKeyDecodeError {
    match err {
        CompactPrimaryKeyDecodeError::Empty
        | CompactPrimaryKeyDecodeError::InvalidLength { .. } => StorageKeyDecodeError::InvalidSize,
        CompactPrimaryKeyDecodeError::UnknownKind { .. } => StorageKeyDecodeError::InvalidTag,
        CompactPrimaryKeyDecodeError::InvalidPrincipalLength { .. } => {
            StorageKeyDecodeError::InvalidPrincipalLength
        }
        CompactPrimaryKeyDecodeError::InvalidAccount { reason } => {
            StorageKeyDecodeError::InvalidAccountPayload { reason }
        }
        CompactPrimaryKeyDecodeError::InvalidCompositeCount { .. }
        | CompactPrimaryKeyDecodeError::UnitCompositeComponent { .. }
        | CompactPrimaryKeyDecodeError::NestedComposite
        | CompactPrimaryKeyDecodeError::TrailingCompositeBytes { .. } => {
            StorageKeyDecodeError::InvalidSize
        }
    }
}

// Parse the fixed-width index-key prefix and return the decoded frame header.
fn parse_index_key_header(
    bytes: &[u8],
) -> Result<(IndexKeyKind, IndexId, usize, usize), &'static str> {
    if bytes.len() < IndexKey::MIN_STORED_SIZE_USIZE
        || bytes.len() > IndexKey::MAX_STORED_SIZE_USIZE
    {
        return Err(ERR_INVALID_SIZE);
    }

    let mut offset = 0;

    let key_kind = IndexKeyKind::from_tag(bytes[offset])?;
    offset += KEY_KIND_TAG_SIZE;

    let index_id = IndexId::from_bytes(&bytes[offset..offset + INDEX_ID_SIZE])
        .ok_or(ERR_INVALID_INDEX_ID_BYTES)?;
    offset += INDEX_ID_SIZE;

    let component_count = bytes[offset];
    offset += COMPONENT_COUNT_SIZE;

    let component_count_usize = usize::from(component_count);
    if component_count_usize > MAX_INDEX_FIELDS {
        return Err(ERR_INVALID_INDEX_LENGTH);
    }

    Ok((key_kind, index_id, component_count_usize, offset))
}

impl RawIndexStoreKey {
    // Validate one raw index key while extracting only the requested component
    // segment. This keeps single-component covering scans from allocating a
    // full decoded `IndexKey` when they only need one component payload.
    #[cfg(test)]
    pub(in crate::db) fn validated_component(
        &self,
        index: usize,
    ) -> Result<Option<&[u8]>, &'static str> {
        let bytes = self.as_bytes();
        let (_, _, component_count, mut offset) = parse_index_key_header(bytes)?;
        if index >= component_count {
            return Ok(None);
        }

        let mut target = None;
        for component_offset in 0..component_count {
            let segment = read_segment(
                bytes,
                &mut offset,
                IndexKey::MAX_COMPONENT_SIZE,
                "component segment",
            )?;
            if component_offset == index {
                target = Some(segment);
            }
        }

        read_segment(bytes, &mut offset, IndexKey::MAX_PK_SIZE, "primary key")?;
        if offset != bytes.len() {
            return Err(ERR_TRAILING_BYTES);
        }

        Ok(target)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
