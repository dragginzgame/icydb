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
        index::key::IndexId,
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
    fn to_raw_with_primary_key(&self, primary_key: &[u8]) -> RawIndexKey {
        // Phase 1: precompute capacity and validate in-memory invariants.
        let component_count = self.components.len();
        debug_assert!(component_count <= MAX_INDEX_FIELDS);
        debug_assert!(u8::try_from(component_count).is_ok());
        debug_assert!(!primary_key.is_empty());
        debug_assert!(primary_key.len() <= Self::MAX_PK_SIZE);

        let mut capacity = KEY_PREFIX_SIZE + SEGMENT_LEN_SIZE + primary_key.len();
        for component in &self.components {
            debug_assert!(!component.is_empty());
            debug_assert!(component.len() <= Self::MAX_COMPONENT_SIZE);
            capacity += SEGMENT_LEN_SIZE + component.len();
        }

        let mut bytes = Vec::with_capacity(capacity);

        // Phase 2: write key kind, index id, component count, and segments.
        bytes.push(self.key_kind.tag());

        bytes.extend_from_slice(&self.index_id.to_bytes());

        let component_count_u8 =
            u8::try_from(component_count).expect("component count should fit in one byte");
        bytes.push(component_count_u8);

        for component in &self.components {
            push_segment(&mut bytes, component);
        }

        push_segment(&mut bytes, primary_key);

        RawIndexKey(bytes)
    }

    #[must_use]
    pub(crate) fn to_raw(&self) -> RawIndexKey {
        self.to_raw_with_primary_key(&self.primary_key)
    }

    #[must_use]
    pub(in crate::db) fn raw_bounds_for_all_components(&self) -> (RawIndexKey, RawIndexKey) {
        (
            self.to_raw_with_primary_key(&Self::wildcard_low_pk()),
            self.to_raw_with_primary_key(&Self::wildcard_high_pk()),
        )
    }

    pub(crate) fn try_from_raw(raw: &RawIndexKey) -> Result<Self, &'static str> {
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

    pub(in crate::db) fn primary_storage_key(&self) -> Result<StorageKey, StorageKeyDecodeError> {
        let bytes: &[u8; StorageKey::STORED_SIZE_USIZE] = self
            .primary_key
            .as_slice()
            .try_into()
            .map_err(|_| StorageKeyDecodeError::InvalidSize)?;

        StorageKey::try_from_stored_bytes(bytes)
    }
}

// Parse the fixed-width index-key prefix and return the decoded frame header.
fn parse_index_key_header(
    bytes: &[u8],
) -> Result<(IndexKeyKind, IndexId, usize, usize), &'static str> {
    if bytes.len() < IndexKey::MIN_STORED_SIZE_USIZE || bytes.len() > IndexKey::STORED_SIZE_USIZE {
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

fn compare_raw_index_key_bytes(left: &[u8], right: &[u8]) -> Ordering {
    // Phase 1: compare the fixed-width semantic header, or fall back to raw bytes
    // if either side is malformed. Stable storage should only contain canonical
    // keys, but `Ord` still needs a total ordering for diagnostics/tests.
    let Ok((left_kind, left_index_id, left_component_count, left_offset)) =
        parse_index_key_header(left)
    else {
        return left.cmp(right);
    };
    let Ok((right_kind, right_index_id, right_component_count, right_offset)) =
        parse_index_key_header(right)
    else {
        return left.cmp(right);
    };

    left_kind
        .cmp(&right_kind)
        .then_with(|| left_index_id.cmp(&right_index_id))
        .then_with(|| left_component_count.cmp(&right_component_count))
        .then_with(|| {
            compare_raw_index_key_segments(
                left,
                right,
                left_component_count,
                left_offset,
                right_offset,
            )
        })
}

fn compare_raw_index_key_segments(
    left: &[u8],
    right: &[u8],
    component_count: usize,
    mut left_offset: usize,
    mut right_offset: usize,
) -> Ordering {
    // Phase 1: decode and compare the indexed components while ignoring tuple
    // framing bytes. The ordered component payload already encodes semantic order.
    for _ in 0..component_count {
        let Ok(left_segment) = read_segment(
            left,
            &mut left_offset,
            IndexKey::MAX_COMPONENT_SIZE,
            "component segment",
        ) else {
            return left.cmp(right);
        };
        let Ok(right_segment) = read_segment(
            right,
            &mut right_offset,
            IndexKey::MAX_COMPONENT_SIZE,
            "component segment",
        ) else {
            return left.cmp(right);
        };

        let segment_order = compare_segment_bytes(left_segment, right_segment);
        if segment_order != Ordering::Equal {
            return segment_order;
        }
    }

    // Phase 2: compare the trailing primary-key segment and reject malformed
    // trailing payloads by falling back to raw bytes.
    let Ok(left_primary_key) =
        read_segment(left, &mut left_offset, IndexKey::MAX_PK_SIZE, "primary key")
    else {
        return left.cmp(right);
    };
    let Ok(right_primary_key) = read_segment(
        right,
        &mut right_offset,
        IndexKey::MAX_PK_SIZE,
        "primary key",
    ) else {
        return left.cmp(right);
    };

    let primary_key_order = compare_segment_bytes(left_primary_key, right_primary_key);
    if primary_key_order != Ordering::Equal {
        return primary_key_order;
    }

    if left_offset != left.len() || right_offset != right.len() {
        return left.cmp(right);
    }

    Ordering::Equal
}

///
/// RawIndexKey
///
/// Variable-length, stable-memory representation of IndexKey.
/// This is the form stored in BTreeMap keys.
/// Ordering follows decoded key semantics rather than serialized tuple framing bytes.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RawIndexKey(Vec<u8>);

impl Ord for RawIndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_raw_index_key_bytes(&self.0, &other.0)
    }
}

impl PartialOrd for RawIndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl RawIndexKey {
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
