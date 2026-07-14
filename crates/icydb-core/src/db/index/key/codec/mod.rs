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
use error::IndexKeyDecodeError;
use std::{cmp::Ordering, ops::Bound};
use tuple::{compare_component_segments, compare_segment_bytes, push_segment, read_segment};

pub(crate) use crate::db::key_taxonomy::RawIndexStoreKey;
pub(in crate::db) use error::IndexKeyEncodeError;
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
    /// Clone this decoded key under a reassigned dense index identity.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn clone_with_index_id(&self, index_id: IndexId) -> Self {
        Self {
            key_kind: self.key_kind,
            index_id,
            components: self.components.clone(),
            primary_key: self.primary_key.clone(),
        }
    }

    fn to_raw_with_primary_key(
        &self,
        primary_key: &[u8],
    ) -> Result<RawIndexStoreKey, IndexKeyEncodeError> {
        // Phase 1: validate in-memory invariants before crossing into the
        // store-key taxonomy wrapper.
        let component_count = self.components.len();
        if component_count > MAX_INDEX_FIELDS || u8::try_from(component_count).is_err() {
            return Err(IndexKeyEncodeError::TooManyComponents);
        }
        if primary_key.is_empty() {
            return Err(IndexKeyEncodeError::EmptySegment);
        }
        if primary_key.len() > Self::MAX_PK_SIZE {
            return Err(IndexKeyEncodeError::SegmentTooLarge);
        }

        for component in &self.components {
            if component.is_empty() {
                return Err(IndexKeyEncodeError::EmptySegment);
            }
            if component.len() > Self::MAX_COMPONENT_SIZE {
                return Err(IndexKeyEncodeError::SegmentTooLarge);
            }
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
        .map_err(IndexKeyEncodeError::from)?;

        Ok(RawIndexStoreKey::from_persisted_bytes(
            raw.as_bytes().to_vec(),
        ))
    }

    fn to_raw_with_primary_key_segment(
        &self,
        primary_key: &[u8],
    ) -> Result<RawIndexStoreKey, IndexKeyEncodeError> {
        let component_count = self.components.len();
        let mut capacity = KEY_PREFIX_SIZE + SEGMENT_LEN_SIZE + primary_key.len();
        for component in &self.components {
            capacity += SEGMENT_LEN_SIZE + component.len();
        }

        let mut bytes = Vec::with_capacity(capacity);
        bytes.push(index_key_kind_to_store_key_kind(self.key_kind).tag());
        bytes.extend_from_slice(&self.index_id.to_bytes());
        let component_count_u8 =
            u8::try_from(component_count).map_err(|_| IndexKeyEncodeError::TooManyComponents)?;
        bytes.push(component_count_u8);

        for component in &self.components {
            push_segment(&mut bytes, component)?;
        }
        push_segment(&mut bytes, primary_key)?;

        Ok(RawIndexStoreKey::from_persisted_bytes(bytes))
    }

    pub(crate) fn to_raw(&self) -> Result<RawIndexStoreKey, IndexKeyEncodeError> {
        self.to_raw_with_primary_key(&self.primary_key)
    }

    pub(in crate::db) fn raw_bounds_for_all_components(
        &self,
    ) -> Result<(RawIndexStoreKey, RawIndexStoreKey), IndexKeyEncodeError> {
        let lower = self.to_raw_with_primary_key(&Self::wildcard_low_pk())?;
        let upper = self.to_raw_with_primary_key(&Self::wildcard_high_pk())?;

        Ok((lower, upper))
    }

    pub(in crate::db::index) fn raw_bounds_for_prefix_with_kind<C: AsRef<[u8]>>(
        index_id: &IndexId,
        key_kind: IndexKeyKind,
        index_len: usize,
        prefix: &[C],
    ) -> Result<(RawIndexStoreKey, RawIndexStoreKey), IndexKeyEncodeError> {
        if index_len > MAX_INDEX_FIELDS || prefix.len() > index_len {
            debug_assert!(
                false,
                "invalid prefix bounds input: index_len={} prefix_len={} (max={})",
                index_len,
                prefix.len(),
                MAX_INDEX_FIELDS
            );
            let empty = Self::empty_with_kind(index_id, key_kind).to_raw()?;
            return Ok((empty.clone(), empty));
        }
        for component in prefix {
            let component = component.as_ref();
            if component.is_empty() {
                return Err(IndexKeyEncodeError::EmptySegment);
            }
            if component.len() > Self::MAX_COMPONENT_SIZE {
                return Err(IndexKeyEncodeError::SegmentTooLarge);
            }
        }

        let lower = Self::raw_prefix_bound_with_kind(
            index_id,
            key_kind,
            index_len,
            prefix,
            PrefixBoundSentinel::Low,
        )?;
        let upper = Self::raw_prefix_bound_with_kind(
            index_id,
            key_kind,
            index_len,
            prefix,
            PrefixBoundSentinel::High,
        )?;

        Ok((lower, upper))
    }

    pub(in crate::db::index) fn raw_bounds_for_prefix_component_range_with_kind<
        C: AsRef<[u8]>,
        B: AsRef<[u8]>,
    >(
        index_id: &IndexId,
        key_kind: IndexKeyKind,
        index_len: usize,
        prefix: &[C],
        lower: &Bound<B>,
        upper: &Bound<B>,
    ) -> Result<(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>), IndexKeyEncodeError> {
        if index_len == 0 || index_len > MAX_INDEX_FIELDS || prefix.len() >= index_len {
            debug_assert!(
                false,
                "invalid component-range bounds shape: index_len={} prefix_len={} (max={})",
                index_len,
                prefix.len(),
                MAX_INDEX_FIELDS
            );
            let empty = Self::empty_with_kind(index_id, key_kind).to_raw()?;
            return Ok((Bound::Included(empty.clone()), Bound::Included(empty)));
        }
        for component in prefix {
            let component = component.as_ref();
            if component.is_empty() {
                return Err(IndexKeyEncodeError::EmptySegment);
            }
            if component.len() > Self::MAX_COMPONENT_SIZE {
                return Err(IndexKeyEncodeError::SegmentTooLarge);
            }
        }
        validate_component_bound(lower)?;
        validate_component_bound(upper)?;

        let lower_raw = Self::raw_component_range_bound_with_kind(
            index_id,
            key_kind,
            index_len,
            prefix,
            lower,
            RangeBoundSide::Lower,
        )?;
        let upper_raw = Self::raw_component_range_bound_with_kind(
            index_id,
            key_kind,
            index_len,
            prefix,
            upper,
            RangeBoundSide::Upper,
        )?;

        let lower_bound = match lower {
            Bound::Excluded(_) => Bound::Excluded(lower_raw),
            Bound::Included(_) | Bound::Unbounded => Bound::Included(lower_raw),
        };
        let upper_bound = match upper {
            Bound::Excluded(_) => Bound::Excluded(upper_raw),
            Bound::Included(_) | Bound::Unbounded => Bound::Included(upper_raw),
        };

        Ok((lower_bound, upper_bound))
    }

    fn raw_prefix_bound_with_kind<C: AsRef<[u8]>>(
        index_id: &IndexId,
        key_kind: IndexKeyKind,
        index_len: usize,
        prefix: &[C],
        sentinel: PrefixBoundSentinel,
    ) -> Result<RawIndexStoreKey, IndexKeyEncodeError> {
        let suffix_count = index_len.saturating_sub(prefix.len());
        let suffix_len = sentinel.component_len();
        let primary_key_len = sentinel.primary_key_len();
        let mut capacity = KEY_PREFIX_SIZE
            + (suffix_count * (SEGMENT_LEN_SIZE + suffix_len))
            + SEGMENT_LEN_SIZE
            + primary_key_len;
        for component in prefix {
            capacity = capacity.saturating_add(SEGMENT_LEN_SIZE + component.as_ref().len());
        }

        let mut bytes = Vec::with_capacity(capacity);
        bytes.push(index_key_kind_to_store_key_kind(key_kind).tag());
        bytes.extend_from_slice(&index_id.to_bytes());
        let component_count =
            u8::try_from(index_len).map_err(|_| IndexKeyEncodeError::TooManyComponents)?;
        bytes.push(component_count);

        for component in prefix {
            push_segment(&mut bytes, component.as_ref())?;
        }
        for _ in 0..suffix_count {
            push_repeated_segment(
                &mut bytes,
                sentinel.component_len(),
                sentinel.component_byte(),
            )?;
        }
        push_repeated_segment(
            &mut bytes,
            sentinel.primary_key_len(),
            sentinel.primary_key_byte(),
        )?;

        Ok(RawIndexStoreKey::from_persisted_bytes(bytes))
    }

    fn raw_component_range_bound_with_kind<C: AsRef<[u8]>, B: AsRef<[u8]>>(
        index_id: &IndexId,
        key_kind: IndexKeyKind,
        index_len: usize,
        prefix: &[C],
        bound: &Bound<B>,
        side: RangeBoundSide,
    ) -> Result<RawIndexStoreKey, IndexKeyEncodeError> {
        let range_slot = prefix.len();
        let exclusive = matches!(bound, Bound::Excluded(_));
        let suffix_len = side.suffix_sentinel(exclusive).component_len();
        let suffix_count = index_len.saturating_sub(range_slot).saturating_sub(1);
        let primary_key_len = side.primary_key_sentinel(exclusive).primary_key_len();
        let mut capacity = KEY_PREFIX_SIZE
            + SEGMENT_LEN_SIZE
            + primary_key_len
            + (suffix_count * (SEGMENT_LEN_SIZE + suffix_len));
        for component in prefix {
            capacity = capacity
                .checked_add(SEGMENT_LEN_SIZE + component.as_ref().len())
                .ok_or(IndexKeyEncodeError::SegmentTooLarge)?;
        }
        capacity = capacity
            .checked_add(
                SEGMENT_LEN_SIZE + component_bound_encoded_len(bound, side.unbounded_sentinel()),
            )
            .ok_or(IndexKeyEncodeError::SegmentTooLarge)?;

        let mut bytes = Vec::with_capacity(capacity);
        bytes.push(index_key_kind_to_store_key_kind(key_kind).tag());
        bytes.extend_from_slice(&index_id.to_bytes());
        let component_count =
            u8::try_from(index_len).map_err(|_| IndexKeyEncodeError::TooManyComponents)?;
        bytes.push(component_count);

        for component in prefix {
            push_segment(&mut bytes, component.as_ref())?;
        }
        push_component_bound_segment(&mut bytes, bound, side.unbounded_sentinel())?;
        for _ in 0..suffix_count {
            let sentinel = side.suffix_sentinel(exclusive);
            push_repeated_segment(
                &mut bytes,
                sentinel.component_len(),
                sentinel.component_byte(),
            )?;
        }
        let primary_key_sentinel = side.primary_key_sentinel(exclusive);
        push_repeated_segment(
            &mut bytes,
            primary_key_sentinel.primary_key_len(),
            primary_key_sentinel.primary_key_byte(),
        )?;

        Ok(RawIndexStoreKey::from_persisted_bytes(bytes))
    }

    pub(crate) fn try_from_raw(raw: &RawIndexStoreKey) -> Result<Self, IndexKeyDecodeError> {
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
            return Err(IndexKeyDecodeError::TrailingBytes);
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

    pub(in crate::db) fn primary_key_value(
        &self,
    ) -> Result<PrimaryKeyValue, CompactPrimaryKeyDecodeError> {
        EncodedPrimaryKey::try_from(self.primary_key.as_slice())?.decode()
    }

    pub(in crate::db) fn primary_key_value_and_bytes_from_raw(
        raw: &RawIndexStoreKey,
    ) -> Result<(PrimaryKeyValue, &[u8]), IndexKeyDecodeError> {
        let bytes = raw.as_bytes();
        let (_, _, component_count_usize, mut offset) = parse_index_key_header(bytes)?;

        for _ in 0..component_count_usize {
            let _ = read_segment(
                bytes,
                &mut offset,
                Self::MAX_COMPONENT_SIZE,
                "component segment",
            )?;
        }

        let primary_key = read_segment(bytes, &mut offset, Self::MAX_PK_SIZE, "primary key")?;
        if offset != bytes.len() {
            return Err(IndexKeyDecodeError::TrailingBytes);
        }
        let primary_key_value = EncodedPrimaryKey::decode_bytes(primary_key)
            .map_err(|_| IndexKeyDecodeError::InvalidPrimaryKey)?;

        Ok((primary_key_value, primary_key))
    }

    #[must_use]
    pub(in crate::db) const fn primary_key_bytes(&self) -> &[u8] {
        self.primary_key.as_slice()
    }

    pub(in crate::db) fn compact_primary_key_value_bytes(
        primary_key: &PrimaryKeyValue,
    ) -> Result<Vec<u8>, IndexKeyEncodeError> {
        Ok(EncodedPrimaryKey::encode(*primary_key)?.as_bytes().to_vec())
    }
}

#[derive(Clone, Copy)]
enum PrefixBoundSentinel {
    Low,
    High,
}

impl PrefixBoundSentinel {
    const fn component_len(self) -> usize {
        match self {
            Self::Low => 1,
            Self::High => IndexKey::MAX_COMPONENT_SIZE,
        }
    }

    const fn component_byte(self) -> u8 {
        match self {
            Self::Low => 0,
            Self::High => 0xFF,
        }
    }

    const fn primary_key_len(self) -> usize {
        match self {
            Self::Low => 1,
            Self::High => IndexKey::MAX_PK_SIZE,
        }
    }

    const fn primary_key_byte(self) -> u8 {
        match self {
            Self::Low => 0,
            Self::High => 0xFF,
        }
    }
}

#[derive(Clone, Copy)]
enum RangeBoundSide {
    Lower,
    Upper,
}

impl RangeBoundSide {
    const fn unbounded_sentinel(self) -> PrefixBoundSentinel {
        match self {
            Self::Lower => PrefixBoundSentinel::Low,
            Self::Upper => PrefixBoundSentinel::High,
        }
    }

    const fn suffix_sentinel(self, exclusive: bool) -> PrefixBoundSentinel {
        match (self, exclusive) {
            (Self::Lower, true) | (Self::Upper, false) => PrefixBoundSentinel::High,
            (Self::Lower, false) | (Self::Upper, true) => PrefixBoundSentinel::Low,
        }
    }

    const fn primary_key_sentinel(self, exclusive: bool) -> PrefixBoundSentinel {
        match (self, exclusive) {
            (Self::Lower, true) | (Self::Upper, false) => PrefixBoundSentinel::High,
            (Self::Lower, false) | (Self::Upper, true) => PrefixBoundSentinel::Low,
        }
    }
}

fn validate_component_bound<B: AsRef<[u8]>>(bound: &Bound<B>) -> Result<(), IndexKeyEncodeError> {
    match bound {
        Bound::Unbounded => Ok(()),
        Bound::Included(component) | Bound::Excluded(component) => {
            let component = component.as_ref();
            if component.is_empty() {
                return Err(IndexKeyEncodeError::EmptySegment);
            }
            if component.len() > IndexKey::MAX_COMPONENT_SIZE {
                return Err(IndexKeyEncodeError::SegmentTooLarge);
            }

            Ok(())
        }
    }
}

fn component_bound_encoded_len<B: AsRef<[u8]>>(
    bound: &Bound<B>,
    unbounded_sentinel: PrefixBoundSentinel,
) -> usize {
    match bound {
        Bound::Unbounded => unbounded_sentinel.component_len(),
        Bound::Included(component) | Bound::Excluded(component) => component.as_ref().len(),
    }
}

fn push_component_bound_segment<B: AsRef<[u8]>>(
    bytes: &mut Vec<u8>,
    bound: &Bound<B>,
    unbounded_sentinel: PrefixBoundSentinel,
) -> Result<(), IndexKeyEncodeError> {
    match bound {
        Bound::Unbounded => push_repeated_segment(
            bytes,
            unbounded_sentinel.component_len(),
            unbounded_sentinel.component_byte(),
        ),
        Bound::Included(component) | Bound::Excluded(component) => {
            push_segment(bytes, component.as_ref())
        }
    }
}

fn push_repeated_segment(
    bytes: &mut Vec<u8>,
    len: usize,
    byte: u8,
) -> Result<(), IndexKeyEncodeError> {
    if len == 0 {
        return Err(IndexKeyEncodeError::EmptySegment);
    }

    let len_u16 = u16::try_from(len).map_err(|_| IndexKeyEncodeError::SegmentTooLarge)?;
    bytes.extend_from_slice(&len_u16.to_be_bytes());
    let new_len = bytes
        .len()
        .checked_add(len)
        .ok_or(IndexKeyEncodeError::SegmentTooLarge)?;
    bytes.resize(new_len, byte);

    Ok(())
}

const fn index_key_kind_to_store_key_kind(kind: IndexKeyKind) -> IndexStoreKeyKind {
    match kind {
        IndexKeyKind::User => IndexStoreKeyKind::User,
        IndexKeyKind::System => IndexStoreKeyKind::System,
    }
}

// Parse the fixed-width index-key prefix and return the decoded frame header.
fn parse_index_key_header(
    bytes: &[u8],
) -> Result<(IndexKeyKind, IndexId, usize, usize), IndexKeyDecodeError> {
    if bytes.len() < IndexKey::MIN_STORED_SIZE_USIZE
        || bytes.len() > IndexKey::MAX_STORED_SIZE_USIZE
    {
        return Err(IndexKeyDecodeError::InvalidSize);
    }

    let mut offset = 0;

    let key_kind = IndexKeyKind::from_tag(bytes[offset])?;
    offset += KEY_KIND_TAG_SIZE;

    let index_id = IndexId::from_bytes(&bytes[offset..offset + INDEX_ID_SIZE])
        .ok_or(IndexKeyDecodeError::InvalidIndexIdBytes)?;
    offset += INDEX_ID_SIZE;

    let component_count = bytes[offset];
    offset += COMPONENT_COUNT_SIZE;

    let component_count_usize = usize::from(component_count);
    if component_count_usize > MAX_INDEX_FIELDS {
        return Err(IndexKeyDecodeError::InvalidIndexLength);
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
    ) -> Result<Option<&[u8]>, IndexKeyDecodeError> {
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
            return Err(IndexKeyDecodeError::TrailingBytes);
        }

        Ok(target)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
