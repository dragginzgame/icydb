//! Test-only constructors and adapters for the parent module.

use super::*;

impl EncodedPrimaryKey {
    pub(in crate::db) fn decode(&self) -> Result<PrimaryKeyValue, CompactPrimaryKeyDecodeError> {
        Self::decode_bytes(self.as_bytes())
    }

    pub(in crate::db) fn decode_component(
        &self,
    ) -> Result<PrimaryKeyComponent, CompactPrimaryKeyDecodeError> {
        decode_primary_key_component(&self.bytes)
    }

    pub(in crate::db) fn decode_composite(
        &self,
    ) -> Result<CompositePrimaryKeyValue, CompactPrimaryKeyDecodeError> {
        decode_composite_primary_key_value(&self.bytes)
    }

    pub(in crate::db) fn kind(&self) -> Result<PrimaryKeyKind, CompactPrimaryKeyDecodeError> {
        primary_key_kind_from_bytes(self.as_bytes())
    }

    pub(in crate::db) fn payload(&self) -> Result<&[u8], CompactPrimaryKeyDecodeError> {
        let _ = self.kind()?;
        Ok(&self.bytes[TAG_SIZE..])
    }
}

impl DataStoreKey {
    pub(in crate::db) fn try_from_raw_bytes(
        bytes: &[u8],
    ) -> Result<Self, CompactStoreKeyDecodeError> {
        const ENTITY_TAG_SIZE: usize = size_of::<u64>();
        const MIN_RAW_DATA_STORE_KEY_SIZE: usize = ENTITY_TAG_SIZE + TAG_SIZE;

        if bytes.len() < MIN_RAW_DATA_STORE_KEY_SIZE {
            return Err(CompactStoreKeyDecodeError::DataStoreKeyTooShort);
        }

        let mut entity_bytes = [0u8; ENTITY_TAG_SIZE];
        entity_bytes.copy_from_slice(&bytes[..ENTITY_TAG_SIZE]);
        let primary_key = EncodedPrimaryKey::try_from(&bytes[ENTITY_TAG_SIZE..])?;

        Ok(Self::new(
            EntityTag::new(u64::from_be_bytes(entity_bytes)),
            primary_key,
        ))
    }

    #[must_use]
    pub(in crate::db) const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    #[must_use]
    pub(in crate::db) const fn primary_key(&self) -> &EncodedPrimaryKey {
        &self.primary_key
    }
}

impl RawDataStoreKey {
    pub(in crate::db) fn from_bytes(bytes: &[u8]) -> Result<Self, CompactStoreKeyDecodeError> {
        let _ = DataStoreKey::try_from_raw_bytes(bytes)?;
        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }

    pub(in crate::db) fn decode(&self) -> Result<DataStoreKey, CompactStoreKeyDecodeError> {
        DataStoreKey::try_from_raw_bytes(&self.bytes)
    }

    #[must_use]
    pub(in crate::db) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl RawDataStoreKeyRange {
    #[must_use]
    pub(in crate::db) fn contains(&self, key: &RawDataStoreKey) -> bool {
        key.as_bytes() >= self.lower_inclusive.as_slice()
            && self
                .upper_exclusive
                .as_deref()
                .is_none_or(|upper| key.as_bytes() < upper)
    }
}

impl RawIndexStoreKey {
    pub(in crate::db) fn from_bytes(bytes: &[u8]) -> Result<Self, CompactStoreKeyDecodeError> {
        let _ = IndexStoreKey::try_from_raw_bytes(bytes)?;
        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }

    pub(in crate::db) fn decode(&self) -> Result<IndexStoreKey, CompactStoreKeyDecodeError> {
        IndexStoreKey::try_from_raw_bytes(&self.bytes)
    }
}

impl IndexEntryValue {
    #[must_use]
    pub(in crate::db) fn presence_only() -> Self {
        Self { bytes: vec![0] }
    }
}

/// Canonical ordered bytes for one secondary-index component.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct EncodedIndexComponent {
    pub(super) bytes: Vec<u8>,
}

impl EncodedIndexComponent {
    #[cfg(test)]
    pub(in crate::db) fn encode_primary_overlap(
        value: PrimaryKeyComponent,
    ) -> Result<Self, CompactPrimaryKeyEncodeError> {
        let mut bytes = Vec::with_capacity(max_encoded_primary_key_len(value.kind()));
        encode_primary_key_component(value, &mut bytes)?;
        Ok(Self { bytes })
    }

    #[must_use]
    pub(in crate::db) const fn from_canonical_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    #[must_use]
    pub(in crate::db) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[cfg(test)]
    pub(in crate::db) fn payload(&self) -> Result<&[u8], CompactPrimaryKeyDecodeError> {
        let Some(&tag) = self.bytes.first() else {
            return Err(CompactPrimaryKeyDecodeError::Empty);
        };
        let _ = PrimaryKeyKind::from_tag(tag)
            .ok_or(CompactPrimaryKeyDecodeError::UnknownKind { tag })?;
        Ok(&self.bytes[TAG_SIZE..])
    }
}

impl TryFrom<&[u8]> for EncodedIndexComponent {
    type Error = CompactPrimaryKeyDecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.is_empty() {
            return Err(CompactPrimaryKeyDecodeError::Empty);
        }

        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }
}

/// Logical index-store key:
/// `key_kind + IndexId + EncodedIndexComponent[] + EncodedPrimaryKey`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct IndexStoreKey {
    pub(super) key_kind: IndexStoreKeyKind,
    pub(super) index_id: IndexId,
    pub(super) components: Vec<EncodedIndexComponent>,
    pub(super) primary_key: EncodedPrimaryKey,
}

impl IndexStoreKey {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn new(
        index_id: IndexId,
        components: Vec<EncodedIndexComponent>,
        primary_key: EncodedPrimaryKey,
    ) -> Self {
        Self::new_with_kind(IndexStoreKeyKind::User, index_id, components, primary_key)
    }

    #[must_use]
    pub(in crate::db) const fn new_with_kind(
        key_kind: IndexStoreKeyKind,
        index_id: IndexId,
        components: Vec<EncodedIndexComponent>,
        primary_key: EncodedPrimaryKey,
    ) -> Self {
        Self {
            key_kind,
            index_id,
            components,
            primary_key,
        }
    }

    pub(in crate::db) fn to_raw(&self) -> Result<RawIndexStoreKey, CompactStoreKeyEncodeError> {
        encode_index_store_key(
            self.key_kind,
            self.index_id,
            self.components.iter().map(EncodedIndexComponent::as_bytes),
            self.primary_key.as_bytes(),
        )
    }

    #[cfg(test)]
    pub(in crate::db) fn try_from_raw_bytes(
        bytes: &[u8],
    ) -> Result<Self, CompactStoreKeyDecodeError> {
        let _ = decode_raw_index_store_key_segments(bytes)?;
        let mut input = bytes;

        let key_kind = take_exact(&mut input, TAG_SIZE)?[0];
        let key_kind = IndexStoreKeyKind::from_tag(key_kind)
            .ok_or(CompactStoreKeyDecodeError::UnknownIndexKeyKind)?;

        let index_id = IndexId::from_bytes(take_exact(&mut input, IndexId::STORED_SIZE_USIZE)?)
            .ok_or(CompactStoreKeyDecodeError::InvalidIndexId)?;

        let component_count = usize::from(take_exact(&mut input, TAG_SIZE)?[0]);
        let mut components = Vec::with_capacity(component_count);
        for _ in 0..component_count {
            let component_bytes = take_len_prefixed(&mut input)?;
            components.push(EncodedIndexComponent::try_from(component_bytes)?);
        }

        let primary_key = EncodedPrimaryKey::try_from(take_len_prefixed(&mut input)?)?;
        if !input.is_empty() {
            return Err(CompactStoreKeyDecodeError::TrailingIndexBytes);
        }

        Ok(Self::new_with_kind(
            key_kind,
            index_id,
            components,
            primary_key,
        ))
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn key_kind(&self) -> IndexStoreKeyKind {
        self.key_kind
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn components(&self) -> &[EncodedIndexComponent] {
        &self.components
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn primary_key(&self) -> &EncodedPrimaryKey {
        &self.primary_key
    }
}
