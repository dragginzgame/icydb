use crate::{
    db::store::{EntityName, MAX_ENTITY_NAME_LEN, StoreRegistry},
    prelude::*,
    traits::Storable,
};
use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory, storable::Bound};
use derive_more::{Deref, DerefMut};
use std::{
    borrow::Cow,
    fmt::{self, Display},
};

///
/// DataStoreRegistry
///

#[derive(Deref, DerefMut)]
pub struct DataStoreRegistry(StoreRegistry<DataStore>);

impl DataStoreRegistry {
    #[must_use]
    #[allow(clippy::new_without_default)]
    /// Create an empty data store registry.
    pub fn new() -> Self {
        Self(StoreRegistry::new())
    }
}

///
/// DataStore
///

#[derive(Deref, DerefMut)]
pub struct DataStore(BTreeMap<DataKey, Vec<u8>, VirtualMemory<DefaultMemoryImpl>>);

impl DataStore {
    #[must_use]
    /// Initialize a data store with the provided backing memory.
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self(BTreeMap::init(memory))
    }

    /// Sum of bytes used by all stored rows.
    pub fn memory_bytes(&self) -> u64 {
        self.iter()
            .map(|entry| u64::from(DataKey::STORED_SIZE) + entry.value().len() as u64)
            .sum()
    }
}

///
/// DataRow
///

pub type DataRow = (DataKey, Vec<u8>);

///
/// DataKey
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DataKey {
    entity: EntityName,
    key: Key,
}

impl DataKey {
    #[allow(clippy::cast_possible_truncation)]
    pub const STORED_SIZE: u32 = EntityName::STORED_SIZE + Key::STORED_SIZE as u32;

    #[must_use]
    /// Build a data key for the given entity type and primary key.
    pub fn new<E: EntityKind>(key: impl Into<Key>) -> Self {
        Self {
            entity: EntityName::from_static(E::ENTITY_NAME),
            key: key.into(),
        }
    }

    #[must_use]
    pub const fn lower_bound<E: EntityKind>() -> Self {
        Self {
            entity: EntityName::from_static(E::ENTITY_NAME),
            key: Key::lower_bound(),
        }
    }

    #[must_use]
    pub const fn upper_bound<E: EntityKind>() -> Self {
        Self {
            entity: EntityName::from_static(E::ENTITY_NAME),
            key: Key::upper_bound(),
        }
    }

    /// Return the primary key component of this data key.
    #[must_use]
    pub const fn key(&self) -> Key {
        self.key
    }

    /// Entity name (stable, compile-time constant per entity type).
    #[must_use]
    pub const fn entity_name(&self) -> &EntityName {
        &self.entity
    }

    /// Compute the on-disk size used by a single data entry from its value length.
    /// Includes the bounded `DataKey` size and the value bytes.
    #[must_use]
    pub const fn entry_size_bytes(value_len: u64) -> u64 {
        Self::STORED_SIZE as u64 + value_len
    }

    #[must_use]
    /// Max sentinel key for sizing.
    pub fn max_storable() -> Self {
        Self {
            entity: EntityName::max_storable(),
            key: Key::max_storable(),
        }
    }
}

impl Display for DataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{} ({})", self.entity, self.key)
    }
}

impl From<DataKey> for Key {
    fn from(key: DataKey) -> Self {
        key.key()
    }
}

/// Binary layout (fixed-size):
/// [u8 entity_len]
/// [MAX_ENTITY_NAME_LEN bytes entity_name]
/// [Key bytes...]
impl Storable for DataKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        let mut buf = Vec::with_capacity(Self::STORED_SIZE as usize);

        // ── EntityName (fixed-size) ─────────────────
        buf.push(self.entity.len);
        buf.extend_from_slice(&self.entity.bytes);

        // ── Key (fixed-size) ───────────────────────
        let key_bytes = self.key.to_bytes();
        debug_assert_eq!(
            key_bytes.len(),
            Key::STORED_SIZE,
            "Key serialization must be exactly fixed-size"
        );
        buf.extend_from_slice(&key_bytes);

        debug_assert_eq!(
            buf.len(),
            Self::STORED_SIZE as usize,
            "DataKey serialized size mismatch"
        );

        Cow::Owned(buf)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        let bytes = bytes.as_ref();
        assert_eq!(
            bytes.len(),
            Self::STORED_SIZE as usize,
            "corrupted DataKey: invalid size"
        );

        let mut offset = 0;

        // ── EntityName ─────────────────────────────
        let len = bytes[offset];
        assert!(
            len > 0 && len as usize <= MAX_ENTITY_NAME_LEN,
            "corrupted DataKey: invalid entity name length"
        );
        offset += 1;

        let mut entity_bytes = [0u8; MAX_ENTITY_NAME_LEN];
        entity_bytes.copy_from_slice(&bytes[offset..offset + MAX_ENTITY_NAME_LEN]);
        offset += MAX_ENTITY_NAME_LEN;

        let entity = EntityName {
            len,
            bytes: entity_bytes,
        };

        // ── Key ────────────────────────────────────
        let key = Key::from_bytes(bytes[offset..offset + Key::STORED_SIZE].into());

        Self { entity, key }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.to_bytes().into_owned()
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: Self::STORED_SIZE,
        is_fixed_size: true,
    };
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Storable;

    #[test]
    fn data_key_is_exactly_fixed_size() {
        let data_key = DataKey::max_storable();
        let size = data_key.to_bytes().len();

        assert_eq!(
            size,
            DataKey::STORED_SIZE as usize,
            "DataKey must serialize to exactly STORED_SIZE bytes"
        );
    }

    #[test]
    fn data_key_ordering_matches_bytes() {
        let keys = vec![
            DataKey {
                entity: EntityName::from_static("a"),
                key: Key::Int(0),
            },
            DataKey {
                entity: EntityName::from_static("aa"),
                key: Key::Int(0),
            },
            DataKey {
                entity: EntityName::from_static("b"),
                key: Key::Int(0),
            },
            DataKey {
                entity: EntityName::from_static("a"),
                key: Key::Uint(1),
            },
        ];

        let mut sorted_by_ord = keys.clone();
        sorted_by_ord.sort();

        let mut sorted_by_bytes = keys;
        sorted_by_bytes.sort_by(|a, b| a.to_bytes().cmp(&b.to_bytes()));

        assert_eq!(
            sorted_by_ord, sorted_by_bytes,
            "DataKey Ord and byte ordering diverged"
        );
    }

    #[test]
    #[should_panic(expected = "corrupted DataKey: invalid size")]
    fn data_key_rejects_undersized_bytes() {
        let buf = vec![0u8; DataKey::STORED_SIZE as usize - 1];
        let _ = DataKey::from_bytes(buf.into());
    }

    #[test]
    #[should_panic(expected = "corrupted DataKey: invalid size")]
    fn data_key_rejects_oversized_bytes() {
        let buf = vec![0u8; DataKey::STORED_SIZE as usize + 1];
        let _ = DataKey::from_bytes(buf.into());
    }

    #[test]
    #[should_panic(expected = "corrupted DataKey: invalid entity name length")]
    fn data_key_rejects_invalid_entity_len() {
        let mut buf = DataKey::max_storable().to_bytes().into_owned();
        buf[0] = 0;
        let _ = DataKey::from_bytes(buf.into());
    }
}
