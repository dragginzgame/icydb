use crate::{Key, db::store::StoreRegistry, traits::EntityKind};
use candid::CandidType;
use canic::{
    cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory},
    impl_storable_bounded,
};
use derive_more::{Deref, DerefMut};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

///
/// DataStoreRegistry
///

#[derive(Deref, DerefMut)]
pub struct DataStoreRegistry(StoreRegistry<DataStore>);

impl DataStoreRegistry {
    #[must_use]
    #[allow(clippy::new_without_default)]
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
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self(BTreeMap::init(memory))
    }

    pub fn memory_bytes(&self) -> u64 {
        self.iter()
            .map(|entry| u64::from(DataKey::STORABLE_MAX_SIZE) + entry.value().len() as u64)
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

#[derive(
    CandidType, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct DataKey {
    entity_id: u64,
    key: Key,
}

impl DataKey {
    pub const STORABLE_MAX_SIZE: u32 = 160;

    #[must_use]
    pub fn new<E: EntityKind>(key: impl Into<Key>) -> Self {
        Self {
            entity_id: E::ENTITY_ID,
            key: key.into(),
        }
    }

    #[must_use]
    pub const fn lower_bound<E: EntityKind>() -> Self {
        Self {
            entity_id: E::ENTITY_ID,
            key: Key::lower_bound(),
        }
    }

    #[must_use]
    pub const fn upper_bound<E: EntityKind>() -> Self {
        Self {
            entity_id: E::ENTITY_ID,
            key: Key::upper_bound(),
        }
    }

    /// Return the primary key component of this data key.
    #[must_use]
    pub const fn key(&self) -> Key {
        self.key
    }

    /// Entity identifier (stable, compile-time constant per entity type).
    #[must_use]
    pub const fn entity_id(&self) -> u64 {
        self.entity_id
    }

    /// Compute the on-disk size used by a single data entry from its value length.
    /// Includes the bounded `DataKey` size and the value bytes.
    #[must_use]
    pub const fn entry_size_bytes(value_len: u64) -> u64 {
        Self::STORABLE_MAX_SIZE as u64 + value_len
    }

    #[must_use]
    pub fn max_storable() -> Self {
        Self {
            entity_id: u64::MAX,
            key: Key::max_storable(),
        }
    }
}

impl Display for DataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{} ({})", self.entity_id, self.key)
    }
}

impl From<DataKey> for Key {
    fn from(key: DataKey) -> Self {
        key.key()
    }
}

impl_storable_bounded!(DataKey, Self::STORABLE_MAX_SIZE, false);

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Storable;

    #[test]
    fn data_key_max_size_is_bounded() {
        let data_key = DataKey::max_storable();
        let size = Storable::to_bytes(&data_key).len();

        assert!(
            size <= DataKey::STORABLE_MAX_SIZE as usize,
            "serialized DataKey too large: got {size} bytes (limit {})",
            DataKey::STORABLE_MAX_SIZE
        );
    }
}
