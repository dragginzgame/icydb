use crate::{
    db::{
        index::{
            entry::RawIndexEntry,
            fingerprint,
            key::{IndexId, IndexKey, RawIndexKey},
        },
        store::{DataKey, StoreRegistry},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    prelude::{EntityKind, IndexModel, Value},
};
use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory};
use derive_more::{Deref, DerefMut};

///
/// IndexStoreRegistry
///
/// Registry managing all index stores for the database.
/// Provides lifecycle and lookup for per-index stable-memory stores.
///

#[derive(Deref, DerefMut)]
pub struct IndexStoreRegistry(StoreRegistry<IndexStore>);

impl IndexStoreRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self(StoreRegistry::new())
    }
}

impl Default for IndexStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

///
/// IndexStore
///
/// Stable-memory backed secondary index.
/// Maps composite index keys to one or more entity primary keys.
///

#[derive(Deref, DerefMut)]
pub struct IndexStore(BTreeMap<RawIndexKey, RawIndexEntry, VirtualMemory<DefaultMemoryImpl>>);

impl IndexStore {
    #[must_use]
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self(BTreeMap::init(memory))
    }

    pub fn resolve_data_values<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
    ) -> Result<Vec<DataKey>, InternalError> {
        let mut out = Vec::new();
        if prefix.len() > index.fields.len() {
            return Err(InternalError::new(
                ErrorClass::Unsupported,
                ErrorOrigin::Index,
                format!(
                    "index prefix length {} exceeds field count {}",
                    prefix.len(),
                    index.fields.len()
                ),
            ));
        }
        let index_id = IndexId::new_unchecked::<E>(index);

        let mut fps = Vec::with_capacity(prefix.len());
        for value in prefix {
            let Some(fp) = fingerprint::to_index_fingerprint(value)? else {
                return Err(InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    "index prefix value is not indexable",
                ));
            };
            fps.push(fp);
        }

        let (start, end) = IndexKey::bounds_for_prefix(index_id, index.fields.len(), &fps);
        let start_raw = start.to_raw();
        let end_raw = end.to_raw();

        for entry in self.range(start_raw..=end_raw) {
            let _ = IndexKey::try_from_raw(entry.key()).map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Index,
                    format!("index key corrupted during resolve: {err}"),
                )
            })?;
            let decoded = entry.value().try_decode().map_err(|err| {
                InternalError::new(ErrorClass::Corruption, ErrorOrigin::Index, err.to_string())
            })?;
            if index.unique && decoded.len() != 1 {
                return Err(InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Index,
                    "unique index entry contains an unexpected number of keys",
                ));
            }
            out.extend(decoded.iter_keys().map(|k| DataKey::new::<E>(k)));
        }

        Ok(out)
    }

    /// Sum of bytes used by all index entries.
    pub fn memory_bytes(&self) -> u64 {
        self.iter()
            .map(|entry| u64::from(IndexKey::STORED_SIZE) + entry.value().len() as u64)
            .sum()
    }
}
