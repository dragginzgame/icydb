use crate::{
    db::{
        index::{
            entry::{IndexEntry, IndexEntryCorruption, MAX_INDEX_ENTRY_KEYS, RawIndexEntry},
            fingerprint,
            key::{IndexId, IndexKey, RawIndexKey},
        },
        store::{DataKey, StoreRegistry},
    },
    prelude::{EntityKind, IndexModel, Value},
};
use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory};
use derive_more::{Deref, DerefMut};
use thiserror::Error as ThisError;

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
/// IndexInsertOutcome
///
/// Result of attempting to insert an entity into an index.
/// Distinguishes between a successful mutation and a no-op.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexInsertOutcome {
    Inserted,
    Skipped,
}

///
/// IndexRemoveOutcome
///
/// Result of attempting to remove an entity from an index.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexRemoveOutcome {
    Removed,
    Skipped,
}

///
/// IndexInsertError
///
/// Errors that may occur while inserting into an index.
/// Represents both logical constraint violations and corruption.
///

#[derive(Debug, ThisError)]
pub enum IndexInsertError {
    #[error("unique index violation")]
    UniqueViolation,
    #[error("index entry corrupted: {0}")]
    CorruptedEntry(#[from] IndexEntryCorruption),
    #[error("index entry exceeds max keys: {keys} (limit {MAX_INDEX_ENTRY_KEYS})")]
    EntryTooLarge { keys: usize },
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

    pub fn insert_index_entry<E: EntityKind>(
        &mut self,
        entity: &E,
        index: &IndexModel,
    ) -> Result<IndexInsertOutcome, IndexInsertError> {
        let Some(index_key) = IndexKey::new(entity, index) else {
            return Ok(IndexInsertOutcome::Skipped);
        };
        let raw_key = index_key.to_raw();
        let key = entity.key();

        if let Some(raw_entry) = self.get(&raw_key) {
            let mut entry = raw_entry.try_decode()?;
            if index.unique {
                if entry.len() > 1 {
                    return Err(IndexEntryCorruption::NonUniqueEntry { keys: entry.len() }.into());
                }
                if entry.contains(&key) {
                    return Ok(IndexInsertOutcome::Skipped);
                }
                if !entry.is_empty() {
                    return Err(IndexInsertError::UniqueViolation);
                }
                let entry = IndexEntry::new(key);
                let raw_entry = RawIndexEntry::try_from_entry(&entry)
                    .map_err(|err| IndexInsertError::EntryTooLarge { keys: err.keys() })?;
                self.insert(raw_key, raw_entry);
            } else {
                entry.insert_key(key);
                let raw_entry = RawIndexEntry::try_from_entry(&entry)
                    .map_err(|err| IndexInsertError::EntryTooLarge { keys: err.keys() })?;
                self.insert(raw_key, raw_entry);
            }
        } else {
            let entry = IndexEntry::new(key);
            let raw_entry = RawIndexEntry::try_from_entry(&entry)
                .map_err(|err| IndexInsertError::EntryTooLarge { keys: err.keys() })?;
            self.insert(raw_key, raw_entry);
        }

        Ok(IndexInsertOutcome::Inserted)
    }

    pub fn remove_index_entry<E: EntityKind>(
        &mut self,
        entity: &E,
        index: &IndexModel,
    ) -> Result<IndexRemoveOutcome, IndexEntryCorruption> {
        let Some(index_key) = IndexKey::new(entity, index) else {
            return Ok(IndexRemoveOutcome::Skipped);
        };
        let raw_key = index_key.to_raw();

        if let Some(raw_entry) = self.get(&raw_key) {
            let mut entry = raw_entry.try_decode()?;
            entry.remove_key(&entity.key());
            if entry.is_empty() {
                self.remove(&raw_key);
            } else {
                let raw_entry = RawIndexEntry::try_from_entry(&entry)
                    .map_err(|err| IndexEntryCorruption::TooManyKeys { count: err.keys() })?;
                self.insert(raw_key, raw_entry);
            }
            return Ok(IndexRemoveOutcome::Removed);
        }

        Ok(IndexRemoveOutcome::Skipped)
    }

    pub fn resolve_data_values<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
    ) -> Result<Vec<DataKey>, &'static str> {
        let mut out = Vec::new();
        let index_id = IndexId::new::<E>(index);

        let Some(fps) = prefix
            .iter()
            .map(fingerprint::to_index_fingerprint)
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(out);
        };

        let (start, end) = IndexKey::bounds_for_prefix(index_id, index.fields.len(), &fps);
        let start_raw = start.to_raw();
        let end_raw = end.to_raw();

        for entry in self.range(start_raw..=end_raw) {
            let _ = IndexKey::try_from_raw(entry.key())?;
            let decoded = entry.value().try_decode().map_err(|err| err.message())?;
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
