use crate::{
    db::{
        index::{
            entry::{
                IndexEntry, IndexEntryCorruption, IndexEntryEncodeError, MAX_INDEX_ENTRY_KEYS,
                RawIndexEntry,
            },
            fingerprint,
            key::{IndexId, IndexKey, RawIndexKey},
        },
        store::{DataKey, StoreRegistry},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    key::KeyEncodeError,
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
/// IndexRemoveError
///
/// Errors returned when removing an entry from an index.
///

#[derive(Debug, ThisError)]
pub enum IndexRemoveError {
    #[error("index entry corrupted: {0}")]
    Corruption(#[from] IndexEntryCorruption),
    #[error("index entry key encoding failed: {0}")]
    KeyEncoding(#[from] KeyEncodeError),
    #[error("index key construction failed: {0}")]
    KeyConstruction(#[from] InternalError),
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
    #[error("index entry key encoding failed: {0}")]
    KeyEncoding(#[from] KeyEncodeError),
    #[error("index key construction failed: {0}")]
    KeyConstruction(#[from] InternalError),
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
        let Some(index_key) = IndexKey::new(entity, index)? else {
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
                let raw_entry = RawIndexEntry::try_from_entry(&entry).map_err(|err| match err {
                    IndexEntryEncodeError::TooManyKeys { keys } => {
                        IndexInsertError::EntryTooLarge { keys }
                    }
                    IndexEntryEncodeError::KeyEncoding(err) => IndexInsertError::KeyEncoding(err),
                })?;
                self.insert(raw_key, raw_entry);
            } else {
                entry.insert_key(key);
                let raw_entry = RawIndexEntry::try_from_entry(&entry).map_err(|err| match err {
                    IndexEntryEncodeError::TooManyKeys { keys } => {
                        IndexInsertError::EntryTooLarge { keys }
                    }
                    IndexEntryEncodeError::KeyEncoding(err) => IndexInsertError::KeyEncoding(err),
                })?;
                self.insert(raw_key, raw_entry);
            }
        } else {
            let entry = IndexEntry::new(key);
            let raw_entry = RawIndexEntry::try_from_entry(&entry).map_err(|err| match err {
                IndexEntryEncodeError::TooManyKeys { keys } => {
                    IndexInsertError::EntryTooLarge { keys }
                }
                IndexEntryEncodeError::KeyEncoding(err) => IndexInsertError::KeyEncoding(err),
            })?;
            self.insert(raw_key, raw_entry);
        }

        Ok(IndexInsertOutcome::Inserted)
    }

    pub fn remove_index_entry<E: EntityKind>(
        &mut self,
        entity: &E,
        index: &IndexModel,
    ) -> Result<IndexRemoveOutcome, IndexRemoveError> {
        let Some(index_key) = IndexKey::new(entity, index)? else {
            return Ok(IndexRemoveOutcome::Skipped);
        };
        let raw_key = index_key.to_raw();

        if let Some(raw_entry) = self.get(&raw_key) {
            let mut entry = raw_entry.try_decode()?;
            let entity_key = entity.key();
            // Treat missing membership as index/data divergence.
            if !entry.contains(&entity_key) {
                return Err(IndexRemoveError::Corruption(
                    IndexEntryCorruption::missing_key(raw_key, entity_key),
                ));
            }
            entry.remove_key(&entity_key);
            if entry.is_empty() {
                self.remove(&raw_key);
            } else {
                let raw_entry = RawIndexEntry::try_from_entry(&entry).map_err(|err| match err {
                    IndexEntryEncodeError::TooManyKeys { keys } => {
                        IndexEntryCorruption::TooManyKeys { count: keys }.into()
                    }
                    IndexEntryEncodeError::KeyEncoding(err) => IndexRemoveError::KeyEncoding(err),
                })?;
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
