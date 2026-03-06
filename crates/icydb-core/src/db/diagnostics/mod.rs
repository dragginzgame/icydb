//! Module: diagnostics
//! Responsibility: read-only storage footprint and integrity snapshots.
//! Does not own: recovery, write-path mutation, or query planning semantics.
//! Boundary: consumes `Db`/store read APIs and returns DTO snapshots.

mod execution_trace;
#[cfg(test)]
mod tests;

use crate::{
    db::{
        Db, EntityName,
        data::{DataKey, StorageKey},
        index::IndexKey,
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};
use candid::CandidType;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub use execution_trace::{
    ExecutionAccessPathVariant, ExecutionMetrics, ExecutionOptimization, ExecutionTrace,
};

///
/// StorageReport
/// Live storage snapshot report
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct StorageReport {
    pub(crate) storage_data: Vec<DataStoreSnapshot>,
    pub(crate) storage_index: Vec<IndexStoreSnapshot>,
    pub(crate) entity_storage: Vec<EntitySnapshot>,
    pub(crate) corrupted_keys: u64,
    pub(crate) corrupted_entries: u64,
}

impl StorageReport {
    /// Construct one storage report payload.
    #[must_use]
    pub const fn new(
        storage_data: Vec<DataStoreSnapshot>,
        storage_index: Vec<IndexStoreSnapshot>,
        entity_storage: Vec<EntitySnapshot>,
        corrupted_keys: u64,
        corrupted_entries: u64,
    ) -> Self {
        Self {
            storage_data,
            storage_index,
            entity_storage,
            corrupted_keys,
            corrupted_entries,
        }
    }

    /// Borrow data-store snapshots.
    #[must_use]
    pub const fn storage_data(&self) -> &[DataStoreSnapshot] {
        self.storage_data.as_slice()
    }

    /// Borrow index-store snapshots.
    #[must_use]
    pub const fn storage_index(&self) -> &[IndexStoreSnapshot] {
        self.storage_index.as_slice()
    }

    /// Borrow entity-level storage snapshots.
    #[must_use]
    pub const fn entity_storage(&self) -> &[EntitySnapshot] {
        self.entity_storage.as_slice()
    }

    /// Return count of corrupted decoded data keys.
    #[must_use]
    pub const fn corrupted_keys(&self) -> u64 {
        self.corrupted_keys
    }

    /// Return count of corrupted index entries.
    #[must_use]
    pub const fn corrupted_entries(&self) -> u64 {
        self.corrupted_entries
    }
}

///
/// DataStoreSnapshot
/// Store-level snapshot metrics.
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct DataStoreSnapshot {
    pub(crate) path: String,
    pub(crate) entries: u64,
    pub(crate) memory_bytes: u64,
}

impl DataStoreSnapshot {
    /// Construct one data-store snapshot row.
    #[must_use]
    pub const fn new(path: String, entries: u64, memory_bytes: u64) -> Self {
        Self {
            path,
            entries,
            memory_bytes,
        }
    }

    /// Borrow store path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return row count.
    #[must_use]
    pub const fn entries(&self) -> u64 {
        self.entries
    }

    /// Return memory usage in bytes.
    #[must_use]
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }
}

///
/// IndexStoreSnapshot
/// Index-store snapshot metrics
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct IndexStoreSnapshot {
    pub(crate) path: String,
    pub(crate) entries: u64,
    pub(crate) user_entries: u64,
    pub(crate) system_entries: u64,
    pub(crate) memory_bytes: u64,
}

impl IndexStoreSnapshot {
    /// Construct one index-store snapshot row.
    #[must_use]
    pub const fn new(
        path: String,
        entries: u64,
        user_entries: u64,
        system_entries: u64,
        memory_bytes: u64,
    ) -> Self {
        Self {
            path,
            entries,
            user_entries,
            system_entries,
            memory_bytes,
        }
    }

    /// Borrow store path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return total entry count.
    #[must_use]
    pub const fn entries(&self) -> u64 {
        self.entries
    }

    /// Return user-namespace entry count.
    #[must_use]
    pub const fn user_entries(&self) -> u64 {
        self.user_entries
    }

    /// Return system-namespace entry count.
    #[must_use]
    pub const fn system_entries(&self) -> u64 {
        self.system_entries
    }

    /// Return memory usage in bytes.
    #[must_use]
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }
}

///
/// EntitySnapshot
/// Per-entity storage breakdown across stores
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EntitySnapshot {
    /// Store path (e.g., icydb_schema_tests::schema::TestDataStore)
    pub(crate) store: String,

    /// Entity path (e.g., icydb_schema_tests::canister::db::Index)
    pub(crate) path: String,

    /// Number of rows for this entity in the store
    pub(crate) entries: u64,

    /// Approximate bytes used (key + value)
    pub(crate) memory_bytes: u64,

    /// Minimum primary key for this entity (entity-local ordering)
    pub(crate) min_key: Option<Value>,

    /// Maximum primary key for this entity (entity-local ordering)
    pub(crate) max_key: Option<Value>,
}

impl EntitySnapshot {
    /// Construct one entity-storage snapshot row.
    #[must_use]
    pub const fn new(
        store: String,
        path: String,
        entries: u64,
        memory_bytes: u64,
        min_key: Option<Value>,
        max_key: Option<Value>,
    ) -> Self {
        Self {
            store,
            path,
            entries,
            memory_bytes,
            min_key,
            max_key,
        }
    }

    /// Borrow store path.
    #[must_use]
    pub const fn store(&self) -> &str {
        self.store.as_str()
    }

    /// Borrow entity path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return row count.
    #[must_use]
    pub const fn entries(&self) -> u64 {
        self.entries
    }

    /// Return memory usage in bytes.
    #[must_use]
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    /// Borrow optional minimum primary key.
    #[must_use]
    pub const fn min_key(&self) -> Option<&Value> {
        self.min_key.as_ref()
    }

    /// Borrow optional maximum primary key.
    #[must_use]
    pub const fn max_key(&self) -> Option<&Value> {
        self.max_key.as_ref()
    }
}

///
/// EntityStats
/// Internal struct for building per-entity stats before snapshotting.
///

#[derive(Default)]
struct EntityStats {
    entries: u64,
    memory_bytes: u64,
    min_key: Option<StorageKey>,
    max_key: Option<StorageKey>,
}

impl EntityStats {
    // Accumulate per-entity counters and keep min/max over entity-local storage keys.
    fn update(&mut self, dk: &DataKey, value_len: u64) {
        self.entries = self.entries.saturating_add(1);
        self.memory_bytes = self
            .memory_bytes
            .saturating_add(DataKey::entry_size_bytes(value_len));

        let k = dk.storage_key();

        match &mut self.min_key {
            Some(min) if k < *min => *min = k,
            None => self.min_key = Some(k),
            _ => {}
        }

        match &mut self.max_key {
            Some(max) if k > *max => *max = k,
            None => self.max_key = Some(k),
            _ => {}
        }
    }
}

/// Build one deterministic storage snapshot with per-entity rollups.
///
/// This path is read-only and fail-closed on decode/validation errors by counting
/// corrupted keys/entries instead of panicking.
pub(crate) fn storage_report<C: CanisterKind>(
    db: &Db<C>,
    name_to_path: &[(&'static str, &'static str)],
) -> Result<StorageReport, InternalError> {
    db.ensure_recovered_state()?;
    // Build name→path map once, reuse across stores.
    let name_map: BTreeMap<&'static str, &str> = name_to_path.iter().copied().collect();
    let mut data = Vec::new();
    let mut index = Vec::new();
    let mut entity_storage: Vec<EntitySnapshot> = Vec::new();
    let mut corrupted_keys = 0u64;
    let mut corrupted_entries = 0u64;

    db.with_store_registry(|reg| {
        // Keep diagnostics snapshots deterministic by traversing stores in path order.
        let mut stores = reg.iter().collect::<Vec<_>>();
        stores.sort_by_key(|(path, _)| *path);

        for (path, store_handle) in stores {
            // Phase 1: collect data-store snapshots and per-entity stats.
            store_handle.with_data(|store| {
                data.push(DataStoreSnapshot::new(
                    path.to_string(),
                    store.len(),
                    store.memory_bytes(),
                ));

                // Track per-entity counts, memory, and min/max Keys (not DataKeys)
                let mut by_entity: BTreeMap<EntityName, EntityStats> = BTreeMap::new();

                for entry in store.iter() {
                    let Ok(dk) = DataKey::try_from_raw(entry.key()) else {
                        corrupted_keys = corrupted_keys.saturating_add(1);
                        continue;
                    };

                    let value_len = entry.value().len() as u64;

                    by_entity
                        .entry(*dk.entity_name())
                        .or_default()
                        .update(&dk, value_len);
                }

                for (entity_name, stats) in by_entity {
                    let path_name = name_map
                        .get(entity_name.as_str())
                        .copied()
                        .unwrap_or(entity_name.as_str());
                    entity_storage.push(EntitySnapshot::new(
                        path.to_string(),
                        path_name.to_string(),
                        stats.entries,
                        stats.memory_bytes,
                        stats.min_key.map(|key| key.as_value()),
                        stats.max_key.map(|key| key.as_value()),
                    ));
                }
            });

            // Phase 2: collect index-store snapshots and integrity counters.
            store_handle.with_index(|store| {
                let mut user_entries = 0u64;
                let mut system_entries = 0u64;

                for (key, value) in store.entries() {
                    let Ok(decoded_key) = IndexKey::try_from_raw(&key) else {
                        corrupted_entries = corrupted_entries.saturating_add(1);
                        continue;
                    };

                    if decoded_key.uses_system_namespace() {
                        system_entries = system_entries.saturating_add(1);
                    } else {
                        user_entries = user_entries.saturating_add(1);
                    }

                    if value.validate().is_err() {
                        corrupted_entries = corrupted_entries.saturating_add(1);
                    }
                }

                index.push(IndexStoreSnapshot::new(
                    path.to_string(),
                    store.len(),
                    user_entries,
                    system_entries,
                    store.memory_bytes(),
                ));
            });
        }
    });

    // Phase 3: enforce deterministic entity snapshot emission order.
    // This remains stable even if outer store traversal internals change.
    entity_storage
        .sort_by(|left, right| (left.store(), left.path()).cmp(&(right.store(), right.path())));

    Ok(StorageReport::new(
        data,
        index,
        entity_storage,
        corrupted_keys,
        corrupted_entries,
    ))
}
