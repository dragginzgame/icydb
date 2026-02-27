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

///
/// StorageReport
/// Live storage snapshot report
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct StorageReport {
    pub storage_data: Vec<DataStoreSnapshot>,
    pub storage_index: Vec<IndexStoreSnapshot>,
    pub entity_storage: Vec<EntitySnapshot>,
    pub corrupted_keys: u64,
    pub corrupted_entries: u64,
}

///
/// DataStoreSnapshot
/// Store-level snapshot metrics.
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct DataStoreSnapshot {
    pub path: String,
    pub entries: u64,
    pub memory_bytes: u64,
}

///
/// IndexStoreSnapshot
/// Index-store snapshot metrics
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct IndexStoreSnapshot {
    pub path: String,
    pub entries: u64,
    pub user_entries: u64,
    pub system_entries: u64,
    pub memory_bytes: u64,
}

///
/// EntitySnapshot
/// Per-entity storage breakdown across stores
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EntitySnapshot {
    /// Store path (e.g., icydb_schema_tests::schema::TestDataStore)
    pub store: String,

    /// Entity path (e.g., icydb_schema_tests::canister::db::Index)
    pub path: String,

    /// Number of rows for this entity in the store
    pub entries: u64,

    /// Approximate bytes used (key + value)
    pub memory_bytes: u64,

    /// Minimum primary key for this entity (entity-local ordering)
    pub min_key: Option<Value>,

    /// Maximum primary key for this entity (entity-local ordering)
    pub max_key: Option<Value>,
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

/// Build storage snapshot and per-entity breakdown; enrich path names using name→path map
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
                data.push(DataStoreSnapshot {
                    path: path.to_string(),
                    entries: store.len(),
                    memory_bytes: store.memory_bytes(),
                });

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
                    entity_storage.push(EntitySnapshot {
                        store: path.to_string(),
                        path: path_name.to_string(),
                        entries: stats.entries,
                        memory_bytes: stats.memory_bytes,
                        min_key: stats.min_key.map(|key| key.as_value()),
                        max_key: stats.max_key.map(|key| key.as_value()),
                    });
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

                index.push(IndexStoreSnapshot {
                    path: path.to_string(),
                    entries: store.len(),
                    user_entries,
                    system_entries,
                    memory_bytes: store.memory_bytes(),
                });
            });
        }
    });

    // Keep entity snapshot emission deterministic as an explicit contract,
    // independent of outer store traversal implementation details.
    entity_storage.sort_by(|left, right| {
        (left.store.as_str(), left.path.as_str()).cmp(&(right.store.as_str(), right.path.as_str()))
    });

    Ok(StorageReport {
        storage_data: data,
        storage_index: index,
        entity_storage,
        corrupted_keys,
        corrupted_entries,
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
