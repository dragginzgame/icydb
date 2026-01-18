use crate::{
    db::store::EntityName,
    db::{Db, store::DataKey},
    key::Key,
    traits::CanisterKind,
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
    pub memory_bytes: u64,
}

///
/// EntitySnapshot
/// Per-entity storage breakdown across stores
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EntitySnapshot {
    /// Store path (e.g., test_design::schema::TestDataStore)
    pub store: String,
    /// Entity path (e.g., test_design::canister::db::Index)
    pub path: String,
    /// Number of rows for this entity in the store
    pub entries: u64,
    /// Approximate bytes used (key + value)
    pub memory_bytes: u64,
    /// Minimum primary key for this entity (entity-local ordering)
    pub min_key: Option<Key>,
    /// Maximum primary key for this entity (entity-local ordering)
    pub max_key: Option<Key>,
}

///
/// EntityStats
/// Internal struct for building per-entity stats before snapshotting.
///

#[derive(Default)]
struct EntityStats {
    entries: u64,
    memory_bytes: u64,
    min_key: Option<Key>,
    max_key: Option<Key>,
}

impl EntityStats {
    fn update(&mut self, dk: &DataKey, value_len: u64) {
        self.entries = self.entries.saturating_add(1);
        self.memory_bytes = self
            .memory_bytes
            .saturating_add(DataKey::entry_size_bytes(value_len));

        let k = dk.key();

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
#[must_use]
pub fn storage_report<C: CanisterKind>(
    db: &Db<C>,
    name_to_path: &[(&'static str, &'static str)],
) -> StorageReport {
    // Build name→path map once, reuse across stores
    let name_map: BTreeMap<&'static str, &str> = name_to_path.iter().copied().collect();
    let mut data = Vec::new();
    let mut index = Vec::new();
    let mut entity_storage: Vec<EntitySnapshot> = Vec::new();

    db.with_data(|reg| {
        reg.for_each(|path, store| {
            data.push(DataStoreSnapshot {
                path: path.to_string(),
                entries: store.len(),
                memory_bytes: store.memory_bytes(),
            });

            // Track per-entity counts, memory, and min/max Keys (not DataKeys)
            let mut by_entity: BTreeMap<EntityName, EntityStats> = BTreeMap::new();

            for entry in store.iter() {
                let dk = entry.key();
                let value_len = entry.value().len() as u64;
                by_entity
                    .entry(*dk.entity_name())
                    .or_default()
                    .update(dk, value_len);
            }

            for (entity_name, stats) in by_entity {
                let path_name = name_map.get(entity_name.as_str()).copied().unwrap_or("");
                entity_storage.push(EntitySnapshot {
                    store: path.to_string(),
                    path: path_name.to_string(),
                    entries: stats.entries,
                    memory_bytes: stats.memory_bytes,
                    min_key: stats.min_key,
                    max_key: stats.max_key,
                });
            }
        });
    });

    db.with_index(|reg| {
        reg.for_each(|path, store| {
            index.push(IndexStoreSnapshot {
                path: path.to_string(),
                entries: store.len(),
                memory_bytes: store.memory_bytes(),
            });
        });
    });

    StorageReport {
        storage_data: data,
        storage_index: index,
        entity_storage,
    }
}
