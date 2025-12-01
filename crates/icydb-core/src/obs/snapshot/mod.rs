use crate::{
    db::{Db, store::DataKey},
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
    /// Minimum DataKey for this entity (by full DataKey ordering)
    pub min_key: Option<DataKey>,
    /// Maximum DataKey for this entity (by full DataKey ordering)
    pub max_key: Option<DataKey>,
}

///
/// EntityStats
/// Internal struct for building per-entity stats before snapshotting.
///

#[derive(Default)]
struct EntityStats {
    entries: u64,
    memory_bytes: u64,
    min_key: Option<DataKey>,
    max_key: Option<DataKey>,
}

impl EntityStats {
    fn update(&mut self, dk: &DataKey, value_len: u64) {
        self.entries = self.entries.saturating_add(1);
        self.memory_bytes = self
            .memory_bytes
            .saturating_add(DataKey::entry_size_bytes(value_len));

        match &mut self.min_key {
            Some(min) if dk < min => *min = dk.clone(),
            None => self.min_key = Some(dk.clone()),
            _ => {}
        }

        match &mut self.max_key {
            Some(max) if dk > max => *max = dk.clone(),
            None => self.max_key = Some(dk.clone()),
            _ => {}
        }
    }
}

/// Build storage snapshot and per-entity breakdown; enrich path names using id→path map
#[must_use]
pub fn storage_report<C: CanisterKind>(
    db: &Db<C>,
    id_to_path: &[(u64, &'static str)],
) -> StorageReport {
    // Build id→path map once, reuse across stores
    let id_map: BTreeMap<u64, &str> = id_to_path.iter().copied().collect();
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

            // Track per-entity counts, memory, and min/max DataKey
            let mut by_entity: BTreeMap<u64, EntityStats> = BTreeMap::new();

            for entry in store.iter() {
                let dk = entry.key();
                let value_len = entry.value().len() as u64;
                by_entity
                    .entry(dk.entity_id())
                    .or_default()
                    .update(dk, value_len);
            }

            for (entity_id, stats) in by_entity {
                let path_name = id_map.get(&entity_id).copied().unwrap_or("");
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
