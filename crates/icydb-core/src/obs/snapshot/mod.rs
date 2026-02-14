use crate::{
    db::{
        Db, ensure_recovered,
        identity::EntityName,
        index::IndexKey,
        store::{DataKey, StorageKey},
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
pub fn storage_report<C: CanisterKind>(
    db: &Db<C>,
    name_to_path: &[(&'static str, &'static str)],
) -> Result<StorageReport, InternalError> {
    ensure_recovered(db)?;
    // Build name→path map once, reuse across stores
    let name_map: BTreeMap<&'static str, &str> = name_to_path.iter().copied().collect();
    let mut data = Vec::new();
    let mut index = Vec::new();
    let mut entity_storage: Vec<EntitySnapshot> = Vec::new();
    let mut corrupted_keys = 0u64;
    let mut corrupted_entries = 0u64;

    db.with_store_registry(|reg| {
        reg.iter().for_each(|(path, store_handle)| {
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
                    let path_name = name_map.get(entity_name.as_str()).copied().unwrap_or("");
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
        });
    });

    db.with_store_registry(|reg| {
        reg.iter().for_each(|(path, store_handle)| {
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
        });
    });

    Ok(StorageReport {
        storage_data: data,
        storage_index: index,
        entity_storage,
        corrupted_keys,
        corrupted_entries,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            Db,
            identity::{EntityName, IndexName},
            index::{IndexId, IndexKey, IndexStore, RawIndexEntry},
            init_commit_store_for_tests,
            store::{DataKey, DataStore, RawRow, StorageKey, StoreRegistry},
        },
        obs::snapshot::storage_report,
        test_support::test_memory,
        traits::{CanisterKind, Path, Storable},
    };
    use std::{borrow::Cow, cell::RefCell};

    const STORE_PATH: &str = "snapshot_tests::Store";

    struct SnapshotTestCanister;

    impl Path for SnapshotTestCanister {
        const PATH: &'static str = "snapshot_tests::Canister";
    }

    impl CanisterKind for SnapshotTestCanister {}

    thread_local! {
        static SNAPSHOT_DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(101)));
        static SNAPSHOT_INDEX_STORE: RefCell<IndexStore> =
            RefCell::new(IndexStore::init(test_memory(102)));
        static SNAPSHOT_STORE_REGISTRY: StoreRegistry = {
            let mut reg = StoreRegistry::new();
            reg.register_store(STORE_PATH, &SNAPSHOT_DATA_STORE, &SNAPSHOT_INDEX_STORE)
                .expect("snapshot store registration should succeed");
            reg
        };
    }

    static DB: Db<SnapshotTestCanister> = Db::new(&SNAPSHOT_STORE_REGISTRY);

    fn with_snapshot_store<R>(f: impl FnOnce(crate::db::store::StoreHandle) -> R) -> R {
        DB.with_store_registry(|reg| reg.try_get_store(STORE_PATH).map(f))
            .expect("snapshot store access should succeed")
    }

    fn reset_snapshot_state() {
        init_commit_store_for_tests().expect("commit store init should succeed");

        with_snapshot_store(|store| {
            store.with_data_mut(DataStore::clear);
            store.with_index_mut(IndexStore::clear);
        });
    }

    #[test]
    fn storage_report_lists_registered_store_snapshots() {
        reset_snapshot_state();

        let report = storage_report(&DB, &[]).expect("storage report should succeed");
        assert_eq!(report.storage_data.len(), 1);
        assert_eq!(report.storage_data[0].path, STORE_PATH);
        assert_eq!(report.storage_data[0].entries, 0);
        assert_eq!(report.storage_index.len(), 1);
        assert_eq!(report.storage_index[0].path, STORE_PATH);
        assert_eq!(report.storage_index[0].entries, 0);
        assert_eq!(report.storage_index[0].user_entries, 0);
        assert_eq!(report.storage_index[0].system_entries, 0);
        assert!(report.entity_storage.is_empty());
        assert_eq!(report.corrupted_keys, 0);
        assert_eq!(report.corrupted_entries, 0);
    }

    #[test]
    fn storage_report_counts_entity_rows_and_corrupted_index_entries() {
        reset_snapshot_state();

        let data_key = DataKey::max_storable()
            .to_raw()
            .expect("max storable data key should encode");
        let row = RawRow::try_new(vec![1, 2, 3]).expect("row bytes should be valid");
        with_snapshot_store(|store| {
            store.with_data_mut(|data_store| {
                data_store.insert(data_key, row);
            });
        });

        let index_key = IndexKey::empty(IndexId::max_storable()).to_raw();
        let malformed_index_entry = RawIndexEntry::from_bytes(Cow::Owned(vec![0, 0, 0, 0]));
        with_snapshot_store(|store| {
            store.with_index_mut(|index_store| {
                index_store.insert(index_key, malformed_index_entry);
            });
        });

        let report = storage_report(&DB, &[]).expect("storage report should succeed");
        assert_eq!(report.storage_data[0].entries, 1);
        assert_eq!(report.storage_index[0].entries, 1);
        assert_eq!(report.storage_index[0].user_entries, 1);
        assert_eq!(report.storage_index[0].system_entries, 0);
        assert_eq!(report.entity_storage.len(), 1);
        assert_eq!(report.entity_storage[0].path, "");
        assert_eq!(report.entity_storage[0].entries, 1);
        assert!(report.entity_storage[0].min_key.is_some());
        assert!(report.entity_storage[0].max_key.is_some());
        assert_eq!(report.corrupted_entries, 1);
        assert_eq!(report.corrupted_keys, 0);
    }

    #[test]
    fn storage_report_splits_user_and_system_index_entries() {
        reset_snapshot_state();

        let entity = EntityName::try_from_str("snapshot_entity").expect("entity name should parse");
        let user_index = IndexName::try_from_parts(&entity, &["email"]).expect("index name");
        let system_index = IndexName::try_from_parts(&entity, &["~ri"]).expect("index name");
        let user_key = IndexKey::empty(IndexId(user_index)).to_raw();
        let system_key = IndexKey::empty(IndexId(system_index)).to_raw();
        let entry = RawIndexEntry::try_from_keys([StorageKey::max_storable()])
            .expect("entry should encode");

        with_snapshot_store(|store| {
            store.with_index_mut(|index_store| {
                index_store.insert(user_key, entry.clone());
                index_store.insert(system_key, entry);
            });
        });

        let report = storage_report(&DB, &[]).expect("storage report should succeed");
        assert_eq!(report.storage_index[0].entries, 2);
        assert_eq!(report.storage_index[0].user_entries, 1);
        assert_eq!(report.storage_index[0].system_entries, 1);
        assert_eq!(report.corrupted_entries, 0);
    }
}
