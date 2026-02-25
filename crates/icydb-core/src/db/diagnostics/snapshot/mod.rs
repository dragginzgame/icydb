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
mod tests {
    use crate::{
        db::{
            Db,
            commit::{ensure_recovered_for_write, init_commit_store_for_tests},
            data::{DataKey, DataStore, RawDataKey, RawRow, StorageKey},
            identity::{EntityName, IndexName},
            index::{IndexId, IndexKey, IndexKeyKind, IndexStore, RawIndexEntry, RawIndexKey},
            registry::StoreRegistry,
        },
        test_support::test_memory,
        traits::Storable,
    };
    use std::{borrow::Cow, cell::RefCell};

    use super::{StorageReport, storage_report};

    crate::test_canister! {
        ident = DiagnosticsCanister,
        commit_memory_id = 254,
    }

    const STORE_Z_PATH: &str = "diagnostics_tests::z_store";
    const STORE_A_PATH: &str = "diagnostics_tests::a_store";
    const SINGLE_ENTITY_NAME: &str = "diag_single_entity";
    const SINGLE_ENTITY_PATH: &str = "diagnostics_tests::entity::single";
    const FIRST_ENTITY_NAME: &str = "diag_first_entity";
    const FIRST_ENTITY_PATH: &str = "diagnostics_tests::entity::first";
    const SECOND_ENTITY_NAME: &str = "diag_second_entity";
    const SECOND_ENTITY_PATH: &str = "diagnostics_tests::entity::second";
    const MINMAX_ENTITY_NAME: &str = "diag_minmax_entity";
    const MINMAX_ENTITY_PATH: &str = "diagnostics_tests::entity::minmax";
    const VALID_ENTITY_NAME: &str = "diag_valid_entity";
    const VALID_ENTITY_PATH: &str = "diagnostics_tests::entity::valid";

    thread_local! {
        static STORE_Z_DATA: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(153)));
        static STORE_Z_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init(test_memory(154)));
        static STORE_A_DATA: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(155)));
        static STORE_A_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init(test_memory(156)));
        static DIAGNOSTICS_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry
                .register_store(STORE_Z_PATH, &STORE_Z_DATA, &STORE_Z_INDEX)
                .expect("diagnostics test z-store registration should succeed");
            registry
                .register_store(STORE_A_PATH, &STORE_A_DATA, &STORE_A_INDEX)
                .expect("diagnostics test a-store registration should succeed");
            registry
        };
    }

    static DB: Db<DiagnosticsCanister> = Db::new(&DIAGNOSTICS_REGISTRY);

    fn with_data_store_mut<R>(path: &'static str, f: impl FnOnce(&mut DataStore) -> R) -> R {
        DB.with_store_registry(|registry| {
            registry
                .try_get_store(path)
                .map(|store_handle| store_handle.with_data_mut(f))
        })
        .expect("data store lookup should succeed")
    }

    fn with_index_store_mut<R>(path: &'static str, f: impl FnOnce(&mut IndexStore) -> R) -> R {
        DB.with_store_registry(|registry| {
            registry
                .try_get_store(path)
                .map(|store_handle| store_handle.with_index_mut(f))
        })
        .expect("index store lookup should succeed")
    }

    fn reset_stores() {
        init_commit_store_for_tests().expect("commit store init should succeed");
        ensure_recovered_for_write(&DB).expect("write-side recovery should succeed");
        DB.with_store_registry(|registry| {
            // Test cleanup only: this clear-all sweep has set semantics, so
            // `StoreRegistry` HashMap iteration order is intentionally irrelevant.
            for (_, store_handle) in registry.iter() {
                store_handle.with_data_mut(DataStore::clear);
                store_handle.with_index_mut(IndexStore::clear);
            }
        });
    }

    fn insert_data_row(path: &'static str, entity_name: &str, key: StorageKey, row_len: usize) {
        let entity =
            EntityName::try_from_str(entity_name).expect("diagnostics test entity name is valid");
        let raw_key = DataKey::raw_from_parts(entity, key)
            .expect("diagnostics test data key should encode from valid parts");
        let row_bytes = vec![0xAB; row_len.max(1)];
        let raw_row = RawRow::try_new(row_bytes).expect("diagnostics test row should encode");

        with_data_store_mut(path, |store| {
            store.insert(raw_key, raw_row);
        });
    }

    fn insert_corrupted_data_key(path: &'static str) {
        let valid = DataKey::raw_from_parts(
            EntityName::try_from_str(VALID_ENTITY_NAME).expect("valid test entity name"),
            StorageKey::Int(1),
        )
        .expect("valid data key should encode");

        let mut corrupted_bytes = valid.as_bytes().to_vec();
        corrupted_bytes[0] = 0;
        let corrupted_key = <RawDataKey as Storable>::from_bytes(Cow::Owned(corrupted_bytes));
        let raw_row = RawRow::try_new(vec![0xCD]).expect("diagnostics test row should encode");

        with_data_store_mut(path, |store| {
            store.insert(corrupted_key, raw_row);
        });
    }

    fn index_id(entity_name: &str, field: &str) -> IndexId {
        let entity =
            EntityName::try_from_str(entity_name).expect("diagnostics test entity name is valid");
        let name = IndexName::try_from_parts(&entity, &[field])
            .expect("diagnostics test index name should encode");

        IndexId(name)
    }

    fn index_key(kind: IndexKeyKind, entity_name: &str, field: &str) -> RawIndexKey {
        let id = index_id(entity_name, field);
        IndexKey::empty_with_kind(&id, kind).to_raw()
    }

    fn insert_index_entry(path: &'static str, key: RawIndexKey, entry: RawIndexEntry) {
        with_index_store_mut(path, |store| {
            store.insert(key, entry);
        });
    }

    fn diagnostics_report(name_to_path: &[(&'static str, &'static str)]) -> StorageReport {
        storage_report(&DB, name_to_path).expect("diagnostics snapshot should succeed")
    }

    fn data_paths(report: &StorageReport) -> Vec<&str> {
        report
            .storage_data
            .iter()
            .map(|snapshot| snapshot.path.as_str())
            .collect()
    }

    fn index_paths(report: &StorageReport) -> Vec<&str> {
        report
            .storage_index
            .iter()
            .map(|snapshot| snapshot.path.as_str())
            .collect()
    }

    fn entity_store_paths(report: &StorageReport) -> Vec<(&str, &str)> {
        report
            .entity_storage
            .iter()
            .map(|snapshot| (snapshot.store.as_str(), snapshot.path.as_str()))
            .collect()
    }

    #[test]
    fn storage_report_empty_store_snapshot() {
        reset_stores();

        let report = diagnostics_report(&[]);

        assert_eq!(report.corrupted_keys, 0);
        assert_eq!(report.corrupted_entries, 0);
        assert!(report.entity_storage.is_empty());

        assert_eq!(data_paths(&report), vec![STORE_A_PATH, STORE_Z_PATH]);
        assert_eq!(index_paths(&report), vec![STORE_A_PATH, STORE_Z_PATH]);
        assert!(
            report
                .storage_data
                .iter()
                .all(|snapshot| snapshot.entries == 0)
        );
        assert!(
            report
                .storage_index
                .iter()
                .all(|snapshot| snapshot.entries == 0)
        );
    }

    #[test]
    fn storage_report_single_entity_multiple_rows() {
        reset_stores();

        insert_data_row(STORE_A_PATH, SINGLE_ENTITY_NAME, StorageKey::Int(3), 3);
        insert_data_row(STORE_A_PATH, SINGLE_ENTITY_NAME, StorageKey::Int(1), 1);
        insert_data_row(STORE_A_PATH, SINGLE_ENTITY_NAME, StorageKey::Int(2), 2);

        let report = diagnostics_report(&[(SINGLE_ENTITY_NAME, SINGLE_ENTITY_PATH)]);
        let entity_snapshot = report
            .entity_storage
            .iter()
            .find(|snapshot| snapshot.store == STORE_A_PATH && snapshot.path == SINGLE_ENTITY_PATH)
            .expect("single-entity snapshot should exist");

        assert_eq!(entity_snapshot.entries, 3);
    }

    #[test]
    fn storage_report_multiple_entities_in_same_store() {
        reset_stores();

        insert_data_row(STORE_A_PATH, FIRST_ENTITY_NAME, StorageKey::Int(10), 1);
        insert_data_row(STORE_A_PATH, FIRST_ENTITY_NAME, StorageKey::Int(11), 1);
        insert_data_row(STORE_A_PATH, SECOND_ENTITY_NAME, StorageKey::Int(20), 1);

        let report = diagnostics_report(&[
            (FIRST_ENTITY_NAME, FIRST_ENTITY_PATH),
            (SECOND_ENTITY_NAME, SECOND_ENTITY_PATH),
        ]);

        let first = report
            .entity_storage
            .iter()
            .find(|snapshot| snapshot.store == STORE_A_PATH && snapshot.path == FIRST_ENTITY_PATH)
            .expect("first-entity snapshot should exist");
        let second = report
            .entity_storage
            .iter()
            .find(|snapshot| snapshot.store == STORE_A_PATH && snapshot.path == SECOND_ENTITY_PATH)
            .expect("second-entity snapshot should exist");

        assert_eq!(first.entries, 2);
        assert_eq!(second.entries, 1);
    }

    #[test]
    fn storage_report_entity_snapshots_are_sorted_by_store_then_path() {
        reset_stores();

        insert_data_row(STORE_Z_PATH, FIRST_ENTITY_NAME, StorageKey::Int(1), 1);
        insert_data_row(STORE_A_PATH, SECOND_ENTITY_NAME, StorageKey::Int(2), 1);
        insert_data_row(STORE_A_PATH, FIRST_ENTITY_NAME, StorageKey::Int(3), 1);

        let report = diagnostics_report(&[
            (FIRST_ENTITY_NAME, "diagnostics_tests::entity::z_first"),
            (SECOND_ENTITY_NAME, "diagnostics_tests::entity::a_second"),
        ]);

        assert_eq!(
            entity_store_paths(&report),
            vec![
                (STORE_A_PATH, "diagnostics_tests::entity::a_second"),
                (STORE_A_PATH, "diagnostics_tests::entity::z_first"),
                (STORE_Z_PATH, "diagnostics_tests::entity::z_first"),
            ]
        );
    }

    #[test]
    fn storage_report_min_max_key_correctness() {
        reset_stores();

        insert_data_row(STORE_A_PATH, MINMAX_ENTITY_NAME, StorageKey::Int(9), 1);
        insert_data_row(STORE_A_PATH, MINMAX_ENTITY_NAME, StorageKey::Int(-5), 1);
        insert_data_row(STORE_A_PATH, MINMAX_ENTITY_NAME, StorageKey::Int(3), 1);

        let report = diagnostics_report(&[(MINMAX_ENTITY_NAME, MINMAX_ENTITY_PATH)]);
        let entity_snapshot = report
            .entity_storage
            .iter()
            .find(|snapshot| snapshot.store == STORE_A_PATH && snapshot.path == MINMAX_ENTITY_PATH)
            .expect("min/max snapshot should exist");

        assert_eq!(
            entity_snapshot.min_key,
            Some(StorageKey::Int(-5).as_value())
        );
        assert_eq!(entity_snapshot.max_key, Some(StorageKey::Int(9).as_value()));
    }

    #[test]
    fn storage_report_corrupted_key_detection() {
        reset_stores();

        insert_data_row(STORE_A_PATH, VALID_ENTITY_NAME, StorageKey::Int(7), 1);
        insert_corrupted_data_key(STORE_A_PATH);

        let report = diagnostics_report(&[(VALID_ENTITY_NAME, VALID_ENTITY_PATH)]);

        assert_eq!(report.corrupted_keys, 1);
        let entity_snapshot = report
            .entity_storage
            .iter()
            .find(|snapshot| snapshot.store == STORE_A_PATH && snapshot.path == VALID_ENTITY_PATH)
            .expect("valid-entity snapshot should exist");
        assert_eq!(entity_snapshot.entries, 1);
    }

    #[test]
    fn storage_report_corrupted_index_value_detection() {
        reset_stores();

        let key = index_key(IndexKeyKind::User, "diag_index_entity", "email");
        let corrupted_entry = <RawIndexEntry as Storable>::from_bytes(Cow::Owned(vec![0, 0, 0, 0]));
        insert_index_entry(STORE_A_PATH, key, corrupted_entry);

        let report = diagnostics_report(&[]);
        let index_snapshot = report
            .storage_index
            .iter()
            .find(|snapshot| snapshot.path == STORE_A_PATH)
            .expect("index snapshot should exist");

        assert_eq!(report.corrupted_entries, 1);
        assert_eq!(index_snapshot.entries, 1);
        assert_eq!(index_snapshot.user_entries, 1);
        assert_eq!(index_snapshot.system_entries, 0);
    }

    #[test]
    fn storage_report_system_vs_user_namespace_split() {
        reset_stores();

        let user_key = index_key(IndexKeyKind::User, "diag_namespace_entity", "email");
        let system_key = index_key(IndexKeyKind::System, "diag_namespace_entity", "email");
        let user_entry =
            RawIndexEntry::try_from_keys([StorageKey::Int(1)]).expect("user entry should encode");
        let system_entry =
            RawIndexEntry::try_from_keys([StorageKey::Int(2)]).expect("system entry should encode");
        insert_index_entry(STORE_A_PATH, user_key, user_entry);
        insert_index_entry(STORE_A_PATH, system_key, system_entry);

        let report = diagnostics_report(&[]);
        let index_snapshot = report
            .storage_index
            .iter()
            .find(|snapshot| snapshot.path == STORE_A_PATH)
            .expect("index snapshot should exist");

        assert_eq!(report.corrupted_entries, 0);
        assert_eq!(index_snapshot.entries, 2);
        assert_eq!(index_snapshot.user_entries, 1);
        assert_eq!(index_snapshot.system_entries, 1);
    }
}
