use crate::{
    db::{
        Db,
        commit::{CommitDataOp, CommitIndexOp, CommitKind, CommitMarker, with_commit_store},
        index::{IndexEntry, IndexKey, IndexStore, IndexStoreRegistry, RawIndexEntry},
        store::{DataKey, DataStore, DataStoreRegistry, RawRow},
    },
    error::{ErrorClass, ErrorOrigin},
    serialize::serialize,
    test_support::{TEST_DATA_STORE_PATH, TEST_INDEX_STORE_PATH, TestCanister},
    traits::{
        EntityKind, EntityValue, FieldValues, SanitizeAuto, SanitizeCustom, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    types::Ulid,
    value::Value,
};
use canic_memory::runtime::registry::MemoryRegistryRuntime;
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, sync::Once};

// ---------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------

const DATA_STORE_PATH: &str = TEST_DATA_STORE_PATH;
const INDEX_STORE_PATH: &str = TEST_INDEX_STORE_PATH;

// ---------------------------------------------------------------------
// Entity
// ---------------------------------------------------------------------

crate::test_entity! {
    ///
    /// TestEntity
    ///
    #[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
    struct TestEntity {
        id: Ulid,
        name: String,
    }

    path: "commit_test::TestEntity",
    pk: id,

    fields {
        id: Ulid,
        name: Text,
    }

    indexes {
        index idx_0(name) unique;
    }

    impls {
        ViewClone,
        SanitizeAuto,
        SanitizeCustom,
        ValidateAuto,
        ValidateCustom,
        Visitable,
    }
}

// ---------------------------------------------------------------------
// Stores & DB
// ---------------------------------------------------------------------

canic_memory::eager_static! {
    static TEST_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(canic_memory::ic_memory!(DataStore, 10)));
}

canic_memory::eager_static! {
    static TEST_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(
            canic_memory::ic_memory!(IndexStore, 11),
            canic_memory::ic_memory!(IndexStore, 12),
        ));
}

thread_local! {
    static DATA_REGISTRY: DataStoreRegistry = {
        let mut reg = DataStoreRegistry::new();
        reg.register(DATA_STORE_PATH, &TEST_DATA_STORE);
        reg
    };

    static INDEX_REGISTRY: IndexStoreRegistry = {
        let mut reg = IndexStoreRegistry::new();
        reg.register(INDEX_STORE_PATH, &TEST_INDEX_STORE);
        reg
    };
}

static DB: Db<TestCanister> = Db::new(&DATA_REGISTRY, &INDEX_REGISTRY, &[]);

// ---------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------

canic_memory::eager_init!({
    canic_memory::ic_memory_range!(0, 20);
});

static INIT_REGISTRY: Once = Once::new();

fn init_memory_registry() {
    INIT_REGISTRY.call_once(|| {
        MemoryRegistryRuntime::init(Some((env!("CARGO_PKG_NAME"), 0, 20)))
            .expect("memory registry init");
    });
}

fn reset_stores() {
    TEST_DATA_STORE.with_borrow_mut(DataStore::clear);
    TEST_INDEX_STORE.with_borrow_mut(IndexStore::clear);
    init_memory_registry();
    let _ = with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    });
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[test]
fn commit_marker_recovery_rejects_corrupted_index_key() {
    reset_stores();

    let entity = TestEntity {
        id: Ulid::from_u128(7),
        name: "alpha".to_string(),
    };

    let data_key = DataKey::try_new::<TestEntity>(entity.id).unwrap();
    let raw_data_key = data_key.to_raw().expect("data key encode");
    let raw_row = RawRow::try_new(serialize(&entity).unwrap()).unwrap();

    let index_key = IndexKey::new(&entity, <TestEntity as EntityKind>::INDEXES[0])
        .expect("index key")
        .expect("index key missing");
    let raw_index_key = index_key.to_raw();

    let entry = IndexEntry::new(entity.id);
    let raw_index_entry = RawIndexEntry::try_from_entry(&entry).unwrap();

    let mut marker = CommitMarker::new(
        CommitKind::Save,
        vec![CommitIndexOp {
            store: INDEX_STORE_PATH.to_string(),
            key: raw_index_key.as_bytes().to_vec(),
            value: Some(raw_index_entry.as_bytes().to_vec()),
        }],
        vec![CommitDataOp {
            store: DATA_STORE_PATH.to_string(),
            key: raw_data_key.as_bytes().to_vec(),
            value: Some(raw_row.as_bytes().to_vec()),
        }],
    )
    .unwrap();

    // Corrupt the index key
    marker.index_ops[0]
        .key
        .last_mut()
        .unwrap()
        .bitxor_assign(0xFF);

    let _guard = begin_commit(marker).unwrap();
    force_recovery_for_tests();

    let err = ensure_recovered(&DB).expect_err("corrupted marker should fail");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Index);
}

#[test]
fn recovery_rejects_delete_marker_with_payload() {
    reset_stores();

    let entity = TestEntity {
        id: Ulid::from_u128(8),
        name: "alpha".to_string(),
    };

    let data_key = DataKey::try_new::<TestEntity>(entity.id).unwrap();
    let raw_data_key = data_key.to_raw().expect("data key encode");
    let raw_row = RawRow::try_new(serialize(&entity).unwrap()).unwrap();

    let marker = CommitMarker::new(
        CommitKind::Delete,
        vec![],
        vec![CommitDataOp {
            store: DATA_STORE_PATH.to_string(),
            key: raw_data_key.as_bytes().to_vec(),
            value: Some(raw_row.as_bytes().to_vec()),
        }],
    )
    .unwrap();

    let _guard = begin_commit(marker).unwrap();
    force_recovery_for_tests();

    let err = ensure_recovered(&DB).expect_err("delete payload should fail");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}
