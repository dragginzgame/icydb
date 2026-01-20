use icydb::__internal::core::db::store::{DataKey, IndexEntry, IndexKey, RawIndexEntry, RawRow};
use icydb::{db::UniqueIndexHandle, design::prelude::*, serialize};
use test_design::{e2e::db::Index, schema::TestIndexStore};

///
/// DeleteUniqueSuite
///

pub struct DeleteUniqueSuite;

impl DeleteUniqueSuite {
    pub fn test() {
        let tests: Vec<(&str, fn())> = vec![
            (
                "delete_unique_dangling_index_entry_errors",
                Self::delete_unique_dangling_index_entry_errors,
            ),
            (
                "delete_unique_index_corruption_errors",
                Self::delete_unique_index_corruption_errors,
            ),
            (
                "delete_unique_key_type_mismatch_errors",
                Self::delete_unique_key_type_mismatch_errors,
            ),
            (
                "delete_unique_missing_primary_row_errors",
                Self::delete_unique_missing_primary_row_errors,
            ),
        ];

        for (name, test_fn) in tests {
            crate::clear_test_data_store();
            println!("Running test: {name}");
            test_fn();
        }
    }

    fn delete_unique_dangling_index_entry_errors() {
        let saved = db!().insert(Index::new(1, 55)).unwrap();

        let index = Self::unique_index();
        let index_key = IndexKey::new(&saved, index).expect("index key should be present");

        crate::DATA_REGISTRY
            .with(|reg| {
                reg.with_store_mut(<Index as EntityKind>::Store::PATH, |store| {
                    let data_key = DataKey::new::<Index>(saved.key());
                    let raw = data_key.to_raw();
                    store.remove(&raw);
                })
            })
            .unwrap();

        let err = db!()
            .delete::<Index>()
            .by_unique_index(Self::unique_handle(), Index::new(2, 55))
            .unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("index corrupted") || msg.contains("corruption"),
            "expected corruption error, got: {msg}"
        );

        let _ = crate::INDEX_REGISTRY.with(|reg| {
            reg.with_store_mut(TestIndexStore::PATH, |store| {
                let raw = index_key.to_raw();
                store.remove(&raw)
            })
        });
    }

    fn delete_unique_index_corruption_errors() {
        let saved = db!().insert(Index::new(1, 88)).unwrap();
        let other = db!().insert(Index::new(2, 99)).unwrap();

        let index = Self::unique_index();
        let index_key = IndexKey::new(&saved, index).expect("index key should be present");

        crate::INDEX_REGISTRY
            .with(|reg| {
                reg.with_store_mut(TestIndexStore::PATH, |store| {
                    let raw = index_key.to_raw();
                    let mut entry = store
                        .get(&raw)
                        .expect("index entry should exist")
                        .try_decode()
                        .expect("index entry should decode");
                    entry.insert_key(other.key());
                    let raw_entry = RawIndexEntry::try_from_entry(&entry).unwrap();
                    store.insert(raw, raw_entry);
                })
            })
            .unwrap();

        let err = db!()
            .delete::<Index>()
            .by_unique_index(Self::unique_handle(), Index::new(3, 88))
            .unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("index corrupted") || msg.contains("corruption"),
            "expected corruption error, got: {msg}"
        );
    }

    fn delete_unique_key_type_mismatch_errors() {
        let index = Self::unique_index();
        let index_key =
            IndexKey::new(&Index::new(1, 777), index).expect("index key should be present");
        let bad_key = Key::Uint(123);

        crate::INDEX_REGISTRY
            .with(|reg| {
                reg.with_store_mut(TestIndexStore::PATH, |store| {
                    let entry = IndexEntry::new(bad_key);
                    let raw = index_key.to_raw();
                    let raw_entry = RawIndexEntry::try_from_entry(&entry).unwrap();
                    store.insert(raw, raw_entry);
                })
            })
            .unwrap();

        let bytes = serialize(&Index::new(9, 777)).unwrap();
        crate::DATA_REGISTRY
            .with(|reg| {
                reg.with_store_mut(<Index as EntityKind>::Store::PATH, |store| {
                    let data_key = DataKey::new::<Index>(bad_key);
                    let raw = data_key.to_raw();
                    store.insert(raw, RawRow::try_new(bytes).unwrap());
                })
            })
            .unwrap();

        let err = db!()
            .delete::<Index>()
            .by_unique_index(Self::unique_handle(), Index::new(2, 777))
            .unwrap_err();

        assert!(err.to_string().contains("primary key type mismatch"));
    }

    fn delete_unique_missing_primary_row_errors() {
        let index = Self::unique_index();
        let index_key =
            IndexKey::new(&Index::new(1, 444), index).expect("index key should be present");
        let missing_key = Key::Ulid(Ulid::generate());

        crate::INDEX_REGISTRY
            .with(|reg| {
                reg.with_store_mut(TestIndexStore::PATH, |store| {
                    let entry = IndexEntry::new(missing_key);
                    let raw = index_key.to_raw();
                    let raw_entry = RawIndexEntry::try_from_entry(&entry).unwrap();
                    store.insert(raw, raw_entry);
                })
            })
            .unwrap();

        let err = db!()
            .delete::<Index>()
            .by_unique_index(Self::unique_handle(), Index::new(2, 444))
            .unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("index corrupted") || msg.contains("corruption"),
            "expected corruption error, got: {msg}"
        );
    }

    fn unique_handle() -> UniqueIndexHandle {
        UniqueIndexHandle::for_fields::<Index>(&["y"]).expect("expected unique index on y")
    }

    fn unique_index() -> &'static icydb::model::index::IndexModel {
        Index::INDEXES
            .iter()
            .find(|idx| idx.fields == ["y"])
            .expect("expected unique index on y")
    }
}
