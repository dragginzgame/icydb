use icydb::{
    core::db::store::{DataKey, IndexKey},
    prelude::*,
};
use test_design::{
    e2e::db::{Index, IndexUniqueOpt},
    schema::TestIndexStore,
};

///
/// UpsertSuite
///

pub struct UpsertSuite;

impl UpsertSuite {
    pub fn test() {
        let tests: Vec<(&str, fn())> = vec![
            (
                "upsert_insert_then_update_unique_index",
                Self::upsert_insert_then_update_unique_index,
            ),
            (
                "upsert_missing_index_value_errors",
                Self::upsert_missing_index_value_errors,
            ),
            (
                "upsert_index_corruption_errors",
                Self::upsert_index_corruption_errors,
            ),
            (
                "upsert_dangling_index_entry_errors",
                Self::upsert_dangling_index_entry_errors,
            ),
            (
                "upsert_empty_index_entry_errors",
                Self::upsert_empty_index_entry_errors,
            ),
            (
                "upsert_view_preserves_existing_key",
                Self::upsert_view_preserves_existing_key,
            ),
        ];

        for (name, test_fn) in tests {
            crate::clear_test_data_store();
            println!("Running test: {name}");
            test_fn();
        }
    }

    fn upsert_insert_then_update_unique_index() {
        let inserted = db!()
            .upsert::<Index>()
            .by_unique_fields(&["y"], Index::new(1, 77))
            .unwrap();

        let pk = inserted.primary_key();
        assert_eq!(inserted.y, 77);

        let updated = db!()
            .upsert::<Index>()
            .by_unique_fields(&["y"], Index::new(2, 77))
            .unwrap();

        assert_eq!(updated.primary_key(), pk);
        assert_eq!(updated.x, 2);

        let loaded = db!().load::<Index>().one(pk).unwrap().one_entity().unwrap();
        assert_eq!(loaded.x, 2);
        assert_eq!(loaded.y, 77);
    }

    fn upsert_missing_index_value_errors() {
        let entity = IndexUniqueOpt {
            value: None,
            ..Default::default()
        };

        let err = db!()
            .upsert::<IndexUniqueOpt>()
            .by_unique_fields(&["value"], entity)
            .unwrap_err();

        assert!(err.to_string().contains("index key missing"));
    }

    fn upsert_index_corruption_errors() {
        let saved = db!().insert(Index::new(1, 88)).unwrap();
        let other = db!().insert(Index::new(2, 99)).unwrap();

        let fields = ["y"];
        let index = Index::INDEXES
            .iter()
            .find(|idx| idx.fields == fields)
            .expect("expected unique index on y");

        let index_key = IndexKey::new(&saved, index).expect("index key should be present");

        crate::INDEX_REGISTRY
            .with(|reg| {
                reg.with_store_mut(TestIndexStore::PATH, |store| {
                    let mut entry = store.get(&index_key).expect("index entry should exist");
                    entry.insert_key(other.key());
                    store.insert(index_key.clone(), entry);
                })
            })
            .unwrap();

        let err = db!()
            .upsert::<Index>()
            .by_unique_fields(&["y"], Index::new(3, 88))
            .unwrap_err();

        assert!(err.to_string().contains("index corrupted"));
    }

    fn upsert_dangling_index_entry_errors() {
        let saved = db!().insert(Index::new(1, 55)).unwrap();

        let fields = ["y"];
        let index = Index::INDEXES
            .iter()
            .find(|idx| idx.fields == fields)
            .expect("expected unique index on y");
        let index_key = IndexKey::new(&saved, index).expect("index key should be present");

        crate::DATA_REGISTRY
            .with(|reg| {
                reg.with_store_mut(
                    <Index as icydb::core::traits::EntityKind>::Store::PATH,
                    |store| {
                        let data_key = DataKey::new::<Index>(saved.key());
                        store.remove(&data_key);
                    },
                )
            })
            .unwrap();

        let err = db!()
            .upsert::<Index>()
            .by_unique_fields(&["y"], Index::new(2, 55))
            .unwrap_err();

        assert!(err.to_string().contains("index corrupted"));

        let _ = crate::INDEX_REGISTRY
            .with(|reg| reg.with_store_mut(TestIndexStore::PATH, |store| store.remove(&index_key)));
    }

    fn upsert_empty_index_entry_errors() {
        let saved = db!().insert(Index::new(1, 66)).unwrap();

        let fields = ["y"];
        let index = Index::INDEXES
            .iter()
            .find(|idx| idx.fields == fields)
            .expect("expected unique index on y");
        let index_key = IndexKey::new(&saved, index).expect("index key should be present");

        crate::INDEX_REGISTRY
            .with(|reg| {
                reg.with_store_mut(TestIndexStore::PATH, |store| {
                    let mut entry = store.get(&index_key).expect("index entry should exist");
                    entry.remove_key(&saved.key());
                    store.insert(index_key.clone(), entry);
                })
            })
            .unwrap();

        let err = db!()
            .upsert::<Index>()
            .by_unique_fields(&["y"], Index::new(2, 66))
            .unwrap_err();

        assert!(err.to_string().contains("index corrupted"));
    }

    fn upsert_view_preserves_existing_key() {
        let inserted_view = db!()
            .upsert::<Index>()
            .by_unique_fields_view(&["y"], Index::new(4, 123).to_view())
            .unwrap();

        let pk = inserted_view.id;
        assert_eq!(inserted_view.x, 4);

        let updated_view = db!()
            .upsert::<Index>()
            .by_unique_fields_view(&["y"], Index::new(9, 123).to_view())
            .unwrap();

        assert_eq!(updated_view.id, pk);
        assert_eq!(updated_view.x, 9);
        assert_eq!(updated_view.y, 123);
    }
}
