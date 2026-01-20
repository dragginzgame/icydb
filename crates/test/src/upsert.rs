use icydb::__internal::core::db::store::{DataKey, IndexKey, RawIndexEntry};
use icydb::prelude::*;
use test_design::{
    e2e::db::{Index, IndexSanitized, IndexUniqueOpt, LowerIndexText},
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
                "upsert_result_reports_inserted",
                Self::upsert_result_reports_inserted,
            ),
            (
                "upsert_merge_updates_existing",
                Self::upsert_merge_updates_existing,
            ),
            (
                "upsert_merge_inserts_without_closure",
                Self::upsert_merge_inserts_without_closure,
            ),
            (
                "upsert_matches_sanitized_unique_index",
                Self::upsert_matches_sanitized_unique_index,
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
            .upsert::<Index>()
            .by_unique_fields(&["y"], Index::new(3, 88))
            .unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("index corrupted") || msg.contains("corruption"),
            "expected corruption error, got: {msg}"
        );
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
                reg.with_store_mut(<Index as icydb::traits::EntityKind>::Store::PATH, |store| {
                    let data_key = DataKey::new::<Index>(saved.key());
                    let raw = data_key.to_raw();
                    store.remove(&raw);
                })
            })
            .unwrap();

        let err = db!()
            .upsert::<Index>()
            .by_unique_fields(&["y"], Index::new(2, 55))
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
                    let raw = index_key.to_raw();
                    let mut entry = store
                        .get(&raw)
                        .expect("index entry should exist")
                        .try_decode()
                        .expect("index entry should decode");
                    entry.remove_key(&saved.key());
                    let raw_entry = RawIndexEntry::try_from_entry(&entry).unwrap();
                    store.insert(raw, raw_entry);
                })
            })
            .unwrap();

        let err = db!()
            .upsert::<Index>()
            .by_unique_fields(&["y"], Index::new(2, 66))
            .unwrap_err();

        assert!(err.to_string().contains("index corrupted"));
    }

    fn upsert_result_reports_inserted() {
        let inserted = db!()
            .upsert::<Index>()
            .by_unique_fields_result(&["y"], Index::new(1, 777))
            .unwrap();

        assert!(inserted.inserted);
        let pk = inserted.entity.primary_key();

        let updated = db!()
            .upsert::<Index>()
            .by_unique_fields_result(&["y"], Index::new(2, 777))
            .unwrap();

        assert!(!updated.inserted);
        assert_eq!(updated.entity.primary_key(), pk);
        assert_eq!(updated.entity.x, 2);
    }

    fn upsert_merge_updates_existing() {
        let inserted = db!().insert(Index::new(3, 444)).unwrap();

        let merged = db!()
            .upsert::<Index>()
            .by_unique_fields_merge(&["y"], Index::new(7, 444), |mut existing, incoming| {
                existing.x += incoming.x;
                existing
            })
            .unwrap();

        assert_eq!(merged.primary_key(), inserted.primary_key());
        assert_eq!(merged.x, 10);
    }

    fn upsert_merge_inserts_without_closure() {
        let inserted = db!()
            .upsert::<Index>()
            .by_unique_fields_merge_result(&["y"], Index::new(1, 555), |_, _| {
                panic!("merge closure should not run on insert");
            })
            .unwrap();

        assert!(inserted.inserted);
        assert_eq!(inserted.entity.x, 1);
        assert_eq!(inserted.entity.y, 555);
    }

    fn upsert_matches_sanitized_unique_index() {
        let inserted = db!()
            .insert(IndexSanitized {
                username: LowerIndexText::from("MiXeD"),
                score: 1,
                ..Default::default()
            })
            .unwrap();

        let pk = inserted.primary_key();

        let updated = db!()
            .upsert::<IndexSanitized>()
            .by_unique_fields(
                &["username"],
                IndexSanitized {
                    username: LowerIndexText::from("MIXED"),
                    score: 42,
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(updated.primary_key(), pk);
        assert_eq!(updated.score, 42);
        assert_eq!(updated.username, LowerIndexText::from("mixed"));
    }
}
