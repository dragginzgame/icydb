use icydb::{core::traits::Path, prelude::*};
use test_design::schema::TestDataStore;

///
/// DbSuite
///

pub struct DbSuite {}

impl DbSuite {
    pub fn test() {
        let tests: Vec<(&str, fn())> = vec![
            ("query_fail_filter", Self::query_fail_filter),
            ("query_fail_sort", Self::query_fail_sort),
            ("blob", Self::blob),
            ("insert", Self::insert),
            ("insert_lots", Self::insert_lots),
            ("insert_lots_blob", Self::insert_lots_blob),
            ("delete_lots", Self::delete_lots),
            ("data_key_order", Self::data_key_order),
            ("index_create_and_delete", Self::index_create_and_delete),
            ("index_option", Self::index_option),
            ("limit_query", Self::limit_query),
            ("load_one", Self::load_one),
            ("load_many", Self::load_many),
            ("unit_primary_key", Self::unit_primary_key),
            ("perf_options", Self::perf_options),
            ("perf_many_relations", Self::perf_many_relations),
        ];

        for (name, test_fn) in tests {
            println!("clearing db");
            crate::DATA_REGISTRY
                .with(|reg| reg.with_store_mut(TestDataStore::PATH, |store| store.clear()))
                .unwrap();

            println!("Running test: {name}");
            test_fn();
        }
    }

    //
    // TESTS
    //

    fn query_fail_filter() {
        use test_design::e2e::db::SimpleEntity;

        let query = db::query::load().filter(|f| f.eq("wefwefasd", "A"));
        let res = db!().load::<SimpleEntity>().execute(query);

        assert!(res.is_err(), "filter query should fail");
    }

    fn query_fail_sort() {
        use test_design::e2e::db::SimpleEntity;

        let res = db!()
            .load::<SimpleEntity>()
            .execute(db::query::load().sort(|s| s.asc("jwjehrjrh")));

        assert!(res.is_err(), "sort query should fail");
    }

    fn blob() {
        use test_design::e2e::db::ContainsBlob;

        const ROWS: usize = 100;

        // Insert rows
        for _ in 0..ROWS {
            let e = ContainsBlob::default();
            db!().insert(e).unwrap();
        }

        // Retrieve rows in B-Tree order
        let keys = db!()
            .load::<ContainsBlob>()
            .execute(db::query::load().sort(|s| s.asc("id")))
            .unwrap()
            .keys();

        assert!(
            keys.len() == ROWS,
            "{} rows returned, should be {ROWS}",
            keys.len()
        );

        // Verify the order
        for pair in keys.windows(2) {
            assert!(pair[0] < pair[1], "key ordering is incorrect");
        }
    }

    fn insert() {
        use test_design::e2e::db::SimpleEntity;

        let e = SimpleEntity::default();
        db!().insert(e).unwrap();

        // count keys
        let num_keys = db!().load::<SimpleEntity>().count_all().unwrap();
        assert_eq!(num_keys, 1);

        // insert another
        let e = SimpleEntity::default();
        db!().insert(e).unwrap();

        // count keys
        assert_eq!(db!().load::<SimpleEntity>().count_all().unwrap(), 2);
    }

    fn insert_lots() {
        use test_design::e2e::db::SimpleEntity;
        const ROWS: u32 = 1_000;

        // insert rows
        for _ in 0..ROWS {
            let e = SimpleEntity::default();
            db!().insert(e).unwrap();
        }

        // Retrieve the count from the store
        let count = db!().load::<SimpleEntity>().count_all().unwrap();

        // Assert that the count matches the expected number
        assert_eq!(count, ROWS, "Expected {ROWS} keys in the store");
    }

    fn insert_lots_blob() {
        use test_design::e2e::db::BlobEntity;
        const ROWS: u32 = 500;
        const BLOB_SIZE: usize = 1024 * 2;

        // insert rows
        for _ in 0..ROWS {
            let e = BlobEntity {
                bytes: vec![0u8; BLOB_SIZE].into(),
                ..Default::default()
            };

            db!().insert(e).unwrap();
        }

        // Retrieve the count from the store
        let count = db!().load::<BlobEntity>().count_all().unwrap();

        // Assert that the count matches the expected number
        assert_eq!(count, ROWS, "Expected {ROWS} keys in the store");
    }

    fn data_key_order() {
        use test_design::e2e::db::DataKeyOrder;

        const ROWS: u16 = 1_000;

        // Insert rows
        for _ in 1..ROWS {
            let e = DataKeyOrder::default();
            db!().insert(e).unwrap();
        }

        // Retrieve rows in B-Tree order
        let keys = db!()
            .load::<DataKeyOrder>()
            .execute(db::query::load().sort(|s| s.asc("id")))
            .unwrap()
            .keys();

        // Verify the order
        for pair in keys.windows(2) {
            assert!(pair[0] < pair[1], "key ordering is incorrect");
        }
    }

    fn delete_lots() {
        use test_design::e2e::db::SimpleEntity;

        const ROWS: u32 = 500;

        // Step 1: Insert rows and collect keys
        let mut keys = Vec::new();
        for _ in 0..ROWS {
            let key = db!().insert(SimpleEntity::default()).unwrap().key();
            keys.push(key);
        }

        // Step 2: Ensure the count is correct
        let count_before = db!().load::<SimpleEntity>().count_all().unwrap();
        assert_eq!(count_before, ROWS, "Expected {ROWS} inserted rows");

        // Step 3: Delete all inserted rows
        let deleted = db!().delete::<SimpleEntity>().many(keys.clone()).unwrap();

        assert_eq!(
            deleted.count(),
            ROWS,
            "Expected to delete {ROWS} rows, but got {}",
            deleted.count()
        );

        // Step 4: Ensure all have been deleted
        let count_after = db!().load::<SimpleEntity>().count_all().unwrap();
        assert_eq!(count_after, 0, "Expected 0 rows after deletion");
    }

    fn index_create_and_delete() {
        use test_design::e2e::db::Index;

        // Step 1: Insert entity e1 with x=1, y=10
        let e1 = Index::new(1, 10);
        let id1 = db!().insert(e1).unwrap().key();

        // COUNT
        let rows = db!().load::<Index>().count_all().unwrap();
        assert_eq!(rows, 1);

        // Step 2: Insert entity e2 with x=1 (non-unique), y=20 (unique)
        let e2 = Index::new(1, 20);
        db!().insert(e2).unwrap();

        // COUNT
        let rows = db!().load::<Index>().count_all().unwrap();
        assert_eq!(rows, 2);

        // Step 3: Attempt to insert another with duplicate y=10 (should fail)
        let e3 = Index::new(2, 10);
        let result = db!().insert(e3.clone());
        assert!(result.is_err(), "expected unique index violation on y=10");

        // COUNT
        let rows = db!().load::<Index>().count_all().unwrap();
        assert_eq!(rows, 2);

        // Step 4: Delete e1 (y=10)
        db!().delete::<Index>().one(id1).unwrap();

        // COUNT
        let rows = db!().load::<Index>().count_all().unwrap();
        assert_eq!(rows, 1);

        // Step 5: Try inserting e3 again (y=10 should now be free)
        let result = db!().insert(e3);
        assert!(
            result.is_ok(),
            "expected insert to succeed after y=10 was freed by delete"
        );

        // COUNT
        let rows = db!().load::<Index>().count_all().unwrap();
        assert_eq!(rows, 2);

        // Step 6: Confirm only 2 entities remain
        let rows = db!().load::<Index>().count_all().unwrap();

        assert_eq!(rows, 2);
    }

    fn index_option() {
        use test_design::e2e::db::IndexUniqueOpt;

        // Insert entity with Some(10)
        let e1 = IndexUniqueOpt {
            id: Ulid::generate(),
            value: Some(10),
            ..Default::default()
        };
        let id1 = db!().insert(e1).unwrap().key();

        // Insert entity with Some(20)
        let e2 = IndexUniqueOpt {
            id: Ulid::generate(),
            value: Some(20),
            ..Default::default()
        };
        db!().insert(e2).unwrap().key();

        // Insert entity with None (should not conflict with anything)
        let e3 = IndexUniqueOpt {
            id: Ulid::generate(),
            value: None,
            ..Default::default()
        };
        let id3 = db!().insert(e3).unwrap().key();

        // Insert duplicate Some(10) — should fail (if index is unique)
        let e4 = IndexUniqueOpt {
            id: Ulid::generate(),
            value: Some(10),
            ..Default::default()
        };
        let result = db!().insert(e4.clone());
        assert!(
            result.is_err(),
            "Expected duplicate index error on Some(10)"
        );

        // Delete e1 (frees up Some(10))
        db!().delete::<IndexUniqueOpt>().one(id1).unwrap();

        // Retry insert of e4 — should now succeed
        let result = db!().insert(e4);
        assert!(
            result.is_ok(),
            "Expected insert to succeed after deleting conflicting index"
        );

        // Delete e3 (value = None)
        db!().delete::<IndexUniqueOpt>().one(id3).unwrap();

        // Insert another entity with value = None — should be fine (no uniqueness enforced)
        let e5 = IndexUniqueOpt {
            id: Ulid::generate(),
            value: None,
            ..Default::default()
        };
        db!().insert(e5).unwrap();

        // Confirm only 3 entities now exist
        let rows = db!().load::<IndexUniqueOpt>().count_all().unwrap();
        assert_eq!(rows, 3);
    }

    fn limit_query() {
        use test_design::e2e::db::Limit;

        // Insert 100 rows
        // overwrite the ulid with replace()
        for value in 1..100 {
            let e = Limit {
                value,
                ..Default::default()
            };
            db!().replace(e).unwrap();
        }

        // Test various limits and offsets
        for limit in [10, 20, 50] {
            for offset in [0, 5, 10] {
                let results = db!()
                    .load::<Limit>()
                    .execute(db::query::load().offset(offset).limit(limit))
                    .unwrap();

                let count = results.count();
                assert_eq!(count, limit, "{limit} not equal to {count}");

                // Verify first entity corresponds to offset (ascending key order)
                if let Some(first) = results.entity() {
                    assert_eq!(first.value, offset + 1);
                }
            }
        }
    }

    fn load_one() {
        use test_design::e2e::db::SimpleEntity;

        let saved_key = db!().insert(SimpleEntity::default()).unwrap().key();
        let loaded_key = db!()
            .load::<SimpleEntity>()
            .one(saved_key)
            .unwrap()
            .try_key()
            .unwrap();

        assert_eq!(saved_key, loaded_key);
    }

    fn load_many() {
        use test_design::e2e::db::SimpleEntity;

        let key1 = db!().insert(SimpleEntity::default()).unwrap().key();
        let key2 = db!().insert(SimpleEntity::default()).unwrap().key();
        let key3 = db!().insert(SimpleEntity::default()).unwrap().key();

        // Pass a slice of IDs
        let many_keys = vec![key1, key2, key3];
        let loaded = db!().load::<SimpleEntity>().many(&many_keys).unwrap();

        // Assert correct count
        assert_eq!(loaded.count(), 3);

        // Optionally assert that each loaded item has a matching ID
        for key in loaded.keys() {
            assert!(many_keys.contains(&key));
        }
    }

    fn unit_primary_key() {
        use icydb::core::{Key, types::Unit};
        use test_design::test::entity::UnitKey;

        let inserted = db!().insert(UnitKey::default()).unwrap();
        assert_eq!(inserted.primary_key(), Unit, "insert should retain unit PK");

        let response = db!().load::<UnitKey>().one(()).unwrap();
        let key = response.key().expect("expected key for unit entity");
        assert_eq!(key, Key::Unit, "unit PK should map to Key::Unit");

        let fetched = response.try_entity().expect("entity should be present");
        assert_eq!(
            fetched.primary_key(),
            Unit,
            "loaded entity should have unit PK"
        );
        assert_eq!(
            fetched.a, inserted.a,
            "loaded payload should match the saved record"
        );

        let count = db!().load::<UnitKey>().count_all().unwrap();
        assert_eq!(count, 1, "unit entity should be counted exactly once");
    }

    fn perf_options() {
        use test_design::e2e::db::ContainsOpts;

        const ROWS: u16 = 500;

        // Insert rows
        for _ in 1..ROWS {
            let e = ContainsOpts::default();
            db!().insert(e).unwrap();
        }

        // Retrieve rows in B-Tree order
        let keys = db!().load::<ContainsOpts>().all().unwrap().keys();
        assert!(
            !keys.is_empty(),
            "perf_options: expected at least one row after inserts"
        );
    }

    fn perf_many_relations() {
        use test_design::e2e::db::ContainsManyRelations;

        const ROWS: u16 = 500;

        // Insert rows
        for _ in 1..ROWS {
            let e = ContainsManyRelations::default();
            db!().insert(e).unwrap();
        }

        // Retrieve rows in B-Tree order
        let rows = db!().load::<ContainsManyRelations>().all().unwrap();
        assert!(
            rows.count() > 0,
            "perf_many_relations: expected at least one row after inserts"
        );
    }
}
