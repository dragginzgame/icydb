/*

    use super::*;
    use crate::{
        db::{
            Db,
            index::IndexStoreRegistry,
            index::fingerprint::with_test_hash_override,
            store::{DataStore, DataStoreRegistry, RawRow},
        },
        error::{ErrorClass, ErrorOrigin},
        serialize::serialize,
        test_support::{TEST_DATA_STORE_PATH, TEST_INDEX_STORE_PATH, TestCanister},
        traits::{
            EntityKind, FieldValues, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom,
            View, Visitable,
        },
        types::Ulid,
        value::Value,
    };
    use serde::{Deserialize, Serialize};
    use std::cell::RefCell;

    const DATA_STORE_PATH: &str = TEST_DATA_STORE_PATH;
    const INDEX_STORE_PATH: &str = TEST_INDEX_STORE_PATH;

    #[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
    struct TestEntity {
        id: Ulid,
        tag: String,
    }

    crate::test_entity! {
        entity TestEntity {
            path: "index_plan_test::TestEntity",
            pk: id: Ulid,

            fields { id: Ulid, tag: Text }

            indexes { index idx_tag(tag) unique; }
        }
    }

    impl View for TestEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl SanitizeAuto for TestEntity {}
    impl SanitizeCustom for TestEntity {}
    impl ValidateAuto for TestEntity {}
    impl ValidateCustom for TestEntity {}
    impl Visitable for TestEntity {}

    impl FieldValues for TestEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => Some(Value::Ulid(self.id)),
                "tag" => Some(Value::Text(self.tag.clone())),
                _ => None,
            }
        }
    }

    #[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
    struct MissingFieldEntity {
        id: Ulid,
        tag: String,
    }

    crate::test_entity! {
        entity MissingFieldEntity {
            path: "index_plan_test::MissingFieldEntity",
            pk: id: Ulid,

            fields { id: Ulid, tag: Text }

            indexes { index idx_tag(tag) unique; }
        }
    }

    impl View for MissingFieldEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl SanitizeAuto for MissingFieldEntity {}
    impl SanitizeCustom for MissingFieldEntity {}
    impl ValidateAuto for MissingFieldEntity {}
    impl ValidateCustom for MissingFieldEntity {}
    impl Visitable for MissingFieldEntity {}

    impl FieldValues for MissingFieldEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => Some(Value::Ulid(self.id)),
                "tag" if self.tag == "__missing__" => None,
                "tag" => Some(Value::Text(self.tag.clone())),
                _ => None,
            }
        }
    }

    canic_memory::eager_static! {
        static TEST_DATA_STORE: RefCell<DataStore> =
            RefCell::new(DataStore::init(canic_memory::ic_memory!(DataStore, 20)));
    }

    canic_memory::eager_static! {
        static TEST_INDEX_STORE: RefCell<IndexStore> =
            RefCell::new(IndexStore::init(
                canic_memory::ic_memory!(IndexStore, 21),
                canic_memory::ic_memory!(IndexStore, 22),
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

    canic_memory::eager_init!({
        canic_memory::ic_memory_range!(0, 60);
    });

    fn reset_stores() {
        TEST_DATA_STORE.with_borrow_mut(DataStore::clear);
        TEST_INDEX_STORE.with_borrow_mut(IndexStore::clear);
    }

    fn seed_entity(entity: &TestEntity) {
        let data_key = DataKey::try_new::<TestEntity>(entity.id).unwrap();
        let raw_key = data_key.to_raw().expect("data key encode");
        let raw_row = RawRow::try_new(serialize(entity).unwrap()).unwrap();
        TEST_DATA_STORE.with_borrow_mut(|store| store.insert(raw_key, raw_row));

        let index_key = IndexKey::new(entity, <TestEntity as EntityKind>::INDEXES[0])
            .expect("index key")
            .expect("index key missing");
        let raw_index_key = index_key.to_raw();
        let entry = IndexEntry::new(entity.id);
        let raw_entry = RawIndexEntry::try_from_entry(&entry).unwrap();
        TEST_INDEX_STORE.with_borrow_mut(|store| store.insert(raw_index_key, raw_entry));
    }

    #[test]
    fn unique_collision_equal_values_is_conflict() {
        with_test_hash_override([0xAA; 16], || {
            reset_stores();

            let existing = TestEntity {
                id: Ulid::from_u128(1),
                tag: "alpha".to_string(),
            };
            seed_entity(&existing);

            let incoming = TestEntity {
                id: Ulid::from_u128(2),
                tag: "alpha".to_string(),
            };

            let err = plan_index_mutation_for_entity::<TestEntity>(&DB, None, Some(&incoming))
                .expect_err("expected unique conflict");
            assert_eq!(err.class, ErrorClass::Conflict);
        });
    }

    #[test]
    fn unique_collision_different_values_is_corruption() {
        with_test_hash_override([0xAA; 16], || {
            reset_stores();

            let existing = TestEntity {
                id: Ulid::from_u128(1),
                tag: "alpha".to_string(),
            };
            seed_entity(&existing);

            let incoming = TestEntity {
                id: Ulid::from_u128(2),
                tag: "beta".to_string(),
            };

            let err = plan_index_mutation_for_entity::<TestEntity>(&DB, None, Some(&incoming))
                .expect_err("expected hash collision");
            assert_eq!(err.class, ErrorClass::Corruption);
            assert_eq!(err.origin, ErrorOrigin::Index);
        });
    }

    #[test]
    fn unique_collision_row_key_mismatch_is_corruption() {
        reset_stores();

        let indexed = TestEntity {
            id: Ulid::from_u128(1),
            tag: "alpha".to_string(),
        };
        let corrupted = TestEntity {
            id: Ulid::from_u128(2),
            tag: "alpha".to_string(),
        };

        let data_key = DataKey::try_new::<TestEntity>(indexed.id).unwrap();
        let raw_key = data_key.to_raw().expect("data key encode");
        let raw_row = RawRow::try_new(serialize(&corrupted).unwrap()).unwrap();
        TEST_DATA_STORE.with_borrow_mut(|store| store.insert(raw_key, raw_row));

        let index_key = IndexKey::new(&indexed, <TestEntity as EntityKind>::INDEXES[0])
            .expect("index key")
            .expect("index key missing");
        let raw_index_key = index_key.to_raw();
        let entry = IndexEntry::new(indexed.id);
        let raw_entry = RawIndexEntry::try_from_entry(&entry).unwrap();
        TEST_INDEX_STORE.with_borrow_mut(|store| store.insert(raw_index_key, raw_entry));

        let incoming = TestEntity {
            id: Ulid::from_u128(3),
            tag: "alpha".to_string(),
        };

        let err = plan_index_mutation_for_entity::<TestEntity>(&DB, None, Some(&incoming))
            .expect_err("expected row key mismatch corruption");
        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Store);
    }

    #[test]
    fn unique_collision_stored_missing_field_is_corruption() {
        reset_stores();

        let stored = MissingFieldEntity {
            id: Ulid::from_u128(1),
            tag: "__missing__".to_string(),
        };
        let data_key = DataKey::try_new::<MissingFieldEntity>(stored.id).unwrap();
        let raw_key = data_key.to_raw().expect("data key encode");
        let raw_row = RawRow::try_new(serialize(&stored).unwrap()).unwrap();
        TEST_DATA_STORE.with_borrow_mut(|store| store.insert(raw_key, raw_row));

        let incoming = MissingFieldEntity {
            id: Ulid::from_u128(2),
            tag: "alpha".to_string(),
        };

        let index_key = IndexKey::new(&incoming, <MissingFieldEntity as EntityKind>::INDEXES[0])
            .expect("index key")
            .expect("index key missing");
        let raw_index_key = index_key.to_raw();
        let entry = IndexEntry::new(stored.id);
        let raw_entry = RawIndexEntry::try_from_entry(&entry).unwrap();
        TEST_INDEX_STORE.with_borrow_mut(|store| store.insert(raw_index_key, raw_entry));

        let err = plan_index_mutation_for_entity::<MissingFieldEntity>(&DB, None, Some(&incoming))
            .expect_err("expected missing stored field corruption");
        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Index);
}
*/
