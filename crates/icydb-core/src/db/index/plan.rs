use crate::{
    db::{
        CommitIndexOp,
        executor::ExecutorError,
        index::{
            IndexEntry, IndexEntryCorruption, IndexEntryEncodeError, IndexKey, IndexStore,
            RawIndexEntry, RawIndexKey,
        },
        store::DataKey,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    key::Key,
    model::index::IndexModel,
    obs::sink::{self, MetricsEvent},
    traits::{EntityKind, Storable},
};
use std::{cell::RefCell, collections::BTreeMap, thread::LocalKey};

///
/// IndexApplyPlan
///

#[derive(Debug)]
pub struct IndexApplyPlan {
    pub index: &'static IndexModel,
    pub store: &'static LocalKey<RefCell<IndexStore>>,
}

///
/// IndexMutationPlan
///

#[derive(Debug)]
pub struct IndexMutationPlan {
    pub apply: Vec<IndexApplyPlan>,
    pub commit_ops: Vec<CommitIndexOp>,
}

/// Plan all index mutations for a single entity transition.
///
/// This function:
/// - Loads existing index entries
/// - Validates unique constraints
/// - Computes the exact index writes/deletes required
///
/// All fallible work happens here. The returned plan is safe to apply
/// infallibly after a commit marker is written.
pub fn plan_index_mutation_for_entity<E: EntityKind>(
    db: &crate::db::Db<E::Canister>,
    old: Option<&E>,
    new: Option<&E>,
) -> Result<IndexMutationPlan, InternalError> {
    let old_entity_key = old.map(EntityKind::key);
    let new_entity_key = new.map(EntityKind::key);

    let mut apply = Vec::with_capacity(E::INDEXES.len());
    let mut commit_ops = Vec::new();

    for index in E::INDEXES {
        let store = db.with_index(|reg| reg.try_get_store(index.store))?;

        let old_key = match old {
            Some(entity) => IndexKey::new(entity, index)?,
            None => None,
        };
        let new_key = match new {
            Some(entity) => IndexKey::new(entity, index)?,
            None => None,
        };

        let old_entry = load_existing_entry(store, index, old)?;
        // Prevalidate membership so commit-phase mutations cannot surface corruption.
        if let Some(old_key) = &old_key {
            let Some(old_entity_key) = old_entity_key else {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Index,
                    "missing old entity key for index removal".to_string(),
                ));
            };
            let entry = old_entry.as_ref().ok_or_else(|| {
                ExecutorError::corruption(
                    ErrorOrigin::Index,
                    format!(
                        "index corrupted: {} ({}) -> {}",
                        E::PATH,
                        index.fields.join(", "),
                        IndexEntryCorruption::missing_key(old_key.to_raw(), old_entity_key)
                    ),
                )
            })?;
            if index.unique && entry.len() > 1 {
                return Err(ExecutorError::corruption(
                    ErrorOrigin::Index,
                    format!(
                        "index corrupted: {} ({}) -> {}",
                        E::PATH,
                        index.fields.join(", "),
                        IndexEntryCorruption::NonUniqueEntry { keys: entry.len() }
                    ),
                )
                .into());
            }
            if !entry.contains(&old_entity_key) {
                return Err(ExecutorError::corruption(
                    ErrorOrigin::Index,
                    format!(
                        "index corrupted: {} ({}) -> {}",
                        E::PATH,
                        index.fields.join(", "),
                        IndexEntryCorruption::missing_key(old_key.to_raw(), old_entity_key)
                    ),
                )
                .into());
            }
        }
        let new_entry = if old_key == new_key {
            old_entry.clone()
        } else {
            load_existing_entry(store, index, new)?
        };

        validate_unique_constraint::<E>(
            db,
            index,
            new_entry.as_ref(),
            new_entity_key.as_ref(),
            new,
        )?;

        build_commit_ops_for_index::<E>(
            &mut commit_ops,
            index,
            old_key,
            new_key,
            old_entry,
            new_entry,
            old_entity_key,
            new_entity_key,
        )?;

        apply.push(IndexApplyPlan { index, store });
    }

    Ok(IndexMutationPlan { apply, commit_ops })
}

fn load_existing_entry<E: EntityKind>(
    store: &'static LocalKey<RefCell<IndexStore>>,
    index: &'static IndexModel,
    entity: Option<&E>,
) -> Result<Option<IndexEntry>, InternalError> {
    let Some(entity) = entity else {
        return Ok(None);
    };
    let Some(key) = IndexKey::new(entity, index)? else {
        return Ok(None);
    };

    store
        .with_borrow(|s| s.get(&key.to_raw()))
        .map(|raw| {
            raw.try_decode().map_err(|err| {
                ExecutorError::corruption(
                    ErrorOrigin::Index,
                    format!(
                        "index corrupted: {} ({}) -> {}",
                        E::PATH,
                        index.fields.join(", "),
                        err
                    ),
                )
                .into()
            })
        })
        .transpose()
}

/// Validate unique index constraints against the existing index entry.
///
/// This detects:
/// - Index corruption (multiple keys in a unique entry)
/// - Uniqueness violations (conflicting key ownership)
#[expect(clippy::too_many_lines)]
fn validate_unique_constraint<E: EntityKind>(
    db: &crate::db::Db<E::Canister>,
    index: &IndexModel,
    entry: Option<&IndexEntry>,
    new_key: Option<&Key>,
    new_entity: Option<&E>,
) -> Result<(), InternalError> {
    if !index.unique {
        return Ok(());
    }

    let Some(entry) = entry else {
        return Ok(());
    };

    if entry.len() > 1 {
        return Err(ExecutorError::corruption(
            ErrorOrigin::Index,
            format!(
                "index corrupted: {} ({}) -> {} keys",
                E::PATH,
                index.fields.join(", "),
                entry.len()
            ),
        )
        .into());
    }

    let Some(new_key) = new_key else {
        return Ok(());
    };
    if entry.contains(new_key) {
        return Ok(());
    }

    let Some(new_entity) = new_entity else {
        return Err(InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Index,
            "missing entity payload during unique validation".to_string(),
        ));
    };
    let existing_key = entry.single_key().ok_or_else(|| {
        ExecutorError::corruption(
            ErrorOrigin::Index,
            format!(
                "index corrupted: {} ({}) -> {} keys",
                E::PATH,
                index.fields.join(", "),
                entry.len()
            ),
        )
    })?;

    let stored = {
        let data_key = DataKey::new::<E>(existing_key);
        let row = db.context::<E>().read_strict(&data_key)?;
        row.try_decode::<E>().map_err(|err| {
            ExecutorError::corruption(
                ErrorOrigin::Serialize,
                format!("failed to deserialize row: {data_key} ({err})"),
            )
        })?
    };
    let stored_key = stored.key();
    if stored_key != existing_key {
        // Stored row decoded successfully but key mismatch indicates index/data divergence; treat as corruption.
        return Err(ExecutorError::corruption(
            ErrorOrigin::Index,
            format!(
                "index corrupted: {} ({}) -> {}",
                E::PATH,
                index.fields.join(", "),
                IndexEntryCorruption::RowKeyMismatch {
                    indexed_key: Box::new(existing_key),
                    row_key: Box::new(stored_key),
                }
            ),
        )
        .into());
    }

    for field in index.fields {
        let expected = new_entity.get_value(field).ok_or_else(|| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Index,
                format!(
                    "index field missing on lookup entity: {} ({})",
                    E::PATH,
                    field
                ),
            )
        })?;
        let actual = stored.get_value(field).ok_or_else(|| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Index,
                format!(
                    "index field missing on stored entity: {} ({})",
                    E::PATH,
                    field
                ),
            )
        })?;

        if expected != actual {
            return Err(ExecutorError::corruption(
                ErrorOrigin::Index,
                format!("index hash collision: {} ({})", E::PATH, field),
            )
            .into());
        }
    }

    sink::record(MetricsEvent::UniqueViolation {
        entity_path: E::PATH,
    });

    Err(ExecutorError::index_violation(E::PATH, index.fields).into())
}

/// Compute commit-time index operations for a single index.
///
/// Produces a minimal set of index updates:
/// - `Some(bytes)` → insert/update index entry
/// - `None`        → delete index entry
///
/// Correctly handles old/new key overlap and guarantees that
/// apply-time mutations cannot fail except by invariant violation.
#[allow(clippy::too_many_arguments)]
fn build_commit_ops_for_index<E: EntityKind>(
    commit_ops: &mut Vec<CommitIndexOp>,
    index: &'static IndexModel,
    old_key: Option<IndexKey>,
    new_key: Option<IndexKey>,
    old_entry: Option<IndexEntry>,
    new_entry: Option<IndexEntry>,
    old_entity_key: Option<Key>,
    new_entity_key: Option<Key>,
) -> Result<(), InternalError> {
    let mut touched: BTreeMap<RawIndexKey, Option<IndexEntry>> = BTreeMap::new();
    let fields = index.fields.join(", ");

    // ── Removal ────────────────────────────────

    if let Some(old_key) = old_key {
        let Some(old_entity_key) = old_entity_key else {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Index,
                "missing old entity key for index removal".to_string(),
            ));
        };

        if let Some(mut entry) = old_entry {
            entry.remove_key(&old_entity_key);
            let after = if entry.is_empty() { None } else { Some(entry) };
            touched.insert(old_key.to_raw(), after);
        } else {
            // No existing index entry -> nothing to remove.
            touched.insert(old_key.to_raw(), None);
        }
    }

    // ── Insertion ──────────────────────────────

    if let Some(new_key) = new_key {
        let Some(new_entity_key) = new_entity_key else {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Index,
                "missing new entity key for index insertion".to_string(),
            ));
        };

        let raw_key = new_key.to_raw();

        // Start from:
        //   1. result of removal (if same key)
        //   2. existing entry loaded from store
        //   3. brand new entry
        let mut entry = if let Some(existing) = touched.remove(&raw_key) {
            existing.unwrap_or_else(|| IndexEntry::new(new_entity_key))
        } else if let Some(existing) = new_entry {
            existing
        } else {
            IndexEntry::new(new_entity_key)
        };

        entry.insert_key(new_entity_key);
        touched.insert(raw_key, Some(entry));
    }

    // ── Emit commit ops ────────────────────────

    for (raw_key, entry) in touched {
        let value = if let Some(entry) = entry {
            let raw = RawIndexEntry::try_from_entry(&entry).map_err(|err| match err {
                IndexEntryEncodeError::TooManyKeys { keys } => InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    format!(
                        "index entry exceeds max keys: {} ({}) -> {} keys",
                        E::PATH,
                        fields,
                        keys
                    ),
                ),
                IndexEntryEncodeError::KeyEncoding(err) => InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    format!(
                        "index entry key encoding failed: {} ({}) -> {err}",
                        E::PATH,
                        fields
                    ),
                ),
            })?;
            Some(raw.into_bytes())
        } else {
            None
        };

        commit_ops.push(CommitIndexOp {
            store: index.store.to_string(),
            key: raw_key.as_bytes().to_vec(),
            value,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            Db,
            index::IndexStoreRegistry,
            index::fingerprint::with_test_hash_override,
            store::{DataStore, DataStoreRegistry, RawRow},
        },
        error::{ErrorClass, ErrorOrigin},
        model::{
            entity::EntityModel,
            field::{EntityFieldKind, EntityFieldModel},
            index::IndexModel,
        },
        serialize::serialize,
        traits::{
            CanisterKind, EntityKind, FieldValues, Path, SanitizeAuto, SanitizeCustom, StoreKind,
            ValidateAuto, ValidateCustom, View, Visitable,
        },
        types::Ulid,
        value::Value,
    };
    use serde::{Deserialize, Serialize};
    use std::cell::RefCell;

    const CANISTER_PATH: &str = "index_plan_test::TestCanister";
    const DATA_STORE_PATH: &str = "index_plan_test::TestDataStore";
    const INDEX_STORE_PATH: &str = "index_plan_test::TestIndexStore";
    const ENTITY_PATH: &str = "index_plan_test::TestEntity";

    const INDEX_FIELDS: [&str; 1] = ["tag"];
    const INDEX_MODEL: IndexModel = IndexModel::new(
        "index_plan_test::idx_tag",
        INDEX_STORE_PATH,
        &INDEX_FIELDS,
        true,
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    const TEST_FIELDS: [EntityFieldModel; 2] = [
        EntityFieldModel {
            name: "id",
            kind: EntityFieldKind::Ulid,
        },
        EntityFieldModel {
            name: "tag",
            kind: EntityFieldKind::Text,
        },
    ];
    const TEST_MODEL: EntityModel = EntityModel {
        path: ENTITY_PATH,
        entity_name: "TestEntity",
        primary_key: &TEST_FIELDS[0],
        fields: &TEST_FIELDS,
        indexes: &INDEXES,
    };

    #[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
    struct TestEntity {
        id: Ulid,
        tag: String,
    }

    impl Path for TestEntity {
        const PATH: &'static str = ENTITY_PATH;
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

    #[derive(Clone, Copy)]
    struct TestCanister;

    impl Path for TestCanister {
        const PATH: &'static str = CANISTER_PATH;
    }

    impl CanisterKind for TestCanister {}

    struct TestStore;

    impl Path for TestStore {
        const PATH: &'static str = DATA_STORE_PATH;
    }

    impl StoreKind for TestStore {
        type Canister = TestCanister;
    }

    impl EntityKind for TestEntity {
        type PrimaryKey = Ulid;
        type Store = TestStore;
        type Canister = TestCanister;

        const ENTITY_NAME: &'static str = "TestEntity";
        const PRIMARY_KEY: &'static str = "id";
        const FIELDS: &'static [&'static str] = &["id", "tag"];
        const INDEXES: &'static [&'static IndexModel] = &INDEXES;
        const MODEL: &'static EntityModel = &TEST_MODEL;

        fn key(&self) -> crate::key::Key {
            self.id.into()
        }

        fn primary_key(&self) -> Self::PrimaryKey {
            self.id
        }

        fn set_primary_key(&mut self, key: Self::PrimaryKey) {
            self.id = key;
        }
    }

    canic_memory::eager_static! {
        static TEST_DATA_STORE: RefCell<DataStore> =
            RefCell::new(DataStore::init(canic_memory::ic_memory!(DataStore, 20)));
    }

    canic_memory::eager_static! {
        static TEST_INDEX_STORE: RefCell<IndexStore> =
            RefCell::new(IndexStore::init(canic_memory::ic_memory!(IndexStore, 21)));
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

    static DB: Db<TestCanister> = Db::new(&DATA_REGISTRY, &INDEX_REGISTRY);

    canic_memory::eager_init!({
        canic_memory::ic_memory_range!(0, 60);
    });

    fn reset_stores() {
        TEST_DATA_STORE.with_borrow_mut(|store| store.clear());
        TEST_INDEX_STORE.with_borrow_mut(|store| store.clear());
    }

    fn seed_entity(entity: &TestEntity) {
        let data_key = DataKey::new::<TestEntity>(entity.id);
        let raw_key = data_key.to_raw().expect("data key encode");
        let raw_row = RawRow::try_new(serialize(entity).unwrap()).unwrap();
        TEST_DATA_STORE.with_borrow_mut(|store| store.insert(raw_key, raw_row));

        let index_key = IndexKey::new(entity, &INDEX_MODEL)
            .expect("index key")
            .expect("index key missing");
        let raw_index_key = index_key.to_raw();
        let entry = IndexEntry::new(entity.key());
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

        let data_key = DataKey::new::<TestEntity>(indexed.id);
        let raw_key = data_key.to_raw().expect("data key encode");
        let raw_row = RawRow::try_new(serialize(&corrupted).unwrap()).unwrap();
        TEST_DATA_STORE.with_borrow_mut(|store| store.insert(raw_key, raw_row));

        let index_key = IndexKey::new(&indexed, &INDEX_MODEL)
            .expect("index key")
            .expect("index key missing");
        let raw_index_key = index_key.to_raw();
        let entry = IndexEntry::new(indexed.key());
        let raw_entry = RawIndexEntry::try_from_entry(&entry).unwrap();
        TEST_INDEX_STORE.with_borrow_mut(|store| store.insert(raw_index_key, raw_entry));

        let incoming = TestEntity {
            id: Ulid::from_u128(3),
            tag: "alpha".to_string(),
        };

        let err = plan_index_mutation_for_entity::<TestEntity>(&DB, None, Some(&incoming))
            .expect_err("expected row key mismatch corruption");
        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Index);
    }
}
