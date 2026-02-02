//! IcyDB commit protocol and atomicity guardrails.
//!
//! Contract: once `begin_commit` succeeds, mutations must either complete
//! successfully or roll back before `finish_commit` returns. The commit marker
//! must cover all mutations, and recovery replays index ops before data ops.
//!
//! ## Commit Boundary and Authority of CommitMarker
//!
//! The `CommitMarker` fully specifies every index and data mutation. After
//! the marker is persisted, executors must not re-derive semantics or branch
//! on entity/index contents; apply logic deterministically replays the marker
//! ops. Recovery replays commit ops as recorded, not planner logic.

mod decode;
mod memory;
mod recovery;
mod store;

use crate::{
    db::commit::store::{CommitStore, with_commit_store, with_commit_store_infallible},
    error::{ErrorClass, ErrorOrigin, InternalError},
    types::Ulid,
};
use serde::{Deserialize, Serialize};

pub use recovery::ensure_recovered;

#[cfg(test)]
/// Return true if a commit marker is currently persisted.
pub fn commit_marker_present() -> Result<bool, InternalError> {
    store::commit_marker_present()
}

// Stage-2 invariant:
// - We persist a commit marker before any stable mutation.
// - After marker creation, executor apply phases are infallible or trap.
// - Recovery replays the stored mutation plan (index ops, then data ops).
// This makes partial mutations deterministic without a WAL.

const COMMIT_LABEL: &str = "CommitMarker";
const COMMIT_ID_BYTES: usize = 16;

// Conservative upper bound to avoid rejecting valid commits when index entries
// are large; still small enough to fit typical canister constraints.
pub const MAX_COMMIT_BYTES: u32 = 16 * 1024 * 1024;

///
/// CommitKind
///

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum CommitKind {
    Save,
    Delete,
}

///
/// CommitIndexOp
///
/// Raw index mutation recorded in a commit marker.
/// Carries store identity plus raw key/value bytes.

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitIndexOp {
    pub store: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

///
/// CommitDataOp
///
/// Raw data-store mutation recorded in a commit marker.
/// Carries store identity plus raw key/value bytes.

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitDataOp {
    pub store: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

///
/// CommitMarker
///
/// Persisted mutation plan covering all index and data operations.
/// Recovery replays the marker exactly as stored.
/// Unknown fields are rejected as corruption; commit markers are not forward-compatible.
/// This is internal commit-protocol metadata, not a user-schema type.

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitMarker {
    pub id: [u8; COMMIT_ID_BYTES],
    pub kind: CommitKind,
    pub index_ops: Vec<CommitIndexOp>,
    pub data_ops: Vec<CommitDataOp>,
}

impl CommitMarker {
    /// Construct a new commit marker with a fresh commit id.
    pub fn new(
        kind: CommitKind,
        index_ops: Vec<CommitIndexOp>,
        data_ops: Vec<CommitDataOp>,
    ) -> Result<Self, InternalError> {
        let id = Ulid::try_generate()
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit id generation failed: {err}"),
                )
            })?
            .to_bytes();

        Ok(Self {
            id,
            kind,
            index_ops,
            data_ops,
        })
    }
}

///
/// CommitGuard
///
/// In-flight commit handle that clears the marker on completion.
/// Must not be leaked across mutation boundaries.
///

#[derive(Clone, Debug)]
pub struct CommitGuard {
    pub marker: CommitMarker,
}

impl CommitGuard {
    // Clear the commit marker without surfacing errors.
    fn clear(self) {
        let _ = self;
        with_commit_store_infallible(CommitStore::clear_infallible);
    }
}

/// Persist a commit marker and open the commit window.
pub fn begin_commit(marker: CommitMarker) -> Result<CommitGuard, InternalError> {
    with_commit_store(|store| {
        if store.load()?.is_some() {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Store,
                "commit marker already present before begin",
            ));
        }
        store.set(&marker)?;

        Ok(CommitGuard { marker })
    })
}

/// Apply commit ops and clear the marker regardless of outcome.
pub fn finish_commit(
    mut guard: CommitGuard,
    apply: impl FnOnce(&mut CommitGuard) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    // COMMIT WINDOW:
    // Apply must either complete successfully or roll back all mutations before
    // returning an error. We clear the marker on any outcome so recovery does
    // not replay an already-rolled-back write.
    let result = apply(&mut guard);
    let commit_id = guard.marker.id;
    guard.clear();
    // Internal invariant: commit markers must not persist after a finished mutation.
    assert!(
        with_commit_store_infallible(|store| store.is_empty()),
        "commit marker must be cleared after finish_commit (commit_id={commit_id:?})"
    );
    result
}

///
/// TESTS
///
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            Db,
            index::{IndexEntry, IndexKey, IndexStore, IndexStoreRegistry, RawIndexEntry},
            store::{DataKey, DataStore, DataStoreRegistry, RawRow},
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

    #[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
    struct TestEntity {
        id: Ulid,
        name: String,
    }

    crate::test_entity! {
        entity TestEntity {
            path: "commit_test::TestEntity",
            pk: id: Ulid,

            fields { id: Ulid, name: Text }

            indexes { index index_name(name) unique; }
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
                "name" => Some(Value::Text(self.name.clone())),
                _ => None,
            }
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
}
