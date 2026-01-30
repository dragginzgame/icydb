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
    db::{
        Db,
        index::{IndexKey, MAX_INDEX_ENTRY_BYTES, RawIndexEntry, RawIndexKey},
        store::{DataKey, DataStore, RawDataKey, RawRow},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    serialize::{deserialize, serialize},
    types::Ulid,
};
use canic_cdk::structures::{
    Cell as StableCell, DefaultMemoryImpl, Storable,
    memory::{MemoryId, VirtualMemory},
    storable::Bound,
};
use canic_memory::{
    MEMORY_MANAGER,
    registry::{MemoryRange, MemoryRegistry, MemoryRegistryEntry},
    runtime::registry::MemoryRegistryRuntime,
};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, cell::RefCell, collections::BTreeSet, sync::OnceLock};

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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitIndexOp {
    pub store: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

///
/// CommitDataOp
///

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitDataOp {
    pub store: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

///
/// CommitMarker
///

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitMarker {
    pub id: [u8; COMMIT_ID_BYTES],
    pub kind: CommitKind,
    pub index_ops: Vec<CommitIndexOp>,
    pub data_ops: Vec<CommitDataOp>,
}

impl CommitMarker {
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
/// RawCommitMarker
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawCommitMarker(Vec<u8>);

impl RawCommitMarker {
    const fn empty() -> Self {
        Self(Vec::new())
    }

    const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn try_from_marker(marker: &CommitMarker) -> Result<Self, InternalError> {
        let bytes = serialize(marker)?;
        if bytes.len() > MAX_COMMIT_BYTES as usize {
            return Err(InternalError::new(
                ErrorClass::Unsupported,
                ErrorOrigin::Store,
                format!(
                    "commit marker exceeds max size: {} bytes (limit {MAX_COMMIT_BYTES})",
                    bytes.len()
                ),
            ));
        }
        Ok(Self(bytes))
    }

    fn try_decode(&self) -> Result<Option<CommitMarker>, InternalError> {
        if self.is_empty() {
            return Ok(None);
        }

        deserialize::<CommitMarker>(&self.0)
            .map(Some)
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    format!("commit marker corrupted: {err}"),
                )
            })
    }
}

impl Storable for RawCommitMarker {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: MAX_COMMIT_BYTES,
        is_fixed_size: false,
    };
}

///
/// CommitStore
///

struct CommitStore {
    cell: StableCell<RawCommitMarker, VirtualMemory<DefaultMemoryImpl>>,
}

impl CommitStore {
    fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        let cell = StableCell::init(memory, RawCommitMarker::empty());
        Self { cell }
    }

    fn load(&self) -> Result<Option<CommitMarker>, InternalError> {
        self.cell.get().try_decode()
    }

    fn is_empty(&self) -> bool {
        self.cell.get().is_empty()
    }

    fn set(&mut self, marker: &CommitMarker) -> Result<(), InternalError> {
        let raw = RawCommitMarker::try_from_marker(marker)?;
        self.cell.set(raw);
        Ok(())
    }

    fn clear_infallible(&mut self) {
        self.cell.set(RawCommitMarker::empty());
    }
}

///
/// CommitGuard
///

#[derive(Clone, Debug)]
pub struct CommitGuard {
    pub marker: CommitMarker,
}

impl CommitGuard {
    fn clear(self) {
        let _ = self;
        with_commit_store_infallible(CommitStore::clear_infallible);
    }
}

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

// -----------------------------------------------------------------------------
// Recovery
// -----------------------------------------------------------------------------

static COMMIT_STORE_ID: OnceLock<u8> = OnceLock::new();
static RECOVERED: OnceLock<()> = OnceLock::new();

#[cfg(test)]
thread_local! {
    static FORCE_RECOVERY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub fn force_recovery_for_tests() {
    FORCE_RECOVERY.with(|flag| flag.set(true));
}

#[allow(clippy::missing_const_for_fn)]
fn should_force_recovery() -> bool {
    #[cfg(test)]
    {
        FORCE_RECOVERY.with(|flag| {
            let force = flag.get();
            if force {
                flag.set(false);
            }
            force
        })
    }

    #[cfg(not(test))]
    {
        false
    }
}

// Recovery is invoked from read and mutation entrypoints to prevent
// observing partial commit state.
pub fn ensure_recovered(db: &Db<impl crate::traits::CanisterKind>) -> Result<(), InternalError> {
    let force = should_force_recovery();
    if !force && RECOVERED.get().is_some() {
        return Ok(());
    }

    let marker = with_commit_store(|store| store.load())?;
    if let Some(marker) = marker {
        let (index_ops, data_ops) = prevalidate_recovery(db, &marker)?;
        apply_recovery_ops(index_ops, data_ops);
        with_commit_store(|store| {
            store.clear_infallible();
            Ok(())
        })?;
    }

    let _ = RECOVERED.set(());
    Ok(())
}

///
/// DecodedIndexOp
///

struct DecodedIndexOp {
    store: &'static std::thread::LocalKey<RefCell<crate::db::index::IndexStore>>,
    key: RawIndexKey,
    value: Option<RawIndexEntry>,
}

///
/// DecodedDataOp
///

struct DecodedDataOp {
    store: &'static std::thread::LocalKey<RefCell<DataStore>>,
    key: RawDataKey,
    value: Option<RawRow>,
}

fn prevalidate_recovery(
    db: &Db<impl crate::traits::CanisterKind>,
    marker: &CommitMarker,
) -> Result<(Vec<DecodedIndexOp>, Vec<DecodedDataOp>), InternalError> {
    match marker.kind {
        CommitKind::Save => {
            if marker.data_ops.iter().any(|op| op.value.is_none()) {
                return Err(InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    "commit marker corrupted: save op missing data payload",
                ));
            }
        }
        CommitKind::Delete => {
            if marker.data_ops.iter().any(|op| op.value.is_some()) {
                return Err(InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    "commit marker corrupted: delete op includes data payload",
                ));
            }
        }
    }

    let mut decoded_index = Vec::with_capacity(marker.index_ops.len());
    let mut decoded_data = Vec::with_capacity(marker.data_ops.len());

    for op in &marker.index_ops {
        let store = db
            .with_index(|reg| reg.try_get_store(&op.store))
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Index,
                    format!("missing index store '{}': {err}", op.store),
                )
            })?;
        let key = decode_index_key(&op.key)?;
        let value = match &op.value {
            Some(bytes) => Some(decode_index_entry(bytes)?),
            None => None,
        };
        decoded_index.push(DecodedIndexOp { store, key, value });
    }

    for op in &marker.data_ops {
        let store = db
            .with_data(|reg| reg.try_get_store(&op.store))
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    format!("missing data store '{}': {err}", op.store),
                )
            })?;
        let key = decode_data_key(&op.key)?;
        let value = match &op.value {
            Some(bytes) => Some(RawRow::try_new(bytes.clone())?),
            None => None,
        };
        decoded_data.push(DecodedDataOp { store, key, value });
    }

    Ok((decoded_index, decoded_data))
}

fn apply_recovery_ops(index_ops: Vec<DecodedIndexOp>, data_ops: Vec<DecodedDataOp>) {
    // Apply indexes first, then data, mirroring executor ordering.
    for op in index_ops {
        op.store.with_borrow_mut(|store| {
            if let Some(value) = op.value {
                store.insert(op.key, value);
            } else {
                store.remove(&op.key);
            }
        });
    }

    for op in data_ops {
        op.store.with_borrow_mut(|store| {
            if let Some(value) = op.value {
                store.insert(op.key, value);
            } else {
                store.remove(&op.key);
            }
        });
    }
}

// -----------------------------------------------------------------------------
// Commit store plumbing
// -----------------------------------------------------------------------------

#[cfg(test)]
pub fn commit_marker_present() -> Result<bool, InternalError> {
    with_commit_store(|store| Ok(store.load()?.is_some()))
}

thread_local! {
    static COMMIT_STORE: RefCell<Option<CommitStore>> = const { RefCell::new(None) };
}

fn with_commit_store<R>(
    f: impl FnOnce(&mut CommitStore) -> Result<R, InternalError>,
) -> Result<R, InternalError> {
    COMMIT_STORE.with(|cell| {
        if cell.borrow().is_none() {
            // StableCell::init performs a benign stable write for the empty marker.
            let store = CommitStore::init(commit_memory()?);
            *cell.borrow_mut() = Some(store);
        }
        let mut guard = cell.borrow_mut();
        let store = guard.as_mut().expect("commit store missing after init");
        f(store)
    })
}

fn with_commit_store_infallible<R>(f: impl FnOnce(&mut CommitStore) -> R) -> R {
    COMMIT_STORE.with(|cell| {
        let mut guard = cell.borrow_mut();
        let store = guard.as_mut().expect("commit store not initialized");
        f(store)
    })
}

fn commit_memory() -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    let id = commit_memory_id()?;
    Ok(MEMORY_MANAGER.with_borrow_mut(|mgr| mgr.get(MemoryId::new(id))))
}

fn commit_memory_id() -> Result<u8, InternalError> {
    if let Some(id) = COMMIT_STORE_ID.get() {
        return Ok(*id);
    }

    MemoryRegistryRuntime::init(None).map_err(|err| {
        InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Store,
            format!("memory registry init failed: {err}"),
        )
    })?;

    let (owner, range, used_ids) = select_commit_range()?;
    let id = allocate_commit_id(range, &used_ids)?;
    MemoryRegistry::register(id, &owner, COMMIT_LABEL).map_err(|err| {
        InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Store,
            format!("commit memory id registration failed: {err}"),
        )
    })?;

    let _ = COMMIT_STORE_ID.set(id);
    Ok(id)
}

fn select_commit_range() -> Result<(String, MemoryRange, BTreeSet<u8>), InternalError> {
    let snapshots = MemoryRegistryRuntime::snapshot_ids_by_range();
    for snapshot in snapshots {
        if snapshot
            .entries
            .iter()
            .any(|(_, entry)| is_db_store_entry(entry))
        {
            let used_ids = snapshot
                .entries
                .iter()
                .map(|(id, _)| *id)
                .collect::<BTreeSet<_>>();
            return Ok((snapshot.owner, snapshot.range, used_ids));
        }
    }

    Err(InternalError::new(
        ErrorClass::Internal,
        ErrorOrigin::Store,
        "unable to locate reserved memory range for commit markers",
    ))
}

fn allocate_commit_id(range: MemoryRange, used: &BTreeSet<u8>) -> Result<u8, InternalError> {
    for id in (range.start..=range.end).rev() {
        if !used.contains(&id) {
            return Ok(id);
        }
    }

    Err(InternalError::new(
        ErrorClass::Unsupported,
        ErrorOrigin::Store,
        format!(
            "no free memory ids available for commit markers in range {}-{}",
            range.start, range.end
        ),
    ))
}

fn is_db_store_entry(entry: &MemoryRegistryEntry) -> bool {
    entry.label.ends_with("DataStore") || entry.label.ends_with("IndexStore")
}

// -----------------------------------------------------------------------------
// Raw decoding helpers
// -----------------------------------------------------------------------------

fn decode_index_key(bytes: &[u8]) -> Result<RawIndexKey, InternalError> {
    if bytes.len() != IndexKey::STORED_SIZE as usize {
        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Index,
            "commit marker index key has invalid length",
        ));
    }

    let raw = <RawIndexKey as Storable>::from_bytes(Cow::Borrowed(bytes));
    IndexKey::try_from_raw(&raw).map_err(|err| {
        InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Index,
            format!("commit marker index key corrupted: {err}"),
        )
    })?;
    Ok(raw)
}

fn decode_index_entry(bytes: &[u8]) -> Result<RawIndexEntry, InternalError> {
    if bytes.len() > MAX_INDEX_ENTRY_BYTES as usize {
        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Index,
            "commit marker index entry exceeds max size",
        ));
    }

    let raw = <RawIndexEntry as Storable>::from_bytes(Cow::Borrowed(bytes));
    raw.try_decode().map_err(|err| {
        InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Index,
            format!("commit marker index entry corrupted: {err}"),
        )
    })?;
    Ok(raw)
}

fn decode_data_key(bytes: &[u8]) -> Result<RawDataKey, InternalError> {
    if bytes.len() != DataKey::STORED_SIZE as usize {
        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            "commit marker data key has invalid length",
        ));
    }

    let raw = <RawDataKey as Storable>::from_bytes(Cow::Borrowed(bytes));
    DataKey::try_from_raw(&raw).map_err(|err| {
        InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            format!("commit marker data key corrupted: {err}"),
        )
    })?;
    Ok(raw)
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
    use canic_memory::runtime::registry::MemoryRegistryRuntime;
    use serde::{Deserialize, Serialize};
    use std::{cell::RefCell, sync::Once};

    const CANISTER_PATH: &str = "commit_test::TestCanister";
    const DATA_STORE_PATH: &str = "commit_test::TestDataStore";
    const INDEX_STORE_PATH: &str = "commit_test::TestIndexStore";
    const ENTITY_PATH: &str = "commit_test::TestEntity";

    const INDEX_FIELDS: [&str; 1] = ["name"];
    const INDEX_MODEL: IndexModel = IndexModel::new(
        "commit_test::index_name",
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
            name: "name",
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
        name: String,
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
                "name" => Some(Value::Text(self.name.clone())),
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
        const FIELDS: &'static [&'static str] = &["id", "name"];
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
            RefCell::new(DataStore::init(canic_memory::ic_memory!(DataStore, 10)));
    }

    canic_memory::eager_static! {
        static TEST_INDEX_STORE: RefCell<IndexStore> =
            RefCell::new(IndexStore::init(canic_memory::ic_memory!(IndexStore, 11)));
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
        TEST_DATA_STORE.with_borrow_mut(|store| store.clear());
        TEST_INDEX_STORE.with_borrow_mut(|store| store.clear());
        init_memory_registry();
        let _ = with_commit_store(|store| {
            store.clear_infallible();
            Ok(())
        });
    }

    #[test]
    fn commit_marker_recovery_rejects_corrupted_index_key() {
        reset_stores();

        // Stage 1: build a valid commit marker payload.
        let entity = TestEntity {
            id: Ulid::from_u128(7),
            name: "alpha".to_string(),
        };
        let data_key = DataKey::new::<TestEntity>(entity.id);
        let raw_data_key = data_key.to_raw().expect("data key encode");
        let raw_row = RawRow::try_new(serialize(&entity).unwrap()).unwrap();

        let index_key = IndexKey::new(&entity, &INDEX_MODEL)
            .expect("index key")
            .expect("index key missing");
        let raw_index_key = index_key.to_raw();
        let entry = IndexEntry::new(entity.key());
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

        // Stage 2: corrupt the stored index key bytes.
        if let Some(last) = marker.index_ops[0].key.last_mut() {
            *last ^= 0xFF;
        }

        let _guard = begin_commit(marker).unwrap();
        assert!(commit_marker_present().unwrap());

        // Stage 3: recovery should fail with a corruption error.
        force_recovery_for_tests();
        let err = ensure_recovered(&DB).expect_err("corrupted marker should fail recovery");
        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Index);

        let _ = with_commit_store(|store| {
            store.clear_infallible();
            Ok(())
        });
    }

    #[test]
    fn recovery_rejects_delete_marker_with_payload() {
        reset_stores();

        let entity = TestEntity {
            id: Ulid::from_u128(8),
            name: "alpha".to_string(),
        };
        let data_key = DataKey::new::<TestEntity>(entity.id);
        let raw_data_key = data_key.to_raw().expect("data key encode");
        let raw_row = RawRow::try_new(serialize(&entity).unwrap()).unwrap();

        let marker = CommitMarker::new(
            CommitKind::Delete,
            Vec::new(),
            vec![CommitDataOp {
                store: DATA_STORE_PATH.to_string(),
                key: raw_data_key.as_bytes().to_vec(),
                value: Some(raw_row.as_bytes().to_vec()),
            }],
        )
        .unwrap();

        let _guard = begin_commit(marker).unwrap();
        assert!(commit_marker_present().unwrap());

        force_recovery_for_tests();
        let err = ensure_recovered(&DB).expect_err("delete marker payload should fail recovery");
        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Store);

        let _ = with_commit_store(|store| {
            store.clear_infallible();
            Ok(())
        });
    }
}
