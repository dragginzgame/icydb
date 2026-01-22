//! IcyDB commit protocol and atomicity guardrails.
//!
//! Contract: once `begin_commit` succeeds, no fallible work or async/yield is
//! permitted until `finish_commit` completes. The commit marker must cover all
//! mutations, and recovery replays index ops before data ops.

use crate::{
    MAX_INDEX_FIELDS,
    db::{
        Db,
        index::{IndexKey, MAX_INDEX_ENTRY_BYTES, RawIndexEntry, RawIndexKey},
        store::{DataStore, MAX_ROW_BYTES, RawDataKey, RawRow},
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
const COMMIT_META_PADDING: u32 = 1024;
#[allow(clippy::cast_possible_truncation)]
pub const MAX_COMMIT_BYTES: u32 = MAX_ROW_BYTES
    .saturating_add(MAX_INDEX_ENTRY_BYTES.saturating_mul(MAX_INDEX_FIELDS as u32))
    .saturating_add(COMMIT_META_PADDING);

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum CommitKind {
    Save,
    Delete,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum CommitPhase {
    Started,
    IndexWritten,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitIndexOp {
    pub store: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitDataOp {
    pub store: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitMarker {
    pub id: [u8; COMMIT_ID_BYTES],
    pub kind: CommitKind,
    pub phase: CommitPhase,
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
            phase: CommitPhase::Started,
            index_ops,
            data_ops,
        })
    }
}

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

    fn set(&mut self, marker: &CommitMarker) -> Result<(), InternalError> {
        let raw = RawCommitMarker::try_from_marker(marker)?;
        self.cell.set(raw);
        Ok(())
    }

    fn set_infallible(&mut self, marker: &CommitMarker) {
        let raw = RawCommitMarker::try_from_marker(marker)
            .expect("commit marker encode failed after prevalidation");
        self.cell.set(raw);
    }

    fn clear_infallible(&mut self) {
        self.cell.set(RawCommitMarker::empty());
    }
}

#[derive(Clone, Debug)]
pub struct CommitGuard {
    marker: CommitMarker,
}

impl CommitGuard {
    fn mark_index_written(&mut self) {
        debug_assert!(
            matches!(self.marker.phase, CommitPhase::Started),
            "commit phase must transition from Started -> IndexWritten once"
        );
        self.marker.phase = CommitPhase::IndexWritten;
        with_commit_store_infallible(|store| store.set_infallible(&self.marker));
    }

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
    apply_indexes: impl FnOnce(),
    apply_data: impl FnOnce(),
) {
    // COMMIT WINDOW:
    // Do not introduce fallible work or async/yield after `begin_commit`.
    // Apply is infallible or traps; recovery replays marker on next mutation.
    // Centralize commit phases so executors stay infallible after mutation begins,
    // preserving Stage-1's "no fallible work after first stable write" invariant.
    apply_indexes();
    guard.mark_index_written();
    apply_data();
    guard.clear();
}

// -----------------------------------------------------------------------------
// Recovery
// -----------------------------------------------------------------------------

static COMMIT_STORE_ID: OnceLock<u8> = OnceLock::new();
static RECOVERED: OnceLock<()> = OnceLock::new();

// Recovery is invoked only from mutation entrypoints; read paths must remain
// side-effect free to avoid re-entrancy and performance risks.
pub fn ensure_recovered(db: &Db<impl crate::traits::CanisterKind>) -> Result<(), InternalError> {
    if RECOVERED.get().is_some() {
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

struct DecodedIndexOp {
    store: &'static std::thread::LocalKey<RefCell<crate::db::index::IndexStore>>,
    key: RawIndexKey,
    value: Option<RawIndexEntry>,
}

struct DecodedDataOp {
    store: &'static std::thread::LocalKey<RefCell<DataStore>>,
    key: RawDataKey,
    value: Option<RawRow>,
}

fn prevalidate_recovery(
    db: &Db<impl crate::traits::CanisterKind>,
    marker: &CommitMarker,
) -> Result<(Vec<DecodedIndexOp>, Vec<DecodedDataOp>), InternalError> {
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

    Ok(<RawIndexKey as Storable>::from_bytes(Cow::Borrowed(bytes)))
}

fn decode_index_entry(bytes: &[u8]) -> Result<RawIndexEntry, InternalError> {
    if bytes.len() > MAX_INDEX_ENTRY_BYTES as usize {
        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Index,
            "commit marker index entry exceeds max size",
        ));
    }

    Ok(<RawIndexEntry as Storable>::from_bytes(Cow::Borrowed(
        bytes,
    )))
}

fn decode_data_key(bytes: &[u8]) -> Result<RawDataKey, InternalError> {
    if bytes.len() != crate::db::store::DataKey::STORED_SIZE as usize {
        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            "commit marker data key has invalid length",
        ));
    }

    Ok(<RawDataKey as Storable>::from_bytes(Cow::Borrowed(bytes)))
}
