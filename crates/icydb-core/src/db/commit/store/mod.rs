//! Module: db::commit::store
//! Responsibility: persist, load, and clear commit markers in stable memory.
//! Does not own: marker shape semantics, recovery orchestration, or commit-window policy.
//! Boundary: commit::{guard,recovery} -> commit::store (one-way).

mod bytes;
mod control_slot;
mod marker_envelope;
#[cfg(test)]
mod tests;

use crate::{
    db::{
        commit::{
            marker::{CommitMarker, MAX_COMMIT_BYTES, validate_commit_marker_shape},
            memory::{
                CommitMemoryAllocation, commit_memory_handle, current_commit_memory_allocation,
            },
            store::{
                control_slot::{
                    COMMIT_CONTROL_HEADER_BYTES, commit_control_slot_encoded_len,
                    decode_commit_control_slot, encode_commit_control_slot_from_marker,
                    encode_empty_commit_control_slot, inspect_commit_control_slot,
                },
                marker_envelope::decode_commit_marker,
            },
        },
        database_format::{DATABASE_BOOT_RECORD_BYTES, validate_current_boot_record},
        integrity::{DatabaseIncarnationId, generate_database_incarnation_id},
    },
    error::InternalError,
};
use ic_stable_structures::{DefaultMemoryImpl, Memory, memory_manager::VirtualMemory};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
#[cfg(not(test))]
use std::sync::{Mutex, OnceLock};

#[cfg(test)]
use crate::db::commit::failpoint::{CommitFailpoint, hit_commit_failpoint};
#[cfg(test)]
use crate::db::commit::store::control_slot::encode_commit_control_slot;
#[cfg(test)]
use crate::db::commit::store::marker_envelope::encode_commit_marker_bytes;
use crate::db::database_format::crc32c;
#[cfg(test)]
use crate::db::database_format::initialize_current_database_control_for_tests;

#[cfg(feature = "sql")]
pub(in crate::db::commit) use control_slot::commit_control_slot_encoded_len_for_marker_payload;

#[cfg(not(test))]
static COMMIT_MARKER_PRESENCE_HINTS: OnceLock<Mutex<Vec<CommitMarkerPresenceHint>>> =
    OnceLock::new();

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CommitMarkerPresenceHint {
    allocation: CommitMemoryAllocation,
    may_be_present: bool,
}

///
/// RawCommitMarker
///
/// Raw, bounded commit control-plane bytes decoded from stable memory.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawCommitMarker(Vec<u8>);

impl RawCommitMarker {
    const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Deserialize the stored payload, treating failures as corruption.
    fn try_decode(&self) -> Result<Option<CommitMarker>, InternalError> {
        // Phase 1: fast empty-marker check.
        if self.is_empty() {
            return Ok(None);
        }

        // Phase 2: enforce byte-size upper bound before decode.
        if self.0.len() > MAX_COMMIT_BYTES as usize {
            return Err(InternalError::commit_marker_exceeds_max_size());
        }

        // Phase 3: decode + semantic shape validation.
        let marker = decode_commit_marker(&self.0)?;
        validate_commit_marker_shape(&marker)?;

        Ok(Some(marker))
    }
}

#[cfg(test)]
pub(in crate::db) fn validate_commit_marker_envelope_for_tests(
    bytes: &[u8],
) -> Result<(), InternalError> {
    RawCommitMarker(bytes.to_vec()).try_decode().map(drop)
}

///
/// CommitStore
///
/// Database-wide control store over the existing commit allocation.
/// The permanent format prefix is followed by one bounded transient marker slot.
///

pub(super) struct CommitStore {
    memory: VirtualMemory<DefaultMemoryImpl>,
}

const DATABASE_CONTROL_SLOT_FRAME_OFFSET: u64 = DATABASE_BOOT_RECORD_BYTES as u64;
const DATABASE_CONTROL_SLOT_FRAME_MAGIC: &[u8; 4] = b"IDCS";
const DATABASE_CONTROL_SLOT_FRAME_VERSION: u8 = 1;
const DATABASE_CONTROL_SLOT_FRAME_HEADER_BYTES: usize = 13;
const DATABASE_CONTROL_SLOT_FRAME_LENGTH_OFFSET: usize = 5;
const DATABASE_CONTROL_SLOT_FRAME_CHECKSUM_OFFSET: usize = 9;
const COMMIT_CONTROL_SLOT_OFFSET: u64 =
    DATABASE_CONTROL_SLOT_FRAME_OFFSET + DATABASE_CONTROL_SLOT_FRAME_HEADER_BYTES as u64;
const WASM_PAGE_BYTES: u64 = 65_536;

impl CommitStore {
    /// Encode one raw commit-control slot payload for recovery tests.
    #[cfg(test)]
    pub(super) fn encode_raw_control_slot_for_tests(
        marker_bytes: Vec<u8>,
    ) -> Result<Vec<u8>, InternalError> {
        encode_commit_control_slot(DatabaseIncarnationId::for_tests(0x31), &marker_bytes)
    }

    /// Encode one raw commit-marker envelope for recovery tests.
    #[cfg(test)]
    pub(super) fn encode_raw_marker_envelope_for_tests(
        format_version: u8,
        marker_payload: Vec<u8>,
    ) -> Result<Vec<u8>, InternalError> {
        encode_commit_marker_bytes(format_version, &marker_payload)
    }

    /// Encode one multi-row commit-control slot payload for regression tests.
    #[cfg(test)]
    pub(super) fn encode_raw_direct_control_slot_for_tests(
        marker: &CommitMarker,
    ) -> Result<Vec<u8>, InternalError> {
        encode_commit_control_slot_from_marker(DatabaseIncarnationId::for_tests(0x31), marker)
    }

    /// Open the database control store after format admission.
    fn open(memory: VirtualMemory<DefaultMemoryImpl>) -> Result<Self, InternalError> {
        validate_current_boot_record(&memory)?;
        let store = Self { memory };
        if store.control_slot_is_uninitialized() {
            let incarnation = generate_database_incarnation_id()?;
            store.write_control_slot(&encode_empty_commit_control_slot(incarnation))?;
        } else {
            store.read_control_slot()?;
        }
        Ok(store)
    }

    /// Initialize one current-format database control store for direct tests.
    #[cfg(test)]
    fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        initialize_current_database_control_for_tests(&memory);
        Self::open(memory).expect("test database control store should initialize")
    }

    /// Load and decode the current commit marker (if any).
    pub(super) fn load(&self) -> Result<Option<CommitMarker>, InternalError> {
        let control_slot = self.read_control_slot()?;
        let marker_bytes = decode_commit_control_slot(&control_slot)?;

        RawCommitMarker(marker_bytes).try_decode()
    }

    /// Load the durable database-lifecycle identity.
    pub(super) fn database_incarnation_id(&self) -> Result<DatabaseIncarnationId, InternalError> {
        self.read_control_slot()
            .and_then(|bytes| Ok(inspect_commit_control_slot(&bytes)?.database_incarnation_id))
    }

    /// Fingerprint the exact current database-control envelope.
    ///
    /// This is Deep inspection proof state, not schema meaning. Marker writes,
    /// clears, or incarnation replacement necessarily change it.
    pub(super) fn proof_identity(&self) -> Result<[u8; 32], InternalError> {
        let mut hasher = Sha256::new();
        hasher.update(b"icydb.database-control-proof.v1");
        hasher.update(self.read_control_slot()?);
        Ok(hasher.finalize().into())
    }

    /// Return whether the marker slot is empty without decoding.
    pub(super) fn is_empty(&self) -> bool {
        self.read_control_slot()
            .and_then(|bytes| {
                inspect_commit_control_slot(&bytes).map(|slot| slot.marker_bytes.is_empty())
            })
            .unwrap_or(false)
    }

    /// Return whether the marker payload is empty while still validating the
    /// outer control-slot envelope.
    pub(super) fn marker_is_empty(&self) -> Result<bool, InternalError> {
        self.read_control_slot().and_then(|bytes| {
            inspect_commit_control_slot(&bytes).map(|slot| slot.marker_bytes.is_empty())
        })
    }

    /// Persist one commit marker while proving the current slot has no marker.
    pub(super) fn set_if_empty(&self, marker: &CommitMarker) -> Result<(), InternalError> {
        let database_incarnation_id = self.require_empty_marker_slot()?;
        let encoded = encode_commit_control_slot_from_marker(database_incarnation_id, marker)?;

        #[cfg(test)]
        hit_commit_failpoint(CommitFailpoint::BeforeMarkerWrite)?;
        self.write_control_slot(&encoded)?;
        mark_commit_marker_may_be_present();
        #[cfg(test)]
        hit_commit_failpoint(CommitFailpoint::AfterMarkerWrite)?;
        Ok(())
    }

    /// Clear marker bytes after a verified commit/recovery success.
    pub(super) fn clear_verified(&self) -> Result<(), InternalError> {
        let control_slot = self.read_control_slot()?;
        let slot = inspect_commit_control_slot(&control_slot)?;
        #[cfg(test)]
        hit_commit_failpoint(CommitFailpoint::BeforeMarkerClear)?;
        self.write_control_slot(&encode_empty_commit_control_slot(
            slot.database_incarnation_id,
        ))?;
        mark_commit_marker_verified_absent();
        #[cfg(test)]
        hit_commit_failpoint(CommitFailpoint::AfterMarkerClear)?;

        Ok(())
    }

    /// Clear the marker slot directly for tests that intentionally persist corruption.
    #[cfg(test)]
    pub(super) fn clear_raw_for_tests(&self) {
        let incarnation = self
            .database_incarnation_id()
            .unwrap_or_else(|_| DatabaseIncarnationId::for_tests(0x31));
        self.write_control_slot(&encode_empty_commit_control_slot(incarnation))
            .expect("test database control slot should clear");
        mark_commit_marker_verified_absent();
    }

    /// Overwrite the raw marker bytes directly for recovery tests.
    #[cfg(test)]
    pub(super) fn set_raw_marker_bytes_for_tests(&self, bytes: Vec<u8>) {
        if bytes.is_empty() {
            mark_commit_marker_verified_absent();
        } else {
            mark_commit_marker_may_be_present();
        }

        let encoded = if bytes.is_empty() {
            let incarnation = self
                .database_incarnation_id()
                .unwrap_or_else(|_| DatabaseIncarnationId::for_tests(0x31));
            encode_empty_commit_control_slot(incarnation)
        } else {
            bytes
        };
        self.write_control_slot(&encoded)
            .expect("test raw commit marker bytes should fit control memory");
    }

    // Decode the control slot once and require that no marker bytes are present
    // before commit-window open persists a fresh marker.
    fn require_empty_marker_slot(&self) -> Result<DatabaseIncarnationId, InternalError> {
        let bytes = self.read_control_slot()?;
        let slot = inspect_commit_control_slot(&bytes)?;
        if !slot.marker_bytes.is_empty() {
            return Err(InternalError::store_invariant());
        }

        Ok(slot.database_incarnation_id)
    }

    fn control_slot_is_uninitialized(&self) -> bool {
        let mut header = [0_u8; DATABASE_CONTROL_SLOT_FRAME_HEADER_BYTES];
        self.memory
            .read(DATABASE_CONTROL_SLOT_FRAME_OFFSET, &mut header);
        header.iter().all(|byte| *byte == 0)
    }

    fn read_control_slot(&self) -> Result<Vec<u8>, InternalError> {
        validate_current_boot_record(&self.memory)?;
        let bytes = self.read_framed_control_slot()?;
        let encoded_len = commit_control_slot_encoded_len(&bytes)?;
        if encoded_len != bytes.len() {
            return Err(InternalError::commit_corruption());
        }
        Ok(bytes)
    }

    fn read_framed_control_slot(&self) -> Result<Vec<u8>, InternalError> {
        let mut header = [0_u8; DATABASE_CONTROL_SLOT_FRAME_HEADER_BYTES];
        self.memory
            .read(DATABASE_CONTROL_SLOT_FRAME_OFFSET, &mut header);
        if &header[..DATABASE_CONTROL_SLOT_FRAME_MAGIC.len()] != DATABASE_CONTROL_SLOT_FRAME_MAGIC {
            return Err(InternalError::commit_corruption());
        }
        if header[DATABASE_CONTROL_SLOT_FRAME_MAGIC.len()] != DATABASE_CONTROL_SLOT_FRAME_VERSION {
            return Err(InternalError::serialize_incompatible_persisted_format());
        }

        let mut length_bytes = [0_u8; size_of::<u32>()];
        length_bytes.copy_from_slice(
            &header[DATABASE_CONTROL_SLOT_FRAME_LENGTH_OFFSET
                ..DATABASE_CONTROL_SLOT_FRAME_CHECKSUM_OFFSET],
        );
        let encoded_len = u32::from_be_bytes(length_bytes) as usize;
        if !(COMMIT_CONTROL_HEADER_BYTES..=MAX_COMMIT_BYTES as usize).contains(&encoded_len) {
            return Err(InternalError::commit_corruption());
        }
        let end = COMMIT_CONTROL_SLOT_OFFSET.saturating_add(encoded_len as u64);
        if end > self.memory.size().saturating_mul(WASM_PAGE_BYTES) {
            return Err(InternalError::commit_corruption());
        }

        let mut bytes = vec![0_u8; encoded_len];
        self.memory.read(COMMIT_CONTROL_SLOT_OFFSET, &mut bytes);
        let mut checksum_bytes = [0_u8; size_of::<u32>()];
        checksum_bytes.copy_from_slice(&header[DATABASE_CONTROL_SLOT_FRAME_CHECKSUM_OFFSET..]);
        if u32::from_be_bytes(checksum_bytes) != crc32c(&bytes) {
            return Err(InternalError::commit_corruption());
        }
        Ok(bytes)
    }

    fn write_control_slot(&self, bytes: &[u8]) -> Result<(), InternalError> {
        if bytes.len() > MAX_COMMIT_BYTES as usize {
            return Err(InternalError::commit_marker_exceeds_max_size());
        }

        let end = COMMIT_CONTROL_SLOT_OFFSET.saturating_add(bytes.len() as u64);
        let required_pages = end.div_ceil(WASM_PAGE_BYTES);
        let current_pages = self.memory.size();
        if required_pages > current_pages && self.memory.grow(required_pages - current_pages) < 0 {
            return Err(InternalError::commit_control_memory_growth_failed());
        }

        self.memory.write(COMMIT_CONTROL_SLOT_OFFSET, bytes);
        let mut header = [0_u8; DATABASE_CONTROL_SLOT_FRAME_HEADER_BYTES];
        header[..DATABASE_CONTROL_SLOT_FRAME_MAGIC.len()]
            .copy_from_slice(DATABASE_CONTROL_SLOT_FRAME_MAGIC);
        header[DATABASE_CONTROL_SLOT_FRAME_MAGIC.len()] = DATABASE_CONTROL_SLOT_FRAME_VERSION;
        let encoded_len = u32::try_from(bytes.len())
            .map_err(|_| InternalError::commit_control_slot_exceeds_max_size())?;
        header[DATABASE_CONTROL_SLOT_FRAME_LENGTH_OFFSET
            ..DATABASE_CONTROL_SLOT_FRAME_CHECKSUM_OFFSET]
            .copy_from_slice(&encoded_len.to_be_bytes());
        header[DATABASE_CONTROL_SLOT_FRAME_CHECKSUM_OFFSET..]
            .copy_from_slice(&crc32c(bytes).to_be_bytes());
        self.memory
            .write(DATABASE_CONTROL_SLOT_FRAME_OFFSET, &header);
        Ok(())
    }

    #[cfg(test)]
    fn raw_control_slot_bytes_for_tests(&self) -> Vec<u8> {
        self.read_framed_control_slot()
            .expect("test database control frame should decode")
    }
}

struct CommitStoreEntry {
    allocation: CommitMemoryAllocation,
    store: CommitStore,
}

thread_local! {
    static COMMIT_STORES: RefCell<Vec<CommitStoreEntry>> = const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(super) fn commit_marker_present() -> Result<bool, InternalError> {
    with_commit_store(|store| Ok(store.load()?.is_some()))
}

/// Return exact current marker-control and embedded journal-batch bytes for tests.
#[cfg(test)]
#[cfg(feature = "sql")]
pub(in crate::db) fn persisted_commit_marker_lengths_for_tests()
-> Result<(usize, usize), InternalError> {
    with_commit_store(|store| {
        let control_slot_bytes = store.raw_control_slot_bytes_for_tests().len();
        let marker = store.load()?.ok_or_else(InternalError::store_invariant)?;
        let journal_batch_bytes = marker
            .journal_batches()
            .iter()
            .fold(0usize, |bytes, batch| {
                bytes.saturating_add(crate::db::journal::journal_batch_encoded_len(batch))
            });

        Ok((control_slot_bytes, journal_batch_bytes))
    })
}

/// Lazily initialize and access the commit marker store.
pub(super) fn with_commit_store<R>(
    f: impl FnOnce(&CommitStore) -> Result<R, InternalError>,
) -> Result<R, InternalError> {
    let allocation = current_commit_memory_allocation()?;

    COMMIT_STORES.with(|cell| {
        let mut stores = cell.borrow_mut();
        if let Some(index) = stores
            .iter()
            .position(|entry| entry.allocation == allocation)
        {
            return f(&stores[index].store);
        }

        let store = CommitStore::open(commit_memory_handle(allocation)?)?;
        stores.push(CommitStoreEntry { allocation, store });
        let index = stores.len().saturating_sub(1);
        f(&stores[index].store)
    })
}

/// Load the current durable database-lifecycle identity.
pub(in crate::db) fn database_incarnation_id() -> Result<DatabaseIncarnationId, InternalError> {
    with_commit_store(CommitStore::database_incarnation_id)
}

/// Capture the exact current database-control proof identity.
pub(in crate::db) fn database_control_proof_identity() -> Result<[u8; 32], InternalError> {
    with_commit_store(CommitStore::proof_identity)
}

/// Fast, observational check for marker presence without decoding.
pub(super) fn commit_marker_present_fast() -> Result<bool, InternalError> {
    with_commit_store(|store| Ok(!store.marker_is_empty()?))
}

/// Return whether a process-local commit-window event requires a stable marker check.
#[cfg(not(test))]
pub(super) fn commit_marker_may_be_present() -> bool {
    let Ok(allocation) = current_commit_memory_allocation() else {
        return true;
    };
    let Ok(hints) = commit_marker_presence_hints().lock() else {
        return true;
    };

    hints
        .iter()
        .find(|hint| hint.allocation == allocation)
        .is_none_or(|hint| hint.may_be_present)
}

/// Return whether a process-local commit-window event requires a stable marker check.
#[cfg(test)]
pub(super) const fn commit_marker_may_be_present() -> bool {
    // Core unit tests intentionally exercise many synthetic commit/recovery
    // states in parallel against the same process-local marker machinery.
    // Keeping tests stable-marker authoritative avoids cross-test races in the
    // process-local optimization hint while production builds retain the fast path.
    true
}

/// Mark the process-local marker hint clean after a verified empty-marker observation.
#[cfg(not(test))]
pub(super) fn mark_commit_marker_verified_absent() {
    set_commit_marker_presence_hint(false);
}

/// Mark the process-local marker hint clean after a verified empty-marker observation.
#[cfg(test)]
pub(super) const fn mark_commit_marker_verified_absent() {}

// Mark the process-local marker hint dirty after this process persists marker bytes.
#[cfg(not(test))]
fn mark_commit_marker_may_be_present() {
    set_commit_marker_presence_hint(true);
}

// Mark the process-local marker hint dirty after this process persists marker bytes.
#[cfg(test)]
const fn mark_commit_marker_may_be_present() {}

#[cfg(not(test))]
fn commit_marker_presence_hints() -> &'static Mutex<Vec<CommitMarkerPresenceHint>> {
    COMMIT_MARKER_PRESENCE_HINTS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(not(test))]
fn set_commit_marker_presence_hint(may_be_present: bool) {
    let Ok(allocation) = current_commit_memory_allocation() else {
        return;
    };
    let Ok(mut hints) = commit_marker_presence_hints().lock() else {
        return;
    };

    if let Some(hint) = hints.iter_mut().find(|hint| hint.allocation == allocation) {
        hint.may_be_present = may_be_present;
        return;
    }

    hints.push(CommitMarkerPresenceHint {
        allocation,
        may_be_present,
    });
}

/// Access the commit store without fallible initialization.
///
/// Invariant: caller must ensure `with_commit_store(...)` was called first
/// on the current thread.
pub(super) fn with_commit_store_infallible<R>(f: impl FnOnce(&CommitStore) -> R) -> R {
    let allocation =
        current_commit_memory_allocation().expect("commit memory allocation not configured");

    COMMIT_STORES.with(|cell| {
        let stores = cell.borrow();
        let store = stores
            .iter()
            .find(|entry| entry.allocation == allocation)
            .map(|entry| &entry.store)
            .expect("commit store not initialized");
        f(store)
    })
}
