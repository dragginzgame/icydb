//! Module: db::commit::store
//! Responsibility: persist, load, and clear commit markers in stable memory.
//! Does not own: marker shape semantics, recovery orchestration, or commit-window policy.
//! Boundary: commit::{guard,recovery} -> commit::store (one-way).

mod bytes;
mod control_slot;
mod marker_envelope;
#[cfg(test)]
mod tests;

pub(super) use control_slot::EncodedCommitControlSlot;
pub(in crate::db) use control_slot::{
    MAX_PERSISTED_STORE_ALLOCATIONS, PersistedStoreAllocation, PersistedStoreAllocationState,
    canonicalize_store_registry,
};

use crate::{
    db::{
        commit::{
            marker::{CommitMarker, MAX_COMMIT_BYTES, validate_commit_marker_shape},
            memory::{
                CommitMemoryAllocation, commit_memory_handle, current_commit_memory_allocation,
            },
            store::{
                control_slot::{
                    MAX_CURRENT_COMMIT_CONTROL_HEADER_BYTES,
                    PREDECESSOR_COMMIT_CONTROL_HEADER_BYTES, commit_control_slot_encoded_len,
                    decode_commit_control_slot, encode_commit_control_slot_from_marker,
                    encode_empty_commit_control_slot, inspect_commit_control_header,
                    inspect_commit_control_slot,
                },
                marker_envelope::decode_commit_marker,
            },
        },
        database_format::{DATABASE_BOOT_RECORD_BYTES, validate_current_boot_record},
        integrity::DatabaseIncarnationId,
    },
    error::InternalError,
};
use ic_stable_structures::{DefaultMemoryImpl, Memory, memory_manager::VirtualMemory};
use sha2::{Digest, Sha256};
use std::cell::RefCell;

#[cfg(test)]
use crate::db::commit::store::control_slot::encode_commit_control_slot;
use crate::db::database_format::crc32c;
#[cfg(test)]
use crate::db::database_format::initialize_current_database_control_for_tests;

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CommitMarkerPresenceHint {
    allocation: CommitMemoryAllocation,
    may_be_present: bool,
}

#[cfg(not(test))]
thread_local! {
    // Stable-memory stores are thread-local, so the observational marker hint
    // must never suppress inspection for another runtime's memory.
    static COMMIT_MARKER_PRESENCE_HINTS: RefCell<Vec<CommitMarkerPresenceHint>> =
        const { RefCell::new(Vec::new()) };
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

/// Persist one valid database-control slot whose marker payload is supplied
/// directly by a recovery test.
#[cfg(test)]
pub(in crate::db) fn persist_raw_commit_marker_for_tests(
    marker_bytes: Vec<u8>,
) -> Result<(), InternalError> {
    let control_slot = CommitStore::encode_raw_control_slot_for_tests(marker_bytes)?;
    with_commit_store(|store| {
        store.set_raw_marker_bytes_for_tests(control_slot);
        Ok(())
    })
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CommitControlObservation {
    Uninitialized,
    Present {
        incarnation: DatabaseIncarnationId,
        empty_control_proof: Option<[u8; 32]>,
        marker_present: bool,
        current_format: bool,
    },
}

/// Bounded persisted-format observation used only by current-format initialization.
pub(in crate::db) enum PersistedCommitControlObservation {
    Uninitialized,
    Predecessor {
        incarnation: DatabaseIncarnationId,
        cursor_authentication_key: [u8; 32],
        control_proof: [u8; 32],
    },
    Current {
        incarnation: DatabaseIncarnationId,
        cursor_authentication_key: [u8; 32],
        database_commit_sequence: u64,
        registry: Vec<PersistedStoreAllocation>,
        marker_present: bool,
    },
}

/// Fully encoded and capacity-preflighted current database-control replacement.
pub(in crate::db) struct PreparedCommitControlReplacement {
    memory: VirtualMemory<DefaultMemoryImpl>,
    encoded: Vec<u8>,
    encoded_len: u32,
}

impl CommitStore {
    /// Encode one raw commit-control slot payload for recovery tests.
    #[cfg(test)]
    pub(super) fn encode_raw_control_slot_for_tests(
        marker_bytes: Vec<u8>,
    ) -> Result<Vec<u8>, InternalError> {
        encode_commit_control_slot(
            DatabaseIncarnationId::for_tests(0x31),
            [0x42; 32],
            &marker_bytes,
        )
    }

    /// Open the database control store after format admission.
    fn open(memory: VirtualMemory<DefaultMemoryImpl>) -> Result<Self, InternalError> {
        validate_current_boot_record(&memory)?;
        let store = Self { memory };
        if store.control_slot_is_uninitialized() {
            return Err(InternalError::commit_store_uninitialized());
        }
        store.read_control_slot()?;
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

    /// Load the durable scalar-cursor authentication key.
    pub(super) fn cursor_authentication_key(&self) -> Result<[u8; 32], InternalError> {
        self.read_control_slot()
            .and_then(|bytes| Ok(inspect_commit_control_slot(&bytes)?.cursor_authentication_key))
    }

    /// Preview the next database-wide commit order without durable mutation.
    pub(super) fn next_database_commit_sequence(&self) -> Result<u64, InternalError> {
        let bytes = self.read_control_slot()?;
        inspect_commit_control_slot(&bytes)?
            .database_commit_sequence
            .checked_add(1)
            .ok_or_else(InternalError::store_unsupported)
    }

    /// Fingerprint the exact current database-control envelope.
    ///
    /// This is Deep inspection proof state, not schema meaning. Marker writes,
    /// clears, or incarnation replacement necessarily change it.
    pub(super) fn proof_identity(&self) -> Result<[u8; 32], InternalError> {
        self.read_control_slot().map(|bytes| control_proof(&bytes))
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
    pub(super) fn set_if_empty(
        &self,
        marker: &CommitMarker,
    ) -> Result<EncodedCommitControlSlot, InternalError> {
        let bytes = self.read_control_slot()?;
        let slot = inspect_commit_control_slot(&bytes)?;
        if !slot.marker_bytes.is_empty() {
            return Err(InternalError::store_invariant());
        }
        let database_commit_sequence = slot
            .database_commit_sequence
            .checked_add(1)
            .ok_or_else(InternalError::store_unsupported)?;
        for batch in marker.journal_batches() {
            if batch.database_commit_sequence().get() != database_commit_sequence {
                return Err(InternalError::store_invariant());
            }
        }
        let encoded =
            encode_commit_control_slot_from_marker(&slot, database_commit_sequence, marker)?;

        self.write_control_slot(encoded.as_bytes())?;
        mark_commit_marker_may_be_present();
        Ok(encoded)
    }

    /// Clear marker bytes after a verified commit/recovery success.
    pub(super) fn clear_verified(&self) -> Result<(), InternalError> {
        let control_slot = self.read_control_slot()?;
        let slot = inspect_commit_control_slot(&control_slot)?;
        let encoded = encode_empty_commit_control_slot(
            slot.database_incarnation_id,
            slot.cursor_authentication_key,
            slot.database_commit_sequence,
            &slot.registry,
        )?;
        self.write_control_slot(&encoded)?;
        mark_commit_marker_verified_absent();

        Ok(())
    }

    /// Clear the marker slot directly for tests that intentionally persist corruption.
    #[cfg(test)]
    pub(super) fn clear_raw_for_tests(&self) {
        let bytes = self
            .read_control_slot()
            .expect("test control should decode");
        let slot = inspect_commit_control_slot(&bytes).expect("test control should inspect");
        let encoded = encode_empty_commit_control_slot(
            slot.database_incarnation_id,
            slot.cursor_authentication_key,
            slot.database_commit_sequence,
            &slot.registry,
        )
        .expect("test empty control should encode");
        self.write_control_slot(&encoded)
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
            let control = self
                .read_control_slot()
                .expect("test control should decode");
            let slot = inspect_commit_control_slot(&control).expect("test control should inspect");
            encode_empty_commit_control_slot(
                slot.database_incarnation_id,
                slot.cursor_authentication_key,
                slot.database_commit_sequence,
                &slot.registry,
            )
            .expect("test empty control should encode")
        } else {
            bytes
        };
        self.write_control_slot(&encoded)
            .expect("test raw commit marker bytes should fit control memory");
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
        if !(PREDECESSOR_COMMIT_CONTROL_HEADER_BYTES..=MAX_COMMIT_BYTES as usize)
            .contains(&encoded_len)
        {
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
        let encoded_len = u32::try_from(bytes.len())
            .map_err(|_| InternalError::commit_control_slot_exceeds_max_size())?;

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

pub(in crate::db) fn inspect_persisted_commit_control(
    memory: VirtualMemory<DefaultMemoryImpl>,
) -> Result<PersistedCommitControlObservation, InternalError> {
    validate_current_boot_record(&memory)?;
    let store = CommitStore { memory };
    if store.control_slot_is_uninitialized() {
        return Ok(PersistedCommitControlObservation::Uninitialized);
    }
    let bytes = store.read_framed_control_slot()?;
    let header = inspect_commit_control_header(&bytes)?;
    if header.current_format {
        let current = inspect_commit_control_slot(&bytes)?;
        return Ok(PersistedCommitControlObservation::Current {
            incarnation: current.database_incarnation_id,
            cursor_authentication_key: current.cursor_authentication_key,
            database_commit_sequence: current.database_commit_sequence,
            registry: current.registry,
            marker_present: !current.marker_bytes.is_empty(),
        });
    }
    if header.marker_len != 0 {
        return Err(InternalError::store_unsupported());
    }
    let (incarnation, cursor_authentication_key) =
        control_slot::predecessor_empty_control_identity(&bytes)?;
    Ok(PersistedCommitControlObservation::Predecessor {
        incarnation,
        cursor_authentication_key,
        control_proof: control_proof(&bytes),
    })
}

fn control_proof(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"icydb.database-control-proof.v1");
    hasher.update(bytes);
    hasher.finalize().into()
}

pub(in crate::db) fn prepare_commit_control_replacement(
    memory: VirtualMemory<DefaultMemoryImpl>,
    incarnation: DatabaseIncarnationId,
    cursor_authentication_key: [u8; 32],
    database_commit_sequence: u64,
    registry: &[PersistedStoreAllocation],
) -> Result<PreparedCommitControlReplacement, InternalError> {
    let encoded = encode_empty_commit_control_slot(
        incarnation,
        cursor_authentication_key,
        database_commit_sequence,
        registry,
    )?;
    let encoded_len = u32::try_from(encoded.len())
        .map_err(|_| InternalError::commit_control_slot_exceeds_max_size())?;
    let end = COMMIT_CONTROL_SLOT_OFFSET
        .checked_add(u64::try_from(encoded.len()).map_err(|_| InternalError::store_unsupported())?)
        .ok_or_else(InternalError::store_unsupported)?;
    let required_pages = end.div_ceil(WASM_PAGE_BYTES);
    let current_pages = memory.size();
    if required_pages > current_pages && memory.grow(required_pages - current_pages) < 0 {
        return Err(InternalError::commit_control_memory_growth_failed());
    }
    Ok(PreparedCommitControlReplacement {
        memory,
        encoded,
        encoded_len,
    })
}

pub(in crate::db) fn apply_prepared_commit_control_replacement(
    replacement: PreparedCommitControlReplacement,
) {
    let checksum = crc32c(&replacement.encoded);
    replacement
        .memory
        .write(COMMIT_CONTROL_SLOT_OFFSET, &replacement.encoded);
    let mut header = [0_u8; DATABASE_CONTROL_SLOT_FRAME_HEADER_BYTES];
    header[..DATABASE_CONTROL_SLOT_FRAME_MAGIC.len()]
        .copy_from_slice(DATABASE_CONTROL_SLOT_FRAME_MAGIC);
    header[DATABASE_CONTROL_SLOT_FRAME_MAGIC.len()] = DATABASE_CONTROL_SLOT_FRAME_VERSION;
    header[DATABASE_CONTROL_SLOT_FRAME_LENGTH_OFFSET..DATABASE_CONTROL_SLOT_FRAME_CHECKSUM_OFFSET]
        .copy_from_slice(&replacement.encoded_len.to_be_bytes());
    header[DATABASE_CONTROL_SLOT_FRAME_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_be_bytes());
    replacement
        .memory
        .write(DATABASE_CONTROL_SLOT_FRAME_OFFSET, &header);
}

#[cfg(test)]
pub(in crate::db) fn initialize_current_commit_control_for_tests(
    memory: VirtualMemory<DefaultMemoryImpl>,
) -> Result<(), InternalError> {
    let store = CommitStore {
        memory: memory.clone(),
    };
    if !store.control_slot_is_uninitialized() {
        store.read_control_slot()?;
        return Ok(());
    }
    let replacement = prepare_commit_control_replacement(
        memory,
        DatabaseIncarnationId::for_tests(0x31),
        [0x42; 32],
        0,
        &[],
    )?;
    apply_prepared_commit_control_replacement(replacement);
    Ok(())
}

#[cfg(test)]
pub(in crate::db) fn initialize_predecessor_commit_control_for_tests(
    memory: VirtualMemory<DefaultMemoryImpl>,
    incarnation: DatabaseIncarnationId,
    cursor_authentication_key: [u8; 32],
) -> Result<(), InternalError> {
    let encoded = control_slot::encode_empty_predecessor_commit_control_slot(
        incarnation,
        cursor_authentication_key,
    )?;
    CommitStore { memory }.write_control_slot(&encoded)
}

pub(in crate::db) fn observe_commit_control() -> Result<CommitControlObservation, InternalError> {
    let allocation = current_commit_memory_allocation()?;
    let memory = commit_memory_handle(allocation)?;
    validate_current_boot_record(&memory)?;
    let mut frame = [0_u8; DATABASE_CONTROL_SLOT_FRAME_HEADER_BYTES];
    memory.read(DATABASE_CONTROL_SLOT_FRAME_OFFSET, &mut frame);
    if frame.iter().all(|byte| *byte == 0) {
        return Ok(CommitControlObservation::Uninitialized);
    }
    if &frame[..DATABASE_CONTROL_SLOT_FRAME_MAGIC.len()] != DATABASE_CONTROL_SLOT_FRAME_MAGIC {
        return Err(InternalError::commit_corruption());
    }
    if frame[DATABASE_CONTROL_SLOT_FRAME_MAGIC.len()] != DATABASE_CONTROL_SLOT_FRAME_VERSION {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }
    let encoded_len = u32::from_be_bytes(
        frame[DATABASE_CONTROL_SLOT_FRAME_LENGTH_OFFSET
            ..DATABASE_CONTROL_SLOT_FRAME_CHECKSUM_OFFSET]
            .try_into()
            .map_err(|_| InternalError::commit_corruption())?,
    ) as usize;
    let encoded_end = COMMIT_CONTROL_SLOT_OFFSET
        .checked_add(encoded_len as u64)
        .ok_or_else(InternalError::commit_corruption)?;
    let memory_bytes = memory
        .size()
        .checked_mul(WASM_PAGE_BYTES)
        .ok_or_else(InternalError::commit_corruption)?;
    if !(PREDECESSOR_COMMIT_CONTROL_HEADER_BYTES..=MAX_COMMIT_BYTES as usize).contains(&encoded_len)
        || encoded_end > memory_bytes
    {
        return Err(InternalError::commit_corruption());
    }
    let inspected_len = encoded_len.min(MAX_CURRENT_COMMIT_CONTROL_HEADER_BYTES);
    let mut control = vec![0_u8; inspected_len];
    memory.read(COMMIT_CONTROL_SLOT_OFFSET, &mut control);
    let header = inspect_commit_control_header(&control)?;
    let observed_len = header
        .header_len
        .checked_add(header.marker_len)
        .ok_or_else(InternalError::commit_corruption)?;
    if encoded_len != observed_len {
        return Err(InternalError::commit_corruption());
    }
    let marker_present = header.marker_len != 0;
    let empty_control_proof = if marker_present {
        None
    } else {
        let stored_checksum = u32::from_be_bytes(
            frame[DATABASE_CONTROL_SLOT_FRAME_CHECKSUM_OFFSET..]
                .try_into()
                .map_err(|_| InternalError::commit_corruption())?,
        );
        if encoded_len != control.len() || stored_checksum != crc32c(&control) {
            return Err(InternalError::commit_corruption());
        }
        let mut hasher = Sha256::new();
        hasher.update(b"icydb.database-control-proof.v1");
        hasher.update(control);
        Some(hasher.finalize().into())
    };
    Ok(CommitControlObservation::Present {
        incarnation: header.database_incarnation_id,
        empty_control_proof,
        marker_present,
        current_format: header.current_format,
    })
}

struct CommitStoreEntry {
    allocation: CommitMemoryAllocation,
    store: CommitStore,
}

thread_local! {
    static COMMIT_STORES: RefCell<Vec<CommitStoreEntry>> = const { RefCell::new(Vec::new()) };
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

/// Load the durable database-lifecycle scalar-cursor authentication key.
pub(in crate::db) fn cursor_authentication_key() -> Result<[u8; 32], InternalError> {
    with_commit_store(CommitStore::cursor_authentication_key)
}

/// Preview the next database-wide commit sequence without consuming it.
pub(in crate::db) fn next_database_commit_sequence() -> Result<u64, InternalError> {
    with_commit_store(CommitStore::next_database_commit_sequence)
}

/// Capture the exact current database-control proof identity.
pub(in crate::db) fn database_control_proof_identity() -> Result<[u8; 32], InternalError> {
    with_commit_store(CommitStore::proof_identity)
}

/// Fast, observational check for marker presence without decoding.
pub(super) fn commit_marker_present_fast() -> Result<bool, InternalError> {
    with_commit_store(|store| Ok(!store.marker_is_empty()?))
}

/// Return whether a runtime-local commit-window event requires a stable marker check.
#[cfg(not(test))]
pub(super) fn commit_marker_may_be_present() -> bool {
    let Ok(allocation) = current_commit_memory_allocation() else {
        return true;
    };

    with_commit_marker_presence_hints(|hints| {
        hints
            .iter()
            .find(|hint| hint.allocation == allocation)
            .is_none_or(|hint| hint.may_be_present)
    })
    .unwrap_or(true)
}

/// Return whether a runtime-local commit-window event requires a stable marker check.
#[cfg(test)]
pub(super) const fn commit_marker_may_be_present() -> bool {
    // Core unit tests intentionally exercise many synthetic commit/recovery
    // states in parallel against the same process-local marker machinery.
    // Keeping tests stable-marker authoritative avoids cross-test races in the
    // process-local optimization hint while production builds retain the fast path.
    true
}

/// Mark the runtime-local marker hint clean after a verified empty-marker observation.
#[cfg(not(test))]
pub(super) fn mark_commit_marker_verified_absent() {
    set_commit_marker_presence_hint(false);
}

/// Mark the runtime-local marker hint clean after a verified empty-marker observation.
#[cfg(test)]
pub(super) const fn mark_commit_marker_verified_absent() {}

// Mark the runtime-local marker hint dirty after this runtime persists marker bytes.
#[cfg(not(test))]
fn mark_commit_marker_may_be_present() {
    set_commit_marker_presence_hint(true);
}

// Mark the runtime-local marker hint dirty after this runtime persists marker bytes.
#[cfg(test)]
const fn mark_commit_marker_may_be_present() {}

#[cfg(not(test))]
fn with_commit_marker_presence_hints<R>(
    f: impl FnOnce(&mut Vec<CommitMarkerPresenceHint>) -> R,
) -> Option<R> {
    COMMIT_MARKER_PRESENCE_HINTS.with(|hints| {
        let mut hints = hints.try_borrow_mut().ok()?;
        Some(f(&mut hints))
    })
}

#[cfg(not(test))]
fn set_commit_marker_presence_hint(may_be_present: bool) {
    let Ok(allocation) = current_commit_memory_allocation() else {
        return;
    };
    let _ = with_commit_marker_presence_hints(|hints| {
        update_commit_marker_presence_hints(hints, allocation, may_be_present);
    });
}

#[cfg(not(test))]
fn update_commit_marker_presence_hints(
    hints: &mut Vec<CommitMarkerPresenceHint>,
    allocation: CommitMemoryAllocation,
    may_be_present: bool,
) {
    if let Some(hint) = hints.iter_mut().find(|hint| hint.allocation == allocation) {
        hint.may_be_present = may_be_present;
        return;
    }

    hints.push(CommitMarkerPresenceHint {
        allocation,
        may_be_present,
    });
}

/// Access an already initialized commit store without reopening stable memory.
pub(super) fn with_initialized_commit_store<R>(
    f: impl FnOnce(&CommitStore) -> R,
) -> Result<R, InternalError> {
    let allocation = current_commit_memory_allocation()?;

    COMMIT_STORES.with(|cell| {
        let stores = cell.borrow();
        let store = stores
            .iter()
            .find(|entry| entry.allocation == allocation)
            .map(|entry| &entry.store)
            .ok_or_else(InternalError::commit_store_uninitialized)?;
        Ok(f(store))
    })
}
