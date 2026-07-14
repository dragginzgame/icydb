//! Module: db::database_format
//! Responsibility: admit the database durable format before recovery decoding.
//! Does not own: commit-marker, schema, row, or journal payload decoding.
//! Boundary: database control memory + registered durable allocations -> recovery gate.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        commit::{CommitMemoryAllocation, commit_memory_handle, current_commit_memory_allocation},
        registry::{StoreAllocationIdentities, StoreAllocationIdentity},
    },
    error::{InternalError, RecoveryFormatMarkerError},
};
#[cfg(not(test))]
use ic_memory::open_default_memory_manager_memory;
use ic_stable_structures::memory_manager::VirtualMemory;
use ic_stable_structures::{DefaultMemoryImpl, Memory};
#[cfg(test)]
use std::cell::RefCell;
#[cfg(not(test))]
use std::sync::{Mutex, OnceLock};

pub(in crate::db) const DATABASE_BOOT_RECORD_BYTES: usize = 15;
const DATABASE_BOOT_MAGIC: &[u8; 8] = b"ICYDBNOW";
const LEGACY_STABLE_CELL_MAGIC: &[u8; 3] = b"SCL";
const DATABASE_BOOT_CHECKSUM_OFFSET: usize = 11;
const DATABASE_BOOT_INITIALIZED_STATE: u8 = 0x01;
const DATABASE_FORMAT_VERSION_CURRENT: DatabaseFormatVersion = DatabaseFormatVersion(1);
const CRC32C_REVERSED_POLYNOMIAL: u32 = 0x82f6_3b78;
const WASM_PAGE_BYTES: u64 = 65_536;
const VIRGIN_SCAN_CHUNK_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DatabaseFormatVersion(u16);

impl DatabaseFormatVersion {
    const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DatabaseBootState {
    Initialized,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DatabaseBootRecord {
    format_version: DatabaseFormatVersion,
    state: DatabaseBootState,
}

impl DatabaseBootRecord {
    const fn current() -> Self {
        Self {
            format_version: DATABASE_FORMAT_VERSION_CURRENT,
            state: DatabaseBootState::Initialized,
        }
    }

    fn encode(self) -> [u8; DATABASE_BOOT_RECORD_BYTES] {
        let mut bytes = [0_u8; DATABASE_BOOT_RECORD_BYTES];
        bytes[..DATABASE_BOOT_MAGIC.len()].copy_from_slice(DATABASE_BOOT_MAGIC);
        bytes[8..10].copy_from_slice(&self.format_version.get().to_be_bytes());
        bytes[10] = match self.state {
            DatabaseBootState::Initialized => DATABASE_BOOT_INITIALIZED_STATE,
        };
        let checksum = crc32c(&bytes[..DATABASE_BOOT_CHECKSUM_OFFSET]);
        bytes[DATABASE_BOOT_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8; DATABASE_BOOT_RECORD_BYTES]) -> Result<Self, RecoveryFormatMarkerError> {
        if &bytes[..DATABASE_BOOT_MAGIC.len()] != DATABASE_BOOT_MAGIC {
            return Err(RecoveryFormatMarkerError::Magic);
        }

        let mut checksum_bytes = [0_u8; size_of::<u32>()];
        checksum_bytes.copy_from_slice(&bytes[DATABASE_BOOT_CHECKSUM_OFFSET..]);
        let stored_checksum = u32::from_be_bytes(checksum_bytes);
        let expected_checksum = crc32c(&bytes[..DATABASE_BOOT_CHECKSUM_OFFSET]);
        if stored_checksum != expected_checksum {
            return Err(RecoveryFormatMarkerError::Checksum);
        }

        let state = match bytes[10] {
            DATABASE_BOOT_INITIALIZED_STATE => DatabaseBootState::Initialized,
            _ => return Err(RecoveryFormatMarkerError::State),
        };
        let mut version_bytes = [0_u8; size_of::<u16>()];
        version_bytes.copy_from_slice(&bytes[8..10]);

        Ok(Self {
            format_version: DatabaseFormatVersion(u16::from_be_bytes(version_bytes)),
            state,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DatabaseFormatAdmissionError {
    UnsupportedFormatVersion {
        found: Option<DatabaseFormatVersion>,
        required: DatabaseFormatVersion,
    },
    MalformedFormatMarker {
        reason: RecoveryFormatMarkerError,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DatabaseFormatGateError {
    Admission(DatabaseFormatAdmissionError),
    ControlMemoryGrowthFailed,
}

impl From<DatabaseFormatAdmissionError> for DatabaseFormatGateError {
    fn from(error: DatabaseFormatAdmissionError) -> Self {
        Self::Admission(error)
    }
}

enum BootInspection {
    Missing,
    Present(DatabaseBootRecord),
}

struct StoreRoleMemories<M> {
    data: M,
    index: M,
    schema: M,
    journal: M,
}

impl StoreRoleMemories<VirtualMemory<DefaultMemoryImpl>> {
    fn open(allocations: StoreAllocationIdentities) -> Result<Self, InternalError> {
        Ok(Self {
            data: store_memory_handle(required_allocation(allocations.data())?)?,
            index: store_memory_handle(required_allocation(allocations.index())?)?,
            schema: store_memory_handle(required_allocation(allocations.schema())?)?,
            journal: store_memory_handle(required_allocation(allocations.journal())?)?,
        })
    }
}

impl<M: Memory> StoreRoleMemories<M> {
    fn physically_virgin(&self) -> bool {
        self.data.size() == 0
            && self.index.size() == 0
            && self.schema.size() == 0
            && self.journal.size() == 0
    }
}

/// Admit the database control format before any recovery-owned decoder runs.
pub(in crate::db) fn ensure_database_format_admitted<C: crate::traits::CanisterKind>(
    db: &crate::db::Db<C>,
) -> Result<(), InternalError> {
    let allocation = current_commit_memory_allocation()?;
    if database_format_already_admitted(allocation)? {
        return Ok(());
    }

    let control_memory = commit_memory_handle(allocation)?;
    let store_roles = open_registered_store_roles(db)?;
    admit_or_initialize_database_format(&control_memory, &store_roles).map_err(map_gate_error)?;
    mark_database_format_admitted(allocation)
}

fn admit_or_initialize_database_format<M: Memory>(
    control_memory: &M,
    store_roles: &[StoreRoleMemories<M>],
) -> Result<(), DatabaseFormatGateError> {
    match inspect_boot_record(control_memory)? {
        BootInspection::Present(record)
            if record.format_version == DATABASE_FORMAT_VERSION_CURRENT =>
        {
            Ok(())
        }
        BootInspection::Present(record) => {
            Err(DatabaseFormatAdmissionError::UnsupportedFormatVersion {
                found: Some(record.format_version),
                required: DATABASE_FORMAT_VERSION_CURRENT,
            }
            .into())
        }
        BootInspection::Missing
            if control_memory_is_virgin(control_memory)
                && store_roles.iter().all(StoreRoleMemories::physically_virgin) =>
        {
            write_current_boot_record(control_memory)
        }
        BootInspection::Missing => Err(DatabaseFormatAdmissionError::UnsupportedFormatVersion {
            found: None,
            required: DATABASE_FORMAT_VERSION_CURRENT,
        }
        .into()),
    }
}

#[cfg(test)]
pub(in crate::db) fn clear_database_format_admission_for_tests() {
    if let Ok(allocation) = current_commit_memory_allocation() {
        TEST_ADMITTED_DATABASE_FORMATS.with(|allocations| {
            allocations
                .borrow_mut()
                .retain(|existing| *existing != allocation);
        });
    }
}

/// Validate the current boot prefix without decoding the commit slot.
pub(in crate::db) fn validate_current_boot_record<M: Memory>(
    memory: &M,
) -> Result<(), InternalError> {
    match inspect_boot_record(memory).map_err(map_admission_error)? {
        BootInspection::Present(record)
            if record.format_version == DATABASE_FORMAT_VERSION_CURRENT =>
        {
            Ok(())
        }
        BootInspection::Present(record) => Err(map_admission_error(
            DatabaseFormatAdmissionError::UnsupportedFormatVersion {
                found: Some(record.format_version),
                required: DATABASE_FORMAT_VERSION_CURRENT,
            },
        )),
        BootInspection::Missing => Err(map_admission_error(
            DatabaseFormatAdmissionError::UnsupportedFormatVersion {
                found: None,
                required: DATABASE_FORMAT_VERSION_CURRENT,
            },
        )),
    }
}

#[cfg(test)]
pub(in crate::db) fn initialize_current_database_control_for_tests<M: Memory>(memory: &M) {
    write_current_boot_record(memory).expect("test database control memory should grow");
}

fn open_registered_store_roles<C: crate::traits::CanisterKind>(
    db: &crate::db::Db<C>,
) -> Result<Vec<StoreRoleMemories<VirtualMemory<DefaultMemoryImpl>>>, InternalError> {
    db.with_store_registry(|registry| {
        registry
            .iter()
            .filter(|(_, handle)| {
                handle.storage_capabilities().storage_mode()
                    == crate::db::registry::StoreRuntimeStorageMode::Journaled
            })
            .map(|(_, handle)| StoreRoleMemories::open(handle.allocation_identities()))
            .collect()
    })
}

fn inspect_boot_record<M: Memory>(
    control_memory: &M,
) -> Result<BootInspection, DatabaseFormatAdmissionError> {
    if control_memory.size() == 0 {
        return Ok(BootInspection::Missing);
    }

    let mut bytes = [0_u8; DATABASE_BOOT_RECORD_BYTES];
    control_memory.read(0, &mut bytes);
    if bytes.iter().all(|byte| *byte == 0)
        || &bytes[..LEGACY_STABLE_CELL_MAGIC.len()] == LEGACY_STABLE_CELL_MAGIC
    {
        return Ok(BootInspection::Missing);
    }

    DatabaseBootRecord::decode(&bytes)
        .map(BootInspection::Present)
        .map_err(|reason| DatabaseFormatAdmissionError::MalformedFormatMarker { reason })
}

fn write_current_boot_record<M: Memory>(control_memory: &M) -> Result<(), DatabaseFormatGateError> {
    if control_memory.size() == 0 && control_memory.grow(1) < 0 {
        return Err(DatabaseFormatGateError::ControlMemoryGrowthFailed);
    }

    control_memory.write(0, &DatabaseBootRecord::current().encode());
    Ok(())
}

fn control_memory_is_virgin<M: Memory>(memory: &M) -> bool {
    match memory.size() {
        0 => true,
        1 => memory_page_is_zero(memory),
        _ => false,
    }
}

fn memory_page_is_zero<M: Memory>(memory: &M) -> bool {
    let mut chunk = [0_u8; VIRGIN_SCAN_CHUNK_BYTES];
    let mut offset = 0_u64;
    while offset < WASM_PAGE_BYTES {
        memory.read(offset, &mut chunk);
        if chunk.iter().any(|byte| *byte != 0) {
            return false;
        }
        offset += VIRGIN_SCAN_CHUNK_BYTES as u64;
    }
    true
}

pub(in crate::db) fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (CRC32C_REVERSED_POLYNOMIAL & mask);
        }
    }
    !crc
}

fn required_allocation(
    allocation: Option<StoreAllocationIdentity>,
) -> Result<StoreAllocationIdentity, InternalError> {
    allocation.ok_or_else(InternalError::store_invariant)
}

fn map_admission_error(error: DatabaseFormatAdmissionError) -> InternalError {
    match error {
        DatabaseFormatAdmissionError::UnsupportedFormatVersion { found, required } => {
            InternalError::recovery_unsupported_database_format(
                found.map(DatabaseFormatVersion::get),
                required.get(),
            )
        }
        DatabaseFormatAdmissionError::MalformedFormatMarker { reason } => {
            InternalError::recovery_malformed_database_format_marker(reason)
        }
    }
}

fn map_gate_error(error: DatabaseFormatGateError) -> InternalError {
    match error {
        DatabaseFormatGateError::Admission(error) => map_admission_error(error),
        DatabaseFormatGateError::ControlMemoryGrowthFailed => {
            InternalError::recovery_database_format_control_unavailable()
        }
    }
}

#[cfg(test)]
thread_local! {
    static TEST_STORE_ROLE_MEMORIES: RefCell<
        Vec<(StoreAllocationIdentity, VirtualMemory<DefaultMemoryImpl>)>
    > = const { RefCell::new(Vec::new()) };
    static TEST_ADMITTED_DATABASE_FORMATS: RefCell<Vec<CommitMemoryAllocation>> =
        const { RefCell::new(Vec::new()) };
}

#[cfg(not(test))]
static ADMITTED_DATABASE_FORMATS: OnceLock<Mutex<Vec<CommitMemoryAllocation>>> = OnceLock::new();

#[cfg(test)]
#[expect(
    clippy::unnecessary_wraps,
    reason = "test cache keeps the fallible production cache contract"
)]
fn database_format_already_admitted(
    allocation: CommitMemoryAllocation,
) -> Result<bool, InternalError> {
    Ok(TEST_ADMITTED_DATABASE_FORMATS
        .with(|allocations| allocations.borrow().contains(&allocation)))
}

#[cfg(not(test))]
fn database_format_already_admitted(
    allocation: CommitMemoryAllocation,
) -> Result<bool, InternalError> {
    admitted_database_formats()
        .lock()
        .map(|allocations| allocations.contains(&allocation))
        .map_err(|_| InternalError::store_invariant())
}

#[cfg(test)]
#[expect(
    clippy::unnecessary_wraps,
    reason = "test cache keeps the fallible production cache contract"
)]
fn mark_database_format_admitted(allocation: CommitMemoryAllocation) -> Result<(), InternalError> {
    TEST_ADMITTED_DATABASE_FORMATS.with(|allocations| {
        let mut allocations = allocations.borrow_mut();
        if !allocations.contains(&allocation) {
            allocations.push(allocation);
        }
    });
    Ok(())
}

#[cfg(not(test))]
fn mark_database_format_admitted(allocation: CommitMemoryAllocation) -> Result<(), InternalError> {
    let mut allocations = admitted_database_formats()
        .lock()
        .map_err(|_| InternalError::store_invariant())?;
    if !allocations.contains(&allocation) {
        allocations.push(allocation);
    }
    drop(allocations);
    Ok(())
}

#[cfg(not(test))]
fn admitted_database_formats() -> &'static Mutex<Vec<CommitMemoryAllocation>> {
    ADMITTED_DATABASE_FORMATS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn store_memory_handle(
    allocation: StoreAllocationIdentity,
) -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    TEST_STORE_ROLE_MEMORIES.with(|memories| {
        let mut memories = memories.borrow_mut();
        if let Some((_, memory)) = memories
            .iter()
            .find(|(existing, _)| *existing == allocation)
        {
            return Ok(memory.clone());
        }

        let memory = crate::testing::test_memory(allocation.memory_id());
        memories.push((allocation, memory.clone()));
        Ok(memory)
    })
}

#[cfg(not(test))]
fn store_memory_handle(
    allocation: StoreAllocationIdentity,
) -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    open_default_memory_manager_memory(allocation.stable_key(), allocation.memory_id())
        .map_err(InternalError::database_format_memory_registration_failed)
}
