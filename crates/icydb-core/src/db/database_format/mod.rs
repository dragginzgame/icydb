//! Module: db::database_format
//! Responsibility: admit the database durable format before recovery decoding.
//! Does not own: commit-marker, schema, row, or journal payload decoding.
//! Boundary: database control memory + registered durable allocations -> recovery gate.

mod convergence;
mod entropy;

pub use entropy::provide_boot_entropy;

pub(in crate::db) use convergence::retired_journal_tail_control;

use crate::{
    db::{
        commit::{CommitMemoryAllocation, commit_memory_handle, current_commit_memory_allocation},
        registry::{StoreAllocationIdentities, StoreAllocationIdentity},
    },
    error::{InternalError, RecoveryFormatMarkerError},
};
use ic_memory::Memory;
#[cfg(test)]
use ic_memory::{DefaultMemoryImpl, RuntimeMemory};
use std::cell::RefCell;

#[cfg(all(test, feature = "sql", not(target_arch = "wasm32")))]
pub(in crate::db) use entropy::set_boot_entropy_for_tests;

pub(in crate::db) const DATABASE_BOOT_RECORD_BYTES: usize = 15;
const DATABASE_BOOT_MAGIC: &[u8; 8] = b"ICYDBCTL";
const DATABASE_BOOT_CHECKSUM_OFFSET: usize = 11;
const DATABASE_BOOT_INITIALIZED_STATE: u8 = 0x01;
// The sole current pre-1.0 database boot format uses version 1.
const DATABASE_FORMAT_VERSION_CURRENT: DatabaseFormatVersion = DatabaseFormatVersion(1);
const CRC32C_REVERSED_POLYNOMIAL: u32 = 0x82f6_3b78;
const CRC32C_TABLE: [u32; 256] = crc32c_table();
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

enum BootInspection {
    Missing,
    Present(DatabaseBootRecord),
}

/// Read-only result for database control-format admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum DatabaseFormatObservation {
    /// No current database incarnation has been initialized yet.
    Uninitialized,
    /// The current database boot record is present.
    Current,
}

struct StoreRoleExtents {
    pages: [u64; 4],
}

impl StoreRoleExtents {
    fn observe(allocations: StoreAllocationIdentities) -> Result<Self, InternalError> {
        Ok(Self {
            pages: [
                allocation_pages(required_allocation(allocations.data())?)?,
                allocation_pages(required_allocation(allocations.index())?)?,
                allocation_pages(required_allocation(allocations.schema())?)?,
                allocation_pages(required_allocation(allocations.journal())?)?,
            ],
        })
    }

    fn physically_virgin(&self) -> bool {
        self.pages.iter().all(|pages| *pages == 0)
    }
}

/// Admit the database control format before any recovery-owned decoder runs.
pub(in crate::db) fn ensure_database_format_admitted<C: crate::traits::CanisterKind>(
    db: &crate::db::Db<C>,
) -> Result<(), InternalError> {
    let allocation = current_commit_memory_allocation()?;
    ADMITTED_DATABASE_FORMATS.with(|admitted| {
        let mut admitted = admitted
            .try_borrow_mut()
            .map_err(|_| InternalError::store_invariant())?;
        if admitted.contains(&allocation) {
            return Ok(());
        }

        let boot_entropy = entropy::require_boot_entropy()?;
        let control_memory = commit_memory_handle(allocation)?;
        let store_roles = observe_registered_store_roles(db)?;
        let fresh = admit_or_initialize_database_format(control_memory.as_ref(), &store_roles)
            .map_err(map_admission_error)?;
        convergence::ensure_current_convergence_format::<C>(
            db,
            &control_memory,
            fresh,
            boot_entropy,
        )?;
        admitted.push(allocation);
        Ok(())
    })
}

// Cursor signing and verification must not consume a persisted key before
// this boot's admission has replaced it with an entropy-backed key.
pub(in crate::db) fn require_cursor_key_admitted() -> Result<(), InternalError> {
    let allocation = current_commit_memory_allocation()?;
    if ADMITTED_DATABASE_FORMATS.with_borrow(|admitted| admitted.contains(&allocation)) {
        Ok(())
    } else {
        Err(InternalError::recovery_pending())
    }
}

/// Model the volatile format-admission reset performed by an upgrade.
#[cfg(test)]
pub(in crate::db) fn forget_database_format_admission_for_tests() {
    ADMITTED_DATABASE_FORMATS.with_borrow_mut(Vec::clear);
}

/// Inspect the boot record and fresh-control admission without mutation.
pub(in crate::db) fn observe_database_format(
    stores: &'static std::thread::LocalKey<crate::db::registry::StoreRegistry>,
) -> Result<DatabaseFormatObservation, InternalError> {
    let allocation = current_commit_memory_allocation()?;
    let control_memory = commit_memory_handle(allocation)?;
    match inspect_boot_record(&control_memory).map_err(map_admission_error)? {
        BootInspection::Present(record)
            if record.format_version == DATABASE_FORMAT_VERSION_CURRENT =>
        {
            Ok(DatabaseFormatObservation::Current)
        }
        BootInspection::Present(record) => Err(map_admission_error(
            DatabaseFormatAdmissionError::UnsupportedFormatVersion {
                found: Some(record.format_version),
                required: DATABASE_FORMAT_VERSION_CURRENT,
            },
        )),
        BootInspection::Missing => {
            let roles = observe_registered_store_roles_from_registry(stores)?;
            if control_memory_is_virgin(&control_memory)
                && roles.iter().all(StoreRoleExtents::physically_virgin)
            {
                Ok(DatabaseFormatObservation::Uninitialized)
            } else {
                Err(map_admission_error(
                    DatabaseFormatAdmissionError::UnsupportedFormatVersion {
                        found: None,
                        required: DATABASE_FORMAT_VERSION_CURRENT,
                    },
                ))
            }
        }
    }
}

fn admit_or_initialize_database_format<M: Memory>(
    control_memory: &M,
    store_roles: &[StoreRoleExtents],
) -> Result<bool, DatabaseFormatAdmissionError> {
    match inspect_boot_record(control_memory)? {
        BootInspection::Present(record)
            if record.format_version == DATABASE_FORMAT_VERSION_CURRENT =>
        {
            Ok(false)
        }
        BootInspection::Present(record) => {
            Err(DatabaseFormatAdmissionError::UnsupportedFormatVersion {
                found: Some(record.format_version),
                required: DATABASE_FORMAT_VERSION_CURRENT,
            })
        }
        BootInspection::Missing
            if control_memory_is_virgin(control_memory)
                && store_roles.iter().all(StoreRoleExtents::physically_virgin) =>
        {
            Ok(true)
        }
        BootInspection::Missing => Err(DatabaseFormatAdmissionError::UnsupportedFormatVersion {
            found: None,
            required: DATABASE_FORMAT_VERSION_CURRENT,
        }),
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
pub(in crate::db) fn initialize_current_database_control_for_tests(
    memory: &std::rc::Rc<RuntimeMemory<DefaultMemoryImpl>>,
) {
    write_current_boot_record(memory).expect("test database control memory should grow");
    crate::db::commit::initialize_current_commit_control_for_tests(memory.clone())
        .expect("test current commit control should initialize");
}

fn observe_registered_store_roles<C: crate::traits::CanisterKind>(
    db: &crate::db::Db<C>,
) -> Result<Vec<StoreRoleExtents>, InternalError> {
    db.with_store_registry(|registry| {
        registry
            .iter()
            .filter(|(_, handle)| {
                handle.storage_capabilities().storage_mode()
                    == crate::db::registry::StoreRuntimeStorageMode::Journaled
            })
            .map(|(_, handle)| StoreRoleExtents::observe(handle.allocation_identities()))
            .collect()
    })
}

fn observe_registered_store_roles_from_registry(
    stores: &'static std::thread::LocalKey<crate::db::registry::StoreRegistry>,
) -> Result<Vec<StoreRoleExtents>, InternalError> {
    stores.with(|registry| {
        registry
            .iter()
            .filter(|(_, handle)| {
                handle.storage_capabilities().storage_mode()
                    == crate::db::registry::StoreRuntimeStorageMode::Journaled
            })
            .map(|(_, handle)| StoreRoleExtents::observe(handle.allocation_identities()))
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
    if bytes.iter().all(|byte| *byte == 0) {
        return Ok(BootInspection::Missing);
    }

    match DatabaseBootRecord::decode(&bytes) {
        Ok(record) => Ok(BootInspection::Present(record)),
        Err(RecoveryFormatMarkerError::Magic) => {
            Err(DatabaseFormatAdmissionError::UnsupportedFormatVersion {
                found: None,
                required: DATABASE_FORMAT_VERSION_CURRENT,
            })
        }
        Err(reason) => Err(DatabaseFormatAdmissionError::MalformedFormatMarker { reason }),
    }
}

#[cfg(test)]
fn write_current_boot_record<M: Memory>(
    control_memory: &RuntimeMemory<M>,
) -> Result<(), InternalError> {
    if control_memory.size() == 0 {
        control_memory
            .grow(1)
            .map_err(|_| InternalError::recovery_database_format_control_unavailable())?;
    }

    control_memory.write(0, &DatabaseBootRecord::current().encode());
    Ok(())
}

fn publish_preflighted_current_boot_record<M: Memory>(control_memory: &M) {
    debug_assert!(control_memory.size() > 0);
    control_memory.write(0, &DatabaseBootRecord::current().encode());
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

// Derive the byte transitions at compile time. The persisted checksum remains
// CRC32C; this changes only the work required to compute it.
const fn crc32c_table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut index = 0_u32;
    while index < 256 {
        let mut crc = index;
        let mut bit = 0;
        while bit < 8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (CRC32C_REVERSED_POLYNOMIAL & mask);
            bit += 1;
        }
        table[index as usize] = crc;
        index += 1;
    }
    table
}

pub(in crate::db) fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc = (crc >> 8) ^ CRC32C_TABLE[usize::from(crc.to_le_bytes()[0] ^ byte)];
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

thread_local! {
    #[cfg(test)]
    static TEST_STORE_ROLE_MEMORIES: RefCell<
        Vec<(StoreAllocationIdentity, crate::testing::TestMemoryOwner)>
    > = const { RefCell::new(Vec::new()) };
    // Generated stable-memory stores are thread-local. Admission authority
    // must have the same lifetime so one runtime cannot inherit readiness for
    // another runtime's empty memory.
    static ADMITTED_DATABASE_FORMATS: RefCell<Vec<CommitMemoryAllocation>> =
        const { RefCell::new(Vec::new()) };
}

// Extent inspection must not acquire a second lease over an initialized tree.
#[cfg(test)]
pub(super) fn allocation_pages(allocation: StoreAllocationIdentity) -> Result<u64, InternalError> {
    TEST_STORE_ROLE_MEMORIES.with_borrow_mut(|memories| {
        if let Some((_, owner)) = memories
            .iter()
            .find(|(existing, _)| *existing == allocation)
        {
            return Ok(owner.size());
        }
        memories.push((
            allocation,
            crate::testing::test_memory_owner(allocation.memory_id()),
        ));
        Ok(0)
    })
}

#[cfg(not(test))]
pub(super) fn allocation_pages(allocation: StoreAllocationIdentity) -> Result<u64, InternalError> {
    let runtime = crate::db::memory_runtime().ok_or_else(InternalError::store_internal)?;
    let id = runtime
        .memory_id(allocation.stable_key())
        .map_err(|_| InternalError::store_internal())?;
    if id != allocation.memory_id() {
        return Err(InternalError::store_invariant());
    }
    runtime
        .memory_allocations()
        .map_err(|_| InternalError::store_internal())?
        .memories
        .get(usize::from(id))
        .map(|memory| memory.virtual_extent.wasm_pages)
        .ok_or_else(InternalError::store_invariant)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{commit::select_commit_memory_allocation, registry::StoreRegistry};

    #[test]
    fn observation_and_admission_agree_on_control_memory_virginity() {
        thread_local! {
            static STORES: StoreRegistry = StoreRegistry::new();
        }
        select_commit_memory_allocation(247, "icydb.test.format-observation.commit.v1");
        let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
        let page_bytes = usize::try_from(WASM_PAGE_BYTES).unwrap();
        assert_eq!(
            observe_database_format(&STORES).unwrap(),
            DatabaseFormatObservation::Uninitialized
        );
        assert_eq!(admit_or_initialize_database_format(&memory, &[]), Ok(true));
        assert_eq!(memory.size(), 0);
        assert_eq!(memory.as_ref().grow(1), Ok(0));
        assert_eq!(
            observe_database_format(&STORES).unwrap(),
            DatabaseFormatObservation::Uninitialized
        );
        assert_eq!(admit_or_initialize_database_format(&memory, &[]), Ok(true));

        for (offset, bytes) in [
            (0, &b"SCL"[..]),
            (DATABASE_BOOT_RECORD_BYTES as u64, &[1][..]),
            (VIRGIN_SCAN_CHUNK_BYTES as u64 - 1, &[1][..]),
            (VIRGIN_SCAN_CHUNK_BYTES as u64, &[1][..]),
            (WASM_PAGE_BYTES - 1, &[1][..]),
        ] {
            memory.write(offset, bytes);
            let mut before = vec![0; page_bytes];
            memory.read(0, &mut before);
            let observed = observe_database_format(&STORES)
                .expect_err("non-virgin control memory must not appear fresh");
            let admitted = admit_or_initialize_database_format(&memory, &[])
                .map_err(map_admission_error)
                .unwrap_err();
            assert_eq!(observed.diagnostic(), admitted.diagnostic());
            assert_eq!(observed.diagnostic_facts(), admitted.diagnostic_facts());
            let mut after = vec![0; page_bytes];
            memory.read(0, &mut after);
            assert_eq!(before, after);
            memory.write(offset, &[0; 3][..bytes.len()]);
        }

        assert_eq!(memory.as_ref().grow(1), Ok(1));
        let error = observe_database_format(&STORES).unwrap_err();
        let admitted = admit_or_initialize_database_format(&memory, &[])
            .map_err(map_admission_error)
            .unwrap_err();
        assert_eq!(error.diagnostic(), admitted.diagnostic());
        memory.write(0, &DatabaseBootRecord::current().encode());
        assert_eq!(
            observe_database_format(&STORES).unwrap(),
            DatabaseFormatObservation::Current
        );
        assert_eq!(admit_or_initialize_database_format(&memory, &[]), Ok(false));
        assert_eq!(memory.size(), 2);
    }

    #[test]
    fn crc32c_matches_known_vectors_and_bit_recurrence() {
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(crc32c(&[0; 32]), 0x8a91_36aa);
        assert_eq!(crc32c(&[255; 32]), 0x62a8_ab43);

        // Independently exercise every byte transition and long/tail lengths.
        let bytes: Vec<u8> = (0_u8..=255)
            .map(|byte| byte.wrapping_mul(73))
            .cycle()
            .take(4097)
            .collect();
        for len in (0..=256).chain([511, 512, 513, 4096, 4097]) {
            let mut remainder = u32::MAX;
            for byte in &bytes[..len] {
                remainder ^= u32::from(*byte);
                for _ in 0..8 {
                    remainder = if remainder & 1 == 0 {
                        remainder >> 1
                    } else {
                        (remainder >> 1) ^ 0x82f6_3b78
                    };
                }
            }
            assert_eq!(crc32c(&bytes[..len]), !remainder, "length {len}");
        }
    }
}
