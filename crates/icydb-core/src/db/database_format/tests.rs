use super::*;
use crate::db::{
    Db, StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
    StoreRuntimeStorageCapabilities,
    commit::{CommitMemoryAllocation, commit_memory_handle},
    data::DataStore,
    index::IndexStore,
    journal::JournalTailStore,
    schema::SchemaStore,
};
use crate::error::{ErrorClass, ErrorDetail, ErrorOrigin, RecoveryErrorDetail};
use crate::testing::test_memory;
use crate::traits::{CanisterKind, Path};
use ic_stable_structures::{Memory, VectorMemory};
use std::cell::RefCell;

crate::test_canister! {
    ident = DatabaseFormatTestCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = DatabaseFormatTestStore,
    canister = DatabaseFormatTestCanister,
}

const FORMAT_DATA_ALLOCATION: StoreAllocationIdentity =
    StoreAllocationIdentity::new(230, "icydb.test.database_format.data.v1");
const FORMAT_INDEX_ALLOCATION: StoreAllocationIdentity =
    StoreAllocationIdentity::new(231, "icydb.test.database_format.index.v1");
const FORMAT_SCHEMA_ALLOCATION: StoreAllocationIdentity =
    StoreAllocationIdentity::new(232, "icydb.test.database_format.schema.v1");
const FORMAT_JOURNAL_ALLOCATION: StoreAllocationIdentity =
    StoreAllocationIdentity::new(233, "icydb.test.database_format.journal.v1");

thread_local! {
    static FORMAT_DATA: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(230)));
    static FORMAT_INDEX: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(231)));
    static FORMAT_SCHEMA: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(232)));
    static FORMAT_JOURNAL: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(233)));
    static FORMAT_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry
            .register_journaled_store(
                DatabaseFormatTestStore::PATH,
                &FORMAT_DATA,
                &FORMAT_INDEX,
                &FORMAT_SCHEMA,
                &FORMAT_JOURNAL,
                StoreAllocationIdentities::new_journaled(
                    FORMAT_DATA_ALLOCATION,
                    FORMAT_INDEX_ALLOCATION,
                    FORMAT_SCHEMA_ALLOCATION,
                    FORMAT_JOURNAL_ALLOCATION,
                ),
                StoreRuntimeStorageCapabilities::journaled(),
            )
            .expect("database-format test registry should be valid");
        registry
    };
}

static FORMAT_DB: Db<DatabaseFormatTestCanister> = Db::new(&FORMAT_REGISTRY);

fn empty_store_roles() -> StoreRoleMemories<VectorMemory> {
    StoreRoleMemories {
        data: VectorMemory::default(),
        index: VectorMemory::default(),
        schema: VectorMemory::default(),
        journal: VectorMemory::default(),
    }
}

fn write_boot_bytes(memory: &impl Memory, bytes: &[u8; DATABASE_BOOT_RECORD_BYTES]) {
    if memory.size() == 0 {
        assert!(memory.grow(1) >= 0);
    }
    memory.write(0, bytes);
}

#[test]
fn current_database_boot_record_wire_vector_is_frozen() {
    assert_eq!(
        DatabaseBootRecord::current().encode(),
        [
            0x49, 0x43, 0x59, 0x44, 0x42, 0x4e, 0x4f, 0x57, 0x00, 0x01, 0x01, 0x19, 0x31, 0x87,
            0x4b,
        ],
    );
    assert_eq!(crc32c(b"123456789"), 0xe306_9283);
}

#[test]
fn virgin_database_writes_current_boot_before_other_allocations() {
    let control = VectorMemory::default();
    let roles = [empty_store_roles()];

    admit_or_initialize_database_format(&control, &roles)
        .expect("virgin database should initialize");

    assert_eq!(control.size(), 1);
    assert!(matches!(
        inspect_boot_record(&control).expect("boot record should decode"),
        BootInspection::Present(DatabaseBootRecord {
            format_version: DATABASE_FORMAT_VERSION_CURRENT,
            state: DatabaseBootState::Initialized,
        })
    ));
    assert!(roles.iter().all(StoreRoleMemories::physically_virgin));
}

#[test]
fn zeroed_control_page_retries_as_virgin_after_interrupted_growth() {
    let control = VectorMemory::default();
    let roles = [empty_store_roles()];
    assert!(control.grow(1) >= 0);

    admit_or_initialize_database_format(&control, &roles)
        .expect("zeroed control page should resume initialization");

    assert!(matches!(
        inspect_boot_record(&control).expect("boot record should decode"),
        BootInspection::Present(_)
    ));
}

#[test]
fn missing_boot_with_nonvirgin_store_role_rejects_as_unsupported() {
    let control = VectorMemory::default();
    let roles = [empty_store_roles()];
    assert!(roles[0].data.grow(1) >= 0);

    assert_eq!(
        admit_or_initialize_database_format(&control, &roles),
        Err(DatabaseFormatGateError::Admission(
            DatabaseFormatAdmissionError::UnsupportedFormatVersion {
                found: None,
                required: DATABASE_FORMAT_VERSION_CURRENT,
            }
        )),
    );
}

#[test]
fn current_boot_admits_nonempty_current_store_roles_without_decoding_them() {
    let control = VectorMemory::default();
    let roles = [empty_store_roles()];
    write_boot_bytes(&control, &DatabaseBootRecord::current().encode());
    assert!(roles[0].schema.grow(1) >= 0);

    admit_or_initialize_database_format(&control, &roles)
        .expect("current boot should admit current store allocations");
}

#[test]
fn future_valid_boot_record_rejects_with_found_version() {
    let control = VectorMemory::default();
    let roles = [empty_store_roles()];
    let record = DatabaseBootRecord {
        format_version: DatabaseFormatVersion(DATABASE_FORMAT_VERSION_CURRENT.get() + 1),
        state: DatabaseBootState::Initialized,
    };
    write_boot_bytes(&control, &record.encode());

    assert_eq!(
        admit_or_initialize_database_format(&control, &roles),
        Err(DatabaseFormatGateError::Admission(
            DatabaseFormatAdmissionError::UnsupportedFormatVersion {
                found: Some(record.format_version),
                required: DATABASE_FORMAT_VERSION_CURRENT,
            }
        )),
    );
}

#[test]
fn malformed_checksum_rejects_as_marker_corruption() {
    let control = VectorMemory::default();
    let roles = [empty_store_roles()];
    let mut bytes = DatabaseBootRecord::current().encode();
    bytes[DATABASE_BOOT_CHECKSUM_OFFSET] ^= 0xff;
    write_boot_bytes(&control, &bytes);

    assert_eq!(
        admit_or_initialize_database_format(&control, &roles),
        Err(DatabaseFormatGateError::Admission(
            DatabaseFormatAdmissionError::MalformedFormatMarker {
                reason: RecoveryFormatMarkerError::Checksum,
            }
        )),
    );
}

#[test]
fn malformed_state_rejects_after_checksum_validation() {
    let control = VectorMemory::default();
    let roles = [empty_store_roles()];
    let mut bytes = DatabaseBootRecord::current().encode();
    bytes[10] = 0xff;
    let checksum = crc32c(&bytes[..DATABASE_BOOT_CHECKSUM_OFFSET]);
    bytes[DATABASE_BOOT_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_be_bytes());
    write_boot_bytes(&control, &bytes);

    assert_eq!(
        admit_or_initialize_database_format(&control, &roles),
        Err(DatabaseFormatGateError::Admission(
            DatabaseFormatAdmissionError::MalformedFormatMarker {
                reason: RecoveryFormatMarkerError::State,
            }
        )),
    );
}

#[test]
fn malformed_magic_rejects_as_marker_corruption() {
    let control = VectorMemory::default();
    let roles = [empty_store_roles()];
    let mut bytes = DatabaseBootRecord::current().encode();
    bytes[0] = b'X';
    write_boot_bytes(&control, &bytes);

    assert_eq!(
        admit_or_initialize_database_format(&control, &roles),
        Err(DatabaseFormatGateError::Admission(
            DatabaseFormatAdmissionError::MalformedFormatMarker {
                reason: RecoveryFormatMarkerError::Magic,
            }
        )),
    );
}

#[test]
fn admission_errors_keep_unsupported_and_malformed_details_distinct() {
    let unsupported = map_admission_error(DatabaseFormatAdmissionError::UnsupportedFormatVersion {
        found: None,
        required: DATABASE_FORMAT_VERSION_CURRENT,
    });
    let malformed = map_admission_error(DatabaseFormatAdmissionError::MalformedFormatMarker {
        reason: RecoveryFormatMarkerError::Magic,
    });

    assert_eq!(unsupported.class(), ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(unsupported.origin(), ErrorOrigin::Recovery);
    assert!(matches!(
        unsupported.detail(),
        Some(ErrorDetail::Recovery(
            RecoveryErrorDetail::UnsupportedFormatVersion {
                found: None,
                required,
            }
        )) if *required == DATABASE_FORMAT_VERSION_CURRENT.get()
    ));
    assert_eq!(malformed.class(), ErrorClass::Corruption);
    assert_eq!(malformed.origin(), ErrorOrigin::Recovery);
    assert!(matches!(
        malformed.detail(),
        Some(ErrorDetail::Recovery(
            RecoveryErrorDetail::MalformedFormatMarker {
                reason: RecoveryFormatMarkerError::Magic,
            }
        ))
    ));
}

#[test]
fn recovery_entry_rejects_future_boot_before_commit_decode() {
    let allocation = CommitMemoryAllocation {
        memory_id: DatabaseFormatTestCanister::COMMIT_MEMORY_ID,
        stable_key: DatabaseFormatTestCanister::COMMIT_STABLE_KEY,
    };
    let control = commit_memory_handle(allocation).expect("test control memory should open");
    let future = DatabaseBootRecord {
        format_version: DatabaseFormatVersion(DATABASE_FORMAT_VERSION_CURRENT.get() + 1),
        state: DatabaseBootState::Initialized,
    };
    write_boot_bytes(&control, &future.encode());

    let error = crate::db::commit::ensure_recovered(&FORMAT_DB)
        .expect_err("recovery must reject future database format first");

    assert_eq!(error.class(), ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(error.origin(), ErrorOrigin::Recovery);
    assert!(matches!(
        error.detail(),
        Some(ErrorDetail::Recovery(
            RecoveryErrorDetail::UnsupportedFormatVersion {
                found: Some(found),
                required,
            }
        )) if *found == future.format_version.get()
            && *required == DATABASE_FORMAT_VERSION_CURRENT.get()
    ));
}
