use super::*;
use crate::traits::Path;
use ic_storage::VectorMemory;

struct ReceiptCanister;

impl Path for ReceiptCanister {
    const PATH: &'static str = "startup_receipt_tests::ReceiptCanister";
}

impl CanisterKind for ReceiptCanister {
    fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(236)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.startup_receipt.commit.v1";
    fn startup_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(237)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.startup_receipt.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(238)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.test.startup_receipt.integrity.progress.v1";
}

fn failure(kind: StartupFailureKind) -> StartupFailure {
    StartupFailure::new(
        kind,
        ErrorCode::RUNTIME_CORRUPTION.diagnostic(ErrorOrigin::Recovery),
        Vec::new(),
    )
}

fn incarnation() -> DatabaseIncarnationId {
    DatabaseIncarnationId::for_tests(0x35)
}

#[test]
fn receipt_growth_refusal_preserves_empty_control_and_manager_metadata() {
    use crate::testing::{GrowthLimitedMemory, test_memory_runtime};
    use ic_storage::StorageConfig;

    let backing = VectorMemory::default();
    let maximum_pages = std::rc::Rc::new(std::cell::Cell::new(u64::MAX));
    let runtime = test_memory_runtime(
        GrowthLimitedMemory {
            backing: backing.clone(),
            maximum_pages: std::rc::Rc::clone(&maximum_pages),
        },
        StorageConfig::default(),
    );
    let memory = runtime
        .open_memory(&ic_storage::StableKey::parse("icydb.core_tests.slot_237.v1").unwrap())
        .unwrap();
    maximum_pages.set(ic_storage::Memory::size(&backing));
    let before = backing.borrow().clone();
    let summary = runtime.memory_allocations().unwrap();
    let error = write_cell(&memory, &[]).unwrap_err();
    assert_eq!(error.class(), crate::error::ErrorClass::Internal);
    assert_eq!(error.origin(), crate::error::ErrorOrigin::Recovery);
    assert_eq!(memory.size(), 0);
    assert_eq!(*backing.borrow(), before);
    assert_eq!(runtime.memory_allocations().unwrap(), summary);
    assert!(decode_cell(&memory).unwrap().is_none());
}

fn round_trip(receipt: &StartupFailureReceipt) {
    let encoded = encode_receipt(receipt).expect("valid receipt should encode");
    assert_eq!(encoded[8], 1);
    assert!(encoded.len() <= MAX_STARTUP_FAILURE_RECEIPT_BYTES);
    let memory = VectorMemory::default();
    assert_eq!(memory.grow(1), 0);
    memory.write(0, &encoded);
    assert_eq!(
        decode_cell(&memory).expect("valid receipt should decode"),
        Some(receipt.clone())
    );
}

#[test]
fn every_closed_binding_round_trips_in_the_current_envelope() {
    assert_eq!(MAX_ENCODED_STARTUP_FAILURE_RECEIPT_BYTES, 934);
    let database = StartupFailureReceipt::new(
        failure(StartupFailureKind::DatabaseControl),
        StartupFailureBinding::DatabaseControl {
            commit_memory_id: 1_236,
            commit_stable_key: ReceiptCanister::COMMIT_STABLE_KEY.to_string(),
            control: Some(DatabaseControlBinding::new(incarnation(), [7; 32])),
        },
    )
    .expect("database binding should admit");
    round_trip(&database);

    let journal = StartupFailureReceipt::new(
        failure(StartupFailureKind::JournalRecovery),
        StartupFailureBinding::JournalRecovery {
            incarnation: incarnation(),
            allocation: StoreAllocationIdentityOwned::from_identity(StoreAllocationIdentity::new(
                1_239,
                "icydb.test.rows.journal.v1",
            )),
            proof: JournalTailProofIdentity::from_persisted_parts(1, 0, 0, 1, 2),
        },
    )
    .expect("journal binding should admit");
    round_trip(&journal);

    let schema = StartupFailureReceipt::new(
        failure(StartupFailureKind::SchemaReconciliation),
        StartupFailureBinding::SchemaReconciliation {
            incarnation: incarnation(),
            submission_key: "generated/0123456789abcdef".to_string(),
            accepted_head: AcceptedHeadBinding::Exact {
                revision: 3,
                fingerprint: [9; 32],
            },
        },
    )
    .expect("schema binding should admit");
    round_trip(&schema);
}

#[test]
fn malformed_future_and_max_plus_one_receipts_fail_closed() {
    let memory = VectorMemory::default();
    assert_eq!(memory.grow(1), 0);
    let mut future = [0_u8; MAX_STARTUP_FAILURE_RECEIPT_BYTES];
    future[..8].copy_from_slice(RECEIPT_MAGIC);
    future[8] = RECEIPT_VERSION + 1;
    memory.write(0, &future);
    let future = decode_cell(&memory).expect_err("future version must reject");
    assert_eq!(
        future.class(),
        crate::error::ErrorClass::IncompatiblePersistedFormat
    );

    let mut oversized = [0_u8; MAX_STARTUP_FAILURE_RECEIPT_BYTES];
    oversized[..8].copy_from_slice(RECEIPT_MAGIC);
    oversized[8] = RECEIPT_VERSION;
    let max_plus_one_payload =
        u16::try_from(MAX_STARTUP_FAILURE_RECEIPT_BYTES - RECEIPT_HEADER_BYTES + 1)
            .expect("test maximum should fit the persisted length field");
    oversized[9..11].copy_from_slice(&max_plus_one_payload.to_le_bytes());
    memory.write(0, &oversized);
    assert_eq!(
        decode_cell(&memory)
            .expect_err("maximum plus one envelope must reject")
            .class(),
        crate::error::ErrorClass::Corruption,
    );

    let mut max_plus_one = vec![
        encode_kind(StartupFailureKind::DatabaseControl),
        (ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING.raw() & 0xff) as u8,
        (ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING.raw() >> 8) as u8,
        ErrorOrigin::Recovery.wire_code(),
        u8::try_from(MAX_PUBLIC_DIAGNOSTIC_FACTS + 1)
            .expect("maximum plus one fact count should fit the persisted byte"),
    ];
    max_plus_one.resize(
        max_plus_one.len() + (MAX_PUBLIC_DIAGNOSTIC_FACTS + 1) * 9,
        0,
    );
    assert!(decode_payload(&max_plus_one).is_err());

    let mismatch = StartupFailureReceipt::new(
        failure(StartupFailureKind::JournalRecovery),
        StartupFailureBinding::DatabaseControl {
            commit_memory_id: 1_236,
            commit_stable_key: ReceiptCanister::COMMIT_STABLE_KEY.to_string(),
            control: None,
        },
    );
    assert!(mismatch.is_err());

    let malformed_allocation = StartupFailureReceipt::new(
        failure(StartupFailureKind::DatabaseControl),
        StartupFailureBinding::DatabaseControl {
            commit_memory_id: 1_236,
            commit_stable_key: "icydb.test.not-canonical.v1".to_string(),
            control: None,
        },
    );
    assert!(malformed_allocation.is_err());
}

#[test]
fn publication_is_idempotent_and_clear_restores_all_zero_absence() {
    let _ = clear::<ReceiptCanister>();
    let receipt = StartupFailureReceipt::new(
        failure(StartupFailureKind::DatabaseControl),
        StartupFailureBinding::DatabaseControl {
            commit_memory_id: ReceiptCanister::commit_memory_id().expect("test allocation"),
            commit_stable_key: ReceiptCanister::COMMIT_STABLE_KEY.to_string(),
            control: None,
        },
    )
    .expect("receipt should admit");
    assert!(publish::<ReceiptCanister>(&receipt).expect("first publish should succeed"));
    assert!(!publish::<ReceiptCanister>(&receipt).expect("replay should succeed"));
    assert_eq!(
        load::<ReceiptCanister>().expect("receipt should load"),
        Some(receipt)
    );
    let replacement = StartupFailureReceipt::new(
        failure(StartupFailureKind::DatabaseControl),
        StartupFailureBinding::DatabaseControl {
            commit_memory_id: ReceiptCanister::commit_memory_id().expect("test allocation"),
            commit_stable_key: ReceiptCanister::COMMIT_STABLE_KEY.to_string(),
            control: Some(DatabaseControlBinding::new(incarnation(), [0xa5; 32])),
        },
    )
    .expect("replacement receipt should admit");
    assert!(publish::<ReceiptCanister>(&replacement).expect("changed binding should replace"));
    assert_eq!(
        load::<ReceiptCanister>().expect("replacement should load"),
        Some(replacement),
    );
    assert!(clear::<ReceiptCanister>().expect("receipt should clear"));
    assert_eq!(
        load::<ReceiptCanister>().expect("absence should load"),
        None
    );
}
