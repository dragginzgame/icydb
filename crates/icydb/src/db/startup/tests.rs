use super::*;

#[test]
fn rejected_image_failure_survives_wire_roundtrip() {
    let cause = ic_storage::RuntimeConstructionError::Directory(
        ic_storage::DirectoryError::Invalid("fixture image"),
    );
    let failure = __startup_bootstrap_failure(DatabaseBootstrapError::from(cause));
    let bytes = icydb_schema::wire::encode_one(&failure).unwrap();
    let decoded: StartupFailure = icydb_schema::wire::decode_one(&bytes).unwrap();
    assert_eq!(decoded, failure);
    assert_eq!(decoded.kind(), StartupFailureKind::DatabaseControl);
    assert_eq!(
        decoded.error().code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_CORRUPTION
    );
    assert_eq!(decoded.error().origin(), crate::ErrorOrigin::Runtime);
    assert!(decoded.error().facts().is_empty());
}

#[test]
fn public_startup_failure_wire_shape_round_trips_exactly() {
    let failure = StartupFailure {
        kind: StartupFailureKind::JournalRecovery,
        diagnostic: Error::from_runtime_boundary(
            icydb_diagnostic_code::RuntimeBoundaryCode::DatabaseStartupRecoveryPending,
            crate::ErrorOrigin::Recovery,
        ),
    };
    let bytes = icydb_schema::wire::encode_one(&failure).expect("startup failure should encode");
    let decoded = icydb_schema::wire::decode_one::<StartupFailure>(bytes.as_slice())
        .expect("startup failure should decode");
    assert_eq!(decoded, failure);
    assert_eq!(decoded.kind(), StartupFailureKind::JournalRecovery);
    assert_eq!(
        decoded.error().code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );
}
