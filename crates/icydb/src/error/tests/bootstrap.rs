//! Exact public classifications for nested upstream causes and internal controls.

use crate::{
    Error, ErrorOrigin,
    db::{DatabaseBootstrapError, MemoryBootstrapAdmissionError},
};
use ic_memory::{
    AllocationValidationError, BootstrapAdmissionError, MemoryManagerRangeAuthorityError,
    RuntimeAdoptionError, RuntimeBootstrapError, RuntimeOpenError, RuntimePolicyError,
    RuntimeStateError, StableKey, StaticMemoryDeclarationError,
};
use icydb_diagnostic_code::{DiagnosticFactTag, ErrorCode};

fn public(cause: RuntimeBootstrapError<MemoryBootstrapAdmissionError>) -> Error {
    Error::from(DatabaseBootstrapError::from(cause))
}

fn grant() -> MemoryManagerRangeAuthorityError {
    MemoryManagerRangeAuthorityError::UnclaimedId { id: 42 }
}

fn historical() -> BootstrapAdmissionError {
    BootstrapAdmissionError::Range {
        stable_key: StableKey::parse("icydb.main.store.old.journal.v1").unwrap(),
        authority: "icydb.main".into(),
        source: grant(),
    }
}

#[test]
fn memory_bootstrap_grant_wrappers_preserve_bounded_slot_evidence() {
    for cause in [
        RuntimeBootstrapError::Resolution(ic_memory::MemoryResolutionError::Range(grant())),
        RuntimeBootstrapError::Registry(StaticMemoryDeclarationError::Range(grant())),
        RuntimeBootstrapError::Validation(AllocationValidationError::Policy(
            RuntimePolicyError::Range(grant()),
        )),
    ] {
        let error = public(cause);
        assert_eq!(
            error.code(),
            ErrorCode::RUNTIME_BOUNDARY_MEMORY_ALLOCATION_RESOLUTION_FAILED
        );
        assert_eq!(
            error.core_facts().unwrap(),
            [(DiagnosticFactTag::ActualMemoryId, 42)]
        );
        assert_eq!(error.origin(), ErrorOrigin::Runtime);
    }
    for cause in [
        RuntimeBootstrapError::Admission(historical()),
        RuntimeBootstrapError::AdmissionPolicy(MemoryBootstrapAdmissionError::HistoricalJournal(
            historical(),
        )),
    ] {
        let error = public(cause);
        assert_eq!(
            error.code(),
            ErrorCode::RUNTIME_BOUNDARY_MEMORY_HISTORICAL_JOURNAL_UNAVAILABLE
        );
        assert_eq!(
            error.core_facts().unwrap(),
            [(DiagnosticFactTag::ActualMemoryId, 42)]
        );
    }
}

#[test]
fn memory_bootstrap_roles_and_invalid_declarations_keep_their_identity() {
    for store in [None, Some("main".to_string())] {
        let count = if store.is_some() { 4 } else { 3 };
        let error = public(RuntimeBootstrapError::AdmissionPolicy(
            MemoryBootstrapAdmissionError::IncompleteRoles {
                namespace: "main".into(),
                store,
            },
        ));
        assert_eq!(
            error.code(),
            ErrorCode::RUNTIME_BOUNDARY_MEMORY_ALLOCATION_ROLES_INCOMPLETE
        );
        assert_eq!(
            error.core_facts().unwrap(),
            [(DiagnosticFactTag::ExpectedCount, count)]
        );
    }
    for cause in [
        RuntimeBootstrapError::AdmissionPolicy(MemoryBootstrapAdmissionError::UnsupportedKey(
            "icydb.main.invalid.v1".into(),
        )),
        RuntimeBootstrapError::Validation(AllocationValidationError::Policy(
            RuntimePolicyError::Custom(MemoryBootstrapAdmissionError::InvalidDeclaration(
                "icydb.main.commit.control.v1".into(),
            )),
        )),
        RuntimeBootstrapError::Registry(StaticMemoryDeclarationError::DuplicateRequest {
            stable_key: StableKey::parse("icydb.main.commit.control.v1").unwrap(),
        }),
    ] {
        let error = public(cause);
        assert_eq!(
            error.code(),
            ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_INVALID
        );
        assert!(error.facts().is_empty());
    }
}

#[test]
fn memory_adoption_requirement_drift_is_a_public_conflict() {
    for cause in [
        RuntimeAdoptionError::UnknownAuthority {
            authority: "icydb.main".into(),
        },
        RuntimeAdoptionError::AuthorityMismatch {
            stable_key: "icydb.main.commit.control.v1".into(),
            committed_authority: "host.other".into(),
            requested_authority: "icydb.main".into(),
        },
        RuntimeAdoptionError::DeclarationMetadataMismatch {
            stable_key: "icydb.main.commit.control.v1".into(),
        },
        RuntimeAdoptionError::Open(RuntimeOpenError::StableKeyNotCommitted(
            "icydb.main.commit.control.v1".into(),
        )),
    ] {
        let error = Error::from(DatabaseBootstrapError::from(cause));
        assert_eq!(
            error.code(),
            ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_SNAPSHOT_MISMATCH
        );
        assert_eq!(error.class(), icydb_diagnostic_code::ErrorClass::Conflict);
        assert!(error.facts().is_empty());
    }
}

#[test]
fn memory_bootstrap_internal_failures_retain_internal_classification() {
    for cause in [
        RuntimeBootstrapError::State(RuntimeStateError::ReentrantAccess),
        RuntimeBootstrapError::State(RuntimeStateError::InconsistentLifecycle),
        RuntimeBootstrapError::Registry(StaticMemoryDeclarationError::RegistryPoisoned),
        RuntimeBootstrapError::Registry(StaticMemoryDeclarationError::EagerInitPanicked),
    ] {
        let error = public(cause);
        assert_eq!(error.code(), ErrorCode::RUNTIME_INTERNAL);
        assert_eq!(error.class(), icydb_diagnostic_code::ErrorClass::Internal);
        assert_eq!(error.origin(), ErrorOrigin::Runtime);
        assert!(error.facts().is_empty());
    }
    let error = Error::from(DatabaseBootstrapError::from(RuntimeAdoptionError::Open(
        RuntimeOpenError::State(RuntimeStateError::Unavailable),
    )));
    assert_eq!(error.code(), ErrorCode::RUNTIME_INTERNAL);
}
