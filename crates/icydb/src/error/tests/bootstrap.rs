//! Exact public classifications for current Storage causes.
use crate::{
    Error, ErrorOrigin,
    db::{DatabaseBootstrapError, MemoryBootstrapAdmissionError},
};
use ic_storage::{
    AdmissionError, DirectoryError, RuntimeBootstrapError, RuntimeConstructionError,
    RuntimeOpenError, StableKey,
};
use icydb_diagnostic_code::{DiagnosticFactTag, ErrorCode};

fn public(cause: RuntimeBootstrapError<MemoryBootstrapAdmissionError>) -> Error {
    Error::from(DatabaseBootstrapError::from(cause))
}

#[test]
fn admission_roles_and_namespace_failures_preserve_identity() {
    for store in [None, Some("main".to_string())] {
        let count = if store.is_some() { 4 } else { 3 };
        let error = public(RuntimeBootstrapError::Policy(
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
    for (cause, code) in [
        (
            RuntimeBootstrapError::Admission(AdmissionError::Unauthorized),
            ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_INVALID,
        ),
        (
            RuntimeBootstrapError::Policy(MemoryBootstrapAdmissionError::HistoricalJournal(
                AdmissionError::Unauthorized,
            )),
            ErrorCode::RUNTIME_BOUNDARY_MEMORY_HISTORICAL_JOURNAL_UNAVAILABLE,
        ),
        (
            RuntimeBootstrapError::Policy(MemoryBootstrapAdmissionError::NamespaceRemoved(
                "main".into(),
            )),
            ErrorCode::RUNTIME_BOUNDARY_MEMORY_NAMESPACE_REMOVED,
        ),
        (
            RuntimeBootstrapError::BindingMismatch,
            ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_SNAPSHOT_MISMATCH,
        ),
    ] {
        let error = public(cause);
        assert_eq!(error.code(), code);
        assert_eq!(error.origin(), ErrorOrigin::Runtime);
        assert!(error.facts().is_empty());
    }
}

#[test]
fn directory_and_adoption_failures_have_stable_public_classifications() {
    let corruption = Error::from(DatabaseBootstrapError::from(
        RuntimeConstructionError::Directory(DirectoryError::Invalid("fixture")),
    ));
    assert_eq!(corruption.code(), ErrorCode::RUNTIME_CORRUPTION);
    let unsupported = Error::from(DatabaseBootstrapError::from(
        RuntimeConstructionError::Directory(DirectoryError::Budget),
    ));
    assert_eq!(unsupported.code(), ErrorCode::RUNTIME_UNSUPPORTED);
    let error = Error::from(DatabaseBootstrapError::from(RuntimeOpenError::NotAdmitted(
        StableKey::parse("icydb.main.commit.control.v1").unwrap(),
    )));
    assert_eq!(
        error.code(),
        ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_SNAPSHOT_MISMATCH
    );
}
