//! Project typed memory bootstrap causes into bounded public diagnostics.
//! Allocation/admission/recovery ownership remains with their existing owners.

use crate::{
    db::{DatabaseBootstrapError, MemoryBootstrapAdmissionError},
    error::{Error, ErrorKind, ErrorOrigin, RuntimeErrorKind},
};
use ic_memory::{
    AllocationValidationError, BootstrapAdmissionError, MemoryManagerRangeAuthorityError,
    MemoryResolutionError, RuntimeAdoptionError, RuntimeBootstrapError, RuntimeConstructionError,
    RuntimeOpenError, RuntimePolicyError, RuntimeStateError, StaticMemoryDeclarationError,
};
use icydb_diagnostic_code::{DiagnosticFactTag, RuntimeBoundaryCode};

pub(super) fn bootstrap_error(cause: &DatabaseBootstrapError) -> Error {
    let classified = match cause {
        DatabaseBootstrapError::Bootstrap(cause) => cold_bootstrap_error(cause),
        DatabaseBootstrapError::Adoption(cause) => adoption_error(cause),
    };
    // Unknown upstream variants and unrelated internal faults must not acquire a
    // configuration classification merely because they crossed bootstrap.
    classified.unwrap_or_else(|| {
        Error::from_kind(
            ErrorKind::Runtime(RuntimeErrorKind::Internal),
            ErrorOrigin::Runtime,
        )
    })
}

fn cold_bootstrap_error(
    cause: &RuntimeBootstrapError<MemoryBootstrapAdmissionError>,
) -> Option<Error> {
    match cause {
        RuntimeBootstrapError::State(RuntimeStateError::Construction(
            RuntimeConstructionError::BucketSizeMismatch {
                persisted,
                requested,
            },
        )) => Some(boundary(
            RuntimeBoundaryCode::MemoryBucketSizeMismatch,
            vec![
                (DiagnosticFactTag::Expected, u64::from(*requested)),
                (DiagnosticFactTag::Actual, u64::from(*persisted)),
            ],
        )),
        RuntimeBootstrapError::DeclarationSnapshotMismatch => Some(boundary(
            RuntimeBoundaryCode::MemoryDeclarationSnapshotMismatch,
            vec![],
        )),
        RuntimeBootstrapError::AdmissionPolicy(cause)
        | RuntimeBootstrapError::Validation(AllocationValidationError::Policy(
            RuntimePolicyError::Custom(cause),
        )) => admission_error(cause),
        // Poisoned historical selection is returned before the policy wrapper.
        RuntimeBootstrapError::Admission(cause) => historical_error(cause),
        RuntimeBootstrapError::Resolution(MemoryResolutionError::Exhausted { .. }) => {
            Some(boundary(
                RuntimeBoundaryCode::MemoryAllocationResolutionFailed,
                vec![],
            ))
        }
        RuntimeBootstrapError::Resolution(MemoryResolutionError::Range(cause))
        | RuntimeBootstrapError::Validation(AllocationValidationError::Policy(
            RuntimePolicyError::Range(cause),
        )) => Some(boundary(
            RuntimeBoundaryCode::MemoryAllocationResolutionFailed,
            memory_id_facts(cause),
        )),
        RuntimeBootstrapError::Registry(cause)
        | RuntimeBootstrapError::Resolution(MemoryResolutionError::Registry(cause)) => {
            registry_error(cause)
        }
        RuntimeBootstrapError::Validation(AllocationValidationError::Snapshot(_)) => Some(
            boundary(RuntimeBoundaryCode::MemoryDeclarationInvalid, vec![]),
        ),
        _ => None,
    }
}

fn admission_error(cause: &MemoryBootstrapAdmissionError) -> Option<Error> {
    match cause {
        MemoryBootstrapAdmissionError::UnsupportedKey(_)
        | MemoryBootstrapAdmissionError::InvalidDeclaration(_) => Some(boundary(
            RuntimeBoundaryCode::MemoryDeclarationInvalid,
            vec![],
        )),
        MemoryBootstrapAdmissionError::IncompleteRoles { store, .. } => Some(boundary(
            RuntimeBoundaryCode::MemoryAllocationRolesIncomplete,
            vec![(
                DiagnosticFactTag::ExpectedCount,
                if store.is_some() { 4 } else { 3 },
            )],
        )),
        MemoryBootstrapAdmissionError::NamespaceRemoved(_) => Some(boundary(
            RuntimeBoundaryCode::MemoryNamespaceRemoved,
            vec![],
        )),
        MemoryBootstrapAdmissionError::HistoricalJournal(cause) => historical_error(cause),
    }
}

fn historical_error(cause: &BootstrapAdmissionError) -> Option<Error> {
    let facts = match cause {
        BootstrapAdmissionError::Range { source, .. } => memory_id_facts(source),
        BootstrapAdmissionError::Unknown(_)
        | BootstrapAdmissionError::Retired(_)
        | BootstrapAdmissionError::Duplicate(_)
        | BootstrapAdmissionError::TooManyDeclarations => vec![],
        BootstrapAdmissionError::Registry(cause) => return registry_error(cause),
        _ => return None,
    };
    Some(boundary(
        RuntimeBoundaryCode::MemoryHistoricalJournalUnavailable,
        facts,
    ))
}

fn registry_error(cause: &StaticMemoryDeclarationError) -> Option<Error> {
    match cause {
        StaticMemoryDeclarationError::Range(cause) => Some(boundary(
            RuntimeBoundaryCode::MemoryAllocationResolutionFailed,
            memory_id_facts(cause),
        )),
        StaticMemoryDeclarationError::Declaration(_)
        | StaticMemoryDeclarationError::TooManyDeclarations
        | StaticMemoryDeclarationError::DuplicateRequest { .. }
        | StaticMemoryDeclarationError::InvalidAuthority { .. }
        | StaticMemoryDeclarationError::ReservedAuthority { .. }
        | StaticMemoryDeclarationError::ReservedStableKey { .. } => Some(boundary(
            RuntimeBoundaryCode::MemoryDeclarationInvalid,
            vec![],
        )),
        _ => None,
    }
}

fn adoption_error(cause: &RuntimeAdoptionError) -> Option<Error> {
    let facts = match cause {
        RuntimeAdoptionError::UnknownAuthority { .. }
        | RuntimeAdoptionError::AuthorityMismatch { .. }
        | RuntimeAdoptionError::DeclarationMetadataMismatch { .. }
        | RuntimeAdoptionError::Open(RuntimeOpenError::StableKeyNotCommitted(_)) => vec![],
        RuntimeAdoptionError::Open(RuntimeOpenError::MemoryIdMismatch {
            committed_id,
            requested_id,
            ..
        }) => vec![
            (
                DiagnosticFactTag::ExpectedMemoryId,
                u64::from(*requested_id),
            ),
            (DiagnosticFactTag::ActualMemoryId, u64::from(*committed_id)),
        ],
        _ => return None,
    };
    Some(boundary(
        RuntimeBoundaryCode::MemoryDeclarationSnapshotMismatch,
        facts,
    ))
}

fn memory_id_facts(cause: &MemoryManagerRangeAuthorityError) -> Vec<(DiagnosticFactTag, u64)> {
    match cause {
        MemoryManagerRangeAuthorityError::UnclaimedId { id }
        | MemoryManagerRangeAuthorityError::AuthorityMismatch { id, .. }
        | MemoryManagerRangeAuthorityError::ModeMismatch { id, .. } => {
            vec![(DiagnosticFactTag::ActualMemoryId, u64::from(*id))]
        }
        _ => vec![],
    }
}

fn boundary(code: RuntimeBoundaryCode, facts: Vec<(DiagnosticFactTag, u64)>) -> Error {
    Error::from_diagnostic_and_facts(
        Error::from_runtime_boundary(code, ErrorOrigin::Runtime).diagnostic(),
        facts,
    )
}
