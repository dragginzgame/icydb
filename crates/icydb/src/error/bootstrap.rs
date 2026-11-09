//! Project current Storage admission and directory failures into public diagnostics.

use crate::{
    db::{DatabaseBootstrapError, MemoryBootstrapAdmissionError},
    error::{Error, ErrorKind, ErrorOrigin, RuntimeErrorKind},
};
use ic_storage::{
    DirectoryError, RuntimeBootstrapError, RuntimeConstructionError, RuntimeOpenError,
};
use icydb_diagnostic_code::{DiagnosticFactTag, RuntimeBoundaryCode};

pub(super) fn bootstrap_error(cause: &DatabaseBootstrapError) -> Error {
    let classified = match cause {
        DatabaseBootstrapError::Construction(cause) => match cause.as_ref() {
            RuntimeConstructionError::Directory(error) => Some(directory_error(error)),
            RuntimeConstructionError::BackingAlreadyClaimed => None,
        },
        DatabaseBootstrapError::Binding(_) => None,
        DatabaseBootstrapError::Bootstrap(cause) => match cause.as_ref() {
            RuntimeBootstrapError::Admission(_) => Some(boundary(
                RuntimeBoundaryCode::MemoryDeclarationInvalid,
                vec![],
            )),
            RuntimeBootstrapError::BindingMismatch => Some(boundary(
                RuntimeBoundaryCode::MemoryDeclarationSnapshotMismatch,
                vec![],
            )),
            RuntimeBootstrapError::Policy(error) => Some(admission_error(error)),
            RuntimeBootstrapError::Directory(error) => Some(directory_error(error)),
            RuntimeBootstrapError::NotBootstrapped => None,
        },
        DatabaseBootstrapError::Adoption(cause) => match cause.as_ref() {
            RuntimeOpenError::NotAdmitted(_) => Some(boundary(
                RuntimeBoundaryCode::MemoryDeclarationSnapshotMismatch,
                vec![],
            )),
            RuntimeOpenError::Directory(error) => Some(directory_error(error)),
            _ => None,
        },
    };
    classified.unwrap_or_else(|| runtime_error(RuntimeErrorKind::Internal))
}

const fn runtime_error(kind: RuntimeErrorKind) -> Error {
    Error::from_kind(ErrorKind::Runtime(kind), ErrorOrigin::Runtime)
}

const fn directory_error(error: &DirectoryError) -> Error {
    // Storage owns framing validation. Do not parse its diagnostic strings to
    // distinguish unsupported bytes from corrupt current-format metadata.
    runtime_error(match error {
        DirectoryError::Invalid(_) => RuntimeErrorKind::Corruption,
        DirectoryError::Budget | DirectoryError::ArithmeticOverflow => {
            RuntimeErrorKind::Unsupported
        }
        _ => RuntimeErrorKind::Internal,
    })
}

fn admission_error(cause: &MemoryBootstrapAdmissionError) -> Error {
    match cause {
        MemoryBootstrapAdmissionError::UnsupportedKey(_)
        | MemoryBootstrapAdmissionError::InvalidDeclaration(_) => {
            boundary(RuntimeBoundaryCode::MemoryDeclarationInvalid, vec![])
        }
        MemoryBootstrapAdmissionError::IncompleteRoles { store, .. } => boundary(
            RuntimeBoundaryCode::MemoryAllocationRolesIncomplete,
            vec![(
                DiagnosticFactTag::ExpectedCount,
                if store.is_some() { 4 } else { 3 },
            )],
        ),
        MemoryBootstrapAdmissionError::NamespaceRemoved(_) => {
            boundary(RuntimeBoundaryCode::MemoryNamespaceRemoved, vec![])
        }
        MemoryBootstrapAdmissionError::HistoricalJournal(_) => boundary(
            RuntimeBoundaryCode::MemoryHistoricalJournalUnavailable,
            vec![],
        ),
    }
}

fn boundary(code: RuntimeBoundaryCode, facts: Vec<(DiagnosticFactTag, u64)>) -> Error {
    Error::from_diagnostic_and_facts(
        Error::from_runtime_boundary(code, ErrorOrigin::Runtime).diagnostic(),
        facts,
    )
}
