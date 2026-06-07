use candid::CandidType;
use icydb_core::{
    db::{QueryError, ResponseError},
    error::{ErrorOrigin as CoreErrorOrigin, InternalError},
};
use serde::Deserialize;
use std::fmt;

//
// Error
//

#[cfg_attr(doc, doc = "Error\n\nPublic error payload.")]
#[derive(CandidType, Debug, Deserialize)]
pub struct Error {
    code: icydb_diagnostic_code::DiagnosticCode,
    class: icydb_diagnostic_code::ErrorClass,
    origin: ErrorOrigin,
    detail: Option<icydb_diagnostic_code::DiagnosticDetail>,
}

impl Error {
    /// Build a compact public error from one diagnostic code and origin.
    #[must_use]
    pub const fn from_code(
        code: icydb_diagnostic_code::DiagnosticCode,
        origin: ErrorOrigin,
    ) -> Self {
        Self {
            code,
            class: code.class(),
            origin,
            detail: None,
        }
    }

    /// Build a compact public error from one public category and origin.
    ///
    /// This helper keeps generated endpoint code concise while the wire
    /// payload itself remains code/detail-first.
    #[must_use]
    pub const fn from_kind(kind: ErrorKind, origin: ErrorOrigin) -> Self {
        let code = kind.diagnostic_code();

        Self {
            code,
            class: code.class(),
            origin,
            detail: Some(kind.diagnostic_detail()),
        }
    }

    /// Build a compact public error from a full diagnostic payload.
    #[must_use]
    pub fn from_diagnostic(diagnostic: icydb_diagnostic_code::Diagnostic) -> Self {
        Self {
            code: diagnostic.code(),
            class: diagnostic.class(),
            origin: diagnostic.origin().into(),
            detail: diagnostic.detail().cloned(),
        }
    }

    const fn from_response_error(err: ResponseError) -> Self {
        match err {
            ResponseError::NotFound { .. } => Self::from_kind(
                ErrorKind::Query(QueryErrorKind::NotFound),
                ErrorOrigin::Response,
            ),

            ResponseError::NotUnique { .. } => Self::from_kind(
                ErrorKind::Query(QueryErrorKind::NotUnique),
                ErrorOrigin::Response,
            ),
        }
    }

    /// Return the compact diagnostic code.
    #[must_use]
    pub const fn code(&self) -> icydb_diagnostic_code::DiagnosticCode {
        self.code
    }

    /// Return the broad compact diagnostic class.
    #[must_use]
    pub const fn class(&self) -> icydb_diagnostic_code::ErrorClass {
        self.class
    }

    /// Return the diagnostic origin.
    #[must_use]
    pub const fn origin(&self) -> ErrorOrigin {
        self.origin
    }

    /// Return compact structured diagnostic detail, when available.
    #[must_use]
    pub const fn detail(&self) -> Option<&icydb_diagnostic_code::DiagnosticDetail> {
        self.detail.as_ref()
    }

    /// Return compact diagnostic identity for this error.
    #[must_use]
    pub fn diagnostic(&self) -> icydb_diagnostic_code::Diagnostic {
        icydb_diagnostic_code::Diagnostic::new(self.code, self.origin.into(), self.detail.clone())
    }

    /// Return the compact diagnostic code for this error.
    #[must_use]
    pub const fn diagnostic_code(&self) -> icydb_diagnostic_code::DiagnosticCode {
        self.code
    }
}

impl From<InternalError> for Error {
    fn from(err: InternalError) -> Self {
        Self::from_diagnostic(err.diagnostic())
    }
}

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        Self::from_diagnostic(err.diagnostic())
    }
}

impl From<ResponseError> for Error {
    fn from(err: ResponseError) -> Self {
        Self::from_response_error(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.code)
    }
}

impl std::error::Error for Error {}

#[cfg_attr(doc, doc = "ErrorKind\n\nPublic error category.")]
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum ErrorKind {
    Query(QueryErrorKind),

    /// Runtime failure.
    Runtime(RuntimeErrorKind),
}

impl ErrorKind {
    /// Return the compact diagnostic code for this public error category.
    #[must_use]
    pub const fn diagnostic_code(self) -> icydb_diagnostic_code::DiagnosticCode {
        match self {
            Self::Query(kind) => kind.diagnostic_code(),
            Self::Runtime(kind) => kind.diagnostic_code(),
        }
    }

    const fn diagnostic_detail(self) -> icydb_diagnostic_code::DiagnosticDetail {
        match self {
            Self::Query(kind) => icydb_diagnostic_code::DiagnosticDetail::QueryKind {
                kind: kind.diagnostic_kind(),
            },
            Self::Runtime(kind) => icydb_diagnostic_code::DiagnosticDetail::RuntimeKind {
                kind: kind.diagnostic_kind(),
            },
        }
    }
}

#[cfg_attr(doc, doc = "RuntimeErrorKind\n\nPublic runtime error class.")]
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum RuntimeErrorKind {
    Corruption,
    IncompatiblePersistedFormat,
    InvariantViolation,
    Conflict,
    NotFound,
    Unsupported,
    Internal,
}

impl RuntimeErrorKind {
    /// Return the compact diagnostic code for this runtime category.
    #[must_use]
    pub const fn diagnostic_code(self) -> icydb_diagnostic_code::DiagnosticCode {
        match self {
            Self::Corruption => icydb_diagnostic_code::DiagnosticCode::RuntimeCorruption,
            Self::IncompatiblePersistedFormat => {
                icydb_diagnostic_code::DiagnosticCode::RuntimeIncompatiblePersistedFormat
            }
            Self::InvariantViolation => {
                icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation
            }
            Self::Conflict => icydb_diagnostic_code::DiagnosticCode::RuntimeConflict,
            Self::NotFound => icydb_diagnostic_code::DiagnosticCode::RuntimeNotFound,
            Self::Unsupported => icydb_diagnostic_code::DiagnosticCode::RuntimeUnsupported,
            Self::Internal => icydb_diagnostic_code::DiagnosticCode::RuntimeInternal,
        }
    }

    const fn diagnostic_kind(self) -> icydb_diagnostic_code::RuntimeErrorKind {
        match self {
            Self::Corruption => icydb_diagnostic_code::RuntimeErrorKind::Corruption,
            Self::IncompatiblePersistedFormat => {
                icydb_diagnostic_code::RuntimeErrorKind::IncompatiblePersistedFormat
            }
            Self::InvariantViolation => icydb_diagnostic_code::RuntimeErrorKind::InvariantViolation,
            Self::Conflict => icydb_diagnostic_code::RuntimeErrorKind::Conflict,
            Self::NotFound => icydb_diagnostic_code::RuntimeErrorKind::NotFound,
            Self::Unsupported => icydb_diagnostic_code::RuntimeErrorKind::Unsupported,
            Self::Internal => icydb_diagnostic_code::RuntimeErrorKind::Internal,
        }
    }
}

#[cfg_attr(doc, doc = "QueryErrorKind\n\nPublic query error class.")]
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum QueryErrorKind {
    /// Validation failed.
    Validate,

    /// Intent validation failed.
    Intent,

    /// Planning failed.
    Plan,

    /// Pagination lacked ordering.
    UnorderedPagination,

    /// Continuation cursor was invalid.
    InvalidContinuationCursor,

    /// No rows matched.
    NotFound,

    /// More than one row matched.
    NotUnique,
}

impl QueryErrorKind {
    /// Return the compact diagnostic code for this query category.
    #[must_use]
    pub const fn diagnostic_code(self) -> icydb_diagnostic_code::DiagnosticCode {
        match self {
            Self::Validate => icydb_diagnostic_code::DiagnosticCode::QueryValidate,
            Self::Intent => icydb_diagnostic_code::DiagnosticCode::QueryIntent,
            Self::Plan => icydb_diagnostic_code::DiagnosticCode::QueryPlan,
            Self::UnorderedPagination => {
                icydb_diagnostic_code::DiagnosticCode::QueryUnorderedPagination
            }
            Self::InvalidContinuationCursor => {
                icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor
            }
            Self::NotFound => icydb_diagnostic_code::DiagnosticCode::QueryNotFound,
            Self::NotUnique => icydb_diagnostic_code::DiagnosticCode::QueryNotUnique,
        }
    }

    const fn diagnostic_kind(self) -> icydb_diagnostic_code::QueryErrorKind {
        match self {
            Self::Validate => icydb_diagnostic_code::QueryErrorKind::Validate,
            Self::Intent => icydb_diagnostic_code::QueryErrorKind::Intent,
            Self::Plan => icydb_diagnostic_code::QueryErrorKind::Plan,
            Self::UnorderedPagination => icydb_diagnostic_code::QueryErrorKind::UnorderedPagination,
            Self::InvalidContinuationCursor => {
                icydb_diagnostic_code::QueryErrorKind::InvalidContinuationCursor
            }
            Self::NotFound => icydb_diagnostic_code::QueryErrorKind::NotFound,
            Self::NotUnique => icydb_diagnostic_code::QueryErrorKind::NotUnique,
        }
    }
}

#[cfg_attr(doc, doc = "ErrorOrigin\n\nPublic error origin.")]
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum ErrorOrigin {
    Cursor,
    Executor,
    Identity,
    Index,
    Interface,
    Planner,
    Query,
    Recovery,
    Response,
    Runtime,
    Serialize,
    Store,
}

impl From<CoreErrorOrigin> for ErrorOrigin {
    fn from(origin: CoreErrorOrigin) -> Self {
        match origin {
            CoreErrorOrigin::Cursor => Self::Cursor,
            CoreErrorOrigin::Executor => Self::Executor,
            CoreErrorOrigin::Identity => Self::Identity,
            CoreErrorOrigin::Index => Self::Index,
            CoreErrorOrigin::Interface => Self::Interface,
            CoreErrorOrigin::Planner => Self::Planner,
            CoreErrorOrigin::Query => Self::Query,
            CoreErrorOrigin::Recovery => Self::Recovery,
            CoreErrorOrigin::Response => Self::Response,
            CoreErrorOrigin::Serialize => Self::Serialize,
            CoreErrorOrigin::Store => Self::Store,
        }
    }
}

impl From<ErrorOrigin> for icydb_diagnostic_code::ErrorOrigin {
    fn from(origin: ErrorOrigin) -> Self {
        match origin {
            ErrorOrigin::Cursor => Self::Cursor,
            ErrorOrigin::Executor => Self::Executor,
            ErrorOrigin::Identity => Self::Identity,
            ErrorOrigin::Index => Self::Index,
            ErrorOrigin::Interface => Self::Interface,
            ErrorOrigin::Planner => Self::Planner,
            ErrorOrigin::Query => Self::Query,
            ErrorOrigin::Recovery => Self::Recovery,
            ErrorOrigin::Response => Self::Response,
            ErrorOrigin::Runtime => Self::Runtime,
            ErrorOrigin::Serialize => Self::Serialize,
            ErrorOrigin::Store => Self::Store,
        }
    }
}

impl From<icydb_diagnostic_code::ErrorOrigin> for ErrorOrigin {
    fn from(origin: icydb_diagnostic_code::ErrorOrigin) -> Self {
        match origin {
            icydb_diagnostic_code::ErrorOrigin::Cursor => Self::Cursor,
            icydb_diagnostic_code::ErrorOrigin::Executor => Self::Executor,
            icydb_diagnostic_code::ErrorOrigin::Identity => Self::Identity,
            icydb_diagnostic_code::ErrorOrigin::Index => Self::Index,
            icydb_diagnostic_code::ErrorOrigin::Interface => Self::Interface,
            icydb_diagnostic_code::ErrorOrigin::Planner => Self::Planner,
            icydb_diagnostic_code::ErrorOrigin::Query => Self::Query,
            icydb_diagnostic_code::ErrorOrigin::Recovery => Self::Recovery,
            icydb_diagnostic_code::ErrorOrigin::Response => Self::Response,
            icydb_diagnostic_code::ErrorOrigin::Runtime => Self::Runtime,
            icydb_diagnostic_code::ErrorOrigin::Serialize => Self::Serialize,
            icydb_diagnostic_code::ErrorOrigin::Store => Self::Store,
        }
    }
}

//
// TESTS
//

#[cfg(test)]
mod tests;
