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
    code: u16,
    class: u8,
    origin: u8,
}

impl Error {
    /// Build a compact public error from one diagnostic code and origin.
    #[must_use]
    pub const fn from_code(
        code: icydb_diagnostic_code::DiagnosticCode,
        origin: ErrorOrigin,
    ) -> Self {
        Self::from_error_code(code.error_code(), origin)
    }

    /// Build a public error from one numeric wire code and origin.
    #[must_use]
    pub const fn from_error_code(
        code: icydb_diagnostic_code::ErrorCode,
        origin: ErrorOrigin,
    ) -> Self {
        Self {
            code: code.raw(),
            class: error_class_wire_code(code.class()),
            origin: origin.wire_code(),
        }
    }

    /// Build a compact public error from one public category and origin.
    ///
    /// This helper keeps generated endpoint code concise while the wire
    /// payload itself remains code/detail-first.
    #[must_use]
    pub const fn from_kind(kind: ErrorKind, origin: ErrorOrigin) -> Self {
        let code = kind.diagnostic_code();
        let error_code =
            icydb_diagnostic_code::ErrorCode::from_parts(code, Some(kind.diagnostic_detail()));

        Self::from_error_code(error_code, origin)
    }

    /// Build a compact public runtime-boundary error.
    #[must_use]
    pub const fn from_runtime_boundary(
        boundary: icydb_diagnostic_code::RuntimeBoundaryCode,
        origin: ErrorOrigin,
    ) -> Self {
        let error_code = icydb_diagnostic_code::ErrorCode::from_parts(
            icydb_diagnostic_code::DiagnosticCode::RuntimeUnsupported,
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary { boundary }),
        );

        Self::from_error_code(error_code, origin)
    }

    /// Build a compact public error from a full diagnostic payload.
    #[must_use]
    pub fn from_diagnostic(diagnostic: icydb_diagnostic_code::Diagnostic) -> Self {
        Self::from_error_code(diagnostic.error_code(), diagnostic.origin().into())
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
    pub const fn code(&self) -> icydb_diagnostic_code::ErrorCode {
        icydb_diagnostic_code::ErrorCode::from_raw(self.code)
    }

    /// Return the broad compact diagnostic class.
    #[must_use]
    pub const fn class(&self) -> icydb_diagnostic_code::ErrorClass {
        match self.class {
            1 => icydb_diagnostic_code::ErrorClass::Query,
            2 => icydb_diagnostic_code::ErrorClass::Corruption,
            3 => icydb_diagnostic_code::ErrorClass::IncompatiblePersistedFormat,
            4 => icydb_diagnostic_code::ErrorClass::NotFound,
            5 => icydb_diagnostic_code::ErrorClass::Internal,
            6 => icydb_diagnostic_code::ErrorClass::Conflict,
            7 => icydb_diagnostic_code::ErrorClass::Unsupported,
            8 => icydb_diagnostic_code::ErrorClass::InvariantViolation,
            _ => self.code().class(),
        }
    }

    /// Return the diagnostic origin.
    #[must_use]
    pub const fn origin(&self) -> ErrorOrigin {
        ErrorOrigin::from_wire_code(self.origin)
    }

    /// Return compact diagnostic identity for this error.
    #[must_use]
    pub fn diagnostic(&self) -> icydb_diagnostic_code::Diagnostic {
        self.code().diagnostic(self.origin().into())
    }

    /// Return the compact diagnostic code for this error.
    #[must_use]
    pub const fn diagnostic_code(&self) -> icydb_diagnostic_code::DiagnosticCode {
        self.code().diagnostic_code()
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
        write!(f, "E{}", self.code)
    }
}

impl std::error::Error for Error {}

const fn error_class_wire_code(class: icydb_diagnostic_code::ErrorClass) -> u8 {
    match class {
        icydb_diagnostic_code::ErrorClass::Query => 1,
        icydb_diagnostic_code::ErrorClass::Corruption => 2,
        icydb_diagnostic_code::ErrorClass::IncompatiblePersistedFormat => 3,
        icydb_diagnostic_code::ErrorClass::NotFound => 4,
        icydb_diagnostic_code::ErrorClass::Internal => 5,
        icydb_diagnostic_code::ErrorClass::Conflict => 6,
        icydb_diagnostic_code::ErrorClass::Unsupported => 7,
        icydb_diagnostic_code::ErrorClass::InvariantViolation => 8,
    }
}

#[cfg_attr(doc, doc = "ErrorKind\n\nPublic error category.")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

impl ErrorOrigin {
    const fn wire_code(self) -> u8 {
        match self {
            Self::Cursor => 1,
            Self::Executor => 2,
            Self::Identity => 3,
            Self::Index => 4,
            Self::Interface => 5,
            Self::Planner => 6,
            Self::Query => 7,
            Self::Recovery => 8,
            Self::Response => 9,
            Self::Runtime => 10,
            Self::Serialize => 11,
            Self::Store => 12,
        }
    }

    const fn from_wire_code(code: u8) -> Self {
        match code {
            1 => Self::Cursor,
            2 => Self::Executor,
            3 => Self::Identity,
            4 => Self::Index,
            5 => Self::Interface,
            6 => Self::Planner,
            7 => Self::Query,
            8 => Self::Recovery,
            9 => Self::Response,
            11 => Self::Serialize,
            12 => Self::Store,
            _ => Self::Runtime,
        }
    }
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
