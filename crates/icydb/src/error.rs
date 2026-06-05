use candid::CandidType;
use icydb_core::{
    db::{QueryError, QueryExecutionError, ResponseError},
    error::{ErrorClass as CoreErrorClass, ErrorOrigin as CoreErrorOrigin, InternalError},
};
use serde::Deserialize;
use thiserror::Error as ThisError;

//
// Error
//

#[cfg_attr(doc, doc = "Error\n\nPublic error payload.")]
#[derive(CandidType, Debug, Deserialize, ThisError)]
#[error("{message}")]
pub struct Error {
    kind: ErrorKind,
    origin: ErrorOrigin,
    message: String,
}

impl Error {
    pub fn new(kind: ErrorKind, origin: ErrorOrigin, message: impl Into<String>) -> Self {
        Self {
            kind,
            origin,
            message: message.into(),
        }
    }

    fn from_response_error(err: ResponseError) -> Self {
        match err {
            ResponseError::NotFound { .. } => Self::new(
                ErrorKind::Query(QueryErrorKind::NotFound),
                ErrorOrigin::Response,
                err.to_string(),
            ),

            ResponseError::NotUnique { .. } => Self::new(
                ErrorKind::Query(QueryErrorKind::NotUnique),
                ErrorOrigin::Response,
                err.to_string(),
            ),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    #[must_use]
    pub const fn origin(&self) -> ErrorOrigin {
        self.origin
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl From<InternalError> for Error {
    fn from(err: InternalError) -> Self {
        Self::new(
            ErrorKind::Runtime(map_class(err.class())),
            err.origin().into(),
            err.into_message(),
        )
    }
}

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        match err {
            QueryError::Validate(_) => Self::new(
                ErrorKind::Query(QueryErrorKind::Validate),
                ErrorOrigin::Query,
                err.to_string(),
            ),

            QueryError::Intent(_) => Self::new(
                ErrorKind::Query(QueryErrorKind::Intent),
                ErrorOrigin::Query,
                err.to_string(),
            ),

            QueryError::Plan(ref plan) => {
                let kind = if plan.as_ref().is_unordered_pagination() {
                    QueryErrorKind::UnorderedPagination
                } else {
                    QueryErrorKind::Plan
                };

                Self::new(ErrorKind::Query(kind), ErrorOrigin::Query, err.to_string())
            }

            QueryError::AccessRequirement(_) => Self::new(
                ErrorKind::Query(QueryErrorKind::Plan),
                ErrorOrigin::Query,
                err.to_string(),
            ),

            QueryError::Response(err) => Self::from_response_error(err),

            QueryError::Execute(err) => match err {
                QueryExecutionError::Corruption(inner)
                | QueryExecutionError::IncompatiblePersistedFormat(inner)
                | QueryExecutionError::InvariantViolation(inner)
                | QueryExecutionError::Conflict(inner)
                | QueryExecutionError::NotFound(inner)
                | QueryExecutionError::Unsupported(inner)
                | QueryExecutionError::Internal(inner) => inner.into(),
            },
        }
    }
}

const fn map_class(class: CoreErrorClass) -> RuntimeErrorKind {
    match class {
        CoreErrorClass::Corruption => RuntimeErrorKind::Corruption,
        CoreErrorClass::IncompatiblePersistedFormat => {
            RuntimeErrorKind::IncompatiblePersistedFormat
        }
        CoreErrorClass::InvariantViolation => RuntimeErrorKind::InvariantViolation,
        CoreErrorClass::Conflict => RuntimeErrorKind::Conflict,
        CoreErrorClass::NotFound => RuntimeErrorKind::NotFound,
        CoreErrorClass::Unsupported => RuntimeErrorKind::Unsupported,
        CoreErrorClass::Internal => RuntimeErrorKind::Internal,
    }
}

impl From<ResponseError> for Error {
    fn from(err: ResponseError) -> Self {
        Self::from_response_error(err)
    }
}

#[cfg_attr(doc, doc = "ErrorKind\n\nPublic error category.")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum ErrorKind {
    Query(QueryErrorKind),

    /// Runtime failure.
    Runtime(RuntimeErrorKind),
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

#[cfg_attr(doc, doc = "QueryErrorKind\n\nPublic query error class.")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
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

//
// TESTS
//

#[cfg(test)]
mod tests;
