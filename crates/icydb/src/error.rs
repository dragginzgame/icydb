use candid::CandidType;
use derive_more::Display;
use icydb_core::{
    db::{query::QueryError, response::ResponseError},
    error::{ErrorOrigin as CoreErrorOrigin, InternalError},
    patch::MergePatchError,
};
use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;

///
/// Error
/// Public error type with a stable class + origin taxonomy.
///

#[derive(CandidType, Debug, Deserialize, Serialize, ThisError)]
#[error("{message}")]
pub struct Error {
    pub kind: ErrorKind,
    pub origin: ErrorOrigin,
    pub message: String,
}

impl Error {
    pub fn new(kind: ErrorKind, origin: ErrorOrigin, message: impl Into<String>) -> Self {
        Self {
            kind,
            origin,
            message: message.into(),
        }
    }
}

impl From<InternalError> for Error {
    fn from(err: InternalError) -> Self {
        Self::new(ErrorKind::Internal, err.origin.into(), err.message)
    }
}

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        match err {
            QueryError::Validate(_) | QueryError::Intent(_) | QueryError::Plan(_) => Self::new(
                ErrorKind::Query(QueryErrorKind::Invalid),
                ErrorOrigin::Query,
                err.to_string(),
            ),

            QueryError::UnsupportedQueryFeature(_) => Self::new(
                ErrorKind::Query(QueryErrorKind::Unsupported),
                ErrorOrigin::Query,
                err.to_string(),
            ),

            QueryError::Response(ResponseError::NotFound { .. }) => Self::new(
                ErrorKind::Query(QueryErrorKind::NotFound),
                ErrorOrigin::Response,
                err.to_string(),
            ),

            QueryError::Response(ResponseError::NotUnique { .. }) => Self::new(
                ErrorKind::Query(QueryErrorKind::NotUnique),
                ErrorOrigin::Response,
                err.to_string(),
            ),

            QueryError::Execute(err) => err.into(),
        }
    }
}

impl From<ResponseError> for Error {
    fn from(err: ResponseError) -> Self {
        match err {
            ResponseError::NotFound { .. } => Self::new(
                ErrorKind::Query(QueryErrorKind::NotFound),
                ErrorOrigin::Query,
                err.to_string(),
            ),

            ResponseError::NotUnique { .. } => Self::new(
                ErrorKind::Query(QueryErrorKind::NotUnique),
                ErrorOrigin::Query,
                err.to_string(),
            ),
        }
    }
}

///
/// ErrorKind
/// Public error taxonomy for callers and canister interfaces.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ErrorKind {
    Query(QueryErrorKind),
    Update(UpdateErrorKind),
    Store(StoreErrorKind),

    /// The caller cannot remediate this.
    Internal,
}

///
/// QueryErrorKind
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum QueryErrorKind {
    /// Query shape is invalid (unknown fields, bad predicates).
    Invalid,

    /// The query is valid but requests an unsupported feature.
    Unsupported,

    /// Pagination requires ordering but none was provided.
    UnorderedPagination,

    /// Valid query, but no rows matched.
    NotFound,

    /// Query expected one row but matched many.
    NotUnique,
}

///
/// UpdateErrorKind
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum UpdateErrorKind {
    /// Patch application failed for semantic reasons.
    Patch(PatchError),

    /// Target entity does not exist.
    NotFound,

    /// Domain or schema constraint violated.
    ConstraintViolation,

    /// Concurrent or state conflict.
    Conflict,
}

///
/// PatchError
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PatchError {
    InvalidShape,
    MissingKey,
    CardinalityViolation,
}

impl From<MergePatchError> for PatchError {
    fn from(err: MergePatchError) -> Self {
        match err {
            MergePatchError::InvalidShape { .. } => Self::InvalidShape,
            MergePatchError::MissingKey { .. } => Self::MissingKey,
            MergePatchError::CardinalityViolation { .. } => Self::CardinalityViolation,
            MergePatchError::Context { source, .. } => (*source).into(),
        }
    }
}

///
/// StoreErrorKind
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StoreErrorKind {
    NotFound,
    Unavailable,
}

///
/// ErrorOrigin
/// Public origin taxonomy for callers and canister interfaces.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Display, Eq, PartialEq, Serialize)]
pub enum ErrorOrigin {
    Executor,
    Index,
    Interface,
    Query,
    Response,
    Serialize,
    Store,
}

impl From<CoreErrorOrigin> for ErrorOrigin {
    fn from(origin: CoreErrorOrigin) -> Self {
        match origin {
            CoreErrorOrigin::Executor => Self::Executor,
            CoreErrorOrigin::Index => Self::Index,
            CoreErrorOrigin::Interface => Self::Interface,
            CoreErrorOrigin::Query => Self::Query,
            CoreErrorOrigin::Response => Self::Response,
            CoreErrorOrigin::Serialize => Self::Serialize,
            CoreErrorOrigin::Store => Self::Store,
        }
    }
}
