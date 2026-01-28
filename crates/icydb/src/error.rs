use candid::CandidType;
use derive_more::Display;
use icydb_core::{
    db::query::QueryError,
    error::{ErrorClass as CoreErrorClass, ErrorOrigin as CoreErrorOrigin, InternalError},
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
    pub class: ErrorClass,
    pub origin: ErrorOrigin,
    pub message: String,
}

impl Error {
    #[must_use]
    pub fn new(class: ErrorClass, origin: ErrorOrigin, message: impl Into<String>) -> Self {
        Self {
            class,
            origin,
            message: message.into(),
        }
    }
}

impl From<InternalError> for Error {
    fn from(err: InternalError) -> Self {
        Self {
            class: err.class.into(),
            origin: err.origin.into(),
            message: err.message,
        }
    }
}

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        match err {
            QueryError::Validate(err) => {
                Self::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
            }
            QueryError::Plan(err) => {
                Self::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
            }
            QueryError::Intent(err) => {
                Self::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
            }
            QueryError::Execute(err) => Self::from(err),
        }
    }
}

///
/// ErrorClass
/// Public error taxonomy for callers and canister interfaces.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Display, Eq, PartialEq, Serialize)]
pub enum ErrorClass {
    Conflict,
    Corruption,
    NotFound,
    Internal,
    InvariantViolation,
    Unsupported,
}

impl From<CoreErrorClass> for ErrorClass {
    fn from(class: CoreErrorClass) -> Self {
        match class {
            CoreErrorClass::Conflict => Self::Conflict,
            CoreErrorClass::Corruption => Self::Corruption,
            CoreErrorClass::NotFound => Self::NotFound,
            CoreErrorClass::Internal => Self::Internal,
            CoreErrorClass::InvariantViolation => Self::InvariantViolation,
            CoreErrorClass::Unsupported => Self::Unsupported,
        }
    }
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
