use crate::db::store::StoreError;
use std::fmt;
use thiserror::Error as ThisError;

///
/// InternalError
/// Structured runtime error with a stable internal classification.
/// Not a stable API; intended for internal use and may change without notice.
///

#[derive(Debug, ThisError)]
#[error("{message}")]
pub struct InternalError {
    pub class: ErrorClass,
    pub origin: ErrorOrigin,
    pub message: String,
    pub store: Option<StoreError>,
}

impl InternalError {
    pub fn new(class: ErrorClass, origin: ErrorOrigin, message: impl Into<String>) -> Self {
        let message = message.into();
        let store = match (class, origin) {
            (ErrorClass::Corruption, ErrorOrigin::Store) => Some(StoreError::Corrupt {
                message: message.clone(),
            }),
            (ErrorClass::InvariantViolation, ErrorOrigin::Store) => {
                Some(StoreError::InvariantViolation {
                    message: message.clone(),
                })
            }
            _ => None,
        };
        Self {
            class,
            origin,
            message,
            store,
        }
    }

    pub fn store_not_found(key: impl Into<String>) -> Self {
        let key = key.into();
        Self {
            class: ErrorClass::NotFound,
            origin: ErrorOrigin::Store,
            message: format!("data key not found: {key}"),
            store: Some(StoreError::NotFound { key }),
        }
    }

    #[must_use]
    pub const fn is_not_found(&self) -> bool {
        matches!(self.store, Some(StoreError::NotFound { .. }))
    }

    #[must_use]
    pub fn display_with_class(&self) -> String {
        format!("{}:{}: {}", self.origin, self.class, self.message)
    }
}

///
/// ErrorClass
/// Internal error taxonomy for runtime classification.
/// Not a stable API; may change without notice.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorClass {
    Corruption,
    NotFound,
    Internal,
    Conflict,
    Unsupported,
    InvariantViolation,
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Corruption => "corruption",
            Self::NotFound => "not_found",
            Self::Internal => "internal",
            Self::Conflict => "conflict",
            Self::Unsupported => "unsupported",
            Self::InvariantViolation => "invariant_violation",
        };
        write!(f, "{label}")
    }
}

///
/// ErrorOrigin
/// Internal origin taxonomy for runtime classification.
/// Not a stable API; may change without notice.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorOrigin {
    Serialize,
    Store,
    Index,
    Query,
    Response,
    Executor,
    Interface,
}

impl fmt::Display for ErrorOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Serialize => "serialize",
            Self::Store => "store",
            Self::Index => "index",
            Self::Query => "query",
            Self::Response => "response",
            Self::Executor => "executor",
            Self::Interface => "interface",
        };
        write!(f, "{label}")
    }
}
