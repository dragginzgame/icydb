use crate::{
    db::query::plan::{CursorPlanError, PlanError},
    patch::MergePatchError,
};
use std::fmt;
use thiserror::Error as ThisError;

///
/// InternalError
///
/// Structured runtime error with a stable internal classification.
/// Not a stable API; intended for internal use and may change without notice.
///

#[derive(Debug, ThisError)]
#[error("{message}")]
pub struct InternalError {
    pub class: ErrorClass,
    pub origin: ErrorOrigin,
    pub message: String,

    /// Optional structured error detail.
    /// The variant (if present) must correspond to `origin`.
    pub detail: Option<ErrorDetail>,
}

impl InternalError {
    /// Construct an InternalError with optional origin-specific detail.
    /// This constructor provides default StoreError details for certain
    /// (class, origin) combinations but does not guarantee a detail payload.
    pub fn new(class: ErrorClass, origin: ErrorOrigin, message: impl Into<String>) -> Self {
        let message = message.into();

        let detail = match (class, origin) {
            (ErrorClass::Corruption, ErrorOrigin::Store) => {
                Some(ErrorDetail::Store(StoreError::Corrupt {
                    message: message.clone(),
                }))
            }
            (ErrorClass::InvariantViolation, ErrorOrigin::Store) => {
                Some(ErrorDetail::Store(StoreError::InvariantViolation {
                    message: message.clone(),
                }))
            }
            _ => None,
        };

        Self {
            class,
            origin,
            message,
            detail,
        }
    }

    /// Construct a query-origin invariant violation.
    pub(crate) fn query_invariant(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            message.into(),
        )
    }

    /// Construct an index-origin invariant violation.
    pub(crate) fn index_invariant(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Index,
            message.into(),
        )
    }

    /// Construct an executor-origin invariant violation.
    pub(crate) fn executor_invariant(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Executor,
            message.into(),
        )
    }

    /// Construct a store-origin invariant violation.
    pub(crate) fn store_invariant(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Store,
            message.into(),
        )
    }

    /// Construct a corruption error for a specific origin.
    pub(crate) fn corruption(origin: ErrorOrigin, message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Corruption, origin, message.into())
    }

    /// Construct a store-origin internal error.
    pub(crate) fn store_internal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Store, message.into())
    }

    /// Construct an executor-origin internal error.
    pub(crate) fn executor_internal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Executor, message.into())
    }

    /// Construct an index-origin internal error.
    pub(crate) fn index_internal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Index, message.into())
    }

    /// Construct a serialize-origin internal error.
    pub(crate) fn serialize_internal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Serialize, message.into())
    }

    /// Construct a store-origin corruption error.
    pub(crate) fn store_corruption(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Corruption, ErrorOrigin::Store, message.into())
    }

    /// Construct an index-origin corruption error.
    pub(crate) fn index_corruption(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Corruption, ErrorOrigin::Index, message.into())
    }

    /// Construct a serialize-origin corruption error.
    pub(crate) fn serialize_corruption(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Corruption,
            ErrorOrigin::Serialize,
            message.into(),
        )
    }

    /// Construct a store-origin unsupported error.
    pub(crate) fn store_unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Store, message.into())
    }

    /// Construct an index-origin unsupported error.
    pub(crate) fn index_unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Index, message.into())
    }

    /// Construct an executor-origin unsupported error.
    pub(crate) fn executor_unsupported(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Executor,
            message.into(),
        )
    }

    /// Construct a serialize-origin unsupported error.
    pub(crate) fn serialize_unsupported(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Serialize,
            message.into(),
        )
    }

    pub fn store_not_found(key: impl Into<String>) -> Self {
        let key = key.into();

        Self {
            class: ErrorClass::NotFound,
            origin: ErrorOrigin::Store,
            message: format!("data key not found: {key}"),
            detail: Some(ErrorDetail::Store(StoreError::NotFound { key })),
        }
    }

    /// Construct a standardized unsupported-entity-path error.
    pub fn unsupported_entity_path(path: impl Into<String>) -> Self {
        let path = path.into();

        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Store,
            format!("unsupported entity path: '{path}'"),
        )
    }

    #[must_use]
    pub const fn is_not_found(&self) -> bool {
        matches!(
            self.detail,
            Some(ErrorDetail::Store(StoreError::NotFound { .. }))
        )
    }

    #[must_use]
    pub fn display_with_class(&self) -> String {
        format!("{}:{}: {}", self.origin, self.class, self.message)
    }

    /// Construct an index-plan corruption error with a canonical prefix.
    pub(crate) fn index_plan_corruption(origin: ErrorOrigin, message: impl Into<String>) -> Self {
        let message = message.into();
        Self::new(
            ErrorClass::Corruption,
            origin,
            format!("corruption detected ({origin}): {message}"),
        )
    }

    /// Construct an index uniqueness violation conflict error.
    pub(crate) fn index_violation(path: &str, index_fields: &[&str]) -> Self {
        Self::new(
            ErrorClass::Conflict,
            ErrorOrigin::Index,
            format!(
                "index constraint violation: {path} ({})",
                index_fields.join(", ")
            ),
        )
    }

    /// Map plan-surface cursor failures into executor-boundary invariants.
    pub(crate) fn from_cursor_plan_error(err: PlanError) -> Self {
        let message = match &err {
            PlanError::Cursor(inner) => match inner.as_ref() {
                CursorPlanError::ContinuationCursorBoundaryArityMismatch { expected: 1, found } => {
                    format!(
                        "executor invariant violated: pk-ordered continuation boundary must contain exactly 1 slot, found {found}"
                    )
                }
                CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                    value: None, ..
                } => "executor invariant violated: pk cursor slot must be present".to_string(),
                CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                    value: Some(_),
                    ..
                } => "executor invariant violated: pk cursor slot type mismatch".to_string(),
                _ => err.to_string(),
            },
            _ => err.to_string(),
        };

        Self::query_invariant(message)
    }

    /// Map shared plan-validation failures into executor-boundary invariants.
    pub(crate) fn from_executor_plan_error(err: PlanError) -> Self {
        Self::query_invariant(err.to_string())
    }
}

///
/// ErrorDetail
///
/// Structured, origin-specific error detail carried by [`InternalError`].
/// This enum is intentionally extensible.
///

#[derive(Debug, ThisError)]
pub enum ErrorDetail {
    #[error("{0}")]
    Store(StoreError),
    #[error("{0}")]
    ViewPatch(crate::patch::MergePatchError),
    // Future-proofing:
    // #[error("{0}")]
    // Index(IndexError),
    //
    // #[error("{0}")]
    // Query(QueryErrorDetail),
    //
    // #[error("{0}")]
    // Executor(ExecutorErrorDetail),
}

impl From<MergePatchError> for InternalError {
    fn from(err: MergePatchError) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Interface,
            message: err.to_string(),
            detail: Some(ErrorDetail::ViewPatch(err)),
        }
    }
}

///
/// StoreError
///
/// Store-specific structured error detail.
/// Never returned directly; always wrapped in [`ErrorDetail::Store`].
///

#[derive(Debug, ThisError)]
pub enum StoreError {
    #[error("key not found: {key}")]
    NotFound { key: String },

    #[error("store corruption: {message}")]
    Corrupt { message: String },

    #[error("store invariant violation: {message}")]
    InvariantViolation { message: String },
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
