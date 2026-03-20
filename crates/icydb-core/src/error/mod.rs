//! Module: error
//!
//! Responsibility: module-local ownership and contracts for error.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(test)]
mod tests;

use std::fmt;
use thiserror::Error as ThisError;

// ============================================================================
// INTERNAL ERROR TAXONOMY — ARCHITECTURAL CONTRACT
// ============================================================================
//
// This file defines the canonical runtime error classification system for
// icydb-core. It is the single source of truth for:
//
//   • ErrorClass   (semantic domain)
//   • ErrorOrigin  (subsystem boundary)
//   • Structured detail payloads
//   • Canonical constructor entry points
//
// -----------------------------------------------------------------------------
// DESIGN INTENT
// -----------------------------------------------------------------------------
//
// 1. InternalError is a *taxonomy carrier*, not a formatting utility.
//
//    - ErrorClass represents semantic meaning (corruption, invariant_violation,
//      unsupported, etc).
//    - ErrorOrigin represents the subsystem boundary (store, index, query,
//      executor, serialize, interface, etc).
//    - The (class, origin) pair must remain stable and intentional.
//
// 2. Call sites MUST prefer canonical constructors.
//
//    Do NOT construct errors manually via:
//        InternalError::new(class, origin, ...)
//    unless you are defining a new canonical helper here.
//
//    If a pattern appears more than once, centralize it here.
//
// 3. Constructors in this file must represent real architectural boundaries.
//
//    Add a new helper ONLY if it:
//
//      • Encodes a cross-cutting invariant,
//      • Represents a subsystem boundary,
//      • Or prevents taxonomy drift across call sites.
//
//    Do NOT add feature-specific helpers.
//    Do NOT add one-off formatting helpers.
//    Do NOT turn this file into a generic message factory.
//
// 4. ErrorDetail must align with ErrorOrigin.
//
//    If detail is present, it MUST correspond to the origin.
//    Do not attach mismatched detail variants.
//
// 5. Plan-layer errors are NOT runtime failures.
//
//    PlanError and CursorPlanError must be translated into
//    executor/query invariants via the canonical mapping functions.
//    Do not leak plan-layer error types across execution boundaries.
//
// 6. Preserve taxonomy stability.
//
//    Do NOT:
//      • Merge error classes.
//      • Reclassify corruption as internal.
//      • Downgrade invariant violations.
//      • Introduce ambiguous class/origin combinations.
//
//    Any change to ErrorClass or ErrorOrigin is an architectural change
//    and must be reviewed accordingly.
//
// -----------------------------------------------------------------------------
// NON-GOALS
// -----------------------------------------------------------------------------
//
// This is NOT:
//
//   • A public API contract.
//   • A generic error abstraction layer.
//   • A feature-specific message builder.
//   • A dumping ground for temporary error conversions.
//
// -----------------------------------------------------------------------------
// MAINTENANCE GUIDELINES
// -----------------------------------------------------------------------------
//
// When modifying this file:
//
//   1. Ensure classification semantics remain consistent.
//   2. Avoid constructor proliferation.
//   3. Prefer narrow, origin-specific helpers over ad-hoc new(...).
//   4. Keep formatting minimal and standardized.
//   5. Keep this file boring and stable.
//
// If this file grows rapidly, something is wrong at the call sites.
//
// ============================================================================

///
/// InternalError
///
/// Structured runtime error with a stable internal classification.
/// Not a stable API; intended for internal use and may change without notice.
///

#[derive(Debug, ThisError)]
#[error("{message}")]
pub struct InternalError {
    pub(crate) class: ErrorClass,
    pub(crate) origin: ErrorOrigin,
    pub(crate) message: String,

    /// Optional structured error detail.
    /// The variant (if present) must correspond to `origin`.
    pub(crate) detail: Option<ErrorDetail>,
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

    /// Return the internal error class taxonomy.
    #[must_use]
    pub const fn class(&self) -> ErrorClass {
        self.class
    }

    /// Return the internal error origin taxonomy.
    #[must_use]
    pub const fn origin(&self) -> ErrorOrigin {
        self.origin
    }

    /// Return the rendered internal error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Return the optional structured detail payload.
    #[must_use]
    pub const fn detail(&self) -> Option<&ErrorDetail> {
        self.detail.as_ref()
    }

    /// Consume and return the rendered internal error message.
    #[must_use]
    pub fn into_message(self) -> String {
        self.message
    }

    /// Construct an error while preserving an explicit class/origin taxonomy pair.
    pub(crate) fn classified(
        class: ErrorClass,
        origin: ErrorOrigin,
        message: impl Into<String>,
    ) -> Self {
        Self::new(class, origin, message)
    }

    /// Rebuild this error with a new message while preserving class/origin taxonomy.
    pub(crate) fn with_message(self, message: impl Into<String>) -> Self {
        Self::classified(self.class, self.origin, message)
    }

    /// Rebuild this error with a new origin while preserving class/message.
    ///
    /// Origin-scoped detail payloads are intentionally dropped when re-origining.
    pub(crate) fn with_origin(self, origin: ErrorOrigin) -> Self {
        Self::classified(self.class, origin, self.message)
    }

    /// Construct an index-origin invariant violation.
    pub(crate) fn index_invariant(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Index,
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

    /// Construct a store-origin internal error.
    pub(crate) fn store_internal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Store, message.into())
    }

    /// Construct an index-origin internal error.
    pub(crate) fn index_internal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Index, message.into())
    }

    /// Construct a query-origin internal error.
    #[cfg(test)]
    pub(crate) fn query_internal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Query, message.into())
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

    /// Construct an identity-origin corruption error.
    pub(crate) fn identity_corruption(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Corruption,
            ErrorOrigin::Identity,
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

    /// Construct a serialize-origin unsupported error.
    pub(crate) fn serialize_unsupported(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Serialize,
            message.into(),
        )
    }

    /// Construct a cursor-origin unsupported error.
    pub(crate) fn cursor_unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Unsupported, ErrorOrigin::Cursor, message.into())
    }

    /// Construct a serialize-origin incompatible persisted-format error.
    pub(crate) fn serialize_incompatible_persisted_format(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::IncompatiblePersistedFormat,
            ErrorOrigin::Serialize,
            message.into(),
        )
    }

    /// Construct a query-origin unsupported error preserving one SQL parser
    /// unsupported-feature label in structured error detail.
    #[cfg(feature = "sql")]
    pub(crate) fn query_unsupported_sql_feature(feature: &'static str) -> Self {
        let message = format!(
            "SQL query is not executable in this release: unsupported SQL feature: {feature}"
        );

        Self {
            class: ErrorClass::Unsupported,
            origin: ErrorOrigin::Query,
            message,
            detail: Some(ErrorDetail::Query(
                QueryErrorDetail::UnsupportedSqlFeature { feature },
            )),
        }
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

    /// Construct an index-plan corruption error for index-origin failures.
    pub(crate) fn index_plan_index_corruption(message: impl Into<String>) -> Self {
        Self::index_plan_corruption(ErrorOrigin::Index, message)
    }

    /// Construct an index-plan corruption error for store-origin failures.
    pub(crate) fn index_plan_store_corruption(message: impl Into<String>) -> Self {
        Self::index_plan_corruption(ErrorOrigin::Store, message)
    }

    /// Construct an index-plan corruption error for serialize-origin failures.
    pub(crate) fn index_plan_serialize_corruption(message: impl Into<String>) -> Self {
        Self::index_plan_corruption(ErrorOrigin::Serialize, message)
    }

    /// Construct an index-plan invariant violation error with a canonical prefix.
    pub(crate) fn index_plan_invariant(origin: ErrorOrigin, message: impl Into<String>) -> Self {
        let message = message.into();
        Self::new(
            ErrorClass::InvariantViolation,
            origin,
            format!("invariant violation detected ({origin}): {message}"),
        )
    }

    /// Construct an index-plan invariant violation error for store-origin failures.
    pub(crate) fn index_plan_store_invariant(message: impl Into<String>) -> Self {
        Self::index_plan_invariant(ErrorOrigin::Store, message)
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
    Query(QueryErrorDetail),
    // Future-proofing:
    // #[error("{0}")]
    // Index(IndexError),
    //
    // #[error("{0}")]
    // Executor(ExecutorErrorDetail),
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
/// QueryErrorDetail
///
/// Query-origin structured error detail payload.
///

#[derive(Debug, ThisError)]
pub enum QueryErrorDetail {
    #[error("unsupported SQL feature: {feature}")]
    UnsupportedSqlFeature { feature: &'static str },
}

///
/// ErrorClass
/// Internal error taxonomy for runtime classification.
/// Not a stable API; may change without notice.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorClass {
    Corruption,
    IncompatiblePersistedFormat,
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
            Self::IncompatiblePersistedFormat => "incompatible_persisted_format",
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
    Identity,
    Query,
    Planner,
    Cursor,
    Recovery,
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
            Self::Identity => "identity",
            Self::Query => "query",
            Self::Planner => "planner",
            Self::Cursor => "cursor",
            Self::Recovery => "recovery",
            Self::Response => "response",
            Self::Executor => "executor",
            Self::Interface => "interface",
        };
        write!(f, "{label}")
    }
}
