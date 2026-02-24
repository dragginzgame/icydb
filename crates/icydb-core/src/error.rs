use crate::{
    db::query::{
        plan::{CursorPlanError, PlanError},
        policy::PlanPolicyError,
    },
    patch::MergePatchError,
};
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

    /// Construct a query-origin invariant violation.
    pub(crate) fn query_invariant(message: impl Into<String>) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            message.into(),
        )
    }

    /// Build the canonical executor-invariant message prefix.
    #[must_use]
    pub(crate) fn executor_invariant_message(reason: impl Into<String>) -> String {
        format!("executor invariant violated: {}", reason.into())
    }

    /// Build the canonical invalid-logical-plan message prefix.
    #[must_use]
    pub(crate) fn invalid_logical_plan_message(reason: impl Into<String>) -> String {
        format!("invalid logical plan: {}", reason.into())
    }

    /// Construct a query-origin invariant with the canonical executor prefix.
    pub(crate) fn query_executor_invariant(reason: impl Into<String>) -> Self {
        Self::query_invariant(Self::executor_invariant_message(reason))
    }

    /// Construct a query-origin invariant with the canonical invalid-plan prefix.
    pub(crate) fn query_invalid_logical_plan(reason: impl Into<String>) -> Self {
        Self::query_invariant(Self::invalid_logical_plan_message(reason))
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
                    Self::executor_invariant_message(format!(
                        "pk-ordered continuation boundary must contain exactly 1 slot, found {found}"
                    ))
                }
                CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                    value: None, ..
                } => Self::executor_invariant_message("pk cursor slot must be present"),
                CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                    value: Some(_),
                    ..
                } => Self::executor_invariant_message("pk cursor slot type mismatch"),
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

    /// Map plan-shape policy variants into executor-boundary invariants without
    /// string-based conversion paths.
    pub(crate) fn plan_invariant_violation(err: PlanPolicyError) -> Self {
        let reason = match err {
            PlanPolicyError::EmptyOrderSpec => {
                "order specification must include at least one field"
            }
            PlanPolicyError::DeletePlanWithPagination => "delete plans must not include pagination",
            PlanPolicyError::LoadPlanWithDeleteLimit => "load plans must not carry delete limits",
            PlanPolicyError::DeleteLimitRequiresOrder => "delete limit requires explicit ordering",
            PlanPolicyError::UnorderedPagination => "pagination requires explicit ordering",
        };

        Self::query_executor_invariant(reason)
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::query::plan::{CursorPlanError, PlanError};

    #[test]
    fn index_plan_index_corruption_uses_index_origin() {
        let err = InternalError::index_plan_index_corruption("broken key payload");
        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Index);
        assert_eq!(
            err.message,
            "corruption detected (index): broken key payload"
        );
    }

    #[test]
    fn index_plan_store_corruption_uses_store_origin() {
        let err = InternalError::index_plan_store_corruption("row/key mismatch");
        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert_eq!(err.message, "corruption detected (store): row/key mismatch");
    }

    #[test]
    fn index_plan_serialize_corruption_uses_serialize_origin() {
        let err = InternalError::index_plan_serialize_corruption("decode failed");
        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Serialize);
        assert_eq!(
            err.message,
            "corruption detected (serialize): decode failed"
        );
    }

    #[test]
    fn query_executor_invariant_uses_invariant_violation_class() {
        let err = InternalError::query_executor_invariant("route contract mismatch");
        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Query);
    }

    #[test]
    fn executor_plan_error_mapping_stays_invariant_violation() {
        let plan_err = PlanError::from(CursorPlanError::InvalidContinuationCursorPayload {
            reason: "bad token".to_string(),
        });
        let err = InternalError::from_executor_plan_error(plan_err);
        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Query);
    }

    #[test]
    fn plan_policy_error_mapping_uses_executor_invariant_prefix() {
        let err =
            InternalError::plan_invariant_violation(PlanPolicyError::DeleteLimitRequiresOrder);
        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Query);
        assert_eq!(
            err.message,
            "executor invariant violated: delete limit requires explicit ordering",
        );
    }
}
