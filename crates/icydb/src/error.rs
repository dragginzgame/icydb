use candid::CandidType;
use derive_more::Display;
use icydb_core::{
    db::{
        query::{QueryError, plan::PlanError},
        response::ResponseError,
    },
    error::{ErrorOrigin as CoreErrorOrigin, InternalError},
    patch::MergePatchError as CoreMergePatchError,
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

    pub(crate) fn from_merge_patch_error(err: CoreMergePatchError) -> Self {
        let message = err.to_string();
        let patch_error = PatchError::from_merge_patch_error(err);
        Self::new(
            ErrorKind::Update(UpdateErrorKind::Patch(patch_error)),
            ErrorOrigin::Interface,
            message,
        )
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

            QueryError::Plan(PlanError::UnorderedPagination) => Self::new(
                ErrorKind::Query(QueryErrorKind::UnorderedPagination),
                ErrorOrigin::Query,
                err.to_string(),
            ),

            QueryError::Plan(_) => Self::new(
                ErrorKind::Query(QueryErrorKind::Plan),
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
    /// Predicate/model validation failed.
    Validate,

    /// Query intent contract was violated before planning.
    Intent,

    /// Planning rejected the query.
    Plan,

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

impl PatchError {
    pub(crate) fn from_merge_patch_error(err: CoreMergePatchError) -> Self {
        match err {
            CoreMergePatchError::InvalidShape { .. } => Self::InvalidShape,
            CoreMergePatchError::MissingKey { .. } => Self::MissingKey,
            CoreMergePatchError::CardinalityViolation { .. } => Self::CardinalityViolation,
            CoreMergePatchError::Context { source, .. } => Self::from_merge_patch_error(*source),
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

#[cfg(test)]
mod tests {
    use super::*;
    use icydb_core::db::query::{IntentError, QueryError, predicate::ValidateError};

    #[test]
    fn query_validate_maps_to_validate_kind() {
        let err = QueryError::Validate(ValidateError::UnknownField {
            field: "field".to_string(),
        });
        let facade = Error::from(err);

        assert_eq!(facade.kind, ErrorKind::Query(QueryErrorKind::Validate));
        assert_eq!(facade.origin, ErrorOrigin::Query);
    }

    #[test]
    fn query_intent_maps_to_intent_kind() {
        let err = QueryError::Intent(IntentError::DeleteLimitRequiresOrder);
        let facade = Error::from(err);

        assert_eq!(facade.kind, ErrorKind::Query(QueryErrorKind::Intent));
        assert_eq!(facade.origin, ErrorOrigin::Query);
    }

    #[test]
    fn plan_unordered_pagination_maps_to_dedicated_kind() {
        let err = QueryError::Plan(PlanError::UnorderedPagination);
        let facade = Error::from(err);

        assert_eq!(
            facade.kind,
            ErrorKind::Query(QueryErrorKind::UnorderedPagination),
        );
        assert_eq!(facade.origin, ErrorOrigin::Query);
    }

    #[test]
    fn plan_errors_map_to_plan_kind() {
        let err = QueryError::Plan(PlanError::EmptyOrderSpec);
        let facade = Error::from(err);

        assert_eq!(facade.kind, ErrorKind::Query(QueryErrorKind::Plan));
        assert_eq!(facade.origin, ErrorOrigin::Query);
    }

    #[test]
    fn response_error_maps_with_response_origin() {
        let facade = Error::from(ResponseError::NotFound { entity: "Entity" });

        assert_eq!(facade.kind, ErrorKind::Query(QueryErrorKind::NotFound));
        assert_eq!(facade.origin, ErrorOrigin::Response);
    }
}
