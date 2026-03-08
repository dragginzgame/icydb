use candid::CandidType;
use derive_more::Display;
use icydb_core::{
    db::{QueryError, QueryExecutionError, ResponseError},
    error::{ErrorClass as CoreErrorClass, ErrorOrigin as CoreErrorOrigin, InternalError},
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
#[serde(rename_all = "snake_case")]
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

    pub(crate) fn from_merge_patch_error(err: CoreMergePatchError) -> Self {
        let message = err.to_string();
        let patch_error = PatchError::from_merge_patch_error(err);
        Self::new(
            ErrorKind::Update(UpdateErrorKind::Patch(patch_error)),
            ErrorOrigin::Interface,
            message,
        )
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

            QueryError::Plan(_) => Self::new(
                ErrorKind::Query(QueryErrorKind::Plan),
                ErrorOrigin::Query,
                err.to_string(),
            ),

            QueryError::Response(err) => Self::from_response_error(err),

            QueryError::Execute(err) => match err {
                QueryExecutionError::Corruption(inner)
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

///
/// ErrorKind
/// Public error taxonomy for callers and canister interfaces.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum ErrorKind {
    Query(QueryErrorKind),
    Update(UpdateErrorKind),

    /// Runtime failure preserving the core semantic error class.
    Runtime(RuntimeErrorKind),
}

///
/// RuntimeErrorKind
/// Public runtime class taxonomy mirrored from `icydb-core::ErrorClass`.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum RuntimeErrorKind {
    Corruption,
    InvariantViolation,
    Conflict,
    NotFound,
    Unsupported,
    Internal,
}

///
/// QueryErrorKind
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum QueryErrorKind {
    /// Predicate/model validation failed.
    Validate,

    /// Query intent contract was violated before planning.
    Intent,

    /// Planning rejected the query.
    Plan,

    /// Pagination requires ordering but none was provided.
    UnorderedPagination,

    /// Continuation cursor token was invalid for this query.
    InvalidContinuationCursor,

    /// Valid query, but no rows matched.
    NotFound,

    /// Query expected one row but matched many.
    NotUnique,
}

///
/// UpdateErrorKind
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum UpdateErrorKind {
    /// Patch application failed for semantic reasons.
    Patch(PatchError),
}

///
/// PatchError
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum PatchError {
    InvalidShape,
    CardinalityViolation,
}

impl PatchError {
    pub(crate) fn from_merge_patch_error(err: CoreMergePatchError) -> Self {
        match err {
            CoreMergePatchError::InvalidShape { .. } => Self::InvalidShape,
            CoreMergePatchError::CardinalityViolation { .. } => Self::CardinalityViolation,
            CoreMergePatchError::Context { source, .. } => Self::from_merge_patch_error(*source),
        }
    }
}

///
/// ErrorOrigin
/// Public origin taxonomy for callers and canister interfaces.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Display, Eq, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb_core::db::{IntentError, PlanError, ValidateError};
    use icydb_core::error::{ErrorClass as CoreErrorClass, ErrorOrigin as CoreErrorOrigin};
    use serde::Serialize;
    use serde_cbor::Value as CborValue;
    use std::collections::BTreeMap;

    fn to_cbor_value<T: Serialize>(value: &T) -> CborValue {
        let bytes =
            serde_cbor::to_vec(value).expect("test fixtures must serialize into CBOR payloads");
        serde_cbor::from_slice::<CborValue>(&bytes)
            .expect("test fixtures must deserialize into CBOR value trees")
    }

    fn expect_cbor_map(value: &CborValue) -> &BTreeMap<CborValue, CborValue> {
        match value {
            CborValue::Map(map) => map,
            other => panic!("expected CBOR map, got {other:?}"),
        }
    }

    fn map_field<'a>(map: &'a BTreeMap<CborValue, CborValue>, key: &str) -> Option<&'a CborValue> {
        map.get(&CborValue::Text(key.to_string()))
    }

    #[test]
    fn query_validate_maps_to_validate_kind() {
        let err = QueryError::Validate(ValidateError::UnknownField {
            field: "field".to_string(),
        });
        let facade = Error::from(err);

        assert_eq!(facade.kind(), &ErrorKind::Query(QueryErrorKind::Validate));
        assert_eq!(facade.origin(), ErrorOrigin::Query);
    }

    #[test]
    fn query_intent_maps_to_intent_kind() {
        let err = QueryError::Intent(IntentError::ByIdsWithPredicate);
        let facade = Error::from(err);

        assert_eq!(facade.kind(), &ErrorKind::Query(QueryErrorKind::Intent));
        assert_eq!(facade.origin(), ErrorOrigin::Query);
    }

    #[test]
    fn plan_errors_map_to_plan_kind() {
        let err = QueryError::Plan(Box::new(PlanError::from(ValidateError::UnknownField {
            field: "field".to_string(),
        })));
        let facade = Error::from(err);

        assert_eq!(facade.kind(), &ErrorKind::Query(QueryErrorKind::Plan));
        assert_eq!(facade.origin(), ErrorOrigin::Query);
    }

    #[test]
    fn response_error_maps_with_response_origin() {
        let facade = Error::from(ResponseError::NotFound { entity: "Entity" });

        assert_eq!(facade.kind(), &ErrorKind::Query(QueryErrorKind::NotFound));
        assert_eq!(facade.origin(), ErrorOrigin::Response);
    }

    #[test]
    fn internal_error_class_matrix_maps_to_runtime_kind_and_preserves_origin() {
        let cases = [
            (CoreErrorClass::Corruption, RuntimeErrorKind::Corruption),
            (
                CoreErrorClass::InvariantViolation,
                RuntimeErrorKind::InvariantViolation,
            ),
            (CoreErrorClass::Conflict, RuntimeErrorKind::Conflict),
            (CoreErrorClass::NotFound, RuntimeErrorKind::NotFound),
            (CoreErrorClass::Unsupported, RuntimeErrorKind::Unsupported),
            (CoreErrorClass::Internal, RuntimeErrorKind::Internal),
        ];

        for (class, expected_kind) in cases {
            let core_err = InternalError::new(class, CoreErrorOrigin::Index, "runtime failure");
            let facade = Error::from(core_err);

            assert_eq!(facade.kind(), &ErrorKind::Runtime(expected_kind));
            assert_eq!(facade.origin(), ErrorOrigin::Index);
        }
    }

    #[test]
    fn query_execute_preserves_runtime_class_and_origin() {
        let cases = [
            (
                CoreErrorClass::Conflict,
                CoreErrorOrigin::Store,
                RuntimeErrorKind::Conflict,
                ErrorOrigin::Store,
                "write conflict",
            ),
            (
                CoreErrorClass::NotFound,
                CoreErrorOrigin::Executor,
                RuntimeErrorKind::NotFound,
                ErrorOrigin::Executor,
                "row missing",
            ),
            (
                CoreErrorClass::Internal,
                CoreErrorOrigin::Planner,
                RuntimeErrorKind::Internal,
                ErrorOrigin::Planner,
                "planner internal",
            ),
        ];

        for (class, origin, expected_kind, expected_origin, message) in cases {
            let query_err = QueryError::Execute(QueryExecutionError::from(InternalError::new(
                class, origin, message,
            )));
            let facade = Error::from(query_err);

            assert_eq!(facade.kind(), &ErrorKind::Runtime(expected_kind));
            assert_eq!(facade.origin(), expected_origin);
        }
    }

    #[test]
    fn query_execute_storage_and_index_origins_map_to_runtime_contract() {
        let cases = [
            (
                CoreErrorClass::Internal,
                CoreErrorOrigin::Store,
                RuntimeErrorKind::Internal,
                ErrorOrigin::Store,
                "store internal",
            ),
            (
                CoreErrorClass::Corruption,
                CoreErrorOrigin::Index,
                RuntimeErrorKind::Corruption,
                ErrorOrigin::Index,
                "index corruption",
            ),
            (
                CoreErrorClass::Unsupported,
                CoreErrorOrigin::Store,
                RuntimeErrorKind::Unsupported,
                ErrorOrigin::Store,
                "store unsupported",
            ),
        ];

        for (class, origin, expected_kind, expected_origin, message) in cases {
            let query_err = QueryError::Execute(QueryExecutionError::from(InternalError::new(
                class, origin, message,
            )));
            let facade = Error::from(query_err);

            assert_eq!(facade.kind(), &ErrorKind::Runtime(expected_kind));
            assert_eq!(facade.origin(), expected_origin);
        }
    }

    #[test]
    fn origin_mapping_includes_new_core_domains() {
        let cases = [
            (CoreErrorOrigin::Cursor, ErrorOrigin::Cursor),
            (CoreErrorOrigin::Planner, ErrorOrigin::Planner),
            (CoreErrorOrigin::Recovery, ErrorOrigin::Recovery),
            (CoreErrorOrigin::Identity, ErrorOrigin::Identity),
        ];

        for (origin, expected) in cases {
            let facade = Error::from(InternalError::new(
                CoreErrorClass::Internal,
                origin,
                "origin mapping",
            ));
            assert_eq!(facade.origin(), expected);
        }
    }

    #[test]
    fn error_struct_serialization_shape_is_stable() {
        let encoded = to_cbor_value(&Error::new(
            ErrorKind::Runtime(RuntimeErrorKind::Internal),
            ErrorOrigin::Executor,
            "runtime failure",
        ));
        let root = expect_cbor_map(&encoded);

        assert!(
            map_field(root, "kind").is_some(),
            "Error must keep `kind` as serialized field key",
        );
        assert!(
            map_field(root, "origin").is_some(),
            "Error must keep `origin` as serialized field key",
        );
        assert!(
            map_field(root, "message").is_some(),
            "Error must keep `message` as serialized field key",
        );
    }

    #[test]
    fn error_kind_serialization_shape_is_stable() {
        let encoded = to_cbor_value(&ErrorKind::Update(UpdateErrorKind::Patch(
            PatchError::InvalidShape,
        )));
        let root = expect_cbor_map(&encoded);
        let update_payload =
            map_field(root, "Update").expect("expected externally-tagged Update variant");
        let update_map = expect_cbor_map(update_payload);
        assert!(
            map_field(update_map, "Patch").is_some(),
            "Update payload must keep Patch variant key",
        );
    }

    #[test]
    fn runtime_error_and_origin_variant_labels_are_stable() {
        assert_eq!(
            to_cbor_value(&RuntimeErrorKind::InvariantViolation),
            CborValue::Text("InvariantViolation".to_string())
        );
        assert_eq!(
            to_cbor_value(&ErrorOrigin::Serialize),
            CborValue::Text("Serialize".to_string())
        );
    }

    #[test]
    fn query_error_kind_variant_labels_are_stable() {
        assert_eq!(
            to_cbor_value(&QueryErrorKind::Validate),
            CborValue::Text("Validate".to_string())
        );
        assert_eq!(
            to_cbor_value(&QueryErrorKind::Intent),
            CborValue::Text("Intent".to_string())
        );
        assert_eq!(
            to_cbor_value(&QueryErrorKind::Plan),
            CborValue::Text("Plan".to_string())
        );
        assert_eq!(
            to_cbor_value(&QueryErrorKind::UnorderedPagination),
            CborValue::Text("UnorderedPagination".to_string())
        );
        assert_eq!(
            to_cbor_value(&QueryErrorKind::InvalidContinuationCursor),
            CborValue::Text("InvalidContinuationCursor".to_string())
        );
        assert_eq!(
            to_cbor_value(&QueryErrorKind::NotFound),
            CborValue::Text("NotFound".to_string())
        );
        assert_eq!(
            to_cbor_value(&QueryErrorKind::NotUnique),
            CborValue::Text("NotUnique".to_string())
        );
    }

    #[test]
    fn patch_error_variant_labels_are_stable() {
        assert_eq!(
            to_cbor_value(&PatchError::InvalidShape),
            CborValue::Text("InvalidShape".to_string())
        );
        assert_eq!(
            to_cbor_value(&PatchError::CardinalityViolation),
            CborValue::Text("CardinalityViolation".to_string())
        );
    }

    #[test]
    fn update_error_kind_patch_payload_shape_is_stable() {
        let encoded = to_cbor_value(&UpdateErrorKind::Patch(PatchError::CardinalityViolation));
        let root = expect_cbor_map(&encoded);

        assert!(
            map_field(root, "Patch").is_some(),
            "UpdateErrorKind::Patch must keep `Patch` payload key",
        );
    }
}
