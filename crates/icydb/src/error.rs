use candid::CandidType;
use icydb_core::{
    db::{QueryError, QueryExecutionError, ResponseError},
    error::{ErrorClass as CoreErrorClass, ErrorOrigin as CoreErrorOrigin, InternalError},
};
use serde::Deserialize;
use thiserror::Error as ThisError;

#[cfg_attr(doc, doc = "Error\n\nPublic error payload.")]
#[derive(CandidType, Debug, Deserialize, ThisError)]
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
#[serde(rename_all = "PascalCase")]
pub enum ErrorKind {
    Query(QueryErrorKind),

    /// Runtime failure.
    Runtime(RuntimeErrorKind),
}

#[cfg_attr(doc, doc = "RuntimeErrorKind\n\nPublic runtime error class.")]
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
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
#[serde(rename_all = "PascalCase")]
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

//
// TESTS
//

#[cfg(test)]
mod tests {
    use super::*;
    use candid::types::{CandidType, Label, Type, TypeInner};
    use icydb_core::db::{IntentError, PlanError, ValidateError};
    use icydb_core::error::{ErrorClass as CoreErrorClass, ErrorOrigin as CoreErrorOrigin};

    fn expect_record_fields(ty: Type) -> Vec<String> {
        match ty.as_ref() {
            TypeInner::Record(fields) => fields
                .iter()
                .map(|field| match field.id.as_ref() {
                    Label::Named(name) => name.clone(),
                    other => panic!("expected named record field, got {other:?}"),
                })
                .collect(),
            other => panic!("expected candid record, got {other:?}"),
        }
    }

    fn expect_variant_labels(ty: Type) -> Vec<String> {
        match ty.as_ref() {
            TypeInner::Variant(fields) => fields
                .iter()
                .map(|field| match field.id.as_ref() {
                    Label::Named(name) => name.clone(),
                    other => panic!("expected named variant label, got {other:?}"),
                })
                .collect(),
            other => panic!("expected candid variant, got {other:?}"),
        }
    }

    #[test]
    fn query_validate_maps_to_validate_kind() {
        let err = QueryError::Validate(Box::new(ValidateError::UnknownField {
            field: "field".to_string(),
        }));
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
                CoreErrorClass::IncompatiblePersistedFormat,
                RuntimeErrorKind::IncompatiblePersistedFormat,
            ),
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
            (
                CoreErrorClass::Unsupported,
                CoreErrorOrigin::Query,
                RuntimeErrorKind::Unsupported,
                ErrorOrigin::Query,
                "unsupported SQL feature",
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
            (
                CoreErrorClass::IncompatiblePersistedFormat,
                CoreErrorOrigin::Serialize,
                RuntimeErrorKind::IncompatiblePersistedFormat,
                ErrorOrigin::Serialize,
                "incompatible persisted format",
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
    fn error_struct_candid_shape_is_stable() {
        let fields = expect_record_fields(Error::ty());

        for field in ["kind", "origin", "message"] {
            assert!(
                fields.iter().any(|candidate| candidate == field),
                "Error must keep `{field}` as Candid field key",
            );
        }
    }

    #[test]
    fn error_kind_candid_shape_is_stable() {
        let labels = expect_variant_labels(ErrorKind::ty());
        assert!(
            labels.iter().any(|candidate| candidate == "Runtime"),
            "ErrorKind must keep `Runtime` variant label",
        );
    }

    #[test]
    fn runtime_error_and_origin_variant_labels_are_stable() {
        let runtime_labels = expect_variant_labels(RuntimeErrorKind::ty());
        assert!(
            runtime_labels
                .iter()
                .any(|candidate| candidate == "InvariantViolation"),
            "RuntimeErrorKind must keep `InvariantViolation` variant label",
        );

        let origin_labels = expect_variant_labels(ErrorOrigin::ty());
        assert!(
            origin_labels
                .iter()
                .any(|candidate| candidate == "Serialize"),
            "ErrorOrigin must keep `Serialize` variant label",
        );
    }

    #[test]
    fn query_error_kind_variant_labels_are_stable() {
        let labels = expect_variant_labels(QueryErrorKind::ty());

        for label in [
            "Validate",
            "Intent",
            "Plan",
            "UnorderedPagination",
            "InvalidContinuationCursor",
            "NotFound",
            "NotUnique",
        ] {
            assert!(
                labels.iter().any(|candidate| candidate == label),
                "QueryErrorKind must keep `{label}` variant label",
            );
        }
    }
}
