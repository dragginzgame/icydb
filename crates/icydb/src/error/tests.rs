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
fn query_validate_exposes_compact_diagnostic_bridge() {
    let err = QueryError::Validate(Box::new(ValidateError::UnknownField {
        field: "field".to_string(),
    }));
    let facade = Error::from(err);
    let diagnostic = facade.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryValidate
    );
    assert_eq!(diagnostic.class(), icydb_diagnostic_code::ErrorClass::Query);
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Query
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::QueryKind {
            kind: icydb_diagnostic_code::QueryErrorKind::Validate,
        })
    );
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
fn response_error_preserves_origin_in_compact_diagnostic_bridge() {
    let facade = Error::from(ResponseError::NotFound { entity: "Entity" });
    let diagnostic = facade.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryNotFound
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Response
    );
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
fn runtime_error_exposes_compact_diagnostic_bridge() {
    let facade = Error::from(InternalError::new(
        CoreErrorClass::Unsupported,
        CoreErrorOrigin::Query,
        "unsupported query path",
    ));
    let diagnostic = facade.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeUnsupported
    );
    assert_eq!(
        diagnostic.class(),
        icydb_diagnostic_code::ErrorClass::Unsupported
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Query
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::RuntimeKind {
            kind: icydb_diagnostic_code::RuntimeErrorKind::Unsupported,
        })
    );
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
