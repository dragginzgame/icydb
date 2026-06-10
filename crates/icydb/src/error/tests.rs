use super::*;
use candid::types::{CandidType, Label, Type, TypeInner};
use icydb_core::db::{IntentError, PlanError, QueryExecutionError, ValidateError};
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

#[test]
fn query_validate_maps_to_validate_kind() {
    let err = QueryError::Validate(Box::new(ValidateError::UnknownField {
        field: "field".to_string(),
    }));
    let facade = Error::from(err);

    assert_eq!(
        facade.code(),
        icydb_diagnostic_code::ErrorCode::QUERY_VALIDATE
    );
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

    assert_eq!(
        facade.code(),
        icydb_diagnostic_code::ErrorCode::QUERY_INTENT
    );
    assert_eq!(facade.origin(), ErrorOrigin::Query);
}

#[test]
fn plan_errors_map_to_plan_kind() {
    let err = QueryError::Plan(Box::new(PlanError::from(ValidateError::UnknownField {
        field: "field".to_string(),
    })));
    let facade = Error::from(err);

    assert_eq!(facade.code(), icydb_diagnostic_code::ErrorCode::QUERY_PLAN);
    assert_eq!(facade.origin(), ErrorOrigin::Query);
}

#[test]
fn response_error_maps_with_response_origin() {
    let facade = Error::from(ResponseError::NotFound { entity: "Entity" });

    assert_eq!(
        facade.code(),
        icydb_diagnostic_code::ErrorCode::QUERY_NOT_FOUND
    );
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
fn public_error_from_diagnostic_collapses_detail_to_leaf_code() {
    let diagnostic = icydb_diagnostic_code::Diagnostic::new(
        icydb_diagnostic_code::DiagnosticCode::SchemaDdlAdmission,
        icydb_diagnostic_code::ErrorOrigin::Query,
        Some(
            icydb_diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                reason: icydb_diagnostic_code::SchemaDdlAdmissionCode::PublicationRaceLost,
            },
        ),
    );
    let facade = Error::from_diagnostic(diagnostic);

    assert_eq!(
        facade.code(),
        icydb_diagnostic_code::ErrorCode::SCHEMA_DDL_PUBLICATION_RACE_LOST
    );
    assert_eq!(facade.class(), icydb_diagnostic_code::ErrorClass::Query);
    assert_eq!(facade.origin(), ErrorOrigin::Query);
    let diagnostic = facade.diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::SchemaDdlAdmission
    );
    assert_eq!(
        diagnostic.detail(),
        Some(
            &icydb_diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                reason: icydb_diagnostic_code::SchemaDdlAdmissionCode::PublicationRaceLost,
            }
        ),
    );
}

#[test]
fn public_error_runtime_boundary_collapses_detail_to_leaf_code() {
    let facade = Error::from_runtime_boundary(
        icydb_diagnostic_code::RuntimeBoundaryCode::SqlDdlTargetRequired,
        ErrorOrigin::Interface,
    );

    assert_eq!(
        facade.code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_SQL_DDL_TARGET_REQUIRED,
    );
    assert_eq!(
        facade.class(),
        icydb_diagnostic_code::ErrorClass::Unsupported
    );
    assert_eq!(facade.origin(), ErrorOrigin::Interface);
    let diagnostic = facade.diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeUnsupported,
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::SqlDdlTargetRequired,
        }),
    );
}

#[test]
fn public_error_sql_write_boundary_collapses_detail_to_leaf_code() {
    let diagnostic = icydb_diagnostic_code::Diagnostic::new(
        icydb_diagnostic_code::DiagnosticCode::QuerySqlWriteBoundary,
        icydb_diagnostic_code::ErrorOrigin::Query,
        Some(icydb_diagnostic_code::DiagnosticDetail::SqlWriteBoundary {
            boundary: icydb_diagnostic_code::SqlWriteBoundaryCode::MissingPrimaryKey,
        }),
    );
    let facade = Error::from_diagnostic(diagnostic);

    assert_eq!(
        facade.code(),
        icydb_diagnostic_code::ErrorCode::SQL_WRITE_MISSING_PRIMARY_KEY,
    );
    assert_eq!(
        facade.class(),
        icydb_diagnostic_code::ErrorClass::Unsupported
    );
    assert_eq!(facade.origin(), ErrorOrigin::Query);
    let diagnostic = facade.diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QuerySqlWriteBoundary,
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::SqlWriteBoundary {
            boundary: icydb_diagnostic_code::SqlWriteBoundaryCode::MissingPrimaryKey,
        }),
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
        let core_err = InternalError::new(class, CoreErrorOrigin::Index);
        let facade = Error::from(core_err);

        assert_eq!(facade.code(), expected_kind.diagnostic_code().error_code());
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
        ),
        (
            CoreErrorClass::NotFound,
            CoreErrorOrigin::Executor,
            RuntimeErrorKind::NotFound,
            ErrorOrigin::Executor,
        ),
        (
            CoreErrorClass::Internal,
            CoreErrorOrigin::Planner,
            RuntimeErrorKind::Internal,
            ErrorOrigin::Planner,
        ),
        (
            CoreErrorClass::Unsupported,
            CoreErrorOrigin::Query,
            RuntimeErrorKind::Unsupported,
            ErrorOrigin::Query,
        ),
    ];

    for (class, origin, expected_kind, expected_origin) in cases {
        let query_err =
            QueryError::Execute(QueryExecutionError::from(InternalError::new(class, origin)));
        let facade = Error::from(query_err);

        assert_eq!(facade.code(), expected_kind.diagnostic_code().error_code());
        assert_eq!(facade.origin(), expected_origin);
    }
}

#[test]
fn runtime_error_exposes_compact_diagnostic_bridge() {
    let facade = Error::from(InternalError::new(
        CoreErrorClass::Unsupported,
        CoreErrorOrigin::Query,
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
        }),
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
        ),
        (
            CoreErrorClass::Corruption,
            CoreErrorOrigin::Index,
            RuntimeErrorKind::Corruption,
            ErrorOrigin::Index,
        ),
        (
            CoreErrorClass::Unsupported,
            CoreErrorOrigin::Store,
            RuntimeErrorKind::Unsupported,
            ErrorOrigin::Store,
        ),
        (
            CoreErrorClass::IncompatiblePersistedFormat,
            CoreErrorOrigin::Serialize,
            RuntimeErrorKind::IncompatiblePersistedFormat,
            ErrorOrigin::Serialize,
        ),
    ];

    for (class, origin, expected_kind, expected_origin) in cases {
        let query_err =
            QueryError::Execute(QueryExecutionError::from(InternalError::new(class, origin)));
        let facade = Error::from(query_err);

        assert_eq!(facade.code(), expected_kind.diagnostic_code().error_code());
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
        let facade = Error::from(InternalError::new(CoreErrorClass::Internal, origin));
        assert_eq!(facade.origin(), expected);
    }
}

#[test]
fn error_struct_candid_shape_is_stable() {
    let fields = expect_record_fields(Error::ty());

    for field in ["code", "class", "origin"] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "Error must keep `{field}` as Candid field key",
        );
    }
    for removed in ["detail", "kind", "message"] {
        assert!(
            fields.iter().all(|candidate| candidate != removed),
            "Error compact wire shape must not keep legacy `{removed}` field",
        );
    }
}
