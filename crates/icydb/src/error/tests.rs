//! Module: error::tests
//!
//! Responsibility: module boundary tests.
//! Does not own: production implementation or public API ownership.
//! Boundary: verifies facade contracts through local module behavior.

use super::*;
use candid::{
    Decode, Encode,
    types::{CandidType, Label, Type, TypeInner},
};
use ic_memory::{RuntimeBootstrapError, RuntimeStateError};
#[cfg(feature = "sql")]
use icydb_core::db::{PlanError, QueryExecutionError, ValidateError};
use icydb_core::error::{ErrorClass as CoreErrorClass, ErrorOrigin as CoreErrorOrigin};
use serde::Serialize;

#[derive(CandidType, Serialize)]
struct DiagnosticFactWire {
    tag: u8,
    value: u64,
}

#[derive(CandidType, Serialize)]
struct ErrorWire {
    code: u16,
    class: u8,
    origin: u8,
    facts: Vec<DiagnosticFactWire>,
}

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
#[cfg(feature = "sql")]
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
#[cfg(feature = "sql")]
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
#[cfg(feature = "sql")]
fn plan_errors_map_to_plan_kind() {
    let err = QueryError::Plan(Box::new(PlanError::from(ValidateError::UnknownField {
        field: "field".to_string(),
    })));
    let facade = Error::from(err);

    assert_eq!(facade.code(), icydb_diagnostic_code::ErrorCode::QUERY_PLAN);
    assert_eq!(facade.origin(), ErrorOrigin::Query);
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
fn public_operational_controller_boundary_is_typed_and_interface_owned() {
    let facade = Error::from_runtime_boundary(
        icydb_diagnostic_code::RuntimeBoundaryCode::OperationalSurfaceControllerRequired,
        ErrorOrigin::Interface,
    );

    assert_eq!(
        facade.code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_OPERATIONAL_SURFACE_CONTROLLER_REQUIRED,
    );
    assert_eq!(
        facade.class(),
        icydb_diagnostic_code::ErrorClass::Unsupported,
    );
    assert_eq!(facade.origin(), ErrorOrigin::Interface);
    assert_eq!(
        facade.diagnostic().detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary:
                icydb_diagnostic_code::RuntimeBoundaryCode::OperationalSurfaceControllerRequired,
        }),
    );
}

#[test]
fn public_error_runtime_corruption_boundary_preserves_its_broad_code() {
    let facade = Error::from_runtime_boundary(
        icydb_diagnostic_code::RuntimeBoundaryCode::PersistedRowLayoutOutsideAcceptedWindow,
        ErrorOrigin::Serialize,
    );

    assert_eq!(
        facade.code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_PERSISTED_ROW_LAYOUT_OUTSIDE_ACCEPTED_WINDOW,
    );
    assert_eq!(
        facade.class(),
        icydb_diagnostic_code::ErrorClass::Corruption
    );
    assert_eq!(facade.origin(), ErrorOrigin::Serialize);
    let diagnostic = facade.diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeCorruption,
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary:
                icydb_diagnostic_code::RuntimeBoundaryCode::PersistedRowLayoutOutsideAcceptedWindow,
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
#[cfg(feature = "sql")]
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
#[cfg(feature = "sql")]
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
    let mut fields = expect_record_fields(Error::ty());
    fields.sort();

    assert_eq!(fields, ["class", "code", "facts", "origin"]);
}

#[test]
fn diagnostic_fact_candid_shape_is_stable() {
    let mut fields = expect_record_fields(DiagnosticFact::ty());
    fields.sort();

    assert_eq!(fields, ["tag", "value"]);
}

#[test]
fn public_error_candid_preserves_bounded_numeric_facts() {
    let bytes = Encode!(&ErrorWire {
        code: icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MUTATION_BATCH_TOO_MANY_ITEMS
            .raw(),
        class: icydb_diagnostic_code::ErrorClass::Unsupported.wire_code(),
        origin: icydb_diagnostic_code::ErrorOrigin::Executor.wire_code(),
        facts: vec![
            DiagnosticFactWire {
                tag: icydb_diagnostic_code::DiagnosticFactTag::ActualCount.raw(),
                value: 5_000,
            },
            DiagnosticFactWire {
                tag: icydb_diagnostic_code::DiagnosticFactTag::Limit.raw(),
                value: 4_096,
            },
        ],
    })
    .expect("numeric public error facts should encode");
    let error = Decode!(bytes.as_slice(), Error).expect("numeric public error should decode");

    assert_eq!(
        error.code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MUTATION_BATCH_TOO_MANY_ITEMS,
    );
    assert_eq!(
        error.facts(),
        &[
            DiagnosticFact {
                tag: icydb_diagnostic_code::DiagnosticFactTag::ActualCount.raw(),
                value: 5_000,
            },
            DiagnosticFact {
                tag: icydb_diagnostic_code::DiagnosticFactTag::Limit.raw(),
                value: 4_096,
            },
        ],
    );
}

#[test]
fn core_numeric_facts_cross_the_public_facade_once_in_order() {
    let diagnostic = icydb_diagnostic_code::Diagnostic::new(
        icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
        icydb_diagnostic_code::ErrorOrigin::Cursor,
        None,
    );
    let error = Error::from_diagnostic_and_facts(
        diagnostic,
        vec![
            (icydb_diagnostic_code::DiagnosticFactTag::ExpectedOffset, 4),
            (icydb_diagnostic_code::DiagnosticFactTag::ActualOffset, 2),
        ],
    );

    assert_eq!(
        error.facts(),
        &[
            DiagnosticFact {
                tag: icydb_diagnostic_code::DiagnosticFactTag::ExpectedOffset.raw(),
                value: 4,
            },
            DiagnosticFact {
                tag: icydb_diagnostic_code::DiagnosticFactTag::ActualOffset.raw(),
                value: 2,
            },
        ],
    );
}

#[test]
fn malformed_numeric_facts_fail_compactly_at_the_public_facade() {
    let diagnostic = icydb_diagnostic_code::Diagnostic::new(
        icydb_diagnostic_code::DiagnosticCode::QuerySqlWriteBoundary,
        icydb_diagnostic_code::ErrorOrigin::Query,
        Some(icydb_diagnostic_code::DiagnosticDetail::SqlWriteBoundary {
            boundary: icydb_diagnostic_code::SqlWriteBoundaryCode::UnknownReturningField,
        }),
    );
    let error = Error::from_diagnostic_and_facts(
        diagnostic,
        vec![(icydb_diagnostic_code::DiagnosticFactTag::Limit, 1)],
    );

    assert_eq!(
        error.code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_INVARIANT_VIOLATION
    );
    assert_eq!(error.origin(), ErrorOrigin::Query);
    assert!(error.facts().is_empty());
}

#[test]
fn database_bootstrap_preserves_typed_cause_until_public_projection() {
    let cause: RuntimeBootstrapError<std::convert::Infallible> =
        RuntimeBootstrapError::State(RuntimeStateError::ReentrantAccess);
    let bootstrap = crate::db::DatabaseBootstrapError::from(cause);
    assert!(matches!(
        bootstrap.cause(),
        RuntimeBootstrapError::State(RuntimeStateError::ReentrantAccess)
    ));

    let facade = Error::from(bootstrap);
    assert_eq!(
        facade.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInternal
    );
    assert_eq!(facade.origin(), ErrorOrigin::Runtime);
}
