//! Typed errors preserve Rust causes and project into compact public diagnostics.

use crate::{
    Error, ErrorOrigin,
    db::{TypedAdapterError, TypedOperationError},
};
use icydb_diagnostic_code::{ErrorClass, ErrorCode, validate_known_diagnostic_fact_schema};

#[test]
fn typed_operation_projects_to_public_error() {
    let error: Error = TypedOperationError::Adapter(TypedAdapterError::StaleBinding).into();
    assert_eq!(error.class(), icydb_diagnostic_code::ErrorClass::Conflict);
    assert_eq!(error.origin(), ErrorOrigin::Interface);
}

#[test]
fn typed_adapter_causes_have_distinct_public_diagnostics_and_wire_payloads() {
    for (cause, code, class) in [
        (
            TypedAdapterError::BatchHandleMismatch,
            ErrorCode::RUNTIME_BOUNDARY_TYPED_ADAPTER_BATCH_HANDLE_MISMATCH,
            ErrorClass::Unsupported,
        ),
        (
            TypedAdapterError::BatchRowConsumed,
            ErrorCode::RUNTIME_BOUNDARY_TYPED_ADAPTER_BATCH_ROW_CONSUMED,
            ErrorClass::Conflict,
        ),
        (
            TypedAdapterError::EntityMismatch,
            ErrorCode::RUNTIME_BOUNDARY_TYPED_ADAPTER_ENTITY_MISMATCH,
            ErrorClass::Unsupported,
        ),
        (
            TypedAdapterError::FieldUnavailable,
            ErrorCode::RUNTIME_BOUNDARY_TYPED_ADAPTER_FIELD_UNAVAILABLE,
            ErrorClass::Unsupported,
        ),
        (
            TypedAdapterError::IncompatibleField,
            ErrorCode::RUNTIME_BOUNDARY_TYPED_ADAPTER_INCOMPATIBLE_FIELD,
            ErrorClass::Unsupported,
        ),
        (
            TypedAdapterError::RowFieldUnavailable,
            ErrorCode::RUNTIME_BOUNDARY_TYPED_ADAPTER_ROW_FIELD_UNAVAILABLE,
            ErrorClass::Unsupported,
        ),
        (
            TypedAdapterError::RowShapeMismatch,
            ErrorCode::RUNTIME_BOUNDARY_TYPED_ADAPTER_ROW_SHAPE_MISMATCH,
            ErrorClass::InvariantViolation,
        ),
        (
            TypedAdapterError::StaleBinding,
            ErrorCode::RUNTIME_BOUNDARY_TYPED_ADAPTER_STALE_BINDING,
            ErrorClass::Conflict,
        ),
        (
            TypedAdapterError::ValueShapeMismatch,
            ErrorCode::RUNTIME_BOUNDARY_TYPED_ADAPTER_VALUE_SHAPE_MISMATCH,
            ErrorClass::Unsupported,
        ),
    ] {
        for error in [
            Error::from(cause),
            Error::from(TypedOperationError::Adapter(cause)),
        ] {
            assert_eq!(error.code(), code);
            assert_eq!(error.class(), class);
            assert_eq!(error.origin(), ErrorOrigin::Interface);
            assert!(error.facts().is_empty());
            assert!(error.query_field().is_none());
            assert_eq!(validate_known_diagnostic_fact_schema(code, &[]), Ok(()));
            assert_public_error_frame(&error);
        }
    }
}

fn assert_public_error_frame(error: &Error) {
    use super::ErrorWire;
    use crate::__macro::RpcMethod;
    use icydb_diagnostic_code::{DiagnosticFactSchemaMismatch, DiagnosticFactTag};

    assert_eq!(
        validate_known_diagnostic_fact_schema(error.code(), &[]),
        Ok(())
    );
    assert_eq!(
        validate_known_diagnostic_fact_schema(error.code(), &[(DiagnosticFactTag::EntityTag, 7)]),
        Err(DiagnosticFactSchemaMismatch::CodeMaximumExceeded)
    );
    // Compare to the existing numeric error contract, rather than round-tripping
    // only the new producer through itself. No extra adapter field/string exists.
    let expected = ErrorWire {
        code: error.code().raw(),
        class: error.class().wire_code(),
        origin: error.origin().wire_code(),
        facts: Vec::new(),
        query_field: None,
    };
    let limits = crate::wire::limits(crate::wire::MAX_REPLY_BYTES);
    let frame = RpcMethod::Query
        .encode_response(Err::<(), _>(error), limits)
        .unwrap();
    assert_eq!(
        frame,
        RpcMethod::Query
            .encode_response(Err::<(), _>(expected), limits)
            .unwrap()
    );
    assert_eq!(
        RpcMethod::Query
            .decode_response::<Result<(), Error>>(&frame, limits)
            .unwrap(),
        Err(error.clone())
    );
}

struct DiagnosticCanister;

impl icydb_core::traits::Path for DiagnosticCanister {
    const PATH: &'static str = "error::tests::DiagnosticCanister";
}

impl icydb_core::traits::CanisterKind for DiagnosticCanister {
    const COMMIT_STABLE_KEY: &'static str = "diagnostic.commit.v1";
    const STARTUP_STABLE_KEY: &'static str = "diagnostic.startup.v1";
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "diagnostic.integrity.v1";
}

#[test]
fn real_binding_refusal_projects_without_exporting_generated_source_strings() {
    use crate::db::{DbSession, TypedEntityDescriptor, with_request_execution_root};
    use icydb_core::db::StoreRegistry;

    thread_local! {
        static STORES: StoreRegistry = StoreRegistry::new();
    }
    with_request_execution_root(|root| {
        let session = DbSession::new(icydb_core::db::DbSession::<DiagnosticCanister>::new(
            &STORES,
            root.__core(),
        ));
        // These descriptors fail their actual source-key intake before database
        // access; no allocator or fabricated opaque binding context is needed.
        for source in ["", "invalid source"] {
            let descriptor = TypedEntityDescriptor::new(source, &[], &[]);
            let cause = session.bind_typed_entity(&descriptor).unwrap_err();
            let TypedOperationError::Adapter(TypedAdapterError::BindingUnavailable(context)) =
                &cause
            else {
                panic!("expected a source binding refusal");
            };
            assert_eq!(context.entity_source(), source);
            assert_eq!(context.field_source(), None);
            let error = Error::from(cause);
            assert_eq!(
                error.code(),
                ErrorCode::RUNTIME_BOUNDARY_TYPED_ADAPTER_BINDING_UNAVAILABLE
            );
            assert_eq!(error.class(), ErrorClass::Unsupported);
            assert_eq!(error.origin(), ErrorOrigin::Interface);
            assert!(error.facts().is_empty());
            assert_public_error_frame(&error);
        }
    });
}

#[test]
fn typed_and_exhaustive_error_sources_retain_the_original_causes() {
    use crate::db::{ExhaustiveReadError, ReadSetRevisionError, query::TypedExhaustiveQueryError};
    use std::error::Error as _;

    let database = Error::from_error_code(ErrorCode::QUERY_VALIDATE, ErrorOrigin::Query);
    let operation = TypedOperationError::Database(database.clone());
    assert_eq!(
        operation.source().unwrap().downcast_ref::<Error>(),
        Some(&database)
    );
    let adapter = TypedAdapterError::StaleBinding;
    let operation = TypedOperationError::Adapter(adapter);
    assert_eq!(
        operation
            .source()
            .unwrap()
            .downcast_ref::<TypedAdapterError>(),
        Some(&adapter)
    );

    let revision = ReadSetRevisionError::NonCanonical;
    let exhaustive = ExhaustiveReadError::Revision(revision);
    assert!(exhaustive.source().unwrap().is::<ReadSetRevisionError>());
    let exhaustive = TypedExhaustiveQueryError::Exhaustive(exhaustive);
    assert!(exhaustive.source().unwrap().is::<ExhaustiveReadError>());
    assert!(
        exhaustive
            .source()
            .unwrap()
            .source()
            .unwrap()
            .is::<ReadSetRevisionError>()
    );

    let exhaustive = ExhaustiveReadError::Database(database.clone());
    assert_eq!(
        exhaustive.source().unwrap().downcast_ref::<Error>(),
        Some(&database)
    );
    let exhaustive = TypedExhaustiveQueryError::from(database.clone());
    assert!(exhaustive.source().unwrap().is::<TypedOperationError>());
    assert_eq!(
        exhaustive
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<Error>(),
        Some(&database)
    );
}
