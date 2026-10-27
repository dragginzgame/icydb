//! Typed error wrappers preserve their original Rust source chains.

use crate::{
    Error, ErrorOrigin,
    db::{TypedAdapterError, TypedOperationError},
};
use icydb_diagnostic_code::ErrorCode;

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
