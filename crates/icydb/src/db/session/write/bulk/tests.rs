//! Structural-work admission and stop behavior, independent of database fixtures.

use super::*;

#[test]
fn invalid_empty_and_work_limited_bulk_never_attempt_an_unadmitted_input() {
    let invalid = execute_bulk(
        0,
        TypedBulkWriteLimits {
            max_commits: 0,
            ..Default::default()
        },
        |_, _| panic!("invalid limits must not execute"),
    );
    assert!(matches!(
        invalid.unwrap_err().error,
        TypedBulkWriteError::InvalidLimits
    ));
    let empty = execute_bulk(0, TypedBulkWriteLimits::default(), |_, _| {
        panic!("empty input must not execute")
    })
    .unwrap();
    assert_eq!(empty.next_input, 0);
    for (max_attempted_rows, max_commits) in [(2, 10), (10, 1)] {
        let mut calls = Vec::new();
        let failure = execute_bulk(
            3,
            TypedBulkWriteLimits {
                max_batch_rows: 2,
                max_attempted_rows,
                max_commits,
                ..Default::default()
            },
            |start, count| {
                calls.push((start, count));
                Ok(DynamicTypedBatchAttempt::Committed(Vec::new()))
            },
        )
        .unwrap_err();
        assert_eq!(calls, vec![(0, 2)]);
        assert_eq!(failure.progress.next_input, 2);
        assert_eq!(failure.progress.attempted_rows, 2);
        assert!(matches!(failure.error, TypedBulkWriteError::WorkLimit));
    }
}

#[test]
fn ordinary_failures_stop_once_and_keep_the_successful_prefix() {
    let mut calls = Vec::new();
    let failure = execute_bulk(
        6,
        TypedBulkWriteLimits {
            max_batch_rows: 2,
            ..Default::default()
        },
        |start, count| {
            calls.push((start, count));
            if start == 0 {
                Ok(DynamicTypedBatchAttempt::Committed(Vec::new()))
            } else {
                Err(TypedOperationError::Adapter(
                    TypedAdapterError::StaleBinding,
                ))
            }
        },
    )
    .unwrap_err();
    assert_eq!(calls, vec![(0, 2), (2, 2)]);
    assert_eq!(failure.progress.next_input, 2);
    assert_eq!(failure.progress.batches.len(), 1);
    assert_eq!(failure.progress.attempted_rows, 4);
    assert!(matches!(
        failure.error,
        TypedBulkWriteError::Operation(TypedOperationError::Adapter(
            TypedAdapterError::StaleBinding
        ))
    ));
}

#[test]
fn public_byte_budget_and_backlog_codes_alone_never_authorize_retry() {
    use icydb_diagnostic_code::RuntimeBoundaryCode;

    for boundary in [
        RuntimeBoundaryCode::MutationBatchResultBytesExceeded,
        RuntimeBoundaryCode::MutationBatchStagedBytesExceeded,
        RuntimeBoundaryCode::ExecutionBudgetExceeded,
        RuntimeBoundaryCode::ConvergenceBacklogPressure,
    ] {
        let mut calls = 0;
        let failure = execute_bulk(4, TypedBulkWriteLimits::default(), |_, _| {
            calls += 1;
            Err(TypedOperationError::Database(Error::from_runtime_boundary(
                boundary,
                crate::error::ErrorOrigin::Executor,
            )))
        })
        .unwrap_err();
        assert_eq!(calls, 1);
        assert_eq!(failure.progress.next_input, 0);
        assert!(matches!(failure.error, TypedBulkWriteError::Operation(_)));
    }
}
