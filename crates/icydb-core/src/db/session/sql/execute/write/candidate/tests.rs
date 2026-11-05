use super::{SqlWriteCandidateBounds, SqlWriteMutationBatch, sql_write_candidate_bounds};
use crate::db::{
    data::AcceptedMutationIntentPatch,
    session::sql::{SqlWriteExecutionBounds, SqlWriteReturningBounds},
};
use icydb_diagnostic_code::{DiagnosticDetail, DiagnosticFactTag, SqlWriteBoundaryCode};

#[test]
fn sql_write_candidate_row_bound_accepts_unbounded_and_within_limit() {
    SqlWriteCandidateBounds::from_max_rows(None)
        .validate_len(2)
        .expect("unbounded candidate rows should be accepted");
    SqlWriteCandidateBounds::from_max_rows(Some(2))
        .validate_len(2)
        .expect("candidate rows equal to the bound should be accepted");
}

#[test]
fn sql_write_candidate_row_bound_rejects_over_limit() {
    let err = SqlWriteCandidateBounds::from_max_rows(Some(1))
        .validate_len(2)
        .expect_err("candidate rows over the bound should reject");

    assert_eq!(
        err.diagnostic().detail(),
        Some(&DiagnosticDetail::SqlWriteBoundary {
            boundary: SqlWriteBoundaryCode::StagedRowsTooMany,
        }),
    );
    assert_eq!(
        err.diagnostic_facts(),
        vec![
            (DiagnosticFactTag::ActualCount, 2),
            (DiagnosticFactTag::Limit, 1),
        ],
    );
}

#[test]
fn sql_write_candidate_bounds_use_tighter_staged_or_returning_cap() {
    let returning_is_tighter = SqlWriteExecutionBounds {
        max_staged_rows: Some(5),
        returning: SqlWriteReturningBounds {
            max_rows: Some(3),
            max_response_bytes: None,
        },
    };
    assert_eq!(
        sql_write_candidate_bounds(Some(returning_is_tighter)).max_rows(),
        Some(3),
    );

    let staged_is_tighter = SqlWriteExecutionBounds {
        max_staged_rows: Some(2),
        returning: SqlWriteReturningBounds {
            max_rows: Some(4),
            max_response_bytes: None,
        },
    };
    assert_eq!(
        sql_write_candidate_bounds(Some(staged_is_tighter)).max_rows(),
        Some(2),
    );
    assert_eq!(sql_write_candidate_bounds(None).max_rows(), None);
}

#[test]
fn sql_write_mutation_batch_validates_its_staged_rows() {
    let mut rows = SqlWriteMutationBatch::<u64>::new();
    rows.push(1, AcceptedMutationIntentPatch::new());
    rows.push(2, AcceptedMutationIntentPatch::new());

    rows.validate_bounds(SqlWriteCandidateBounds::from_max_rows(Some(2)))
        .expect("batch staged rows at the bound should be accepted");

    assert!(
        rows.validate_bounds(SqlWriteCandidateBounds::from_max_rows(Some(1)))
            .is_err(),
        "batch staged rows over the bound should reject",
    );
}
