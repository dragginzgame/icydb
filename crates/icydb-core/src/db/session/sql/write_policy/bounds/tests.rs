use super::*;

#[test]
fn bounded_write_policy_rejection_keeps_public_priority_order() {
    assert_eq!(
        bounded_write_policy_rejection(Some(1), None, 10, SqlWriteOrderProof::CanonicalPrimaryKey,),
        Some(SqlWriteBoundedPolicyRejection::OffsetUnsupported),
    );
    assert_eq!(
        bounded_write_policy_rejection(None, None, 10, SqlWriteOrderProof::CanonicalPrimaryKey,),
        Some(SqlWriteBoundedPolicyRejection::MissingLimit),
    );
    assert_eq!(
        bounded_write_policy_rejection(None, Some(0), 10, SqlWriteOrderProof::CanonicalPrimaryKey,),
        Some(SqlWriteBoundedPolicyRejection::MissingLimit),
    );
    assert_eq!(
        bounded_write_policy_rejection(None, Some(11), 10, SqlWriteOrderProof::Other),
        Some(SqlWriteBoundedPolicyRejection::LimitTooHigh),
    );
    assert_eq!(
        bounded_write_policy_rejection(
            None,
            Some(10),
            10,
            SqlWriteOrderProof::DescendingPrimaryKey,
        ),
        Some(SqlWriteBoundedPolicyRejection::DescendingOrder),
    );
    assert_eq!(
        bounded_write_policy_rejection(None, Some(10), 10, SqlWriteOrderProof::Missing),
        Some(SqlWriteBoundedPolicyRejection::MissingCanonicalPrimaryKeyOrder),
    );
    assert_eq!(
        bounded_write_policy_rejection(None, Some(10), 10, SqlWriteOrderProof::CanonicalPrimaryKey,),
        None,
    );
}

#[test]
fn sql_write_execution_bounds_combines_staged_and_returning_limits() {
    let bounded_returning = sql_write_execution_bounds(Some(10), true, Some(4), Some(1024));
    assert_eq!(bounded_returning.max_staged_rows, Some(10));
    assert_eq!(bounded_returning.returning.max_rows, Some(4));
    assert_eq!(bounded_returning.returning.max_response_bytes, Some(1024),);

    let staged_only_returning = sql_write_execution_bounds(Some(10), true, None, Some(1024));
    assert_eq!(staged_only_returning.returning.max_rows, Some(10));

    let configured_only_returning = sql_write_execution_bounds(None, true, Some(4), Some(1024));
    assert_eq!(configured_only_returning.returning.max_rows, Some(4));

    let no_returning = sql_write_execution_bounds(Some(10), false, Some(4), Some(1024));
    assert_eq!(no_returning.max_staged_rows, Some(10));
    assert_eq!(no_returning.returning.max_rows, None);
    assert_eq!(no_returning.returning.max_response_bytes, Some(1024));
}
