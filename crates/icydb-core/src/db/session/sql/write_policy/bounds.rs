//! Module: db::session::sql::write_policy::bounds
//! Responsibility: shared SQL write row/returning bound derivation.
//! Does not own: parser-shape classification or statement-family policy.
//! Boundary: maps admitted write lanes into execution and returning caps.

use super::model::{
    SqlWriteBoundedPolicyRejection, SqlWriteExecutionBounds, SqlWriteOrderProof,
    SqlWriteReturningBounds, SqlWriteStagedRowBoundKind,
};

pub(in crate::db::session::sql) const fn combined_optional_row_bound(
    policy_max_rows: Option<u32>,
    configured_max_rows: Option<u32>,
) -> Option<u32> {
    match (policy_max_rows, configured_max_rows) {
        (Some(policy), Some(configured)) => Some(if policy < configured {
            policy
        } else {
            configured
        }),
        (Some(policy), None) => Some(policy),
        (None, Some(configured)) => Some(configured),
        (None, None) => None,
    }
}

const fn sql_write_staged_row_bound(
    kind: SqlWriteStagedRowBoundKind,
    limit: Option<u32>,
) -> Option<u32> {
    match kind {
        SqlWriteStagedRowBoundKind::One => Some(1),
        SqlWriteStagedRowBoundKind::Limit => limit,
        SqlWriteStagedRowBoundKind::Unbounded => None,
    }
}

pub(super) const fn bounded_write_policy_rejection(
    offset: Option<u32>,
    limit: Option<u32>,
    max_limit: u32,
    order_proof: SqlWriteOrderProof,
) -> Option<SqlWriteBoundedPolicyRejection> {
    if offset.is_some() {
        return Some(SqlWriteBoundedPolicyRejection::OffsetUnsupported);
    }

    let Some(limit) = limit else {
        return Some(SqlWriteBoundedPolicyRejection::MissingLimit);
    };
    if limit == 0 {
        return Some(SqlWriteBoundedPolicyRejection::MissingLimit);
    }
    if limit > max_limit {
        return Some(SqlWriteBoundedPolicyRejection::LimitTooHigh);
    }

    match order_proof {
        SqlWriteOrderProof::CanonicalPrimaryKey => None,
        SqlWriteOrderProof::DescendingPrimaryKey => {
            Some(SqlWriteBoundedPolicyRejection::DescendingOrder)
        }
        SqlWriteOrderProof::Missing | SqlWriteOrderProof::Other => {
            Some(SqlWriteBoundedPolicyRejection::MissingCanonicalPrimaryKeyOrder)
        }
    }
}

const fn sql_write_execution_bounds(
    max_staged_rows: Option<u32>,
    returning_requested: bool,
    max_returning_rows: Option<u32>,
    max_returning_response_bytes: Option<u32>,
) -> SqlWriteExecutionBounds {
    let max_rows = if returning_requested {
        combined_optional_row_bound(max_staged_rows, max_returning_rows)
    } else {
        None
    };

    SqlWriteExecutionBounds {
        max_staged_rows,
        returning: SqlWriteReturningBounds {
            max_rows,
            max_response_bytes: max_returning_response_bytes,
        },
    }
}

pub(super) const fn sql_write_execution_bounds_for_staged_kind(
    staged_row_bound_kind: SqlWriteStagedRowBoundKind,
    limit: Option<u32>,
    returning_requested: bool,
    max_returning_rows: Option<u32>,
    max_returning_response_bytes: Option<u32>,
) -> SqlWriteExecutionBounds {
    sql_write_execution_bounds(
        sql_write_staged_row_bound(staged_row_bound_kind, limit),
        returning_requested,
        max_returning_rows,
        max_returning_response_bytes,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_write_staged_row_bound_maps_shared_policy_kinds() {
        assert_eq!(
            sql_write_staged_row_bound(SqlWriteStagedRowBoundKind::One, Some(10)),
            Some(1),
        );
        assert_eq!(
            sql_write_staged_row_bound(SqlWriteStagedRowBoundKind::Limit, Some(10)),
            Some(10),
        );
        assert_eq!(
            sql_write_staged_row_bound(SqlWriteStagedRowBoundKind::Limit, None),
            None,
        );
        assert_eq!(
            sql_write_staged_row_bound(SqlWriteStagedRowBoundKind::Unbounded, Some(10)),
            None,
        );
    }

    #[test]
    fn sql_write_execution_bounds_for_staged_kind_combines_policy_and_returning_caps() {
        let bounded = sql_write_execution_bounds_for_staged_kind(
            SqlWriteStagedRowBoundKind::Limit,
            Some(10),
            true,
            Some(3),
            Some(1024),
        );

        assert_eq!(bounded.max_staged_rows, Some(10));
        assert_eq!(bounded.returning.max_rows, Some(3));
        assert_eq!(bounded.returning.max_response_bytes, Some(1024));

        let primary_key_only = sql_write_execution_bounds_for_staged_kind(
            SqlWriteStagedRowBoundKind::One,
            Some(10),
            false,
            Some(3),
            Some(1024),
        );

        assert_eq!(primary_key_only.max_staged_rows, Some(1));
        assert_eq!(primary_key_only.returning.max_rows, None);
        assert_eq!(primary_key_only.returning.max_response_bytes, Some(1024),);
    }

    #[test]
    fn bounded_write_policy_rejection_keeps_public_priority_order() {
        assert_eq!(
            bounded_write_policy_rejection(
                Some(1),
                None,
                10,
                SqlWriteOrderProof::CanonicalPrimaryKey,
            ),
            Some(SqlWriteBoundedPolicyRejection::OffsetUnsupported),
        );
        assert_eq!(
            bounded_write_policy_rejection(None, None, 10, SqlWriteOrderProof::CanonicalPrimaryKey,),
            Some(SqlWriteBoundedPolicyRejection::MissingLimit),
        );
        assert_eq!(
            bounded_write_policy_rejection(
                None,
                Some(0),
                10,
                SqlWriteOrderProof::CanonicalPrimaryKey,
            ),
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
            bounded_write_policy_rejection(
                None,
                Some(10),
                10,
                SqlWriteOrderProof::CanonicalPrimaryKey,
            ),
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
}
